# P6-H Replicated Group Coordinator / Recovery Qualification

Date: 2026-10-01

Status: **PASS — P6-H QUALIFIED / P6-I NEXT**

This report closes actions **P6-097 through P6-108** only. P6-H binds the already-qualified P6-D/E/F/G group semantics to real committed `__astralog_groups` P4/P3 history, qualifies coordinator restart/failover and bounded runtime admission, and preserves controller-outage data-plane isolation. It does **not** implement durable offset values/fences in `__astralog_offsets`, execute CommitOffsets/FetchOffsets, or advertise/dispatch public API keys 30..35.

## Authority and commit boundary

`GroupCoordinatorRuntime` is deliberately not a second authority plane. For a GroupId it derives the frozen group shard, requires the frozen `__astralog_groups` topology, observes the local P4 partition actor, and serves only while that actor is the current P3 leader at the caller's exact coordinator epoch. P5 controller metadata supplies topology and broker placement, but never declares group authority.

A candidate GroupRecord is first replay-validated against committed history in a temporary `GroupStateMachine`. That validation does not publish authority. A non-idempotent candidate is encoded as the frozen `InternalRecordV1::GroupState`, appended through P4 with `AckRequirement::Quorum`, then committed history is replayed again. Success is returned only if that replay reconstructs the candidate as the current canonical state. Therefore a broker response cannot make an uncommitted group transition authoritative.

An exact candidate that is already the current committed state is recognized as idempotent and returns `appended=false` without another write. Errors after a mutation proposal are classified as ambiguous rather than converted into blind retry permission.

## Committed-history recovery

Recovery begins at stream offset zero and fetches committed P4 records in a configured finite batch size. The coordinator advances by actual stored offsets and applies only records whose key equals the canonical encoded GroupId state key.

For the matching key, recovery fails closed when it sees:

- a tombstone;
- malformed/non-canonical internal bytes;
- an internal record kind other than GroupState;
- a GroupState sequence that violates exact `GroupStateMachine::apply_committed` state-version/epoch rules.

Unrelated keys are ignored. There is no guessed repair, volatile “latest state” shortcut, side database, or controller-owned copy of group authority.

## Workflow recovery and coordinator failure

The real RF3 qualification exercises the durable workflow boundaries rather than only unit-level replay:

- a committed member admission is recovered after coordinator leader death;
- a leader killed before a first join commits leaves no phantom group history on the successor;
- PREPARING_EPOCH_FENCE is committed, the coordinator is killed, and the higher-term successor reconstructs the exact pending epoch/fence source state;
- cooperative REVOKING/release state is durably committed, the coordinator is killed, and the successor reconstructs the same released-partition authority before target activation continues;
- a committed Leave survives coordinator failure with the departed member absent after recovery, which is also the durable authority path used by the P6-G timeout candidate;
- retrying an identical already-committed mutation after a hypothetical lost successful response is idempotent and does not advance state version or epoch again.

These scenarios preserve the P6-F one-way ownership rule: recovery never invents a target owner before the required release/fence evidence and never resurrects an old member from leader-local state.

## Controller-outage isolation

After the P6 internal topics already exist and their P3/P4 actors are serving, the qualification stops the complete P5 controller quorum. The current `__astralog_groups` shard still accepts and quorum-commits a group mutation and reconstructs it from committed history.

This proves the intended authority split: a controller outage prevents new P5 metadata mutation but does not revoke already-committed partition assignments or turn the controller into a dependency for ordinary group-log consensus.

## Bounded coordinator runtime

The coordinator runtime has explicit finite bounds for:

- control-request queue count;
- mutation-request queue count;
- total admitted pending bytes shared across both classes;
- committed-history replay page size.

Admission uses non-blocking `try_send` plus `try_acquire_many_owned`; queue or byte saturation fails immediately with `Busy` instead of creating an unbounded task/backlog. Shutdown signals the coordinator worker, closes its work loop and awaits the task before broker shutdown completes.

The separate control channel is selected before mutation work when both are ready, preserving the P6-G control-priority foundation without adding P6-I/P6-J offset/API queues early.

## Restart durability boundary repair

P6-H stress uncovered an intermittent pre-existing P4 restart ordering gap. P1 intentionally does **not** infer that a surviving uncommitted tail was fsynced merely because valid bytes are present after reopen; its runtime `durable_raft_index` therefore restarts at the committed boundary. P4, however, reconstructed those same validated surviving entries into Raft's stable log. A new leader could then send a commit-only update for one of those entries, causing the follower's first persistence batch to contain only `AdvanceCommit` and correctly fail P1's “cannot commit beyond locally durable Raft index” guard.

The repair preserves the P1 contract and on-disk format: during `PartitionPersistence::open_inner`, P4 explicitly re-fsyncs the validated recovered log tail **before** constructing the `RecoveredState` handed to Raft. P1 still makes no durability assumption; P4 establishes the durability it requires before claiming the entries are stable.

A dedicated regression reopens an uncommitted surviving P4 entry and proves a commit-only `AdvanceCommit` then succeeds. The original P1 regression `restart_does_not_assume_uncommitted_tail_was_durable` remains green. Three consecutive full P6-H coordinator failover/rejoin stress runs passed after the repair.

## Mechanical qualification

- formatting — **PASS**
- affected broker/replication/storage Clippy `--all-targets -D warnings` — **PASS**
- group-coordinator unit qualification — **3/3 PASS**
- P6-C/H real RF3 routing/recovery/failover integration — **3/3 PASS**
- `astralog-groups` — **44/44 PASS**
- protocol — **38/38 PASS**
- frozen P2 protocol goldens — **11/11 PASS**
- P6 protocol goldens — **6/6 PASS**
- frozen P5 dynamic integration — **4/4 PASS**
- controller — **91/91 PASS**
- Raft — **122/122 PASS**
- replication — **47/47 PASS**
- storage — **43/43 PASS**
- complete workspace — **444/444 PASS**

The P6 routing integration suite is test-locally serialized because each case launches an independent heavyweight three-controller/four-broker cluster; this prevents parallel setup churn from turning an already-committed broker-registration retry into a stale-incarnation harness failure. Production scheduling/authority behavior is unchanged.

No P1 format bytes, P2 request/response body bytes, P3 consensus format, P4 wire format or P5 controller metadata format changed. P6 API keys 30..35 remain unadvertised and undispatched.

## Boundary

P6-I and later remain disabled. In particular P6-H does not add:

- canonical OffsetValue application in `__astralog_offsets`;
- executable OffsetGroupFence / OffsetOwnerFence persistence;
- CommitOffsets validation, atomic write or quorum-response handling;
- FetchOffsets committed-only execution;
- broker/client handlers or ApiVersions advertisement for API keys 30..35;
- any P7+ producer-idempotency, compaction, work-queue, security or operations behavior.

The next bounded checkpoint is **P6-I only — actions P6-109 through P6-124: durable offsets and fencing**.
