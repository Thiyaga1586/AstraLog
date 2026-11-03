# P6 — Consumer Offsets and Cooperative Consumer Groups Closure Report

Date: 2026-10-02
Status: **PASS — P6 CLOSED AFTER EXACT-HEAD QUALIFICATION / P7 PLANNING NEXT**

## 1. Closure statement

P6 is the first AstraLog phase that adds durable consumer-group authority and durable consumer offsets. It is closed only because the final implementation keeps those two authorities explicit and independently fenced:

- `__astralog_groups` owns canonical group membership, GroupEpoch, MemberEpoch and cooperative assignment state;
- `__astralog_offsets` owns durable offset values plus GroupEpoch and partition-owner fences;
- a group epoch cannot activate before its offset group fence is durably committed;
- a target partition owner cannot activate before the old owner is durably released and the corresponding offset owner fence is committed;
- committed state is reconstructed from P4 committed history rather than from a side database or volatile cache;
- public APIs 30..35 enter through the current group coordinator, while CommitOffsets/FetchOffsets are routed internally to the independently elected offsets-shard leader;
- clients cannot choose or race the offsets coordinator directly.

The authoritative P6 closure commit is the commit containing this report and the fully checked `P6_CHECKLIST.md`. Its exact SHA is resolved after commit with `git rev-parse HEAD`. P6 is not considered closed until that exact SHA passes the post-commit gate and receives the qualification Git note described below.

## 2. Qualified P6 history

The final P6 implementation was built and independently qualified in bounded checkpoints:

- P6-A — frozen consumer-group/offset contract and execution checklist;
- P6-B — bounded public/internal types, codecs, keys and golden bytes;
- P6-C — internal topic bootstrap, routing and coordinator authority;
- P6-D — pure durable group state machine;
- P6-E — deterministic uniform-sticky assignor;
- P6-F — cooperative revoke/release/fence/activation workflow;
- P6-G — heartbeat/session timeout/static membership semantics;
- P6-H — replicated group coordinator and committed-history recovery;
- P6-I — durable offsets, GroupEpoch/owner fencing and committed FetchOffsets;
- P6-J — public broker/client integration for API keys 30..35;
- P6-K — real failure/churn qualification plus deterministic model/property campaigns;
- P6-L — compatibility/scope audit and exact closure.

The final bounded implementation checkpoints immediately before closure are:

- `e097806` — replicated group coordinator and recovery;
- `a9d320d` — durable offsets and fencing;
- `78ee8d5` — broker/client integration;
- `24e74e3` — failure/churn/model qualification.

## 3. Frozen safety invariants

P6 closes with the following phase invariants mechanically qualified:

1. **I-18 — single authoritative owner.** No TopicPartition has two active owners in canonical group state.
2. **I-19 — cooperative movement.** Ownership changes only through old-active -> released -> new-active; unaffected assignments remain usable during partial rebalance.
3. **I-20 — stale offset writers fenced.** Old GroupEpoch, old MemberEpoch and released old-owner writes cannot overwrite the effective committed offset after the corresponding durable fence takes effect.
4. **Committed group recovery.** Group coordinator failover/restart reconstructs committed state and pending workflows without guessing volatile authority.
5. **Committed offset recovery.** Offsets-shard failover/restart reconstructs group fences, owner fences and latest logical offset values from committed history only.
6. **Bounded operation.** Coordinator queues, byte admission, replay pages, session maps, client caches, retry counts, request shapes and encoded state all have explicit finite limits.

## 4. Group authority and cooperative handoff

Canonical group state uses monotonically versioned committed `GroupRecordV1` records. Membership mutation is atomic and fail-closed on stale GroupEpoch/MemberEpoch, duplicate static identity, invalid assignment ownership, codec bounds and illegal phase transitions.

Uniform-sticky assignment freezes a deterministic objective: minimize maximum load, then movement count, then canonical owner ordering. Exhaustive small-state qualification matches the production assignor to that oracle.

Every ownership transition uses the frozen cooperative sequence:

`PREPARING_EPOCH_FENCE -> committed OffsetGroupFence -> epoch activation -> REVOKING/release -> committed OffsetOwnerFence -> target activation -> STABLE`

Real RF3 qualification kills the coordinator in STABLE, PREPARING and REVOKING and proves the higher-term successor reconstructs the exact committed workflow before progress resumes.

## 5. Session timeout and static membership

Heartbeats mutate only leader-local volatile observations. Expiry itself never revokes authority; it produces a candidate that must still pass the durable group transition and cooperative fence workflow.

New coordinator terms reconstruct committed members and grant one fresh full session grace because the prior leader's heartbeat observations are intentionally non-replicated. A dedicated RF3 test proves a silent member survives the new leader's fresh grace and is still removed once that grace expires.

Static replacement increments MemberEpoch exactly once. Concurrent replacement attempts converge to one canonical member authority and the old session is fenced.

## 6. Durable offsets and fencing

`OffsetValueV1`, `OffsetGroupFenceV1` and `OffsetOwnerFenceV1` are folded from committed `__astralog_offsets` history by a deterministic logical state machine.

CommitOffsets validates the entire request before proposal: canonical request shape, group/member epochs, current membership, active assignment, effective durable owner, metadata bounds and projected logical-state growth. The batch either fails before proposal or commits as a complete request.

Success is returned only after P4 quorum commit and committed-history replay verification. FetchOffsets exposes committed history only. Authorized rewinds are intentionally valid; fencing prevents stale writers rather than imposing application-progress policy.

Real RF3 tests kill the offsets leader before group-fence commit and separately after old-owner release but before owner-fence commit. Neither group epoch nor target owner activates early. The existing stale old-group and stale old-owner races remain rejected after the respective fence becomes durable.

## 7. Public broker/client integration

API keys 30..35 activate only when committed feature level and both frozen internal topics are ready. Before activation the original `UNSUPPORTED_API_KEY` surface remains exact.

JoinSubscribe, Heartbeat, RevokeAck and Leave require the exact current group-coordinator P3 leader/term. Wrong-broker requests return `NOT_LEADER` plus the current coordinator hint without mutating history.

CommitOffsets and FetchOffsets always enter through the group coordinator. The broker then uses a bounded implementation-private hop to the independent offsets leader. Private key 65000 is not part of the public ApiKey registry or ApiVersions and does not alter the frozen P4 Raft peer transport.

The Rust client keeps bounded non-regressing coordinator/member caches, a bounded in-flight group semaphore and finite retry budget. Ambiguous transport loss after mutation is surfaced explicitly rather than blindly retried.

## 8. Failure, churn and model qualification

P6-K adds the following final stress evidence:

- join and Leave while committed offset traffic continues;
- crash without Leave plus durable timeout/fence/reassignment;
- slow heartbeat preservation before timeout and stale fencing afterward;
- coordinator failure in STABLE, PREPARING and REVOKING;
- offsets-leader failure at group-fence and owner-fence boundaries;
- static-member replacement race;
- live topic expansion using a deterministic higher-epoch cooperative rebalance;
- 32 deterministic seeds x 32 membership transitions, executed twice with byte-identical traces;
- exhaustive frozen assignor oracle;
- bounded cooperative handoff state-space model;
- all 120 permutations of the five-event offset-fence model;
- committed-history tombstone/malformed/conflict/state-version-gap rejection;
- member/subscription/assignment/offset-batch one-over-limit rejection;
- 10,000-cycle control-priority model plus real RF3 scheduling pressure.

## 9. P1-P5 compatibility audit

### P1 storage bytes

P1 format freeze is `3515000`. Current `docs/FORMAT_V1.md`, `astralog-storage/src/format.rs` and `index.rs` are source-identical to that checkpoint. Later partition durability/recovery mechanics do not change Storage V1 frame/index bytes. Storage qualification is **43/43 PASS**, including exact V1 golden checks.

### P2 wire compatibility

P2 closure is `a3cf08d`. The original `crates/astralog-protocol/tests/golden_vectors.rs` is source-identical to that closure and all **11/11** original goldens pass. P6 adds separate API 30..35 codecs/goldens without rewriting old fixtures.

### P3 consensus

P3 closure is `4a3474d`; P5 closure is `ea6c311`. P6 introduces zero source delta in `crates/astralog-raft` relative to the already-qualified P5 closure state. Current Raft regression is **122/122 PASS**.

### P4 replication/visibility

P4 closure is `54aa8d5`. P6's only replication-core source change relative to P5 closure is the P6-H restart repair that re-fsyncs a validated surviving uncommitted P1 tail before reconstructing it as Raft-stable history. Replication is **47/47 PASS** and the frozen RF3 quorum/redirect/term/committed-read E2E is **1/1 PASS**.

### P5 controller/placement

P5 closure is `ea6c311`. P6 controller changes only reserve and validate the frozen P6 internal-topic topology. Existing P5 placement/movement/runtime logic is otherwise unchanged. Controller is **91/91 PASS** and P5 dynamic integration is **4/4 PASS**.

## 10. Later-phase scope audit

P6 deliberately does **not** implement later architecture phases:

- **P7 producer idempotency:** Produce V0 still has no producer session, producer epoch, sequence number, retry token or request deduplication. P5's producer-ID block allocator remains allocation infrastructure only.
- **P8 compression/performance:** no compression codec/runtime or benchmark claim is introduced; timing values in P6 tests are correctness bounds, not product performance numbers.
- **P9 retention/compaction:** no physical group/offset compactor, retention timer, tombstone lifecycle or segment cleanup policy exists. Logical keyed replay is not physical compaction.
- **P10 queue semantics:** no visibility timeout, lease, redelivery counter, retry queue or DLQ semantics exist. Internal bounded queues are scheduler/admission structures only.
- **P11 production security/operations:** no TLS/mTLS/authentication/ACL enforcement, broker drain workflow or continuous balancing policy is introduced. The private P6 broker hop remains inside the pre-P11 trusted-cluster boundary.
- **P12 final chaos/benchmark closure:** no final performance/chaos completion claim is made in P6.

## 11. Pre-commit closure qualification

The final P6-L pre-commit gate is green:

- `cargo fmt --all -- --check` — **PASS**
- `cargo clippy --workspace --all-targets --all-features -- -D warnings` — **PASS**
- `git diff --check` — **PASS**
- broker — **19/19**
- P2 network integration — **9/9**
- P4 RF3 integration — **1/1**
- P5 dynamic integration — **4/4**
- P6 RF3 routing/failover/churn/public integration — **9/9**
- client — **10/10**
- common — **2/2**
- controller — **91/91**
- `astralog-groups` — **55/55**
- protocol — **38/38**
- frozen P2 goldens — **11/11**
- P6 goldens — **6/6**
- Raft — **122/122**
- replication — **47/47**
- storage — **43/43**
- doc tests — **PASS**
- complete serialized workspace — **467/467 PASS**

The workspace command is intentionally serialized with `-j 1` on this Windows machine because earlier P6 qualification established that parallel test-target linking can exhaust the local paging file. Serialization changes neither test semantics nor coverage.

During P6-L an initial pre-closure workspace attempt produced one timing-sensitive failure in the existing coordinator-history RF3 test. No production or test assertion was weakened. The same test then passed unchanged in isolation, passed in the complete serialized **9/9** P6 suite, and passed again in the restarted full **467/467** workspace run. The successful complete rerun is the authoritative pre-commit gate; exact closure HEAD is required to pass again after commit.

## 12. Correctness-debt audit

`TODO|FIXME|XXX|HACK` search over the P6 broker/client/groups/protocol sources and P6 RF3 test harness returns zero matches. No known P6 correctness implementation debt is deferred into P7.

## 13. Known boundaries, not P6 defects

- P6 provides consumer-group/offset semantics, not producer idempotency.
- Logical keyed group/offset history can grow physically until P9 defines retention/compaction policy.
- Broker-private P6 coordination uses the pre-P11 trusted-cluster transport boundary; P11 owns cryptographic/authenticated production transport.
- P6 correctness qualification is not a throughput/latency benchmark.
- P7 must freeze its own contract/checklist before implementation begins; P6 closure does not authorize architecture changes outside that process.

## 14. Exact-HEAD closure procedure

1. commit this report, the fully checked P6 checklist and canonical status updates as the P6 closure commit;
2. require `git status --short` to be empty;
3. resolve the exact closure SHA with `git rev-parse HEAD`;
4. rerun formatting, workspace Clippy and the complete serialized workspace test gate on that exact SHA;
5. rerun the focused immutable/source compatibility audit against the frozen P1/P2/P5 checkpoints;
6. attach a Git note to the closure commit recording the exact-HEAD PASS evidence;
7. only then declare P6 closed and permit **P7 planning/decomposition**.

P7 implementation remains disabled until a P7 contract/checklist is separately frozen.
