# AstraLog Persistence Frontier V2

Status: **FROZEN / IMPLEMENTED BY P8-E CLOSURE CANDIDATE — EXACT-HEAD P8-E PASS NOTE REQUIRED FOR CLOSURE AUTHORITY**

Date: 2026-10-06

## 1. Scope and supersession

Persistence Frontier V2 is the additive P8-E contract for true per-partition persistence group commit.
It supersedes only the scheduling restriction in RAFT_V1 section 8.1 that limited one Raft group to
one unacknowledged Persist effect.

It does not change:

- the meaning of PersistenceCompleted(token);
- the meaning of PersistenceFailed(token, error);
- P3 log/election/commit safety rules;
- P4 disk bytes or Storage V1 formats;
- leader/quorum acknowledgement semantics;
- snapshot, truncation or rebootstrap authority;
- P7 producer fencing/idempotency semantics.

Historical P3/P4 reports remain valid evidence for V1 behavior. Where this document conflicts with
the single-outstanding-token scheduling rule, this V2 contract is authoritative only after its exact
re-freeze commit is qualified.

## 2. Two frontiers, one durability authority

Each Raft group has:

1. a durable frontier — the greatest state whose required persistence token has completed;
2. a staged logical frontier — deterministic append history accepted for future persistence but not
   yet proven durable.

The staged frontier exists only to allocate and validate later append positions while earlier append
tokens are waiting for a shared physical sync.

Staged state is never durability authority. It must not:

- advance self durable match_index;
- satisfy leader-ack or quorum-ack durability;
- advance durable commit state;
- authorize a successful follower AppendEntries response;
- authorize a vote, campaign, term or role action that depends on durable hard state;
- survive restart unless the storage layer actually made it durable.

## 3. Bounded ordered persistence queue

The Raft core may have more than one persistence token outstanding only when every queued token is
append-compatible under this contract.

The queue is bounded by both:

- max_pending_persistence_batches;
- max_pending_persistence_bytes.

Both limits are finite, checked before admission and part of RaftLimits V2 validation. Queue
saturation rejects or defers new append work without dropping already accepted authority.

Persistence tokens remain strictly monotonic. Queue order is token order and operation order.

## 4. Append-compatible token

A token is group-commit compatible only when its persistence operations are append-only and its
continuation does not require an earlier queued token to become durable before the append can be
logically staged.

For P8-E V1 closure the compatible production form is a PersistenceBatch containing exactly one
non-empty Append(entries) operation with contiguous entries.

Multiple compatible append tokens may extend the staged logical frontier. Index assignment and
conflict checks use durable history plus that staged append suffix.

## 5. Hard barriers

The following operations are hard group barriers:

- UpdateHardState;
- AdvanceCommit;
- TruncateSuffix;
- InstallSnapshot;
- DiscardPrefix when coupled to snapshot/rebootstrap sequencing;
- rebootstrap package install;
- any mixed batch containing one of the above;
- any role/term transition whose dependent action requires durability first.

Before a barrier executes, every earlier queued append token must either:

1. complete durably in order; or
2. fail as an unproven suffix.

No barrier may be reordered into or through an append group.

## 6. Completion semantics

PersistenceCompleted(token) retains the RAFT_V1 section 9.1 meaning: every operation for that token
has reached the required durability boundary.

A storage executor may perform one physical sync that proves durability for tokens T1 through Tn
only when:

- T1 through Tn are the exact contiguous queue prefix;
- all required append bytes for those tokens were included before that sync;
- no hard barrier lies inside the group;
- the sync returned success.

After that proof, completion inputs are delivered to the core strictly in token order. The runtime
may not skip a token or complete a later token while an earlier one is unresolved.

Completion of each token independently releases only the P3 continuation bound to that token.

## 7. Failure and ambiguity

If staging fails before a physical sync, the affected token and every later dependent staged token
whose durability cannot be proven fail.

If the physical sync returns an error or ambiguous outcome, success must not be fabricated. Every
token not independently provable durable fails according to invariant I-30.

If a sync succeeded but completion delivery is lost because the process crashes, restart recovery
uses existing P1/P4 durable bytes and P7 ambiguity rules. The pre-crash process does not invent
success.

Persistence failure remains fatal/non-serving wherever frozen P3/P4 currently require it.

## 8. Storage executor

P8-E introduces a bounded per-partition persistence staging executor with finite:

- staged token count;
- staged logical bytes;
- maximum group delay.

The executor may delay the physical append sync only until the earliest of:

- token-count boundary;
- byte boundary;
- maximum group-delay deadline;
- explicit flush or hard barrier;
- shutdown;
- resource pressure requiring bounded progress.

The executor preserves every PersistenceBatch operation order. It may coalesce only the physical
append flush/sync of compatible adjacent append tokens.

## 9. Control and liveness

Group delay is scheduling state, not Raft authority. It uses injected monotonic runtime time.

Pending append pressure must not starve heartbeat, election or control processing. The
implementation must retain bounded control progress while append tokens wait for a group deadline.

A term or role transition that requires a hard-state durability action forces the append group to a
terminal durable-or-failed boundary before that hard-state barrier proceeds.

## 10. Ack semantics

Leader ack remains tied to proven local durability exactly as before. Merely staging an append token
does not satisfy leader ack.

Quorum ack remains tied to P3 majority commit. Group commit changes local sync scheduling only and
does not alter quorum mathematics or follower authority.

Successful follower AppendEntries acknowledgement remains durability-gated.

## 11. Compatibility mode

With max_pending_persistence_batches equal to one, or with group commit disabled, V2 must reproduce
the historical single-token persistence behavior.

No wire, controller or Storage V1 byte version changes are introduced.

## 12. Required deterministic proofs

Before P8-E may close, tests and models must prove:

- exact count and byte one-over queue rejection;
- deterministic maximum-delay flush;
- two and three ordered append tokens match sequential logical execution;
- group completion is prefix-only;
- AdvanceCommit cannot outrun the append durability it depends on;
- hard-state, truncate, snapshot/install and rebootstrap barriers cannot be crossed;
- ambiguous sync never creates success;
- staged-append crash and post-sync/pre-completion crash behavior;
- leader and quorum ack semantics remain unchanged;
- follower durability acknowledgement remains unchanged;
- heartbeats and elections progress under sustained append staging;
- mixed None/LZ4/Zstd history preserves logical order and offsets;
- physical-sync counters distinguish logical tokens from actual sync operations;
- queue-depth-one compatibility reproduces prior P3/P4 behavior.

## 13. Requalification boundary

Because this contract supersedes a P3 safety boundary, P8-E implementation qualification must
include:

- complete astralog-raft deterministic and formal suites;
- P4 persistence/RF3 crash/failover qualification;
- storage and replication recovery/crash suites;
- P6 group/offset/fencing/failover suite;
- P7 idempotency/failover suite;
- P8-C compressed recovery proofs;
- P8-D buffered producer proofs;
- complete bounded workspace and mechanical gate.

P8-F remains disabled until P8-E independently closes on an exact qualified SHA.
