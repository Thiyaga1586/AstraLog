# Proposed ADR — P8-E Persistence Frontier for True Per-Partition Group Commit

Status: **ACCEPTED FOR P8-E RE-FREEZE — EXACT-HEAD FREEZE NOTE REQUIRED FOR IMPLEMENTATION AUTHORITY**

Date: 2026-10-06

Supersedes: **RAFT_V1 section 8.1 single-outstanding persistence scheduling only, once the exact re-freeze commit carries its PASS note**

Affected frozen decisions/contracts:

- P3 one in-flight persistence barrier per Raft group;
- P4 synchronous PersistenceBatch execution/completion;
- P8-E per-partition group-commit contract.

## Observed problem

P8-E requires multiple original persistence tokens for one partition to be eligible for one physical
sync. The current P3 core permits only one outstanding persistence token, and P4 completes that token
only after its durability operation succeeds. The next same-partition token therefore cannot be
created early enough to share the sync.

## Evidence

- P3_FOUNDATION_REPORT.md freezes one in-flight persistence barrier per Raft group and one explicit
  durability frontier.
- RaftCore rejects a second staged persistence batch with PersistenceAlreadyInFlight.
- P4_REPLICATION_V1.md requires PersistenceCompleted(token) only after the batch's required durability
  succeeds.
- P8_PERFORMANCE_V1.md requires multiple original tokens to complete independently from proven
  durable state and allows one physical sync to satisfy multiple earlier tokens.

## Accepted decision

Replace the P3 **single outstanding token** representation with a bounded ordered persistence queue
while retaining a single monotonic **durability frontier** and introducing a separate non-authoritative
**staged logical frontier** for not-yet-durable append history.

The core may emit multiple ordered PersistenceBatch tokens up to fixed count/byte bounds, but:

1. persistence effects remain in exact Raft order;
2. completion is prefix-only;
3. token N+1 may never become durable/visible before token N;
4. consensus actions gated on durability remain gated on their exact token;
5. AdvanceCommit cannot cross an undurable append dependency;
6. hard-state term/vote changes are barriers unless their exact durable-order proof is modeled;
7. TruncateSuffix, InstallSnapshot, DiscardPrefix and rebootstrap are hard queue barriers;
8. any ambiguous sync fails every token whose required durable prefix cannot be proven;
9. queue count/bytes and maximum group delay are finite;
10. existing ack semantics remain unchanged.
11. staged entries never advance durable self-match, durable commit or acknowledgement authority;
12. only append-compatible work may coexist in the queue; hard-state, commit, truncate, snapshot/install, discard/rebootstrap and durability-dependent transitions are barriers;
13. one physical sync may prove multiple contiguous tokens, but completion is fed back in strict token order;
14. queue depth one reproduces the historical single-token behavior.

## Required model changes

RaftCore needs:

- ordered pending persistence descriptors;
- a staged append frontier separate from durable stable_log;
- next token generation independent from completion of the oldest token only for append-compatible
  transitions whose index/term can be derived from the staged frontier;
- explicit dependency mapping from pending durable actions to tokens;
- prefix completion/failure validation;
- bounds in RaftLimits;
- deterministic queue-delay input owned by the runtime, not consensus authority.

The formal/deterministic model must cover:

- two and three queued appends;
- append followed by AdvanceCommit;
- term/vote hard-state barriers;
- truncate conflicts;
- snapshot/install/rebootstrap barriers;
- fsync success followed by lost completion;
- ambiguous sync failure;
- leadership loss while tokens are staged;
- queue saturation and control-progress behavior.

## Correctness impact

High. This changes a P3 safety boundary and cannot be treated as a local P8 storage optimization.

No token is allowed to be reported complete before proven durability. The purpose of the change is
only to permit multiple not-yet-complete ordered tokens to exist so the storage executor can legally
coalesce compatible writes.

## Performance impact

Potentially positive when multiple adjacent append tokens are available within the group-delay
window. No throughput/latency claim is accepted until P8-H benchmark provenance exists.

## Wire/disk compatibility impact

None intended:

- no protocol version changes;
- no Storage V1 format changes;
- no controller format changes.

The change is in in-memory consensus/runtime persistence scheduling.

## Migration strategy

No on-disk migration should be required. Activation must be gated by the P8 architecture freeze and
qualified against existing persisted state.

## Rollback strategy

Before phase closure, revert to the single-token P3 contract and leave P8-E open. After acceptance
and closure, rollback requires the same architecture-change discipline because runtime behavior will
have been requalified against the queued frontier.

## Required qualification before acceptance

- updated RAFT/P3 persistence contract and foundation report;
- deterministic/formal persistence-frontier proofs;
- Raft full suite;
- storage/replication crash suite;
- P4 leader/quorum ack qualification;
- P6/P7 failover/idempotency qualification;
- P8-C compressed recovery;
- P8-D client batching;
- complete workspace and exact-HEAD closure.

This ADR is accepted as the architecture decision, but implementation remains disabled until the
docs-only P8-E re-freeze commit is exact-HEAD qualified and carries its freeze PASS note.
