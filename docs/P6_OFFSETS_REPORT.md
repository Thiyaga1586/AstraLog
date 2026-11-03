# P6-I Durable Offsets / Fencing Qualification

Date: 2026-10-01

Status: **PASS — P6-I QUALIFIED / P6-J NEXT**

This report closes actions **P6-109 through P6-124** only. P6-I binds the already-frozen `OffsetValueV1`, `OffsetGroupFenceV1` and `OffsetOwnerFenceV1` representations to real committed `__astralog_offsets` P4/P3 history, qualifies cross-topic stale-write fencing, atomic CommitOffsets semantics, committed-only FetchOffsets, and offsets-shard failover/restart. It does **not** advertise or dispatch public API keys 30..35; P6-J owns broker/client routing and group-coordinator orchestration.

## Logical offset state

`OffsetStateMachine` owns one GroupId's logical offsets state only. It reconstructs:

- the latest committed group fence;
- same-epoch per-partition owner-fence overrides;
- the latest committed `OffsetValueV1` for each `(group, topic, partition)`.

All maps are ordered and bounded by the frozen P6 group-partition limit. Replay validates canonical internal-record bytes, logical keys, monotonic group-fence epochs, owner-fence source versions and exact owner authority. Conflicting/regressing committed histories fail closed rather than being repaired heuristically.

The pre-proposal atomicity gate accounts for the complete batch's projected logical-state growth. A syntactically valid request that would exceed the frozen live-state bound is rejected before any P4 proposal, so replay cannot discover an overflow only after an already-committed write.

## Cross-topic fencing

The real RF3 qualification exercises the race that motivates the two durable fence kinds.

For GroupEpoch advancement, the test first commits the epoch-2 `OffsetGroupFenceV1` while committed group state still exposes epoch 1. An epoch-1 CommitOffsets attempt during this availability gap returns `GROUP_EPOCH_STALE`; only after that proof does the cooperative group machine observe the committed fence and activate epoch 2.

For ownership handoff inside epoch 2, the old owner is durably released in group history, the target `OffsetOwnerFenceV1` is quorum-committed, and a deliberately late write from the old owner using a pre-release group image is rejected as `INVALID_OFFSET_COMMIT`. The target owner is activated only after the cooperative machine observes that exact durable owner fence.

This qualifies P6 invariant I-20 without pretending that a read from `__astralog_groups` and a later write to `__astralog_offsets` form one transaction. The offsets shard independently enforces the durable minimum epoch and exact effective owner at commit time.

## CommitOffsets

The internal P6-I CommitOffsets execution path:

1. recovers committed offsets/fence history from the current offsets-shard P3 leader/term;
2. validates exact GroupId/GroupEpoch/MemberId/MemberEpoch against the authoritative committed group record supplied by the P6 group-coordinator layer;
3. requires every requested partition to be actively assigned to that member;
4. requires the durable effective offsets owner to match the same member authority;
5. validates canonical order, uniqueness, metadata bounds and projected logical-state bounds for the entire batch before proposal;
6. assigns one broker/server commit timestamp for the batch;
7. appends all requested values in one P4 proposal with `AckRequirement::Quorum`;
8. replays committed history and verifies every requested value is current before returning success;
9. returns the actual P3/P4 Raft proposal index as `offsets_commit_index`.

An authorized application may intentionally move `next_offset` backward. P6 does not add a monotonic business-progress policy: the qualification commits offset 120 and then an authorized rewind to 5, which becomes the current committed logical value.

Stale GroupEpoch maps to `GROUP_EPOCH_STALE`; stale MemberEpoch maps to `UNKNOWN_MEMBER`; a member lacking current partition ownership or durable owner-fence authority maps to `INVALID_OFFSET_COMMIT`.

## FetchOffsets

FetchOffsets reconstructs state exclusively through the existing P4 committed Fetch path in bounded pages. It does not inspect uncommitted partition tail bytes or storage files directly. Requested partitions return their latest committed `OffsetValueV1` or `None` when no committed value exists.

The RF3 test proves metadata and the broker-generated commit timestamp survive offsets-shard leader failure, continued writes on the higher-term successor, replica restart/rejoin, and leadership transfer back to the restarted broker.

## Lost response semantics

P6-I intentionally does not introduce P7 producer/request idempotency. If a successful CommitOffsets response is lost, an equivalent retry may append another logical replacement record with a later server timestamp and later commit index. What is guaranteed is authority safety: the retry cannot regress GroupEpoch/MemberEpoch/owner authority, and the effective logical offset/metadata remain the requested current-authority value.

Post-proposal routing/leadership errors are classified as mutation-ambiguous rather than converted into blind safe-retry claims.

## Bounded runtime and recovery

`OffsetCoordinatorRuntime` mirrors the P6-H bounded coordinator structure:

- bounded control queue;
- bounded mutation queue;
- one shared byte semaphore sized to admit a maximum frozen CommitOffsets request plus authoritative group record;
- bounded committed-history replay page size;
- fail-fast `Busy` admission;
- explicit worker shutdown and join.

Unrelated groups sharing the same physical shard are ignored by logical-key identity. Malformed/tombstone data for another group cannot poison this group's replay; a tombstone or malformed record targeting this group fails closed because P9 physical compaction/tombstone semantics do not exist yet.

## Real RF3 failure qualification

The P6-I scenario runs on the existing three-controller/four-broker cluster with the real 64-partition RF3 P6 internal topics. It proves:

- group fence commits before epoch activation;
- stale old-epoch commit rejection in the cross-topic race window;
- owner fence commits before target activation;
- late old-owner rejection;
- active-assignment enforcement and atomic multi-offset rejection;
- server timestamp + metadata persistence;
- authorized rewind;
- stale MemberEpoch rejection;
- committed-only FetchOffsets;
- lost-response equivalent retry safety;
- offsets-shard leader death and higher-term recovery;
- continued writes after failover;
- failed-replica rejoin/restart and recovery after leadership transfer back.

The focused P6-I RF3 scenario passed **1/1** in isolation. The internal-topic bootstrap/failover/restart scenario also passed **1/1** after its test harness was hardened to treat only the old leader's exact post-transfer observation timeout as ambiguous; a fresh client must still prove the requested target broker is leader at a strictly higher term. Production preferred-leader logic is unchanged. The final clean P6 routing/failover suite passed **4/4**.

Committed replay also fails closed when a logical key claims this GroupId but the decoded record value claims another GroupId. Unrelated-group keys are ignored before decode, but matching-key/value identity mismatch is corruption rather than silently skipped history.

## Mechanical qualification

- `cargo fmt --all -- --check` — **PASS**
- `git diff --check` — **PASS**
- `cargo clippy --workspace --all-targets -- -D warnings` — **PASS**
- broker unit tests — **19/19 PASS**
- P2 network integration — **9/9 PASS**
- P4 RF3 integration — **1/1 PASS**
- P5 dynamic integration — **4/4 PASS**
- P6 RF3 routing/failover integration — **4/4 PASS**
- client — **6/6 PASS**
- common — **2/2 PASS**
- controller — **91/91 PASS**
- `astralog-groups` — **50/50 PASS**
- protocol — **38/38 PASS**
- frozen P2 golden vectors — **11/11 PASS**
- P6 golden vectors — **6/6 PASS**
- Raft — **122/122 PASS**
- replication — **47/47 PASS**
- storage — **43/43 PASS**
- complete workspace — **453/453 PASS**

## P9 boundary

P6-I defines only logical replacement identity. No physical compaction, retention clock, tombstone lifecycle, segment rewrite, compaction checkpoint or retention policy is introduced. `__astralog_offsets` recovery still scans committed history and folds it into bounded logical state. Physical cleanup remains P9.

## Boundary

P6-J and later remain disabled. In particular P6-I does not add:

- ApiVersions advertisement for API keys 30..35;
- broker network dispatch/response handling for JoinSubscribe, Heartbeat, RevokeAck, Leave, CommitOffsets or FetchOffsets;
- client coordinator caching/retry/member-state workflow;
- the P6-J group-coordinator-to-offsets-shard routing layer that prevents a client-side two-coordinator race;
- P7 request/producer idempotency;
- P9 physical compaction/retention;
- P10 queue semantics;
- P11 security/drain/continuous balancing.

The next bounded checkpoint is **P6-J only — actions P6-125 through P6-138: broker/client integration**.
