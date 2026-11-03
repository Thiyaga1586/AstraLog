# P9-E Copy-on-Write Keyed Compaction Qualification Report

Status: **P9-E PRE-COMMIT QUALIFICATION CANDIDATE — FULL SERIALIZED GATE / CLOSURE COMMIT PENDING**

Authoritative parent: `fe64a0a23166840b90e81ef3e820952490627483` (P9-D exact-HEAD PASS)

Scope: **P9-088..P9-111 only**. P9-F producer-state checkpoints, P9-G tombstone removal/compaction-safe authority,
P9-H cleaner scheduling/throttling and every later checkpoint remain disabled.

## 1. Cleaner-plan authority

P9-E adds a deterministic `CompactionPlan` that captures the exact manifest generation, controller policy epoch, persisted P3
snapshot floor, committed Raft index, committed logical log-start maintenance state, active filename, source identities and
finite resource ceilings. The plan is a proposal for a local physical optimization only; it creates no new logical authority.

The broker exposes one explicit `run_keyed_compaction_once` maintenance hook. It is usable only after committed feature level 5,
on an ACTIVE topic with `Compact` or `CompactDelete`, under the exact current partition leader epoch. There is no background
cleaner task in P9-E; P9-H owns scheduling, throttling and queueing.

Before planning, the leader persists a P3 snapshot through the already-qualified snapshot path. Replacement sources must be
immutable, committed and wholly at/below that persisted snapshot floor. The cleaner may inspect newer committed history to
choose the latest key winner, but it cannot rewrite any source frame above the floor.

## 2. Resource bounds

`CompactionLimits` provides hard nonzero ceilings for:

- source segment count;
- aggregate source `.log` bytes;
- decoded record count;
- decoded record/key/header/value memory;
- key-index entry count;
- key-index memory;
- aggregate replacement output bytes.

All accumulation uses checked arithmetic. One-over decoded-record, source-byte and key-index-entry cases fail before manifest
publication. The active mutable segment is never a source.

## 3. Winner semantics

The planner streams only committed manifest-authoritative retained history and builds a deterministic BTreeMap from each
non-null key to its greatest original committed stream offset at/above `log_start_offset`.

Ordinary compaction then follows the frozen rules:

- null-key records are always retained unless P9-D has already moved the logical floor past them;
- keyed non-tombstones are omitted only when a greater committed offset for that key exists;
- tombstones are conservatively retained in P9-E, including the latest tombstone required by the contract;
- surviving `DataRecord` values move unchanged, preserving timestamps, key/value nullability, header names/values and bytes;
- survivor order is original stream order and stream offsets are never renumbered.

The same-batch proof removes superseded records from inside one P8 V1 batch while retaining offsets `[1,2,4,5]`. A request at
the compacted hole offset 3 returns offset 4. Cross-segment qualification repeats winner selection over None/LZ4/Zstd source
segments and produces deterministic sparse retained history.

## 4. P7 producer-state safety boundary

P9-E deliberately refuses to compact any V1 source segment carrying P7 producer identity. Removing that history before an
authoritative retained producer checkpoint would make idempotent recovery incomplete. P9-F owns `ProducerStateCheckpoint` and
is the checkpoint that can later make such source history reclaimable.

This is a conservative temporary boundary, not a hidden producer checkpoint implementation. Existing `producer.snapshot`
remains a derived accelerator only.

## 5. Maintenance anchors and future controls

When the source containing the latest committed `AdvanceLogStartOffset` control is rewritten, P9-E emits that exact authority as
the frozen V2 retained control kind 1. Reconstruction after compaction therefore preserves the logical floor.

The already-frozen V2 enum values for `ProducerStateCheckpoint` and `AdvanceCompactionSafeOffset` remain decoder vocabulary only.
P9-E explicitly rejects those future controls if encountered; it does not construct, apply or advance either authority.

## 6. COW replacement and durability

Every selected source is rewritten independently to immutable Storage V2. Replacement identity preserves TopicId, PartitionId,
base stream offset, base Raft index and original creation time while choosing a collision-free higher segment generation.

P9-E reuses the frozen P1 `GenerationTransaction` durability protocol:

1. write only a generation-qualified staged file;
2. write the complete replacement bytes;
3. fsync replacement bytes;
4. rename to the final immutable segment filename;
5. sync the directory;
6. write/fsync the next manifest temporary file;
7. atomically switch `segments.manifest`;
8. sync the directory;
9. only then make old files eligible for reclamation.

Derived V2 offset/time/Raft indexes are rebuilt from the newly authoritative replacement after publication.

## 7. Stale-plan fencing at publication

Immediately before manifest switch, P9-E revalidates:

- manifest generation and exact source/active identities;
- controller policy epoch;
- persisted snapshot floor;
- committed Raft index;
- committed log-start maintenance epoch/floor.

The broker also forces one controller metadata refresh after the potentially expensive planning pass. A shared metadata-epoch
guard is updated as soon as newer committed metadata is observed. Storage takes the read side only around final validation and
manifest commit; therefore a broker-observed metadata change after staging either becomes visible first and aborts publication,
or waits until the already-authorized atomic switch completes. Expensive source decode/staging does not hold this guard.

Focused tests exercise stale manifest, policy, snapshot, maintenance and post-staging metadata-epoch cases.

## 8. Reader generation ownership and reclamation

`PartitionReadGeneration` captures one complete manifest path set, committed boundary and logical floor behind Arc ownership.
When compaction publishes a new generation, paths unreachable from the new manifest move to a retired-generation record instead
of being unlinked immediately.

A reader holding the old generation continues to read the complete old record set while new Fetches use the complete new sparse
generation. Reclamation checks Arc ownership and deletes old segment families only after no external reader can reference them.

This same lifetime mechanism now protects P9-D manifest reclamation and manifest COW suffix replacement as well, preserving the
single-generation reader rule across all live manifest swaps.

## 9. Crash boundaries

Pre-switch qualification stages and fsyncs replacement files, then drops the transaction before manifest publication. Reopen
uses only the old generation and original record set. Existing P1 staged-file/prepared-manifest tests independently cover the
individual pre-switch publication boundaries.

Post-switch qualification publishes the new manifest while deliberately retaining old files behind an active old-generation
reader. A fresh reopen selects only the new sparse manifest, proving unreachable old files cannot become authoritative again.
Existing P1 generation tests cover durable post-switch manifest recovery.

## 10. Runtime and policy qualification

RF1 replication qualification proves:

- no source is selected before a P3 snapshot floor is persisted;
- stale policy epoch rejects execution;
- one eligible closed source compacts after snapshot persistence;
- committed high watermark is unchanged;
- sparse Fetch from a removed offset continues at the first survivor.

Dynamic broker/controller qualification proves committed feature-level-5 gating and cleanup-policy routing: a CompactDelete topic
reaches bounded cleaner validation, while an altered committed Delete policy disables keyed compaction before cleaner limits are
evaluated.

## 11. P9-F/G/H exclusion

P9-E adds no producer-state checkpoint command/application, no compaction-safe replicated watermark, no `clean_compacted_offset`,
no tombstone-grace removal, no retained-history RebootstrapPackage V2 and no cleaner background task. Source segments containing
P7 producer identity remain ineligible until P9-F independently qualifies.

## 12. Candidate qualification

On the final implementation candidate before the complete serialized workspace run:

- focused workspace P9-E: **8/8 PASS**;
- combined workspace P9: **45/45 PASS**;
- Storage: **78/78 PASS**;
- replication library: **105/105 PASS**;
- broker units: **38/38 PASS**;
- dynamic controller/broker integration: **7/7 PASS**;
- `cargo fmt --all -- --check`: PASS;
- `cargo clippy --workspace --all-targets --all-features -- -D warnings`: PASS;
- `cargo test --workspace --doc`: PASS;
- `git diff --check`: PASS.

The complete serialized workspace gate for this documented candidate is **684/684 PASS**.

## 13. Closure boundary

P9-E remains **pre-commit only** until the bounded closure commit is created and that exact clean SHA repeats
focused/mechanical/full-workspace qualification with a
`P9-E exact-HEAD qualification: PASS` Git note.

Only then may **P9-F — P9-112..129: producer-state checkpoint and compaction-safe P7 recovery** begin. P9-G and later remain
disabled until their preceding checkpoint qualifies. P10-P12 remain disabled.
