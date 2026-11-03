# P9-D Replicated Log Start and Delete-Retention Qualification Report

Status: **P9-D PRE-COMMIT QUALIFIED — CLOSURE COMMIT / EXACT-HEAD AUTHORITY PENDING**

Authoritative parent: `dc39dd7cf8ca278ec0d1a4a4b04fa965ae0636ee` (P9-C exact-HEAD PASS)

Scope: **P9-067..P9-087 only**. P9-E keyed compaction and every later P9 checkpoint remain disabled.

## 1. Replicated logical-retention authority

P9-D adds one versioned application-maintenance command:

`AdvanceLogStartOffset { new_log_start_offset }`

Its canonical application payload is V3/kind 1 and contains only the u64 stream floor. Storage materializes it as a V1
control frame with P9 control kind `0x0901`, control version 1 and an exact eight-byte little-endian body. It consumes a
Raft index but **no stream offset**. V1/V2 record command bytes and P4 control kinds are unchanged.

`PartitionLog::committed_log_start_offset()` reconstructs authority strictly from committed P9 maintenance controls in the
manifest-selected history. Historical partitions with no P9 command derive zero. A committed floor may not regress or exceed
the committed stream high watermark; unknown/malformed versions fail closed.

## 2. Leader and policy fencing

Retention is an explicit bounded maintenance hook in P9-D; no background cleaner/scheduler is activated. One attempt requires:

- committed feature level 5;
- ACTIVE topic lifecycle and a Delete/CompactDelete policy;
- exact current partition leader epoch;
- current controller topic/policy state;
- a persisted snapshot floor covering any source selected for physical deletion.

The broker captures the controller metadata epoch/topic/policy, performs the bounded disk plan, then re-reads authority before
the replicated floor proposal. Any metadata/topic/policy drift aborts with stale authority. The partition actor independently
fences the exact leader epoch and manifest generation.

## 3. Manifest-authoritative retention planning

Planning first adopts a legacy generation-0 inventory when necessary, then plans only from the authoritative manifest. Each
segment view records base stream offset, durable creation time, maximum Raft index and exact `.log` bytes. A source can be
removed only when it is immutable/closed, committed and at/below the persisted snapshot floor.

Candidates are **only successor segment base offsets**, so logical advancement never invents a boundary inside a physical
source. The active segment is never a delete source.

## 4. Conservative time retention

For source segment `i`, successor `i+1` supplies the frozen close lower-bound. Expiry requires checked:

`now_ms - successor.created_at_ms >= retention_ms`

Negative age retains data. If the successor timestamp is earlier than the source's own creation time, the wall clock moved
backwards across the roll; P9-D stops the contiguous time-retention prefix instead of treating that smaller timestamp as an
older close bound. Thus backward wall-clock movement can only **delay**, never accelerate, deletion.

## 5. Size retention and protected floor

Size planning sums exact manifest-authoritative `.log` lengths with checked u64 arithmetic and removes only oldest eligible
closed prefixes until the target is met or safety prevents further progress. Active bytes are always retained, so a single
active/protected range may legitimately exceed the configured target.

`protected_log_start_ceiling`, when present, caps the candidate at the greatest eligible closed boundary at/below the ceiling.
If controller policy requires a protected-floor provider but no value is available, the attempt returns without deletion.

## 6. Commit-before-delete publication

Physical cleanup never creates semantics. The leader first commits the P9 maintenance command through ordinary P3 quorum
ordering. Only after the committed floor is observable may Storage publish the next manifest generation that omits wholly
obsolete immutable source files.

Publication is generation-fenced. The new manifest is durably authoritative before old file families are unlinked. The segment
carrying the latest still-required floor control remains pinned until a later retained authority supersedes it. A file that
straddles the logical floor remains byte-identical; expired prefix records are hidden logically and await a later safe rewrite.

## 7. Public Fetch and ListOffsets behavior

No wire-body version changes are introduced.

- `FetchPartitionResponseV0.log_start_offset` now reports the committed floor.
- Fetch below that floor returns existing `OFFSET_OUT_OF_RANGE`, the authoritative floor/high watermark and no stale records.
- ListOffsets selector `-2` returns the committed floor.
- selector `-1` retains committed high-watermark semantics.
- timestamp ListOffsets ignores offsets below the floor and physically absent manifest history.

Both local and replicated broker paths delegate to the same Storage visibility rules.

## 8. Failure and restart qualification

RF1 proves a quorum-committed floor consumes no stream offset, survives restart, rejects stale leader epoch and cannot outrun
committed visibility.

RF3 qualification creates a level-5 RF3 Delete topic with both time and size retention, produces rolled history, stops one
voter, advances the floor on the leader, verifies public Fetch/ListOffsets behavior, rejoins the lagging voter, then kills the
leader. The successor preserves the same floor, rejects the old leader epoch, and the rejoined voter can later serve the same
earliest offset after leadership transfer.

Storage tests independently cover time-only, size-only, protected-floor, snapshot-floor, active-byte, straddling-prefix,
commit-before-delete and floor-authority pinning behavior.

## 9. P9-E exclusion

P9-D does **not** implement keyed winner selection, COW compaction plans, sparse rewrite publication, tombstone removal,
`clean_compacted_offset`, replicated compaction-safe offset, producer-state checkpoint cleanup, retained-history rebootstrap V2,
or cleaner queues/scheduling. Those remain owned by P9-E and later checkpoints.

## 10. Pre-commit qualification

Focused/component evidence on the final source candidate:

- P9-D focused: **10/10 PASS**;
- combined workspace P9: **38/38 PASS**;
- Storage: **72/72 PASS**;
- replication library: **104/104 PASS**;
- broker units: **38/38 PASS**;
- dynamic controller/broker integration: **7/7 PASS**;
- `cargo fmt --all -- --check`: PASS;
- `cargo clippy --workspace --all-targets --all-features -- -D warnings`: PASS;
- `cargo test --workspace --doc`: PASS;
- `git diff --check`: PASS.

Complete serialized workspace:

`cargo test --workspace --all-targets -- --test-threads=1` = **677/677 PASS**.

Component totals:

- benchmark tooling **20/20**;
- broker units **38/38**;
- P2 network **9/9**;
- P4 RF3 **1/1**;
- P5/P8/P9 dynamic **7/7**;
- P6 **9/9** serialized;
- P7 failover/runtime **12/12 + 2/2** serialized;
- client **24/24**;
- common **3/3**;
- controller **122/122**;
- groups **55/55**;
- protocol **48/48**;
- P2/P6/P7/P9 protocol goldens **11/11 + 6/6 + 4/4 + 2/2**;
- Raft **128/128**;
- replication **104/104**;
- Storage **72/72**.

Qualification-host note: one earlier broad filtered build was aborted by Windows paging-file exhaustion (`os error 1455`)
while Cargo compiled many test binaries in parallel. No source change was made for that condition. Qualification was rerun with
`CARGO_BUILD_JOBS=1` and process-local `TEMP/TMP` on `D:/Workspace/AstraLog/target/qualification-temp`; all bounded component
gates and the complete serialized workspace then passed.

## 11. Closure boundary

The above evidence permits one bounded P9-D closure commit. P9-D is **not authoritative** until that exact clean SHA repeats
the required focused/mechanical/full-workspace gates and receives a `P9-D exact-HEAD qualification: PASS` Git note.

Only after that note exists may **P9-E — P9-088..P9-111: copy-on-write keyed compaction and sparse retained-history reads**
begin. P9-F and later remain disabled until their preceding checkpoint qualifies. P10-P12 remain disabled.
