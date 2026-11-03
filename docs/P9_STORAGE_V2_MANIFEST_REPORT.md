# AstraLog P9-C Storage V2 and Manifest Inventory Qualification

Date: 2026-10-09

Status: **P9-C PRE-COMMIT QUALIFIED — CLOSURE COMMIT / EXACT-HEAD AUTHORITY PENDING**

## 1. Boundary

P9-C implements checklist actions **P9-044 through P9-066** on top of authoritative P9-B:

`f583b6b8e8a4196c22a6b36ae946c8d5501065f1 — P9-B close controller retention metadata and codecs`

The P9-B exact-HEAD PASS note authorizes only P9-C. This checkpoint implements the frozen retained-history
Storage V2 codec and makes the P1 segment manifest authoritative for live PartitionLog inventory after
explicit adoption.

P9-C does **not** implement logical retention planning, `AdvanceLogStartOffset` application, time/size
expiry, physical delete-retention generations, keyed cleaner execution, producer-state checkpoint
application/recovery, compaction-safe/tombstone watermarks or retained-history rebootstrap V2. Those are
P9-D and later.

## 2. V1 immutability and version dispatch

P9-C does not edit the frozen V1 codec or compression profile. `format_v2.rs` is a parallel decoder/encoder
surface selected only after reading the segment magic/version prefix.

The frozen-source audit from authoritative P9-B is empty for:

- `docs/FORMAT_V1.md`;
- `crates/astralog-storage/src/format.rs`;
- `crates/astralog-storage/src/compression.rs`;
- every historical V1/P8 Storage golden fixture.

Therefore historical V1 bytes keep their exact previous interpretation.

## 3. Retained-history Segment V2

`RetainedSegmentHeaderV2` implements the frozen 72-byte layout:

- `ASTRALOG` magic;
- format version 2;
- header length 72;
- flags exactly `RETAINED_HISTORY`;
- unchanged topic/partition/generation/base-offset/base-Raft/creation identity;
- reserved zero;
- CRC32C over bytes 0..68.

Headers without the retained-history bit, with any unknown flag/reserved field, wrong length/version or
bad CRC fail closed.

V2 files remain immutable. The one live mutable segment in a partition inventory must remain V1.

## 4. Sparse data schema 3

V2 sparse data retains the original committed source frame Raft term/index while allowing physical
record survivors to have stream-offset holes. Each logical record encodes a **u64** delta from the first
survivor offset rather than the V1 record ordinal.

Canonical rules enforced by encode and decode:

- nonempty batch;
- first delta exactly zero;
- every later delta strictly greater than the prior delta;
- checked `base + delta` arithmetic;
- no duplicate survivor offsets;
- record/header attributes zero;
- frozen nullable key/value/header semantics;
- bounded record/header/key/payload allocation;
- record timestamps inside the declared frame timestamp range;
- no trailing bytes.

The malformed campaign checks every truncated-frame boundary plus CRC corruption, invalid schema,
unknown compression, noncanonical first delta, producer-field mutation and stream-offset overflow.

## 5. Compression

P9-C reuses the already-qualified P8 compression helpers without modification:

- None: stored payload exactly equals the sparse logical payload;
- LZ4: frozen raw-block profile with exact bounded output length;
- Zstd: frozen deterministic level-3 profile with exact bounded output length.

No V2-specific compression reinterpretation or dependency is introduced.

## 6. Retained P9 control frames

V2 retained control uses schema 2 and compression None. The current codec admits only the three P9 V1
maintenance kind identities frozen by the P9 contract:

1. AdvanceLogStartOffset;
2. ProducerStateCheckpoint;
3. AdvanceCompactionSafeOffset.

Only control version 1 is accepted. This checkpoint stores/validates the retained byte envelope only;
it does not activate the P9-D/P9-F/P9-G state machines represented by those future committed commands.

## 7. Producer authority boundary

V2 frame producer id, producer epoch, base sequence and frame attributes are encoded as zero and must
decode as zero. Sparse records do not carry producer authority.

This is intentional: physical retained-history bytes never become a second source of P7 producer
authority. P9-F remains responsible for committed producer-state checkpoint semantics.

## 8. Golden bytes

New P9-C fixtures under `crates/astralog-storage/tests/golden/` are:

- `segment_header_v2.hex`;
- `sparse_none_v2.hex` — survivors at offsets 11 and 14;
- `sparse_lz4_v2.hex`;
- `sparse_zstd_v2.hex`;
- `retained_control_v2.hex`;
- `manifest_p9_mixed_v1.hex`.

The tests regenerate each logical object and require byte equality. Existing V1 goldens are unchanged.

## 9. Manifest authority activation

The existing P1 `segments.manifest` bytes and `GenerationTransaction` publication protocol are reused.
P9-C adds a stricter live-partition validator without changing generic P1 manifest encoding.

When a manifest exists, `PartitionLog::open_or_create` uses only its referenced files. It validates:

- every referenced file exists;
- exact immutable size where declared;
- strict ordered segment-file identity;
- filename/header identity equality;
- topic/partition identity;
- known V1/V2 format;
- exactly one mutable active entry;
- mutable active entry is last;
- mutable active entry is V1;
- every V2 entry is immutable.

A missing/corrupt referenced file fails reopen. The implementation never falls back to a stray directory
`.log` file once a manifest exists.

## 10. Legacy adoption

Absence of `segments.manifest` preserves the exact historical generation-0 directory-discovery path.
Restarting a historical partition does not silently create a manifest.

`PartitionLog::adopt_manifest_inventory` is explicit. It:

1. accepts only the current legacy V1 inventory;
2. flushes the mutable V1 tail;
3. publishes generation 1 through `GenerationTransaction`;
4. marks prior closed segments immutable with exact file sizes;
5. marks exactly the last segment mutable;
6. validates the resulting live inventory;
7. does not rewrite any V1 segment byte.

The qualification test byte-compares the original V1 segment before and after adoption.

## 11. Manifest-governed roll and suffix repair

After adoption, a segment roll publishes the next manifest generation. The new V1 file is durable before
the manifest switch; the old active becomes exact-size immutable; stale generation transactions fail
rather than replacing newer authority.

If Raft must truncate an uncommitted suffix inside the current mutable V1 segment, the existing in-place
V1 tail truncation remains valid. If the truncation target is an immutable manifest-selected V1 segment,
P9-C performs copy-on-write:

- preserve bytes through the target position;
- publish a replacement V1 file under a higher per-file generation;
- publish a new manifest generation containing the safe prefix plus replacement mutable tail;
- only then reclaim old unreachable segment families.

Crash before the manifest switch leaves the old inventory authoritative; crash after the switch leaves
the new inventory authoritative while old files are harmless garbage until cleanup.

## 12. Mixed V1/V2 recovery and Fetch

Manifest recovery scans immutable V1/V2 history and the mutable V1 suffix independently. Immutable V2
must be fully valid and cannot extend above the committed boundary. Across retained history, Raft indexes
and logical survivor offsets are strictly increasing.

Fetch paths understand V2 sparse records and return their **exact stored logical offsets**, preserving
holes. The mixed integration constructs V2 survivors at offsets 0 and 2 with a live V1 suffix beginning
at offset 3; restart returns `0, 2`, then after a live append returns `0, 2, 3`.

The committed stream high watermark is not inferred solely from the highest survivor. Manifest segment
boundaries preserve the original live stream frontier even when compaction removes trailing source
records; a dedicated test keeps only offset 0 from an original 0/1/2 batch and still reports committed
high watermark 3.

## 13. P3 separation

`PartitionLog::read_frames` and `read_frames_from_raft` intentionally skip V2 files. This prevents a
retained physical representation from being reconstructed as the live P3 Raft suffix.

The stronger replication-adapter test persists snapshot floor 1, replaces the below-snapshot V1 history
with manifest-selected V2, reopens `PartitionPersistence`, and proves:

- Fetch/history still sees the retained stream record;
- `RecoveredState.snapshot` remains the committed snapshot;
- `RecoveredState.log` is empty because there is no post-snapshot V1 suffix.

Therefore P3 receives no V2 retained frame as a live log entry.

## 14. Derived indexes

Indexes remain non-authoritative/rebuildable. V1 uses the unchanged index rebuild path. V2 rebuilds:

- offset index from exact first survivor offsets;
- time index from monotonic maximum logical timestamps;
- Raft index from retained source Raft indexes.

The index file identity stays bound to the segment's existing base/generation identity. Mixed restart
recreates V2 index files without changing manifest authority.

## 15. Current qualification evidence

Final pre-commit candidate evidence:

- focused Storage P9-C: **13/13 PASS**;
- replication-adapter P9-C boundary: **1/1 PASS**;
- workspace `p9c` filter: **14/14 PASS**;
- complete Storage package: **66/66 PASS**;
- complete replication library: **102/102 PASS**;
- combined workspace P9 filter: **28/28 PASS**;
- workspace fmt: PASS;
- workspace Clippy all targets/features with `-D warnings`: PASS;
- workspace doc tests: PASS;
- `git diff --check`: PASS;
- historical V1 format/compression/golden audit: no changes.

Complete serialized workspace:

`cargo test --workspace --all-targets -- --test-threads=1` = **667/667 PASS**.

Component evidence:

- benchmark tooling: **20/20**;
- broker units: **37/37**;
- P2 network: **9/9**;
- P4 RF3: **1/1**;
- P5/P8/P9 dynamic integration: **6/6**;
- P6 routing/group/offset/fencing/churn/public integration: **9/9** serialized;
- P7 real RF3 failover: **12/12** serialized;
- P7 runtime: **2/2**;
- client: **24/24**;
- common: **3/3**;
- controller: **122/122**;
- groups: **55/55**;
- protocol: **48/48**;
- original P2/P6/P7 protocol goldens: **11/11 + 6/6 + 4/4**;
- P9 public-body goldens: **2/2**;
- Raft: **128/128**;
- replication: **102/102**;
- Storage: **66/66**.

The original P1 manifest crash matrix remains green inside Storage 66/66: staged temp files, files renamed
before manifest publication, stale generation transactions, invalid retained files, manifest CRC and file-size
mismatch all preserve/fail the previously frozen publication boundary. P9-C adds live adoption/roll/COW
publication and fail-closed manifest inventory tests on top of those P1 proofs.

## 16. P9-D exclusion

P9-C does not add/apply a replicated `AdvanceLogStartOffset` command, a logical log-start state variable,
retention planner, clock/size expiry logic, protected-floor ceiling, physical delete-retention generation,
Fetch/ListOffsets log-start behavior or delete-retention metrics.

The retained-control **kind identity** for AdvanceLogStartOffset exists only because P9-C must encode and
validate frozen V2 retained control envelopes; no P9-D execution path is activated.

## 17. Remaining closure gate

P9-044..066 are pre-commit qualified. This evidence permits one bounded P9-C closure commit. P9-C is not
authoritative until that exact clean SHA repeats the required focused/mechanical/complete-workspace gates and
receives `P9-C exact-HEAD qualification: PASS` as a Git note.

Only then may P9-D begin.
