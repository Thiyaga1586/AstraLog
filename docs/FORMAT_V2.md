# AstraLog Retained-History Storage Format V2

Status: **P9-A FROZEN SPECIFICATION — IMPLEMENTATION DISABLED UNTIL P9-B/C GATES**

## 1. Purpose and compatibility boundary

Storage V2 exists for one reason: physical keyed compaction can remove records from the middle of a
previously batched V1 data frame while surviving records must keep their original stream offsets.
Storage V1 requires `offset_delta == record ordinal`, so a V1 batch cannot represent sparse survivors
without renumbering offsets. Renumbering is forbidden by invariant I-13.

V2 therefore defines an **immutable sparse retained-history segment**. It is not a replacement for the
ordinary append WAL:

- active append segments remain byte-exact Storage V1;
- the live Raft suffix remains byte-exact Storage V1;
- V2 may be written only by the P9 copy-on-write cleaner;
- every V2 frame must be at or below a committed P3 snapshot floor;
- V2 frames are retained stream history and are never reconstructed into the live P3 Raft suffix;
- historical V1 files/goldens remain immutable and valid forever;
- a manifest-governed partition may contain immutable V1 and V2 retained-history files plus exactly
  one mutable V1 active segment.

The P9 feature gate is therefore a reader-before-writer gate. A node must understand both V1 and V2
before cluster feature level 5 may activate P9 writes.

## 2. Segment file identity

V2 uses the existing `.log` filename shape:

```text
{base_stream_offset:020}-{base_raft_index:020}-g{generation:08}.log
```

The filename remains a collision-free physical identity. `generation` is the per-file copy-on-write
generation and is distinct from the u64 `segments.manifest` inventory generation.

## 3. Segment header V2

The V2 segment header remains exactly 72 bytes so mixed V1/V2 inventory can be identified without
probing frame bodies.

| Byte range | Size | Type | Field | V2 rule |
|---|---:|---|---|---|
| 0..8 | 8 | bytes | magic | ASCII `ASTRALOG` |
| 8..10 | 2 | u16 | format_version | `2` |
| 10..12 | 2 | u16 | header_len | `72` |
| 12..16 | 4 | u32 | flags | bit 0 `RETAINED_HISTORY`; all other bits zero |
| 16..32 | 16 | bytes | topic_id | canonical TopicId bytes |
| 32..36 | 4 | u32 | partition_id | partition number |
| 36..40 | 4 | u32 | generation | filename generation |
| 40..48 | 8 | u64 | base_stream_offset | source range lower stream boundary |
| 48..56 | 8 | u64 | base_raft_index | source range lower Raft boundary; >=1 |
| 56..64 | 8 | i64 | created_at_ms | cleaner publication time |
| 64..68 | 4 | u32 | reserved_0 | `0` |
| 68..72 | 4 | u32 | header_crc32c | CRC32C of bytes 0..68 |

`RETAINED_HISTORY` is mandatory in V2. A V2 segment is always immutable and may never be the manifest's
`MUTABLE_ACTIVE` entry.

The base stream/Raft fields describe the original source range lower boundary. Sparse cleaning may
remove the exact original first frame/record, so the first surviving frame is allowed to begin after
either base value.

## 4. Frame envelope V2

Every V2 frame retains the 96-byte fixed header plus payload plus CRC shape.

| Byte range | Size | Type | Field | V2 rule |
|---|---:|---|---|---|
| 0..4 | 4 | u32 | total_frame_len | 96 + stored payload + 4 |
| 4..8 | 4 | bytes | frame_magic | ASCII `ALF2` |
| 8..10 | 2 | u16 | format_version | `2` |
| 10..11 | 1 | u8 | frame_type | `2=retained control`, `3=sparse retained data` |
| 11..12 | 1 | u8 | compression | `0=None`, `1=LZ4`, `2=Zstd` |
| 12..16 | 4 | u32 | attributes | must be `0` |
| 16..24 | 8 | u64 | raft_term | original committed source term; >=1 |
| 24..32 | 8 | u64 | raft_index | original committed source index; >=1 |
| 32..40 | 8 | u64 | base_stream_offset | first surviving record offset; `u64::MAX` for control |
| 40..44 | 4 | u32 | record_count | >0 for sparse data, 0 for control |
| 44..48 | 4 | u32 | reserved_0 | `0` |
| 48..56 | 8 | i64 | min_timestamp_ms | surviving-record minimum; 0 for control |
| 56..64 | 8 | i64 | max_timestamp_ms | surviving-record maximum; 0 for control |
| 64..72 | 8 | u64 | producer_id | `0` |
| 72..76 | 4 | u32 | producer_epoch | `0` |
| 76..80 | 4 | u32 | base_sequence | `0` |
| 80..84 | 4 | u32 | stored_payload_len | physical payload bytes |
| 84..88 | 4 | u32 | uncompressed_payload_len | canonical logical payload bytes |
| 88..90 | 2 | u16 | payload_schema | `2=control`, `3=sparse-record-set` |
| 90..92 | 2 | u16 | header_len | `96` |
| 92..96 | 4 | u32 | reserved_1 | `0` |
| 96..N-4 | variable | bytes | stored payload | bounded payload |
| N-4..N | 4 | u32 | frame_crc32c | CRC32C bytes 0..N-4 |

V2 deliberately carries no P7 producer header identity. P9 producer-state checkpoints preserve the
authoritative accepted epoch, next sequence, recent retry offsets/Raft indexes and canonical digests
before any source idempotent frame needed by P7 is reclaimed.

Compression follows the already-frozen P8 raw LZ4 and Zstd profiles. Sparse data may use None/LZ4/Zstd;
control remains `compression=0`. CRC validation and declared-size bounds happen before decompression.

## 5. Sparse retained data payload schema 3

A sparse frame represents surviving records from exactly one original committed V1/V2 data frame.
Its frame-level `raft_term` and `raft_index` are the original source values. All surviving records from
that source frame are emitted in one V2 sparse frame, so a source Raft index is never duplicated.

For record number `i`:

```text
u32 record_len
u64 offset_delta
i64 timestamp_delta_ms
i32 key_len
i32 value_len
u16 header_count
u16 record_attributes
key bytes
value bytes
headers
```

Rules:

- `offset_delta` is relative to the frame `base_stream_offset`;
- the first record has `offset_delta=0`;
- later deltas are strictly increasing but need not be contiguous;
- `base_stream_offset + offset_delta` must not overflow u64;
- `record_attributes=0` in V2;
- nullable key/value/header values keep the V1 `-1` convention;
- record timestamps and user bytes are preserved exactly;
- the frame min/max timestamp fields are recomputed from surviving records;
- records are returned to Fetch at their reconstructed original stream offsets.

The remaining record/header encoding and length limits are identical to V1 except for the widened,
sparse `offset_delta` field.

## 6. Retained control payload schema 2

V2 control payloads retain the V1 control body shape:

```text
u16 control_kind
u16 control_version
u32 body_len
body bytes
```

Only P9 control kinds explicitly defined by `P9_RETENTION_COMPACTION_V1.md` may be retained in V2.
Their frame term/index remain the original committed source term/index. Unknown control kinds fail
closed; the cleaner never guesses whether unknown control state is discardable.

## 7. Ordering and recovery model

Within one V2 segment:

- frame Raft indexes are strictly increasing but may contain gaps;
- surviving stream offsets are strictly increasing across sparse data frames but may contain gaps;
- every frame is at or below the committed P3 snapshot floor captured by the cleaning plan;
- duplicate Raft indexes or duplicate stream offsets are corruption.

Across a manifest-selected retained-history set, the same strict monotonic rules apply. The one mutable
V1 active segment begins the ordinary contiguous live append domain.

P3 recovery must **not** call `frame_to_log_entry` for V2 retained-history frames. Raft reconstructs
from `SnapshotMetadata` plus the contiguous V1 suffix strictly above the snapshot floor. Stream Fetch,
ListOffsets, producer-state recovery and later cleaning may scan retained V1/V2 history separately.

## 8. Manifest authority

P9 activates the already-frozen `segments.manifest` V1 inventory primitive.

- if a valid manifest exists, only referenced files are authoritative;
- missing/corrupt manifest bytes or missing/size-mismatched referenced files fail closed;
- recovery never falls back to directory scanning after manifest activation;
- unreferenced V1/V2/temp files are garbage and cannot become visible after restart;
- exactly one manifest entry may be mutable, and that entry must be a V1 segment;
- V2 entries are immutable and therefore carry exact expected sizes;
- every roll, retention generation change and compaction generation change serializes through one
  partition segment-set publication boundary.

Legacy no-manifest partitions remain readable with the exact P1 generation-0 discovery rule. P9 may
atomically adopt that existing set into manifest generation 1 before the first physical cleanup; the
adoption itself does not rewrite historical V1 segment bytes.

## 9. Copy-on-write publication

The P1 `GenerationTransaction` durability order remains authoritative:

1. build replacement files under unique temp names;
2. write complete V1/V2 bytes;
3. fsync every replacement file;
4. rename to final generation-qualified names;
5. sync the partition directory;
6. write and fsync `segments.manifest.tmp` for exactly base generation + 1;
7. atomically replace `segments.manifest`;
8. sync the partition directory;
9. only then reclaim files unreachable from the new manifest.

Cleaner interruption at any point before manifest replacement leaves the old generation authoritative.
After manifest replacement the new generation is authoritative even if garbage reclamation is delayed.

## 10. V2 writer eligibility

A cleaner may write a V2 replacement only when all source frames it rewrites are:

- durably committed;
- at or below the local persisted P3 snapshot floor;
- outside the mutable active segment;
- selected by the current authoritative manifest/legacy generation;
- covered by the P9 policy and maintenance-state plan used for that exact generation transaction.

The cleaner must abort and re-plan if the manifest generation, active segment identity, snapshot floor,
topic policy epoch or relevant P9 maintenance state changes before publication.

## 11. Golden compatibility obligations

P9-C must commit immutable golden vectors for at least:

1. a V2 retained-history segment header;
2. one sparse uncompressed frame with non-contiguous offsets;
3. one sparse compressed frame for each enabled P8 codec;
4. one retained P9 control frame;
5. a mixed manifest selecting immutable V1 + immutable V2 + mutable V1 files.

Every historical V1 Storage golden remains byte-identical. A V1 reader is allowed to reject V2; a
P9-capable reader must decode both explicitly and must never reinterpret V1 bytes as V2.
