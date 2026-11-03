# AstraLog Storage Format V1

Status: **FROZEN — P1 qualified**

Format version: **1**

This document defines the authoritative on-disk bytes for AstraLog P1. The writer, reader, recovery code, indexes, and future Raft integration must conform to this document.

The purpose of V1 is not to optimize every byte. The purpose is to make crash recovery, corruption detection, future replication, idempotent producer metadata, compaction, and compatibility mechanically unambiguous.

---

## 1. Global encoding rules

1. All integer fields are **little-endian**.
2. All offsets and indexes are unsigned unless explicitly stated otherwise.
3. All reserved fields **MUST be written as zero** and **MUST be ignored when reading V1**, unless a future feature level gives them meaning.
4. Stream offsets start at **0**.
5. Raft indexes start at **1**. Raft index **0** means “no committed entry yet” only in partition metadata and is never a valid stored frame index.
6. Raft terms are `u64`.
7. Timestamps are signed Unix epoch milliseconds (`i64`).
8. CRCs use **CRC32C / Castagnoli**.
9. Readers must validate lengths **before allocating**.
10. A V1 reader must reject a file/frame with a different magic, unsupported version, impossible length, or checksum failure.
11. Closed/rolled segment files are immutable.
12. Index files are derived state. The `.log` segment plus partition metadata are authoritative.

### 1.1 Sentinel values

| Field | Sentinel | Meaning |
|---|---:|---|
| `base_stream_offset` in a control frame | `u64::MAX` | control entries do not consume stream offsets |
| nullable key/value length | `-1_i32` | null |
| nullable header value length | `-1_i32` | null |
| producer metadata when idempotence flag is clear | all zero | ignored |
| partition `committed_raft_index` | `0` | nothing committed yet |

No other sentinel values are implied.

---

## 2. File naming

Each partition directory contains:

```text
<base-stream-20>-<base-raft-20>-g<generation-8>.log
<base-stream-20>-<base-raft-20>-g<generation-8>.index
<base-stream-20>-<base-raft-20>-g<generation-8>.timeindex
<base-stream-20>-<base-raft-20>-g<generation-8>.raftindex
```

Example:

```text
00000000000000000000-00000000000000000001-g00000000.log
00000000000000000000-00000000000000000001-g00000000.index
00000000000000000000-00000000000000000001-g00000000.timeindex
00000000000000000000-00000000000000000001-g00000000.raftindex
```

The first numeric field is the segment's `base_stream_offset`; the second is its `base_raft_index`.

This two-dimensional identity is required because control frames advance the Raft index but do not advance the consumer-visible stream offset. Therefore two legitimate segment boundaries can have the same next stream offset but can never have the same next Raft index.

The generation suffix makes copy-on-write replacement unambiguous: a replacement file can coexist with the old immutable file until an atomic manifest switch chooses the new generation.

Temporary files used during copy-on-write operations end in `.tmp` and are never treated as live segments during normal reads.

---

## 3. Segment file header

Every `.log` file begins with one fixed **72-byte** segment header.

### 3.1 Layout

| Byte range | Size | Type | Field | V1 rule |
|---|---:|---|---|---|
| 0..8 | 8 | bytes | magic | ASCII `ASTRALOG` |
| 8..10 | 2 | u16 | format_version | `1` |
| 10..12 | 2 | u16 | header_len | `72` |
| 12..16 | 4 | u32 | flags | `0` |
| 16..32 | 16 | bytes | topic_id | UUID bytes in canonical network byte order |
| 32..36 | 4 | u32 | partition_id | partition number |
| 36..40 | 4 | u32 | generation | file generation |
| 40..48 | 8 | u64 | base_stream_offset | must equal numeric filename |
| 48..56 | 8 | u64 | base_raft_index | must equal numeric filename; >=1 |
| 56..64 | 8 | i64 | created_at_ms | segment creation time |
| 64..68 | 4 | u32 | reserved_0 | `0` |
| 68..72 | 4 | u32 | header_crc32c | CRC32C of bytes 0..68 |

### 3.2 Validation

A reader must reject the segment before scanning frames if:

- the magic/version/header length is invalid,
- the topic/partition identity does not match the directory being opened,
- filename base stream/Raft offset or generation differs from the header,
- reserved V1 bits are nonzero where strict validation is required,
- or `header_crc32c` does not match.

Segment-header corruption is never silently repaired.

---

## 4. Partition metadata: `partition.meta`

`partition.meta` is a fixed **64-byte** checksummed record.

P1 uses it primarily to preserve the committed boundary needed to decide whether crash recovery may truncate an invalid active tail. P3 will connect this field to the real Raft commit state without changing V1 bytes.

The P1 partition-log API refuses to advance `committed_raft_index` beyond its tracked locally durable Raft index. Appending bytes alone does not make an entry eligible for the committed boundary; the relevant `sync_data`/`sync_all` operation must have succeeded first.

### 4.1 Layout

| Byte range | Size | Type | Field | V1 rule |
|---|---:|---|---|---|
| 0..8 | 8 | bytes | magic | ASCII `ASTMETA1` |
| 8..10 | 2 | u16 | format_version | `1` |
| 10..12 | 2 | u16 | record_len | `64` |
| 12..16 | 4 | u32 | flags | `0` |
| 16..32 | 16 | bytes | topic_id | same identity as segments |
| 32..36 | 4 | u32 | partition_id | same partition |
| 36..40 | 4 | u32 | reserved_0 | `0` |
| 40..48 | 8 | u64 | committed_raft_index | `0` means none committed |
| 48..56 | 8 | u64 | metadata_epoch | monotonically increases on durable rewrite |
| 56..60 | 4 | u32 | reserved_1 | `0` |
| 60..64 | 4 | u32 | crc32c | CRC32C of bytes 0..60 |

### 4.2 Update protocol

Metadata updates use:

1. write complete new bytes to `partition.meta.tmp`,
2. flush the temporary file,
3. atomically replace/rename it to `partition.meta`,
4. durably sync the parent directory where the platform requires it.

If an update fails before the atomic replace, the prior `partition.meta` remains authoritative.

---

## 5. Log frame envelope

Every entry after the 64-byte segment header is one self-contained frame:

```text
+----------------------+ 0
| fixed header (96 B)  |
+----------------------+ 96
| stored payload       | stored_payload_len bytes
+----------------------+
| CRC32C trailer (4 B) |
+----------------------+
```

Minimum frame size is **100 bytes**.

### 5.1 Fixed header layout

| Byte range | Size | Type | Field | V1 rule |
|---|---:|---|---|---|
| 0..4 | 4 | u32 | total_frame_len | 96 + stored payload + 4 |
| 4..8 | 4 | bytes | frame_magic | ASCII `ALF1` |
| 8..10 | 2 | u16 | format_version | `1` |
| 10..11 | 1 | u8 | frame_type | 1=data batch, 2=control |
| 11..12 | 1 | u8 | compression | 0=none, 1=LZ4, 2=Zstd |
| 12..16 | 4 | u32 | attributes | bit field defined below |
| 16..24 | 8 | u64 | raft_term | term associated with this entry |
| 24..32 | 8 | u64 | raft_index | must be >=1 |
| 32..40 | 8 | u64 | base_stream_offset | first data offset or `u64::MAX` for control |
| 40..44 | 4 | u32 | record_count | >0 for data, 0 for control |
| 44..48 | 4 | u32 | reserved_0 | `0` |
| 48..56 | 8 | i64 | min_timestamp_ms | minimum record timestamp in data batch, 0 for control |
| 56..64 | 8 | i64 | max_timestamp_ms | data timestamp, 0 for control |
| 64..72 | 8 | u64 | producer_id | meaningful only with idempotent flag |
| 72..76 | 4 | u32 | producer_epoch | meaningful only with idempotent flag |
| 76..80 | 4 | u32 | base_sequence | meaningful only with idempotent flag |
| 80..84 | 4 | u32 | stored_payload_len | bytes physically stored after header |
| 84..88 | 4 | u32 | uncompressed_payload_len | canonical payload size before compression |
| 88..90 | 2 | u16 | payload_schema | 1=data-record-set, 2=control-entry |
| 90..92 | 2 | u16 | header_len | `96` |
| 92..96 | 4 | u32 | reserved_1 | `0` |
| 96..N-4 | variable | bytes | stored_payload | compressed or raw payload |
| N-4..N | 4 | u32 | frame_crc32c | CRC32C of frame bytes 0..N-4 |

`N == total_frame_len`.

### 5.2 Attribute bits

| Bit | Name | Meaning |
|---:|---|---|
| 0 | IDEMPOTENT | producer_id/epoch/base_sequence are valid |
| 1..31 | reserved | must be zero in V1 |

Compression is encoded in its dedicated byte and is not duplicated in `attributes`.

### 5.3 P1 compression rule

The V1 byte format reserves:

- `0` = none
- `1` = LZ4
- `2` = Zstandard

but **P1 writers emit only compression=0**. LZ4/Zstd behavior is activated in P8 without changing the V1 frame envelope.

For `compression=0`:

`stored_payload_len == uncompressed_payload_len`.

P8 activation preserves the same V1 envelope and adds these writer/reader rules:

- only data-frame payload schema 1 may use LZ4/Zstd; control frames remain `compression=0`;
- the compression input is exactly the canonical uncompressed data-record-set payload;
- LZ4 uses the frozen P8 raw-block profile and Zstd uses the frozen P8 level-3 single-frame profile;
- a requested compressed codec is stored only when its bytes are strictly smaller than the canonical raw payload, otherwise the writer stores `compression=0`;
- `stored_payload_len` always names the physical payload bytes and `uncompressed_payload_len` always names the canonical logical payload size;
- the reader bounds the declared logical size and validates physical frame CRC32C before allocating/decompressing;
- decode must produce exactly `uncompressed_payload_len` bytes and reject malformed, truncated, trailing or unknown codec data;
- mixed None/LZ4/Zstd frames are valid within one segment and reconstruct identical logical records, offsets and P7 producer metadata.

### 5.4 Global frame validation

A V1 reader rejects a frame if any of the following is true:

- `total_frame_len < 100`,
- `total_frame_len` exceeds configured maximum frame bytes,
- `header_len != 96`,
- magic/version is invalid,
- unknown frame type/compression is not enabled,
- `stored_payload_len != total_frame_len - 100`,
- reserved bits required to be zero are nonzero,
- Raft index is 0,
- or CRC32C mismatches.

Length validation occurs before payload allocation.

---

## 6. Data batch payload schema 1

A data frame contains one canonical record set. It consumes exactly `record_count` consecutive stream offsets beginning at `base_stream_offset`.

For record number `i`:

`stream_offset = base_stream_offset + offset_delta`.

V1 requires offsets in one data batch to be exactly contiguous:

`offset_delta == i` for `i = 0..record_count-1`.

This lets recovery determine the logical end offset without parsing application semantics outside the record set.

### 6.1 Record layout

Each record begins with a `record_len` prefix.

| Field | Size | Type | Rule |
|---|---:|---|---|
| record_len | 4 | u32 | bytes following this field for this record |
| offset_delta | 4 | u32 | must equal record ordinal within the batch |
| timestamp_delta_ms | 8 | i64 | added to frame min_timestamp_ms |
| key_len | 4 | i32 | -1=null, >=0 byte length |
| value_len | 4 | i32 | -1=null/tombstone, >=0 byte length |
| header_count | 2 | u16 | number of user headers |
| record_attributes | 2 | u16 | must be 0 in V1 |
| key | variable | bytes | omitted when key_len=-1 |
| value | variable | bytes | omitted when value_len=-1 |
| headers | variable | bytes | repeated header encoding below |

The minimum encoded record size is 28 bytes including the `record_len` prefix.

### 6.2 User header encoding

Repeated `header_count` times:

| Field | Size | Type | Rule |
|---|---:|---|---|
| name_len | 2 | u16 | number of name bytes |
| value_len | 4 | i32 | -1=null, >=0 byte length |
| name | variable | bytes | uninterpreted UTF-8-compatible application bytes; V1 storage does not normalize |
| value | variable | bytes | omitted when null |

### 6.3 Data-frame validation

For data frames:

- `frame_type == 1`
- `payload_schema == 1`
- `record_count > 0`
- `base_stream_offset != u64::MAX`
- `min_timestamp_ms <= max_timestamp_ms`
- decoded record count equals header `record_count`
- each record's `offset_delta` equals its ordinal
- reconstructed timestamps fit in `i64`
- reconstructed timestamp is between frame min/max timestamp inclusive
- all nullable lengths are -1 or nonnegative
- summed record lengths consume the payload exactly
- no integer addition/multiplication may wrap

An application value with `value_len=-1` is a tombstone/null value. P1 stores it but does not perform compaction.

---

## 7. Control payload schema 2

Control frames allow future Raft/state-machine metadata to share the same ordered durable log without consuming consumer-visible stream offsets.

Payload:

| Field | Size | Type | Rule |
|---|---:|---|---|
| control_kind | 2 | u16 | nonzero |
| control_version | 2 | u16 | control-type-specific version |
| body_len | 4 | u32 | exact body bytes |
| body | variable | bytes | opaque to generic segment scanner |

For control frames:

- `frame_type == 2`
- `payload_schema == 2`
- `base_stream_offset == u64::MAX`
- `record_count == 0`
- timestamps are 0
- idempotent attribute is clear
- producer fields are zero
- `body_len + 8 == uncompressed_payload_len`

P1 supports generic control entries but does not assign Raft protocol meaning to specific `control_kind` values.

---

## 8. Ordering constraints inside one partition

Frames in authoritative log order must satisfy:

1. Raft indexes are exactly contiguous and increase by 1.
2. Data-frame stream offsets are exactly contiguous across data frames.
3. Control frames do not advance the stream offset.
4. A data frame consuming `k` records advances the next stream offset by `k`.
5. Arithmetic overflow is a fatal format error.

These constraints are essential for crash classification when a damaged frame header can no longer be trusted.

---

## 9. Sparse index files

All V1 index files start with a fixed **40-byte** header and contain fixed-width **16-byte** entries.

Indexes are always safe to delete and rebuild.

### 9.1 Index header

| Byte range | Size | Type | Field |
|---|---:|---|---|
| 0..8 | 8 | bytes | magic |
| 8..10 | 2 | u16 | format_version=1 |
| 10..12 | 2 | u16 | header_len=40 |
| 12..14 | 2 | u16 | kind |
| 14..16 | 2 | u16 | entry_len=16 |
| 16..24 | 8 | u64 | segment_base_stream_offset |
| 24..32 | 8 | u64 | segment_base_raft_index |
| 32..36 | 4 | u32 | generation |
| 36..40 | 4 | u32 | header_crc32c over bytes 0..36 |

### 9.2 Kinds and magic

| File | Magic (8 bytes) | kind |
|---|---|---:|
| `.index` | `ASTOIDX1` | 1 |
| `.timeindex` | `ASTTIDX1` | 2 |
| `.raftindex` | `ASTRIDX1` | 3 |

### 9.3 Offset-index entry

```text
u64 stream_offset
u64 file_position
```

`file_position` is an absolute byte position from the beginning of the `.log` file and therefore is always >=72.

### 9.4 Time-index entry

```text
i64 timestamp_ms
u64 stream_offset
```

The indexed timestamp is a monotonic running maximum of batch timestamps at each sparse index point. This preserves binary-searchability even if application event timestamps arrive out of order.

### 9.5 Raft-index entry

```text
u64 raft_index
u64 file_position
```

### 9.6 Sparsity

V1 creates a candidate index entry:

- for the first relevant frame in a segment,
- and whenever at least `index_interval_bytes` have been appended since the previous indexed frame.

The interval is runtime configuration, not part of the disk format.

### 9.7 Validation

On recovery an index is discarded and rebuilt if:

- header magic/version/kind/CRC is wrong,
- file length after the header is not divisible by 16,
- keys are not monotonic,
- a file position is outside the segment,
- or the pointed frame does not contain the indexed offset/Raft index.

Index corruption is not log corruption.

---

## 10. Active versus rolled segment recovery

The final segment by base stream offset is the only segment that may be active/mutable.

Rolled segments must be fully valid. Any malformed frame or CRC failure in a rolled segment is a hard corruption error.

For the active segment, recovery scans sequentially from byte 72 while tracking the next expected Raft index and stream offset.

### 10.1 Incomplete tail

If EOF occurs:

- inside the 96-byte frame header,
- before `total_frame_len` bytes are available,
- or before the final CRC trailer,

the candidate frame is an incomplete tail.

It may be truncated **only if its next expected Raft index is greater than `committed_raft_index`**.

### 10.2 CRC/format failure in active tail

If the next candidate active frame is present but invalid (bad CRC, impossible length, invalid payload structure), it may be truncated from that frame onward **only if its next expected Raft index is greater than `committed_raft_index`**.

### 10.3 Committed corruption

If the invalid/incomplete next expected Raft index is <= `committed_raft_index`, recovery must fail with committed corruption.

No truncation, zero-fill, resynchronization scan, or best-effort skipping is permitted.

This is invariant I-10.

### 10.4 Trailing garbage

There is no attempt to scan for a later `ALF1` magic after corruption. Once frame boundaries stop being trustworthy, the suffix is either:

- a safely truncatable uncommitted active tail, or
- committed corruption.

This avoids “repair by guessing”.

---

## 11. Logical end offsets after recovery

After scanning all valid frames:

- `next_raft_index = last_valid_raft_index + 1`
- `next_stream_offset = last data frame base offset + record_count`

If the partition contains no data records, `next_stream_offset=0`.

If it contains no frames, `next_raft_index=1`.

---

## 12. Size limits

The byte format itself uses `u32` frame/payload lengths, but implementations must impose stricter configured limits.

P1 defaults:

- maximum stored frame bytes: **16 MiB**
- maximum record bytes: **8 MiB**
- maximum key bytes: **1 MiB**
- maximum header count per record: **1024**
- maximum header-name bytes: **64 KiB**

These are implementation defaults and may later become configurable without changing V1.

No decoder may allocate based on a length field before validating it against the active configured bound and enclosing frame boundary.

---

## 13. Copy-on-write generation manifest

P1 defines the crash-safe publication primitive that P9 compaction will reuse. It does **not** define compaction policy and it does **not** activate manifest-governed partition inventory in P1.

In P1, normal `PartitionLog` startup discovers the ordinary generation-0 segment set directly from valid V1 segment filenames and headers. The `GenerationTransaction` / `segments.manifest` mechanism is qualified independently as a dormant replacement primitive.

P9 is responsible for integrating manifest-selected generations into live `PartitionLog` inventory. Until that integration exists, calling `GenerationTransaction` against a live P1 partition directory is unsupported.

### 13.1 Manifest binary format

Header:

| Field | Size | Type |
|---|---:|---|
| magic | 8 | ASCII `ASTMANI1` |
| version | 2 | u16=1 |
| header_len | 2 | u16=32 |
| generation | 8 | u64 |
| entry_count | 4 | u32 |
| body_len | 4 | u32 |
| reserved | 4 | u32=0 |

Header size before body: 32 bytes.

Each body entry:

```text
u16 filename_len
u16 flags
u64 expected_size
filename bytes   # UTF-8 ASCII subset produced by AstraLog
```

Entry flag bit 0 is `MUTABLE_ACTIVE`.

- If clear, `expected_size` is the exact immutable file size and must match during manifest validation.
- If set, the file is the one active mutable segment and `expected_size` is written as 0 and ignored while the file grows.
- At most one entry in a valid V1 manifest may carry `MUTABLE_ACTIVE`.

Once manifest-governed inventory is activated in P9, manifest entries select authoritative `.log` files for that generation. Derived index files are not required to appear in the manifest because they are always rebuildable.

The file ends with:

```text
u32 manifest_crc32c
```

CRC covers every byte before the CRC trailer.

### 13.2 Generation switch protocol

To publish generation `G+1`:

1. create all replacement segment/index files under unique `.tmp` names,
2. write complete bytes,
3. flush every replacement file,
4. rename replacement files to generation-qualified final names,
5. sync the partition directory,
6. write `segments.manifest.tmp` with generation `G+1`,
7. flush the manifest temp file,
8. atomically replace `segments.manifest`,
9. sync the partition directory,
10. only then may files unreachable from the new manifest be reclaimed.

For a manifest-governed generation, recovery chooses the last valid committed manifest. Unreferenced temp/new-generation files are garbage relative to that manifest and are not selected by the generation reader.

P1 validates this publication primitive and its failure boundaries independently. P1's active `PartitionLog` does not consume `segments.manifest`; that integration is deliberately deferred to P9 together with compaction/retention generation semantics.

---

## 14. Golden compatibility vectors

Before P1 closes, the repository must contain immutable golden byte vectors for at least:

1. empty segment header,
2. one non-idempotent data frame with one record,
3. one idempotent data frame with multiple records and headers,
4. one control frame,
5. partition metadata with no commit,
6. partition metadata with nonzero committed Raft index,
7. each of the three index headers and representative entries,
8. one generation manifest.

The canonical fixtures may be stored as raw binary or a lossless hexadecimal representation suitable for code review. Tests must both:

- decode the fixture representation to the exact golden bytes and then decode those bytes into the expected values, and
- encode the expected values back to byte-identical golden bytes.

Any intentional future incompatible byte change requires a new format version; V1 golden bytes never change.

---

## 15. P1 compatibility promise

P1 is closed. Therefore:

- existing V1 segment headers do not change,
- existing V1 frame headers do not change,
- existing V1 record encoding does not change,
- V1 CRC coverage does not change,
- V1 index entry widths do not change,
- and V1 readers continue to read P1 golden files.

Future features must use:

- reserved fields/bits where explicitly safe,
- feature-level-gated payload semantics,
- or a new format version.

“It is easier for the implementation” is not sufficient reason to mutate V1.
