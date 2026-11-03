# P1 Execution Checklist — Single-Node Durable Log Engine

This is the next implementation plan. It deliberately excludes networking and replication.

## P1.0 — Scaffold

- [x] create Rust workspace and pin stable toolchain
- [x] create `astralog-common` and `astralog-storage`
- [x] configure fmt/clippy/test CI commands
- [x] deny unsafe by default
- [x] establish storage test temp-directory helpers

## P1.1 — Freeze storage vocabulary and types

- [x] `TopicId`
- [x] `PartitionId`
- [x] `StreamOffset`
- [x] `RaftIndex`
- [x] `RaftTerm`
- [x] `SegmentBaseOffset`
- [x] `RecordBatch`
- [x] `ControlEntry`
- [x] checked conversions and overflow behavior

## P1.2 — Specify FORMAT_V1 before full writer implementation

Define exact byte order and byte widths for:

- [x] segment/file magic
- [x] format version
- [x] frame length
- [x] frame type
- [x] Raft term/index
- [x] stream base offset
- [x] record count
- [x] timestamps
- [x] producer fields
- [x] compression flags
- [x] payload lengths
- [x] CRC32C coverage

Deliverable: `docs/FORMAT_V1.md`.

## P1.3 — Frame codec

- [x] encode data batch
- [x] encode control entry
- [x] decode with bounds checks
- [x] CRC32C verify
- [x] reject impossible lengths before allocation
- [x] golden-byte tests
- [x] fuzz decoder

## P1.4 — Segment writer

- [x] create active segment safely
- [x] sequential append
- [x] handle partial/short write errors
- [x] track byte position
- [x] group-flush API
- [x] segment roll by size
- [x] segment roll by time hook
- [x] directory sync where required

## P1.5 — Segment reader

- [x] sequential scan
- [x] fetch frame at position
- [x] offset-range iteration
- [x] malformed/truncated-tail classification
- [x] committed-region corruption classification

## P1.6 — Indexes

- [x] sparse offset index
- [x] time index
- [x] Raft index
- [x] monotonicity/range validation
- [x] rebuild indexes from `.log`
- [x] deleting indexes never destroys authoritative data

## P1.7 — Partition log abstraction

- [x] active + immutable segment set
- [x] append API
- [x] read-from-offset API
- [x] roll API
- [x] logical end offsets
- [x] committed boundary placeholder metadata without implementing Raft

## P1.8 — Crash recovery

- [x] scan final active segment
- [x] validate frame lengths/CRC
- [x] truncate only incomplete/corrupt uncommitted active tail
- [x] refuse silent repair inside committed region
- [x] rebuild missing/stale indexes
- [x] recover segment inventory after interrupted roll

## P1.9 — Copy-on-write segment generation primitive

This does **not** implement compaction yet. It creates the safe file-replacement mechanism P9 will reuse.

- [x] temp generation
- [x] fsync files
- [x] fsync directory
- [x] atomic manifest/generation switch
- [x] crash recovery for every swap point

## P1.10 — Adversarial tests

- [x] truncate generated logs at every byte boundary
- [x] flip random bits
- [x] corrupt frame length
- [x] corrupt CRC
- [x] delete index files
- [x] stale index positions
- [x] simulated ENOSPC/write error
- [x] simulated flush error
- [x] restart after each controlled append/roll stage

## P1 closure requirements

- [x] `FORMAT_V1.md` frozen
- [x] golden V1 files committed
- [x] all storage/property/fuzz tests green
- [x] no silent corruption path
- [x] indexes proven derived/rebuildable
- [x] P1 closure report written
- [x] P2 not started early
