# AstraLog P8-C Storage V1 Compression and Recovery Qualification Report

Date: 2026-10-06

Status: **P8-C CLOSURE COMMIT — EXACT-HEAD QUALIFICATION NOTE REQUIRED FOR AUTHORITY**

## Scope

P8-C closes actions **P8-039 through P8-056** only.

It activates the compression identifiers already reserved by Storage V1 and binds the committed
P8-B topic compression policy into partition persistence. It does not implement P8-D client
batching, P8-E group commit, P8-F replication pipelining, P8-G fetch optimization/backpressure or
later benchmark work.

The qualified entering authority is:

    339d87ffda5d23777d47ea6860bf65a057384c9d

That exact P8-B HEAD carries the P8-B PASS Git note authorizing P8-C only.

## Frozen codec profiles

P8-C pins these direct workspace dependencies:

- lz4_flex 0.14.0, exact version, raw-block encoding with safe/checked decode;
- zstd 0.14.0, exact version, level 3, no dictionary, one frame per Storage V1 payload.

Cargo.lock binds the exact resolved transitive codec implementation.

Compression input is the exact canonical data-record-set payload that Storage V1 previously wrote
uncompressed. Compression therefore changes only the physical payload representation, never the
logical record encoding or the P7 canonical producer digest.

## Storage V1 activation

The existing 96-byte Storage V1 header is unchanged.

For data frames:

- compression byte 0 remains None;
- compression byte 1 activates LZ4;
- compression byte 2 activates Zstd;
- compressed bytes are stored only when strictly smaller than the canonical raw payload;
- otherwise the writer falls back to None;
- stored_payload_len names physical payload bytes;
- uncompressed_payload_len names canonical logical payload bytes.

Control-frame writers remain None-only and the reader rejects a forged compressed control frame.

Historical None frame/index/manifest/partition-meta golden fixtures remain byte-identical to P8-B.

P8-C adds immutable compressed Storage V1 fixtures:

- data_p8_lz4_v1.hex;
- data_p8_zstd_v1.hex.

They bind the exact 96-byte header, physical codec bytes and frame CRC32C.

## Bounded decode order

The reader fails closed in this order:

1. physical frame length and stored-payload consistency;
2. declared logical uncompressed_payload_len against the hard P8 ceiling;
3. frame CRC32C over the physical compressed bytes;
4. compression identifier validation;
5. bounded codec decode;
6. exact decompressed output length;
7. existing logical record/schema validation.

The default logical decompression ceiling is **16 MiB - 96 bytes**.

Qualification covers CRC-valid unknown codec id, one-over declared logical size, truncated streams,
trailing codec data, short/long output declarations, control compression forgery and deterministic
no-gain fallback. No decode path allocates from an unbounded storage length.

## Recovery and index semantics

P8-C proves one segment may contain mixed None/LZ4/Zstd data frames. Restart and scan reconstruct
the same Raft indexes, stream offsets, timestamps and logical records.

Sparse offset/time/Raft indexes are rebuilt from decoded logical frame metadata, not compressed
byte positions inside record payloads.

Uncommitted compressed suffix truncation never crosses the durable commit boundary, restores the
exact next Raft index and stream offset, and permits a replacement compressed suffix without gaps.

## P4/P8-B persistence policy handoff

PartitionPersistence retains None as its default compression mode, preserving every historical P4
caller.

P8-C adds an explicit bounded data-compression selector. Dynamic broker topology derives it only
from committed P8-B controller topic policy after feature-level-4 activation:

- controller None -> Storage None;
- controller LZ4 -> Storage LZ4;
- controller Zstd -> Storage Zstd.

The selected codec participates in dynamic topology equality/reconfiguration and is applied when a
replicated partition runtime opens.

The real dynamic-broker P8 test creates a Zstd topic, produces/fetches the same logical record,
shuts the broker down and scans the real partition log, proving the committed data frame is
physically stored as Zstd.

## Rebootstrap and P7 producer authority

Compressed P4 rebootstrap qualification preserves None control frames, LZ4/Zstd data frames,
committed logical history, snapshot/package encode/decode/install and restart recovery.

The explicit persistence compression selector survives an in-process atomic rebootstrap so a
subsequent suffix append keeps the selected codec.

P7 producer recovery is representation-independent. The same two logical batches persisted as
LZ4/Zstd and as None reconstruct identical ProducerId/epoch, next sequence, recent retry window,
stream-offset ranges and canonical P7 retry digests.

## Qualification evidence

Focused P8-C storage:

- P8 storage tests: **9/9 PASS**;
- complete storage crate: **53/53 PASS**.

Focused P8-C replication/recovery:

- compressed rebootstrap/producer recovery: **2/2 PASS**;
- complete replication crate: **84/84 PASS**.

Frozen compatibility:

- P2 network: **9/9 PASS**;
- P4 RF3: **1/1 PASS**;
- P5/P8 dynamic broker: **5/5 PASS**;
- P6 RF3/group/offset/churn/public integration: **9/9 PASS**;
- P7 runtime: **2/2 PASS**;
- P7 failover: **12/12 PASS**;
- original P2/P6/P7 protocol goldens: **11/11 + 6/6 + 4/4 PASS**;
- historical Storage None golden files: tree-identical to P8-B authority.

Core crates:

- broker: **22/22 PASS**;
- client: **12/12 PASS**;
- common: **3/3 PASS**;
- controller: **113/113 PASS**;
- groups: **55/55 PASS**;
- protocol: **45/45 PASS**;
- Raft: **122/122 PASS**;
- replication: **84/84 PASS**;
- storage: **53/53 PASS**.

The bounded split workspace gate covers **568/568 tests**.

One earlier monolithic all-target run was deliberately not accepted as qualification because the
P7 controller-outage integration case exhausted its 60-second broker-health harness deadline after
all P6 cases had already passed. The P8-C tree has zero source delta in broker/controller/client/
protocol at that failure boundary. The exact failed case immediately passed **1/1** unchanged, and
the complete P7 failover target then passed **12/12** unchanged. Qualification therefore uses the
deterministic bounded target decomposition rather than treating that timing-sensitive monolithic
harness failure as a storage semantic failure.

Mechanical gates:

- cargo fmt --all -- --check: **PASS**;
- cargo clippy --workspace --all-targets --all-features -- -D warnings: **PASS**;
- workspace doc tests: **PASS**;
- git diff --check: **PASS**.

## Closure boundary

P8-039 through P8-056 are mechanically satisfied.

P8-D and every later checkpoint remain disabled until the exact P8-C closure commit is clean,
passes the same bounded exact-HEAD qualification, and carries the exact-HEAD PASS Git note.

Only then may **P8-D — P8-057 through P8-073** begin. P8-074 is the first P8-E action.
