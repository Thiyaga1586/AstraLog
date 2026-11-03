# AstraLog P8-A Contract / Checklist Freeze Report

Date: 2026-10-04

Status: **P8-A FREEZE COMMIT — EXACT-HEAD QUALIFICATION NOTE REQUIRED FOR AUTHORITY**

## Scope

P8-A is specification and decomposition only. It introduces no compression codec/runtime, no
producer accumulator, no group-commit executor, no replication pipeline, no quota runtime and no
benchmark runner.

The exact P7 semantic closure authority is:

    d0d0e858832179909f959306b4fe7e0d63e6795b

That commit is the sole P7 closure commit and carries the P7 exact-HEAD PASS Git note. P7 is
therefore authoritatively closed before any P8 planning bytes are introduced.

P8-A is frozen on the qualified post-P7 execution base:

    13842791e1193778c6ea90ad472a3558ed89f1a0

`1384279` is the final test-harness-only P6 qualification base and includes the earlier `6f17a29`
silent-member repair beneath it. Both commits change only `crates/astralog-broker/tests/p6_routing.rs`;
the second hardens only the public-integration second-client setup deadline before mutation. Exact
`1384279` carries its own exact-HEAD PASS Git note, preserves all production P0-P7 semantics, and
passes P6 9/9 plus the complete serialized workspace 548/548 when qualified without overlapping Cargo
runs. P8-A's documentation-only delta is measured from this clean qualified base.

Frozen P8-A artifacts:

- P8_PERFORMANCE_V1.md — normative batching/compression/flow-control/performance contract;
- P8_CHECKLIST.md — complete P8-A through P8-I execution ledger;
- this report — architecture inputs, frozen decisions, boundary and qualification procedure;
- synchronized README.md and CURRENT_STATE.md status.

## Architecture inputs audited

P8-A is derived from existing AstraLog contracts rather than replacing them:

- PHASE_PLAN.md P8 goal/features/tests/exit gate;
- ASTRALOG_BIBLE.md batching/compression, overload-control and benchmark constitution sections;
- FEATURE_MATRIX.md P8 ownership for batching, compression, backpressure and baseline benchmarking;
- INVARIANTS.md I-01, I-02, I-04, I-05, I-14 through I-17, I-26 through I-30;
- FORMAT_V1.md reserved None/LZ4/Zstd compression byte plus stored/uncompressed length fields;
- PROTOCOL_V1.md feature key 14 BATCH_COMPRESSION, response throttle hint, Fetch bounds and permanent API registry;
- P4_REPLICATION_V1.md durability/persistence ordering and P8 performance boundary;
- P5_CONTROLLER_V1.md frozen TopicRecord V1 bytes and later performance-metadata ownership;
- P7_PRODUCER_V1.md one-unresolved-range producer rule and explicit P8 batching/pipelining handoff;
- exact P7 closure report and qualification note;
- current implementation hard bounds in protocol, storage, client, broker, controller and replication crates.

## Frozen P8-A decisions

### Correctness before throughput

P8 performance is never authority. Commit, durability, fencing, ordering, duplicate suppression and
visibility continue to come only from the already-qualified P1-P7 state machines.

No performance option may alter ack semantics.

### Feature activation

P8 uses the already-reserved feature key 14 BATCH_COMPRESSION, version 1.

Irreversible P8 behavior is gated by committed cluster feature level 4. The existing P5 monotonic
feature-level mechanism remains the sole activation authority.

Before level 4, writers continue emitting only Storage V1 compression=None and no P8 controller V3
mutation or P8 topic-performance API is usable.

### Controller-owned topic performance policy

The Bible classifies compression and batch/message limits as topic metadata. P8 therefore freezes a
controller-owned TopicPerformancePolicyV1 instead of a broker-local compression setting.

The policy carries:

- topic identity;
- None/LZ4/Zstd codec;
- maximum record bytes;
- maximum batch records;
- maximum logical uncompressed batch bytes.

Historical topics deterministically migrate to None plus existing global limits. Migration never
rewrites historical frames.

Frozen ControllerCommandV1/V2 and ControllerSnapshotV1/V2 are not extended in place. P8 plans
additive ControllerCommandV3 and ControllerSnapshotV3.

### Public API boundary

Produce V0, Produce V1 and Fetch V0 bodies remain byte-exact.

P8 plans additive CreateTopic V1 and DescribeTopic V1 for creation/readback of performance policy.
Permanent API key 6 AlterTopicConfig remains unavailable because dynamic topic-policy mutation is not
owned by P8.

### P7 producer boundary

P8 V1 deliberately does not widen the P7 one-unresolved-idempotent-range rule.

One closed per-partition accumulator batch maps to one producer sequence range. A second idempotent
range for that producer-partition waits until the first is terminal. Different partitions may
progress concurrently.

The P7 canonical digest remains over uncompressed logical record bytes. Compression is a storage
representation detail, not producer identity.

### Storage V1 compression activation

P8 activates only the compression fields already frozen in Storage V1:

- 0=None;
- 1=LZ4;
- 2=Zstd;
- stored payload length;
- uncompressed payload length.

No Storage V1 header/index/segment version changes are allowed.

Only data payloads are compressed in P8 V1. Control frames remain None.

Codec dependencies and settings must be pinned and immutable compressed golden vectors must exist
before activation.

Size and CRC checks occur before bounded decompression. Declared output above the configured ceiling,
unknown codec, malformed stream or exact-length mismatch fails closed.

### Batching and bounded client state

The client accumulator is per topic-partition and closes on count, bytes, linger, explicit flush,
delivery pressure, metadata invalidation or shutdown.

Accumulator bytes, record count, active partition count, in-flight requests and delivery deadline are
all finite. Linger influences when a valid batch closes, never partitioning/order/authority.

### Persistence group commit

P8 may coalesce compatible per-partition append/flush work to amortize physical sync, but may not
reorder P3 PersistenceBatch semantics, move commit ahead of required durability, cross
snapshot/truncation boundaries unsafely or turn ambiguous fsync into success.

Every original persistence token completes only from durability that can be proven for that token.

### Replication pipelining

Each leader-partition/follower path has hard in-flight message and byte windows plus bounded
persisted-history catch-up.

Queued/sent bytes are not replication authority; only P3 acknowledgements advance match/commit state.
Windows reset on leadership, transport/incarnation and rebootstrap boundaries. Control traffic keeps
reserved scheduling capacity.

### Fetch optimization

Fetch V0 wire and committed-read semantics remain unchanged. P8 may optimize wakeup coalescing,
bounded read-ahead, response allocation and copies.

The optional copy-minimized plaintext path is not required for closure; it may remain disabled if a
safe material benefit cannot be shown.

### Backpressure and watermarks

P8 requires finite client, connection, broker, persistence and replication resource budgets.

Broker data admission is separated from control capacity. Memory pressure uses high/low watermark
hysteresis. Overload degrades to bounded waiting, retryable busy/throttle, explicit timeout or TCP
flow control rather than unbounded memory.

### Quota boundary

P8 quotas are performance shaping only:

- broker-local anonymous/client scope;
- broker-local topic scope.

Authenticated principal mapping, cluster-managed security policy and ACLs remain P11.

Response-bearing throttled requests are rejected before mutation and may carry nonzero throttle_ms.
Request throttle_ms remains zero. No-response Produce remains bounded through admission/TCP flow
control and retains its existing delivery semantics.

### Benchmark constitution

P8 produces an engineering baseline, not the P12 release benchmark.

No benchmark figure is accepted without raw-data provenance including exact SHA, toolchain/build,
system hardware/software, AstraLog configuration, workload shape, RF/ack/compression/batching,
concurrency, warmup, measurement duration and repeat identity.

Required workload dimensions and metrics are frozen in P8_PERFORMANCE_V1.md. Long performance runs
remain outside normal cargo test; deterministic harness/report smoke tests remain in qualification.

## Hard bounds inherited by the freeze

P8 starts from and must respect current frozen/global limits including:

- Wire V1 maximum frame: 64 MiB;
- default broker/client/storage operational frame: 16 MiB;
- public record value maximum: 8 MiB;
- records per partition request: 10,000;
- partitions per request: 1,024;
- client default in-flight requests: 64;
- replication outbound messages: 4,096 and 64 MiB;
- replication pending proposals: 4,096 and 64 MiB;
- Fetch V1 max_wait_ms: 60,000 and never above request timeout.

P8 may add lower/tunable operational bounds but may not silently raise frozen global protocol/storage
limits.

## Checklist freeze

P8_CHECKLIST.md defines exactly **160 unique actions P8-001 through P8-160** across:

- P8-A contract/checklist freeze;
- P8-B controller topic-performance metadata and feature activation;
- P8-C Storage V1 LZ4/Zstd activation and recovery;
- P8-D client per-partition accumulator and batching;
- P8-E persistence group commit;
- P8-F replication pipelining and fetch tuning;
- P8-G bounded backpressure, watermarks and quotas;
- P8-H benchmark harness, baseline and deterministic stress;
- P8-I compatibility audit and exact closure.

Only **P8-001 through P8-018** are checked by P8-A.

P8-019 through P8-160 and every phase-exit invariant remain unchecked until their stated proof exists.

## Later-phase boundary

P8 does not pull forward:

- P9 physical retention/compaction/tombstone cleanup;
- P10 queue receive/lease/retry/DLQ semantics;
- P11 TLS/authentication/ACLs, authenticated-principal quota policy, drain, rolling upgrades or
  production observability;
- P12 final release chaos certification and final benchmark publication.

P8 stress may reuse existing failover machinery only to prove optimization safety.

## P8-A qualification gate

The P8-A freeze is qualified only when:

1. the contract/checklist/report/status delta is documentation-only;
2. checklist IDs are exactly P8-001 through P8-160 with no gaps/duplicates;
3. only P8-001 through P8-018 are checked;
4. all P8 phase-exit invariants remain unchecked;
5. no Rust/Cargo/dependency or frozen P0-P7 contract file is changed;
6. git diff hygiene and workspace formatting/Clippy/doc tests pass;
7. the complete serialized workspace regression remains green;
8. the P8-A commit repeats the gate on its exact clean HEAD.

### Pre-commit qualification evidence

The documentation-only freeze tree passed before commit:

- changed-path audit: exactly README.md, docs/CURRENT_STATE.md, docs/P8_CHECKLIST.md,
  docs/P8_CONTRACT_FREEZE_REPORT.md and docs/P8_PERFORMANCE_V1.md;
- checklist IDs: exactly 160 unique contiguous actions P8-001..P8-160;
- checked state: exactly P8-001..P8-018 checked; P8-019..P8-160 unchecked;
- all phase-exit invariants unchecked;
- git diff --check: PASS;
- cargo fmt --all -- --check: PASS;
- workspace Clippy --all-targets --all-features with -D warnings: PASS;
- workspace doc tests: PASS;
- complete serialized workspace: **548/548 PASS**;
- repaired P6 RF3 suite within that workspace: **9/9 PASS**;
- P4 RF3 within the accepted clean rerun: **1/1 PASS**;
- P5 dynamic: **4/4 PASS**.

This evidence permits creating the documentation-only P8-A freeze commit. It does not make that
commit authoritative by itself. The exact commit SHA must independently repeat the mechanical and
workspace gates and carry the P8-A exact-HEAD PASS Git note.

After exact-HEAD P8-A qualification, the only permitted implementation checkpoint is:

> **P8-B — P8-019 through P8-038: controller topic-performance metadata and feature activation**

P8-C and later remain disabled until P8-B independently qualifies.
