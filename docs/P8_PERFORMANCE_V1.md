# AstraLog P8 — Batching, Compression, Flow Control and Performance Baseline

Status: **P8-A FREEZE COMMIT — AUTHORITATIVE ONLY WITH EXACT-HEAD PASS GIT NOTE**

Date: 2026-10-04

## 1. Purpose

P8 improves throughput and overload behavior without weakening any P0-P7 correctness contract.

P8 owns:

- per-partition client batching;
- activation of the compression identifiers already reserved in Storage V1;
- deterministic LZ4 and Zstandard data-frame compression;
- bounded persistence group commit;
- bounded replication pipelining;
- long-poll/fetch-path tuning;
- client/broker/replication memory backpressure;
- broker-local token-bucket load shaping and response throttle hints;
- an optional copy-minimized plaintext fetch path;
- a reproducible performance baseline harness and generated baseline report.

P8 does not own:

- retention, physical log compaction or tombstone cleanup — P9;
- queue leases, redelivery, retry scheduling or DLQ semantics — P10;
- authenticated principal mapping, ACLs, TLS/mTLS, operational drain, continuous balancing or cluster-managed security policy — P11;
- final release chaos certification or final published production benchmark claims — P12.

No P8 optimization may change what counts as committed, successful, ordered, fenced, deduplicated or visible.

## 2. Entering authority boundary

P7 semantic authority remains the exact authoritative closure:

    d0d0e858832179909f959306b4fe7e0d63e6795b

That SHA carries the P7 exact-HEAD qualification Git note and closes P7-001 through P7-155.

Before P8-A, the final post-P7 **test-harness-only** execution base was independently qualified:

    13842791e1193778c6ea90ad472a3558ed89f1a0

That commit and its parent `6f17a29` change only `crates/astralog-broker/tests/p6_routing.rs`, carry
exact-HEAD qualification notes, and harden long P6 qualification fixtures without changing mutation/
retry authority. Exact `1384279` passes P6 9/9 and the complete serialized workspace 548/548 with no
overlapping Cargo qualification jobs. It changes no production P0-P7 semantics. Therefore
`1384279` is the execution base for P8-A while `d0d0e85` remains the
authoritative P7 semantic closure.

The following remain authoritative inputs rather than P8 redesign targets:

- Storage Format V1;
- Wire Protocol V1 and permanent API/feature numbers;
- P3 Raft commit/durability semantics;
- P4 quorum acknowledgement and committed-read semantics;
- P5 controller/placement authority;
- P6 consumer-group/offset authority;
- P7 ProducerId/ProducerEpoch/sequence/deduplication authority.

## 3. Safety invariants P8 must preserve

P8 qualification must preserve at minimum:

- I-01 one committed history per partition;
- I-02 quorum-ack durability;
- I-04 committed-only default reads;
- I-05 fencing across Raft/broker/producer/group authority;
- I-14 accepted producer order within one partition;
- I-15 exact idempotent retry never creates a second committed logical copy;
- I-16 older producer epochs remain fenced;
- I-17 sequence gaps/partial overlaps remain explicit failures;
- I-26 every queue/buffer/window/decompression allocation is bounded;
- I-27 data overload cannot starve Raft/heartbeat/control progress;
- I-28 irreversible feature use requires explicit compatibility activation;
- I-29 every published benchmark number has raw-data provenance;
- I-30 ambiguous durability never becomes success.

Performance is never an authority source.

## 4. Versioning and feature activation

### 4.1 Feature identity

P8 uses the already-reserved Wire V1 feature:

- feature key 14 = BATCH_COMPRESSION;
- feature version 1 is the P8 V1 capability.

### 4.2 Cluster feature level

P8 irreversible behavior requires committed cluster feature level 4.

The existing P5 feature-level rules remain unchanged:

- every active broker must advertise support for level 4 before the controller can commit the bump;
- the bump is monotonic and controller-Raft committed;
- a broker that cannot support the active level cannot become ACTIVE.

### 4.3 What is gated

Before committed feature level 4:

- writers emit Storage V1 compression=None only;
- controller V3 topic-performance commands are rejected;
- CreateTopic/DescribeTopic P8 performance versions are not advertised;
- ApiVersions feature key 14 is supported-but-inactive or absent according to the local binary;
- no compressed frame may be newly written.

After committed level 4 and local metadata convergence:

- feature key 14 advertises active version 1;
- P8 topic-performance APIs/config become usable;
- writers may emit LZ4 or Zstd according to committed topic policy.

Readers in a P8-capable binary must always be able to read already-written P8 compressed frames during restart/recovery even though write activation remains feature-gated.

Client-local batching alone does not change wire semantics; however the official P8 optimized producer path activates only after key 14 is active so fallback behavior is explicit and observable.

## 5. Topic performance policy and controller versioning

### 5.1 Controller-owned policy

Compression policy is topic metadata in the AstraLog architecture. A leader failover must not change the codec or admission limit because of broker-local configuration.

P8 introduces controller-owned TopicPerformancePolicyV1:

    topic_id                      16 bytes
    compression                   u8, 0=None, 1=LZ4, 2=Zstd
    max_record_bytes              u32
    max_batch_records             u32
    max_batch_uncompressed_bytes  u32

Validation:

- max_record_bytes is in 1..=8 MiB and never exceeds the frozen Wire V1 record-value envelope;
- max_batch_records is in 1..=10,000 and never exceeds MAX_RECORDS_PER_PARTITION;
- max_batch_uncompressed_bytes is positive and must fit the configured Storage V1 decompression ceiling;
- a record is not admitted when its encoded logical bytes exceed the topic batch ceiling;
- compression accepts exactly None/LZ4/Zstd.

### 5.2 Existing topics

Migration of a pre-P8 topic is deterministic:

- compression = None;
- max record bytes = existing global P2/P1-compatible record limit;
- max batch records = existing protocol maximum;
- max batch uncompressed bytes = active P8 decompression ceiling.

Migration does not rewrite historical frames.

### 5.3 Controller bytes

Frozen ControllerCommandV1/V2 and ControllerSnapshotV1/V2 are never extended in place.

P8 uses:

- ControllerCommandV3 for atomic topic creation with TopicPerformancePolicyV1;
- ControllerSnapshotV3 wrapping the exact canonical V2 snapshot plus a strictly sorted TopicId-to-policy table and outer CRC.

Decoding remains version-dispatched:

- V1 -> existing state + no producer sessions + default P8 policies;
- V2 -> existing P7 state + default P8 policies;
- V3 -> existing P7 state + explicit P8 policies.

Controller V3 mutation emission is feature-level-4 gated.

### 5.4 Public topic API

P8 does not take ownership of permanent API key 6 AlterTopicConfig.

Instead:

- CreateTopic V0 remains byte-exact and creates the default P8 policy;
- CreateTopic V1 may add P8 performance policy for a newly created topic;
- DescribeTopic V0 remains byte-exact;
- DescribeTopic V1 may return the committed performance policy.

P8 V1 does not mutate an existing topic's compression policy after creation. Dynamic topic-policy alteration remains with the later owner of AlterTopicConfig.

## 6. Client producer batching

### 6.1 Partition locality

The accumulator is keyed by topic and partition. Records for different partitions are never merged into one partition batch.

Keyed partitioning remains the frozen Murmur3 mapping. Null-key sticky partitioning retains the current sticky partition while the active batch is open and rotates only after that batch closes or metadata invalidates the partition.

### 6.2 Batch close conditions

A partition batch closes at the first applicable condition:

1. configured maximum record count;
2. configured maximum logical batch bytes;
3. configured linger deadline;
4. explicit client flush;
5. delivery deadline pressure;
6. metadata invalidates the current leader/partition route;
7. client shutdown.

No clock participates in partition choice, record ordering, sequence calculation or durable authority; time only decides when an otherwise-valid batch closes for send.

### 6.3 Hard bounds

The client must have independent finite bounds for:

- total accumulator bytes;
- total buffered records;
- active partition accumulators;
- requests in flight;
- delivery/retry deadline.

Admission occurs before copying an unbounded record into an accumulator.

When the accumulator is full, behavior is explicit: bounded wait until capacity becomes available or a deterministic buffer/timeout error. It never grows without bound.

### 6.4 P7 idempotent compatibility

P8 V1 does not widen the P7 producer in-flight model.

For idempotent Produce V1:

- one closed accumulator batch becomes exactly one producer sequence range for that partition;
- at most one unresolved range exists per producer-partition;
- a second range for the same producer-partition waits until the first is terminal;
- different partitions may progress concurrently;
- exact retry uses the identical logical record set and sequence range;
- the P7 digest is computed over uncompressed canonical record-set bytes;
- compression choice/output is not part of producer identity.

Any future wider in-flight sequence model requires an explicitly versioned contract and is outside P8 V1.

Produce V0 may benefit from accumulation but retains its existing non-idempotent retry semantics and ack modes.

## 7. Compression contract

### 7.1 Storage V1 envelope

P8 activates fields already frozen in Storage V1:

- compression byte 0=None, 1=LZ4, 2=Zstd;
- stored payload length;
- uncompressed payload length;
- frame CRC32C.

P8 does not change magic, format version, the 96-byte header, frame types, attribute-bit assignments, indexes, segment format or stream/Raft offsets.

### 7.2 Compression input

Only data-frame payloads are compressible in P8 V1. Control frames remain uncompressed.

The input is the exact canonical uncompressed Storage V1 data-record-set payload that would otherwise be stored with None.

The P7 producer digest is computed before compression from canonical logical records. The same producer sequence/digest therefore remains valid regardless of storage codec.

### 7.3 Codec selection

For LZ4 or Zstd topic policy:

- encode the canonical uncompressed payload with the frozen P8 codec profile;
- use compressed form only when stored payload is strictly smaller than raw payload;
- otherwise store None with identical logical payload.

The frozen P8-C codec profiles are:

- LZ4: `lz4_flex = 0.14.0`, raw block encoding, safe encoder/decoder, checked decode;
- Zstd: `zstd = 0.14.0`, compression level `3`, no dictionary, one canonical frame per Storage V1 payload.

Both direct dependencies are exact-version pinned in the workspace manifest and Cargo.lock binds their
resolved transitive codec implementation. The P8-C closure freezes immutable Storage V1 golden frames
for both profiles.

### 7.4 Decode order and compression-bomb safety

For every stored frame:

1. validate frame/header lengths before allocation;
2. validate stored payload length against the physical frame;
3. validate declared uncompressed payload length against the configured P8 decompression ceiling;
4. validate CRC32C over physically stored frame bytes;
5. only then allocate/decompress;
6. require exact declared decompressed length;
7. decode canonical logical payload and apply all existing P1/P7 semantic checks.

Unknown codec, over-limit output, truncated stream, trailing codec data, length mismatch or decoder error fails closed.

The default logical decompression ceiling is the already-frozen P8 policy ceiling of **16 MiB - 96
bytes**, i.e. the maximum frame envelope minus the fixed 96-byte Storage V1 header. This is an
allocation/logical-payload ceiling, not permission to exceed the independent physical frame bound:
every stored frame must still satisfy `96 + stored_payload_len + 4 CRC <= max_frame_bytes`. Therefore a
None frame, including a requested compressed frame that takes the no-gain fallback, can have a smaller
effective payload maximum than a compressible LZ4/Zstd frame. Configuration may lower the logical
ceiling but may not silently raise frozen global logical-record limits.

### 7.5 Recovery and replication

Compressed and uncompressed frames may coexist in one partition/segment.

Restart, scan, index rebuild, Raft suffix truncation, rebootstrap and producer-state reconstruction operate on the same logical records regardless of compression.

All replicas must reconstruct identical logical payload and producer state. P8 qualification also requires deterministic stored bytes for the same codec/input/settings within the pinned P8 build.

## 8. Persistence batching and group commit

P8 may amortize storage flush/sync work, but P3 remains the sole authority for operation order and commit safety.

A per-partition persistence executor may coalesce adjacent compatible append/flush work up to finite operation count, byte count and maximum group-delay deadline.

Group commit must preserve each P3 PersistenceBatch operation order. It may not:

- move AdvanceCommit ahead of required append durability;
- acknowledge a proposal before its ack-mode durability condition is met;
- cross snapshot/install/rebootstrap/truncation barriers unsafely;
- turn failed or ambiguous fsync into success.

Each original persistence token completes independently from proven durable state.

One physical sync may satisfy multiple earlier tokens only when every required write for those tokens is included in that durable sync.

On ambiguous storage failure, every token whose required durability cannot be proven fails.

## 9. Replication pipelining

P8 changes transport scheduling, not Raft quorum rules.

Each leader-partition/follower pair has finite:

- in-flight message count;
- in-flight byte count;
- catch-up/read-ahead window.

The window is reset/fenced on leader-term change, connection replacement, broker-incarnation change and rebootstrap/snapshot boundaries.

Rules:

- only P3 acknowledgements advance match/commit state;
- queued/sent bytes are never replicated authority;
- slow followers consume at most their configured window;
- a slow follower cannot force unbounded leader memory;
- persisted segment history is the catch-up source rather than an unbounded retained send buffer;
- control/heartbeat traffic retains reserved scheduling capacity.

## 10. Fetch and long-poll tuning

Wire Fetch V0 remains byte-exact.

Existing semantics remain:

- committed-read isolation by default;
- max_wait_ms never exceeds request timeout;
- per-partition max_bytes/max_records remain hard bounds;
- min_bytes remains notification-driven;
- leader/term routing and fencing remain unchanged.

P8 may improve wakeup coalescing, response assembly allocation, partition scan batching, bounded read-ahead and copy count.

P8 must not return uncommitted data to improve latency.

An optional plaintext/noncompressed copy-minimized path is allowed only when source data is committed and immutable, no decompression/transformation is required, Fetch V0 bytes remain identical, buffer lifetime is safe and fallback is always correct. It is optional for P8 closure.

## 11. Backpressure and memory watermarks

P8 must bound load at every data-plane layer.

Client bounds:

- accumulator bytes/records/partitions;
- in-flight permits;
- delivery/retry deadline.

Connection bounds remain:

- maximum frame size;
- in-flight request count;
- pending-response byte budget.

Broker data plane adds a global bounded data-admission budget separate from control work.

Memory watermarks use hysteresis:

- crossing high watermark stops or throttles new data admission;
- data admission resumes only below low watermark;
- high is strictly greater than low;
- control work retains separate reserved capacity.

Overload degrades into bounded wait, nonzero throttle hints, BROKER_BUSY, explicit timeout or TCP flow control according to request contract — never unbounded allocation.

Replication windows and group-commit queues have both count and byte bounds.

## 12. Token-bucket quota boundary

P8 implements performance shaping, not security policy.

P8 V1 quota scopes are:

- broker-local anonymous/client traffic;
- broker-local topic traffic.

There is no authenticated principal authority in P8. P11 later maps authenticated principals and cluster-managed policy onto the bounded shaping mechanism.

For response-bearing requests:

- token availability is checked before mutation admission;
- insufficient tokens cause no mutation and return the existing retryable busy surface with nonzero response throttle_ms;
- throttle_ms is a minimum delay before automatic retry/additional load for that scope.

acks=0 Produce cannot receive an authoritative throttle response; it remains governed by connection/broker admission bounds and TCP flow control and remains delivery-unreliable.

Token refill uses monotonic elapsed time only. Clock values never grant durable authority.

## 13. Performance configuration doctrine

P8 separates correctness bounds from tuning defaults.

These relationships may not be tuned away:

- no batch above global protocol/storage limits;
- no decompression above configured hard ceiling;
- no unbounded accumulator/queue/window;
- one unresolved idempotent range per producer-partition;
- reserved control-plane capacity;
- no success before required durability.

Concrete defaults such as linger, group-commit delay, replication window and watermark percentages may be tuned during P8 qualification only within these relationships and must be recorded in benchmark provenance.

## 14. Benchmark constitution for P8 baseline

P8 produces an engineering baseline, not the P12 release benchmark.

No benchmark number may appear in README/status prose unless generated from committed P8 benchmark tooling.

Every baseline result binds exact git SHA, Rust toolchain, release build flags, AstraLog config, OS/kernel, CPU/core count, RAM, disk/filesystem, network topology, topic/partition count, replication factor, ack mode, compression, batching/linger settings, producer/consumer concurrency, record-size distribution, warmup, measurement duration and repeat number.

Raw machine-readable samples are retained separately from the generated human report.

### 14.1 Workload dimensions

The baseline manifest must exercise:

- 100 B, 1 KiB and 10 KiB events;
- keyed and unkeyed;
- RF=1 and RF=3;
- leader and quorum acknowledgement where the selected producer API permits it;
- None/LZ4/Zstd;
- single and multiple producers;
- producer-only;
- consumer-only backlog;
- simultaneous produce+consume.

The manifest may use a deterministic coverage matrix rather than the full Cartesian product, but every factor level and every critical pair of message-size/compression, RF/ack-mode, compression/workload-mode and producer-concurrency/workload-mode must appear.

Published comparisons require multiple measurement repeats.

### 14.2 Metrics

At minimum record:

- events/sec;
- MiB/sec;
- producer latency p50/p95/p99/p999/max;
- fetch latency;
- CPU;
- RSS;
- disk read/write throughput;
- fsync latency;
- network throughput;
- replication lag;
- error/throttle counts.

Percentiles come from raw samples/histograms through repository tooling, never hand-entered Markdown.

### 14.3 Build/run rules

- benchmarks never run in debug builds;
- warmup and measurement are separate;
- raw data is saved before report generation;
- report generation is deterministic from raw data;
- normal cargo test does not execute long performance benchmarks;
- a short deterministic harness smoke test belongs in workspace qualification.

## 15. Required P8 failure/stress qualification

P8 implementation must mechanically cover:

- ordering across batched Produce retries;
- idempotent exact retry after batched/lost response;
- LZ4/Zstd round trip and immutable golden bytes;
- CRC corruption before decompression;
- decompression-bomb/declared-size one-over rejection;
- mixed compressed/uncompressed segment restart/index rebuild;
- group-commit crash/ambiguous-fsync boundaries;
- slow follower under sustained leader writes;
- follower reconnect/rebootstrap with pipelined replication;
- slow consumer/long-poll pressure;
- producer faster than disk until client/broker bounds engage;
- sustained memory-watermark hysteresis;
- quota throttle with no pre-throttle mutation;
- control-plane progress under saturated data queues;
- benchmark manifest/provenance completeness;
- deterministic report regeneration from identical raw input.

## 16. Compatibility requirements

P8 closure must requalify:

- frozen Storage V1 noncompressed goldens;
- original P2 wire goldens;
- P3 Raft tests;
- P4 RF3 quorum/committed-read behavior;
- P5 dynamic controller/placement behavior;
- P6 groups/offsets;
- P7 idempotent producer retry/fencing/failover.

Produce V0/V1 and Fetch V0 bodies remain unchanged by P8 V1.

Historical Storage V1 uncompressed frames remain byte-exact.

## 17. Later-phase boundary

P8 excludes:

### P9

- time/size retention;
- physical segment deletion;
- copy-on-write log compaction;
- tombstone grace/removal;
- compaction safety watermark.

### P10

- queue subscriptions;
- receive/ack/nack;
- visibility leases;
- retry scheduling/backoff;
- delivery count;
- DLQ transfer.

### P11

- TLS/mTLS;
- authentication;
- authenticated principal mapping;
- ACL authorization;
- operator-managed cluster quota policy;
- drain/rebalance/rolling upgrade;
- production observability stack.

### P12

- final end-to-end chaos closure;
- release-grade production benchmark claims;
- final leader-kill benchmark publication.

P8 baseline/failure tests may exercise existing failover behavior to prove optimization safety, but that is not a P12 release-chaos claim.

## 18. Frozen implementation checkpoints

P8 executes only in:

- P8-A — contract/checklist freeze;
- P8-B — controller topic-performance metadata + feature activation;
- P8-C — Storage V1 LZ4/Zstd activation and recovery;
- P8-D — client per-partition accumulator/batching;
- P8-E — persistence group commit;
- P8-F — replication pipelining + fetch tuning;
- P8-G — bounded data-plane backpressure/watermarks/quotas;
- P8-H — benchmark harness, baseline and deterministic stress;
- P8-I — compatibility audit, closure report and exact-HEAD requalification.

No later checkpoint begins until the current checkpoint is independently qualified.

## 19. P8-A freeze gate

P8 implementation remains disabled until a documentation-only P8-A commit:

1. contains this contract, the complete P8 checklist, freeze report and canonical status updates;
2. changes no Rust source, Cargo manifest or frozen P0-P7 specification bytes;
3. has mechanically contiguous checklist numbering;
4. checks only P8-A freeze actions;
5. passes whitespace/diff hygiene and the existing workspace regression;
6. is requalified on its exact clean HEAD.

After P8-A qualifies, **P8-B only** may begin.
