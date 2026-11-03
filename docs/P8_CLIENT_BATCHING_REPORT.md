# AstraLog P8-D Client Per-Partition Accumulator and Batching Qualification Report

Date: 2026-10-06

Status: **P8-D CLOSURE COMMIT — EXACT-HEAD QUALIFICATION NOTE REQUIRED FOR AUTHORITY**

## Scope

P8-D closes actions **P8-057 through P8-073** only.

The canonical checklist assigns **P8-074 to P8-E**, so P8-074 and all persistence group-commit work remain disabled.

The qualified entering authority is:

    2c311fce8470faf10590551dbd78a0cacdc6400b

That exact P8-C HEAD carries the P8-C PASS Git note authorizing P8-D.

P8-D introduces an opt-in client-side batching path over the already-frozen Produce V0/V1 wire formats. Existing immediate `produce()` and `produce_idempotent_partition()` APIs keep their prior semantics.

## Bounded accumulator configuration

P8-D adds finite producer batching controls to `ClientConfig`:

- maximum records per client batch;
- maximum logical bytes per client batch;
- positive linger duration;
- total accumulator byte bound;
- total buffered-record bound;
- active topic-partition accumulator bound;
- finite buffered-producer delivery timeout.

Global client validation preserves every pre-P8 legacy connection configuration. The stronger cross-field rule that a buffered producer delivery budget must cover the configured P7 retry budget is checked before the first buffered record is admitted. This keeps P6/P7 client compatibility while still preventing a buffered producer from starting with an impossible delivery budget.

Admission is deterministic and occurs before the client copies a record into an accumulator. Over-limit records return an explicit record-too-large error. Exhausted byte, record or partition capacity returns an explicit bounded accumulator-full error; no unbounded fallback allocation exists.

## Partition locality and logical sizing

The accumulator is keyed by exact `(TopicId, partition)`.

No batch can contain records from different partitions.

Keyed partitioning still executes the frozen Murmur3 mapping before accumulation. P8-D does not add a second partitioner or reinterpret hash results.

Null-key records use the existing sticky partitioner. The sticky partition remains stable while its batch is open and rotates only after the batch closes or metadata invalidates that partition.

P8-D exposes `WireRecordV0::encoded_logical_len()`, which calls the existing canonical record encoder and returns its exact encoded logical byte count. This is an additive helper only; Produce V0/V1 bytes and record encoding are unchanged.

## Batch close conditions

A per-partition open batch is closed by:

- exact configured record-count boundary;
- exact configured logical-byte boundary;
- deterministic monotonic linger deadline;
- explicit producer flush;
- metadata route invalidation;
- client shutdown.

If adding a record would cross the local record/byte boundary, the existing batch is closed and drained first; the new record is never silently moved into the old range.

Linger time never participates in partition choice, sequence assignment, record order or durable authority. It only requests closure of an otherwise-valid open batch.

## Flush, metadata invalidation and shutdown

`flush_producer_batches()` marks all currently open batches closed and drives their partition-local delivery under the finite producer-delivery configuration.

Metadata refresh compares previous and current partition authority. A leader/epoch/error change:

- invalidates the sticky choice for that exact partition;
- marks that partition's open accumulator batch closed;
- wakes buffered callers so the batch is driven without changing partition identity or record order.

The metadata path does not recursively perform a routing refresh. This avoids recursive async ownership while still making invalidation observable to the waiting producer path.

`close_with_drain()` first closes accumulator admission, requests drain of every current batch, then closes transport regardless of whether drain succeeds or returns a terminal timeout/error. The compatibility `close()` API delegates to this bounded drain behavior and discards the returned drain result, preserving its historical signature.

## P7 idempotent compatibility

P8-D does not widen P7's one-unresolved-range rule.

For Produce V1:

- records may accumulate before any producer sequence is consumed;
- a sequence range is assigned only when a closed batch reaches the existing `reserve_producer_request()` send boundary;
- a second closed V1 batch for the same producer-partition does not drive while a P7 pending request exists;
- different partitions remain independently accumulable;
- the existing encoded pending Produce V1 body remains the retry authority;
- route refresh may update only the leader epoch while preserving producer authority, base sequence, record count and exact logical records;
- completion advances the next producer sequence only after the existing P7 terminal-success path.

The P7 canonical digest remains over the same uncompressed logical records. Storage compression is not part of producer identity.

Produce V0 keeps its frozen non-idempotent semantics. Client-side accumulation does not introduce blind mutation retry behavior.

## Public buffered producer surface

P8-D adds opt-in buffered producer calls:

- `produce_buffered()` for Produce V0;
- `produce_idempotent_buffered()` for Produce V1;
- `flush_producer_batches()`;
- `close_with_drain()`.

Buffered production resolves the committed P8 topic performance policy through DescribeTopic V1 and caches the validated policy by TopicId. Therefore the optimized path is explicitly feature-gated by the P8-ready public topic API.

Existing immediate producer APIs remain available and are not silently converted into batching.

## Real broker/client integration

The existing real dynamic-broker P8 integration now also qualifies P8-D.

The test:

1. commits feature level 4 and a Zstd topic performance policy;
2. configures client batch count to two with a long linger so count closure is deterministic;
3. concurrently admits two ordered Produce V0 records to one keyed partition and receives offsets 0 and 1;
4. initializes a P7 producer session;
5. concurrently admits two ordered Produce V1 records and receives offsets 2 and 3;
6. admits one V0 record and proves explicit flush drains it at offset 4;
7. admits one final V0 record and proves `close_with_drain()` commits it at offset 5 before transport closes;
8. reconnects a verifier client and fetches all six records in exact logical order;
9. scans the real partition log and proves the sum of stored logical records is six and every physical data frame uses committed Zstd compression.

The test intentionally does **not** require one physical P4/Storage frame per client Produce request. Client request batching and P4 storage-frame coalescing are separate phase responsibilities; P8-D constrains logical order/offsets and P8-C constrains physical codec policy.

## Qualification repair encountered during P8-D

An intermediate candidate added a global `ClientConfig::validate()` rule requiring `producer_delivery_timeout >= request_timeout * (max_producer_retries + 1)`.

That candidate was rejected because three frozen P6 integration fixtures use longer legacy request timeouts while never enabling buffered production. The rule made those clients fail at connect time before any P6 behavior ran.

The correction preserves global legacy validation and moves the retry-budget cross-check to the opt-in buffered producer admission boundary. After that repair:

- each of the three previously affected P6 tests passed independently;
- the complete P6 suite passed **9/9**;
- the complete P7 failover suite passed **12/12**.

No P6 or P7 source/test behavior was weakened to accommodate P8-D.

## Qualification evidence

Focused P8-D:

- P8 client batching semantic tests: **10/10 PASS**;
- complete client crate: **22/22 PASS**;
- real P5/P8 dynamic broker integration: **5/5 PASS**, including the buffered V0/V1/flush/shutdown/fetch/codec campaign.

Frozen compatibility:

- P2 network: **9/9 PASS**;
- P4 RF3: **1/1 PASS**;
- P6 RF3/group/offset/churn/public integration: **9/9 PASS**;
- P7 runtime: **2/2 PASS**;
- P7 failover: **12/12 PASS**;
- original P2/P6/P7 protocol goldens: **11/11 + 6/6 + 4/4 PASS**;
- all historical Storage V1 None/LZ4/Zstd golden files: tree-identical to P8-C authority.

Bounded fast/core target totals:

- broker units: **22/22**;
- P2 network: **9/9**;
- P4 RF3: **1/1**;
- P5/P8 dynamic: **5/5**;
- P7 runtime: **2/2**;
- client: **22/22**;
- common: **3/3**;
- controller: **113/113**;
- groups: **55/55**;
- protocol: **45/45**;
- P2/P6/P7 goldens: **11/11 + 6/6 + 4/4**;
- Raft: **122/122**;
- replication: **84/84**;
- storage: **53/53**.

The bounded fast/core gate is **557/557 PASS**.

Adding separately isolated P6 **9/9** and P7 failover **12/12** yields a complete bounded qualification total of **578/578 PASS**.

Mechanical gates:

- `cargo fmt --all -- --check`: **PASS**;
- `cargo clippy --workspace --all-targets --all-features -- -D warnings`: **PASS**;
- workspace doc tests: **PASS**;
- `git diff --check`: **PASS**;
- historical Storage V1 golden fixtures unchanged from `2c311fc`: **PASS**;
- original P2/P6/P7 golden source files unchanged from `2c311fc`: **PASS**.

## Closure boundary

P8-057 through P8-073 are mechanically satisfied.

P8-074 is **not** part of P8-D. It is the first P8-E persistence group-commit action and remains unchecked.

P8-E and every later checkpoint remain disabled until the exact P8-D closure commit:

1. is clean;
2. passes the same bounded **578/578** exact-HEAD qualification;
3. passes fmt/Clippy/doc/diff and frozen-byte compatibility checks;
4. carries the exact-HEAD PASS Git note.

Only then may **P8-E — P8-074 onward** begin.
