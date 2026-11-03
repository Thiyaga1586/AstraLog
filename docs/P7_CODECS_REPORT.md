# AstraLog P7-B Types / Codecs / Golden Bytes Qualification Report

Date: 2026-10-03

Status: **PASS — P7-B QUALIFIED / P7-C NEXT**

## Scope

P7-B implements only the frozen P7 representation layer from actions P7-015 through P7-029:

- checked producer identity/epoch/sequence primitives;
- InitProducer V0 request/response bodies;
- Produce V1 request/response bodies;
- protocol API-version metadata;
- canonical SHA-256 record-set identity;
- ApplicationCommand V2;
- ControllerCommand V2 producer-session records;
- ControllerSnapshot V2 producer-session envelope;
- exact golden and malformed/noncanonical qualification.

No controller producer-session state machine, broker InitProducer runtime, partition dedupe state,
producer snapshot recovery, client idempotent producer, or P7 feature activation is implemented in
this checkpoint. Those remain P7-C and later responsibilities.

The entering freeze commit is:

`0ccb8f79bded084c231568738fe762eb736387ad` — `P7 freeze idempotent producer contract and checklist`

## Shared producer types

`astralog-common` now contains the lowest-layer producer primitives used by protocol, controller and
replication codecs:

- `ProducerId` is a non-zero u64;
- `ProducerEpoch` is a non-zero u32 with overflow-checked increment;
- `ProducerSequence` is u32 and provides checked batch-range arithmetic;
- `ProducerAuthority` is one canonical non-optional `(ProducerId, ProducerEpoch)` pair.

The zero ProducerId/Epoch pair exists only as the InitProducer V0 allocation sentinel on the wire;
it is never representable as active shared authority.

## Public wire codecs

### InitProducer V0

API key 20 V0 now has canonical protocol body types.

The decoder accepts exactly:

- `(producer_id=0, expected_epoch=0)` for new-session allocation; or
- `(producer_id>0, expected_epoch>0)` for a CAS epoch bump.

Mixed sentinel forms, zero success authority and trailing bytes fail closed.

### Produce V1

Produce API key 10 version 1 now has a separate request type rather than modifying Produce V0.
Admission enforced by the codec includes:

- `acks=quorum` only;
- non-zero ProducerId/ProducerEpoch;
- canonical unique TopicPartition entries;
- non-empty bounded record arrays;
- explicit record timestamps `>=0`;
- checked base-sequence range arithmetic with no wrap.

ProduceResponse V1 intentionally reuses the frozen V0 partition-result shape so a deduplicated
future success can return the original base/last offsets without inventing a new response layout.

The broker dispatch/advertisement layer is unchanged in P7-B. Protocol metadata knows that
InitProducer supports V0 and Produce has V0..V1 codec types, but runtime readiness remains disabled.

## Canonical batch digest

P7-B contains a dependency-free SHA-256 implementation inside `astralog-protocol`. It matches
standard FIPS test vectors and avoids introducing an external network/supply-chain dependency.

The exact digest input is frozen in `P7_PRODUCER_V1.md`: canonical record count followed by each
record's explicit timestamp, nullable key/value lengths and bytes, ordered header count, ordered
header key bytes and nullable header value bytes. The existing Wire V1 bounds apply before hashing.

An immutable fixture binds the canonical record-set bytes to its SHA-256 digest.

## ApplicationCommand V2

P4 ApplicationCommand V1 remains immutable.

P7-B adds an isolated V2 codec carrying:

- base stream offset;
- ProducerId/ProducerEpoch;
- base sequence;
- canonical records.

The codec independently enforces record count/size bounds, explicit timestamps and sequence-range
non-wrap so an internal caller cannot bypass Produce V1's representation contract. The V1 decoder
rejects V2 bytes rather than reinterpreting them.

## Controller V2 representation

### ControllerCommand V2

The P7 codec defines canonical producer-session mutation representations for:

- new session allocation; and
- exact CAS epoch bump.

The codec is version-separated from frozen ControllerCommand V1. Invalid zero authority,
non-next epoch bumps, trailing bytes and malformed lengths fail closed.

No controller state transition consumes these commands yet; that begins only in P7-C.

### ControllerSnapshot V2

ControllerSnapshot V2 is deliberately an envelope around the **exact canonical V1 snapshot bytes**
plus a strictly ascending ProducerId -> ProducerEpoch table and an outer CRC32C. This avoids
duplicating or reinterpreting frozen P5 state fields while allowing new binaries to dispatch V1/V2
snapshot formats explicitly.

Qualification proves:

- V1 snapshots continue to decode through the frozen V1 decoder;
- V2 round-trips canonically;
- the embedded V1 snapshot remains byte-exact;
- CRC corruption fails closed;
- duplicate/nonascending producer-session order with a valid recomputed CRC fails closed;
- V1 decoding does not accept V2 bytes.

## Golden compatibility

P7-B adds immutable representation fixtures for:

- InitProducer V0 request/response;
- Produce V1 request/response;
- canonical record-set SHA-256 digest;
- ApplicationCommand V2;
- ControllerCommand V2 new-session and epoch-bump commands;
- ControllerSnapshot V2.

Earlier compatibility remains green:

- original P2 protocol golden suite: **11/11 PASS**;
- P6 public golden suite: **6/6 PASS**;
- P7 public golden suite: **4/4 PASS**;
- frozen Produce V0 request/response bytes remain unchanged.

## Mechanical qualification

Focused and affected-crate gates on the finished P7-B tree are green:

- `cargo fmt --all -- --check`: **PASS**;
- `git diff --check`: **PASS**;
- workspace Clippy `--all-targets --all-features -D warnings`: **PASS**;
- `astralog-common`: **3/3 PASS**;
- protocol unit tests: **44/44 PASS**;
- original P2 protocol goldens: **11/11 PASS**;
- P6 public goldens: **6/6 PASS**;
- P7 public goldens: **4/4 PASS**;
- replication: **49/49 PASS**;
- controller: **96/96 PASS**.

The initially observed broad-workspace P6 churn timeout was re-run alone on the finished tree and
passed **1/1**. A subsequent clean serialized all-target workspace run then passed the complete P6
RF3 suite **9/9** and all lower-layer regressions.

Final serialized workspace qualification on the finished tree:

**485/485 PASS**

with the relevant package totals:

- broker unit: 19;
- P2 network: 9;
- P4 RF3: 1;
- P5 dynamic: 4;
- P6 RF3: 9;
- client: 10;
- common: 3;
- controller: 96;
- groups: 55;
- protocol unit: 44;
- P2 goldens: 11;
- P6 goldens: 6;
- P7 goldens: 4;
- Raft: 122;
- replication: 49;
- storage: 43.

## Boundary

P7-B does **not** activate public InitProducer/Produce V1 service behavior and does not introduce a
producer-session authority map into controller state. Therefore the next clean checkpoint is:

**P7-C only — P7-030 through P7-045: controller producer-session authority and V2 persistence.**

P7-D and later checkpoints remain disabled until P7-C independently qualifies.
