# AstraLog P7-A Contract / Checklist Freeze Report

Date: 2026-10-02

Status: **PASS — P7-A ARCHITECTURE FROZEN / P7-B NEXT**

## Scope

P7-A is planning/decomposition only. It freezes the complete P7 producer-idempotency contract and
execution checklist before any P7 Rust implementation begins.

The exact P6 closure boundary entering this checkpoint is:

`a76b13688d795409e4804fc3d57b416024205eb8` — `P6 close consumer offsets and groups phase`

The worktree was clean at that boundary and contained no partial P7 artifacts.

## Inputs reviewed

P7-A is derived from the actual frozen repository contracts:

- `ASTRALOG_BIBLE.md` sections 10, 12, 14, 15, 19, 23, 31 and the final engineering doctrine;
- `PHASE_PLAN.md` P7 goal/features/tests/exit gates;
- `DECISIONS.md` ADR-012 producer ID+epoch+sequence and ADR-015 feature-level gating;
- `INVARIANTS.md` I-02, I-05, I-15, I-16, I-17, I-26, I-30, I-45 and I-48;
- frozen `FORMAT_V1.md` producer metadata already present in the 96-byte data-frame header;
- frozen `PROTOCOL_V1.md` API key 20, feature key 11, Produce API versioning and producer errors;
- P5 producer-ID allocator qualification and its explicit P7 boundary;
- P6 exact closure report and later-phase scope audit;
- current P4 ApplicationCommand V1 and replication runtime, which still write `producer=None`.

## Frozen P7-A decisions

### Authority

- controller Raft is the sole issuer of public ProducerId/ProducerEpoch authority;
- each data partition owns accepted producer epoch plus per-partition sequence/dedupe authority;
- unseen/higher epochs require controller confirmation;
- equal already-accepted epochs remain data-plane local;
- fencing is partition-local and becomes durable from committed higher-epoch partition history.

### Session lifecycle

- ProducerId is non-zero u64; zero is new-session sentinel only;
- ProducerEpoch is non-zero u32; zero is new-session sentinel only;
- new session starts at epoch 1;
- epoch bump is controller-committed compare-and-swap and increments exactly once;
- ambiguous InitProducer results may waste identity/epoch but never reuse/regress them;
- no P7 producer-session TTL/GC exists.

### Sequence and retry semantics

- Sequence is u32 and never wraps;
- new epoch starts at base sequence 0 per partition;
- exact expected next range is appendable;
- gaps, partial overlaps, changed-content retries and too-old retries fail explicitly;
- exact retry of one of the five retained committed ranges returns original offsets with no append;
- one unresolved idempotent batch per producer-partition is the P7 max-in-flight correctness bound;
- SHA-256 over canonical record-set bytes is the frozen retry-content fingerprint.

### Ack/storage/recovery boundary

- Produce V1 idempotency is quorum-only;
- Produce V1 requires explicit timestamps and does not use broker-generated `-1` timestamps;
- P1 Storage V1 already contains IDEMPOTENT, ProducerId, ProducerEpoch and base-sequence bytes, so
  no P1 format version changes;
- only committed frames advance durable producer authority;
- `producer.snapshot` is CRC-protected bounded derived state and is always rebuildable from
  committed history;
- ApplicationCommand V1 remains immutable; P7 adds ApplicationCommand V2.

### Controller versioning

- frozen ControllerCommandV1/SnapshotV1 bytes are not extended in place;
- P7 uses ControllerCommandV2 and ControllerSnapshotV2 for producer-session state;
- historical V1 commands/snapshots remain decodable;
- migration from V1 preserves the P5 allocator counter and begins with an empty session map.

### Public API and feature activation

- API 20 InitProducer V0 is frozen;
- Produce V1 is frozen while Produce V0 remains unchanged;
- feature key 11 remains `IDEMPOTENT_PRODUCER`;
- P7 requires active cluster feature level >=3;
- before activation, InitProducer remains unavailable and Produce advertises only V0.

### Bounds

- recent committed dedupe window: 5 batches per producer-partition;
- unresolved idempotent ranges: 1 per producer-partition;
- resident partition producer cache: 4,096 entries;
- controller producer-session logical cap: 1,000,000 records;
- producer snapshot: <=64 MiB;
- controller mutation must also honor the existing controller snapshot-size envelope.

## Checklist freeze

`P7_CHECKLIST.md` defines exactly **P7-001 through P7-155** across:

- P7-A contract/checklist freeze;
- P7-B types/codecs/API versions/goldens;
- P7-C controller producer-session authority + V2 persistence;
- P7-D pure partition producer state machine;
- P7-E Storage V1 metadata activation + ApplicationCommand V2;
- P7-F runtime admission/pending/dedupe;
- P7-G producer snapshot/recovery;
- P7-H broker/client InitProducer + Produce V1;
- P7-I real RF3 lost-response/failover/fencing qualification;
- P7-J deterministic models/resource/scheduling stress;
- P7-K compatibility audit + exact-HEAD closure.

Only **P7-001…P7-014** are checked by P7-A. P7-B and every later implementation/qualification item
remain open until the P7-A freeze commit exists.

## Compatibility conclusion

The frozen design requires no P1 byte redesign and no P2 legacy-body rewrite. It uses existing
wire/API versioning, the P5 feature-level mechanism, the P5 monotonic ProducerId allocator, P3/P4
commit authority and the pre-reserved P1 producer frame fields.

The only new persistent formats planned by P7 are explicitly versioned controller V2 records and
the derived ProducerSnapshotV1 sidecar; neither reinterprets frozen P1/P2/P3/P4/P5/P6 bytes.

## Later-phase boundary

P7 does not pull forward P8 batching/compression/performance, P9 physical retention/compaction,
P10 work-queue semantics, P11 security/operations, or P12 final chaos/benchmark closure.

## P7-A exit gate

P7-A is qualified when:

1. the three P7 planning documents and ADR-017 are committed together with canonical status
   updates;
2. the diff contains documentation only;
3. P7 checklist numbering is mechanically exact from 001 through 155;
4. only actions 001 through 014 are checked;
5. Markdown/whitespace hygiene is clean;
6. no P7 implementation code exists in the freeze commit.

After that commit, the next allowed checkpoint is **P7-B only — P7-015 through P7-029**.
