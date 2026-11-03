# AstraLog P9-A Contract / Storage V2 Architecture Freeze Report

Date: 2026-10-07

Status: **P9-A PRE-COMMIT QUALIFIED — FREEZE COMMIT / EXACT-HEAD AUTHORITY PENDING**

## 1. Scope

P9-A is specification, architecture resolution and execution decomposition only. It introduces no Rust
retention timer, cleaner, manifest-mode runtime, Storage V2 codec, controller retention metadata,
tombstone removal, producer checkpoint, compaction watermark or retained-history rebootstrap runtime.

The exact authoritative entering boundary is:

`37c07a5868950873de6aa250428e4f922a6552ec` — `P8 close batching compression flow control baseline`

That exact SHA carries the Git note `P8 exact-HEAD qualification: PASS`, including complete serialized
workspace **638/638 PASS**, frozen compatibility checks and the declaration that P8 is authoritatively
closed/frozen and P9 is the next authorized phase boundary.

P10-P12 remain disabled throughout P9-A.

## 2. Frozen P9-A artifacts

- `P9_RETENTION_COMPACTION_V1.md` — normative retention/compaction contract;
- `FORMAT_V2.md` — sparse immutable retained-history disk format layered alongside frozen V1;
- `P9_CHECKLIST.md` — complete P9-A through P9-I execution ledger;
- `ADR_019_P9_SPARSE_RETAINED_HISTORY.md` — architecture decision resolving the P8-batching/V1 sparse-offset and P4 rebootstrap blockers;
- this report — audit inputs, decisions, decomposition and qualification boundary;
- synchronized `README.md`, `CURRENT_STATE.md` and ADR index status only.

No P9 Rust/Cargo/runtime implementation belongs in this checkpoint.

## 3. Architecture inputs audited

P9-A was derived from the existing repository contracts rather than from a replacement design:

- `PHASE_PLAN.md` P9 goal/features/tests/exit gate;
- `ASTRALOG_BIBLE.md` retention, log-compaction, snapshot-floor and coordinated tombstone rules;
- `ARCHITECTURE_REVIEW.md` rejection of independent tombstone removal;
- `INVARIANTS.md` I-11 through I-13, I-15 through I-17, I-25 through I-32;
- `DECISIONS.md` ADR-013 copy-on-write immutable rolled segments and ADR-015 feature-level activation;
- `FORMAT_V1.md` frozen V1 bytes, contiguous record-offset rule and dormant generation-manifest publication primitive;
- the actual `astralog-storage` manifest, segment and PartitionLog implementation;
- `P4_REPLICATION_V1.md` plus RebootstrapPackage V1 contiguous-prefix contract;
- `P7_PRODUCER_V1.md`, producer snapshot/state implementation and the explicit P7/P9 recovery handoff;
- `P8_PERFORMANCE_V1.md` and the batching/compression behavior now present in Storage V1 frames;
- `PROTOCOL_V1.md` permanent API registry, Fetch log-start field, ListOffsets earliest selector and feature registry;
- controller feature-level machinery and P8 controller metadata versioning;
- exact P8 closure report/commit/note.

## 4. Freeze-time architecture blockers found

### 4.1 Batched V1 frames cannot represent sparse compacted survivors

P8 legitimately batches multiple records into one committed V1 frame. V1 payload schema 1 requires
`offset_delta == record ordinal`. If P9 removes an obsolete keyed record from the middle of that frame,
re-encoding the later survivors as V1 would change their stream offsets.

That would violate I-13 and the P9 exit gate.

Keeping the entire V1 batch whenever any member survives is safe but fails the intended keyed-compaction
capability; a single latest record could pin arbitrarily many obsolete same-batch records.

**Frozen resolution:** ADR-019 + `FORMAT_V2.md` define sparse immutable retained-history segments below
the committed P3 snapshot floor. Normal append and the live Raft suffix remain V1.

### 4.2 P4 rebootstrap V1 requires history P9 is allowed to remove

RebootstrapPackage V1 requires a contiguous Raft prefix beginning at index 1 and ending at the P3
snapshot boundary. Physical retention/compaction intentionally makes that prefix unavailable.

Synthesizing discarded Raft entries or weakening V1 validation would be incorrect.

**Frozen resolution:** P4 package V1 remains byte/semantic compatible. P9 adds a versioned retained-
history RebootstrapPackage V2 carrying P3 snapshot metadata, authoritative retained-history manifest/
files, P9 maintenance state and P7 recovery anchors, followed by the ordinary live V1 suffix.

## 5. Frozen authority model

P9 separates four boundaries that must never be conflated:

1. **P3 snapshot floor** — below it, old entries are no longer required for ordinary Raft matching/catch-up.
2. **P9 log start** — committed logical stream-retention floor; records below it are no longer visible.
3. **local clean-compacted offset** — non-authoritative replica progress proving older keyed values have been cleaned locally.
4. **committed compaction-safe offset** — failover-stable minimum across every current effective eligible voter, used by tombstone-removal safety.

Physical deletion/rewrite follows these authoritative boundaries; local file deletion never creates
cluster semantics by itself.

## 6. Frozen policy model

Controller Raft owns `TopicRetentionPolicyV1` with:

- CleanupPolicyV1 = Delete / Compact / CompactDelete;
- optional `retention_ms`;
- optional `retention_bytes`;
- positive finite `tombstone_grace_ms`, default 24 hours;
- disabled-by-default subscription-protection field reserved for the P9 protected-floor hook.

Historical topics derive infinite delete retention and no physical keyed compaction. Activating P9
therefore cannot silently delete existing topic history.

No internal topic is silently migrated to compacting policy by the feature-level bump.

## 7. Frozen compatibility/version boundary

- public feature key `15 = RETENTION_COMPACTION`;
- irreversible P9 behavior requires committed cluster feature level 5;
- before level 5, no V2 bytes, manifest adoption, P9 maintenance entries, P9 rebootstrap package or
  public retention mutation may be emitted;
- Storage V1 bytes and historical goldens remain exact;
- V2 is an explicit reader-before-writer format extension;
- controller metadata is additive after P8 V3 rather than rewriting prior encodings;
- AlterTopicConfig V0 becomes P9 retention mutation only after readiness;
- CreateTopic/DescribeTopic gain additive V2 bodies;
- Fetch/ListOffsets V0 bodies stay frozen and use their pre-existing log-start/earliest/error surfaces.

## 8. Frozen manifest/publication model

P9 activates the P1-qualified `segments.manifest` primitive as live partition inventory.

Legacy partitions are adopted into manifest generation 1 without rewriting existing V1 segment bytes.
After adoption, the manifest is sole authority; corruption/missing referenced files fail closed and
recovery never falls back to a directory scan.

Segment roll, retention and compaction serialize through one partition generation-publication boundary.
The P1 order remains authoritative: durable replacement bytes -> final names -> directory sync ->
durable atomic manifest switch -> directory sync -> unreachable-file reclamation.

## 9. Frozen logical retention semantics

The leader proposes a policy-valid `AdvanceLogStartOffset` maintenance command before physical delete
cleanup. The committed floor is monotonic, bounded by committed stream visibility, and survives
failover even when replicas have different physical cleanup progress.

Time retention uses conservative closed-segment age from durable successor segment creation, not
user-controlled event timestamps. Backward time movement retains data longer rather than deleting it
early.

Size retention is a reclamation target over authoritative log bytes, not permission to delete active,
snapshot-required or protected data.

Fetch below the logical start returns the existing OFFSET_OUT_OF_RANGE surface and reports the
authoritative log start. Compacted holes return the next surviving original offset at/after the request.

## 10. Frozen keyed-compaction/tombstone semantics

- non-null keys compact by greatest committed retained stream offset;
- null-key records have no compaction identity;
- surviving order and offsets are exact;
- active segment is never compacted;
- cleaner source rewrite is at/below the persisted snapshot floor;
- V2 sparse frames make internal batch holes explicit;
- latest tombstones remain until the coordinated removal gate passes.

A tombstone may be physically removed only after grace, snapshot safety and committed safe progress from
all current effective eligible voters. Missing/offline eligible-voter progress blocks advancement until
that voter catches up or P5 authority commits its exclusion.

## 11. Frozen P7 producer handoff

P7 requires accepted epoch, next sequence and the newest-five committed retry identities/results to
survive physical cleanup. The derived `producer.snapshot` cannot become authoritative merely because
old source frames are removed.

P9 therefore commits a bounded canonical `ProducerStateCheckpoint` through data-partition Raft before
any source idempotent history needed for reconstruction becomes reclaimable. Recovery after compaction
loads committed checkpoint history plus later committed V1 producer frames. Deleting `producer.snapshot`
must still reconstruct the same P7 authority and exact retry results.

## 12. Frozen resource/scheduling boundary

Cleaner jobs, queued partitions, source segments/bytes, decoded records, key-index memory, output bytes,
temp files, progress reports, reader-generation handles and rebootstrap package/file bytes all require
explicit finite bounds.

Cleaner execution is background data-plane maintenance. It receives bounded work/byte slices and may
not consume the reserved scheduling capacity needed for Raft, controller heartbeat/metadata, P6 group
control or health-critical progress.

## 13. P10-P12 boundary

P9 does not activate:

- QueueReceive/Ack/Nack, visibility leases, retry/backoff, delivery counts or DLQ transfer (P10);
- TLS/mTLS, authentication, ACL policy, full operational drain/rolling upgrade or production observability (P11);
- release-grade chaos certification or final benchmark publication (P12).

The P9 protected-retention hook accepts only a bounded abstract logical floor. It does not define or
persist a P10 subscription.

## 14. Execution decomposition

`P9_CHECKLIST.md` defines exactly **180 unique actions P9-001 through P9-180** across:

- P9-A — contract, Storage V2 and execution freeze: P9-001..020;
- P9-B — controller metadata, feature activation and public codecs: P9-021..043;
- P9-C — Storage V2 and manifest-governed live inventory: P9-044..066;
- P9-D — replicated log start and time/size delete retention: P9-067..087;
- P9-E — COW keyed compaction and sparse reads: P9-088..111;
- P9-F — producer-state checkpoint and P7 recovery preservation: P9-112..129;
- P9-G — coordinated tombstone safety and retained-history rebootstrap: P9-130..151;
- P9-H — bounded cleaner/throttling/protected floor/pressure: P9-152..167;
- P9-I — failure/model/compatibility audit and exact closure: P9-168..180.

Only P9-001..020 are checked by the documentation-only P9-A freeze. Every phase-exit invariant and
P9-021..180 remain unchecked until their stated proof exists.

## 15. P9-A qualification gate

P9-A may be committed only after the candidate tree proves:

1. intended delta is documentation-only;
2. checklist IDs are exactly P9-001..P9-180, unique and contiguous;
3. exactly P9-001..P9-020 are checked and P9-021..180 are unchecked;
4. all P9 phase-exit invariants remain unchecked;
5. no Rust/Cargo/dependency or frozen P0-P8 implementation/contract artifact is changed;
6. no P10-P12 runtime scope is introduced;
7. `git diff --check` and `cargo fmt --all -- --check` pass;
8. workspace Clippy `--all-targets --all-features -D warnings` and doc tests pass;
9. the complete serialized all-target workspace regression remains green;
10. after the P9-A commit, its exact clean HEAD repeats the same qualification and receives a P9-A PASS Git note.

## 16. Pre-commit qualification evidence

The final documentation-only candidate passed the bounded pre-commit gate on 2026-10-07:

- intended changed paths are documentation only: README/status/ADR index plus the five new P9-A documents;
- `P9_CHECKLIST.md` contains exactly **180 unique contiguous IDs P9-001..P9-180**;
- exactly **P9-001..P9-020** are checked; **160** later actions remain unchecked;
- checked P9 phase-exit invariants: **0**;
- `git diff --check`: PASS;
- `cargo fmt --all -- --check`: PASS;
- `cargo clippy --workspace --all-targets --all-features -- -D warnings`: PASS;
- `cargo test --workspace --doc`: PASS;
- complete serialized `cargo test --workspace --all-targets -- --test-threads=1`: **638/638 PASS**.

Serialized component evidence:

- benchmark tooling: **20/20**;
- broker units: **37/37**;
- P2 network: **9/9**;
- P4 RF3: **1/1**;
- P5/P8 dynamic integration: **5/5**;
- P6 routing/group/offset/fencing/churn/public integration: **9/9** serialized;
- P7 real RF3 failover: **12/12** serialized;
- P7 runtime: **2/2**;
- client: **24/24**;
- common: **3/3**;
- controller: **113/113**;
- groups: **55/55**;
- protocol: **45/45**;
- original P2/P6/P7 protocol goldens: **11/11 + 6/6 + 4/4**;
- Raft: **128/128**;
- replication: **101/101**;
- Storage: **53/53**.

This evidence permits one documentation-only P9-A freeze commit. It does **not** make P9-A
authoritative by itself. The exact clean commit SHA must independently repeat the gate and receive the
P9-A exact-HEAD PASS Git note.

After exact-HEAD P9-A authority exists, the only authorized implementation checkpoint is:

> **P9-B — P9-021 through P9-043: controller retention metadata, feature level 5 and public config codecs**

P9-C and later remain disabled until P9-B independently qualifies. P10-P12 remain disabled.
