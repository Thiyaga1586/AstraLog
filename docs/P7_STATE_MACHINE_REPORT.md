# P7-D Pure Partition Producer State Machine Qualification

Date: 2026-10-03

Status: **PASS — P7-D QUALIFIED / P7-E NEXT**

## Scope

P7-D closes checklist actions **P7-046 through P7-061** only. It introduces the pure deterministic per-producer/per-partition epoch, sequence and recent committed retry state machine. It does not activate P1 producer bytes, ApplicationCommandV2 persistence, broker Produce V1, producer snapshots, client idempotent production or any P7-E+ runtime path.

The implementation is intentionally isolated in `astralog-replication::producer_state` and is not called by replication runtime/storage code yet.

## Frozen state model

`PartitionProducerStateV1` contains exactly the committed logical authority needed by P7:

- ProducerId,
- accepted ProducerEpoch,
- exact next ProducerSequence,
- at most five newest `RecentBatchV1` records.

Each recent batch retains the exact sequence range, record count, committed stream-offset range, committed Raft index and frozen 32-byte canonical record-set digest.

Admission is side-effect free. `prepare_initial` handles a controller-confirmed unseen producer/epoch and requires sequence zero. `admit` handles existing state and returns either an append candidate, an exact committed duplicate carrying the original offsets/Raft index, or an explicit fencing/order error. **Only `apply_committed` mutates producer state.**

## Sequence and retry behavior

The qualified transition rules are:

- unseen/newer epoch first batch requires `base_sequence = 0`;
- equal epoch accepts a new append only at exact `next_sequence`;
- a sequence gap is rejected;
- a partial overlap is rejected;
- an exact retained range plus exact digest returns the original committed result without another append;
- same retained range with changed digest is rejected;
- a retry older than the five-batch retained window is rejected rather than guessed;
- sequence arithmetic and stream-offset arithmetic are checked and never wrap.

The sixth committed batch evicts exactly the oldest retry record, leaving five newest entries.

## Epoch fencing

A lower epoch is rejected as `ProducerFenced` at admission once a newer epoch is committed authority. A higher controller-confirmed epoch creates only an append candidate; it does not change state. The accepted epoch, next sequence and recent-window reset occur only when that epoch's first sequence-zero batch is applied as committed history.

Committed history containing an epoch regression, sequence gap, non-zero first range for a newer epoch, conflicting retained metadata or unverifiable old/partial replay fails closed with `InvalidCommittedHistory` rather than attempting self-repair.

## Committed replay

Applying identical retained committed metadata twice returns `AlreadyApplied` and leaves state byte-for-byte unchanged. A conflicting replay of the same sequence range is rejected. This keeps ordinary committed replay deterministic without expanding the retry window beyond the frozen five batches.

## I-15 / I-16 / I-17 model proof

The exhaustive small-state oracle iterates accepted epochs, committed prefixes, presented epochs, base sequences, record counts and matching/non-matching digests. For every generated admission it independently computes the frozen expected class and requires the implementation to match:

- I-15: supported exact retained retry -> duplicate/original result, never append;
- I-16: lower accepted epoch -> fenced;
- I-17: gaps, partial overlaps, changed content and unsupported old retries -> explicit out-of-order rejection.

Every admission is also required to leave committed state unchanged.

## Canonical digest boundary

P7-D consumes the P7-B digest as opaque `[u8; 32]` state metadata and does not duplicate hashing logic. The frozen `canonical_record_set_digest` implementation was requalified directly, and the immutable P7 SHA-256/body golden suite remains green. Digest content changes therefore continue to alter retry identity while identical canonical record bytes produce identical digest bytes.

## Mechanical qualification

- focused `producer_state` tests: **7/7 PASS**
- full `astralog-replication`: **56/56 PASS**
- canonical Produce V1 digest test: **1/1 PASS**
- P7 golden vectors: **4/4 PASS**
- workspace formatting: **PASS**
- `git diff --check`: **PASS**
- workspace Clippy `--all-targets --all-features -D warnings`: **PASS**
- broker units: **19/19 PASS**
- P2 network: **9/9 PASS**
- P4 RF3: **1/1 PASS**
- P5 dynamic: **4/4 PASS**
- P6 RF3: **9/9 PASS**
- client: **10/10 PASS**
- common: **3/3 PASS**
- controller: **104/104 PASS**
- groups: **55/55 PASS**
- protocol: **44/44 PASS**
- original P2 goldens: **11/11 PASS**
- P6 goldens: **6/6 PASS**
- P7 goldens: **4/4 PASS**
- Raft: **122/122 PASS**
- replication: **56/56 PASS**
- storage: **43/43 PASS**
- doc tests: **PASS**
- complete serialized workspace: **500/500 PASS**

## Boundary

P7-E is the next allowed checkpoint. It may activate the already-frozen producer metadata fields in Storage V1 and integrate `ApplicationCommandV2` with committed P4 persistence/recovery. It must preserve ApplicationCommandV1/non-idempotent bytes and must not begin P7-F runtime admission/deduplication.

P7-F and later remain disabled until P7-E independently qualifies.
