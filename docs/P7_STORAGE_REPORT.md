# P7-E Storage V1 Producer Metadata / ApplicationCommand V2 Qualification

Date: 2026-10-03

Status: **PASS — P7-E QUALIFIED / P7-F NEXT**

This report closes actions **P7-062 through P7-076** only. It activates the
already-frozen Storage V1 producer metadata fields for `ApplicationCommandV2`
and qualifies committed producer-state reconstruction through the P4/P1
persistence boundary. It does **not** add idempotent proposal admission,
pending-range suppression, public broker InitProducer/Produce V1 dispatch,
producer snapshots, or client-side idempotent production.

## Storage V1 activation without a format change

P7-E does not create Storage V2 or reinterpret legacy bytes. The existing
Storage V1 data-frame idempotent attribute and producer header fields are used
exactly as frozen:

- ProducerId;
- ProducerEpoch;
- base ProducerSequence.

`ApplicationCommandV1` still materializes `producer=None`. An
`ApplicationCommandV2` materializes a Storage V1 `ProducerIdentity`. Recovery
performs the inverse mapping: a frame without producer metadata reconstructs a
V1 application command, while a frame with producer metadata reconstructs V2.
Mixed V1/V2 history therefore remains version-separated while sharing one
contiguous stream-offset space.

Storage V1 now validates the already-reserved producer fields when the
idempotent attribute is present. ProducerId zero, ProducerEpoch zero and a
sequence range whose `base_sequence + record_count` would overflow fail closed
on both encode/decode. No field widths, header offsets, flags, CRC layout or
golden bytes changed.

## Committed producer-state recovery

`PartitionPersistence::recover_committed_producer_states()` is the frozen P7-E
recovery/P9 handoff surface. For the current topic-partition it returns a
canonical `BTreeMap<ProducerId, PartitionProducerStateV1>` reconstructed only
from Storage V1 data frames whose Raft index is at or below the durable committed
Raft boundary.

The surface reconstructs:

- committed accepted ProducerEpoch;
- exact next ProducerSequence;
- the newest five committed retry records, including original stream offsets,
  Raft indexes and canonical record-set digests.

Uncommitted idempotent tail frames are ignored. Conflicting committed epoch or
sequence history is rejected by the already-qualified P7-D state machine rather
than repaired or guessed.

P7-E deliberately does not implement P9 retention/compaction or a producer
snapshot. Later acceleration may consume this logical surface, but cannot become
a competing producer authority.

## Canonical digest reconstruction and dependency direction

The architecture rule that lower layers do not import higher layers remains
intact; `astralog-replication` does not add a dependency on
`astralog-protocol`. Recovery therefore contains a private lower-layer
re-encoder for the already-frozen P7-B canonical record-set byte layout and
SHA-256 algorithm.

This helper is not a new identity definition. Its qualification binds it to the
immutable P7 public digest golden:

`090e5db72735ab045bd82af0dc1fc927465bced52a5e59e617559c3b7966af4a`

Any drift between persisted-record reconstruction and the frozen public digest
therefore fails the replication test suite.

## Failure and recovery qualification

The P7-E focused storage/replication tests prove:

- mixed `ApplicationCommandV1` and V2 entries survive materialization/reopen
  without version reinterpretation;
- legacy and idempotent batches consume contiguous stream offsets `0,1,2,3`;
- identical V2 entries independently materialized on two replica stores produce
  equal Storage V1 frames and equal recovered producer state;
- a committed sequence-zero batch plus an uncommitted next batch reconstructs
  only the committed authority;
- truncating that uncommitted V2 suffix and replacing it removes the tentative
  producer metadata and recovers from the replacement after commit;
- ordinary restart restores exact epoch/sequence state, including a committed
  higher-epoch reset;
- P4 rebootstrap/snapshot installation transfers committed producer metadata,
  reconstructs the same state on the target, and permits the next V2 suffix at
  the exact subsequent sequence;
- a producer-field bit flip is detected by the existing Storage V1 frame CRC;
- even with a recomputed valid CRC, zero ProducerId, zero ProducerEpoch and
  wrapping base sequence are rejected structurally.

The existing Storage V1 idempotent fixture remains exact, and the original
non-idempotent Storage V1 goldens remain green.

## Compatibility qualification

Finished-source qualification is clean:

- `cargo fmt --all -- --check`: **PASS**
- `git diff --check`: **PASS**
- workspace Clippy `--all-targets --all-features -D warnings`: **PASS**
- Storage: **44/44 PASS**
- Replication: **62/62 PASS**
- original P2 protocol goldens: **11/11 PASS**
- P6 protocol goldens: **6/6 PASS**
- P7 protocol goldens: **4/4 PASS**
- P2 network: **9/9 PASS**
- P4 RF3: **1/1 PASS**
- P5 dynamic: **4/4 PASS**
- P6 RF3: **9/9 PASS**
- Controller: **104/104 PASS**
- Raft: **122/122 PASS**
- complete serialized all-target workspace: **507/507 PASS**
- doc tests: **PASS**

One earlier full-workspace attempt observed a transient `NotLeader` while the
pre-existing P6 silent-member test performed its initial JoinSubscribe, before
the test reached any P7-E storage path. The exact unchanged P6 case then passed
**1/1** in isolation, and the clean final serialized workspace run passed the
entire P6 RF3 suite **9/9**. No production code or P6 harness behavior was
changed for that observation.

## Boundary

P7-E is closed. The next clean checkpoint is:

> **P7-F only — P7-077 through P7-093: runtime admission, pending proposal and duplicate suppression**

P7-F may bind the already-qualified controller authority, committed P7-E
recovery surface and P7-D state machine to P4 proposal admission. P7-G producer
snapshots, broker/client public P7 activation and all later P7 work remain
disabled until their own checkpoints.
