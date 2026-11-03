# AstraLog P7-G Producer Snapshot / Crash Recovery Qualification

Date: 2026-10-03

Status: **PASS — P7-G QUALIFIED / P7-H NEXT**

## Scope

P7-G closes actions **P7-094 through P7-106** only. It adds the derived per-partition
`producer.snapshot` acceleration/recovery surface. It does not advertise or dispatch InitProducer,
does not dispatch Produce V1, does not add client producer sessions/retries, and does not perform
the RF3 lost-response/failover campaign owned by P7-I.

The authority rule remains unchanged:

> committed P1/P4 history is authoritative; `producer.snapshot` is rebuildable derived state.

Deleting, corrupting or rejecting the snapshot can increase recovery work but cannot change the
accepted ProducerEpoch, next sequence or committed retry result.

## ProducerSnapshotV1

The canonical V1 sidecar freezes:

- magic `ASTRPSN1` and version 1;
- exact TopicId and PartitionId;
- committed Raft index through which the sidecar is valid;
- strictly sorted ProducerId entries;
- accepted ProducerEpoch and next sequence;
- one through five retained `RecentBatchV1` records per producer;
- original stream-offset range, Raft index and canonical digest for every retained retry;
- reserved-zero fields and a trailing CRC32C.

Decoded state is not trusted as a bag of fields. `PartitionProducerStateV1::from_snapshot_parts`
revalidates retained sequence ranges, stream-offset ranges, monotonic Raft indexes, producer-local
sequence continuity and the final next-sequence relationship before the state can be published.

## Resource bounds

The encoded snapshot has the frozen hard maximum of **64 MiB**.

Both encode and decode reject an over-limit byte slice. More importantly, the production store
checks the filesystem metadata length before calling `fs::read`; a sparse **64 MiB + 1 byte** file
is mechanically rejected before a proportional allocation occurs. Declared producer counts are
also checked against the actual encoded entry-byte envelope before entries are parsed, so an
untrusted count cannot directly drive allocation.

Committed suffix recovery is paged. P7-G adds a read-only P1 helper that returns at most a caller
specified number of existing Storage V1 frames. Producer recovery fixes that page size at **4
frames** and decodes each physical frame under the already-frozen Storage V1 `max_frame_bytes`
limit. The qualification snapshot is taken at Raft index 2 and the suffix contains six committed
frames through index 8, mechanically forcing more than one recovery page.

This is a bounded-memory recovery contract, not a performance benchmark. The current page helper
may rescan earlier segment positions between pages; P7-G makes no throughput/latency claim.

## Crash-safe derived-state publication

`ProducerSnapshotStore` writes `producer.snapshot.tmp`, writes the complete canonical bytes,
durably syncs the file, atomically renames it to `producer.snapshot`, then invokes AstraLog's
parent-directory durability hook. On Unix the hook explicitly fsyncs the directory; other
platforms retain the project's existing portability behavior rather than claiming a stronger
filesystem guarantee than the runtime provides.

Injected interruptions prove the recoverable publication states:

- stop after temporary-file write -> old complete snapshot remains authoritative;
- stop after temporary-file sync -> old complete snapshot remains authoritative;
- stop after rename -> complete new snapshot is readable;
- no partial snapshot becomes producer authority.

## Snapshot validation and fallback

Snapshot-aware producer recovery validates the derived sidecar against the current partition
identity and committed P3/P1 boundary. The following cases all recover from committed P1 history
without rewriting that history:

- snapshot missing;
- malformed/truncated snapshot;
- CRC mismatch;
- noncanonical producer ordering/count/state;
- wrong TopicId or PartitionId;
- snapshot boundary newer than the current committed Raft index;
- snapshot byte limit violation.

Filesystem I/O failures are not silently classified as corruption; unexpected I/O errors remain
loud failures.

## Snapshot plus suffix recovery

`PartitionPersistence::recover_committed_producer_states()` now:

1. loads a valid snapshot only when identity matches and its boundary is not ahead of commit;
2. restores its validated producer states;
3. replays committed frames beginning at `snapshot_boundary + 1` in bounded four-frame pages;
4. uses the same `apply_committed_producer_frame` transition helper as the full-history oracle;
5. falls back to boundary zero when the derived snapshot is missing/rebuildably invalid.

The qualification scenario requires snapshot+suffix recovery, missing-snapshot recovery,
corrupt-snapshot recovery, wrong-identity recovery and ahead-of-commit recovery all to equal the
same full committed-history oracle map.

## Retry and runtime-cache preservation

After a committed idempotent batch, qualification persists a producer snapshot, removes that
ProducerId from the P7-F runtime cache and reconstructs it through the normal cache-miss path. The
recovered `PartitionProducerStateV1` is exactly equal to the pre-eviction state, and retrying the
same sequence/digest returns `Duplicate` with the original base offset, last offset and Raft index.
No new proposal is required to restore retry identity.

## Compatibility boundary

P7-G changes no Storage V1 bytes, frame fields, index formats or retention behavior. The only P1
addition is a read-only bounded frame-page helper built from the existing decoder. P7-G adds no
broker/client public P7 surface and no new controller authority.

The full compatibility gate requalifies P2/P4/P5/P6 and all frozen protocol goldens.

## Mechanical qualification

- focused P7-G snapshot/recovery qualification: **7/7 PASS**
- broker units: **19/19 PASS**
- P2 network: **9/9 PASS**
- P4 RF3: **1/1 PASS**
- P5 dynamic integration: **4/4 PASS**
- P6 RF3: **9/9 PASS**
- P7-F internal runtime integration: **1/1 PASS**
- client: **10/10 PASS**
- common: **3/3 PASS**
- controller: **104/104 PASS**
- groups: **55/55 PASS**
- protocol: **44/44 PASS**
- P2/P6/P7 goldens: **11/11 + 6/6 + 4/4 PASS**
- Raft: **122/122 PASS**
- replication: **73/73 PASS**
- storage: **44/44 PASS**
- complete serialized all-target workspace: **519/519 PASS**
- workspace doc tests: **PASS**
- workspace Clippy `--all-targets --all-features -D warnings`: **PASS**
- formatting and `git diff --check`: **PASS**

## Boundary

P7-G is qualified. The next clean checkpoint is:

> **P7-H only — P7-107 through P7-121: Broker/client InitProducer and Produce V1 integration**

P7-I and later remain disabled until P7-H independently qualifies.
