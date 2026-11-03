# ADR-019 — Sparse Retained-History Storage V2 for P9 Compaction

Date: 2026-10-07

Status: **Accepted / frozen by P9-A**

## Context

Storage V1 was correctly frozen for append/recovery before P8 batching existed. Its data payload requires
every record's `offset_delta` to equal its ordinal inside the batch. After P8, one committed frame can
contain many records with unrelated keys.

P9 keyed compaction must be able to remove an obsolete record from the middle of such a batch while
preserving the original stream offsets of later survivors. Re-encoding those survivors as an ordinary
V1 batch would renumber them and violate I-13. Retaining the entire batch whenever any record survives
would avoid corruption but would not provide production-grade keyed compaction.

P4 RebootstrapPackage V1 creates a second incompatibility: it requires a contiguous Raft prefix from
index 1 through the snapshot boundary. P9 physical retention/compaction intentionally makes such a
prefix unavailable.

## Decision

P9 introduces an explicit retained-history **Storage V2** while keeping ordinary append and the live
Raft suffix byte-exact Storage V1.

V2 is allowed only for immutable copy-on-write retained-history segments whose source entries are
committed and at/below the persisted P3 snapshot floor. Its sparse data schema records explicit original
stream-offset deltas, allowing holes without renumbering survivors. V2 retained-history frames are never
reconstructed into the live P3 Raft log.

P9 also introduces a versioned retained-history-aware RebootstrapPackage V2. P4 package V1 stays
unchanged and remains usable when its contiguous prefix still exists.

Manifest-governed inventory is the publication boundary: after P9 activation, one authoritative
`segments.manifest` may select immutable V1 and V2 retained-history files plus exactly one mutable V1
active segment.

## Rejected alternatives

### Rewrite compacted survivors as V1 batches

Rejected because V1's ordinal offset rule would renumber stream offsets after an internal record is
removed.

### Compact only whole batches

Rejected because one surviving record could pin every obsolete keyed record in a large batch forever.
That is safe but does not meet the P9 keyed-compaction goal.

### Encode sparse consumer records as V1 control frames

Rejected because frozen V1 control frames explicitly consume no consumer-visible stream offsets and are
not Fetch data. Reinterpreting them would silently change the V1 contract.

### Modify V1 record or frame semantics in place

Rejected because historical V1 bytes/goldens are frozen. In-place reinterpretation would violate the
project's compatibility doctrine and I-28.

### Keep P4 rebootstrap V1 by synthesizing missing old Raft entries

Rejected because discarded committed history must not be invented merely to satisfy an old package
shape. Snapshot state plus retained stream history and the live suffix are distinct domains.

## Correctness impact

- surviving stream offsets remain stable;
- compacted holes become explicit rather than renumbered;
- live P3 log matching/catch-up remains on contiguous V1 suffix bytes;
- V2 cannot authorize commit, voting, producer fencing or acknowledgements;
- P7 producer authority is preserved separately by committed P9 producer-state checkpoints before
  reconstructive source frames are reclaimed;
- crash publication remains the P1 copy-on-write manifest protocol.

## Performance impact

Compaction may rewrite records into a wider sparse record representation and requires a bounded key
index. This is background maintenance and is intentionally subordinate to correctness/control progress.
The design avoids forcing sparse-format overhead onto the normal append path.

## Wire/disk compatibility impact

- every historical Storage V1 byte/golden remains unchanged;
- V2 has an explicit segment/frame version and magic/schema dispatch;
- level-5 feature activation is reader-before-writer gating for irreversible V2 output;
- P4 RebootstrapPackage V1 remains decodable; P9 adds V2 rather than changing V1 bytes;
- public Fetch/ListOffsets V0 bodies remain unchanged.

## Migration strategy

Historical partitions remain no-manifest V1 until P9 feature level 5 is active. The first P9 physical
maintenance operation atomically adopts validated existing V1 files into manifest generation 1 without
rewriting them. Later copy-on-write generations may add immutable V2 retained-history files.

## Rollback strategy

Before any V2 file or P9 maintenance state is committed, the feature remains reversible by leaving
level 5 inactive. After irreversible level-5 P9 writes exist, an older binary that lacks P9 support must
not join as an active eligible node; the existing feature-level compatibility mechanism rejects it.

## Required proof updates

- new V2 immutable golden vectors;
- historical V1 golden source/hash audit;
- sparse same-batch/multi-segment compaction tests;
- no-renumber property/model tests;
- P3 recovery proof that V2 retained history is excluded from live Raft reconstruction;
- manifest crash-stage matrix;
- producer-state recovery after source-frame cleanup;
- retained-history RebootstrapPackage V2 install/failover qualification;
- exact-head P9 closure compatibility regression.
