# P9-F Producer-State Checkpoint and Compaction-Safe P7 Recovery Qualification

Status: **P9-F CLOSURE EVIDENCE — AUTHORITY REQUIRES AN EXACT-HEAD PASS GIT NOTE**

Authoritative parent: `9da9aac850344a1a3ee4412bdb0026c409689800` (P9-E exact-HEAD PASS).

Scope: **P9-112..P9-129 only**. P9-G compaction-safe watermarks, tombstone removal,
RebootstrapPackage V2, and P9-H cleaner scheduling remain disabled; P10-P12 remain gated.

## 1. Authority and command compatibility

P9-F adds version-3 application-maintenance kind 2, `ProducerStateCheckpoint`, to the
existing Raft-ordered P9 maintenance path. Its body is the exact canonical bounded P7
`ProducerSnapshotV1` encoding, including TopicId, PartitionId, the covered committed
Raft boundary, and the canonical per-producer epoch, next sequence and newest-five
committed retry entries. These entries retain original base/last stream offsets,
Raft index and canonical digest; no new producer-sequence semantics are introduced.

The encoder and decoder enforce the P7 partition maximum of **4,096** producer entries,
the existing producer-snapshot encoding limit, the application command byte limit,
the Storage control-frame ceiling, canonical round-trip encoding, and committed
covered-boundary validation. The checkpoint is a control entry and consumes **no**
stream offset. Historical P7 application bodies, Storage V1 frame bytes and protocol
goldens are not edited.

## 2. Commit-before-reclaim and snapshot-floor proof

`BrokerState::run_keyed_compaction_once` first asks the current leader to propose a
producer-state checkpoint. The partition actor waits for the **quorum-committed**
proposal outcome; successful local durability alone is insufficient. Only then does
the existing leader-fenced P3 snapshot path establish the cleaning floor.

The P9-E cleaner source gate is extended with a `ProducerCheckpointFence` capturing
the control's committed Raft index and the checkpoint's covered boundary. A source
carrying idempotent P7 metadata may be rewritten only when its maximum producer
Raft index is covered, the control is committed, and the control itself is at/below
the persisted snapshot floor. A missing or insufficient checkpoint preserves the
P9-E conservative refusal to compact producer-bearing source files. The checkpoint
and its required retained control anchor survive COW replacement; publication uses
the already-frozen P1 generation transaction and reader ownership.

The RF1 regression explicitly verifies three states: (1) persisted snapshot without
a checkpoint cannot reclaim producer source frames; (2) committed checkpoint alone
cannot reclaim them before the snapshot floor covers it; (3) a subsequent snapshot
covering the checkpoint enables compaction without changing the committed high
watermark.

## 3. Recovery and sidecar independence

Replica recovery locates the latest valid **retained committed** checkpoint and
replays only later committed V1 idempotent frames. The local `producer.snapshot`
sidecar remains derived acceleration, not post-compaction authority. Missing,
invalid or stale derived sidecar input does not authorize discarding canonical
retained producer state. Recovery validates checkpoint partition identity,
checkpoint/control Raft ordering, producer-map bounds and command limits.

The RF1 restart regression deletes `producer.snapshot` after COW compaction and
reopens the partition. It checks restored P7 producer state, identical newest-five
retry decisions, exact duplicate response with the **original** offset/Raft range,
old-epoch fencing, and unchanged rejections of changed content, overlaps, gaps,
and too-old retry sequences. The compaction does not create an additional stream
append. The 4,096-producer boundary test independently reconstructs all entries
from a retained checkpoint after removing the sidecar and rewriting the original
producer-bearing source.

## 4. RF3 failover proof and bounded readiness

The real three-controller/three-broker RF3 test creates a compacted keyed source
carrying an idempotent producer record, commits checkpoint-backed compaction,
stops the compacting partition leader, elects a successor, waits for the successor
actor's authoritative local leadership observation, then submits the *same*
idempotent batch to the successor. The response must exactly match the first
committed base offset, last offset, and Raft index; a second append is forbidden.

The RF3 proof deliberately does not run physical cleanup on a successor while a
voter is offline: crossing that retained-history rejoin boundary requires the
P9-G RebootstrapPackage V2 mechanism, which is not activated in P9-F. The
previously interrupted RF3 investigation was resumed on the unchanged dirty tree;
the recovered test passed six successive individual runs before broad qualification.

## 5. Resource and compatibility boundaries

Producer checkpoint creation fails closed when the canonical producer map or
encoded command cannot fit its frozen bounds. The many-producer case exercises
the complete P7 per-partition 4,096-entry ceiling, then proves recovered producer
state after checkpoint-backed compaction and restart. Ordinary keyed compaction
still retains tombstones; P9-G safe watermark authority, tombstone grace removal,
V2 retained-history rebootstrap, and automatic background cleaning are absent.

## 6. Qualification evidence

The final documented implementation candidate passes:

- complete serialized workspace: **688/688 PASS**;
- combined P9-focused workspace: **49/49 PASS**;
- P9-F RF3 post-failover exact retry, six sequential recovered-tree runs: **6/6 PASS**;
- dynamic broker integration: **8/8 PASS**;
- P7 failover/runtime: **12/12 + 2/2 PASS**;
- controller: **122/122 PASS**;
- Raft: **128/128 PASS**;
- replication: **108/108 PASS**;
- Storage: **78/78 PASS**;
- `cargo fmt --all -- --check`: **PASS**;
- `cargo clippy --workspace --all-targets --all-features -- -D warnings`: **PASS**;
- `cargo test --workspace --doc`: **PASS**;
- `git diff --check`: **PASS**;
- diff audit of protocol/golden/P3/Raft/client source baselines: **no modified sources**.

Qualification uses `CARGO_BUILD_JOBS=1` and a process-local D: temporary directory
to respect the existing Windows host resource bounds; there is no change to
product semantics or any test assertion for resource management.

A P9-F closure commit is authoritative only after mandatory gates pass on its
**exact committed SHA** and it carries an exact-HEAD PASS Git note. No
pre-commit run substitutes for that note.

## 7. Next authorized boundary

Only after exact-HEAD P9-F closure may **P9-G (P9-130..151)** begin. P9-H/I and
P10-P12 remain disabled behind their independent qualification gates.
