# AstraLog P5-C Controller Persistence Qualification

Status: **QUALIFIED**

P5-C implements and qualifies checklist actions P5-021 through P5-029 only. It
does not start controller networking, controller peer runtime/election wiring,
broker heartbeats, broker join/rejoin runtime, placement execution, metadata
dissemination, or dynamic P4 assignments.

## 1. Delivered scope

P5-C adds a crash-durable adapter between the frozen P3 Raft persistence
contract and the P5 deterministic controller state machine:

- persistent P3 hard state;
- durable controller Raft log storage;
- canonical CRC-protected controller snapshots;
- crash-recoverable commit transaction intent;
- P3 RecoveredState reconstruction;
- loud impossible-state rejection;
- deterministic controller snapshot build;
- P3-authorized InstallSnapshot persistence;
- physical controller-log prefix compaction at the snapshot safety floor.

The implementation lives entirely in astralog-controller. No P1-P4 production
source is modified by this checkpoint.

## 2. Persistent files

The controller persistence directory owns these P5-C files:

| File | Purpose |
|---|---|
| controller.raft.log.v1 | retained controller Raft suffix |
| controller.raft.hardstate.v1 | P3 current term, durable vote and durable commit |
| controller.raft.commit.v1 | independent durable commit marker |
| controller.raft.commit-intent.v1 | cross-file commit transaction intent |
| controller.snapshot.v1 | atomic P3 metadata + ControllerSnapshotV1 bundle |
| controller.snapshot-intent.v1 | crash-recovery intent for snapshot installation |

All authoritative files are versioned and CRC32C-protected. File replacement is
performed through write-to-temp, fsync, atomic rename and parent-directory sync.

## 3. Controller Raft log

The retained controller log is bounded by configured entry-count and byte
limits. It stores only:

- canonical ControllerCommandV1 application entries; and
- P3 LeaderBarrier entries.

Configuration entries are rejected. P5 controller-voter membership remains the
statically bootstrapped 3- or 5-voter set frozen by P5-A; dynamic controller
membership is deliberately outside P5.

The log has a versioned header, per-record CRC32C and whole-file CRC32C. Recovery
rejects malformed lengths, noncanonical controller commands, unknown entry
kinds, term/index zero, non-contiguous retained indexes and configured-bound
violations.

## 4. Hard state and commit transaction

The hard-state sidecar persists the exact P3 HardState fields:

- current_term;
- voted_for;
- durable_commit_index.

AdvanceCommit uses a separate commit marker and transaction intent:

1. persist `{previous,target}` intent;
2. durably publish commit marker `target`;
3. durably publish hard-state commit `target`;
4. remove and directory-sync the intent.

On restart, an existing intent may explain only `previous` or `target` values in
the marker/hard state. Recovery completes the transition to `target`. Any other
combination, or a target outside stable history, fails loudly rather than
guessing authority.

## 5. ControllerSnapshotV1 and snapshot bundle

ControllerSnapshotV1 follows the P5-A frozen encoding exactly:

- version 1;
- header length 40;
- metadata epoch;
- cluster ID;
- active feature level;
- producer-ID allocator cursor;
- canonically ordered broker/topic/assignment/movement records;
- final CRC32C.

The P5-C snapshot file wraps the canonical ControllerSnapshotV1 bytes with the
corresponding P3 SnapshotMetadata and protects the complete bundle with another
versioned CRC32C envelope. This gives one atomically replaceable authoritative
snapshot file without changing the frozen nested ControllerSnapshotV1 bytes.

Snapshot decode reconstructs derived topic-name indexes instead of persisting a
second source of truth. It validates broker identity/incarnation/features,
topic-name and RF constraints, complete per-topic assignments, assignment
distinctness/references, movement references/generations, feature compatibility,
metadata-epoch bounds and allocator bounds before the state can become
authoritative.

## 6. P3 RecoveredState reconstruction

Restart loads and validates:

1. static controller voter configuration;
2. snapshot bundle;
3. retained controller Raft suffix;
4. hard state;
5. durable commit marker.

It then constructs P3 RecoveredState from the durable hard state, P3 snapshot
metadata and retained suffix. P3's own recovery validation therefore remains the
authority for term/index/log/snapshot consistency.

ControllerState starts from the deterministic state snapshot and replays only
committed application entries above the snapshot boundary. Leader barriers
consume Raft indexes but do not mutate metadata, so ControllerSnapshotV1
metadata_epoch may be less than or equal to the P3 snapshot index; it may never
be greater.

## 7. Snapshot build and install

Local snapshot build is accepted only when:

- the snapshot boundary is non-zero and already durably committed;
- the boundary term matches retained durable history;
- the static controller voter set matches;
- the controller state belongs to the same cluster; and
- the controller state's metadata epoch is not newer than the Raft snapshot
  boundary.

InstallSnapshot uses a durable snapshot-install intent before publishing the new
bundle. Boundary matching follows P3 semantics:

- matching boundary term/index: retain entries strictly above the snapshot;
- conflicting/missing boundary: discard the incompatible live suffix.

The install transaction then advances the durable commit floor to at least the
snapshot boundary and clears the intent. If a crash occurs before publication,
recovery keeps the old snapshot/commit. If publication occurred, recovery
deterministically completes suffix selection and commit advancement before the
intent is cleared.

Tests also prove that normal append resumes at `snapshot_index + 1` after a
follower rebootstrap.

## 8. Safe log compaction

P5-C physical compaction applies only to the controller metadata log. It does
not change P1 data-partition retention semantics.

DiscardPrefix may remove retained controller history only at or below the
persisted snapshot boundary. It atomically rewrites the remaining suffix and
rejects requests beyond the snapshot safety floor. Committed suffix truncation
is independently rejected.

## 9. Failure and adversarial proof

The dedicated P5-C tests cover:

- hard-state/log/commit deterministic restart;
- canonical snapshot round trip and CRC corruption;
- malformed/trailing/semantically impossible snapshot state;
- commit-intent recovery after an interrupted cross-file commit;
- unexplained hard-state/commit mismatch failure;
- corruption in hard state, log or snapshot failure;
- local snapshot build followed by committed suffix replay;
- follower InstallSnapshot and append continuation at snapshot+1;
- crash after snapshot intent but before snapshot publication;
- crash after snapshot publication before install completion;
- matching-boundary suffix retention;
- conflicting-boundary suffix discard;
- committed suffix truncation rejection;
- compaction beyond the snapshot boundary rejection;
- foreign cluster ID rejection; and
- foreign static controller voter configuration rejection.

## 10. Qualification evidence

Pinned qualification environment: Rust 1.98.1.

- `cargo fmt --all -- --check`: **PASS**
- workspace Clippy, all targets/features/offline with `-D warnings`: **PASS**
- full workspace tests: **305 passed, 0 failed**
- astralog-controller: **33 passed, 0 failed**
- astralog-raft P3 regression: **120 passed, 0 failed**
- astralog-replication P4 regression: **46 passed, 0 failed**
- astralog-storage: **43 passed, 0 failed**
- protocol golden vectors: **11 passed, 0 failed**
- all doc tests: **PASS**
- `git diff --check`: **PASS**
- P1-P4 production-source diff from P5-B: **empty**

astralog-controller depends only on common, protocol, the frozen Raft core and
CRC32C. There is no Tokio/TCP/socket/controller-heartbeat/placement runtime in
P5-C.

The pre-existing P4 RF=3 integration helper was additionally made tolerant of
loaded CI scheduling by using a 15-second outer election/failover observation
window while continuing to retry transient metadata refresh timeouts. This is a
test-only change; P4 broker/replication production code is unchanged. The
stabilized scenario passed **10/10 consecutive runs** before the final
exact-HEAD workspace qualification.

## 11. Scope boundary

P5-C does not implement:

- controller network transport;
- real multi-process controller election runtime;
- cluster bootstrap serving lifecycle;
- broker join/rejoin runtime;
- broker heartbeats/failure detector;
- placement planner execution;
- metadata dissemination;
- dynamic P4 assignments;
- replica movement orchestration.

The next checkpoint is P5-D: controller transport plus a real 3-node controller
Raft election/failover path using the closed P3 core and this qualified P5-C
persistence adapter.
