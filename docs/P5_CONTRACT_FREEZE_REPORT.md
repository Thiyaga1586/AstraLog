# AstraLog P5-A Contract Freeze Report

Date: 2026-09-27

Status: **PASS — P5-A CONTRACT FROZEN**

## Scope

P5-A contains architecture/contracts only. No controller implementation crate or runtime code is introduced by this checkpoint.

Frozen artifacts:

- P5_CONTROLLER_V1.md;
- P5_CHECKLIST.md with exactly 224 unique action IDs;
- controller-specific invariants I-35 through I-48;
- P5 implementation checkpoints P5-A through P5-Q;
- synchronized README, PHASE_PLAN and CURRENT_STATE.

## Ownership audit

The contract preserves the closed architecture:

- P3 remains sole owner of Raft election, quorum, log matching, membership safety and leadership transfer;
- P4 partitions remain sole owners of event-log leadership/commit and acknowledgement visibility;
- P5 owns durable cluster intent, metadata, incarnation fencing, placement and orchestration;
- controller observation of data-partition state never becomes independent authority;
- controller is not inserted into the normal Produce/Fetch hot path.

## Frozen P5-A decisions

- one dedicated controller P3 Raft group;
- controller quorum size exactly 3 or 5;
- controller voter set statically bootstrapped during P5;
- dynamic controller-voter reconfiguration explicitly out of P5;
- ControllerNodeId is separate from broker_id and maps one-to-one to P3 NodeId;
- controller/data Raft group namespaces are disjoint;
- ControllerCommandV1 envelope, primitive encoding, command kind IDs and canonical record layouts frozen;
- ControllerSnapshotV1 header/field order, deterministic ordering and CRC coverage frozen;
- metadata_epoch defined as the committed/applied controller metadata log index;
- greatest committed broker incarnation is the only valid broker authority;
- deterministic placement with Disabled/Preferred/Required domain policy frozen;
- learner-first crash-resumable movement state machine frozen.

## Compatibility audit

P5-A changes documentation only.

- no P1 storage source or Storage Format V1 bytes changed;
- no P2 protocol source or frozen existing API/error numbers changed;
- no P3 consensus source changed;
- no P4 replication/broker source changed;
- P4 StaticReplicationConfig remains the compatibility/test seam until P5-J replaces static topology only in the production path.

## Checklist audit

P5_CHECKLIST.md contains exactly 224 distinct IDs P5-001 through P5-224 with no gaps or duplicates.

P5-001 through P5-012 are complete at this checkpoint. P5-013 onward remain unchecked and no implementation claim is made for them.

## Pre-commit qualification

Pinned Rust qualification environment: 1.98.1.

- cargo fmt --all -- --check: PASS;
- workspace Clippy, all targets/features, offline, -D warnings: PASS;
- cargo test --workspace --all-features --offline: PASS;
- workspace total: 272 passed, 0 failed;
- astralog-raft P3 regression: 120 passed, 0 failed;
- astralog-replication P4 regression: 46 passed, 0 failed;
- astralog-storage: 43 passed, 0 failed;
- P2 protocol golden vectors: 11 passed, 0 failed;
- all doc-test targets: PASS;
- git diff --check: PASS.

## Next checkpoint

P5-B may begin only after this contract-freeze commit is qualified at exact HEAD.

P5-B scope is limited to:

- create astralog-controller crate;
- controller-native checked IDs/types;
- ControllerCommandV1 deterministic codec and bounds;
- golden vectors;
- deterministic ControllerState;
- pure ordered apply logic;
- impossible-transition rejection.

No controller transport, persistence, broker membership runtime, placement execution, or movement orchestration belongs in P5-B.
