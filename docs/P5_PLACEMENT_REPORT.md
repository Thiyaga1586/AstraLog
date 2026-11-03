# AstraLog P5-H Topic Metadata / Deterministic Placement Qualification

Date: 2026-09-27

Status: **PASS — P5-H QUALIFIED**

## Scope

This checkpoint implements exactly P5-H actions P5-065 through P5-090:

- controller-owned topic create/delete/expand authority;
- pre-proposal TopicId generation;
- bounded topic/RF validation and atomic initial assignments;
- deterministic placement input snapshots and scoring;
- Disabled/Preferred/Required failure-domain behavior;
- lifecycle/liveness/feature eligibility;
- deterministic load/concentration/tie-breaking proofs.

P5-I metadata deltas, P5-J dynamic P4 production assignments, replica movement,
feature-level expansion, producer sessions and later-phase behavior remain out of
scope.

## Topic authority

`ControllerNodeHandle::create_topic` is now the P5 topic-creation authority. A
follower returns `NotLeader`; the leader generates a UUIDv4 TopicId before the
proposal and includes that exact ID plus the complete initial assignment set in
one `ControllerCommand::CreateTopic`. Followers apply committed bytes and never
rerun the placement planner.

Topic validation is duplicated at the typed boundary and canonical command/state
boundary:

- topic name must satisfy frozen Wire V1 naming rules and be unique in current
  controller state;
- replication factor is exactly 1, 3 or 5;
- partition count is in `1..=1024`, matching the frozen Wire V1 partition bound;
- every initial partition has exactly one generation-1 assignment;
- create apply remains clone-stage-commit atomic, so a topic cannot exist without
  all initial assignments.

The old P2 broker-local catalog is retained only as a closed compatibility seam.
P5-J, not P5-H, owns production replacement of static/local broker topology.

## Delete and expansion lifecycle

Deletion is controller-owned and replicated:

1. `BeginDeleteTopic` changes ACTIVE to DELETING;
2. new expansion is rejected while DELETING;
3. `FinalizeDeleteTopic` removes topic name, topic record and assignments after
   lifecycle/movement safety checks;
4. recreating the same name uses a fresh TopicId, so the deleted identity is not
   reused by the typed production path.

Partition expansion requires an exact expected current count and a strict increase
within the 1024-partition bound. Only the newly added contiguous partition range
is planned and replicated. The frozen `TopicRecord` bytes are intentionally not
changed to invent a new field. Instead, a successful expansion returns
`PartitioningEpochChange { keyed_partition_choice_may_change: true, ... }`, which
preserves the frozen Wire V1 rule that `hash % partition_count` may remap keyed
records after expansion.

## Placement snapshot and eligibility

`PlacementInputSnapshot` is an immutable planner input assembled from the
leader's speculative ordered controller state plus P5-G observations. For every
broker it captures:

- broker ID;
- durable lifecycle;
- durable rack and zone labels;
- supported feature range;
- current leader-local liveness classification;
- total current replica obligations, counting voters and learners;
- topic-local replica obligations.

A broker is eligible for a new placement only when all are true:

- lifecycle is ACTIVE;
- active cluster feature level is supported;
- current-incarnation P5-G liveness is Healthy;
- the broker has not already been selected for that partition;
- any Required failure-domain constraints are satisfied.

JOINING, DRAINING, FENCED, REMOVED, Unknown, Suspect, Unavailable and
feature-incompatible brokers are therefore excluded from new placements without
changing their replicated authority merely because of liveness.

## Deterministic scoring

For each partition, candidates are evaluated in the frozen order:

1. satisfy Required rack/zone constraints;
2. maximize Preferred rack/zone diversity;
3. minimize current total replica count;
4. minimize topic-local replica concentration;
5. use broker ID as the final stable tie-break.

Counts are updated after every planned replica, so multi-partition plans remain
balanced without randomness. The emitted voter vector is sorted canonically.
Leader preference is not part of `PlacementPlan`; P3 remains the sole owner of
actual data-partition leadership and P5-M owns the later preference hook.

### Failure-domain policy

- `Disabled`: rack/zone labels do not affect placement.
- `Preferred`: diversity is maximized first, but placement may proceed if full
  diversity is impossible; `degraded_failure_domain=true` makes that fallback
  explicit in create/expand results.
- `Required`: every selected broker must have both rack and zone labels, and the
  selected racks and zones must each be pairwise distinct. The request fails
  instead of weakening this rule.

Replicated-state apply independently validates Required rack/zone distinctness so
a handcrafted command cannot bypass the leader planner at the commit boundary.

## Mechanical proof

Pinned Rust qualification environment: 1.98.1.

- `cargo fmt --all -- --check`: PASS
- controller Clippy, all targets/features, offline, `-D warnings`: PASS
- workspace Clippy, all targets/features, offline, `-D warnings`: PASS
- `cargo test -p astralog-controller --offline`: **70 passed, 0 failed**
- `cargo test --workspace --all-features --offline`: **342 passed, 0 failed**
- P3 regression: **120 passed, 0 failed**
- P4 replication library regression: **46 passed, 0 failed**
- P4 TCP quorum/failover E2E: PASS in full workspace run
- P2 protocol/golden vectors: **44 protocol/golden tests passed, 0 failed**
- P1 storage regression: **43 passed, 0 failed**
- P5-H real three-controller topic/placement/failover scenario: **5/5 PASS**
- P4 deterministic E2E stability campaign after harness correction: **5/5 PASS**
- `git diff --check`: PASS before closure staging

The P5-H controller suite additionally proves:

- follower topic creation is rejected;
- Unknown broker liveness is excluded from new placement;
- Required placement never silently consumes an unavailable fifth broker;
- duplicate topic name is rejected;
- expansion adds exactly the new partition range and surfaces the partitioning
  consequence;
- DELETING fences further expansion;
- delete/recreate produces a new TopicId;
- committed recreated metadata survives controller leader kill, higher-term
  failover, old-controller restart and convergence;
- reordered planner candidate inputs produce exactly identical plans;
- RF=3 assignments contain no duplicate broker and satisfy Required rack
  uniqueness across a 64-partition campaign.

## Qualification-harness correction

The first full-workspace run exposed a pre-existing flaky P4 E2E election timeout.
That test previously gave all three brokers overlapping randomized election ranges
and had already required a widened observation timeout in P5-G. P5-H does not
change P4 production election logic. The correctness-only E2E harness now assigns
each broker a legal non-overlapping election-timeout range, eliminating random
split-vote qualification noise while leaving production defaults and P3 safety
tests untouched. The corrected scenario then passed 5/5 consecutive runs and the
full workspace pass.

Fresh Windows qualification also surfaced `File` imports that are used only under
`cfg(unix)`. Those imports are now gated correspondingly in controller and
replication persistence modules. This is compile-hygiene only; disk bytes,
fsync behavior on Unix and runtime semantics are unchanged.

## Boundary audit

- no P1 Storage Format V1 bytes changed;
- no P2 existing wire/golden bytes changed;
- no P3 consensus source or safety rule changed;
- no P4 production acknowledgement/read/replication semantics changed;
- no P5-I metadata delta/cache implementation was introduced;
- no P5-J dynamic production assignment integration was introduced;
- no P5-K/L movement orchestration was introduced;
- no P6+ feature was pulled forward.

## Next checkpoint

P5-I only — actions P5-121 through P5-127: metadata delta representation,
monotonic broker `applied_metadata_epoch`, bounded delta delivery, snapshot
fallback and atomic stale/out-of-order-safe broker metadata-cache application.

P5-J dynamic replica/peer integration must remain disabled until P5-I is
independently qualified.
