# AstraLog P5-P Failure Qualification

Date: 2026-09-29

Status: **PASS — P5-P QUALIFIED / P5-Q NEXT**

## Scope

This checkpoint implements and qualifies exactly P5-P actions P5-163 through P5-200.
It does not close P5 itself. P5-Q actions P5-201 through P5-224 remain a separate final
closure checkpoint.

P5-P is primarily adversarial qualification over already-frozen P5 ownership and P3/P4
safety semantics. It adds only two production hardenings discovered by those tests:

1. canonical controller live-state admission now guarantees broker/topic/assignment/movement
   growth remains inside both absolute count limits and the existing 64 MiB Snapshot V1 /
   controller-transport envelope; and
2. a broker that rejects an incremental `MetadataSyncResponse::Deltas` batch atomically now
   recovers from a fresh authoritative controller snapshot instead of retrying the same unusable
   incremental history forever.

No storage bytes, public Wire V1 API bytes, Raft consensus algorithm, P4 acknowledgement/read
semantics, consumer-group logic or P7 producer-idempotency semantics are introduced here.

## Deterministic controller fault harness

`failure_qualification.rs` drives the real `ControllerState` and canonical
`ControllerCommandV1` encoder rather than inventing a second metadata state machine.

The seeded simulator:

- starts nine ACTIVE brokers;
- creates four Required-domain topics with eight RF3 partitions each;
- executes 96 seeded operations including committed broker rejoins, monotonic incarnation
  changes, producer-ID allocation, and deliberately stale rejected rejoins;
- validates broker UUID uniqueness, assignment structure, metadata-epoch monotonicity,
  producer-ID non-reuse and controller capacity invariants after every step;
- records canonical command length + CRC trace material; and
- proves the same seed produces identical final state and trace while a different seed diverges.

The deterministic placement campaign separately plans 64 RF3 Required partitions and repeats
that plan for 128 seeded candidate-order permutations. Every result is exactly identical.

## Controller quorum faults

Real three-controller tests qualify the controller failure boundary:

- ordinary elected-leader kill preserves committed metadata and elects a higher-term
  replacement;
- minority isolation shuts down both followers, attempts a mutation on the isolated former
  leader, and proves committed metadata epoch does not advance;
- controller peer transport rejects foreign/unknown source identity before delivery while P3
  remains the only higher-term fencing authority;
- controller-leader replacement deliberately loses volatile failure-detector observations and
  begins at Unknown instead of inheriting suspicion; and
- bounded admin pressure cannot starve heartbeat/consensus progress.

`controller_leader_failure_at_every_movement_stage_preserves_durable_workflow` additionally
kills/restarts the current controller leader at each of the 13 durable movement states:

```text
Planned
LearnerRequested
LearnerCreated
CatchingUp
PromotionReady
EnterJointRequested
JointCommitted
LeaveJointRequested
NewStableCommitted
LeadershipTransfer
OldReplicaRemoveRequested
OldReplicaRemoved
Complete
```

After every kill, the replacement leader must expose the exact same movement stage and metadata
epoch before one legal transition is allowed to continue.

## Broker incarnation crash boundaries

`broker_incarnation_crash_boundaries_preserve_only_committed_authority` exercises both sides of
the commit boundary on the real controller quorum.

### Crash before commit

The elected leader is isolated from both followers and receives an incarnation-2 rejoin. It
cannot commit the mutation without controller quorum. The isolated process is killed and only
the durable surviving majority is restarted. Incarnation 1 remains the authoritative broker
identity.

### Crash after commit before response

The quorum is restored. Incarnation 2 is submitted through the raw control path and allowed to
commit/apply, but the response receiver is deliberately dropped unread. The controller that
observed the commit is then killed. The replacement leader preserves incarnation 2, fences
incarnation 1, rejects an old-process incarnation-1 heartbeat, and accepts the incarnation-2
heartbeat.

Internal replication transport independently fences stale source incarnations before P3 message
delivery, and dynamic broker topology replaces peer incarnation expectations from committed
`BrokerRecord` authority.

## Placement failure matrix

P5-P directly qualifies the required placement edges:

- adding a newly ACTIVE but empty broker never rewrites existing committed assignments;
- RF=3 with only two eligible brokers fails explicitly;
- Required domain policy fails rather than silently reusing an insufficient rack/domain set;
- the same candidate set under Preferred policy may proceed only with
  `degraded_failure_domain=true`; and
- multi-topic / multi-partition placement remains deterministic under input reordering.

Placement remains an explicit controller action. Broker registration alone never promotes a
broker into a data-partition voter set.

## Movement under writes and failures

The RF3 P5 dynamic test now performs eight paced quorum writes during active reassignment and
requires several acknowledgements while the movement is still non-Complete. A concurrent stage
monitor waits until destination broker 4 is `PromotionReady` (or a later safe pre-removal stage),
then kills source broker 1. Reconciliation must still finish with final voters `[2,3,4]`, and
every quorum-acknowledged offset/value from the write campaign must be readable from the new voter
set. Source bytes remain present as non-authoritative orphaned data.

A second real failure scenario continues to kill source and destination during learner bootstrap,
commits destination rejoin at a higher incarnation, kills the controller leader, and resumes the
same durable movement to completion.

The bounded deterministic movement model complements those network scenarios by snapshot/restart
at every durable movement boundary, rejecting illegal stage skips, premature finalization and
duplicate stale-stage reconciliation delivery. P3 independently rejects learner promotion until
the learner is caught up through the committed boundary.

## Controller outage isolation

The real dynamic-broker outage scenario shuts down all three controllers after an RF1 actor is
healthy.

While the controller quorum is absent:

- controller-backed topic administration fails explicitly;
- broker Metadata continues to expose the same cached cluster ID, metadata epoch and topic state;
- quorum Produce succeeds; and
- ReadCommitted Fetch returns the produced record.

Controller unavailability therefore blocks new control-plane authority but does not tear down or
corrupt an already-running healthy data partition.

## Metadata recovery faults

P5-P combines four independent recovery barriers:

1. retained delta miss / controller-Raft index gap returns a full authoritative snapshot;
2. broker delta application is clone-stage-commit atomic, so a late invalid delta cannot partially
   mutate cache state;
3. periodic broker refresh now treats failed `Deltas` application as unusable incremental history
   and requests a fresh controller snapshot; and
4. a broker cache restarted from a stale epoch converges to a newer authoritative snapshot while
   refusing later regression.

Snapshot CRC/canonical validation remains loud; invalid authoritative snapshots are not silently
accepted.

## Snapshot/restart, feature level and producer IDs

Existing independently qualified primitives are rerun as part of the P5-P workspace matrix:

- controller local snapshot/restart reconstructs identical committed metadata state;
- follower InstallSnapshot bootstrap replaces conflicting uncommitted suffix and continues at the
  snapshot boundary + 1;
- unsupported ACTIVE/DRAINING feature participants reject a feature bump;
- a compatible explicit feature bump commits exactly once and survives leader replacement;
- producer-ID allocation remains commit-before-response and monotonic across failover; and
- a committed but unread producer-ID response may create a gap but never ID reuse.

## Bounded controller memory

P5-P found that Snapshot V1 already bounded decoded record counts and total snapshot bytes, but
live replicated state growth previously enforced command-local shape limits without guaranteeing
that future state stayed within the same snapshot envelope.

The canonical state machine now has absolute map limits:

- brokers: 100,000;
- topics: 100,000;
- assignments: 1,000,000;
- movement records: 100,000.

Growth commands additionally compute an O(1) worst-case canonical Snapshot V1 size from projected
record counts and reject before mutation if that worst case could exceed the existing 64 MiB
Snapshot V1 / controller transport package limit. This deliberately keeps the 64 MiB envelope
unchanged instead of increasing a resource bound during failure qualification.

Independent bounds remain in place for command bytes/items, heartbeat progress arrays,
metadata-delta retention and responses, active movements, pending controller mutations and bytes,
control requests, peer transport frames/bytes, snapshots and broker admin operations.

## Control-plane scheduling pressure

`bounded_admin_pressure_does_not_starve_raft_or_heartbeat_control` runs a real three-controller
cluster with only eight control-request slots and 32 pending mutations, launches 24 concurrent
producer-ID allocations, and requires a valid broker heartbeat to complete within a bounded
deadline while controller Raft remains live. Every pressure request also has a completion bound.

This complements lower-layer P3/P4 backpressure tests where bounded application queues cannot
kill Raft control processing.

## P5-P action evidence

The complete action-to-test mapping is recorded mechanically in `docs/P5_CHECKLIST.md`, actions
P5-163 through P5-200. Major anchors are:

- deterministic simulator / seeded churn: P5-163, P5-197;
- real controller quorum failure tests: P5-164 through P5-175 and P5-183;
- deterministic placement campaign/edges: P5-176 through P5-179 and P5-196;
- real + modeled movement faults: P5-180 through P5-187 and P5-198;
- metadata/snapshot recovery: P5-188 through P5-191;
- feature/producer-ID failover: P5-192 through P5-195;
- resource/scheduling audit: P5-199 through P5-200.

## Mechanical qualification

Qualification on the P5-P bytes:

- `cargo fmt --all -- --check`: PASS;
- workspace Clippy with `-D warnings`: PASS;
- `astralog-controller`: **91 passed, 0 failed**;
- broker library: **13 passed, 0 failed**;
- P2 network: **9 passed, 0 failed**;
- frozen P4 RF3 E2E: **1 passed, 0 failed** in the workspace gate;
- P5 dynamic multi-process suite: **4 passed, 0 failed**;
- client: **3 passed, 0 failed**;
- common: **2 passed, 0 failed**;
- protocol: **35 passed, 0 failed**;
- frozen protocol goldens: **11 passed, 0 failed**;
- Raft: **122 passed, 0 failed**;
- replication: **46 passed, 0 failed**;
- storage: **43 passed, 0 failed**;
- total workspace: **380 passed, 0 failed**;
- sustained-write + caught-up-source-failure RF3 scenario: **3/3 PASS**;
- controller minority isolation: **3/3 PASS**;
- bounded admin-pressure / heartbeat scheduling: **3/3 PASS**;
- broker-incarnation crash-boundary scenario: **3/3 PASS**;
- controller-leader failure at every durable movement stage: **3/3 PASS**.

Repeated phase-wide stability and exact-SHA qualification are performed before this checkpoint is
declared frozen; their authoritative results are recorded in `CURRENT_STATE.md` after commit.

## Boundary

P5-P does **not** execute P5-Q closure actions. In particular, it does not mark the overall P5
phase closed, create `P5_CLOSURE_REPORT.md`, or check P5-201 through P5-224.

The next bounded checkpoint after independent P5-P qualification is **P5-Q only — final P5
closure, actions P5-201 through P5-224**.
