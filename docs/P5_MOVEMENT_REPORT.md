# AstraLog P5-K/L Crash-Resumable Replica Movement Qualification

Date: 2026-09-27

Status: **PASS — P5-K/L QUALIFIED**

## Scope

This checkpoint implements exactly P5-K/L actions P5-091 through P5-120. It starts from
the P5-J durable `MovementStage::Planned` record and qualifies the complete movement path:

1. persist learner intent;
2. create the destination replica runtime without voter authority;
3. add the destination as a P3 learner;
4. reuse ordinary P4/P3 replication and rebootstrap for catch-up;
5. publish incarnation/generation-fenced learner progress;
6. require catch-up through the relevant current commit boundary;
7. enter P3 joint consensus;
8. observe the committed joint configuration;
9. leave joint consensus into the target stable voter set;
10. optionally transfer P3 leadership to a preferred target;
11. remove the source serving runtime only after committed P3 exclusion;
12. preserve old replica bytes as non-authoritative orphan state;
13. commit the final controller assignment with a new generation;
14. mark the movement `Complete`;
15. resume the same workflow after controller and broker failures.

P5-N feature-level qualification, P5-O producer-ID closure, the broader P5-M
leadership-policy hooks and P5-P/Q final P5 failure/closure qualification remain outside
this checkpoint.

## Durable movement authority

Movement authority remains the existing replicated `MovementRecord`. Its identity and
preconditions are immutable after `BeginMovement`:

- non-zero `MovementId`;
- topic and partition;
- exact expected `AssignmentGeneration`;
- source broker;
- destination broker;
- complete target voter set;
- optional preferred leader.

The controller does not infer progress from RPC completion. Every workflow transition is
a committed controller command with an exact expected previous stage. If the response is
lost, the next reconciliation iteration reads committed controller state plus observed P3
state and continues from there.

The authoritative stage sequence remains the frozen P5 contract:

```text
Planned
  -> LearnerRequested
  -> LearnerCreated
  -> CatchingUp
  -> PromotionReady
  -> EnterJointRequested
  -> JointCommitted
  -> LeaveJointRequested
  -> NewStableCommitted
  -> [LeadershipTransfer]
  -> OldReplicaRemoveRequested
  -> OldReplicaRemoved
  -> Complete
```

No stage is skipped to manufacture success.

## Dynamic destination actor creation

P5-J previously projected only the committed final assignment into local P4 actor sets.
P5-K/L extends the broker embedding so an unfinished committed movement can temporarily
add the destination actor before the final controller assignment changes.

The important authority split is:

- actor existence is not voting authority;
- a newly opened destination actor initially has no P3 vote;
- the first P3 membership change preserves all old voters and adds the destination only as
  a learner;
- only the later promotion transition may place it in the target voter set.

Static P4 configuration remains unchanged for compatibility tests. Transitional actor sets
exist only in dynamic-controller mode and are derived from committed movement stage.

## Learner creation and ordinary catch-up

The broker currently holding P3 partition leadership performs learner creation. The
controller never reaches into a partition Raft core and mutates membership itself.

The current leader proposes the already-frozen P3 configuration transitions needed to
arrive at a stable configuration containing:

- the original voter set; and
- the destination as a learner.

Once that configuration is effective, all data transfer uses the existing P4/P3
replication machinery. There is no P5-specific file copy, record copy or snapshot format.
AppendEntries, conflict repair and the existing P3-authorized P4 rebootstrap path remain
the only correctness mechanisms for learner catch-up.

The failure E2E proves that the original source broker can disappear during bootstrap and
the destination can still recover/catch up from surviving authority.

## Progress observation and fencing

`ReplicatedPartition::membership_observation` exposes only bounded local observation state:

- current P3 role and observed leader;
- leader epoch;
- durable commit index;
- durable last index;
- effective P3 configuration;
- pending configuration index;
- leader-local peer match indexes.

Dynamic broker heartbeats publish bounded `ReplicaProgressReport`s derived from those P3
observations. They are not replicated controller authority.

Controller movement observation is fenced in three independent dimensions:

1. heartbeat authority must match committed broker ID, node UUID and incarnation;
2. the report's assignment generation must equal the current movement/assignment
   generation;
3. movement-progress identity must match the committed `MovementId`, topic and partition.

The observation cache is leader-local volatile state and is cleared on controller
leadership change. A new controller leader must rebuild it from fresh broker heartbeats.
The adversarial unit test also proves that stored incarnation-1 progress immediately loses
authority after a committed incarnation-2 rejoin.

## Catch-up-before-promotion gate

Controller promotion admission requires both sides of the same safety statement:

- the live P3 leader's peer progress for the destination is at least its current durable
  commit index; and
- the controller leader has a fresh incarnation/generation-fenced destination progress
  report whose durable Raft index reaches the same relevant commit boundary.

Only then may the controller stage advance from `CatchingUp` to `PromotionReady`.

This does not replace P3's own promotion check. The actual P3 `EnterJoint` proposal still
passes through the frozen P3 guard that rejects a new voter unless it is already a learner
caught up through the current commit. P5 therefore has both a controller-side policy gate
and the independent consensus-core safety gate.

Sustained application writes may legitimately move the current commit boundary faster than
a learner catches up. P5-K/L does not weaken the boundary to force promotion. Qualification
therefore proves bounded quorum writes while a movement is active, then allows the learner
to converge on a stable promotion boundary.

## Joint consensus and stable target observation

Promotion uses the existing P3 membership API:

```text
stable(old voters + destination learner)
        -> EnterJoint(old learner config, target voters)
        -> committed joint configuration
        -> LeaveJoint(target voters)
        -> committed stable(target voters)
```

Request acceptance is never treated as durable completion.

`EnterJointRequested -> JointCommitted` requires an observed matching joint
configuration whose pending configuration index is at or below P3's durable commit index.

`LeaveJointRequested -> NewStableCommitted` requires an observed stable target
configuration with no pending configuration entry.

Post-P5-O stress qualification tightened the crash-resumable observation rule without
changing P3 authority: a broker does not need to be the local partition leader merely to
recognize that the required joint/stable configuration is already durably present. Leader
authority is required only on branches that actually submit `EnterJoint` or `LeaveJoint`.
This prevents response-loss/failover recovery from stalling behind an unnecessary local-role
gate while preserving P3 as the sole membership-change authority.

The real RF=3 E2E moves partition voters from `[1,2,3]` to `[2,3,4]` and verifies that the
controller assignment is not replaced until this P3 sequence has completed.

## Optional leadership transfer

When the durable movement has a preferred leader, the target is already validated as a
member of the target voter set. After the stable target configuration is observed, only the
current P3 leader may call the existing P3 leadership-transfer primitive.

The movement advances past `LeadershipTransfer` only after live P3 status reports the
preferred broker as leader. The controller cannot decree or synthesize partition
leadership.

The RF=3 E2E requests broker 4 as preferred leader. The movement cannot advance beyond
`LeadershipTransfer` until live P3 status observes broker 4 as leader; after completion the
test requires ordinary P3 to expose a current leader from the final `[2,3,4]` voter set and
proves quorum Produce/ReadCommitted Fetch remain available. The preference is a transfer
target, not a permanent leadership pin.

Repeated leadership-transfer stress also exposed two P3 integration defects that are now
closed by the post-P5-O boundary repair. First, when a learner's final stable voter
configuration commits, the live Raft role is reconciled from `Learner` to `Follower`; the
promoted node can therefore participate in subsequent elections without restart. Second, a
new leader can legitimately be temporarily behind a follower in its knowledge of the global
commit index. A follower that appends that leader's new entries now preserves
`max(local_commit, min(leader_commit, match_index))` rather than attempting to move its
already-durable commit boundary backward. The latter previously killed surviving target
actors with `InvalidHardState("durable commit index cannot move backward")` after transfer.

## Source removal and orphan-byte preservation

The old source remains a serving actor throughout learner creation, catch-up and voter
promotion. `OldReplicaRemoveRequested` is unreachable until P3 has already reported the
stable target voter configuration excluding the source.

Only then may dynamic broker reconciliation remove the source actor from the local serving
set. Actor removal never deletes its partition directory. The final E2E verifies broker
1's partition directory still exists after `[1,2,3] -> [2,3,4]` completion.

`OldReplicaRemoved -> Complete` is intentionally a controller-metadata operation, not a
fresh data-plane-leader operation. Safe P3 exclusion has already been proven by the
preceding stages. Any broker reconciler may therefore submit `FinalizeMovement`; controller
Raft serialization and expected-stage fencing make duplicate attempts harmless.

Finalization atomically:

- installs the target voter set;
- clears controller learners;
- advances assignment generation with checked arithmetic;
- marks the durable movement `Complete`.

## Crash-resumable reconciliation

The dynamic control loop performs a bounded sequence per tick:

1. metadata refresh;
2. heartbeat/progress publication;
3. at most one durable movement reconciliation action.

Every action first compares durable controller intent with observed P3 state. Already
achieved P3 configurations are accepted as evidence after response loss; contradictory or
stale state does not advance the workflow.

The dedicated failure E2E proves all of the following in one real deployment:

- three-controller quorum;
- four dynamic brokers;
- RF=3 partition initially `[1,2,3]`;
- source broker 1 dies during learner bootstrap;
- destination broker 4 dies during learner bootstrap;
- the active controller leader dies mid-movement;
- the replacement controller leader starts without inherited volatile progress;
- movement remains incomplete while the destination is absent;
- destination rejoin is committed at incarnation 2;
- destination restarts on its existing durable directory;
- fresh heartbeats/progress rebuild controller observations;
- movement converges to `Complete` and final voters `[2,3,4]`;
- post-recovery quorum Produce succeeds.

The workflow is therefore reconstructed from desired/committed state rather than from
in-memory callbacks owned by the failed process.

## Movement resource bounds

P5-K/L adds two explicit controller runtime limits:

- `max_active_movements = 64` by default;
- `max_active_movements_per_broker = 8` by default.

Both must be non-zero. Admission checks the speculative controller `validation_state`, so
accepted-but-not-yet-applied pending proposals cannot bypass the bounds. The pure capacity
test proves both the global and per-broker limits fail closed.

Existing per-partition movement serialization remains independently enforced by the
controller state machine. Reassignment admission also rejects a non-stable controller
assignment that already contains learners.

## P4 integration surface

P5-K/L adds only the P4 runtime hooks required to drive already-frozen P3 membership:

- membership observation;
- configuration proposal forwarding;
- leadership-transfer forwarding.

`astralog-replication` still delegates all membership safety decisions to `astralog-raft`.
There is no new quorum algorithm, commit rule, storage format, transport frame, data-copy
format or acknowledgement/read semantic.

Dynamic topology publication was also tightened so a controller metadata change that does
not alter this broker's local actor set or peer identities updates routing metadata in
place. This avoids destroying healthy P3 leader/term state merely because another broker's
source actor was safely removed.

## Compatibility boundary

P5-K/L preserves:

- P1 storage bytes and generation/recovery semantics;
- every P2 public wire body and golden fixture;
- P3 Raft algorithms, joint-consensus rules, learner-promotion rule and leadership-transfer
  rule;
- P4 persistence, peer transport, acknowledgement, committed-read and rebootstrap
  semantics;
- P5-J controller-authoritative Metadata and controller-outage data-plane behavior.

There are no changes in `astralog-storage`, `astralog-protocol`, or `astralog-raft` in this
checkpoint. The `astralog-replication` changes are embedding/API hooks over existing P3
inputs and observations, not a consensus-semantic change.

## Qualification evidence

Mechanical qualification on the P5-K/L implementation bytes before commit:

- `cargo fmt --all -- --check`: PASS;
- `cargo clippy --workspace --all-targets --all-features --offline -- -D warnings`: PASS;
- `cargo test --workspace --all-features --offline`: **358 passed, 0 failed**;
- broker library: **10 passed, 0 failed**;
- legacy P2 network suite: **9 passed, 0 failed**;
- frozen P4 RF=3 TCP E2E: **1 passed, 0 failed**;
- P5 dynamic integration/movement suite: **3 passed, 0 failed**;
- client/common: **5 passed, 0 failed**;
- controller suite: **75 passed, 0 failed**;
- protocol suite: **35 passed, 0 failed** plus **11/11** frozen golden tests;
- Raft suite: **120 passed, 0 failed**;
- replication suite: **46 passed, 0 failed**;
- storage suite: **43 passed, 0 failed**;
- full controller suite repeated **5/5 PASS**;
- three-scenario P5 dynamic suite repeated **5/5 PASS**;
- frozen P4 RF=3 TCP quorum/redirect/epoch/failover E2E repeated **5/5 PASS**;
- `git diff --check`: PASS;
- boundary audit: no P5-K/L changes in storage, public protocol or Raft-core crates.

The P5 network qualification harness also uses process-salted isolated loopback addresses,
stable leader/term observation and explicit committed-state reconciliation for ambiguous
setup responses. Process salting prevents successive Windows test processes from reusing
the same bind-then-release address set while old sockets are still retiring. These are
test-only determinism improvements; they do not alter production election, transport or
mutation semantics.

Later P5-O regression qualification additionally widened only the P5 integration harness's
controller tick period from 10 ms to 25 ms. The frozen 5..=10 election-timeout tick contract is
unchanged; the wider wall-clock window prevents scheduler-induced perpetual candidacy during
repeated Windows/CI stress and does not change production controller timing.

Post-P5-O boundary-repair qualification adds two focused Raft regressions and re-runs the
real movement deployment under repeated transfer/failover pressure. The repaired bytes pass
the full Raft suite at **122/122**, the full workspace at **368/368**, the three-scenario P5
dynamic movement/controller suite **10/10**, the frozen P4 RF=3 E2E **5/5**, and the full
controller suite **5/5**. This repair is a lower-layer correctness closure discovered before
P5-M; it does not implement any P5-M general leadership policy.

## P5-K/L closure

P5-K/L is qualified only when this report and checklist are committed together with the
implementation and the exact committed HEAD re-passes formatting, Clippy, the full
workspace suite, repeated P5 movement/failure qualification, repeated controller
qualification and repeated frozen P4 E2E.

The next bounded checkpoint is **P5-N only — feature-level qualification, actions P5-145
through P5-151**. P5-O producer-ID closure, P5-M general leadership hooks and later P5
failure/closure work remain disabled until P5-N is independently qualified.
