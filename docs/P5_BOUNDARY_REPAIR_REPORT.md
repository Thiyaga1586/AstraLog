# AstraLog Post-P5-O Raft / Movement Boundary Repair

Date: 2026-09-28

Status: **PASS — LOWER-LAYER BOUNDARY REPAIR QUALIFIED / P5-M NOT STARTED**

## Purpose

P5-O producer-ID allocation is already frozen independently. Before beginning P5-M, the live
worktree contained uncommitted investigation changes created while stress-testing P5-K/L
movement and preferred-leader transfer. This repair checkpoint resolves those bytes instead
of allowing them to leak into P5-M.

It fixes exactly three related lower-layer correctness issues:

1. committed learner promotion did not reconcile the live local Raft role from `Learner` to
   `Follower`;
2. follower AppendEntries could attempt to regress a higher already-durable local commit when
   a newly elected leader advertised a temporarily lower `leader_commit`; and
3. P5 movement reconciliation required local leader role even when it was only observing an
   already-durable membership state rather than proposing a membership change.

No P5-M action P5-158 through P5-162 is implemented here.

## Failure discovered under real RF=3 movement

Repeated `[1,2,3] -> [2,3,4]` reassignment plus preferred-leader-transfer qualification could
reach controller `MovementStage::Complete` and then lose all usable public partition leader
observations.

Temporary diagnostic instrumentation showed brokers 2 and 3 terminating their partition
actors with:

`InvalidHardState("durable commit index cannot move backward")`

The surviving target replica could simultaneously show a stable target configuration in its
durable log but no leader, proving the failure was below client metadata routing.

The diagnostics were removed after reproducing and repairing the defect; no hidden diagnostic
API or stderr instrumentation is part of the qualified checkpoint.

## P3 repair 1 — live learner-to-voter role reconciliation

Raft membership is derived from durable configuration entries, but the live local role also
controls whether a node may campaign and vote. A destination that entered the group as a
learner could receive and commit the final stable configuration that promoted it to voter
while its volatile role remained `Role::Learner`.

After commit persistence resolves a stable configuration and no configuration entry remains
pending, a non-leader now reconciles its local role from the committed effective membership:

- learner-only local member -> `Role::Learner`;
- committed voter -> `Role::Follower`.

When this changes role, stale pre-vote/election/progress/leadership-transfer volatile state is
cleared and a `RoleChanged` effect is emitted. An already-observed leader identity is retained.

The focused test `committed_stable_promotion_changes_local_learner_to_follower` proves the
promotion is not merely present in configuration bytes: the live node becomes an election-
eligible follower after the stable promotion is durably committed.

## P3 repair 2 — follower commit index cannot regress under a new leader

After a leadership transfer/election, a newly elected leader may temporarily know a lower
commit index than a follower that already learned a globally committed prefix from the prior
leader. That is not evidence that the follower's committed prefix became uncommitted.

The old follower AppendEntries path computed:

`commit_to = min(leader_commit, match_index)`

When the request also appended new entries, completion later passed that lower value into the
durable commit finalizer, which correctly rejected commit regression and therefore killed the
partition actor.

Follower AppendEntries now computes:

`commit_to = max(local_commit, min(leader_commit, match_index))`

This preserves the fundamental monotonic-commit invariant while retaining the ordinary Raft
upper bound for forward commit movement. It does not relax the durable finalizer's backward-
movement rejection; an actual internal attempt to regress commit remains an error.

The focused test `follower_append_from_new_leader_never_regresses_existing_commit` recovers a
follower at durable commit 2, accepts a new leader's index-3 append carrying `leader_commit=1`,
persists the append, keeps durable/volatile commit at 2, and returns successful match index 3.

## P5 movement reconciliation repair

P3 remains the sole authority that may change membership. The broker movement reconciler now
distinguishes two cases explicitly:

- **proposal branch:** only the actual P3 leader may submit `EnterJoint` or `LeaveJoint`;
- **observation branch:** any local replica that observes the already-required matching
  durable joint/stable configuration may advance exact-stage-fenced controller bookkeeping.

This makes failover/response-loss reconstruction state-driven instead of accidentally tied to
which broker currently holds local P3 leadership.

Catch-up admission and leadership transfer remain leader-observed where their semantics
require leader-local progress/authority. No controller path can decree a partition leader.

## Qualification-harness cleanup

The movement E2E was also made more faithful to its frozen contract:

- it proves a real quorum write while movement is active, then releases sustained write
  pressure so learner catch-up has a stable promotion boundary;
- post-movement metadata probes surviving target replicas rather than assuming one fixed
  broker remains the metadata source;
- election tick windows use the same deterministic wall-clock headroom philosophy as the
  frozen P4 correctness harness;
- timeout failures report the exact movement/progress/metadata or Produce/refresh context.

These are qualification changes, not production leadership policy.

## Mechanical qualification

On the repaired pre-commit bytes:

- `cargo fmt --all -- --check`: PASS;
- workspace Clippy, all targets/features, offline, `-D warnings`: PASS;
- full Raft suite: **122 passed, 0 failed**;
- full workspace: **368 passed, 0 failed**;
- protocol goldens: **11/11 PASS**;
- three-scenario P5 dynamic movement/controller suite: **10/10 PASS**;
- frozen P4 RF=3 TCP E2E: **5/5 PASS**;
- full controller suite: **5/5 PASS**.

The workspace total increases from 366 to 368 only because the two focused Raft regression
tests were added.

## Boundary statement

This checkpoint intentionally changes P3 Raft behavior because repeated P5 movement stress
proved a lower-layer correctness defect. It does **not** change storage bytes, public protocol
bodies, controller metadata formats, P4 acknowledgement/read semantics, or P5-O producer-ID
semantics.

Most importantly, it does not implement P5-M. Actions P5-158 through P5-162 remain unchecked.

After this repair is committed and the exact committed SHA passes the same qualification
campaign, the next clean checkpoint is **P5-M only — general leadership hooks, actions
P5-158 through P5-162**.
