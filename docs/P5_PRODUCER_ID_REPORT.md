# AstraLog P5-O Producer-ID Allocator Qualification

Date: 2026-09-27

Status: **PASS — P5-O QUALIFIED**

## Scope

This checkpoint implements and qualifies exactly P5-O actions P5-152 through P5-157. It
closes the durable producer-ID allocation primitive that was intentionally seeded into the
frozen P5 controller command/state/snapshot formats for later P7 idempotent-producer work.

P5-O owns only:

1. the durable monotonic next-unallocated counter;
2. bounded contiguous allocation blocks;
3. commit-before-response ordering;
4. safe lost-response ambiguity where IDs may be wasted but never reused;
5. overflow refusal; and
6. the explicit boundary that producer epochs, sessions, sequences and deduplication remain P7.

P5-M general leadership-hook qualification and P5-P/Q remaining failure/closure work are
outside this checkpoint.

## Frozen-format preservation

No controller wire or snapshot format is redesigned in P5-O.

The already-frozen V1 structures remain authoritative:

- `ControllerCommand::AllocateProducerIds { allocation_count: u32 }` remains command kind 12;
- `ControllerState.next_unallocated_producer_id: u64` remains the durable allocator counter;
- ControllerSnapshotV1 continues to persist that `u64` counter in its existing field position;
- `ApplyOutcome.producer_ids` remains the state-machine result surface.

The existing command golden for an allocation count of 32 remains byte-exact. P5-O adds an
implementation-level hard bound without adding fields or changing canonical bytes.

## Durable monotonic authority

`next_unallocated_producer_id` is mutated only by the deterministic controller state machine.
An allocation uses the pre-apply counter as `first`, computes the exclusive next value with
checked `u64` arithmetic, returns `last = next - 1`, and stores the new next-unallocated
counter atomically with the controller metadata epoch.

Because the field is part of ControllerSnapshotV1 and controller Raft replay, allocator
authority survives controller leadership changes, snapshot/recovery and complete process
restart. No broker-local counter participates in ID ownership.

## Bounded contiguous blocks

P5-O defines:

`MAX_PRODUCER_IDS_PER_ALLOCATION = 1_000_000`

One `AllocateProducerIds` command must request `1..=1_000_000` IDs. Zero and larger requests
fail canonical command validation before proposal. The V1 field remains a `u32`; the tighter
bound is an operational/resource invariant rather than a format change.

For a successful request of `N`, the returned inclusive range is exactly:

`[old_next_unallocated, old_next_unallocated + N - 1]`

and the new durable counter is exactly `old_next_unallocated + N`.

## Commit before response

`ControllerClient::allocate_producer_ids` and `ControllerNodeHandle::allocate_producer_ids`
route the frozen allocation command through the ordinary controller mutation path.

That path responds only when the proposal reaches `MutationOutcome::Committed`, which is
produced after P3 quorum commit and ordered controller-state application. A successful range
therefore cannot be returned from an uncommitted local proposal.

Follower requests fail with `NotLeader`; there is no hidden forwarding or local allocation.

## Lost-response ambiguity and I-45

Allocation retries are deliberately not idempotent in P5. If a response is lost after the
command commits, the caller cannot safely reconstruct or reclaim that block. It requests a new
block, leaving the ambiguous committed range unused if necessary.

The real three-controller P5-O scenario proves this mechanically:

1. starting counter is `1000`;
2. allocate 7 -> `[1000,1006]`, durable next becomes `1007`;
3. submit allocate 5 and wait until controller metadata proves it committed/applied;
4. durable next becomes `1012`;
5. discard the unread response, modeling response loss;
6. kill the controller that observed that commit;
7. replacement leader allocates 5 -> `[1012,1016]`, never `[1007,1011]`;
8. restart the stopped voter and converge it;
9. shut down all three controllers and reopen them from the same durable directories;
10. next allocation returns `[1017,1017]`.

Thus the lost `[1007,1011]` block may be wasted but can never re-enter the allocator. This is
the direct P5 qualification of invariant I-45.

## Overflow refusal

The deterministic state machine computes the next counter with checked `u64` addition.
Overflow returns an error before staged state is published. The existing atomic overflow test
proves both allocator counter and metadata epoch remain unchanged on failure; no wraparound or
partial reservation is possible.

The per-command one-million-ID bound is independent defense against pathological allocation
sizes and does not replace the arithmetic overflow check.

## Explicit P7 boundary

P5-O does not implement or reserve semantics for:

- producer epochs;
- producer sessions;
- producer sequence numbers;
- retry/request identities;
- duplicate Produce detection;
- idempotent or transactional Produce behavior.

Those remain P7 responsibilities. P5-O merely provides the durable unique-ID block primitive
that P7 may consume. No public Produce request/response body or retry semantic changes here.

## P5-K/L regression discovered during qualification

Repeated whole-workspace qualification exposed a latent movement liveness defect unrelated to
producer IDs. A movement could remain indefinitely at `OldReplicaRemoveRequested`.

The safety sequence was already complete at that point: P3 had committed and exposed the
stable target voter set excluding the source, and the dynamic broker loop applies refreshed
metadata/topology before running one movement reconciliation action. The old implementation
nevertheless required a fresh local partition membership/leader observation before recording
`OldReplicaRemoved`. After source actor removal that observation can legitimately be absent,
stranding the otherwise safe durable workflow.

The regression repair makes `OldReplicaRemoveRequested -> OldReplicaRemoved` the intended
exact-stage-fenced bookkeeping transition. It does **not** move source removal earlier and does
not weaken P3 membership safety: the stage remains unreachable until stable target exclusion
has already been observed and committed by the preceding workflow.

The RF=3 reassignment E2E passed **10/10** with its original 30-second completion bound after
this repair.

Qualification also corrected an over-strong test assertion around preferred leadership. The
movement still cannot advance beyond `LeadershipTransfer` until live P3 status observes the
requested broker 4 as leader. After movement completion ordinary Raft is free to elect any
member of final voters `[2,3,4]`; preference is a transfer target, not permanent leadership
pinning. The post-completion E2E therefore requires a current target-set leader and then proves
quorum Produce plus ReadCommitted Fetch.

Repeated qualification also exposed scheduler-induced controller election churn in the P5
integration harness: its 10 ms controller tick combined with 5..=7 sampled election ticks gave
only a 50..=70 ms wall-clock election window. The harness tick period is now 25 ms while the
same frozen tick-count timing contract remains unchanged, yielding a 125..=175 ms window. This
is test-only scheduling headroom; no production controller/Raft timing or election rule changes.
The complete three-scenario P5 dynamic suite then passed **10/10** consecutively.

Exact-commit qualification also exposed a stale-leader assumption inside the producer-ID E2E
itself. After waiting for the committed allocator epoch to converge, the test still queried the
controller that had been leader before the wait; a legitimate leadership handoff therefore
returned `NotLeader`. The harness now reacquires a current leader before leader-only metadata
observation and before submitting the deliberately unread lost-response allocation. It does not
retry an ambiguous allocation. With that observation-only hardening, the focused allocator E2E
passed **10/10** and the complete controller suite passed **5/5**.

## Compatibility boundary

P5-O preserves all frozen lower-layer contracts:

- P1 storage format and recovery semantics are unchanged;
- P2 public protocol bodies/goldens are unchanged;
- P3 Raft consensus algorithms and persistence ordering are unchanged;
- P4 replication acknowledgement/read/rebootstrap semantics are unchanged;
- controller command/snapshot V1 bytes are unchanged.

The only production files changed outside `astralog-controller` are the bounded P5-K/L broker
reconciliation regression repair described above. No P5-O change exists in `astralog-storage`,
`astralog-protocol`, `astralog-raft`, or `astralog-replication`.

## Qualification evidence

Mechanical qualification on the final P5-O implementation bytes before commit:

- `cargo fmt --all -- --check`: PASS;
- `cargo clippy --workspace --all-targets --all-features --offline -- -D warnings`: PASS;
- `cargo test --workspace --all-features --offline`: **366 passed, 0 failed**;
- broker library: **11 passed, 0 failed**;
- legacy P2 network suite: **9 passed, 0 failed**;
- frozen P4 RF=3 TCP E2E: **1 passed, 0 failed**;
- P5 dynamic integration/movement suite: **3 passed, 0 failed**;
- client/common: **5 passed, 0 failed**;
- controller suite: **82 passed, 0 failed**;
- protocol suite: **35 passed, 0 failed** plus **11/11** frozen golden tests;
- Raft suite: **120 passed, 0 failed**;
- replication suite: **46 passed, 0 failed**;
- storage suite: **43 passed, 0 failed**;
- focused producer-ID allocation suite: **5/5 PASS**;
- full controller suite: **5/5 PASS**;
- P5 dynamic controller/movement suite: **5/5 PASS**;
- frozen P4 RF=3 TCP E2E: **5/5 PASS**;
- repaired RF=3 reassignment E2E additional stress: **10/10 PASS**;
- complete P5 dynamic suite with scheduler-hardened test timing: **10/10 PASS**;
- `git diff --check`: PASS.

## P5-O closure

P5-O is qualified only when this report, checklist, implementation and the movement-regression
repair are committed together and the exact committed HEAD re-passes formatting, Clippy, the
complete workspace suite, producer-ID/failover qualification, P5 dynamic qualification,
frozen P4 E2E and the frozen-crate boundary audit.

The next untouched bounded checkpoint is **P5-M only — general leadership hooks, actions
P5-158 through P5-162**. P5-P/Q remaining P5 failure/closure qualification stays outside that
checkpoint.
