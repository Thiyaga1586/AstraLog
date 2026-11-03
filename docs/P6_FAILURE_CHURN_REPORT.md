# P6-K Failure / Churn / Model Qualification

Date: 2026-10-02

Status: **PASS — P6-K QUALIFIED / P6-L NEXT**

This report closes actions **P6-139 through P6-158** only. P6-K does not introduce a new consumer-group architecture. It stress-qualifies the P6-D through P6-J authority, cooperative handoff, session, replicated coordinator, durable offset-fence and public broker/client paths under sustained load, churn, coordinator/offsets-leader failure and deterministic adversarial models. P6-L compatibility/closure actions 159..176 and all P7+ behavior remain disabled.

## Runtime maintenance added for frozen P6 semantics

P6-G froze timeout semantics but intentionally left broker scheduling to later integration. P6-K completes that production runtime binding without creating a second authority plane:

- dynamic brokers run a bounded P6 maintenance ticker only when the P6 service exists;
- the interval is configured and validated in `10ms..=5s`, with a 100ms default;
- only committed local `__astralog_groups` P3 leadership may perform maintenance;
- newly elected group-shard leaders reconstruct committed groups from bounded P4 Fetch pages and create fresh leader-local `SessionTracker` grace from committed membership;
- remembered leadership is keyed by shard + P3 term so the same leadership term cannot repeatedly reset timeout grace;
- at most 1,024 live session groups are tracked and one maintenance round awaits its bounded per-group task set before the next ticker round;
- timeout expiry remains only a candidate. Authority changes only when the existing durable Leave/timeout group command quorum-commits and then completes the normal group-fence/cooperative workflow;
- committed P5 topic-count changes are detected from authoritative metadata and enter a normal `RebalanceAssignments` transition that prepares a strictly higher GroupEpoch; ownership never changes directly from the maintenance loop.

`RebalanceAssignments` is deliberately narrow: it is valid only from STABLE state, preserves the complete membership and current active ownership, requires the computed target to differ, and calls the same `prepare_next_epoch` path used by member-driven rebalance. The resulting PREPARING -> group fence -> epoch activation -> release -> owner fence -> target activation ordering is unchanged.

## Load, crash and heartbeat qualification

The real three-controller/four-broker RF3 P6 cluster now contains a combined churn scenario that keeps group and offset APIs active while membership changes.

During member join, committed CommitOffsets/FetchOffsets traffic continues on partitions that remain active. The joining member cannot become active for moved ownership until the old owner has durably released it and the owner fence has committed. The existing client cooperative plan simultaneously proves the unaffected partition remains processable.

During Leave, committed offset traffic remains active while the departing member executes the public Leave path. The survivor converges through the ordinary fenced higher epoch to STABLE ownership of the complete partition set.

A separate 6-second member proves both sides of the session deadline. A heartbeat just before expiry is accepted and preserves authority. When that member later goes silent without Leave, the maintenance loop durably removes it, completes the fenced rebalance and rejects the stale session afterward.

Coordinator failover has its own silent-member test. The old coordinator is killed immediately after a 6-second member becomes STABLE. The successor reconstructs committed membership, the member remains present after two seconds of silence, and a read-only P6 request proves the successor is ready without refreshing the member heartbeat. If coordinator term changes again, the test re-establishes readiness and measures a new full grace. Once one coordinator term remains stable for the full timeout window, the silent member is durably removed. This proves leadership reset grants grace without granting immortality.

## Coordinator workflow failures

The replicated group-history RF3 scenario now explicitly binds three P6-K failure boundaries:

1. **STABLE** — kill the current coordinator; the higher-term successor must reconstruct the exact stable record before further mutation.
2. **PREPARING_EPOCH_FENCE** — kill the coordinator after the prepared record commits but before activation; the successor must reconstruct the exact pending epoch and cannot invent group-fence durability.
3. **REVOKING after durable release** — kill the coordinator after old ownership has been removed but before owner-fence/target activation; the successor must preserve the no-owner handoff state and resume rather than resurrecting the old owner.

Each deliberate failure is exercised on real RF3 P4/P3 history. Where later failure stages need full quorum, the failed replica is rejoined and convergence is mechanically observed before the next failure.

## Offsets-fence leader failures and stale races

Two dedicated RF3 cases kill the actual `__astralog_offsets` shard leader at the two cross-topic safety boundaries.

For the group-fence boundary, the latest group record is committed PREPARING and no OffsetGroupFence exists. After the offsets leader dies, group state still exposes the old active epoch and the successor reconstructs no group fence. Epoch activation remains impossible until the higher-term successor commits the required fence.

For the owner-fence boundary, old ownership is first durably released in group history and target ownership is withheld. The offsets leader then dies before the OffsetOwnerFence exists. The successor reconstructs no owner fence and target activation remains impossible until that exact fence is quorum-committed.

The previously qualified real RF3 stale-write races are rerun with P6-K: a stale old GroupEpoch cannot overwrite after the durable group fence advances, and a released old owner cannot commit at the same GroupEpoch after the durable owner fence switches effective authority.

## Static membership and topic expansion

Two concurrent clients race the same static replacement request from the same old MemberEpoch. Durable group state advances the one logical member to MemberEpoch 2 exactly once. If both callers observe success, the responses describe the same committed authority; the original session is fenced.

For metadata churn, P5 expands a subscribed RF3 topic from two to four partitions without changing membership. The broker maintenance path reads committed topic counts, runs the frozen uniform-sticky assignor, and prepares a strictly higher group epoch. With the single surviving subscriber, all still-valid ownership is retained and only the two new partitions are added after the normal group/owner fencing path. The group converges to STABLE at the expected higher epoch with four active assignments.

## Deterministic campaigns and models

P6-K adds a seeded membership-churn campaign: **32 seeds × 32 join/leave transitions**, with each seed executed twice. The two traces must be byte-identical. After every transition the group is STABLE, active equals target ownership, all six partitions have exactly one current member owner and the uniform-subscriber load spread remains at most one.

The frozen assignor exhaustive oracle is rerun and continues to match the production solver on `(maximum load, movement count, canonical owner vector)`. The existing bounded cooperative handoff model is also rerun and preserves I-18/I-19 across its explored release/fence/activation interleavings.

P6-K adds a separate offset-fence model over five events: group-fence advance, stale old-epoch value, owner switch, old-owner same-epoch value and new-owner value. All **120 permutations** are executed. After every accepted event, rebuilding `OffsetStateMachine` from accepted history yields identical group fence, owner fence and logical value; once a fence takes effect, the corresponding stale writer is rejected.

## Corruption, resources and scheduling

Committed group replay now explicitly tests a state-version gap in addition to matching-key tombstone, malformed bytes and conflicting duplicate state. Every such matching-key violation fails `Corrupt`; unrelated keys remain ignorable.

Resource qualification covers all four frozen shapes named by P6-157:

- adding member 1,025 to a full 1,024-member group fails atomically;
- assigning one member 1,025 partitions fails before state publication;
- supplying 257 subscriptions fails without changing the group snapshot;
- supplying 1,025 offsets fails CommitOffsets preparation without creating any logical offset value.

The existing coordinator count/byte queues, 1,024-group session map, client cache/in-flight/retry limits and replay-page bounds remain in force.

Scheduling qualification combines the existing **10,000-cycle** pure control-priority campaign with real RF3 load. The pure scheduler always selects pending heartbeat/session work ahead of ordinary join/offset pressure. The RF3 join/Leave workload continues committed offset traffic while membership changes, and the silent-member scenarios prove timeout maintenance still reaches durable authority transitions.

## Mechanical qualification

- `cargo fmt --all -- --check` — **PASS**
- `git diff --check` — **PASS**
- `cargo clippy --workspace --all-targets --all-features -j 1 -- -D warnings` — **PASS**
- broker unit tests — **19/19 PASS**
- P2 network integration — **9/9 PASS**
- P4 RF3 integration — **1/1 PASS**
- P5 dynamic integration — **4/4 PASS**
- P6 RF3 routing/failover/churn/public integration — **9/9 PASS**
- client — **10/10 PASS**
- common — **2/2 PASS**
- controller — **91/91 PASS**
- `astralog-groups` — **55/55 PASS**
- protocol — **38/38 PASS**
- frozen P2 golden vectors — **11/11 PASS**
- P6 golden vectors — **6/6 PASS**
- Raft — **122/122 PASS**
- replication — **47/47 PASS**
- storage — **43/43 PASS**
- doc tests — **PASS**
- complete workspace — **467/467 PASS** using `cargo test --workspace --all-features -j 1 -- --test-threads=1`

The serial `-j 1` qualification is intentional on this Windows machine because P6-J established that parallel test-target linking can exhaust the local paging file. It changes no test semantics or coverage.

## Boundary

P6-K is a qualification/runtime-maintenance checkpoint, not phase closure. It does **not** perform the P6-L source/compatibility audit, write `P6_CLOSURE_REPORT.md`, make an exact-HEAD closure claim or start P7.

The next clean checkpoint is **P6-L only — actions P6-159 through P6-176: compatibility audit and P6 closure**. P7 planning/implementation remains disabled until P6-L completes and exact closure HEAD is requalified.
