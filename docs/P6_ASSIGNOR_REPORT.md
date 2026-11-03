# AstraLog P6-E Uniform-Sticky Assignor Qualification

Date: 2026-09-30

Status: **PASS — P6-E QUALIFIED**

## Scope

P6-E closes actions P6-061 through P6-070 only. It adds the frozen deterministic uniform-sticky assignment function to `astralog-groups`. It does not implement cooperative revocation/release/activation, group-fence or owner-fence writes, heartbeat/session runtime, replicated coordinator recovery, offset mutation, or API 30..35 activation.

The production surface is intentionally narrow:

- `crates/astralog-groups/src/assignor.rs` contains the pure assignor and its exact flow solvers;
- `crates/astralog-groups/src/lib.rs` exports that assignor surface;
- no broker/client/controller/Raft/replication/storage/protocol production source is changed by P6-E.

## Frozen input and output

`uniform_sticky_assign` consumes only:

1. canonical `MemberId -> MemberRecordV1` membership/subscription state;
2. the current active `TopicPartition -> MemberId` assignment;
3. committed `TopicId -> partition_count` metadata.

It returns one canonical `BTreeMap<TopicPartitionV0, MemberId>` plus the mechanically observed optimum maximum load and movement count. No clock, RNG, broker load, rack/zone, controller liveness, network, disk or mutable process state participates.

Deleted or now-out-of-range current partitions are not target input and therefore disappear naturally from the new assignment. A current owner that is no longer subscribed to a topic receives no stickiness credit for that topic.

## Exact optimization

The frozen objective is implemented without a heuristic:

1. **Minimum maximum load.** A deterministic Dinic max-flow feasibility graph over topic supplies, eligible members and per-member capacity is binary-searched for the smallest feasible maximum load.
2. **Minimum movement.** With that maximum fixed, deterministic min-cost flow assigns zero cost to retained still-eligible ownership and unit cost to replacing such ownership. Thus new partitions and partitions whose old owner is no longer eligible do not count as movement.
3. **Canonical owner-vector tie-break.** TopicPartitions are considered in canonical ascending order. At each position, candidate MemberIds are considered ascending and the smallest candidate is fixed only when the exact remaining-flow oracle proves that the global optimum maximum load and movement count remain attainable.

This constructs the lexicographically smallest owner vector among assignments that achieve the first two optimum values.

## Same-subscription balance rule

The frozen contract separately requires that when all members have the same subscription, their load spread is at most one. That requirement is treated as an assignment-validity constraint rather than weakened by stickiness. Each identical subscriber therefore has a floor load of `total_partitions / member_count`, while the already-minimized maximum supplies the matching ceil bound.

This distinction matters for cases such as four partitions and three identical subscribers: a sticky `2/2/0` assignment has the same maximum load as `2/1/1`, but violates the frozen spread property and is therefore not a valid P6-E result. The dedicated regression requires `2/1/1`-equivalent load and only then minimizes movement and owner-vector order.

## Mechanical properties

The focused assignor suite proves:

- subscription eligibility is absolute;
- minimum maximum load is exact;
- movement is minimized only for still-eligible existing ownership;
- canonical MemberId owner-vector tie breaking is exact;
- identical subscribers have floor/ceil-balanced load with spread <= 1;
- reordered logical member-map construction yields the same result;
- joining a third identical member to six partitions moves exactly two partitions to reach 2/2/2;
- member removal cannot preserve ownership on the removed member;
- partition expansion preserves every old owner when balance does not require movement;
- missing eligibility, group-partition overflow and per-member capacity exhaustion fail explicitly.

Most importantly, `assignor_matches_exhaustive_frozen_objective_on_small_state_space` enumerates every legal assignment across a bounded heterogeneous two-topic/three-member state space and compares the production solver against the brute-force objective oracle. The production assignment matches the exact optimum in every enumerated case.

## Bounds and complexity boundary

P6-E validates the frozen P6 limits before solving:

- at most 1,024 group members;
- at most 256 subscription topics per member;
- at most 16,384 total group partitions;
- at most 1,024 assigned partitions per member.

The solver is finite and deterministic. P6-E makes no throughput/latency SLA or benchmark claim; performance qualification remains P8. Correctness and bounded input/state are the P6-E gate.

## Qualification evidence

Pre-commit qualification on the P6-E bytes:

- `cargo fmt --all -- --check`: **PASS**
- workspace Clippy with `-D warnings`: **PASS**
- `astralog-groups`: **24/24 PASS**
- frozen P2 protocol goldens: **11/11 PASS**
- P6 protocol goldens: **6/6 PASS**
- P6-C real RF3 routing/bootstrap/failover scenario: **1/1 PASS**
- frozen P5 dynamic suite: **4/4 PASS**
- controller: **91/91 PASS**
- protocol: **38/38 PASS**
- Raft: **122/122 PASS**
- replication: **46/46 PASS**
- storage: **43/43 PASS**
- complete workspace: **418/418 PASS**

One earlier combined sequential compatibility process stalled inside the existing P5 dynamic harness after P6-C had already passed. It was stopped without changing code. The P5 dynamic suite then passed **4/4** immediately in isolation and again **4/4** inside the complete 418-test workspace, so no P6-E or P5 semantic repair was required.

## Boundary

P6-F actions P6-071 and later remain untouched. In particular P6-E does not publish PREPARING/REVOKING cooperative transitions, generate revocation sets, write cross-topic fences, activate target owners, or create any new broker/client runtime behavior.

The next bounded checkpoint is P6-F only — actions P6-071 through P6-084.
