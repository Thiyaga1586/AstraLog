# AstraLog P6-C Internal Topics and Coordinator Routing Qualification

Date: 2026-09-29

Status: **PASS — P6-C QUALIFIED / P6-D NEXT**

## Scope

P6-C closes actions **P6-033 through P6-044** only. It adds the frozen internal-topic/bootstrap
and coordinator-routing foundation required by `P6_GROUPS_V1.md`.

P6-C deliberately does **not** advertise or execute API keys 30..35, create a durable group state
machine, run the uniform-sticky assignor, perform cooperative rebalance transitions, maintain
member heartbeat/session authority, or apply offset/group records. Those remain later P6 work.

## Frozen internal topics

`astralog-groups` now owns the two canonical descriptors:

| Topic | Partitions | RF | Failure-domain policy |
| --- | ---: | ---: | --- |
| `__astralog_groups` | 64 | 3 | Preferred |
| `__astralog_offsets` | 64 | 3 | Preferred |

The broker's bounded internal bootstrap path routes creation through the already-qualified P5
controller leader. Repeated bootstrap first checks committed controller metadata and validates the
existing topic; a concurrent/lost create result is accepted only when a fresh authoritative
snapshot contains the exact frozen topic. No process-local topic identity is authoritative.

The controller preserves the ordinary public topic-name grammar. Its P6 exception accepts only the
two exact reserved names and only with 64 partitions, RF3 and Preferred policy. The same validation
is used before proposal, while encoding the deterministic controller command, and while validating
recovered controller snapshots. Therefore a malformed internal topic cannot become valid merely by
crossing a restart boundary.

Dynamic P4 topology created from committed controller metadata may carry the two reserved names.
The public/static P4 configuration validator still rejects reserved internal names, so P6-C does
not create a user-facing backdoor into the internal namespace.

## Public lifecycle isolation

The existing public Wire V1 topic-name codec still rejects the `__astralog_` prefix. Broker-side
CreateTopic/DeleteTopic additionally reject the two frozen internal names before mutation. Wire V1
has no public ExpandTopic API. The real P6-C RF3 scenario submits public lifecycle attempts and then
proves the authoritative internal TopicIds are unchanged.

## Frozen GroupId shard mapping

P6-C adds `group_shard(GroupId)`, which delegates directly to the already-frozen P2
`partition_for_key(group_id_bytes, 64)` implementation. No second hash implementation exists.

Compatibility vectors are frozen mechanically:

| GroupId | Shard |
| --- | ---: |
| `g` | 22 |
| `orders` | 9 |
| `consumer-a` | 21 |
| `group-00000000000000000000000000000001` | 59 |

Both internal topics use the same 64-partition descriptor and the same `group_shard` result, so
the group and offsets namespaces are numerically aligned by construction.

## Coordinator authority

P5 controller metadata provides only committed replica placement and broker endpoints. It does not
provide partition leadership.

The client resolver:

1. requests targeted Metadata for `__astralog_groups`;
2. validates the exact 64/RF3 topic shape and the committed replica set for the requested shard;
3. probes only those RF3 brokers with ordinary Metadata;
4. ignores unreachable, foreign-cluster or topology-mismatched observations;
5. selects the highest concrete observed P3 term;
6. requires exactly one concrete leader at that highest term; and
7. requires that leader to belong to the committed RF3 assignment.

It does **not** fall back to a lower-term leader when a newer term is observed with no leader, and
it rejects conflicting leaders at the highest observed term. Deterministic client unit tests freeze
all three cases.

`GroupCoordinatorRoute::hint` exposes the selected broker and P3 term through the frozen
`CoordinatorHintV0` shape. Group/member epochs remain zero/unknown because P6-D and later
checkpoints have not created authoritative group state yet.

On the broker, coordinator authority is rechecked immediately against the local P3 actor. A valid
gate requires:

- local role = Leader;
- observed leader broker = the local broker;
- exact concrete P3 term = the caller's expected coordinator epoch.

Thus a previously valid coordinator term fails after P3 step-down even if a client still holds an
old hint.

## Real RF3 bootstrap/failover/restart proof

`crates/astralog-broker/tests/p6_routing.rs` runs three real controllers and four dynamic brokers.
It creates both 64-partition RF3 internal topics, producing 128 real P4/P3 partition groups.

The scenario proves, in one bounded workflow:

1. all four brokers are committed ACTIVE and Healthy;
2. first bootstrap creates both internal topics through P5 authority;
3. a second bootstrap is idempotent and returns the same TopicIds;
4. controller metadata reports exact 64/RF3/Preferred/ACTIVE topology;
5. public lifecycle attempts cannot change either TopicId;
6. GroupId `orders` resolves to the current P3 leader of its groups shard;
7. the current broker passes exact-term coordinator authority validation;
8. the existing qualified P3 preferred-leader hook transfers that shard to another RF3 voter;
9. the new route has the requested broker and a strictly higher P3 term;
10. the old broker/term is rejected by the coordinator authority gate;
11. the transferred leader is shut down and the client follows the next P3 leader at a still higher term;
12. the stopped broker rejoins with committed incarnation 2 and restarts from its existing data directory;
13. repeated internal-topic ensure/validation does not recreate or remap either internal topic; and
14. the final controller snapshot contains the original groups and offsets TopicIds.

Because 128 real replicated groups are present, the qualification harness uses the production-default
100 ms P4 tick rather than the accelerated 20 ms small-P5 test tick. This avoids manufacturing a
synchronized multi-group election/heartbeat storm while leaving production timing unchanged.

Focused stability before closure: **3/3 PASS**.

## Mechanical qualification

Pre-commit qualification on the P6-C working tree:

- `cargo fmt --all -- --check`: **PASS**
- workspace Clippy with `-D warnings`: **PASS**
- `git diff --check`: **PASS**
- broker library: **14/14 PASS**
- real P6-C RF3 bootstrap/routing/failover/restart scenario: **3/3 PASS**
- client: **6/6 PASS**
- controller: **91/91 PASS**
- `astralog-groups`: **7/7 PASS**
- protocol: **38/38 PASS**
- frozen P2 goldens: **11/11 PASS**
- P6 public goldens: **6/6 PASS**
- frozen P4 RF3: **1/1 PASS**
- P5 dynamic: **4/4 PASS**
- Raft: **122/122 PASS**
- replication: **46/46 PASS**
- storage: **43/43 PASS**
- complete workspace: **401 passed, 0 failed**

One exploratory targeted broker-library run observed the pre-existing bind-then-release ephemeral-port
collision in a P5-J unit fixture; the immediate unchanged rerun passed 14/14 and the complete closure
workspace also passed. The topology uniqueness validator was not weakened and no production behavior
was changed to accommodate that fixture race.

## Frozen boundary audit

P6-C does not:

- advertise API 30..35 in ApiVersions;
- add broker wire handlers for JoinSubscribe/Heartbeat/RevokeAck/Leave/CommitOffsets/FetchOffsets;
- apply or replay GroupRecordV1 as authoritative state;
- allocate MemberIds or advance group/member/state versions;
- run the uniform-sticky assignor;
- execute release/fence/activate cooperative handoffs;
- maintain member session timeout observations; or
- implement the offset state machine.

Those remain P6-D and later checkpoints.

## Next checkpoint

The next clean checkpoint is **P6-D only — actions P6-045 through P6-060: pure durable group state
machine**. P6-D must independently qualify before any later assignor/rebalance/session/offset runtime
is enabled.
