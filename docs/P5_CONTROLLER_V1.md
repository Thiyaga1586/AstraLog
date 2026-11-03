# AstraLog P5 Controller and Placement Contract V1

Status: **FROZEN FOR P5 IMPLEMENTATION**

This document freezes the P5 control-plane contract. It extends the closed P0-P4 architecture without redefining P1 storage bytes, P2 wire semantics, P3 Raft safety, or P4 data-partition acknowledgement/read guarantees.

## 1. Ownership boundary

P5 owns authoritative cluster intent and metadata:

- cluster identity;
- controller-committed broker membership and broker incarnations;
- topic definitions;
- desired partition replica assignments;
- rack/zone placement policy;
- broker lifecycle state;
- replica-movement intent and reconciliation state;
- cluster feature level;
- producer-ID allocation ranges;
- leadership-preference hooks.

P3 remains the only owner of Raft safety. P4 data partitions remain the only owners of event-log authority. The controller must never independently decide partition leadership, partition term, match/next indexes, commit index, voter promotion/removal, safe truncation, or quorum acknowledgement.

The controller may request a P3 membership transition or leadership transfer and observe its committed result. It may not decree the result.

## 2. Controller topology and identity

P5 uses one dedicated controller Raft group backed by the closed P3 engine.

- supported controller quorum sizes: exactly 3 or 5 voters;
- controller-voter membership is statically bootstrapped for P5;
- dynamic controller-voter reconfiguration is outside P5 scope;
- controller identity is separate from broker identity;
- controller and broker roles may be co-located but identities remain distinct;
- controller group identity cannot collide with any data-partition group identity.

ControllerNodeId is a checked non-zero u64 and maps one-to-one onto P3 NodeId for the controller Raft core. It is not derived from broker_id.

Controller transport routing uses a disjoint group namespace:

~~~text
ControllerGroupKeyV1 = { group_kind = 1, cluster_id }
DataPartitionGroupKey = { group_kind = 2, topic_id, partition_id }
~~~

The group-kind discriminator is mandatory on any transport capable of carrying both namespaces. A controller frame presented as a data-partition frame, or vice versa, is rejected before delivery to P3.

Every cluster has one stable non-zero cluster_id. Bootstrap persists the cluster ID and complete controller voter set before control-plane mutations are served. A process configured for another cluster ID fails closed rather than merging metadata histories.

## 3. Controller consistency contract

Every authoritative metadata mutation is proposed through the current controller Raft leader and becomes externally successful only after:

1. the command is durably present in the controller Raft log;
2. P3 has quorum-committed it; and
3. the controller state machine has applied it in order.

Followers reject or redirect mutations. Controller availability therefore requires a majority of controller voters.

Normal Produce/Fetch and data-partition Raft traffic do not synchronously depend on controller availability once routing and assignments are known.

## 4. Metadata epoch

metadata_epoch is the controller committed/applied log index of the latest metadata-changing command visible in the derived controller state.

Rules:

- it never decreases;
- replay of an already-applied command does not create a new epoch;
- a broker metadata cache may lag but cannot move backwards;
- a broker detects gaps and requests bounded deltas or a metadata snapshot;
- partition leader_epoch remains the P4/P3 Raft term and is not replaced by metadata_epoch.

## 5. Controller durable state

The deterministic controller state machine owns at least:

~~~text
ControllerState
  cluster_id
  active_feature_level
  brokers: BrokerId -> BrokerRecord
  topics: TopicId -> TopicRecord
  topic_names: TopicName -> TopicId
  assignments: (TopicId, PartitionId) -> AssignmentRecord
  movements: MovementId -> MovementRecord
  producer_id_allocator.next_unallocated_id
  metadata_epoch
~~~

Authoritative controller state uses deterministic ordered collections/iteration where output order affects replicated or external results.

The following are observations and are not durable authority merely because a controller observed them: current partition leader/term, follower match_index, partition commit index, heartbeat receive timestamps, and local suspicion timers.

## 6. Controller command and snapshot encoding

P5 freezes deterministic ControllerCommandV1 and ControllerSnapshotV1 encodings before implementation. These are controller-local replicated-state formats, not P2 network bodies.

### 6.1 Primitive encoding

All integer fields are little-endian. Canonical primitives are:

~~~text
u8/u16/u32/u64/i64      fixed width little-endian
uuid                     exactly 16 bytes
bool                     u8; only 0 or 1 accepted
optional<T>              u8 present marker 0/1 followed by T when present
string16                 u16 byte length + canonical UTF-8 bytes
bytes32                  u32 byte length + bytes
array<T>                 u32 element count + canonical T elements
broker_id                u32; u32::MAX remains reserved by P4 mapping
controller_node_id       non-zero u64
incarnation/epoch/gen    u64
~~~

Decoders reject invalid booleans, invalid UTF-8, duplicate set/map keys, non-canonical ordering, integer overflow, configured-bound violations and trailing bytes.

Where a logical set or map is encoded, keys are emitted in ascending canonical key order. Lists whose order is semantically meaningful preserve that order.

### 6.2 ControllerCommandV1 envelope

Every command is:

~~~text
u16 version = 1
u16 command_kind
u32 body_len
body bytes
~~~

body_len must exactly consume the remaining command bytes. No trailing fields are accepted in V1.

Frozen V1 command kinds:

| Kind | Name | Canonical body fields in order |
|---:|---|---|
| 1 | RegisterBroker | broker_id, node_uuid, initial_incarnation, client_host, client_port, internal_host, internal_port, optional rack, optional zone, min_feature, max_feature |
| 2 | RejoinBroker | broker_id, node_uuid, expected_incarnation, new_incarnation, client_host, client_port, internal_host, internal_port, optional rack, optional zone, min_feature, max_feature |
| 3 | SetBrokerLifecycle | broker_id, expected_incarnation, expected_lifecycle, new_lifecycle |
| 4 | CreateTopic | topic_id, topic_name, partition_count, replication_factor, failure_domain_policy, array of initial AssignmentRecord values |
| 5 | BeginDeleteTopic | topic_id, expected_topic_lifecycle |
| 6 | FinalizeDeleteTopic | topic_id, expected_topic_lifecycle |
| 7 | ExpandTopic | topic_id, expected_partition_count, new_partition_count, array of AssignmentRecord values for only the new partitions |
| 8 | BeginMovement | movement_id, topic_id, partition_id, expected_assignment_generation, source_broker_id, destination_broker_id, target_voter array, preferred_leader optional broker_id |
| 9 | AdvanceMovement | movement_id, expected_stage, new_stage |
| 10 | FinalizeMovement | movement_id, expected_stage, replacement AssignmentRecord |
| 11 | SetFeatureLevel | expected_feature_level, new_feature_level |
| 12 | AllocateProducerIds | allocation_count u32 |

BrokerLifecycle V1 numeric values are frozen as JOINING=1, ACTIVE=2, DRAINING=3, FENCED=4, REMOVED=5.

TopicLifecycle V1 numeric values are ACTIVE=1, DELETING=2.

FailureDomainPolicy V1 values are Disabled=0, Preferred=1, Required=2.

MovementStage V1 values are PLANNED=1, LEARNER_REQUESTED=2, LEARNER_CREATED=3, CATCHING_UP=4, PROMOTION_READY=5, ENTER_JOINT_REQUESTED=6, JOINT_COMMITTED=7, LEAVE_JOINT_REQUESTED=8, NEW_STABLE_COMMITTED=9, LEADERSHIP_TRANSFER=10, OLD_REPLICA_REMOVE_REQUESTED=11, OLD_REPLICA_REMOVED=12, COMPLETE=13.

The planner computes TopicId and initial assignments before CreateTopic is proposed. Followers apply the committed assignment bytes; they do not rerun placement during state-machine replay.

AllocateProducerIds contains only a count. Application of that single committed command returns the range beginning at the pre-apply next_unallocated_id and advances the allocator by count. Reissuing after an ambiguous response is a new command and may intentionally waste the old range; it never reuses it.

### 6.3 Canonical records embedded in commands/snapshots

BrokerRecord V1 field order:

~~~text
broker_id u32
node_uuid [16]
incarnation_epoch u64
lifecycle u8
client_host string16
client_port u16
internal_host string16
internal_port u16
rack optional<string16>
zone optional<string16>
min_feature u16
max_feature u16
~~~

TopicRecord V1 field order:

~~~text
topic_id [16]
topic_name string16
partition_count u32
replication_factor u16
lifecycle u8
failure_domain_policy u8
~~~

AssignmentRecord V1 field order:

~~~text
topic_id [16]
partition_id u32
assignment_generation u64
voters array<u32>      // ascending, non-empty, distinct
learners array<u32>    // ascending, distinct, disjoint from voters
~~~

MovementRecord V1 field order:

~~~text
movement_id [16]
topic_id [16]
partition_id u32
expected_assignment_generation u64
source_broker_id u32
destination_broker_id u32
stage u8
target_voters array<u32>      // ascending/distinct
preferred_leader optional<u32>
~~~

### 6.4 ControllerSnapshotV1

Controller snapshots use:

~~~text
u16 version = 1
u16 header_len = 40
u64 metadata_epoch
16-byte cluster_id
u16 active_feature_level
u16 reserved = 0
u64 next_unallocated_producer_id
u32 broker_count
BrokerRecord[broker_count]             // ascending broker_id
u32 topic_count
TopicRecord[topic_count]               // ascending topic_id bytes
u32 assignment_count
AssignmentRecord[assignment_count]     // ascending (topic_id, partition_id)
u32 movement_count
MovementRecord[movement_count]         // ascending movement_id bytes
u32 crc32c
~~~

header_len is the fixed byte offset to broker_count. CRC32C covers every snapshot byte before the final crc32c field.

Derived indexes such as topic_name to topic_id are reconstructed and validated from authoritative records; they are not independently encoded as competing truth.

Snapshot decode validates uniqueness, references, lifecycle values, assignment disjointness, movement references, feature ranges, metadata epoch and allocator bounds before the snapshot can become authoritative.

No P1 partition-data format change is permitted.

## 7. Broker identity and lifecycle

Broker identity is:

~~~text
stable identity = (broker_id, node_uuid)
current authority = incarnation_epoch
~~~

incarnation_epoch is monotonically increased by a committed controller transition for a valid rejoin. The greatest committed incarnation is the only authoritative process for that broker ID.

Lifecycle:

~~~text
JOINING -> ACTIVE -> DRAINING -> REMOVED
             |
             +-> FENCED
~~~

- JOINING: identity committed but no new placement;
- ACTIVE: eligible for placement and leadership;
- DRAINING: existing obligations may remain while new placement is forbidden;
- FENCED: recorded process/incarnation has no authority;
- REMOVED: not part of active broker membership.

Broker health and broker lifecycle are distinct. Missing heartbeats create suspicion; they do not, by themselves, grant or revoke replicated authority.

## 8. Broker registration and rejoin

First registration validates broker ID, node UUID, addresses, rack/zone labels and supported feature ranges, then commits a JOINING record.

Rejoin rules:

- same broker ID + same node UUID may request a newer incarnation;
- new incarnation becomes authoritative only after controller commit/apply;
- stale incarnations are rejected from heartbeat, progress, movement acknowledgement and internal broker authority;
- same broker ID + different node UUID is rejected;
- duplicate node UUID under another broker ID is rejected.

## 9. Heartbeats and failure detection

Broker heartbeats are liveness observations, not per-heartbeat Raft commands.

A heartbeat contains bounded broker ID, incarnation, applied metadata epoch, supported feature ranges and bounded replica/movement progress summaries.

The controller leader validates durable identity/incarnation, updates leader-local last_seen/suspicion state using injected monotonic time, and returns bounded metadata deltas or snapshot instructions.

Controller leadership change discards/rebuilds volatile suspicion timers. Safety never depends on inherited wall-clock timing.

## 10. Topic metadata

P5 makes the controller the authority for topic metadata. Broker-local catalog state is no longer production cluster truth.

A TopicRecord contains at minimum topic UUID, name, partition count, RF 1/3/5, lifecycle and P5 placement metadata while preserving later-phase ownership for retention/security/performance semantics.

Topic creation is atomic with initial assignments. Partition count may increase but never decrease in v1. Delete uses an explicit deleting lifecycle so stale metadata cannot recreate assignments.

## 11. Deterministic placement

Placement is deterministic; random placement is forbidden.

Eligible candidates must be ACTIVE, feature-compatible, distinct from already selected replicas, not administratively fenced, not DRAINING/REMOVED, and compliant with requested failure-domain policy.

Failure-domain policy is Disabled, Preferred or Required. Required is never silently weakened. Preferred may fall back only through an explicit deterministic degraded result.

Deterministic scoring order:

1. satisfy required rack/zone constraints;
2. maximize preferred failure-domain diversity;
3. minimize current replica count;
4. minimize topic-local concentration;
5. broker ID as final stable tie-break.

Replica placement and leader balancing are separate concerns.

## 12. Assignment identity

Each partition has one committed desired AssignmentRecord with a monotonically increasing assignment_generation.

Movement/progress messages are fenced by broker incarnation, assignment generation and movement ID where applicable. Stale observations cannot promote or remove replicas for a newer assignment.

## 13. Replica movement state machine

~~~text
PLANNED
  -> LEARNER_REQUESTED
  -> LEARNER_CREATED
  -> CATCHING_UP
  -> PROMOTION_READY
  -> ENTER_JOINT_REQUESTED
  -> JOINT_COMMITTED
  -> LEAVE_JOINT_REQUESTED
  -> NEW_STABLE_COMMITTED
  -> [optional LEADERSHIP_TRANSFER]
  -> OLD_REPLICA_REMOVE_REQUESTED
  -> OLD_REPLICA_REMOVED
  -> COMPLETE
~~~

The controller may crash between any stages. On restart, reconciliation uses only committed controller intent plus authoritative/observed P3 partition state to compute the next safe idempotent action. Sending an RPC is never evidence that its requested transition occurred.

## 14. Learner-before-voter contract

Every newly added data replica starts as a P3 learner.

Promotion requires controller observation that the learner is durably caught up through the relevant committed boundary plus P3's independent promotion guard and committed membership transition.

Actual voter change occurs only through P3 EnterJoint/LeaveJoint semantics. Controller timeout or heartbeat status can never promote a learner.

Old replica removal is requested only after P3 reports a stable committed configuration without the old replica. P4 ordinary catch-up/rebootstrap is reused.

## 15. Movement reconciliation and bounds

Reconciliation compares committed desired controller state, authoritative P3 membership observations, and fenced broker progress observations. Every action is idempotent.

P5 hard-bounds global movements, per-source/per-destination movements, pending admin operations, controller transport frames/bytes, heartbeat summaries, metadata deltas and controller work queues.

## 16. Metadata dissemination

Brokers cache committed metadata and track applied_metadata_epoch.

~~~text
incoming epoch < local      -> reject stale
incoming epoch == local     -> idempotent duplicate
incoming epoch == local + 1 -> apply atomically
incoming epoch > local + 1  -> gap; request deltas or snapshot
~~~

Heartbeats report broker-applied epoch. The controller returns bounded retained deltas or a full metadata snapshot instruction. Snapshot install cannot regress epoch.

Dynamic replica management consumes committed assignments and replaces P4 static topology in production. Static P4 topology remains for P4 compatibility tests.

## 17. Dynamic P4 integration

P5 uses P4 runtime machinery without modifying P4 consensus semantics.

Dynamic integration creates local replica actors when assigned, creates movement destinations as learners, updates peer address/incarnation maps, fences old peer incarnations, preserves existing replicas during controller outage, removes authority only after movement completion, and preserves removed bytes as non-authoritative/orphaned data for later cleanup.

## 18. Public metadata and admin APIs

P2 already reserved DescribeCluster, Metadata, CreateTopic, DeleteTopic, DescribeTopic, AlterTopicConfig, ReassignPartitions and DrainBroker API keys.

P5 may implement V0 bodies needed for P5 without renumbering existing keys.

- Metadata uses committed broker/topic/assignment state plus observed P3 leader/term;
- controller never manufactures partition leadership;
- Metadata metadata_epoch is the controller epoch;
- DescribeCluster exposes committed P5 cluster metadata;
- ReassignPartitions creates durable movement intent;
- full drain orchestration remains P11;
- later retention/security/performance semantics remain in later phases.

## 19. Controller feature level

P5 implements ADR-015's durable feature-level primitive. Brokers/controllers advertise supported ranges; bump is explicit and controller-committed; unsupported bumps are rejected; active level is monotonic in P5; irreversible future behavior is gated by the active level. P11 later uses this for rolling upgrades.

## 20. Producer-ID allocator primitive

P5 owns only the durable monotonic allocator later consumed by P7. A bounded contiguous range is committed before return. Lost responses may create unused gaps, but a committed ID is never reused. Overflow fails closed. P5 does not implement producer epochs, sequences or deduplication.

## 21. Leadership balancing hook

P5 may request a preferred leadership target only when it is an eligible caught-up voter. Actual transfer uses P3 leadership transfer. Controller observes rather than decrees the result. Continuous balancing policy remains P11.

## 22. Controller outage contract

Without controller majority, already-routed Produce/Fetch, data-partition election/replication, quorum acknowledgement and existing partition failover continue where routing is known.

Topic/admin mutation, join/rejoin, new placement, movement commands, feature-level changes, producer-ID allocation and authoritative metadata refresh may be unavailable until controller quorum returns.

Controller absence alone must not revoke already-valid P4 data-plane authority.

## 23. Mandatory deterministic/failure qualification

P5 mechanically exercises controller leader death around persistence/commit/response boundaries; controller minority isolation; controller restart/snapshot bootstrap; stale incarnation; duplicate identity; empty broker; unsatisfiable placement; movement under writes; source/destination/controller failure at movement stages; premature promotion; stale progress; controller outage during data traffic and partition failover; stale metadata convergence; feature-level bumps; producer-ID failover/lost response; seeded placement/churn campaigns; and bounded movement safety model checking.

Every discovered distributed bug receives a deterministic regression scenario.

## 24. P5 exit invariants

P5 closes only when tests prove one controller-committed metadata history; complete stale-incarnation fencing; monotonic broker incarnation and metadata epoch; deterministic distinct-broker placement; no silent weakening of Required failure domains; learner-before-voter; catch-up-before-promotion; P3 final membership authority; crash-resumable movement; removal-after-committed-exclusion; controller-outage data-plane isolation; producer-ID non-reuse; explicit committed feature level; bounded resources/control scheduling; and P1-P4 compatibility.

## 25. P5 non-goals

P5 does not implement dynamic controller-voter membership; P6 consumer groups/offsets; P7 producer idempotency beyond allocator foundation; P8 performance/compression; P9 retention/compaction; P10 queue semantics; P11 production security/full drain/continuous rebalance/rolling upgrade; or P12 final release benchmarks.

## 26. Implementation checkpoints

- P5-A: contract/invariants/checklist freeze;
- P5-B: controller types + deterministic metadata state machine;
- P5-C: durable controller P3 adapter + recovery/snapshots;
- P5-D: controller transport + real 3-node election;
- P5-E: cluster bootstrap;
- P5-F: broker join/rejoin/incarnation fencing;
- P5-G: heartbeat + failure detector;
- P5-H: topic metadata + deterministic placement;
- P5-I: metadata epochs/deltas + broker cache;
- P5-J: dynamic assignments replace static P4 production topology;
- P5-K: learner/create/catch-up/promote/remove orchestration;
- P5-L: crash-resumable replica movement;
- P5-M: leadership-transfer hooks;
- P5-N: feature-level primitive;
- P5-O: producer-ID allocator;
- P5-P: multi-process failure qualification;
- P5-Q: compatibility/scope audit and closure.

No later checkpoint may weaken an earlier frozen P0-P5 contract.
