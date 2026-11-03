# AstraLog P5-J Dynamic P4 Integration Qualification

Date: 2026-09-27

Status: **PASS — P5-J QUALIFIED**

## Scope

This checkpoint implements exactly P5-J actions P5-128 through P5-144:

- controller-assignment-driven local partition runtimes;
- controller-authoritative internal peer endpoint/incarnation topology;
- stale/fenced internal broker exclusion at the broker embedding boundary;
- controller-outage-safe retention of the existing data plane;
- safe local actor removal only after the committed assignment changes;
- preservation of removed replica bytes;
- controller-authoritative public Metadata;
- P3-observed partition leader/term reporting;
- committed broker incarnation and controller metadata-epoch exposure;
- DescribeCluster V0;
- ReassignPartitions V0 durable movement intent;
- deterministic controller-leader admin routing;
- bounded broker admin/control-plane work.

P5-K/L learner creation, catch-up, promotion, joint-consensus execution, old-replica
removal commands, movement reconciliation and crash-resume orchestration are explicitly
outside P5-J. DrainBroker workflow and AlterTopicConfig are also not implemented here.

## Production topology authority

P5-I established an atomic broker cache of committed `ControllerState`. P5-J makes that
cache the production source of broker/topic/assignment topology.

The dynamic path is:

```text
controller Raft commit/apply
        -> bounded P5-I metadata sync/snapshot
        -> staged BrokerMetadataCache
        -> DynamicReplicationManager topology projection
        -> existing P4 ReplicatedPartition + peer transport runtime
        -> atomic stable broker routing swap
        -> publish staged metadata cache
```

The P4 compatibility path remains available for the frozen P4 test suite, but production
dynamic-controller mode does not use local P2 catalog topics or static assignments as
cluster authority.

No code in `astralog-storage`, `astralog-raft`, or `astralog-replication` is changed by
P5-J. Existing P3/P4 consensus, durability, quorum, acknowledgement, committed-read,
snapshot/rebootstrap and log semantics are reused rather than reimplemented.

## Dynamic actor creation and replacement

`DynamicReplicationManager` converts every committed topic assignment into the existing
P4 topology form. A partition whose voter set contains the local broker produces a local
P4 actor through the same `ReplicatedPartition::open` path qualified in P4.

Reconciliation is publish-after-success:

1. derive the full next topology from the staged committed controller state;
2. if topology is unchanged, leave the existing runtime untouched;
3. otherwise stop the old embedding runtime and open the replacement against the same
   durable partition directories;
4. atomically replace the stable `ReplicationClient` routing snapshot only after the
   replacement opens successfully;
5. if replacement fails, attempt to restore the prior topology and reject metadata-cache
   publication.

The manager refuses learner-bearing assignments in P5-J. Learner runtime creation belongs
to P5-K/L and therefore cannot be accidentally activated by this checkpoint.

## Peer identity, incarnation and lifecycle fencing

Peer client/internal endpoints and transport incarnations are rebuilt from committed
`BrokerRecord`s. A committed broker rejoin therefore changes the expected transport
incarnation before the new topology is published; the already-qualified P4 peer transport
continues to require the exact source incarnation.

FENCED and REMOVED broker IDs are additionally placed in a broker-embedding blocklist:

- no outgoing internal peer channel is created for a blocked remote broker;
- a blocked local broker receives no local partition actors;
- the broker identity can remain structurally represented while an old P3 voter set is
  being safely changed later, so P5-J does not rewrite data-Raft membership behind P3.

This separates controller broker authority from P3 membership authority instead of
silently conflating the two.

## Controller outage rule

Periodic metadata refresh is observation-only when the controller quorum is unavailable.
A failed refresh does not invoke dynamic reconciliation, does not stop actors and does not
alter the currently published metadata cache.

The real P5-J E2E starts a three-controller quorum and a dynamic broker, creates a real
controller-assigned RF=1 partition, waits for its P3 election, then shuts down all three
controllers. After the controller quorum is gone, the broker still successfully completes:

- quorum Produce;
- ReadCommitted Fetch of the produced record.

Controller authority is required for new metadata/admin decisions, not for continued
healthy operation of an already-established data-Raft partition.

## Safe removal and orphan preservation

`BeginMovement` does not modify `AssignmentRecord`. Consequently a committed
`MovementStage::Planned` intent cannot remove the source actor or create a destination
voter in P5-J.

The topology qualification test drives the already-frozen controller movement state
machine to a final replacement assignment only as test setup. It proves:

- source actor remains present after movement intent alone;
- local actor authority disappears only after the final committed assignment excludes it;
- the existing partition directory remains on disk after actor removal.

P5-J therefore removes serving authority, not bytes. Destructive orphan cleanup remains a
later policy/operations concern.

## Public Metadata authority

Dynamic Metadata is constructed from the applied controller snapshot:

- cluster ID: controller cluster ID;
- metadata epoch: controller applied metadata epoch;
- broker identity: committed broker ID/node UUID/incarnation/client endpoint;
- topic identity/lifecycle/partition count/RF: committed `TopicRecord`;
- replica sets: committed `AssignmentRecord.voters`.

Partition leader and leader epoch are deliberately different. They are populated only
from an observed local P4/P3 actor status. If this broker has no authoritative P3
observation, both values remain UNKNOWN. Replica-list ordering is never interpreted as
leadership.

## DescribeCluster V0

The already-reserved DescribeCluster API key is now live only when the broker is running in
dynamic-controller mode. Its bounded V0 body exposes:

- cluster ID;
- controller metadata epoch;
- active feature level;
- currently observed controller leader ID, or zero when unknown;
- committed broker identity, client endpoint and incarnation records.

The codec has canonical round-trip coverage and an exact byte fixture. Legacy P2 brokers
continue to return UnsupportedApiKey for the reserved key, preserving the frozen P2
compatibility behavior.

## ReassignPartitions V0

The already-reserved ReassignPartitions API key now accepts a bounded, strictly ordered,
duplicate-free set of `(topic, partition, source, destination, preferred leader)` intents.
The body codec has canonical validation and an exact byte fixture.

Each accepted request is routed to the current controller leader and commits the existing
`BeginMovement` command. Admission verifies the current assignment generation, source
membership, destination non-membership, absence of an overlapping unfinished movement and
valid preferred-leader target.

P5-J returns the durable `MovementId` and committed metadata epoch. It does **not** advance
the movement beyond `Planned`; the real E2E verifies the assignment is still unchanged.
The learner/catch-up/promotion/removal workflow starts in P5-K/L.

## Admin routing and bounds

`ControllerRouter` orders controller clients by controller node ID and accepts only a
non-fatal P3 leader observation. Admin mutations are sent to that leader and retry at most
twice on `NotLeader`, preventing unbounded redirect loops.

At the broker boundary, pending dynamic admin work is capped by a configured semaphore and
uses fail-fast acquisition. Controller actor/control queues and pending mutation limits
remain independently bounded by the existing P5-D runtime configuration.

Dynamic V0 CreateTopic maps the public request to the already-qualified controller create
path with `FailureDomainPolicy::Disabled`, because the frozen V0 request does not contain a
failure-domain field. DeleteTopic uses the controller ACTIVE -> DELETING -> finalized
lifecycle. DescribeTopic reads the controller cache. DrainBroker and AlterTopicConfig remain
unimplemented.

## Compatibility boundary

P5-J deliberately preserves:

- P1 disk-format bytes;
- P2 existing request/response golden bytes and reserved-key behavior in legacy mode;
- P3 Raft algorithms and membership semantics;
- P4 data-plane consensus, persistence, acknowledgement/read and rebootstrap semantics;
- the static P4 harness for compatibility qualification.

The protocol change is limited to adding V0 body codecs for the two API keys that were
already permanently reserved for DescribeCluster and ReassignPartitions. No API key or
error-code registry numbering is changed.

## Qualification evidence

Mechanical qualification on the P5-J implementation bytes:

- `cargo fmt --all -- --check`: PASS;
- `cargo clippy --workspace --all-targets --all-features --offline -- -D warnings`: PASS;
- `cargo test --workspace --all-features --offline`: **354 passed, 0 failed**;
- broker library: **10 passed, 0 failed**, including two P5-J topology/fencing/orphan tests;
- legacy P2 network suite: **9 passed, 0 failed**;
- frozen P4 RF=3 TCP E2E: **1 passed, 0 failed**;
- real P5-J controller/broker E2E: **1 passed, 0 failed**;
- controller suite: **73 passed, 0 failed**;
- protocol suite: **35 passed, 0 failed** plus **11/11** existing golden tests;
- Raft: **120 passed, 0 failed**;
- replication: **46 passed, 0 failed**;
- storage: **43 passed, 0 failed**.
- real three-controller/dynamic-broker/controller-outage E2E: **5/5 PASS** consecutive runs;
- dynamic topology/incarnation-fencing/orphan-preservation unit campaign: **5/5 PASS** consecutive runs;
- frozen P4 RF=3 TCP quorum/redirect/epoch/failover E2E: **5/5 PASS** consecutive runs.
- controller persistent-peer reconnect harness after parallel-test address isolation: **10/10 PASS** consecutive runs; the adjustment is test-only and does not change controller transport behavior.

## P5-J closure

P5-J is qualified when this report and checklist are committed together with the
implementation and the exact committed HEAD re-passes formatting, Clippy, the full
workspace suite, repeated dynamic/controller-outage qualification, repeated frozen P4 E2E,
and a boundary diff proving no P1/P3/P4-core crate was modified.

The next checkpoint is P5-K/L movement execution and reconciliation, actions P5-091 through
P5-120. It must start from the durable `MovementStage::Planned` intent without redesigning
or weakening the P5-J topology boundary.
