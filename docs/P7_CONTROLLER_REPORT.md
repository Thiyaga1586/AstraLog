# AstraLog P7-C Controller Producer-Session Authority / V2 Persistence Qualification

Date: 2026-10-03

Status: **PASS — P7-C QUALIFIED / P7-D NEXT**

## Scope

P7-C closes actions **P7-030 through P7-045** only. It activates the controller-side
ProducerId/ProducerEpoch authority frozen by P7-A and encoded by P7-B. It does not implement
partition producer sequencing/deduplication, P1 producer metadata activation, broker public
InitProducer/Produce V1 dispatch, producer snapshots, or client idempotent production.

The entering checkpoint is:

`30be93a68e71e331db413cd5c2bd6e0782d37e76` — `P7 qualify types codecs and golden bytes`

## Authoritative state

`ControllerState` now contains one canonical sorted producer-session table keyed by checked
`ProducerId` with checked `ProducerEpoch` values. The controller remains the only issuer of public
producer authority.

New-session application is one staged deterministic transition:

1. active feature level must be at least 3;
2. epoch must be exactly 1;
3. ProducerId must equal the durable P5 `next_unallocated_producer_id`;
4. logical session and canonical snapshot-byte bounds are checked;
5. the session is inserted and the durable allocator advances exactly once;
6. only then is the staged state published at the committed controller log index.

Epoch bumps are compare-and-swap transitions. The ProducerId must already exist, the supplied
expected epoch must exactly match committed authority, and the replacement must be exactly
`checked_next()`. Unknown IDs, stale epochs and overflow fail without partial mutation.

## Mixed V1/V2 controller history

P7-C adds `ControllerLogCommand` as version dispatch only; it does not reinterpret frozen V1 bytes.

- version 1 continues through the exact existing `ControllerCommand` decoder/application path;
- version 2 dispatches only to the P7 producer-session command codec;
- controller persistence validates and replays both forms in one ordered P3 log;
- a qualification history containing V1 ProducerId allocation followed by V2 session creation
  restarts with the exact allocator and authority reconstructed.

P5 broker metadata delta behavior is not overloaded with producer-session records. A V2 session
commit advances controller state, and an older broker metadata epoch naturally takes the existing
snapshot fallback when a V1 metadata delta for that controller index does not exist.

## Snapshot migration and persistence

The existing outer controller Raft snapshot package remains unchanged. Its embedded state is now
version-dispatched:

- a V1 controller snapshot decodes to the historical controller state plus an empty producer-session
  map, preserving `next_unallocated_producer_id` exactly;
- a non-empty producer-session state encodes as ControllerSnapshotV2: exact canonical V1 base bytes
  plus the sorted session table and outer CRC already frozen by P7-B;
- installation validates producer IDs against the durable allocator and the one-million logical cap;
- corrupt CRC/order/authority relationships fail closed before state publication.

The P7-C persistence test commits a producer session, builds and installs the V2 snapshot, discards
the covered log prefix, restarts with no live application prefix, and recovers the exact ProducerId,
ProducerEpoch, allocator and metadata epoch.

## Runtime InitProducer authority

`ControllerClient::init_producer` and the controller node actor are internal P7-C authority surfaces;
they are not the public broker API activation owned by P7-H.

The leader prepares the mutation from speculative committed-plus-pending controller state, proposes
the V2 command through P3, and holds the caller response in a bounded controller waiter. Success is
sent only after the proposal outcome is committed/applied and the runtime state contains the exact
expected authority. Followers reject with `NotLeader`.

This preserves commit-before-response and allows multiple accepted-but-not-yet-applied controller
mutations to reserve distinct producer IDs without creating a second allocation plane.

## Failure / ambiguity qualification

The real three-controller P7-C test proves in one cluster:

- follower InitProducer rejection;
- committed session creation at ProducerId 1000 / epoch 1;
- committed CAS bump to epoch 2 and stale epoch-1 rejection;
- a committed new-session response deliberately dropped after application; a fresh initialization
  consumes ProducerId 1002 rather than reusing the ambiguous ProducerId 1001;
- a committed epoch-bump response deliberately dropped; retrying the stale expected epoch fails
  while epoch 3 remains authoritative;
- current leader shutdown, higher-term successor election and continued bump to epoch 4;
- restart of the old leader and convergence of all controller replicas on the same producer
  authorities.

IDs or epochs may be wasted after an ambiguous caller outcome, but they are never reused or
regressed.

## Bounds

- producer-session logical cap: **1,000,000**;
- controller snapshot envelope: existing **64 MiB** maximum;
- command application remains bounded by existing controller command/pending mutation limits;
- producer-session waiters use the same bounded controller actor/request admission and are removed
  after completion or failed on actor shutdown.

## Mechanical qualification

- focused P7-C state/persistence/snapshot/RF3 tests: **8/8 PASS**;
- complete controller regression: **104/104 PASS**;
- formatting: **PASS**;
- `git diff --check`: **PASS**;
- workspace Clippy `--all-targets -D warnings`: **PASS**;
- broker units: **19/19**;
- P2 network: **9/9**;
- P4 RF3: **1/1**;
- P5 dynamic integration: **4/4**;
- P6 RF3 routing/failover/churn/public integration: **9/9**;
- client: **10/10**;
- common: **3/3**;
- `astralog-groups`: **55/55**;
- protocol: **44/44**;
- frozen P2 goldens: **11/11**;
- P6 goldens: **6/6**;
- P7 goldens: **4/4**;
- Raft: **122/122**;
- replication: **49/49**;
- storage: **43/43**;
- doc tests: **PASS**;
- complete serialized workspace: **493/493 PASS**.

## Boundary

P7-D and later are not implemented by this checkpoint. In particular, there is still no
PartitionProducerStateV1, no sequence/dedupe admission on data partitions, no P1 idempotent producer
metadata activation in P4 persistence, no public broker InitProducer/Produce V1 service, no
`producer.snapshot`, and no idempotent producer client runtime.

The next clean checkpoint is:

> **P7-D only — P7-046 through P7-061: Pure Partition Producer State Machine**
