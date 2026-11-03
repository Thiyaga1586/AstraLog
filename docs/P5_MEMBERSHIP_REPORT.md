# AstraLog P5-F Broker Membership Qualification

Status: **QUALIFIED**

P5-F implements actions P5-045 through P5-056 only. Heartbeat request/report
encoding, last-seen tracking, suspicion timers and failure detection remain
deliberately disabled until P5-G.

## Delivered scope

### Durable broker authority

BrokerRecord remains authoritative controller state and is persisted through the
qualified P5-C controller log/snapshot path. It contains broker ID, stable node
UUID, current incarnation epoch, lifecycle, client/internal addresses, rack/zone
labels and supported feature range.

### Membership API

The controller exposes typed broker-membership operations:

- BrokerRegistration always proposes first incarnation 1;
- BrokerRejoin derives new incarnation as exactly expected + 1 and fails on
  overflow;
- BrokerLifecycleChange carries expected incarnation and expected lifecycle;
- BrokerAuthority identifies one concrete broker process as broker ID + node UUID
  + incarnation epoch.

Followers do not forward these operations. They return NotLeader through the
existing P5-D leader-only mutation path.

### Pre-proposal semantic validation

ControllerRuntime now maintains a deterministic speculative validation state made
from committed ControllerState plus accepted pending proposals in Raft-index
order. Semantically impossible commands are rejected before P3 proposal rather
than being allowed into the controller log and failing later during Apply.

The speculative state is rebuilt after committed Apply, leadership loss,
rejected proposals and snapshot replacement. Therefore a follower that later
becomes controller leader validates membership against the latest committed
state rather than stale local assumptions.

The replicated ControllerState remains the final validator at Apply time.

### Identity uniqueness

First registration commits a JOINING BrokerRecord and rejects:

- an already-registered broker ID;
- a node UUID already bound to another broker ID;
- malformed/reserved broker identity fields.

Rejected registration attempts do not advance controller metadata state.

### Rejoin/incarnation authority

Rejoin requires:

- the same broker ID;
- the same committed node UUID;
- expected incarnation exactly equal to the committed incarnation;
- new incarnation exactly expected + 1;
- lifecycle not FENCED or REMOVED.

The public rejoin API derives the next incarnation rather than accepting an
arbitrary new value.

The client-visible success response is still governed by the P5-D rule: it is
returned only after P3 quorum commit and ordered ControllerState Apply. Therefore
when incarnation N+1 success becomes visible, incarnation N is already stale in
committed controller state.

### Shared authority gate

The controller leader exposes a committed-state BrokerAuthority validator. It
rejects:

- unknown broker IDs;
- a foreign node UUID for an existing broker ID;
- any incarnation other than the greatest committed incarnation;
- FENCED brokers;
- REMOVED brokers.

JOINING, ACTIVE and DRAINING may retain process identity authority, but only
ACTIVE is placement-eligible under the frozen placement rules.

P5-051 is qualified through this shared authority gate. P5-G heartbeat handling
must call this gate before accepting a heartbeat, but P5-F itself intentionally
contains no heartbeat request, last-seen timestamp, suspicion state or failure
detector.

### Lifecycle

Qualified committed transitions are:

~~~text
JOINING -> ACTIVE
JOINING -> FENCED
JOINING -> REMOVED
ACTIVE  -> DRAINING
ACTIVE  -> FENCED
DRAINING -> FENCED
DRAINING -> REMOVED
FENCED -> REMOVED
~~~

REMOVED is terminal. FENCED and REMOVED cannot implicitly rejoin. Activation is
also rejected unless the broker supports the current committed cluster feature
level.

## Real three-controller qualification

The P5-F integration scenario runs on the real P5-D three-controller TCP quorum
and P5-C disk persistence. It exercises:

1. broker registration as JOINING/incarnation 1;
2. duplicate broker-ID rejection;
3. duplicate node-UUID rejection;
4. JOINING -> ACTIVE;
5. foreign-UUID rejoin rejection;
6. valid incarnation 1 -> 2 rejoin;
7. immediate fencing of incarnation 1 once incarnation 2 success is visible;
8. controller leader shutdown and higher-term failover;
9. old incarnation remains fenced on the replacement leader;
10. ACTIVE -> DRAINING -> FENCED;
11. FENCED authority and implicit rejoin rejection;
12. FENCED -> REMOVED;
13. REMOVED -> ACTIVE rejection;
14. restart/rejoin of the stopped controller from disk and convergence on the
    terminal REMOVED BrokerRecord.

The scenario passed 5/5 consecutive focused runs before phase-wide qualification.

## Qualification evidence

Pre-commit strict qualification:

- cargo fmt --all -- --check: PASS
- workspace Clippy with -D warnings: PASS
- astralog-controller: 54 passed, 0 failed
- astralog-raft: 120 passed, 0 failed
- astralog-replication: 46 passed, 0 failed
- astralog-storage: 43 passed, 0 failed
- protocol unit tests: 33 passed, 0 failed
- protocol goldens: 11 passed, 0 failed
- full workspace: 326 passed, 0 failed
- all doc tests: PASS
- P1-P4 production-source diff from P5-E: empty
- P5-G heartbeat/failure-detector source audit: no implementation present
- git diff --check: PASS

## Scope boundary

P5-F does not implement heartbeat message bodies, periodic heartbeats, last-seen
timestamps, suspicion thresholds, failure-detector ticks, automatic fencing based
on time, placement, metadata dissemination or dynamic data-partition assignment.
Those remain later P5 checkpoints.

The next bounded checkpoint is P5-G heartbeat and failure detection.
