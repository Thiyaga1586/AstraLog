# ADR-017 — Producer Epoch Authority Is Controller-Issued and Partition-Enforced

Status: **Accepted for P7**

Date: 2026-10-02

## Context

ADR-012 already freezes idempotency around `producer_id + producer_epoch + per-partition sequence`,
but the P0 architecture did not yet specify where producer epochs are durably issued or how that
authority interacts with the direct-to-partition-leader data path.

P7 must satisfy several constraints simultaneously:

- ProducerIds are already allocated from the P5 controller's durable non-reusing counter;
- a newer producer incarnation must fence an older one;
- normal Produce traffic must not synchronously traverse the controller;
- producer state must survive partition leader failover/restart;
- controller outage alone must not revoke already-valid data-plane authority;
- the P1 frame already carries ProducerId/ProducerEpoch/base-sequence metadata;
- stale client-chosen epochs must never become authority merely because they are numerically larger.

## Decision

Producer authority is split deliberately:

1. **Controller producer-session authority** issues globally unique ProducerIds and monotonically
   increasing ProducerEpoch values through controller-Raft committed state.
2. **Partition producer authority** accepts one controller-issued epoch into each data partition
   and owns that producer's per-partition sequence/deduplication state from committed partition
   history.

An unseen ProducerId or higher ProducerEpoch must be confirmed against current controller
authority before it can be proposed on a partition. An epoch already accepted on that partition
does not require a controller lookup on every Produce.

Fencing is therefore intentionally **partition-local**. A newer epoch becomes a durable fence on a
partition when that newer epoch has a quorum-committed idempotent batch there. Before that point,
InitProducer alone does not synchronously fence every partition in the cluster.

P7 supports idempotent Produce only with `acks=quorum`. This makes successful retry state
reconstructable from committed history and avoids claiming duplicate suppression for an
acknowledged-but-truncatable leader-only suffix.

## Consequences

### Positive

- controller remains off the steady-state Produce hot path;
- arbitrary client-selected epochs cannot establish authority;
- partition failover can reconstruct epoch/sequence/dedupe state from the committed event log;
- already-accepted producers can continue during controller outage;
- old epochs are fenced deterministically after newer committed partition authority;
- P1 Storage V1 bytes remain usable without format redesign.

### Deliberate limitations

- InitProducer does not create instantaneous cluster-wide fencing;
- an old epoch may still be valid on a partition that has not yet committed the new epoch;
- ambiguous InitProducer mutation results may waste a ProducerId or ProducerEpoch;
- P7 does not implement transactional producer identity or end-to-end exactly-once processing.

## Alternatives rejected

### Controller validation on every Produce

Rejected because it would place the metadata control plane in the data hot path and violate the
existing direct-to-partition-leader/controller-outage architecture.

### Trust any numerically higher client epoch

Rejected because a stale or malicious client could manufacture authority and fence a legitimate
producer without a committed control-plane transition.

### Broker-local epoch allocator

Rejected because broker failover would create competing producer authority and violate the
single controller metadata history.

### Global synchronous partition fencing during InitProducer

Rejected for P7 because it would turn producer restart into a fan-out transaction over every
partition the producer might have used and introduce a new distributed coordination protocol not
required for retry deduplication.

## Compatibility impact

- Storage Format V1 is unchanged; P7 activates its already-reserved idempotent fields.
- Produce V0 remains unchanged.
- InitProducer V0 uses reserved API key 20.
- Produce V1 adds mandatory producer identity/sequence fields through normal API versioning.
- frozen ControllerCommandV1/SnapshotV1 remain immutable; P7 introduces version 2 controller
  records for producer-session authority.
- P7 irreversible behavior is gated by cluster feature level 3.

## Required proofs

P7 cannot close until tests/model checking prove:

- one controller-issued epoch transition from a given expected epoch wins;
- an old epoch cannot append after newer committed partition authority;
- controller loss does not stop equal already-accepted epoch traffic;
- unseen/higher epoch traffic cannot begin during controller outage;
- response loss after quorum commit and partition leader failover yields one committed logical
  sequence range and the original offsets;
- sequence gaps and unsupported old retries fail rather than being guessed into new appends.
