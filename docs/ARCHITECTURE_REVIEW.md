# AstraLog Architecture Review — P0

This document records the major alternatives considered and why they were rejected. Its purpose is to stop future implementation pressure from reopening already-settled macro choices without new evidence.

## 1. Kafka-style ISR replication vs Multi-Raft

### Considered

Implement a controller plus custom leader/follower replication, ISR membership, high watermark, and controller-driven partition elections.

### Rejected for v1

It is valid, but it forces AstraLog to independently invent and prove:

- safe leader-election eligibility
- stale-leader fencing
- committed-prefix preservation
- membership reconfiguration
- quorum acknowledgement semantics

Those are precisely the problems Raft already structures into one replicated-log proof model.

### Chosen

Raft per partition.

The trade-off is many consensus groups, handled by multiplexing rather than one thread per partition.

---

## 2. One global Raft log vs one Raft group per partition

### Rejected

One global Raft log creates a cluster-wide serialization point and couples unrelated topic failures/latency.

### Chosen

Independent partition Raft groups. Ordering and failure domain remain partition-scoped.

---

## 3. External etcd/ZooKeeper vs own controller Raft

### Rejected

An external consensus service would make the project operationally simpler but remove a major first-principles learning objective.

### Chosen

A dedicated AstraLog controller Raft group using the same Raft core crate.

---

## 4. Mandatory API gateway vs direct-to-leader routing

### Rejected

A mandatory gateway would become:

- a throughput bottleneck
- another high-availability tier
- an extra network hop on every produce/fetch

### Chosen

Any broker is a bootstrap/front-door endpoint. Clients cache metadata and route directly to leaders.

An optional stateless gateway can exist later for convenience, but is not a correctness dependency.

---

## 5. Separate storage service like BookKeeper vs broker-local storage

### Considered

Stateless brokers backed by a separate durable-log fleet.

### Rejected for v1

It adds an entire distributed storage tier before the project has proven the core broker/log.

### Chosen

Broker-local replicas. Replica placement across brokers provides durability.

The architecture can teach both disk I/O and consensus directly.

---

## 6. gRPC/HTTP for data plane vs custom binary TCP

### Rejected

They are good production technologies but hide framing, multiplexing, versioning, buffer limits, and binary protocol evolution — all learning targets here.

### Chosen

Custom versioned length-prefixed binary TCP protocol. HTTP may later expose health/metrics/admin convenience, but not the primary event data path.

---

## 7. Eager consumer rebalance first vs cooperative from day one

### Rejected

Building eager first creates a protocol/state model that later has to be redesigned to avoid full stop-the-world revocation.

### Chosen

Incremental cooperative assignment as the baseline.

---

## 8. Stream-only system vs stream + work queue

### Considered

Only Kafka-like consumer offsets.

### Rejected

The project explicitly wants retry queues, visibility-style processing, poison-message handling, and DLQs. Simulating these solely in application code would leave the platform incomplete.

### Chosen

Two subscription modes over the same partitioned durable log:

- stream groups
- work-queue subscriptions

---

## 9. “Exactly once” marketing claim vs scoped guarantees

### Rejected

Idempotent log append does not make an arbitrary database write or email send exactly once.

### Chosen

Use precise claims:

- quorum durability for committed records
- idempotent producer retries for supported sequence windows
- at-least-once stream/queue processing unless an application participates in a stronger protocol

---

## 10. Independent tombstone compaction vs coordinated compaction

### Rejected

If replica A removes a tombstone while replica B still retains the older value that tombstone deleted, then B can later become leader and expose stale state.

### Chosen

- compaction only below a Raft snapshot-safe floor
- per-replica monotonic clean-compacted progress
- tombstone removal only after all currently leader-eligible replicas are safe
- a replica behind that safety boundary must rebootstrap before leadership eligibility

---

## 11. Hard-coded performance targets vs generated benchmark truth

### Rejected

Throughput and latency depend on hardware, message shape, durability mode, batching, compression, partitions, network, and disk.

### Chosen

No fake target numbers. The benchmark harness records exact provenance and generates the published report.

---

## Residual risks to watch during implementation

1. Multi-Raft scheduling overhead with many idle partitions.
2. Disk fsync latency and group-commit trade-offs.
3. Snapshot/bootstrap interaction with long retained stream history.
4. Compaction coordination when replicas remain offline for long periods.
5. Work-queue lease-state explosion if the completion cursor is blocked.
6. Consumer-group edge cases during repeated membership churn.
7. Rolling format upgrades without accidental irreversible writes.
8. Control-plane starvation under data-plane overload.

Every one of these already has an owning phase and must be tested before v1.
