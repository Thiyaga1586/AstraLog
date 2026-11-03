# AstraLog Research and Design References

This file records the external systems knowledge used to shape AstraLog. The implementation does not copy those systems wholesale.

## 1. Raft

**Diego Ongaro, John Ousterhout — In Search of an Understandable Consensus Algorithm (Extended Version)**

https://raft.github.io/raft.pdf

Applied lessons:

- separate election, replication, and safety rules
- up-to-date-log election restriction
- majority intersection
- current-term commit rule
- explicit membership-change safety
- formal specification/model checking is practical for consensus

## 2. Raft reference material

https://raft.github.io/

Applied lesson:

- keep a formal/model-based version of consensus behavior alongside implementation
- keep the consensus core deterministic and separate from transport/disk I/O so
  persistence ordering and fault schedules can be tested mechanically

Production reference consulted for this separation and for pre-vote/joint-
consensus implementation patterns:

https://github.com/etcd-io/raft

## 3. etcd learner design

https://etcd.io/docs/v3.3/learning/learner/

Applied lessons:

- a joining node should first be non-voting
- promote only after catch-up
- do not let an empty learner become leader

This directly informs AstraLog replica movement and controller membership.

## 4. Apache Kafka design/configuration

Current docs used during P0 research:

https://kafka.apache.org/43/design/

https://kafka.apache.org/43/configuration/

https://kafka.apache.org/43/configuration/broker-configs/

https://kafka.apache.org/43/generated/producer_config.html

https://kafka.apache.org/43/implementation/message-format/

Applied lessons:

- log abstraction with immutable segments
- committed/high-watermark visibility boundary
- explicit acks trade-offs
- minimum replica/durability thinking
- producer ID, epoch/sequence-style idempotency
- bounded in-flight requests matter for ordering/deduplication
- CRC32C at record-batch level
- batch compression
- sparse indexes and OS page-cache-friendly sequential I/O
- compaction keeps logical offsets stable

AstraLog differs by using Raft per data partition instead of Kafka's replication/controller model.

## 5. Kafka consumer rebalance protocol

https://kafka.apache.org/43/operations/consumer-rebalance-protocol/

Applied lessons:

- incremental rebalancing reduces disruption
- server-side coordination can simplify clients
- assignment and heartbeat/session ownership are first-class protocol state

AstraLog therefore starts with cooperative/incremental rebalancing rather than implementing eager rebalancing first.

## 6. Apache Pulsar architecture

https://pulsar.apache.org/docs/next/concepts-architecture-overview/

Applied lessons:

- clean separation between serving/coordination concerns and durable log concerns
- persistent cursors/subscriptions are powerful
- ledger/segment rollover simplifies retention and recovery

AstraLog keeps broker-local partition storage instead of separating storage into BookKeeper.

## 7. Apache BookKeeper protocol

https://bookkeeper.apache.org/docs/development/protocol/

Applied lessons:

- explicitly distinguish ensemble/write/ack quorum concepts
- durable replica acknowledgements matter
- fencing old writers is a core recovery requirement
- quorum design must be reasoned about mathematically, not operationally

AstraLog chooses Raft-majority commit for each partition rather than BookKeeper's ledger quorum protocol.

## 8. Linux fsync/fdatasync semantics

https://man7.org/linux/man-pages/man2/fsync.2.html

Applied lessons:

- page-cache write is not equivalent to durable completion
- fsync/fdatasync failures must be propagated
- directory entries require their own durability handling when file creation/rename durability matters

## 9. Amazon SQS visibility and delivery semantics

https://docs.aws.amazon.com/AWSSimpleQueueService/latest/SQSDeveloperGuide/sqs-visibility-timeout.html

https://docs.aws.amazon.com/AWSSimpleQueueService/latest/SQSDeveloperGuide/designing-for-outage-recovery-scenarios.html

Applied lessons:

- visibility lease prevents simultaneous normal processing but does not magically create exactly-once worker execution
- expiry/retry produces at-least-once behavior
- poison messages need a DLQ policy
- retry semantics and ambiguity must be explicit

## 10. Amazon SQS FIFO deduplication and grouping

https://docs.aws.amazon.com/AWSSimpleQueueService/latest/SQSDeveloperGuide/FIFO-queues-exactly-once-processing.html

https://docs.aws.amazon.com/AWSSimpleQueueService/latest/SQSDeveloperGuide/sqs-fifo-queue-message-identifiers.html

Applied lessons:

- duplicate suppression needs an explicit identity/window/state model
- ordering is naturally scoped to a group/partition, not globally

AstraLog's primary producer idempotency is stronger and log-native: producer ID + epoch + sequence, not only a time-window content hash.

## 11. OpenTelemetry messaging conventions

https://opentelemetry.io/docs/specs/semconv/messaging/

https://opentelemetry.io/docs/specs/semconv/messaging/messaging-metrics/

Applied lessons:

- message send/receive/process operations deserve consistent trace/metric naming
- latency histograms and destination/group/partition attributes must control cardinality

## 12. Redpanda — Raft-per-partition architecture and coordinated compaction

https://docs.redpanda.com/cloud-data-platform/get-started/architecture/

https://www.redpanda.com/blog/kafka-log-compaction-bug-fix-streaming

Applied lessons:

- Raft-per-partition is a practical architecture for a Kafka-style distributed log
- a majority commit can define the durable/visible boundary for a partition
- compaction progress across replicas matters for failover correctness
- tombstone removal must not get ahead of replicas that could later become leader and still contain an older value

AstraLog therefore separates the live Raft suffix from compactable retained history using a snapshot floor and requires coordinated tombstone-safety progress.

## 13. Redpanda — resource/backpressure architecture

https://www.redpanda.com/blog/autotune-series-part-1-storage

Applied lesson:

- distributed streaming brokers need explicit resource reservations/quotas and backpressure rather than relying on unbounded queues or host OOM behavior

## Research conclusion

The core AstraLog architecture deliberately combines:

- **Kafka-like** partitioned ordered logs, direct leader routing, batching, consumer offsets, idempotent sequences, retention, compaction
- **SQS-like** visibility leases, redelivery, retry policy, poison-message handling, DLQ
- a **small Pulsar/BookKeeper influence** in thinking explicitly about durable ledgers/cursors/quorums and retention ownership
- **Raft** as the correctness foundation for both metadata consensus and each partition's committed replicated log

The result is intentionally not wire-compatible or storage-compatible with any of them.
