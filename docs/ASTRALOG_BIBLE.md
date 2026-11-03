# AstraLog Project Bible

Version: **1.0 — P0 architecture frozen**

Status: **Normative**

This document is the architectural constitution for AstraLog. It exists so the project can be implemented phase-by-phase without repeatedly redesigning core assumptions.

---

## 1. Mission

Build a production-grade distributed event-streaming platform from first principles that is small enough to understand end-to-end yet deep enough to demonstrate real distributed-systems engineering.

AstraLog should let an engineer explain and prove:

- append-only logs and WAL durability
- disk layout, indexes, checksums, fsync, and crash recovery
- partitioning and ordering
- leader election and replicated state machines
- quorum intersection and fault tolerance
- CAP trade-offs
- metadata consensus
- fencing and epochs
- consumer-group coordination
- idempotent retries and duplicate suppression
- queue visibility and redelivery
- backpressure and overload control
- compaction and retention
- cluster membership and replica movement
- graceful maintenance and upgrades
- chaos engineering and benchmark methodology

The goal is not feature parity with Kafka or Pulsar. The goal is a coherent system whose important guarantees can be traced from API semantics to bytes on disk.

---

## 2. Scope and explicit non-goals

### 2.1 In scope for AstraLog v1

- Linux-first broker runtime with Windows/macOS development support.
- Rust implementation.
- Custom binary protocol over TCP.
- TLS capability without changing application semantics.
- Topics split into ordered partitions.
- Replication factor 1, 3, and 5.
- `acks=0`, `acks=leader`, and `acks=quorum`.
- Raft consensus implemented by AstraLog itself.
- Controller quorum and Multi-Raft data partitions.
- Replicated consumer-group metadata.
- Cooperative incremental group rebalancing.
- Idempotent producer sessions and sequence-number deduplication.
- Standard streaming subscriptions.
- Work-queue subscriptions with visibility leases, retry count, nack, and DLQ.
- Append-only segments, sparse indexes, CRC32C, recovery, retention, and compaction.
- Batch compression: none, LZ4, Zstandard.
- Metrics, logs, traces, health/readiness, quotas, ACLs, and TLS.
- Deterministic simulation, property tests, fuzzing, chaos tests, and reproducible benchmarks.

### 2.2 Explicitly out of scope for v1

- Using Kafka/Pulsar/BookKeeper/RabbitMQ/NATS/etcd/ZooKeeper as an internal dependency.
- Byzantine fault tolerance.
- Cross-region active-active replication.
- Cross-partition exactly-once transactions.
- SQL/stream-processing DSLs.
- Schema registry.
- Tiered object storage.
- Arbitrary partition shrink.
- Transparent infinite retention.
- Claiming end-to-end exactly-once processing for arbitrary consumers.

Those can become future projects only after v1 is stable.

---

## 3. Architectural principles

### 3.1 Correctness before throughput

Every optimization must preserve a named invariant. If the invariant cannot be stated, the optimization is not ready.

### 3.2 Separate control plane from data plane

The controller quorum owns cluster truth. Data partitions own event truth. Controller traffic must never sit in the hot path of every produce/fetch request.

### 3.3 Direct client-to-leader data path

There is no mandatory centralized API gateway bottleneck. Any broker is a bootstrap/front-door endpoint. A client fetches metadata, caches the partition leader and epoch, then talks directly to that leader.

An optional stateless proxy/gateway may be added later for simple clients, but the core architecture never depends on it.

### 3.4 Stable public semantics, versioned internals

Disk and wire formats contain explicit magic/version fields. Once a phase closes a format version, it is immutable. Changes create a new version plus compatibility/migration logic.

### 3.5 Bounded memory everywhere

No unbounded queues, channels, retry buffers, decompression, batch accumulation, in-flight RPCs, or consumer lease tables.

### 3.6 No silent data repair

If committed data is corrupt, AstraLog must detect it and recover from a healthy replica or fail the replica/partition visibly. It must never silently discard committed records to make startup succeed.

### 3.7 Honest delivery guarantees

- `acks=0`: no delivery guarantee.
- `acks=leader`: local durable append on the current leader, but the record may still be lost if that leader fails before quorum commit.
- `acks=quorum`: committed to the partition Raft majority before success is returned.
- stream consumers: at-least-once unless the application makes processing idempotent.
- queue consumers: at-least-once with visibility leases.
- idempotent producer: prevents duplicate log appends caused by supported producer retry patterns; it is not a claim of global end-to-end exactly-once execution.

---

## 4. Technology foundation

### 4.1 Language

**Rust, stable toolchain**, pinned through `rust-toolchain.toml` and `Cargo.lock`.

Why:

- memory safety without a garbage collector
- strong ownership model for buffer and concurrency correctness
- predictable performance
- excellent async/networking ecosystem
- suitable for binary protocols and storage engines
- strong interview value for systems work

### 4.2 Allowed infrastructure libraries

We may use general-purpose building blocks such as:

- Tokio or equivalent async runtime
- `bytes`-style byte buffers
- rustls for TLS
- CRC32C implementation
- LZ4/Zstd codecs
- tracing/OpenTelemetry/Prometheus client libraries
- CLI/config/serde utilities
- testing/property/fuzzing libraries

We may **not** import a Raft implementation, distributed log, message broker, distributed KV store, or consumer-group coordinator. Those are core learning targets and must be ours.

### 4.3 Primary deployment target

Linux x86_64 first. Containerized local clusters are supported for reproducibility. Windows/macOS are development environments, not the source of truth for disk-durability claims.

---

## 5. System topology

```text
                         +----------------------+
                         |  AstraLog CLI/Admin  |
                         +----------+-----------+
                                    |
                                    v
 Producer/Consumer -----> any broker/bootstrap endpoint
                                    |
                          metadata lookup/cache
                                    |
              +---------------------+---------------------+
              |                                           |
              v                                           v
    +--------------------+                     +--------------------+
    | Controller Raft    |                     | Data Plane Brokers |
    | quorum (3 or 5)    |                     |                    |
    |--------------------|                     | Topic: orders      |
    | cluster membership |                     | P0  P1  P2 ...     |
    | topics/config      |                     | each partition is  |
    | replica placement  |                     | its own Raft group |
    | broker epochs      |                     +--------------------+
    | feature levels     |
    +--------------------+
```

Controllers may be co-located with brokers for a small cluster, but controller responsibilities remain logically separate.

### Topic example

```text
Topic: orders, RF=3

P0 Raft group           P1 Raft group           P2 Raft group
Leader: A               Leader: C               Leader: B
Followers: B,C          Followers: A,B          Followers: A,C

Clients route each partition request to its current leader.
```

---

## 6. Why Multi-Raft

Each partition is an independent Raft replicated state machine.

This locks in several desirable properties:

1. A quorum-acknowledged event survives a minority of replica failures.
2. Leader election safety follows from Raft's election restriction and majority intersection rather than a custom “longest replica wins” heuristic.
3. Old leaders are fenced by term/epoch.
4. Replica membership changes can use learner + joint-consensus style transitions.
5. The commit index gives a precise boundary between committed and uncommitted data.
6. Partitions fail and recover independently.

The controller quorum also uses Raft, but for a different state machine: cluster metadata.

Implementation rule: one operating-system thread per partition is forbidden. Thousands of Raft groups are multiplexed across bounded async executors/shards.

Inter-broker Raft traffic is also multiplexed. AstraLog must not create one TCP connection per partition. A small bounded connection pool per broker pair carries frames tagged with partition/Raft-group identity, with control traffic protected from data-plane starvation.

---

## 7. Failure and consistency model

### 7.1 Faults we must handle

- process crash / `kill -9`
- machine loss
- network partition
- packet loss, delay, duplication, and reordering
- slow follower
- controller leader failure
- disk full
- partial/torn active-segment write
- checksum corruption
- broker restart with stale local state
- rolling mixed-version cluster

### 7.2 Faults we do not claim to handle

- malicious/Byzantine broker
- compromised TLS identity
- correlated loss of a majority of all durable replicas
- storage device that lies about persistence beyond the OS/device guarantees we can test

### 7.3 Consistency

- Cluster metadata: linearizable through controller Raft.
- Each partition: one total order of committed records.
- No global total order across partitions.
- Default fetch isolation: committed records only.
- `acks=quorum`: CP behavior. A minority partition sacrifices availability instead of acknowledging unsafe writes.
- `acks=leader`: intentionally weaker durability; an acknowledged uncommitted suffix may disappear after failover.

---

## 8. Cluster identity, brokers, membership, and fencing

Every cluster has a stable `cluster_id`.

Every broker has:

- stable `broker_id`
- generated node UUID
- monotonically increasing `incarnation_epoch` assigned/accepted by the controller
- advertised client/internal addresses
- rack/zone labels
- supported protocol/feature versions

On restart/rejoin, stale incarnations are fenced. A process from an older incarnation cannot resume leadership merely because it still owns old files.

### Broker lifecycle

```text
JOINING -> ACTIVE -> DRAINING -> REMOVED
             |
             +-> FENCED
```

The controller persists membership changes through its Raft log.

### Failure detection

- Partition liveness is primarily determined by Raft heartbeats/election timeouts.
- Broker-level health is determined by broker heartbeats to the controller.
- Failure detection is suspicion, not proof; safety comes from quorum and fencing, not from perfect clocks.

---

## 9. Topic and partition model

### Topic

Normative metadata:

- topic UUID
- human name
- partition count
- replication factor
- retention policy
- cleanup policy: delete, compact, compact+delete
- min/max batch/message sizes
- queue/stream mode options
- compression policy
- quota/security policy

### Partitioning

Keyed record:

```text
partition = positive(murmur3_32(key)) % partition_count
```

The hash algorithm is protocol-defined and never language-runtime-defined.

Unkeyed records use a sticky partitioner to batch efficiently while distributing load.

Partition count may increase but never shrink in v1. Increasing the count can change the partition chosen for a key; therefore ordering for a key is guaranteed within one partitioning epoch, not transparently across partition-count expansion. Admin tooling must display this warning before expansion.

---

## 10. Storage engine

Each replica owns local partition storage.

```text
data/
  <topic-uuid>/
    <partition-id>/
      partition.meta
      00000000000000000000-00000000000000000001-g00000000.log
      00000000000000000000-00000000000000000001-g00000000.index
      00000000000000000000-00000000000000000001-g00000000.timeindex
      00000000000000000000-00000000000000000001-g00000000.raftindex
      ...
      producer.snapshot
```

### 10.1 Segment rules

- One active mutable segment.
- Rolled segments are immutable.
- Segment roll by configured bytes and/or time.
- Segment identity contains the base stream offset, base Raft index, and file generation. Including the Raft index avoids filename collisions across control-only entries that do not consume stream offsets.
- Sparse offset index maps stream offset -> byte position.
- Time index maps timestamp -> approximate offset.
- Raft index maps consensus log index -> byte position.
- Indexes are rebuildable from the log.

### 10.2 Physical log frame

The exact V1 byte layout is frozen in P1, but architecture requires every frame to carry:

- total frame length
- magic/version
- frame type: data batch or internal control entry
- Raft term
- Raft log index
- base stream offset when applicable
- record count
- timestamps
- flags/compression
- producer ID / producer epoch / base sequence when idempotent
- payload length and uncompressed length
- CRC32C over the stored bytes protected by the format

The log is self-describing enough to scan without trusting the index.

### 10.3 Record batch

Compression happens at batch granularity. The exact compressed bytes accepted by the leader are replicated and written by followers; followers do not decompress/recompress in the replication path.

### 10.4 Durability

For durable acknowledgements, “persisted” means the storage layer has completed the required file-data synchronization operation.

Group commit is allowed:

1. collect bounded batches
2. append them sequentially
3. issue one durability flush for the group
4. mark all covered entries locally durable

When creating/renaming segment files, directory durability must also be handled; syncing a file alone does not guarantee the directory entry is durable.

### 10.5 Crash recovery

On startup:

1. validate partition metadata
2. open the final active segment
3. scan frames from the last trusted checkpoint
4. validate lengths and CRC32C
5. truncate only an incomplete/corrupt **uncommitted active tail**
6. rebuild missing/stale indexes
7. restore Raft persistent state
8. restore/rebuild producer dedupe state
9. verify committed boundary

A CRC failure inside committed data marks the replica unhealthy. The repair path is replica replacement/copy from a healthy peer, not silent truncation.

---

## 11. Raft engine contract

The Raft implementation is a reusable AstraLog core crate, not embedded ad hoc in the broker.

Persistent state:

- current term
- voted-for
- log entries
- commit/snapshot metadata needed for safe recovery

Volatile state:

- role: follower/candidate/leader/learner
- commit index
- last applied
- per-follower next/match index on leaders

Required protocol:

- RequestVote / vote response
- AppendEntries / response
- pre-vote
- leader heartbeats
- snapshot/install-snapshot path
- leadership transfer
- learner/non-voting member
- membership transition

### Mandatory safety rules

- one vote per term
- vote only for an up-to-date candidate
- current-term entry required before leader advances commit by quorum rule
- follower rejects conflicting prefix and truncates only uncommitted divergence
- follower acknowledges an append only after required local persistence
- term/epoch fences stale leaders

### Timers

Consensus correctness must not rely on synchronized wall clocks. Election/heartbeat logic uses monotonic time. Election timeout is randomized and significantly larger than heartbeat interval.

---

## 12. Produce path

```text
Producer
   |
   | metadata(topic)
   v
partition leader
   |
   +--> validate epoch, size, ACL, quota, producer sequence
   |
   +--> append batch to partition Raft log
   |
   +--> replicate AppendEntries to followers
   |
   +--> followers persist and ACK
   |
   +--> leader advances commit index/high watermark
   |
   +--> response according to ack mode
```

### Ack semantics

#### `acks=0`

Client does not wait for a response. The request can be dropped before persistence and the client cannot distinguish success/failure.

#### `acks=leader`

Leader locally appends and durably flushes the entry before success. It does **not** wait for majority commit. If leadership is lost before replication commits the entry, the acknowledged entry can later be truncated.

#### `acks=quorum`

Success only after the entry is Raft committed by a majority of current voters. This is the default production mode.

For RF=1, quorum is one replica, so `acks=quorum` provides local durability but no machine-failure tolerance. AstraLog v1 intentionally supports RF values 1, 3, and 5; production fault tolerance requires RF>=3.

---

## 13. Fetch path

Consumer fetch requests specify:

- topic/partition
- starting stream offset
- max bytes / max records
- minimum bytes and max wait for long polling
- isolation level

Default isolation exposes records only through the committed high watermark.

Long polling avoids hot loops when caught up.

Later optimization: plaintext fetch can use zero-copy/sendfile-like paths for immutable segments; TLS naturally uses encrypted buffers. Optimization cannot bypass checksum/authorization semantics.

---

## 14. Client protocol

### 14.1 Transport

- TCP
- one or more multiplexed persistent connections per broker
- length-prefixed frames
- correlation/request ID
- API key + API version
- explicit error code
- feature/version negotiation
- bounded frame sizes
- optional TLS

The exact V1 header and tagged-field encoding is frozen in P2.

### 14.2 Required public APIs

Bootstrap/admin:

- ApiVersions
- DescribeCluster
- Metadata
- CreateTopic
- DeleteTopic
- AlterTopicConfig
- ReassignPartitions
- DrainBroker

Streaming:

- Produce
- Fetch
- ListOffsets

Producer:

- InitProducer

Consumer groups:

- Join/Subscribe
- Heartbeat
- RevokeAck
- Leave
- CommitOffsets
- FetchOffsets

Work queues:

- QueueReceive
- QueueAck
- QueueNack
- ExtendVisibility

### 14.3 Error model

Errors are typed as retryable/non-retryable and include the relevant epoch/leader hints where safe.

Examples:

- NOT_LEADER
- STALE_EPOCH
- NOT_ENOUGH_REPLICAS
- REQUEST_TIMEOUT
- OUT_OF_ORDER_SEQUENCE
- PRODUCER_FENCED
- GROUP_EPOCH_STALE
- OFFSET_OUT_OF_RANGE
- CORRUPT_MESSAGE
- STORAGE_ERROR
- QUOTA_EXCEEDED

---

## 15. Controller quorum and metadata plane

The controller state machine contains:

- cluster ID
- brokers and incarnations
- topic definitions
- partition replica assignments
- desired placement/rack policy
- broker drain state
- controller feature level
- ACLs and cluster-level quotas/config
- producer-ID allocator state

The controller does **not** store user event payloads.

Controller availability requires a majority of controller voters.

### Placement policy

For RF=3, replicas should occupy distinct brokers and, when configured, distinct rack/zone failure domains.

Leader balancing is a separate concern from replica placement.

---

## 16. Replica synchronization and reconfiguration

New replicas join a partition as **learners**, not voters.

Reassignment flow:

1. controller records desired movement
2. destination replica created as learner
3. learner receives snapshot/segment bootstrap if necessary
4. learner streams remaining Raft entries
5. learner reaches a configured catch-up threshold
6. partition performs safe voting-membership transition
7. optional leadership transfer
8. old replica removed
9. controller marks movement complete

A new slow/empty node must never immediately expand the voting quorum and reduce availability.

---

## 17. Consumer offsets

Offsets are logical stream positions, immutable even after compaction.

Committed consumer offsets are stored in an internal replicated compacted topic, conceptually:

```text
__astralog_offsets
key   = (group_id, topic_uuid, partition)
value = (next_offset, metadata, commit_timestamp, group_epoch)
```

Because the internal topic is itself partitioned and Raft-replicated, offset durability uses the same core machinery as normal data.

Offset commits are fenced by group epoch/member identity so a stale member cannot overwrite the new group's progress.

---

## 18. Consumer groups

A group is mapped to a coordinator by hashing `group_id` to a partition of an internal group-state topic.

Group state includes:

- group ID
- group epoch
- members
- subscriptions
- member/session epochs
- current assignments
- pending revocations
- heartbeat deadlines

### Rebalancing policy

AstraLog implements cooperative/incremental rebalancing from the first production version:

1. existing ownership is preserved where possible
2. only partitions that must move are revoked
3. a partition is not activated on the new owner until revocation is acknowledged or the old member is fenced by timeout/epoch
4. unaffected consumers continue processing

The default assignor is “uniform sticky”: balance partition counts while minimizing movement.

### Processing-fence limitation

Group epochs fence coordinator state and stale offset commits; they cannot undo arbitrary external side effects already performed by an application.

A consumer that becomes stale may still finish work on records it fetched before losing ownership. Applications that require stronger per-message processing leases should use AstraLog work-queue subscriptions, and external side effects should still be idempotent where correctness requires it.

---

## 19. Idempotent producers and duplicate detection

Producer initialization returns:

- producer ID
- producer epoch

Each partition batch carries:

- producer ID
- producer epoch
- base sequence
- record count

The partition leader maintains a bounded recent-batch window per producer.

Rules:

- stale producer epoch -> PRODUCER_FENCED
- expected next sequence -> append
- exact retry of a known recent sequence range -> return original result, do not append again
- gap -> OUT_OF_ORDER_SEQUENCE
- unsupported old retry outside the retained dedupe window -> explicit error, never guess

Producer state is derivable from committed log metadata and accelerated by snapshots.

The supported `max_in_flight` window is bounded so the dedupe cache has a provable size.

---

## 20. Stream mode versus work-queue mode

AstraLog exposes two consumption models on the same durable partitioned-log substrate.

### 20.1 Stream subscription

- consumer group owns partitions
- consumer reads monotonically from offsets
- application commits offsets
- suited to event sourcing, CDC, analytics, pipelines

### 20.2 Work-queue subscription

- individual messages are leased to workers
- receive returns an opaque receipt token
- message becomes invisible to that queue subscription until lease expiry
- ack permanently completes it for that queue subscription
- nack/expiry makes it eligible for redelivery
- delivery count increments
- max delivery count can route to DLQ

Queue semantics are at-least-once.

### Queue state model

Queue state is owned by a sharded internal replicated state plane, conceptually `__astralog_queue_state`. A stable hash of `(subscription_id, topic_uuid, partition)` selects the queue-coordinator shard.

For each queue subscription/partition the replicated state contains:

- durable delivery cursor
- bounded in-flight lease map
- completion bitmap/ranges for gaps ahead of the cursor
- delivery-attempt counters for active/retry records

When a contiguous prefix is fully completed, the cursor advances and old per-message state can be discarded.

The delivery window ahead of the cursor is bounded, which also bounds gap/completion metadata even if an early message is slow or repeatedly failing.

Receipt token contains enough identity to fence stale acknowledgements, including subscription, partition, offset, and lease epoch. Tokens are authenticated once security is enabled.

### Queue receive/ack durability

`QueueReceive` does not return a receipt as authoritative until the lease assignment has been quorum-committed in the queue-state shard. If the response is lost, the message may later be redelivered after visibility expiry; this is intentional at-least-once behavior.

`QueueAck`, `QueueNack`, and `ExtendVisibility` are likewise committed state transitions before success is returned.

The in-memory timer wheel/heap is only an acceleration structure. It is rebuilt from persisted queue state after failover.

### Visibility time and clocks

Lease timers use monotonic time while one coordinator is alive. Persisted state includes enough wall-clock deadline information to reconstruct timers after coordinator failover.

Clock skew can cause a lease to become visible somewhat early or late after failover, so production nodes require normal clock synchronization. Safety does not depend on perfect synchronization: early visibility can create a duplicate delivery but cannot mark a message successfully completed by itself.

---

## 21. Retries and DLQs

Retry is a platform primitive, not an infinite immediate redelivery loop.

Policy includes:

- maximum deliveries
- backoff schedule
- jitter
- retryable/non-retryable failure classification
- DLQ topic

Delayed retries use persistent schedule metadata; timers are reconstructable after broker restart. The scheduler never relies solely on in-memory timers.

DLQ transfer uses a deterministic `dlq_transfer_id = (topic, partition, offset, subscription)` carried in the dead-letter record.

The state transition is ordered as:

1. produce the DLQ record and obtain a quorum acknowledgement
2. only then mark the source queue item terminal/completed

If the DLQ result is ambiguous across a coordinator failure, the transfer may be retried and **a duplicate DLQ record is permitted**. All such copies carry the same deterministic transfer ID so downstream processing can collapse them. AstraLog chooses possible duplicate DLQ output over silent loss of the original failed work.

---

## 22. Retention

Stream topics:

- time-based retention
- size-based retention
- delete policy can operate independently of consumer lag

Queue-oriented topics may opt into **subscription-aware retention**, preventing deletion of messages still required by protected queue subscriptions.

Disk-safety policy still wins: operators must configure maximum backlog/age and alarm thresholds. “Keep until consumed forever” is not an excuse for uncontrolled disk exhaustion.

Deletion only removes whole immutable segments that are safe under the chosen policy.

---

## 23. Log compaction

Compaction keeps the latest value for each non-null key while preserving surviving record order and original offsets.

Rules:

- never compact the active segment
- never rewrite an entry range that Raft may still need for log matching or follower catch-up
- compaction operates only below a committed **Raft snapshot floor**; below that floor, the retained stream-history segments are state-machine history, not live Raft-replication suffix
- offsets never get renumbered
- tombstones represent deletes
- tombstones have configurable grace time
- compaction is copy-on-write into temporary segments
- new compacted segment set is fsynced and atomically swapped
- crash recovery can distinguish old, new, and incomplete temporary generations
- producer-sequence metadata needed for idempotence must remain recoverable even when payload records are compacted

### 23.1 Raft snapshot floor versus stream retention

A Raft snapshot is **not** permission to delete event history that stream retention still promises to consumers.

A partition therefore distinguishes:

- **Raft replication suffix** — entries above the snapshot floor still needed for normal Raft matching/catch-up
- **retained stream history** — committed event segments below that floor, kept according to retention/compaction policy

A lagging/new replica that is behind the snapshot floor is bootstrapped from a partition snapshot bundle containing the required replicated state plus the currently retained stream-segment generation, then receives the live Raft suffix.

### 23.2 Coordinated tombstone safety

Tombstone removal is replica-coordinated.

Each eligible voting replica reports a monotonic `clean_compacted_offset`: below this point it has removed superseded values consistently.

The current leader computes a safe minimum and commits advancement of the partition's `compaction_safe_offset` as replicated control state. A future leader therefore inherits the same monotonic safety boundary.

A tombstone is physically removable only when:

1. its grace period has elapsed,
2. the record is below the committed/snapshot-safe boundary, and
3. every replica that is currently eligible to become leader has compacted the relevant older values past the required boundary.

An offline replica that falls behind this compaction safety boundary must rebootstrap before it can become an eligible voter/leader.

This prevents a failover from exposing an old value after the tombstone that deleted it has already been removed elsewhere.

---

## 24. Batching and compression

Producer batching is per partition.

Tunables:

- max batch bytes
- max batch records
- linger time
- max in-flight batches

Compression options:

- none
- LZ4
- Zstandard

The broker validates declared sizes and enforces a maximum decompressed size to prevent compression bombs.

Batching and compression must preserve per-partition ordering.

---

## 25. Backpressure and overload control

Backpressure exists at multiple layers:

### Client

- bounded accumulator
- bounded in-flight requests
- delivery timeout
- retry budget with jitter

### Connection

- bounded decoder frame
- bounded pending-response bytes
- TCP naturally applies flow control

### Broker

- bounded request queues
- separate control/data work classes
- memory high/low watermarks
- per-principal and per-topic quotas
- per-partition in-flight replication limits
- disk queue-depth/fsync pressure feedback

### Replication

Slow followers cannot force unbounded leader memory. Replication streams from persisted segments with bounded windows.

Overload must degrade into explicit latency/throttling/errors, not OOM.

---

## 26. Graceful draining

`astralog-admin broker drain <id>`:

1. controller marks broker DRAINING
2. broker stops accepting new partition leadership
3. leaders transfer to suitable caught-up voters
4. replica reassignments move data away in a throttled manner
5. consumer/group coordinator roles migrate
6. broker reaches zero required leadership/replica obligations
7. health endpoint reports safe-to-stop

Drain is resumable and observable.

---

## 27. Rolling upgrades

Every node advertises:

- binary version
- min/max wire protocol version
- min/max disk format version it can read/write
- supported feature flags

Cluster feature level is controller-managed.

Upgrade pattern:

1. roll new binaries while retaining old feature level
2. verify mixed-version compatibility
3. once every required node supports the new feature, explicitly bump feature level
4. only then may brokers emit the new irreversible format/behavior

Downgrade support is documented per feature.

---

## 28. Security model

P11 production security baseline:

- TLS for client and inter-broker traffic
- optional mutual TLS for broker/controller identity
- authenticated principals
- topic/group/cluster ACLs
- rate quotas
- administrative audit log
- secret redaction in logs
- no unauthenticated admin listener in production profile

Encryption at rest is not a v1 core-storage responsibility; use encrypted volumes unless a later ADR adds native encryption.

---

## 29. Observability

Signals:

- structured logs
- Prometheus/OpenMetrics metrics
- OpenTelemetry traces
- health/live/readiness endpoints

Core metric families:

- produce/fetch throughput
- request latency p50/p95/p99/p999
- fsync latency
- bytes written/read
- Raft term/election count
- leader changes
- commit lag
- follower replication lag
- under-replicated partitions
- controller quorum health
- consumer lag
- rebalance count/duration
- queue in-flight/visible/DLQ counts
- retention/compaction bytes
- request throttling
- connection/memory/disk saturation

Cardinality is controlled: raw message keys, arbitrary client data, and unbounded IDs are not metric labels.

---

## 30. Repository architecture

```text
AstraLog/
  Cargo.toml
  rust-toolchain.toml
  crates/
    astralog-common/
    astralog-protocol/
    astralog-storage/
    astralog-raft/
    astralog-controller/
    astralog-broker/
    astralog-client/
    astralog-groups/
    astralog-queue/
    astralog-admin/
    astralog-observability/
    astralog-bench/
    astralog-chaos/
    astralog-sim/
  spec/
    tla/
  tests/
    integration/
    compatibility/
    chaos/
  deploy/
    docker/
  scripts/
  docs/
    ADR/
```

Dependency direction is enforced. Lower layers never import higher layers:

```text
common
  ^
protocol  storage
   ^       ^
   +--- raft
        ^
 controller / groups / queue
        ^
      broker
        ^
 client/admin/bench/chaos
```

The actual crate graph will be validated in P0/P1 and cycles are forbidden.

---

## 31. Formal and deterministic correctness work

The project will not rely only on happy-path integration tests.

### TLA+ / model specifications

At minimum:

- Raft leader election/log safety
- partition membership transition
- producer sequence/deduplication state machine
- cooperative group ownership fencing

### Deterministic simulation

`astralog-sim` supplies:

- deterministic clock
- simulated network
- programmable packet drop/delay/reorder
- node crash/restart
- storage faults
- seeded reproducibility

Every discovered distributed-systems bug gets a minimal deterministic regression scenario.

### Property/fuzz testing

- arbitrary segment truncation/corruption
- arbitrary frame bytes for wire decoder
- sequence-number state machine
- offset index rebuild
- compaction crash points

---

## 32. Chaos test matrix

Mandatory scenarios before v1:

1. kill partition leader during sustained quorum writes
2. kill follower and let it rejoin/catch up
3. kill controller leader
4. isolate current partition leader from the majority
5. isolate one follower
6. asymmetric packet loss
7. slow one replica severely
8. restart all nodes one-by-one
9. disk-full on leader
10. disk-full on follower
11. corrupt bytes in active uncommitted tail
12. corrupt bytes inside committed historical segment
13. add replica learner, kill source mid-copy, resume
14. drain broker under load
15. rolling upgrade under load
16. rebalance consumers while one consumer is slow
17. retry storm / poison message to DLQ

The oracle records acknowledged unique message IDs and verifies the expected guarantee for each ack mode.

---

## 33. Benchmark constitution

No benchmark number is accepted without:

- git commit/tag
- compiler/toolchain version
- AstraLog config
- OS/kernel
- CPU model/core count
- RAM
- disk model/filesystem
- network topology/link
- topic/partition count
- replication factor
- ack mode
- compression/batch size
- producer/consumer concurrency
- message-size distribution
- warmup and measurement duration

### Required workloads

- 100 B events
- 1 KiB events
- 10 KiB events
- keyed and unkeyed
- RF=1 and RF=3
- `acks=leader` and `acks=quorum`
- compression none/LZ4/Zstd
- single producer and multi-producer
- producer-only, consumer-only backlog, and simultaneous produce+consume

### Required reported metrics

- events/sec
- MiB/sec
- p50/p95/p99/p999/max producer latency
- fetch latency
- CPU
- RSS
- disk write/read throughput
- fsync latency
- network throughput
- replication lag

### Failure benchmark

Under a stable workload:

1. record the active leader
2. `kill -9` that broker
3. measure last successful pre-failure ack
4. measure failure detection
5. measure new leader election
6. measure first successful post-failover quorum ack
7. consume and verify every successfully quorum-acknowledged message
8. verify duplicate count for idempotent producer mode

The README may display output such as:

```text
Broker failure detected:      <measured>
New leader elected:           <measured>
Producer recovery:            <measured>
Messages acknowledged:        <measured>
Messages lost:                0   # only if oracle proves it
Duplicates:                   0   # only if oracle proves it
```

Values are generated, never invented.

---

## 34. Production readiness definition

AstraLog v1 is not “production ready” because it compiles or survives one demo.

Release requires:

- every phase gate closed
- no unresolved correctness-critical TODO
- disk/wire compatibility tests
- deterministic fault tests
- multi-process integration suite
- sanitizer/fuzz/property coverage appropriate to unsafe/binary parsers
- chaos matrix closure
- security baseline
- observability baseline
- reproducible benchmark report
- operator runbook
- backup/recovery story for controller metadata
- rolling-upgrade proof between supported versions
- clean repository and tagged release

---

## 35. Architecture freeze policy

The architecture is intentionally hard to change after P0.

### P0 freezes

- Rust
- controller quorum + Multi-Raft partitions
- direct-to-leader client data path
- segmented append-only storage
- Raft-based quorum semantics
- internal replicated topics for offsets/group state
- cooperative consumer groups
- stream + work-queue consumption model
- no external broker/consensus dependency

### P1 freezes

- disk format V1
- recovery contract

### P2 freezes

- wire protocol V1
- public error taxonomy

### P3 freezes

- Raft persistent-state and safety contract

Later phases may optimize implementations, but cannot weaken a frozen contract.

Any architectural change after freeze requires:

1. ADR
2. explicit problem statement
3. alternatives considered
4. safety impact
5. compatibility impact
6. migration plan
7. new tests/proofs
8. version bump where needed

“It is faster/easier” alone is never sufficient.

---

## 36. Final engineering doctrine

When uncertain, ask these questions in order:

1. What externally visible guarantee are we trying to provide?
2. What invariant makes that guarantee true?
3. Which component owns that invariant?
4. What durable state survives a crash?
5. What happens if the leader dies at this exact instruction?
6. What happens if the response is lost but the request succeeded?
7. What happens under a network partition?
8. What happens after restart with a torn write?
9. Is memory bounded?
10. Can the test harness prove the behavior rather than merely observe it once?

If those answers are clear, implementation can proceed without architectural improvisation.
