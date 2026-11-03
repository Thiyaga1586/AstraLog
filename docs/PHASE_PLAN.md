# AstraLog Phase Plan and Exit Gates

This is the execution roadmap. A later phase may begin only when the previous phase's **must-pass exit gate** is green.

No phase is closed by “looks good”. Closure requires committed artifacts and reproducible evidence.

---

## P0 — Architecture & Correctness Constitution

### Goal

Freeze the macro-architecture before production implementation.

### Deliverables

- project bible
- phase plan
- correctness invariants
- architecture decision log
- research/reference map
- initial repository/crate topology
- dependency-direction policy
- contribution/build/test conventions
- threat/failure model
- versioning/freeze policy

### Proof obligations

- every requested feature has a named owning subsystem
- no circular architectural dependency
- ack semantics are unambiguous
- control plane and data plane responsibilities are separate
- failure assumptions are written
- delivery claims do not overstate exactly-once guarantees

### Exit gate

- [ ] all P0 documents reviewed against one another for contradictions
- [ ] macro architecture frozen
- [ ] no implementation code beyond harmless scaffolding
- [ ] ADR-001 through ADR-008 accepted
- [ ] P1 task list can be executed without redesigning P0

---

## P1 — Single-Node Durable Log Engine

### Goal

Build and freeze the V1 storage format before networking or replication.

### Features

- append-only partition log
- active + immutable rolled segments
- V1 data/control frame format
- CRC32C verification
- sparse offset index
- time index
- Raft-index mapping structure
- segment roll
- durable flush abstraction
- crash recovery
- index rebuild
- corruption classification
- copy-on-write segment replacement primitive

### Tests

- append/read round trip
- random record sizes
- boundary offsets
- restart after every append point
- truncate active file at every byte boundary for generated small logs
- bit flip in header/payload
- stale/missing index rebuild
- segment roll crash points
- file/directory durability integration test on Linux
- fuzz frame decoder

### Format freeze

Create `docs/FORMAT_V1.md`. Once P1 closes, V1 bytes are immutable.

### Exit gate

- [x] no silent corruption path
- [x] incomplete active tails recover deterministically
- [x] committed-region corruption is surfaced, never truncated away
- [x] indexes are fully rebuildable
- [x] V1 compatibility golden files committed
- [x] storage property/fuzz suite green

---

## P2 — Binary Protocol, Single Broker, Client, Partitioning

### Goal

Make a single-node AstraLog usable over the real network while freezing Wire V1.

### Features

- custom TCP frame codec
- ApiVersions
- Metadata
- Create/Delete/Describe topic
- Produce
- Fetch
- ListOffsets
- correlation IDs
- typed errors
- request deadlines
- bounded frame decoder
- connection multiplexing
- Rust producer/consumer client
- stable Murmur3 partitioner
- sticky partitioning for null keys
- long-poll fetch

### Tests

- protocol golden bytes
- fragmented TCP frame input
- multiple requests in flight
- malformed/oversized frame rejection
- slowloris-style partial frames
- leader metadata refresh path even though single-node
- key partition compatibility vectors
- single-broker end-to-end durable restart

### Protocol freeze

Create `docs/PROTOCOL_V1.md`.

### Exit gate

- [x] wire protocol version negotiation works
- [x] all public errors are documented as retryable or terminal
- [x] client never depends on Rust's runtime hash implementation
- [x] memory stays bounded under malformed/slow clients
- [x] compatibility golden tests green

---

## P3 — Raft Core

### Goal

Implement and prove AstraLog's consensus engine independently from brokers.

### Features

- follower/candidate/leader
- pre-vote
- RequestVote
- AppendEntries
- conflict optimization
- durable term/vote/log state
- commit rule
- learner role
- snapshot/install-snapshot
- leadership transfer
- membership transition
- deterministic state-machine interface

### Verification

- TLA+/formal model of essential Raft safety
- deterministic simulation of elections/replication
- random packet loss/delay/dup/reorder
- crash/restart at persistence boundaries
- minority partition cannot commit
- stale leader cannot commit after higher term is established
- committed prefix preserved across elections

### Exit gate

- [x] Raft safety model passes bounded model checking scenarios
- [x] deterministic simulation runs large seeded campaigns
- [x] all failing seeds are reproducible
- [x] no broker code required to make Raft safe
- [x] persistent-state contract frozen

---

## P4 — Replicated Data Partitions and Quorum Acks

### Goal

Turn single-node partitions into independent Multi-Raft groups.

### Features

- RF 1/3/5
- replica RPCs
- per-partition Raft group
- follower persistence before append acknowledgement
- commit-index -> visible high-watermark mapping
- `acks=0`
- `acks=leader`
- `acks=quorum`
- committed-only fetch
- leader redirect/error with epoch
- replica catch-up
- rebootstrap when follower is behind retention boundary

### Tests

- kill leader before local persist
- kill leader after local persist but before quorum
- kill leader after quorum commit before producer receives response
- duplicate request after lost response
- minority network partition
- lagging follower rejoin
- conflicting uncommitted suffix truncation

### Exit gate

- [x] every quorum-acknowledged record survives any minority replica failure tested
- [x] consumers never observe uncommitted data in default isolation
- [x] leader-only ack loss is demonstrated/documented rather than hidden
- [x] no split-brain committed histories

Normative integration contract: `P4_REPLICATION_V1.md`.

---

## P5 — Controller Quorum, Cluster Membership, Placement

### Goal

Create the authoritative metadata/control plane.

Normative contract: P5_CONTROLLER_V1.md. Execution evidence is tracked in
P5_CHECKLIST.md.

### Features

- controller Raft group
- cluster bootstrap
- broker join/rejoin
- broker incarnation fencing
- topic metadata
- replica assignments
- rack/zone-aware placement
- broker heartbeats
- cluster feature levels
- producer-ID allocator primitive
- add learner / catch-up / promote / remove workflow
- leadership balancing hooks

### Tests

- controller leader kill
- stale broker incarnation reconnect
- duplicate broker ID
- controller minority partition
- add empty broker
- move replicas while producing
- rack-placement constraint failures

### Exit gate

- [ ] one authoritative metadata history
- [ ] stale broker processes cannot regain authority
- [ ] new replicas never become voters before catch-up gate
- [ ] controller outage does not interrupt already-routed data traffic unnecessarily
- [ ] deterministic placement never silently weakens Required rack/zone constraints
- [ ] replica movement is crash-resumable and idempotent
- [ ] producer IDs are never reused
- [ ] feature level changes only through explicit committed controller mutation
- [ ] P1-P4 frozen contracts remain compatible

### Implementation checkpoints

- P5-A: contract/invariants/checklist freeze
- P5-B: controller types + deterministic metadata state machine
- P5-C: durable controller P3 adapter + recovery/snapshots
- P5-D: controller transport + real 3-node election
- P5-E: cluster bootstrap
- P5-F: broker join/rejoin/incarnation fencing
- P5-G: heartbeat + failure detector
- P5-H: topic metadata + deterministic placement
- P5-I: metadata epochs/deltas + broker cache
- P5-J: dynamic assignments replace static P4 production topology
- P5-K: learner/create/catch-up/promote/remove orchestration
- P5-L: crash-resumable replica movement
- P5-M: leadership-transfer hooks
- P5-N: feature-level primitive
- P5-O: producer-ID allocator
- P5-P: multi-process failure qualification
- P5-Q: compatibility/scope audit and closure

---

## P6 — Consumer Offsets and Cooperative Consumer Groups

### Goal

Build durable, scalable stream-consumer coordination.

### Features

- `__astralog_offsets` internal compacted topic
- `__astralog_groups` internal topic
- group coordinator routing
- join/subscribe/heartbeat/leave
- group/member epochs
- uniform-sticky assignor
- incremental cooperative revocation
- offset commit/fetch
- session timeout
- optional static membership foundation

### Tests

- member joins/leaves under load
- member crashes without leave
- coordinator leader fails
- stale member attempts offset commit
- slow member and heartbeat timeout
- partition count expansion
- repeated rebalance churn
- assignment movement-minimization property tests

### Exit gate

- [ ] no partition has two active owners within the modeled coordinator state
- [ ] stale commits are fenced
- [ ] unaffected members continue through incremental rebalance
- [ ] group state survives coordinator failover

---

## P7 — Idempotent Producer and Duplicate Detection

### Goal

Make retries safe for supported producer sessions.

Normative contract: `P7_PRODUCER_V1.md`. Execution evidence is tracked in
`P7_CHECKLIST.md`. P7 implementation is gated by the P7-A contract-freeze commit.

### Features

- InitProducer
- producer ID + epoch
- per-partition sequence numbers
- bounded recent-batch dedupe cache
- max-in-flight bound
- producer-state snapshots
- recovery from log metadata
- stale producer fencing
- retry classification

### Tests

- response lost after successful quorum commit
- retry same batch
- retry after leader failover
- out-of-order sequence
- producer restart with new epoch
- old producer continues sending
- snapshot recovery
- compaction does not destroy required producer-state reconstruction

### Exit gate

- [ ] supported retries produce zero duplicate **committed logical records** for one producer sequence range
- [ ] old producer epoch is fenced
- [ ] sequence gaps never get silently accepted
- [ ] dedupe state is bounded and crash-safe

### Implementation checkpoints

- P7-A: contract/checklist freeze
- P7-B: types/codecs/API versions/golden bytes
- P7-C: controller producer-session authority + V2 persistence
- P7-D: pure partition producer state machine
- P7-E: Storage V1 producer metadata activation + ApplicationCommand V2
- P7-F: runtime admission/pending/dedupe
- P7-G: producer snapshot/recovery
- P7-H: broker/client InitProducer + Produce V1
- P7-I: lost-response/failover/fencing qualification
- P7-J: deterministic models/resource/scheduling stress
- P7-K: compatibility audit and exact-HEAD closure

---

## P8 — Batching, Compression, Flow Control, Performance Baseline

### Goal

Improve throughput without weakening correctness.

### Features

- producer accumulator
- linger/batch-byte/batch-count policies
- replication pipelining
- group commit
- LZ4
- Zstandard
- long-poll fetch tuning
- client and broker bounded queues
- memory watermarks
- token-bucket quotas
- per-partition replication windows
- optional zero-copy plaintext fetch path

### Tests

- ordering across batched retries
- compressed CRC validation
- decompression-bomb rejection
- slow consumer
- slow follower
- client sends faster than disk
- memory remains under configured bounds
- benchmark matrix baseline generated

### Exit gate

- [ ] no performance option changes ack semantics
- [ ] throughput baseline is reproducible
- [ ] p50/p95/p99/p999 are captured correctly
- [ ] no unbounded queue identified by stress test

---

## P9 — Retention and Log Compaction

### Goal

Safely reclaim disk while preserving defined history semantics.

### Features

- time retention
- size retention
- delete cleanup
- compact cleanup
- compact+delete
- tombstones
- compaction grace
- copy-on-write cleaner
- crash-safe segment-set swap
- Raft snapshot/compaction safety floor
- per-replica clean-compacted watermark
- coordinated tombstone removal
- rebootstrap rule for replicas behind the compaction safety boundary
- subscription-aware retention option
- compaction throttling

### Tests

- crash at every cleaner swap stage
- concurrent fetch during compaction
- duplicate keys across many segments
- tombstone expiration
- follower offline across tombstone grace and later rejoins
- leader change while replicas are at different local compaction progress
- prove removed tombstone cannot resurrect an older value
- compacted offsets remain stable
- producer-state recovery after compaction
- retention + lagging replica interaction

### Exit gate

- [ ] compaction never renumbers surviving offsets
- [ ] no committed latest value disappears incorrectly
- [ ] failover cannot resurrect a value whose tombstone was safely removed
- [ ] crash cannot leave ambiguous live segment generations
- [ ] retention never deletes data required by its configured protected policy

---

## P10 — SQS-Style Work Queues, Retries, Visibility, DLQ

### Goal

Layer reliable work-dispatch semantics over the durable log.

### Features

- queue subscriptions
- `__astralog_queue_state` replicated coordinator shards
- receive/ack/nack
- visibility lease
- lease extension
- stale receipt fencing
- in-flight bounded state
- delivery count
- persistent retry schedule
- backoff+jitter
- poison-message policy
- dead-letter topic
- deterministic DLQ transfer identity/dedupe

### Tests

- worker dies mid-processing
- visibility expires
- stale receipt tries to ack after redelivery
- queue-coordinator leader fails immediately after committing a lease but before returning it
- queue-coordinator leader fails immediately after committing an ack but before returning it
- bounded delivery-window test with a blocked earliest message
- broker/coordinator fails with active leases
- poison message reaches max delivery count
- DLQ transfer response lost/retried
- huge number of delayed retries within configured bound

### Exit gate

- [ ] queue contract is explicitly at-least-once
- [ ] stale receipts cannot complete a newer lease
- [ ] scheduler survives restart
- [ ] poison messages stop infinite hot-loop redelivery
- [ ] DLQ transfer cannot silently lose source work; ambiguous retries preserve deterministic transfer identity

---

## P11 — Operations, Security, Draining, Rebalancing, Rolling Upgrades

### Goal

Make AstraLog operable rather than merely functional.

### Features

- admin CLI
- broker drain state machine
- leadership transfer orchestration
- replica rebalancer
- movement throttles
- TLS / optional mTLS
- authn principal mapping
- ACLs
- quotas
- audit log
- Prometheus/OpenMetrics
- OpenTelemetry tracing
- structured logs
- liveness/readiness
- feature-level rolling upgrades
- compatibility suite for N/N+1

### Tests

- drain every broker under traffic
- cancel/resume drain
- rebalance data while clients produce/fetch
- unauthorized API matrix
- TLS rotation
- mixed-version rolling upgrade
- attempted downgrade after irreversible feature bump
- observability cardinality audit

### Exit gate

- [ ] operator can safely remove one broker without data loss
- [ ] mixed supported versions interoperate
- [ ] production profile has no open unauthenticated admin path
- [ ] critical operational state is externally observable

---

## P12 — Production Closure: Chaos, Formal Regression, Benchmark Release

### Goal

Prove the entire project as a coherent system and publish only measured claims.

### Required suites

- full unit/property/fuzz suite
- deterministic simulation campaign
- TLA+/formal specs
- multi-node integration tests
- compatibility tests
- full chaos matrix
- sustained soak test
- disk pressure test
- benchmark matrix
- leader-kill benchmark
- resource-leak test

### Release artifacts

- generated benchmark report
- exact benchmark raw data
- operator runbook
- failure-mode runbook
- architecture diagrams
- public protocol/disk specs
- release checklist
- tagged commit
- reproducible Docker/local-cluster setup

### Exit gate

- [ ] zero missing quorum-acknowledged records in the defined crash/network chaos suite
- [ ] zero duplicate log entries in idempotent-producer oracle suite
- [ ] all benchmark figures trace to raw generated data
- [ ] no unresolved correctness-critical bug
- [ ] upgrade and recovery runbooks exercised
- [ ] v1.0.0 tag only after all above pass

---

## Phase discipline

At the end of every phase:

1. freeze the phase contract
2. commit tests before/with implementation
3. write a short closure report
4. record benchmark/regression deltas
5. update ADRs if and only if a real architectural decision was required
6. do not carry known correctness debt silently into the next phase
