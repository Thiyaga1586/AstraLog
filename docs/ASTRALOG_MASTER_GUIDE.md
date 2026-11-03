# AstraLog — The Master Engineering & Interview Guide

> **Purpose:** understand AstraLog from first principles, explain every subsystem in plain English and professional systems terminology, defend trade-offs in an adversarial interview, and distinguish demonstrated behavior from future work.
>
> **Source reviewed:** canonical `main` at `d6236c212a102f374ec3bbb7c3eb98355394996d` (2026-11-03), **before this documentation-only addition**. Inventory: 311 tracked files, ten Rust workspace crates. This guide is a navigational synthesis of the actual source, architecture records, tests, and benchmark evidence, **not** a claim that every possible execution was tested or every source line formally proved.
>
> **Status:** interview-demonstrable Rust distributed log; **not production-qualified**. The previous complete serialized local all-features suite passed **737/737** (32 test-result groups), and formatting / warning-denied Clippy passed. Windows power-loss guarantees, authenticated multi-host deployment, hosted Ubuntu CI, and sustained P9-G V2 liveness are not qualified. P10 queues, P11 production security/operations, and P12 release chaos are not implemented. Historical documents can describe intended future designs; this guide distinguishes them from source-evidenced behavior.

## How to use this document

| Time available | Read this | Outcome |
|---|---|---|
| 5 minutes | Sections 1–3 and 17 | Explain what you built and where the hard part is |
| 20 minutes | Sections 4–8, 12 and 17 | Defend production, replication, reads, retries, and results |
| 60 minutes | Sections 9–16 and the interview challenges in 18 | Respond to failure, trade-off, and concurrency questions |
| Deep dive | Sections 19–20 file atlas and original source | Trace a question to real functions, tests, and documents |

### Table of contents

- [1. The project in four explanations](#1-the-project-in-four-explanations)
- [2. Mental model: one real story](#2-mental-model-one-real-story)
- [3. Vocabulary and the crucial guarantees](#3-vocabulary-and-the-crucial-guarantees)
- [4. Technology stack: why each choice and what you give up](#4-technology-stack-why-each-choice-and-what-you-give-up)
- [5. Ten crates — the map of the system](#5-ten-crates-the-map-of-the-system)
- [6. End-to-end flow I: startup, topic creation, Produce and Fetch](#6-end-to-end-flow-i-startup-topic-creation-produce-and-fetch)
- [7. End-to-end flow II: failures and consistency](#7-end-to-end-flow-ii-failures-and-consistency)
- [8. Deep dives: the core engines](#8-deep-dives-the-core-engines)
- [9. Deep dives: controller, broker, groups and producer identity](#9-deep-dives-controller-broker-groups-and-producer-identity)
- [10. P9 retained-history compaction: the hardest storage safety story](#10-p9-retained-history-compaction-the-hardest-storage-safety-story)
- [11. How to create rigorous failure tests](#11-how-to-create-rigorous-failure-tests)
- [12. Benchmark science: numbers and limits](#12-benchmark-science-numbers-and-limits)
- [13. Adversarial audit: A01–A13 and lessons](#13-adversarial-audit-a01a13-and-lessons)
- [14. Future work without feature inflation](#14-future-work-without-feature-inflation)
- [15. Twelve code walks that teach the system to its core](#15-twelve-code-walks-that-teach-the-system-to-its-core)
- [16. Exact commands and test ownership](#16-exact-commands-and-test-ownership)
- [17. High-impact interview presentation scripts](#17-high-impact-interview-presentation-scripts)
- [18. The adversarial interview question bank (answers you can defend)](#18-the-adversarial-interview-question-bank-answers-you-can-defend)
- [19. Every directory: purpose and navigation](#19-every-directory-purpose-and-navigation)
- [20. Complete 312-path annotated source/document inventory](#20-complete-312-path-annotated-sourcedocument-inventory)
- [21. Final rehearsal — five things that earn trust](#21-final-rehearsal-five-things-that-earn-trust)
- [21. Final rehearsal — five things that earn trust](#21-final-rehearsal-five-things-that-earn-trust)

**Evidence hierarchy:** executable source and committed golden fixtures > tests actually run with their recorded source revision > current audit/closure report > phase design / normative architecture intent. A design document saying *must* does not imply the feature currently exists. Never present a P9-G local candidate as a production qualification.

---

# 1. The project in four explanations

### Ten-second explanation

**Layman:** “I built a small version of a Kafka-like event store: applications send events, several machines keep ordered copies, and clients can read them even after certain servers fail.”

**Engineer:** “AstraLog is a Rust partitioned append-only event log using independent data-partition Raft groups, a distinct Raft-replicated metadata controller, local CRC-protected segmented storage, idempotent producer sequencing, cooperative groups, and bounded binary TCP request handling.”

### One-minute interview pitch

> “I wanted to learn what happens underneath a message broker, so I built AstraLog from scratch in Rust rather than wrapping Kafka. Each topic has ordered partitions. Each partition runs an independent Raft state machine; a separate controller Raft quorum decides cluster membership and assignments. The write path durably appends through broker-local storage and can acknowledge after majority commit. I implemented recovery, follower catch-up, producer epoch and sequence fencing, consumer-group offsets, batching, compression, and retention/compaction primitives. I tested restarts, leader transfers, failover and a genuine static RF3 broker-process kill. I also wrote a source-pinned release benchmark with exact Fetch payload checking and raw per-request latency samples. In one local RF3 quorum workload, buffering raised measured throughput from 10.71 to 33.31 records/second. I don't claim these short single-host numbers represent production capacity, and I can show you the safety boundaries and remaining gaps.”

### Three-minute pitch: architecture, hard bug, measurement

1. **Problem:** a single-server log fails when its disk/process disappears; two replicas alone cannot elect safely through arbitrary failure. Multi-partition systems also need a reliable source of metadata truth.
2. **Design:** topics are sharded into partitions; each partition has one Raft leader and voting followers. Majority agreement protects committed writes, while controller Raft protects cluster membership, broker incarnations, topics, assignments and feature activation.
3. **Hardest correctness lesson:** an obsolete rebootstrap snapshot may still be relevant as Raft metadata, but must not replace bytes already covered by a higher durable commit index. An intermittent P9-G run exposed a fatal stale-V1 file-package path. I added a narrow committed-prefix guard and an exact-byte regression test without weakening storage fences.
4. **Hardest API lesson:** if `JoinSubscribe` timed out, its mutation may already have committed. Retrying can create a second member. The client refuses blind replay; the P6 test now reconciles unique committed group state instead.
5. **Measurement:** release-mode, source-pinned, 1-KiB values, four producer tasks, RF1/RF3, leader/quorum acks, buffered/unbuffered configurations, two repeats. Compare like with like; preserve sample counts, p50/p95/p99 and source SHA. Don't claim tail-latency reliability from a tiny sample.
6. **Limit:** local loopback prototype. No TLS/authentication, P10 queue API is reserved but not active, Windows power-loss crash guarantees are not established, and P9-G sustained liveness is open.

### What NOT to say

- “It is a production-ready Kafka replacement.” **False.**
- “Raft gives exactly-once processing.” **False:** it orders committed entries; external side effects are separate.
- “Three brokers tolerate any two failures.” **False:** RF3 needs two voting replicas for progress.
- “An old leader can never have written an uncommitted record.” **False:** suffixes may be rolled back.
- “My p99 is statistically robust.” **False:** many repeats have fewer than 100 writes.
- “I have verified Linux power-loss and security.” **False.**
- “I completed P10–P12 because their APIs appear in an enum.” **False.**

# 2. Mental model: one real story

Imagine an online shop emits **OrderCreated(42)** and **PaymentApproved(42)**. AstraLog stores them so inventory, analytics and notifications can independently read the event history.

**Layman analogy:**
- A **topic** is a named collection of notebooks (“orders”).
- A **partition** is one numbered notebook; its pages have a fixed order.
- The **leader** is the writer allowed to approve new pages.
- **Followers** copy pages. A majority signing the page makes it committed.
- The **controller** is the directory showing who owns each notebook; it does **not** approve every page.
- A **producer** writes; a **consumer** reads; a **consumer group** assigns which readers process which notebooks.
- A **consumer offset** is a bookmark. A **producer sequence** is a duplicate-check ticket.
- A **snapshot** condenses Raft's agreed past; a **retained segment/manifest** controls which application bytes remain readable.
- A **tombstone** means a keyed value was deleted; removing the tombstone too soon risks resurrecting an older copy.

**Technical version:** A topic is keyed by a stable `TopicId` and partition count. A producer selects a `PartitionId`, resolves the current leader through metadata, and sends a framed request with a correlation ID and epoch context. The broker maps this to a partition actor. The leader proposes an encoded application command to `RaftCore`; persistence effects are executed through `PartitionPersistence` into `PartitionLog`. Replicated entries that pass quorum and durability conditions advance commit; Fetch is constrained to committed logical stream offsets. The controller maintains a separate linearizable order for desired membership and topic metadata.

### One diagram to memorize

```text
                  CONTROL PLANE (rare mutations; metadata)
   Controller C1 ----- C2 ----- C3
          \________ Raft quorum ________/
           broker identities, topic/partition placement,
           feature level, desired replica membership
                       |
                       v   committed metadata / deltas
               broker metadata caches
                       |
  producer/client --> bootstrap broker --(discover leader)-->
                       |
                       v
             DATA PLANE: topic "orders", partition 0
            Broker A (leader) -- AppendEntries --> Broker B
                  |             \---------------> Broker C
                  |                    (RF3 majority = 2)
                  v
          CRC-checked local storage
          segments + indexes + commit metadata
                  |
              Fetch committed
                  v
          consumer / consumer group
```

**Important:** Controllers can be co-located with brokers in local tests, but the two consensus state machines have different authorities. **Controller outage alone must not block already-routed, already-authorized data writes.** The group that owns *partition 0* is not the group that owns *partition 1*. There is no universal total order across partitions.

# 3. Vocabulary and the crucial guarantees

| Term | Plain English | Precise interview meaning |
|---|---|---|
| Event / record | One message | Key, value, headers, timestamp and partition position under a versioned disk/wire representation |
| Topic | Named stream | Metadata object pointing to N partition logs |
| Partition | Ordered lane | Independent ordered log / data Raft group; cross-partition order is not promised |
| RF1 / RF3 / RF5 | Copies of the log | One, three, five voting replicas. Majority sizes: **1, 2, 3**, respectively |
| Term | Election generation | Monotonic Raft authority epoch; stale leaders are fenced |
| Raft index | Replication position | Position in Raft log including metadata/control entries; **not equal** to stream offset |
| Stream offset | Consumer-visible position | Logical event position; compaction/retention preserve identities, even if holes remain |
| Commit index | Safety boundary | Highest Raft position authorized as committed; never overwrite this prefix |
| High watermark | Read visibility boundary | End of committed stream records, not the local uncommitted tail |
| `acks=0` | Fire-and-forget | No response or durability guarantee |
| `acks=leader` | Leader accepted durably | May still be lost if uncommitted when leader fails |
| `acks=quorum` | Majority committed | Intended preservation after any minority of voting replicas fails, subject to honest storage assumptions |
| WAL / segment | Disk notebook | Ordered CRC-protected frames with indexes and crash recovery |
| Fsync / `sync_all` | Ask OS to persist | Durability operation; device/OS honesty and directory rename guarantees remain platform-dependent |
| CRC32C | Damage detector | Detects accidental corruption; **not** cryptographic authentication |
| SHA-256 | File integrity digest | Used for retained V2 package content validation; not a remote identity/authentication protocol |
| Idempotent producer | Duplicate-safe resend | Same producer ID+epoch+sequence under supported window yields one logical committed append |
| Consumer group | Work split among readers | One authoritative coordinator, members and epochs; cooperative revocation fences old owners |
| Rebalance | Reassign work | Change partition owners while preventing two active owners |
| Controller incarnation | Broker process identity version | Higher committed incarnation fences an older broker process |
| Learner | Replica in training | Catches up without voting; promoted only after safe membership transition |
| Joint consensus | Change voter set safely | Old and new configurations must each meet their quorum rule during transition |
| Snapshot | Agreed checkpoint | Raft log-prefix compacting boundary and state information; distinct from application record retention |
| Retention | Remove old readable history | Advances logical log start and eventually deletes eligible physical generations |
| Keyed compaction | Keep latest key state | Rewrites eligible retained segments with stable logical offsets and conservative tombstone rules |
| Ambiguous mutation | Outcome unknown | A timeout does not prove the operation failed; never replay non-idempotent mutations without a safe identity/reconciliation protocol |
| Liveness vs safety | Makes progress vs never corrupts | A timeout can mean liveness failed while safety held; a green test cannot prove all schedules |

### Two equations worth understanding, not just reciting

For ordinary stable Raft with `N` voting replicas, the majority size is `floor(N/2) + 1`. If RF=3 and one voter is down, two votes can still form a quorum. If two are down, safety must win over write availability. *Availability under a particular failure* depends also on the actual voter set, term, disk and network; "RF3 survives one fault" is not an unconditional power-loss SLA.

Throughput in records/second is `acknowledged_records / elapsed_production_seconds`. Logical MiB/s is `acknowledged_records × application_payload_bytes / (1024² × seconds)`. It is **not** wire throughput because networking, replication and framing add bytes. A measured p99 must define both its sample population and percentile convention.

### CAP, briefly but correctly

During a network partition, the quorum side can continue accepting majority-committed data; a minority side **must not** commit a conflicting history. This prioritizes partition-scoped consistency over write availability on isolated minorities. This does **not** mean the service magically handles arbitrary network failures, provides cross-partition transactions, or has measured five-nines availability.

# 4. Technology stack: why each choice and what you give up

| Technology / choice | Why used here | Alternative and reason not chosen | Honest limitation |
|---|---|---|---|
| **Rust 2024 edition, toolchain 1.98.1** | Ownership/type checks, deterministic resource handling, binary formats, memory safety without GC, systems learning | Go is quicker but has GC; C++ allows lower-level control with larger memory-safety burden; Java has mature brokers but less direct disk ownership | Rust learning curve, compile times and async complexity; no speed superiority proven against alternatives |
| **Tokio async TCP** | Many connections/partition events on bounded executors, timers, channels, orchestration | Blocking thread per replica wastes resources; gRPC hides some low-level framing | Async fairness, blocking disk work and backpressure still require explicit design |
| **Custom length-prefixed versioned binary TCP** | Demonstrate wire framing, correlation IDs, bounds, CRC, feature versions | HTTP/gRPC preferable for many production teams, but abstracts away protocol study | No TLS/auth; loopback only; not evidence that custom binary outperforms gRPC |
| **Raft implemented internally** | Make election, replication, durable effects, quorum and reconfiguration auditable | etcd/raft or OpenRaft would reduce production risk but conceal a primary project objective | More correctness risk and review burden than battle-tested library |
| **One Raft group per partition (Multi-Raft)** | Independent order, failure isolation and throughput scaling potential | One global Raft log serializes unrelated topics; Kafka-style ISR is viable but needs independently proven elections/fencing | Scheduling and replica-count overhead; scaling to thousands not qualified |
| **Separate Raft metadata controller** | Strong committed owner of topic/assignment/incarnation authority, off data hot path | External etcd/ZooKeeper simpler operationally but another dependency and less learning | Dedicated controller quorum creates deployment/ops complexity |
| **Broker-local segmented storage** | Hands-on disk layout, append/roll, CRC, indexes, COW, restart | BookKeeper-style separated storage adds another distributed tier | Durable storage depends on host OS/FS; no tiered storage |
| **CRC32C** | Fast bounded accidental corruption detection for frames/manifests | SHA-256 is stronger but more expensive; neither substitutes for TLS identity | CRC cannot stop an adversary forging content |
| **SHA-256 for V2 rebootstrap files** | Detect exact file-byte substitution in a package; CRC complements framing | Only CRC is weaker against accidental multiple changes; content hashes are useful for package consistency | No public-key trust or transport authentication follows from a hash |
| **LZ4 / Zstandard / none** | Explicit space/CPU/latency trade-off on record batches | Universal compression can waste CPU or expand small payloads | Actual per-codec production benchmark comparison not published |
| **Murmur3 partition hash** | Deterministic key-to-partition mapping and test vectors | `std::hash` is not a frozen cross-process compatibility protocol | Partition expansion can change mapping; versioned partition epochs matter |
| **Cooperative consumer groups** | Incremental movement instead of full stop-the-world reassignment | Eager rebalance simpler initially but causes more pauses | Requires epochs and durable revoke/activation fences |
| **PowerShell benchmark launcher + CSV** | Reproducible local Windows workflow, raw audit trail and explicit exit status | A full telemetry stack would be disproportionate for the demo | CI/Linux and sustained workload metrics still missing |
| **Standard library + limited dependencies** | Own core protocol/consensus/state-machine code and easier architectural traceability | Pulling in a framework is faster but masks learning outcomes | More maintained code; bugs possible despite small dependency list |
| **Apache-2.0 license** | Permissive explicit licensing aligned with Cargo manifest | MIT also viable | License says nothing about security or correctness |

**Dependencies actually seen in workspace:** Tokio, `crc32c`, `sha2`, `getrandom`, `lz4_flex`, `zstd`, and the ten internal crates. Do not claim production gRPC, Kubernetes, Docker, Prometheus, OpenTelemetry, rustls or a database as deployed dependencies merely because a design doc mentions them.

### Why not just use Kafka?

For a business: use a well-supported production broker unless requirements justify otherwise. For an engineering portfolio: reimplementing the key mechanisms forces direct reasoning about persistent state, network ambiguity and consensus. **The project is credible when you can show a broken counterexample, minimal fix, and exact test—not when you claim to outperform Kafka.**

### Why not Go?

Go would be reasonable. Rust's type/ownership system made low-level byte buffers and resource limits explicit, which was useful for this learning objective. The counterargument is strong: a smaller Go implementation might deliver operational value sooner. Avoid claiming Rust alone makes distributed protocols safe: memory safety does not prove consensus safety.

# 5. Ten crates — the map of the system

| Crate | Plain-English job | Technical responsibility | Start reading |
|---|---|---|---|
| [`astralog-common`](../crates/astralog-common/src/lib.rs) | Agreed labels and number types | Newtyped TopicId, PartitionId, StreamOffset, RaftIndex, ProducerId/Epoch/Sequence | `src/lib.rs` |
| [`astralog-protocol`](../crates/astralog-protocol/src/lib.rs) | Postal envelope + address format | Network handshake/frame codec, bounded parsing, API schemas, version registry, key partition hash | `registry.rs`, `frame.rs`, `api/` |
| [`astralog-storage`](../crates/astralog-storage/src/lib.rs) | Durable notebook | Segments, CRC, index rebuild, manifest generations, retention and compaction | `partition.rs`, `format.rs`, `manifest.rs` |
| [`astralog-raft`](../crates/astralog-raft/src/lib.rs) | Voting and agreement rules | Deterministic input/effect consensus core, terms, votes, log matching, membership, snapshots | `engine.rs`, `state.rs` |
| [`astralog-replication`](../crates/astralog-replication/src/lib.rs) | Translate votes into durable replicas | Raft core + disk adapter + peer transport + producer state + rebootstrap | `runtime.rs`, `storage_adapter.rs` |
| [`astralog-controller`](../crates/astralog-controller/src/lib.rs) | Cluster directory and assignment authority | Controller Raft state machine, bootstrap, broker fencing, placement, metadata snapshots | `runtime.rs`, `state.rs` |
| [`astralog-broker`](../crates/astralog-broker/src/lib.rs) | Network-facing working server | Listeners, request dispatch, partition actors, controller bridge, group/offset coordinators | `server.rs`, `state.rs`, `replication.rs` |
| [`astralog-client`](../crates/astralog-client/src/lib.rs) | Application-side driver | Metadata cache, routing, retry classification, producer sessions, batching, group requests | `lib.rs`, `batching.rs` |
| [`astralog-groups`](../crates/astralog-groups/src/lib.rs) | Bookmarks and reader ownership | Deterministic group/offset records, sticky assignment, epoch-fenced cooperative transitions | `lib.rs`, `cooperative.rs` |
| [`astralog-benchmark`](../crates/astralog-benchmark/src/lib.rs) | Controlled stopwatch | Workload profiles, runner, manifests, measurement/report schema and CLI entry point | `runner.rs`, `report.rs` |

**Dependency direction to remember:** common primitives at the bottom; protocol/storage/Raft are deliberately separately testable; replication composes Raft + storage; controller also uses Raft but applies *metadata commands*; broker composes both planes; client talks over the public protocol; benchmark drives client. Most crate `lib.rs` files expose APIs and register child modules, not the implementation of the whole subsystem.

# 6. End-to-end flow I: startup, topic creation, Produce and Fetch

## 6.1 Cold start / restart

**Layman:** find the right disk notebook, verify it isn't torn or corrupt, remember which pages were accepted, and only then allow the server to answer.

**Technical trace:** `BrokerConfig::validate` → `astralog_broker::start` in [server.rs](../crates/astralog-broker/src/server.rs) → [BrokerState::open](../crates/astralog-broker/src/state.rs) / dynamic controller setup → actor creation via [broker replication.rs](../crates/astralog-broker/src/replication.rs) → `ReplicatedPartition::open` → `PartitionPersistence::open` / `RecoveredState` → `RaftCore::from_recovered`. In static RF1 the broker can directly operate a `PartitionLog` without a network Raft group; dynamic controller mode creates actors from committed assignments.

**Never confuse:** local filesystem recovery with controller discovery. Recovering bytes does not itself grant authority to lead; a recovered Raft term, membership and current leader govern serving.

## 6.2 Creating a topic

1. Client issues a versioned `CreateTopic` request to a broker front door.
2. Broker validates name/partition/RF, feature and API constraints; in dynamic mode it forwards to controller authority.
3. Controller leader validates, creates a stable TopicId, chooses distinct replicas subject to broker state and rack/zone policies, and proposes a metadata command.
4. **Only controller Raft commit+apply** makes the assignment authoritative. Broker caches receive contiguous metadata deltas or snapshot fallback.
5. Local partition actors appear based on committed assignment, not because a random broker wanted to host a replica.

**Relevant source:** [broker server.rs](../crates/astralog-broker/src/server.rs), [broker state.rs](../crates/astralog-broker/src/state.rs), [controller runtime.rs](../crates/astralog-controller/src/runtime.rs), [controller placement.rs](../crates/astralog-controller/src/placement.rs), [broker metadata_cache.rs](../crates/astralog-broker/src/metadata_cache.rs).

**Interview trap:** “If a controller thinks A should lead, can it decree A as partition leader?” **No.** Controller commits desired placement; partition Raft elects the live leader. The leader/term in client metadata must reflect actual partition Raft observation.

## 6.3 Writing one record, end to end

```text
 client.produce(...)
     -> choose topic partition (key hash / sticky no-key)
     -> resolve/cache leader + current routing epoch
     -> encode WireFrame + ProduceRequest
     -> TCP handshake, bounded frame parse
     -> BrokerState::produce -> produce_partition
     -> replication actor (or RF1 storage path)
     -> ReplicatedPartition::propose_records
     -> RaftCore::handle_input(Input::Propose)
     -> Effect::Persist -> PartitionPersistence -> PartitionLog
     -> AppendEntries to peers; followers persist before ack
     -> quorum advances commit; commit/apply boundary
     -> matching pending proposal outcome -> broker response
     -> client receives offsets / ack or explicit uncertainty
```

**Layman:** the leader writes an event, followers copy it, and the required number of copies agree before the client is told it was committed.

**Critical source anchors:** [client `produce_partition`](../crates/astralog-client/src/lib.rs), [`BrokerState::produce_partition`](../crates/astralog-broker/src/state.rs), [`ReplicatedPartition::propose_records`](../crates/astralog-replication/src/runtime.rs), [`RaftCore` inputs/effects](../crates/astralog-raft/src/engine.rs), [`PartitionPersistence`](../crates/astralog-replication/src/storage_adapter.rs), [`PartitionLog::append`](../crates/astralog-storage/src/partition.rs).

**Why a persistence effect?** Raft core emits an instruction to persist instead of assuming a file write succeeded. The caller executes it and sends back `PersistenceCompleted(token)`. This prevents consensus logic from sending certain acknowledgements or elections before the corresponding required durable action. A persistence failure is a terminal fail-closed state, not a fabricated success.

**Key distinction:** “leader accepted” ≠ “quorum committed” ≠ “consumer saw the exact bytes.” Those are separate proof obligations. A response lost after durable commit can be ambiguous to the client.

## 6.4 Reading a record and long polling

**Layman:** ask the current leader for the notebook from bookmark X; wait briefly for more pages if asked; never show an unfinished page.

**Technical:** `AstraClient::fetch` frames a partition-specific offset/bound; `BrokerState::fetch_long_poll` calls `fetch_once` and uses a notification epoch plus a bounded timeout to avoid missed wake-ups. In replicated mode the actor returns committed-visible data only; storage uses bounded segment scans and indexes. `ListOffsets` provides committed offset boundary lookup. Relevant: [broker state.rs](../crates/astralog-broker/src/state.rs), [storage partition.rs](../crates/astralog-storage/src/partition.rs), [protocol fetch.rs](../crates/astralog-protocol/src/api/fetch.rs).

**Interview trap:** Can an RF3 follower that has physically appended an uncommitted record serve it through normal Fetch? **No**—that could leak a suffix later truncated during leadership change.

## 6.5 Partition key and ordering

With a key, the P2 hash maps it deterministically to a partition (`partition_for_key` and frozen vectors). With no key, a sticky partitioner can cluster records for batching. **Ordering is per partition and per accepted producer sequence—not a global timestamp order across the cluster.** Increasing a topic's partition count may change key placement, so it is a versioned metadata event, not a transparent promise that all historical key mapping remains unchanged.

# 7. End-to-end flow II: failures and consistency

## 7.1 Leader dies after quorum commit

**Scenario:** A, B, C form RF3. Leader A commits record X with B; A crashes. B/C can elect a successor if they form a legal majority. A correctly implemented new leader retains X. A new quorum append Y can then succeed.

**Actual test boundary:** [`rf3_separate_process_leader_kill_preserves_committed_quorum`](../crates/astralog-broker/tests/p4_replication.rs) starts **three broker OS child processes**, uses `Child::kill()` on the elected leader, confirms exact pre-kill payload on the successor, and commits another quorum write. **Static RF3** only; not controller-managed P9-G WAN chaos or physical power loss.

## 7.2 Minority network partition

An isolated one-of-three voter must not manufacture two votes. It may have local data, but cannot safely declare new quorum commits. When connectivity recovers, stale terms and conflicting uncommitted suffixes must reconcile. Deterministic Raft model/simulation tests exercise pieces of this; **sustained real multi-process network partition testing is still missing**.

## 7.3 Crash while writing vs power cut

Active segment may end mid-frame. Recovery checks length/CRC and may truncate an *uncommitted* incomplete suffix, but cannot silently discard corrupt committed history. `fsync` and directory rename ordering matter. [storage io_util.rs](../crates/astralog-storage/src/io_util.rs) calls `File::sync_all` and Unix directory sync; Windows directory sync is deliberately a no-op, so a successful Windows crash-restart test **does not** prove sudden power-loss durability.

## 7.4 Snapshot comes from a stale peer

This was a reproduced engineering bug. A follower already durably committed through Raft index N; an old V1 rebootstrap package at or below N arrived. Replacing files would violate committed-prefix preservation. The storage adapter correctly rejected replacement but the old V1 runtime propagated the rejection as a fatal replica. The bounded fix in [replication runtime.rs](../crates/astralog-replication/src/runtime.rs) **omits the stale file replacement but still passes the InstallSnapshot Raft message to consensus logic**. The regression `v1_snapshot_of_already_committed_prefix_preserves_local_bytes` asserts the original committed value remains exactly the same.

**Why not delete the storage guard?** Because the guard was protecting committed data. A liveness bug must not be “fixed” by allowing committed byte overwrite. **Unqualified remainder:** an earlier distinct missing V2 manifest symptom has not been conclusively diagnosed; sustained P9-G V2 catch-up remains open.

## 7.5 Client sends mutation, response disappears

A `JoinSubscribe` or non-idempotent Produce can be committed just before the reply disappears. TCP timeout ≠ transaction abort. A retry without an idempotency key could double-apply. The [client library](../crates/astralog-client/src/lib.rs) reports `AmbiguousMutation` for the group mutation rather than replaying. The [P6 integration harness](../crates/astralog-broker/tests/p6_routing.rs) checks a uniquely matching committed Stable group/member record and proceeds only if authoritative evidence proves success. Root cause of the one 20-second timeout remains unproved.

**General interview answer:** use unique request identity and dedup state when the API supports it; otherwise reconcile authoritative state and fail closed. Random “retry all 5xx” logic is dangerous for non-idempotent state changes.

## 7.6 Controller leader fails

A controller leader dies; another controller must win through controller Raft. Committed topic/broker/feature assignments survive. The data plane must keep serving already-authorized requests without consulting controller on every Produce/Fetch. **Do not confuse** controller heartbeat suspicion with election authority: a missed heartbeat is a liveness observation, not a committed “remove this broker” decision.

## 7.7 Broker rejoins with old identity

Each broker has durable broker identity and a monotonic committed incarnation. A stale process cannot silently reclaim authority after a newer incarnation has been committed. Moving replicas must first add a **learner**, catch it up, then use safe P3 joint consensus to change the voter set. Removing old replica obligations requires committed exclusion; a desired controller placement change alone is not enough.

## 7.8 Overload / bounded resource exhaustion

Huge network frames, too many simultaneous requests, excessive outstanding replication bytes or too many pending producer records can exhaust memory even if Raft is correct. [broker server.rs](../crates/astralog-broker/src/server.rs) validates connection/global inflight/response caps and admission quotas; [broker load_shaping.rs](../crates/astralog-broker/src/load_shaping.rs) handles pressure. [replication runtime.rs](../crates/astralog-replication/src/runtime.rs) bounds outbound messages and follower catch-up windows, and reserves control capacity. The desired behavior is **reject or throttle while preserving safety**, not accept unbounded buffering or starve elections. Broad hostile-traffic security and long-duration production load qualification are future work.

---

# 8. Deep dives: the core engines

## 8.1 Storage — from record to durable bytes

**Layman:** Segments are the volumes of a notebook, indexes are the table of contents, and a manifest is the authoritative inventory of which volumes belong to the notebook.

**Technical:** `astralog-storage` stores CRC-validated V1/V2 frames in append-only segmented logs. `format.rs` and `format_v2.rs` encode on-disk representations; `segment.rs` handles segment scanning and writes; `partition.rs` owns append, flush, roll, commit, bounded reads, recovery, deletion retention, and suffix repair. `index.rs` rebuilds optional derived indexes; `manifest.rs` encodes a CRC-validated generation and the exact immutable/mutable file inventory. `partition/compaction.rs` performs copy-on-write keyed compaction. `clean_progress.rs` protects P9-G local proof, `maintenance.rs` encodes durable control records, `compression.rs` handles none/LZ4/Zstd, `limits.rs` bounds resources, and `io_util.rs` owns fsync, atomic staging and rename.

**Why copy-on-write?** If a crash occurs midway through an in-place rewrite, neither old nor new data may survive intact. By building complete replacement files and publishing a new manifest, restart can recover a coherent old or new generation. Pinned read generations allow existing readers to finish with the files they opened. The price is temporary disk amplification and more lifecycle management.

**Three different indexes:** A **Raft index** counts consensus log entries, including control entries. A **stream offset** counts logical user records and is stable through retention/compaction. A **commit index** marks the greatest committed Raft prefix. They are not interchangeable. An uncommitted durable suffix may disappear after leadership change; a committed prefix must not be replaced.

**Disk-durability limit:** `io_util::sync_directory` syncs Unix directory metadata, but is a deliberate no-op on Windows; Windows process-restart tests do not prove sudden power-loss durability. Corrupt committed bytes must not be silently truncated. See [storage source](../crates/astralog-storage/src/lib.rs), [durability caveat](../SECURITY.md).

## 8.2 Raft — a committee rulebook without I/O

**Layman:** Raft is the committee that selects one leader and decides which pages count as approved. It does not itself write the notebook or open sockets.

**Technical:** [`RaftCore::handle_input`](../crates/astralog-raft/src/engine.rs) consumes `Input::Tick`, `Step(message)`, `Propose`, `ProposeConfiguration`, durable completion/failure notifications and snapshot events; it produces `Effect::Persist`, `Send`, `Apply`, `RoleChanged`, etc. `RaftCore` is a deterministic state machine, separated from disk/network side effects.

The source modules are `configuration.rs` (stable/joint voter sets), `engine.rs` (Raft algorithm), `entry.rs` (indexed entries), `error.rs` (failure types), `message.rs` (PreVote, RequestVote, AppendEntries, InstallSnapshot, TimeoutNow), `persistence.rs` (typed durable operations), `state.rs` (hard/volatile state, peer progress and limits), and `types.rs` (checked terms/IDs). `simulation.rs` and `formal_model.rs` are **test-only**, not general production proof.

**Election trace:** timeout → PreVote → RequestVote → persist term and vote → collect legal majority → leader-term initialization → replicate AppendEntries → commit under Raft rules → apply committed entries. Old terms and stale leaders are fenced. Persistence tokens explicitly block effects that require completed fsync before acknowledgement. On persistence failure, the engine fails closed, not with fake success.

**Why joint consensus?** Switching from {A,B,C} to {B,C,D} cannot be a sudden replacement of voters. During the joint configuration, required majorities of **both old and new configurations** must agree. D should catch up first as a learner. Only after committed stable exclusion may A cease to be an authoritative voter.

## 8.3 Replication — connecting consensus, disk and transport

**Layman:** consensus produces rules; replication makes sure the matching bytes are saved and delivered across brokers.

**Technical:** [`ReplicatedPartition`](../crates/astralog-replication/src/runtime.rs) owns one partition's RaftCore, proposal receipts, leader/term guards, bounded outbound and in-flight follower catch-up, applied producer state and acknowledgement outcomes. [`PartitionPersistence`](../crates/astralog-replication/src/storage_adapter.rs) executes persistence operations using `PartitionLog`. `peer_transport.rs`, `transport.rs` and `codec.rs` move bounded CRC-protected Raft frames; the peer identity fields and CRC do **not** cryptographically authenticate senders. `command.rs` carries application and maintenance commands; `producer_state.rs` / `producer_snapshot.rs` / `producer_digest.rs` maintain committed idempotency authority; `sidecar.rs` stores hard/snapshot state; `rebootstrap.rs` and `rebootstrap_v2.rs` implement versioned package recovery; `compaction_progress.rs` tracks P9-G all-voter proof.

**The subtle acknowledgement point:** `LocalDurable` is not `QuorumCommitted`. For quorum acknowledgement, only the actual committed index and durable effect ordering justify success. Followers persist before their acknowledgements can contribute to quorum. Replication queues reserve capacity for control traffic and cap individual/in-flight catch-up bytes.

**Snapshot nuance:** V2 retained-history rebootstrap validates complete manifest inventory, snapshot authority, committed data boundaries and optional producer checkpoint using CRC/SHA and bounded size checks, then stages and publishes files. Packages are monolithic/bounded rather than a mature streaming WAN protocol. P9-G opt-ins default off; continuous-write V2 transfer liveness remains unqualified.


# 9. Deep dives: controller, broker, groups and producer identity

## 9.1 Controller: metadata consensus, not data consensus

**Layman:** The controller is the official directory: which brokers exist, who should hold each notebook, and which features are currently permitted.

**Technical:** `astralog-controller/src/runtime.rs` composes the same `RaftCore` with a *different replicated state machine* in `state.rs`, applying encoded metadata commands from `command.rs` and persisting its hard state/log/committed snapshots through `persistence.rs` and `snapshot*.rs`. `bootstrap.rs` seals the controller cluster identity and static voter set. `membership.rs` validates broker UUID/incarnation/lifecycle; `placement.rs` selects distinct brokers and obeys rack/zone policies; `reassignment.rs`/`movement.rs` store resumable replica migrations. `metadata.rs` exposes committed deltas/snapshot fallback; `feature.rs` fences irreversible feature activation; `performance.rs` and `retention.rs` are topic policies; sibling `*_codec.rs` encode versioned commands; `peer_transport.rs` and `transport.rs` handle controller-to-controller communication.

**Why separate controller Raft?** Topic creation, broker membership and feature upgrades need one ordered source of truth, but every Produce/Fetch must not wait on the controller. The controller's committed desired placement is NOT proof of which broker is the *currently elected* data partition leader. Broker `metadata_cache.rs` consumes committed deltas atomically with monotonically increasing metadata epoch, rejecting gaps/foreign clusters and using snapshot fallback when needed.

**Heartbeat vs authority:** `heartbeat.rs` produces volatile Healthy/Suspect/Unavailable observations with controlled logical time; a suspected broker is not automatically removed from committed membership. On controller failover, successor starts a fresh liveness observation. Losing all controllers may block new topic changes or unseen producer authorization, but should not automatically invalidate already-routed data-plane operations.

**Replica migration in plain words:** “add a trainee, let them copy the book, vote them into the committee, and only then dismiss the old voter.” Technically: committed movement intent → destination learner → real Raft catch-up → incarnation/generation-fenced observation → P3 joint consensus → stable voter set → optional leadership transfer → committed old-source exclusion → controller marks movement complete. Recovery resumes from committed steps; no RPC success alone proves a step happened.

## 9.2 Broker: dispatch, admission and actor orchestration

**Layman:** The broker is both the front desk and the owner of some log copies. It checks requests, locates the right partition actor, handles overload and returns the correct answer.

**Technical:** `server.rs` validates listener/connection/frame/buffer caps, performs TCP handshake, accepts versioned WireFrames and dispatches APIs. `state.rs` chooses static RF1 storage versus replicated partition routes, implements public Produce/Fetch/metadata/topic operations and long-poll notifications. `replication.rs` hosts partition actors, broker-peer transport, static or dynamic routing, retries/failures and P9-G progress. `controller.rs` talks to the controller for metadata, leader observations and producer authority. `load_shaping.rs` implements admission quotas; `catalog.rs` handles local topic catalog. `group_coordinator.rs`, `offset_coordinator.rs` and `p6_service.rs` manage P6 logs and cross-log sequencing.

**One process nuance:** the regular RF3 benchmark uses three broker runtime instances in **one test process** with real loopback TCP and separate data directories; the stronger P4 RF3 `Child::kill()` test uses **three independent OS processes**. Neither is a multi-machine WAN benchmark.

## 9.3 P6: cooperative groups and two internal logs

**Layman:** A group is a team reading partitions. The coordinator keeps a roster of workers and decides who may handle each partition; a separate notebook stores their bookmarks.

**Technical:** `astralog-groups/src/lib.rs` defines the deterministic durable `GroupStateMachine` and `GroupRecordV1` with exact state-version replay. `assignor.rs` uses uniform-sticky assignment: minimize maximum load, then movement from still-eligible owners, then canonical tie-breakers. `cooperative.rs` drives prepared epoch → revocations → owner fences → activation → Stable. `offset_state.rs` rejects commits from stale group/member/owner epochs. `session.rs` keeps volatile heartbeat deadlines, with **fresh grace on coordinator leader failover**, avoiding bogus expiry from old volatile timers. `qualification.rs` contains seeded pure-state tests.

The broker protects internal topics `__astralog_groups` and `__astralog_offsets` (64 partitions, RF3); GroupId is deterministically hashed to its group shard, and the shard's **actual Raft leader** is coordinator. Public keys 30–35 are JoinSubscribe, Heartbeat, RevokeAck, Leave, CommitOffsets, FetchOffsets. The public client tracks a group route and member epochs.

**Why two logs?** Membership/assignment and offset commits can be independently sharded and fail over, but this creates a critical **cross-log fence** obligation: before a new group epoch is active, commit an `OffsetGroupFence`; after the old owner has durably released a partition, commit an `OffsetOwnerFence` before activating the new owner. Otherwise a stale member could commit a bookmark or two members could both be active for one partition.

**Adversarial timeline:**

```text
A owns partition 0
  C joins -> coordinator commits PREPARING with a target assignment
  offsets shard commits new group-epoch fence
  coordinator activates epoch / records pending revocation
  A sends RevokeAck -> committed old-owner release
  offsets shard commits owner fence
  C becomes the sole active owner -> group STABLE
  stale A offset commit -> rejected
```

If coordinator dies after PREPARING but before the offset fence, a successor replays PREPARING and resumes rather than guessing success. If the offsets leader dies before an owner fence, the new owner cannot be activated from an ambiguous fence. Heartbeats are volatile to avoid replicating every ping; group membership is durable. This gives **at-least-once application processing**, not atomic commit of an external database transaction.

**Ambiguous JoinSubscribe:** network timeout after a mutation is not proof of noncommit. The client returns `AmbiguousMutation` rather than blindly repeating an anonymous join. The P6 harness reconciles a unique committed Stable epoch-1 member with exact contract. A missing proof remains a test failure. Root cause of the observed 20-second timeout under full-suite load was not definitively traced.

## 9.4 P7: idempotent producer sessions in two layers

**Layman:** Give each writer a permanent number, a generation number, and a numbered ticket for each batch. If the receipt is lost, the writer retries the SAME ticket and content; the server returns the old receipt instead of writing a duplicate.

**Technical:**
1. **Controller** durably allocates ProducerId ranges and monotonically authorizes ProducerEpoch. Restart is a compare-and-set-style expected-epoch advance. A lost allocation reply may waste an ID, but must never cause reuse.
2. **Client** (`astralog-client/src/lib.rs`) reserves exact producer partition sequence range and encoded V1 request body. `drive_pending_produce` retries the same unresolved logical request; it may retarget a leader epoch but not silently change producer ID, sequence or body. Next sequence advances only on authoritative success.
3. **Broker** (`broker/controller.rs`) asks the partition runtime first, using controller proof only for unseen/higher epoch authority. An already-accepted epoch may keep producing through controller outage; an unknown/higher epoch fails closed without controller proof.
4. **Partition** (`replication/producer_state.rs`) persists accepted epoch, next sequence, and **five recent committed batch identities** (range, digest, original offsets and Raft index). A lower epoch is fenced; future sequence gap is rejected; exact recent replay returns original offsets; different-content replay is rejected. An older unprovable retry fails, instead of being guessed duplicate.
5. **Quorum**: P7 Produce V1 requires quorum; local persistence or an uncommitted higher epoch is not globally authoritative. After leader failover, successor reconstructs only committed producer state and can deduplicate a lost-reply retry.

**Why finite five-batch history?** Bounded state avoids endless replay/cache growth. The cost is that a very old exact retry may not be recognized; fail safely rather than weaken deduplication.

**What it does NOT do:** cross-partition transactions, unlimited dedup window, external consumer exactly-once processing, or exactly-once payment/email side effects.

## 9.5 P8: batching, compression and flow control

**Layman:** Put several letters in one envelope rather than paying the fixed round-trip cost for every letter. The downside is waiting to fill the envelope and holding more data in memory.

**Technical:** `astralog-client/src/batching.rs` has per-partition, size/record/linger-bounded accumulators and explicit close/in-flight completion. Topic feature levels gate buffered APIs. Compression policies choose none/LZ4/Zstd with decompression/size checks in storage codecs. Broker quotas, request byte budgets, in-flight connection bounds and replication follower windows protect against OOM and control starvation.

**Why buffering helped in the benchmark:** networking, application scheduling, replicated append and fsync have fixed costs that can be amortized across records. **Why this is not universal:** batch linger can worsen per-record latency at low traffic, compression costs CPU, and memory/replication fairness must be bounded.

# 10. P9 retained-history compaction: the hardest storage safety story

**Delete retention** advances the agreed logical start and deletes generations only when permitted. **Keyed compaction** keeps the latest retained value per key while preserving original stream offset identities. **Tombstone removal** is more dangerous than either: removing “key deleted” too early can expose an older value on an offline replica.

**Counterexample:** A and C compact key K and remove its tombstone; B has been offline since it stored `K=old`. B later becomes leader or is accidentally reintroduced with unverified retained history. K “comes back from the dead.” That violates no-resurrection invariant I-31.

**P9-G defense chain:**
1. Storage writes a small CRC-protected `clean.progress.v1` proof tied to topic, partition and **exact manifest generation**. Stale or corrupt evidence becomes zero.
2. Leader observes clean bounds from **all effective Raft voters**, including union of old/new voters during joint configuration. Reports must match current term, assignment, incarnation, voter role and committed high watermark; a missing/expired voter blocks progress.
3. Only a verified minimum floor can become an `AdvanceCompactionSafeOffset` **quorum-committed maintenance command**.
4. Tombstone omission is gated by local verified prefix, committed safe floor, grace and persisted Raft snapshot floor. An under-cleaned replica cannot campaign or serve as a valid leader.
5. If it cannot catch up ordinarily, a P3-authorized V2 retained-history package transfers bounded exact manifest/files/checkpoint with CRC32C/SHA-256 and staged crash-safe publication.

**Critical reproduced bug:** an obsolete **V1** file-rebootstrap package with snapshot index ≤ locally durable commit index reached a follower. Storage rightly refused overwriting committed files; replication runtime incorrectly made the follower fatal. The fix skips only obsolete V1 *file replacement* but still passes the Raft InstallSnapshot metadata through term/sender/leader checks. Exact committed bytes remain unchanged. Test `v1_snapshot_of_already_committed_prefix_preserves_local_bytes` validates this.

**Honesty boundary:** P9-G local safety tests exist and P5/P9 integration target passed 12/12 in the final earlier serialized run; however an older distinct V2 manifest-convergence episode remains unexplained, default V2/progress opt-ins are false, and **sustained-write liveness and full P9-G production gate are not qualified**. Refer to [P9-G historical ledger](P9_G_WORK_IN_PROGRESS.md) and [audit matrix](AUDIT_CLOSURE_MATRIX.md), not just a green snapshot.

# 11. How to create rigorous failure tests

1. **State the invariant:** “after quorum ACK, bytes and offsets survive leader loss” is better than “node restarts.”
2. **Freeze the initial state:** known topic, voter set, terms, producer epochs, committed payloads and clean directory per process.
3. **Choose the exact fault boundary:** before persist; leader-only persist; after follower persist; after quorum commit before reply; after controller command before applying metadata; before/after COW publish; during joint membership.
4. **Inject the right failure:** deterministic dropped/reordered Raft messages; actor `shutdown` (graceful); `Child::kill()` (real process crash); storage failpoint; corrupt/torn frame; paused voter.
5. **Wait for authoritative evidence**, not sleep for an arbitrary number of milliseconds: committed Raft index, stable membership, manifest generation and exact records. Use bounded deadlines with useful diagnostics.
6. **Assert exact results:** compare full payload, key, offset and expected order; assert absence of duplicates, invalid election, stale offset commit and old-value resurrection.
7. **Preserve RED evidence:** test revision, command, environment, complete failure log. A later GREEN does not erase an intermittent RED or prove every schedule.
8. **Never “fix” a flaky test by weakening the invariant, blindly retrying mutations, or overriding strict no-overwrite checks.**

### Failure matrix

| Injected failure | What must hold | Best code/test evidence | Remaining caveat |
|---|---|---|---|
| RF1 restart | Committed record recovered exactly | `p2_network.rs` | Not electrical power cut |
| Real RF3 elected leader OS kill after quorum | Exact old value read and new quorum append | `p4_replication.rs::rf3_separate_process_leader_kill_preserves_committed_quorum` | Static RF3, not dynamic P9-G |
| Controller leader death | Committed metadata/broker identities survive | controller `failure_qualification.rs` and P5 integration | Wider OS-process fault matrix missing |
| Minority partition | Cannot commit conflicting history | Raft `simulation.rs` / `formal_model.rs` | Sustained multi-process network campaign missing |
| Divergent uncommitted suffix | Conflict repair, no committed replacement | Raft/replication tests | Not exhaustive |
| Lost P6 join response | No accidental second member | `p6_routing.rs` committed record reconciliation | Timeout cause unknown |
| Lost P7 committed Produce response | Same sequence/digest returns original offsets | `p7_failover.rs` | Five-batch dedup window |
| Higher P7 epoch only locally persisted | Cannot supersede committed authority after failover | `p7_failover.rs` | Correctly rejects out-of-order/gaps |
| Corrupt segment/index | CRC rejection; derived index rebuild | storage segment/index tests | Real power loss unqualified |
| Kill during V2 manifest staging | Recover authoritative old or new | replication/storage adapter fault tests | Windows directory fsync missing |
| P9 offline voter / joint membership | Missing voter proof blocks safe tombstone floor | P9-G RF3/RF4 tests | Sustained production liveness open |
| Malformed/oversize network frame | Bounded reject; no unchecked allocation | protocol decoder and broker negative tests | Independent fuzz/security campaign missing |
| Saturation / slow follower | Bounded queues, protect control traffic | broker load-shaping / replication tests | Endurance not qualified |

A “fault-tolerance proof” is not “the test returned green.” It is **fault boundary + authoritative observed state + exact data oracle + specified scope + repeatable source provenance**.


# 12. Benchmark science: numbers and limits

The [post-audit source-pinned optimized benchmark](benchmarks/remediation_20261103/SUMMARY.md) used commit `35190af265ad5ce9c4e23278785e0a3bd0a7ecdf`, **not** the later audit closure commit `d6236c2`. It retained 12 measurements, 1,232 acknowledged and exactly fetched messages, zero producer errors and 2,464 per-request Produce/Fetch latency samples. These were short Windows 11 single-laptop loopback TCP tests with 1,024-byte values, four producers, two repeats and no compression; Fetch was measured **after** production.

| Replication / ack / batching | Records/s | Produce p95 | Produce p99 |
|---|---:|---:|---:|
| RF1 / leader / unbuffered | 76.36 | 58.94 ms | 62.07 ms |
| RF1 / quorum / unbuffered | 68.03 | 65.78 ms | 70.55 ms |
| RF3 / leader / unbuffered | 10.12 | 599.68 ms | 624.59 ms |
| RF3 / leader / buffered | 39.60 | 174.98 ms | 178.71 ms |
| **RF3 / quorum / unbuffered** | **10.71** | **480.50 ms** | **494.24 ms** |
| **RF3 / quorum / buffered** | **33.31** | **138.83 ms** | **148.51 ms** |

The RF3 quorum throughput ratio was ~**3.11×** in this specific workload. Do not treat it as a general performance promise. Six of twelve repeats had fewer than 100 Produce observations; nearest-rank p99 therefore approximates the maximum observed request, not reliable production tail-latency evidence. Percentiles are medians of per-repeat percentiles, not pooled quantiles.

**Historical benchmark:** the older `d7e32d3` source reported 14.34 → 47.09 RF3 quorum records/s (~3.28×), before stronger Fetch payload identity checks. Preserve these two benchmarks as separate source-pinned experiments rather than cherry-picking the more attractive number.

**Why so slow?** This workload performs real storage/consensus/TCP on one machine with short windows. No published breakdown attributes delay to CPU, disk sync, scheduler, wire overhead or lock contention. A convincing next experiment instruments stage timings and fsync latency, tests independent hosts and varies message sizes, partition count, RF, concurrent clients and batching; only then tune the observed bottleneck.

**Benchmark equations:** successful records/s = acknowledged records / elapsed production seconds; logical MiB/s = successful records × payload bytes / seconds / 1,048,576. Those numbers exclude replication and protocol overhead. Always report errors, profile, SHA, dirty flag, host, full raw latency samples and Fetch exact-value oracle.


# 13. Adversarial audit: A01–A13 and lessons

The [audit closure matrix](AUDIT_CLOSURE_MATRIX.md) is the authoritative record of exactly which defect was fixed, what was contained, and what remains unqualified.

| ID | Interview attack | What changed | What remains |
|---|---|---|---|
| A01 | “Your V2 follower sometimes misses its manifest.” | Reproduced a distinct fatal obsolete-V1 package path; added committed-prefix guard, byte-identity regression, P9-G tracing and diagnostics | Original missing-V2 symptom and sustained V2 liveness not conclusively explained |
| A02 | “Your detector test uses fantasy time.” | Removed P7 +1000 tick per readiness poll; fixed logical time and added health diagnostics | Does not qualify every detector interleaving |
| A03 | “Counting fetched messages cannot prove correct data.” | Exact key, payload, sequence identity, offset continuity and uniqueness oracle | Not a formal consistency proof |
| A04 | “Your P7 tests only check counts.” | Changed remaining failover assertions to full payload/offset sequences | Endurance not established |
| A05 | “Where are p99 raw samples?” | Retained individual nanosecond samples and recomputed percentile | Small sample warning disclosed |
| A06 | “Public history leaks private file paths.” | Sanitized logs and created a history-free export tool | Never publish old private Git history |
| A07 | “Declared Apache license but no LICENSE.” | Added full Apache-2.0 license | Licensing is not security |
| A08 | “Windows dir fsync is empty; is power cut safe?” | Scope documented accurately; Unix directory sync exists | Windows sudden-power-loss proof absent |
| A09 | “Unauthenticated remote Raft peers.” | Enforced loopback-only listener binding | TLS/mTLS and local-client auth absent |
| A10 | “CI may pull moving dependencies.” | Pinned checkout SHA, Ubuntu 24.04, Rust 1.98.1 and Cargo `--locked` | No hosted Ubuntu run yet |
| A11 | “Benchmark might be dirty debug.” | Full SHA, profile, dirty flag and sample counts validated | Fresh public-commit benchmark pending |
| A12 | “Graceful shutdown is not a real crash.” | Three distinct OS child processes and `Child::kill()` leader-failover test | Static RF3, not entire dynamic P9-G matrix |
| A13 | “PowerShell transcript hid Cargo errors.” | Direct .NET process capture and explicit exit status | Launcher hygiene, not system qualification |

**Valuable lesson:** distinguish violated **safety** (wrong committed data, double ownership) from failed **liveness** (dead actor, no progress). Fix liveness without weakening storage or consensus protection.

# 14. Future work without feature inflation

| Work | Bounded next step | Qualifying evidence |
|---|---|---|
| P9-G | Trace real V1/V2/AppendEntries under sustained writes | Repeated V2 manifest/byte oracle after offline rejoin, joint membership, dynamic process kill |
| P9-H/I | Bounded cleaner and recovery/failure campaigns | Disk pressure, reader generations, stale manifest, reclaim and crash-point matrix |
| P10 | Deterministic queue state machine, then API keys 40–43 | Committed lease before receive, stale receipt fence, expiry redelivery, poison DLQ |
| P11 | TLS/mTLS, client auth/ACL, readiness, drain, rolling upgrades | Impersonation rejection, rotation, mixed-version traffic |
| P12 | Multi-process chaos and long-run benchmark release | Network partitions/corruption/power cut; throughput/tail/failover distributions |
| Publication | New fresh-history repo, Ubuntu CI and new public-commit benchmark | No private history; hosted run and raw reproducibility |

**P10 design, not implemented:** `QueueReceive` first quorum-commits a receipt lease (offset, epoch, deadline, attempt number) before returning work. `QueueAck` must match the lease; an expired receipt cannot complete the item. Expiry causes at-least-once redelivery. DLQ transfer must carry deterministic source identity; ambiguity must not silently mark the original complete. Reserved API keys do not imply an active queue.

**Avoid overengineering:** for each new feature, name one invariant, implement one narrow state machine, reproduce one counterexample, then test exact recovered bytes/authority. In a business, relying on a battle-tested broker and Raft implementation may be more responsible; this project implements them as a first-principles learning exercise.


# 15. Twelve code walks that teach the system to its core

1. **Type vocabulary:** `crates/astralog-common/src/lib.rs` newtypes and overflow checks. Ask why RaftIndex cannot accidentally become StreamOffset.
2. **Network contract:** `protocol/src/registry.rs` API keys/features; `protocol/src/frame.rs` correlation, timeout, length/CRC; `protocol/src/decoder.rs` bounded incremental parse.
3. **Single-node persistence:** `storage/src/format.rs` → `segment.rs` → `partition.rs` (`append`, `mark_committed`, `read_committed_records_from_bounded`). Locate checksums and committed tail.
4. **Pure Raft:** `raft/src/engine.rs` `Input`/`Effect`, `handle_input`, `complete_persistence`, elections, `maybe_advance_leader_commit`. Show why persistence tokens matter.
5. **Raft + disk:** `replication/src/runtime.rs` `propose_records` → `storage_adapter.rs` `PartitionPersistence::execute` → `raft::Input::PersistenceCompleted`.
6. **Network-facing Produce/Fetch:** `broker/src/server.rs` `dispatch_request` → `broker/src/state.rs` `produce_partition` / `fetch_long_poll` → replication actor.
7. **Metadata authority:** `controller/src/command.rs` → `runtime.rs` → `state.rs`; `broker/src/metadata_cache.rs` delta/snapshot handling.
8. **Replica movement:** `controller/src/placement.rs` → `movement.rs` and `reassignment.rs` → `broker/src/replication.rs` learner→voter→old removal.
9. **P6 groups:** `groups/src/lib.rs` → `assignor.rs` → `cooperative.rs` → `offset_state.rs` → `broker/src/p6_service.rs`. Draw cross-log owner fence.
10. **P7 idempotency:** `protocol/src/api/produce.rs` → `client/src/lib.rs` pending request → `replication/src/producer_state.rs` admission/dedupe → `broker/tests/p7_failover.rs`.
11. **P8 batching:** `client/src/batching.rs` → `broker/src/load_shaping.rs` → `broker/tests/interview_benchmark.rs` → generated release CSV/SUMMARY.
12. **P9 safety:** `storage/src/partition/compaction.rs` → `manifest.rs` → `replication/src/compaction_progress.rs` → `rebootstrap_v2.rs` → `storage_adapter.rs`; finally read `docs/P9_G_WORK_IN_PROGRESS.md`.

**Six questions to trace while reading:**
- Where can a record be durable but not quorum committed?
- Why can a lost reply lead to a successful mutation already on disk?
- What exact state lets a follower acknowledge AppendEntries?
- Why cannot controller metadata override the partition's actual Raft leader?
- What prevents a new consumer owner from starting before its predecessor's revoke is committed?
- How could an offline replica resurrect a deleted key if cleanup lacked all-voter proof?

# 16. Exact commands and test ownership

Run from Windows PowerShell at canonical root `D:\Workspace\AstraLog`. These are commands for future verification; **this document-writing turn does not constitute a new 737-test execution**.

```powershell
cd D:\Workspace\AstraLog

# Fast DEBUG smoke (not for published throughput):
powershell -NoProfile -ExecutionPolicy Bypass -File scripts/run-interview-demo.ps1 -Fast

# Full release demo and focused reliability suite:
powershell -NoProfile -ExecutionPolicy Bypass -File scripts/run-interview-demo.ps1

# Actual independent OS-process RF3 leader crash:
cargo test --offline --locked -p astralog-broker --test p4_replication rf3_separate_process_leader_kill_preserves_committed_quorum -- --exact --nocapture --test-threads=1

# Focused safety modules:
cargo test --offline --locked -p astralog-broker --test p7_failover -- --test-threads=1
cargo test --offline --locked -p astralog-broker --test p6_routing -- --test-threads=1

# Full, slow, serialized audit gate (only when a change justifies it):
cargo fmt --all -- --check
cargo clippy --workspace --all-targets --all-features --offline --locked -- -D warnings
cargo test --workspace --all-features --offline --locked -- --test-threads=1
```

**Test evidence guide:** `broker/tests/interview_benchmark.rs` = real RF1/RF3 TCP workloads and payload validation; `broker/tests/p2_network.rs` = client protocol and restart; `broker/tests/p4_replication.rs` = replicated broker, including new child-kill; `broker/tests/p5_dynamic.rs` = controller-managed topology and P9-G; `broker/tests/p6_routing.rs` = group/offset coordinator/failover; `broker/tests/p7_runtime.rs` and `p7_failover.rs` = idempotent producer/race/failover. `protocol/tests/*golden_vectors.rs` + `tests/golden/*` = exact frozen bytes; crate-local Rust unit tests validate formatting, persistence, state transitions and errors.

**Important provenance:** last exact-source-tree **local** serialized run (prior to this doc) passed 737/737, fmt and warning-denied Clippy passed. No GitHub-hosted Ubuntu run. If you modify source or initialize a new repository, do not pretend these results automatically become new-HEAD qualification.

**New public GitHub repo caution:** canonical local history contains older private path-bearing commits. Prepare from the history-free export, review files, start a **new** Git history and run fresh public-commit benchmarks. Do not push original `main` history.

# 17. High-impact interview presentation scripts

### 20-second recruiter pitch

“I built AstraLog, a Rust distributed event log from scratch with independent Raft replication per partition, a metadata-controller quorum, crash-recoverable segmented storage, consumer groups and idempotent producers. I demonstrated actual broker failure recovery and measured bounded batching improvements with source-pinned tests.”

### 90-second SDE interviewer pitch

“I chose partition-level Raft to keep ordering and failures independent per partition while reusing one explicit consensus safety model. A separate controller Raft group serializes topic assignments and broker incarnation authority, but is outside every normal Produce/Fetch hot path. The broker fronts a versioned binary TCP protocol; each partition actor connects pure Raft Input/Effect logic to durable segments and peer transport. I added consumer groups with durable ownership fences and producer sessions with epoch/sequence/digest-based retry deduplication. My strongest lesson came from adversarial regression: a follower was killed by an obsolete V1 rebootstrap file package even though it had already committed the prefix. I preserved the strict storage no-overwrite rule and skipped only that obsolete file replacement, with exact-byte tests. I also added a real static three-process leader-kill test. On a short four-producer RF3 quorum release workload I measured 10.71 to 33.31 records/sec with batching; I retain raw samples and explicitly do not claim production capacity.”

### Three-minute professional deep dive sequence

Draw a controller quorum, three broker actors and a topic with P0/P1. Trace **one** record all the way to disk and majority; annotate Raft index vs stream offset and ACK modes. Then choose **one** failure where a proof boundary matters: lost quorum ACK reply plus P7 dedup, P6 coordinator failover with owner fence, or stale V1 rebootstrap with no overwrite. End with the evidence and a limitation. A crisp failure invariant is more impressive than reciting 30 phase names.

### Tough question: “What would you change in a real company?”

“I would prefer a battle-tested Raft or Kafka component where appropriate; maintaining a custom consensus engine increases correctness obligations. For a production service I would first validate authenticated peer/client transport, Linux power-loss behavior and a real network fault matrix, then measure resource saturation and rolling upgrade behavior. This implementation was intentionally built for first-principles learning and auditable failure semantics.”

### Tough question: “How did you prove correctness?”

“I did not prove arbitrary correctness. I used deterministic Raft simulations, bounded formal/model tests, frozen wire/disk golden vectors, precise crash-point integrations, exact payload/offset oracles, and a complete serialized local regression. The limitations are visible: one green workspace run does not erase previous intermittent P9-G liveness failures or qualify production failure combinations.”


# 18. The adversarial interview question bank (answers you can defend)

Use the pattern **decision → invariant → failure counterexample → source/test evidence → limitation**. Never memorize the bold conclusion alone; be able to draw the relevant state transition. Questions are intentionally aggressive.

## Round A — architecture, scale and CAP

**Q01. Why build another broker rather than use Kafka?**

**Answer:** “For a production business problem I would normally prefer Kafka. AstraLog is an engineering project whose point was to expose consensus, recovery, wire formats, partition ownership and failure semantics rather than outsourcing them. I limited the actual portfolio target to demonstrable correctness and measurements. I learned more from a failed stale-snapshot test and its minimal fix than I would from wrapping a consumer API.”

**Follow-up:** Cost is significant maintenance and security burden; no claim of production feature parity.

**Q02. Why Raft instead of Kafka-style ISR replication?**

**Answer:** “Kafka's approach is valid, but it would require separately defining safe leader eligibility, epoch fencing, high-watermark behavior and reconfiguration. Raft makes majority election, log matching and commit a coherent proof model. AstraLog implements one Raft group per partition and reuses that core for metadata. The trade-off is more consensus instances and scheduling overhead.”

**Proof:** `raft/src/engine.rs` + `configuration.rs`.

**Q03. Why one Raft group per partition, not one global log?**

**Answer:** “A global log serializes independent topics and couples a slow/failing partition to unrelated traffic. A partition-local group scopes ordering, leadership and failure domains. Multi-Raft can scale across partitions in principle; it also increases heartbeat, election, transport and scheduling cost. I did not benchmark thousands of groups, so that's an architectural potential, not an achieved scale number.”

**Q04. Why not put etcd/ZooKeeper outside the system?**

**Answer:** “Using them would reduce operational risk, but external metadata consensus would hide a main systems-learning objective. The controller uses the same reusable Raft core with a metadata-specific state machine. Because controller authority is separate from data authority, existing Produce/Fetch need not synchronously call a controller.”

**Trade-off:** extra code to test, operate and secure.

**Q05. Why don't requests always pass through a gateway?**

**Answer:** “A mandatory gateway adds an additional network hop and a potential throughput/availability chokepoint. Clients bootstrap through any broker, learn partition leaders and route to leaders directly. They refresh metadata when terms/routes become stale. An optional stateless gateway could help simple clients later but shouldn't be required for data correctness.”

**Q06. How does AstraLog behave under CAP network partitions?**

**Answer:** “In a stable RF3 partition the majority side may progress, while a one-voter minority must not commit conflicting writes. Committed consistency matters more than minority write availability. This doesn't make the system globally CP across every API and topology; I specify the claim per Raft group and acknowledge that long-running real network fault qualification is incomplete.”

**Grind:** distinguish controller quorum loss from data-group quorum loss.

**Q07. RF3 tolerates how many failures? What about RF5?**

**Answer:** “Majority is floor(N/2)+1. RF3 needs two votes, so it can tolerate one voter loss for progress; RF5 needs three, so up to two. It cannot tolerate losing a majority, and a nominal replica count isn't enough if storage lies or the voter set is undergoing a joint transition.”

**Grind:** During joint membership, each old/new configuration needs its own quorum condition.

**Q08. Why Rust instead of Go, Java or C++?**

**Answer:** “Rust gives compile-time ownership and data-race protection for binary buffers and concurrent state without GC pauses, making low-level storage/protocol reasoning explicit. Go is simpler to ship, Java has mature tooling, and C++ offers control with more memory safety burden. Rust does not prove the distributed algorithm correct or guarantee that this prototype is faster; its learning curve is a real cost.”

**Q09. Why Tokio? Does async prevent blocking?**

**Answer:** “Tokio allows bounded multiplexing of sockets/timers across executors instead of a thread per partition, but async tasks can still starve if CPU-heavy or blocking disk work runs on executors. The broker uses bounded worker queues and blocking task offloads in specific paths; replication reserves control-message capacity. Fairness and exhaustion still need testing.”

**Grind:** differentiate `spawn_blocking` from an async function that performs synchronous fsync.

**Q10. Why a custom binary protocol rather than gRPC?**

**Answer:** “The project deliberately studies framing, correlation IDs, versioning, CRC, bounds and partial TCP reads. gRPC is usually a safer production default and already solves many of those concerns. Here the public frame is length-prefixed, versioned, validated, and max-bounded. The custom protocol does NOT provide TLS/authentication or a proven speed advantage.”

**Proof:** `protocol/frame.rs`, `decoder.rs`, `registry.rs`.

**Q11. What is the maximum size of a request? Can it OOM?**

**Answer:** “The public wire format has a 64 MiB maximum frame, plus negotiated and broker-specific smaller bounds, capped in-flight requests/response bytes and constrained decompression. That limits obvious allocations but a large permissible frame still has real memory cost, and a full adversarial exhaustion/fuzz campaign is not qualified.”

**Grind:** protocol bounds must cover allocations *before* decoding, not just after building a Vec.

**Q12. What does ordering mean here?**

**Answer:** “Each partition has one committed total order from Raft. A producer's accepted sequence obeys its per-partition state. No total order across partitions is promised, nor cross-topic transactions. A stable partition hash for a key helps preserve related-event locality, but partition count expansion is a versioned routing change.”

## Round B — consensus, persistence and tricky crash boundaries

**Q13. What is the difference between a durable append and a committed append?**

**Answer:** “A durable append exists on a local disk according to its storage sync contract. Commit is consensus authorization from the voting quorum. A leader may persist a record that remains uncommitted, and a successor may truncate it. `acks=quorum` must not return success merely because the leader called fsync.”

**Proof:** `raft::Effect::Persist`, `replication::ProposalOutcome` and the P7 local-persist-without-quorum regression.

**Q14. If a leader acknowledges `acks=leader`, can you lose that event?**

**Answer:** “Yes. The leader may have durably appended it while followers lack a majority. A successor can legally choose another log and remove the uncommitted suffix. That's an explicit weaker API contract. If the business requires surviving a minority of voters, use quorum acknowledgements and an honest durability model.”

**Q15. Why separate a pure Raft engine from disk and networking?**

**Answer:** “The core emits deterministic Input→Effect transitions, so I can simulate elections/reordering, inspect effect order and inject persist failures without real sockets. Crucially it cannot assume persistence completed; the runtime reports `PersistenceCompleted(token)`. This prevents sending votes or replication acknowledgements prematurely. The trade-off is more orchestration code and tokens to track.”

**Proof:** `raft/engine.rs`.

**Q16. A follower gets an AppendEntries whose prefix conflicts with committed data. What happens?**

**Answer:** “It must reject rather than overwrite a committed prefix. Raft can truncate uncommitted conflicts under a valid newer leader, but log matching and commit-index guards preserve committed history. Deleting committed bytes to make a failing test green would violate the primary invariant.”

**Grind:** the sender must also pass term and authority checks.

**Q17. Can two leaders exist simultaneously?**

**Answer:** “Two processes may temporarily *believe* they lead under old observations, but at most one legitimate winner per Raft term can gain the required voting majority, and stale-term operations are fenced. Quorum intersection protects committed history. What matters is authorized commits, not a UI label saying leader.”

**Grind:** lease-based split brain protection is not the algorithm here.

**Q18. What if the controller leader dies midway through topic creation?**

**Answer:** “If the command committed and applied, another controller reconstructs the metadata state and topic ID; if it didn't commit, the API cannot promise success. A lost response may be ambiguous, so callers must reconcile authoritative metadata rather than assume noncommit. The data-plane leaders remain elected by their own Raft groups, not by the controller's in-memory intention.”


**Q19. Why PreVote?**

**Answer:** “A disconnected follower whose timer fires should not immediately increment the cluster term and disrupt a healthy leader. PreVote asks whether a prospective election is plausible before changing durable voting state. It improves liveness during intermittent links without removing Raft's actual RequestVote and durable term requirements.”

**Q20. What is a leader barrier entry?**

**Answer:** “A new leader may need to commit an entry in its current term before treating prior entries as safely committed under Raft's commitment rule. AstraLog's current-term leader initialization/barrier and idempotent readiness checks prevent treating an older local tail as authoritative merely because a leader was elected. It costs a round of consensus but makes authority explicit.”

**Q21. Why can't an old snapshot replace files even if its Raft metadata is legal?**

**Answer:** “Snapshot term/leader metadata can be valid while its included index is already at or below the receiver's durable commit. Storage must not overwrite committed bytes. The Nov 3 P9-G audit reproduced a fatal V1 package race; the narrow fix drops only obsolete file replacement and still processes the Raft InstallSnapshot message. The exact payload and original durable commit remain.”

**Grind:** do not delete the `storage_adapter` no-overwrite guard.

**Q22. Is restarting a broker in the same test process equivalent to `kill -9`?**

**Answer:** “No. A method-level `shutdown()` exercises graceful cleanup and different failure ordering. We added an integration test using three distinct broker OS child processes and `Child::kill()` on the elected leader, then validated old exact committed bytes and a new quorum append. It still doesn't test machine loss, disk power cut or dynamic P9-G multi-process failover.”

**Q23. Your Windows file sync completes. Are you power-loss durable?**

**Answer:** “I can't claim that. Files call `sync_all`, but directory metadata sync is explicitly no-op on non-Unix in `storage/io_util.rs`. A successful process kill/restart is different from power interruption during rename or a lying disk cache. Production durability qualification needs the intended Linux filesystem and physical/fault-injected crash matrix.”

**Q24. What if an index file disappears?**

**Answer:** “An index is derived state. Authoritative data is in valid segment frames and committed metadata. On restart a missing/invalid index can be rebuilt from verified segments; losing indexes must not silently delete committed events. Corrupt authoritative frames are different and must be surfaced or repaired only with a healthy replica.”

**Proof:** `storage/index.rs` and `segment.rs`.

**Q25. Why CRC32C plus SHA-256? Why isn't CRC a security guarantee?**

**Answer:** “CRC32C cheaply detects accidental frame corruption; SHA-256 confirms exact V2 package file bytes and complements canonical manifest validation. Neither authenticates which remote entity sent a frame; an attacker may forge data and its checksum. That's why the current system is loopback-restricted and still needs TLS/mTLS and identity verification for multi-host deployment.”

## Round C — controller, placement and migration

**Q26. Who decides actual partition leadership: controller or broker Raft?**

**Answer:** “Controller Raft commits desired membership/placement. Partition Raft alone elects the live leader and term. A controller can't simply write ‘A is leader’ and override election safety. Brokers observe P3 leadership and expose it through metadata; clients refresh route/epochs on stale responses.”

**Q27. A controller quorum disappears. Can producers still write?**

**Answer:** “Existing correctly routed data partitions with valid quorum and known producer authority should continue; controller is not consulted on every Produce. New topic creation and new/unknown/higher producer epoch authorization may fail closed. The system's availability needs to be stated separately for metadata mutations and partition data operations.”

**Proof:** P7 controller-outage tests.

**Q28. How do you stop an old broker process rejoining with the same ID?**

**Answer:** “Controller commits monotonic broker incarnation tied to identity; a newer incarnation fences the old one. Transport and placement checks compare identity/epoch rather than a host/port string. A stale process cannot regain authoritative membership solely by sending heartbeats.”

**Q29. How do you move a replica without reducing quorum safety?**

**Answer:** “Commit movement intent, add target as learner, synchronize its durable log, then use Raft joint consensus for voter transition, observe stable new membership, optionally transfer leadership, and remove old source only after committed exclusion. A response to a movement RPC is not proof that the membership transition committed.”

**Proof:** `controller/movement.rs`, `reassignment.rs`, `broker/replication.rs`.

**Q30. What if migration crashes after adding a learner but before promotion?**

**Answer:** “Controller retains durable movement stage and P3 actual membership can be reobserved; reconciliation resumes from the committed boundary rather than blindly replaying the next mutating RPC. The learner must not vote before catch-up and promotion, preserving election safety.”

**Q31. What if a required rack/zone constraint is impossible?**

**Answer:** “The deterministic placement planner rejects Required placement rather than silently degrade to correlated replicas. A Preferred policy may make an explicit documented fallback. The policy should be a visible availability/placement trade-off, never a hidden reduction of failure isolation.”

**Q32. Why cache metadata deltas instead of downloading a snapshot every time?**

**Answer:** “Contiguous committed deltas reduce bandwidth during normal evolution. A broker applies only monotonic epochs and detects gaps/foreign cluster IDs; when incremental history is insufficient, it falls back to a full authoritative snapshot. That balances performance with self-repair while keeping controller committed order the source of truth.”

## Round D — consumers, groups and offsets

**Q33. Why two P6 internal topics instead of one?**

**Answer:** “Group membership/assignment and offset commits have distinct load and leadership behavior. Two RF3 sharded logs allow independent routing and recovery. The hard trade-off is cross-log consistency: the group state cannot activate a new owner before offset group/owner fences commit. We explicitly test that boundary rather than pretending cross-log operations are atomic.”

**Q34. How do you prove two group members never simultaneously own a partition?**

**Answer:** “A pending revocation keeps the old owner active until exact durable RevokeAck releases it. An OffsetOwnerFence then proves old ownership is fenced; only then can the new target activate. The state machine refuses overwriting an active owner. There are focused failover tests for leader death at the fencing boundary.”

**Q35. Coordinator dies after PREPARING but before offset fence. What happens?**

**Answer:** “A successor replays committed group records into PREPARING; it must not silently set the new active epoch. It re-establishes the required offset fence, then performs the next committed transition. The workflow is resumable from committed facts instead of in-memory orchestration state.”

**Q36. Why don't you replicate every consumer heartbeat?**

**Answer:** “Heartbeats are frequent liveness observations, and replicating them would inflate disk/quorum traffic. Member epochs and ownership changes are durable authority; heartbeats are leader-local timers. After coordinator failover the new leader grants fresh session grace, delaying expiry rather than falsely removing a live member from stale predecessor timing.”

**Q37. An old consumer commits offsets after rebalance. Who rejects it?**

**Answer:** “Offset commit checks the committed GroupRecord authority and the offset-log group/owner fence, including exact member and group epochs. An old active owner cannot commit after the release/owner fence has committed; stale messages fail instead of moving the bookmark backwards or stealing active ownership.”

**Q38. JoinSubscribe timed out. Why not retry with the same GroupId?**

**Answer:** “An anonymous join may already have durably committed before the response was lost. A second join can create another member. The client returns AmbiguousMutation; the P6 fixture safely reads the unique committed member record and accepts only a matching Stable epoch-1 state. If it cannot prove success, it fails rather than duplicating the mutation.”

**Q39. Does committing an offset guarantee exactly-once processing?**

**Answer:** “No. The consumer may update a database and crash before committing its offset, then reprocess. Or it may commit an offset and crash before completing an external side effect if the app chooses bad ordering. Exactly-once external effects need an application-side transaction, inbox/outbox or idempotent processing contract.”

**Q40. Why uniform-sticky assignment rather than simple round robin?**

**Answer:** “Round robin can rebalance unnecessarily on membership churn. The deterministic assignor first minimizes maximum load, then minimizes movement from eligible previous owners, then resolves ties canonically. It makes scheduling reproducible and limits disruption at the cost of a more complex algorithm and explicit bounds.”


## Round E — producer idempotency and retry traps

**Q41. Leader commits Produce, reply is lost, then leader dies. What prevents duplicates?**

**Answer:** “The client retains the exact pending producer ID, epoch, sequence and canonical body. The committed partition state stores sequence range, digest and original offsets. The successor recovers that committed state and returns Duplicate with original offsets, not a new append. P7 RF3 lost-response and death-after-quorum tests verify exactly one retained payload.”

**Grind:** this applies to qualified P7 idempotent requests, not arbitrary non-idempotent API calls.

**Q42. Why not generate a new sequence whenever a Produce call times out?**

**Answer:** “A timeout is ambiguous. The old batch may already have committed. A fresh sequence could commit the same business event again. Freeze the unresolved request and replay only the same sequence/bytes; advance sequence after authoritative success. A changed-value retry at the same sequence is rejected.”

**Q43. What if a producer skips sequence number 7?**

**Answer:** “The partition state rejects a gap. Otherwise the system would be silently inventing missing history or accepting reordering with unknown intent. The producer must resolve outstanding previous work rather than ‘repair’ the gap by jumping ahead.”

**Q44. Why track producer epochs if sequence numbers already exist?**

**Answer:** “A crashed or zombie producer instance might resume with stale local sequence state while a replacement has legitimately advanced. Controller-authorized epochs make the newer instance authoritative; the old epoch is fenced. Higher epochs themselves must become quorum-committed in the partition before they can be treated as surviving cluster authority after failover.”

**Q45. What if epoch 2 was written on one leader only, then that leader dies?**

**Answer:** “Tentative epoch 2 may fence epoch 1 on the old leader, but if it never committed, a new leader reconstructs epoch 1 from committed history; epoch 1 can legitimately continue. Once epoch 2 is quorum committed, successors inherit it and reject epoch 1. Both cases have P7 failover tests.”

**Q46. Why can already-known producer epochs work during controller outage?**

**Answer:** “The controller is global authority for issuing ProducerId/Epoch, but each partition's committed state proves epochs already admitted there. Normal same-epoch appends do not need a synchronous controller lookup. An unknown or higher epoch requires fresh controller evidence; during controller outage that case fails closed. It preserves data-plane availability without trusting unknown authority.”

**Q47. What happens when your five-batch dedup window is exceeded?**

**Answer:** “The state machine cannot prove an old range is exactly the committed batch, so it rejects conservatively rather than append a possible duplicate. A finite history makes state/recovery bounded but does not offer unlimited replay idempotency. I'd expose this as an API constraint.”

**Q48. Does Raft itself implement idempotency?**

**Answer:** “No. Raft ensures a single committed order, but if the client submits the same business request as two different log commands, Raft can legitimately commit both. Producer ID + epoch + sequence/digest logic provides the qualified deduplication contract on top of Raft.”

## Round F — compaction, measurements and security

**Q49. How can removing a tombstone cause data loss or stale data resurrection?**

**Answer:** “If offline voter B holds an old K=value, while current leader A removes K's delete marker, B can later regain leadership and expose stale K. Correctness requires verified cleaning across every eligible voter plus quorum-committed safe floor, grace/snapshot checks and rebootstrap fencing for behind replicas. This is a cross-replica correctness property, not just disk cleanup.”

**Q50. Why not compact the uncommitted Raft tail to save space?**

**Answer:** “Raft needs exact log indexes/terms for conflict matching, votes and future catch-up. Rewriting a suffix that may be reconciled by a new leader can violate log matching. Physical COW compaction is restricted below snapshot/commit safety floors, preserving logical offsets and required consensus anchors.”

**Q51. Why does your P9-G feature default to off?**

**Answer:** “The progress sideband and retained V2 rebootstrap require peer capability and committed feature-level agreement. Turning it on against older peers can break compatibility, and sustained V2 transfer liveness is not yet release-qualified. A safe system does not activate destructive cleanup based on a design promise or a single green suite.”

**Q52. Why does the old V1 snapshot bug matter professionally?**

**Answer:** “It demonstrates that correctness requires separating Raft metadata authority from the bytes physically installed. The follower's committed prefix was already newer, and storage correctly refused overwrite. The fix preserved the storage guard while letting legal snapshot metadata processing proceed; an exact-value regression replaced optimistic timing assumptions.”

**Q53. How were p50, p95 and p99 calculated?**

**Answer:** “The new harness saves each Produce/Fetch request's elapsed nanoseconds. The summary script independently recomputes nearest-rank percentile from raw samples, checks expected sample counts and records Git SHA, build profile and dirty status. The report uses medians of per-repeat percentiles. When samples are under 100, a nearest-rank p99 is the slowest observed request and is statistically weak.”

**Q54. Why did buffering improve both throughput and reported p95 here?**

**Answer:** “In this narrow workload fixed request/fsync/replication overhead was likely important, so amortizing it reduced the measured per-record cost. It doesn't prove batching always lowers tail latency; waiting for batch linger could make sparse workloads worse. I would run variance-controlled trials, trace queue wait and fsync time, and compare exact workload topology before naming a bottleneck.”

**Q55. How would you benchmark failure recovery rather than steady state?**

**Answer:** “Run a steady acknowledged producer stream with exact producer IDs/sequences, kill the elected leader at a known point, and measure last success→new leader→first new quorum success. After recovery, scan committed records for missing ACKed bytes, duplicates and offset discontinuity; report error counts and latency spikes across repeated seeded runs, not one best case.”

**Q56. Is your security model adequate for internet deployment?**

**Answer:** “No. Current client/broker/peer transport is unauthenticated and deliberately restricted to loopback binding. CRC and claimed broker IDs are not authentication. Production would need TLS/mTLS, client identity, ACL enforcement, key rotation, secure bootstrap and negative adversarial tests. I won't call local confinement production security.”

**Q57. What is the single biggest production risk?**

**Answer:** “It's not one provably dominant risk. For deployment, unauthenticated networking and unverified platform durability are immediate blockers; P9-G sustained rebootstrap liveness, full fault matrix and CI portability remain qualification gaps. I would prioritize those according to intended deployment threat model rather than add more features.”

**Q58. Is 737/737 enough to say the distributed algorithm is correct?**

**Answer:** “No. It gives meaningful local evidence on selected deterministic and integration schedules. Distributed failure space is large: message order, partial writes, membership changes, stale peers, restart timing and backpressure. We retain prior failing P9/P6 observations, add exact counterexample tests, and separate source-tree regression from exact-HEAD release gates.”

**Q59. What specific independent learning did you gain?**

**Answer:** “Three lessons: (1) durable local state and committed replicated state are different; (2) a network timeout is an unknown transaction outcome, not a rollback; (3) valid metadata progression must not authorize destructive replacement of a committed file generation. I can point to implementations and tests of all three.”

**Q60. What are you most proud of?**

**Answer:** “The consistency of the engineering method: I didn't hide intermittent failures, treated exact payload/offset identity as the oracle, built a real process-kill test instead of relabeling graceful shutdown, and bounded the P9-G stale-snapshot fix without loosening the core safety guard. The honest evidence and limitations make the project defensible.”

## Five whiteboard drills interviewers can add

1. **RF3 A/B/C:** A and B persist index 12 but A dies before its response. Can C become leader and erase 12? Analyze *whether 12 was committed* and whether candidate's log is up-to-date—do not infer from a physical byte on one disk.
2. **Joint voters old {A,B,C}, new {B,C,D}:** Which quorums are required to commit during transition? What if D is caught up but still a learner?
3. **Consumer group:** A currently owns P0; coordinator commits target owner B but offset owner fence is lost. Can B start? Why does group state replay alone not grant B authority?
4. **Producer:** epoch 3 seq 5 committed, reply lost; retry has seq 5 but changed payload. Should server return old offset, append again, or reject? Explain digest.
5. **Tombstone:** leader cleans K on A/C but B is offline. Should committed safe floor advance? If controller later excludes B, which P3 membership commit must exist before safe authority changes?

## Ten questions YOU should ask the interviewer back

- “Which failure envelope matters: one broker process, whole machine, disk loss or zonal partition?”
- “Is consistency per partition sufficient or do you require a cross-partition transaction?”
- “Can downstream operations be idempotent, or is external exactly-once required?”
- “What is the accepted durability promise: process crash, OS crash, power loss or regional outage?”
- “Do you prefer predictable p99, peak throughput, cost, or availability under partial quorum?”
- “Would you isolate metadata into an existing consensus service in production?”
- “Should inactive consumers retain old history, and for how long?”
- “Can the security boundary be a trusted network, or is mutual-auth identity mandatory?”
- “What are the compatibility requirements during rolling upgrades?”
- “What quantitative result would convince you a new batching or snapshot optimization actually helped?”


# 19. Every directory: purpose and navigation

This is a **source-derived inventory** of the complete 311 tracked files at audited HEAD, plus this newly authored guide (312 paths). It covers **all tracked file paths and their parent directories**, not ignored build outputs, local Git history or untracked temporary logs. Each row is a navigation aid; read source and tests to establish actual behavior. New files/commits require regenerating the inventory.

The full list of tracked parent directories is below. A directory name is a *location*, not an independently implemented feature.

| Directory | Purpose / context |
|---|---|
| `.github/workflows` | Repository configuration or workflow support. |
| `crates/astralog-benchmark` | Rust implementation/test workspace for astralog-benchmark |
| `crates/astralog-benchmark/src` | Rust implementation/test workspace for astralog-benchmark |
| `crates/astralog-benchmark/src/bin` | Rust implementation/test workspace for astralog-benchmark |
| `crates/astralog-broker` | Rust implementation/test workspace for astralog-broker |
| `crates/astralog-broker/src` | Rust implementation/test workspace for astralog-broker |
| `crates/astralog-broker/tests` | Rust implementation/test workspace for astralog-broker |
| `crates/astralog-client` | Rust implementation/test workspace for astralog-client |
| `crates/astralog-client/src` | Rust implementation/test workspace for astralog-client |
| `crates/astralog-common` | Rust implementation/test workspace for astralog-common |
| `crates/astralog-common/src` | Rust implementation/test workspace for astralog-common |
| `crates/astralog-controller` | Rust implementation/test workspace for astralog-controller |
| `crates/astralog-controller/src` | Rust implementation/test workspace for astralog-controller |
| `crates/astralog-groups` | Rust implementation/test workspace for astralog-groups |
| `crates/astralog-groups/src` | Rust implementation/test workspace for astralog-groups |
| `crates/astralog-protocol` | Rust implementation/test workspace for astralog-protocol |
| `crates/astralog-protocol/src` | Rust implementation/test workspace for astralog-protocol |
| `crates/astralog-protocol/src/api` | Rust implementation/test workspace for astralog-protocol |
| `crates/astralog-protocol/tests` | Rust implementation/test workspace for astralog-protocol |
| `crates/astralog-protocol/tests/golden` | Rust implementation/test workspace for astralog-protocol |
| `crates/astralog-raft` | Rust implementation/test workspace for astralog-raft |
| `crates/astralog-raft/src` | Rust implementation/test workspace for astralog-raft |
| `crates/astralog-replication` | Rust implementation/test workspace for astralog-replication |
| `crates/astralog-replication/src` | Rust implementation/test workspace for astralog-replication |
| `crates/astralog-storage` | Rust implementation/test workspace for astralog-storage |
| `crates/astralog-storage/src` | Rust implementation/test workspace for astralog-storage |
| `crates/astralog-storage/src/partition` | Rust implementation/test workspace for astralog-storage |
| `crates/astralog-storage/tests/golden` | Rust implementation/test workspace for astralog-storage |
| `docs` | Architecture, phase contracts, test evidence and benchmark history. |
| `docs/baselines` | Architecture, phase closure/ledger or retained benchmark evidence. |
| `docs/benchmarks/interview_20261103` | Architecture, phase closure/ledger or retained benchmark evidence. |
| `docs/benchmarks/remediation_20261103` | Architecture, phase closure/ledger or retained benchmark evidence. |
| `scripts` | Qualification and benchmark/publication helper scripts. |

# 20. Complete 312-path annotated source/document inventory

All links are relative to this guide under `docs/`. Rust rows include up to three public code landmarks extracted from the file, when present. **No listed stage is inferred complete from the mere presence of a file.**

## repository root (7 files)

| File | What it contains / why it exists | Representative public code landmarks |
|---|---|---|
| [`.gitignore`](../.gitignore) | Ignore local/generated build outputs, not a safety/privacy scan. | Not applicable |
| [`Cargo.lock`](../Cargo.lock) | Locked exact Cargo dependency resolutions; required for reproducible builds. | Not applicable |
| [`Cargo.toml`](../Cargo.toml) | Workspace members, shared dependencies, edition, pinned compatible Rust and release profile. | Not applicable |
| [`LICENSE`](../LICENSE) | Apache-2.0 license text. | Not applicable |
| [`README.md`](../README.md) | Top-level introduction, status, demo and links; check current qualifications. | Not applicable |
| [`SECURITY.md`](../SECURITY.md) | Threat/deployment boundary: loopback-only; no TLS/auth; Windows power-loss caveat. | Not applicable |
| [`rust-toolchain.toml`](../rust-toolchain.toml) | Pins Rust 1.98.1, rustfmt and Clippy components. | Not applicable |

## .github (1 files)

| File | What it contains / why it exists | Representative public code landmarks |
|---|---|---|
| [`.github/workflows/ci.yml`](../.github/workflows/ci.yml) | Pinned Ubuntu 24.04 workflow for format, Clippy and locked tests (hosted run not yet verified). | Not applicable |

## scripts (7 files)

| File | What it contains / why it exists | Representative public code landmarks |
|---|---|---|
| [`scripts/prepare-public-snapshot.ps1`](../scripts/prepare-public-snapshot.ps1) | Create a committed-tree history-free export; does not publish. | Not applicable |
| [`scripts/qualify-p1.ps1`](../scripts/qualify-p1.ps1) | Windows storage phase P1 qualification runner. | Not applicable |
| [`scripts/qualify-p1.sh`](../scripts/qualify-p1.sh) | Unix/Linux storage phase P1 qualification runner. | Not applicable |
| [`scripts/qualify-p2.ps1`](../scripts/qualify-p2.ps1) | Windows protocol/client P2 qualification runner. | Not applicable |
| [`scripts/qualify-p2.sh`](../scripts/qualify-p2.sh) | Unix/Linux protocol/client P2 qualification runner. | Not applicable |
| [`scripts/run-interview-demo.ps1`](../scripts/run-interview-demo.ps1) | Native Cargo demo launcher; RF1/RF3 benchmark, focused tests and logs. | Not applicable |
| [`scripts/summarize-interview-benchmark.ps1`](../scripts/summarize-interview-benchmark.ps1) | Independently validate raw latency samples, provenance and percentiles. | Not applicable |

## crates/astralog-benchmark (8 files)

| File | What it contains / why it exists | Representative public code landmarks |
|---|---|---|
| [`crates/astralog-benchmark/Cargo.toml`](../crates/astralog-benchmark/Cargo.toml) | Internal crate dependencies and package manifest. | Not applicable |
| [`crates/astralog-benchmark/src/bin/astralog-benchmark.rs`](../crates/astralog-benchmark/src/bin/astralog-benchmark.rs) | CLI benchmark entry point. | Not applicable |
| [`crates/astralog-benchmark/src/lib.rs`](../crates/astralog-benchmark/src/lib.rs) | Benchmark library exports. | `BenchmarkError` |
| [`crates/astralog-benchmark/src/manifest.rs`](../crates/astralog-benchmark/src/manifest.rs) | Workload/source configuration manifest. | `KeyMode, CompressionSpec, AckModeSpec` |
| [`crates/astralog-benchmark/src/matrix.rs`](../crates/astralog-benchmark/src/matrix.rs) | Configuration/workload matrix. | `WorkloadCell, id, coverage` |
| [`crates/astralog-benchmark/src/report.rs`](../crates/astralog-benchmark/src/report.rs) | Benchmark report construction. | `generate_report_markdown, generate_comparison_markdown` |
| [`crates/astralog-benchmark/src/result.rs`](../crates/astralog-benchmark/src/result.rs) | Measurement/error outcome schema. | `BenchmarkProvenance, validate, SystemMetricsSnapshot` |
| [`crates/astralog-benchmark/src/runner.rs`](../crates/astralog-benchmark/src/runner.rs) | Workload execution and latency collection. | `ensure_release_profile, validate_runtime_provenance, run_benchmark` |

## crates/astralog-broker (20 files)

| File | What it contains / why it exists | Representative public code landmarks |
|---|---|---|
| [`crates/astralog-broker/Cargo.toml`](../crates/astralog-broker/Cargo.toml) | Internal crate dependencies and package manifest. | Not applicable |
| [`crates/astralog-broker/src/catalog.rs`](../crates/astralog-broker/src/catalog.rs) | Static standalone topic catalog. | `TopicDefinition, CatalogSnapshot, Catalog` |
| [`crates/astralog-broker/src/controller.rs`](../crates/astralog-broker/src/controller.rs) | Dynamic controller router, metadata/producer authority bridge. | `PreferredLeaderRequest, PreferredLeaderDisposition, PreferredLeaderOutcome` |
| [`crates/astralog-broker/src/error.rs`](../crates/astralog-broker/src/error.rs) | Public broker errors and domain mapping. | `BrokerError` |
| [`crates/astralog-broker/src/group_coordinator.rs`](../crates/astralog-broker/src/group_coordinator.rs) | P6 group record commits/replay on internal topic. | `GroupCoordinatorRuntimeConfig, validate, GroupCoordinatorError` |
| [`crates/astralog-broker/src/lib.rs`](../crates/astralog-broker/src/lib.rs) | Server, broker/public export surfaces and modules. | Not applicable |
| [`crates/astralog-broker/src/load_shaping.rs`](../crates/astralog-broker/src/load_shaping.rs) | P8 admission, quota, inflight byte and pressure controls. | `DataAdmissionLimits, validate, DataAdmissionRejection` |
| [`crates/astralog-broker/src/metadata_cache.rs`](../crates/astralog-broker/src/metadata_cache.rs) | Monotonic committed metadata delta/snapshot application. | `BrokerMetadataCache, from_snapshot, applied_metadata_epoch` |
| [`crates/astralog-broker/src/offset_coordinator.rs`](../crates/astralog-broker/src/offset_coordinator.rs) | P6 offset and ownership-fence persistence/replay. | `OffsetCoordinatorRuntimeConfig, validate, OffsetCoordinatorError` |
| [`crates/astralog-broker/src/p6_service.rs`](../crates/astralog-broker/src/p6_service.rs) | P6 join, rebalance, sessions and group/offset workflow orchestration. | `P6Failure, P6Service, new` |
| [`crates/astralog-broker/src/replication.rs`](../crates/astralog-broker/src/replication.rs) | Partition actors, peer/outbound traffic, topology, P9 controls. | `StaticBrokerConfig, StaticTopicConfig, partition_count` |
| [`crates/astralog-broker/src/server.rs`](../crates/astralog-broker/src/server.rs) | TCP listener, connection and frame limits, API dispatch, BrokerHandle. | `BrokerConfig, new, BrokerHandle` |
| [`crates/astralog-broker/src/state.rs`](../crates/astralog-broker/src/state.rs) | Public produce/fetch/metadata/topic logic and partition routing. | `RuntimeConfig, BrokerState, run_delete_retention_once` |
| [`crates/astralog-broker/tests/interview_benchmark.rs`](../crates/astralog-broker/tests/interview_benchmark.rs) | RF1/RF3 real client workload, exact fetched payload oracle and raw latency samples. | Not applicable |
| [`crates/astralog-broker/tests/p2_network.rs`](../crates/astralog-broker/tests/p2_network.rs) | Single broker protocol round trip, limits and restart. | Not applicable |
| [`crates/astralog-broker/tests/p4_replication.rs`](../crates/astralog-broker/tests/p4_replication.rs) | RF3 broker replication, including independent OS-process leader kill. | Not applicable |
| [`crates/astralog-broker/tests/p5_dynamic.rs`](../crates/astralog-broker/tests/p5_dynamic.rs) | Controller-managed leader/reassignment and P9-G RF3/RF4 integration. | Not applicable |
| [`crates/astralog-broker/tests/p6_routing.rs`](../crates/astralog-broker/tests/p6_routing.rs) | Group/offset routing, coordinator failure and ambiguity reconciliation. | Not applicable |
| [`crates/astralog-broker/tests/p7_failover.rs`](../crates/astralog-broker/tests/p7_failover.rs) | RF3 idempotent retry/epoch/leader-failover byte oracles. | Not applicable |
| [`crates/astralog-broker/tests/p7_runtime.rs`](../crates/astralog-broker/tests/p7_runtime.rs) | P7 producer client/broker/controller runtime qualification. | Not applicable |

## crates/astralog-client (3 files)

| File | What it contains / why it exists | Representative public code landmarks |
|---|---|---|
| [`crates/astralog-client/Cargo.toml`](../crates/astralog-client/Cargo.toml) | Internal crate dependencies and package manifest. | Not applicable |
| [`crates/astralog-client/src/batching.rs`](../crates/astralog-client/src/batching.rs) | Per-partition P8 accumulator, linger/record/bytes bounds. | `BatchMode, BufferedOffsets, AccumulatorLimits` |
| [`crates/astralog-client/src/lib.rs`](../crates/astralog-client/src/lib.rs) | AstraClient metadata routing, request deadlines, producer/group retry semantics. | `ClientError, ClientConfig, new` |

## crates/astralog-common (2 files)

| File | What it contains / why it exists | Representative public code landmarks |
|---|---|---|
| [`crates/astralog-common/Cargo.toml`](../crates/astralog-common/Cargo.toml) | Internal crate dependencies and package manifest. | Not applicable |
| [`crates/astralog-common/src/lib.rs`](../crates/astralog-common/src/lib.rs) | Checked canonical domain identities and offsets; prevents interchanging producer/stream/Raft numeric domains. | `TopicId, PartitionId, StreamOffset` |

## crates/astralog-controller (30 files)

| File | What it contains / why it exists | Representative public code landmarks |
|---|---|---|
| [`crates/astralog-controller/Cargo.toml`](../crates/astralog-controller/Cargo.toml) | Internal crate dependencies and package manifest. | Not applicable |
| [`crates/astralog-controller/src/bootstrap.rs`](../crates/astralog-controller/src/bootstrap.rs) | Immutable cluster identity/static voter-set bootstrap. | `BootstrapInstallOutcome, ControllerBootstrapManifest, new` |
| [`crates/astralog-controller/src/command.rs`](../crates/astralog-controller/src/command.rs) | Versioned replicated metadata commands/validation. | `CommandLimits, ControllerCommand, encode_command` |
| [`crates/astralog-controller/src/error.rs`](../crates/astralog-controller/src/error.rs) | Controller domain errors. | `ControllerError` |
| [`crates/astralog-controller/src/failure_qualification.rs`](../crates/astralog-controller/src/failure_qualification.rs) | Controller adversarial/seeded tests, test-only. | Not applicable |
| [`crates/astralog-controller/src/feature.rs`](../crates/astralog-controller/src/feature.rs) | Committed active-feature negotiation/fencing. | `FeatureRange, new, FeatureGate` |
| [`crates/astralog-controller/src/heartbeat.rs`](../crates/astralog-controller/src/heartbeat.rs) | Volatile heartbeat/failure-detector observations. | `HeartbeatLimits, validate, ReplicaProgressReport` |
| [`crates/astralog-controller/src/lib.rs`](../crates/astralog-controller/src/lib.rs) | Controller Raft/metadata API and child modules. | Not applicable |
| [`crates/astralog-controller/src/membership.rs`](../crates/astralog-controller/src/membership.rs) | Broker register/rejoin, UUID/incarnation/lifecycle checks. | `BrokerRegistration, into_command, BrokerRejoin` |
| [`crates/astralog-controller/src/metadata.rs`](../crates/astralog-controller/src/metadata.rs) | Committed epoch deltas and snapshot fallback. | `MetadataDelta, ControllerMetadataSnapshot, from_state` |
| [`crates/astralog-controller/src/model.rs`](../crates/astralog-controller/src/model.rs) | Typed durable topics, brokers, assignments, lifecycle and movements. | `BrokerLifecycle, TopicLifecycle, FailureDomainPolicy` |
| [`crates/astralog-controller/src/movement.rs`](../crates/astralog-controller/src/movement.rs) | Observed reassignment stages, durable learner/voter progress. | `ReplicaProgressObservation, MovementProgressObservation, MovementObservations` |
| [`crates/astralog-controller/src/peer_transport.rs`](../crates/astralog-controller/src/peer_transport.rs) | Controller Raft peer network transport and validation. | `ControllerPeerDescriptor, ControllerPeerTransportConfig, validate` |
| [`crates/astralog-controller/src/performance.rs`](../crates/astralog-controller/src/performance.rs) | P8 topic performance policy. | `CompressionPolicyV1, TopicPerformancePolicyV1, TopicPerformanceSpecV1` |
| [`crates/astralog-controller/src/performance_codec.rs`](../crates/astralog-controller/src/performance_codec.rs) | P8 metadata codec/version evolution. | `TopicPerformanceCommandV3, encode_topic_performance_command_v3, decode_topic_performance_command_v3` |
| [`crates/astralog-controller/src/persistence.rs`](../crates/astralog-controller/src/persistence.rs) | Controller Raft hard state/log/commit/snapshot durability. | `ControllerPersistenceConfig, ControllerSnapshotPackage, encode_controller_snapshot_package` |
| [`crates/astralog-controller/src/placement.rs`](../crates/astralog-controller/src/placement.rs) | Deterministic RF and failure-domain replica assignment. | `PlacementBrokerSnapshot, PlacementInputSnapshot, from_controller_state` |
| [`crates/astralog-controller/src/producer_codec.rs`](../crates/astralog-controller/src/producer_codec.rs) | P7 producer authority metadata log encoding. | `ProducerSessionCommandV2, ControllerLogCommand, encode_controller_log_command` |
| [`crates/astralog-controller/src/reassignment.rs`](../crates/astralog-controller/src/reassignment.rs) | Topic-partition reassignment request/plan model. | `ReassignmentSpec, ReassignmentResult, PreparedReassignment` |
| [`crates/astralog-controller/src/retention.rs`](../crates/astralog-controller/src/retention.rs) | P9 topic cleanup/retention policy. | `CleanupPolicyV1, SubscriptionProtectionV1, TopicRetentionPolicyV1` |
| [`crates/astralog-controller/src/retention_codec.rs`](../crates/astralog-controller/src/retention_codec.rs) | P9 durable policy command codec. | `TopicRetentionCommandV4, encode_topic_retention_command_v4, decode_topic_retention_command_v4` |
| [`crates/astralog-controller/src/runtime.rs`](../crates/astralog-controller/src/runtime.rs) | Controller Runtime, quorum mutations, leader client/node handles. | `ControllerRuntimeLimits, ControllerRuntimeConfig, MutationReceipt` |
| [`crates/astralog-controller/src/snapshot.rs`](../crates/astralog-controller/src/snapshot.rs) | Controller metadata snapshot V1 codec. | `SnapshotLimits, canonical_snapshot_worst_case_bytes, ControllerSnapshot` |
| [`crates/astralog-controller/src/snapshot_v2.rs`](../crates/astralog-controller/src/snapshot_v2.rs) | Versioned controller snapshot plus producer state. | `ProducerSessionRecordV1, ControllerSnapshotV2, from_state` |
| [`crates/astralog-controller/src/snapshot_v3.rs`](../crates/astralog-controller/src/snapshot_v3.rs) | Controller snapshot V3 evolution. | `ControllerSnapshotV3, from_state, into_controller_snapshot` |
| [`crates/astralog-controller/src/snapshot_v4.rs`](../crates/astralog-controller/src/snapshot_v4.rs) | Controller snapshot V4 evolution. | `ControllerSnapshotV4, from_state, into_controller_snapshot` |
| [`crates/astralog-controller/src/state.rs`](../crates/astralog-controller/src/state.rs) | Deterministic committed controller metadata state machine. | `ProducerIdRange, ApplyOutcome, ControllerState` |
| [`crates/astralog-controller/src/topic.rs`](../crates/astralog-controller/src/topic.rs) | Create/expand/delete topics and versioned feature choices. | `CreateTopicSpec, validate, validate_controller_topic_name_and_topology` |
| [`crates/astralog-controller/src/transport.rs`](../crates/astralog-controller/src/transport.rs) | CRC/bounded controller internal network frame codec. | `ControllerTransportLimits, validate, ControllerFrame` |
| [`crates/astralog-controller/src/types.rs`](../crates/astralog-controller/src/types.rs) | Controller node ID and assignment/movement generation newtypes. | `ControllerNodeId, new, AssignmentGeneration` |

## crates/astralog-groups (7 files)

| File | What it contains / why it exists | Representative public code landmarks |
|---|---|---|
| [`crates/astralog-groups/Cargo.toml`](../crates/astralog-groups/Cargo.toml) | Internal crate dependencies and package manifest. | Not applicable |
| [`crates/astralog-groups/src/assignor.rs`](../crates/astralog-groups/src/assignor.rs) | Deterministic balanced uniform-sticky assignment. | `AssignorError, UniformStickyAssignment, uniform_sticky_assign` |
| [`crates/astralog-groups/src/cooperative.rs`](../crates/astralog-groups/src/cooperative.rs) | Prepared/revoking/stable ownership and offset fences. | `CooperativeError, CooperativeApplyOutcome, CooperativeStateMachine` |
| [`crates/astralog-groups/src/lib.rs`](../crates/astralog-groups/src/lib.rs) | Durable group record state/version and internal group-log codec. | `InternalTopicKind, InternalTopicDescriptor, is_reserved_internal_topic_name` |
| [`crates/astralog-groups/src/offset_state.rs`](../crates/astralog-groups/src/offset_state.rs) | Durable offset commits, group/member/owner fencing. | `OffsetApplyOutcome, OffsetStateError, public_error` |
| [`crates/astralog-groups/src/qualification.rs`](../crates/astralog-groups/src/qualification.rs) | Seeded P6 state-machine tests, test-only. | Not applicable |
| [`crates/astralog-groups/src/session.rs`](../crates/astralog-groups/src/session.rs) | Volatile member heartbeat tracking and failover grace. | `SessionError, validate_session_timeout, heartbeat_interval_ms` |

## crates/astralog-protocol (44 files)

| File | What it contains / why it exists | Representative public code landmarks |
|---|---|---|
| [`crates/astralog-protocol/Cargo.toml`](../crates/astralog-protocol/Cargo.toml) | Internal crate dependencies and package manifest. | Not applicable |
| [`crates/astralog-protocol/src/api/api_versions.rs`](../crates/astralog-protocol/src/api/api_versions.rs) | Supported protocol version negotiation API. | `ApiVersionsRequestV0, ApiVersionRange, FeatureRange` |
| [`crates/astralog-protocol/src/api/cluster.rs`](../crates/astralog-protocol/src/api/cluster.rs) | Cluster description and reassignment requests/responses. | `DescribeClusterRequestV0, DescribeClusterResponseV0, ReassignPartitionV0` |
| [`crates/astralog-protocol/src/api/fetch.rs`](../crates/astralog-protocol/src/api/fetch.rs) | Committed Fetch request/response bodies and validation. | `IsolationLevel, FetchPartitionRequestV0, FetchRequestV0` |
| [`crates/astralog-protocol/src/api/groups.rs`](../crates/astralog-protocol/src/api/groups.rs) | Consumer group and offset APIs, authority IDs and bounds. | `GroupId, new, as_str` |
| [`crates/astralog-protocol/src/api/init_producer.rs`](../crates/astralog-protocol/src/api/init_producer.rs) | Controller-backed producer session/epoch requests. | `InitProducerRequestV0, InitProducerResponseV0` |
| [`crates/astralog-protocol/src/api/list_offsets.rs`](../crates/astralog-protocol/src/api/list_offsets.rs) | Offset/time-selector lookup request/response. | `ListOffsetsQueryV0, ListOffsetsRequestV0, ListOffsetsResultV0` |
| [`crates/astralog-protocol/src/api/metadata.rs`](../crates/astralog-protocol/src/api/metadata.rs) | Topic/partition leader and broker metadata responses. | `MetadataRequestV0, BrokerMetadataV0, MetadataPartitionV0` |
| [`crates/astralog-protocol/src/api/mod.rs`](../crates/astralog-protocol/src/api/mod.rs) | API-module public re-exports. | `BodyCodec, put_top_level_tags, finish_body` |
| [`crates/astralog-protocol/src/api/produce.rs`](../crates/astralog-protocol/src/api/produce.rs) | Produce V0/V1 bodies, ack semantics, producer sequence identity. | `AckMode, WireHeaderV0, WireRecordV0` |
| [`crates/astralog-protocol/src/api/topic.rs`](../crates/astralog-protocol/src/api/topic.rs) | Topic create/delete/describe/alter configuration schemas. | `TopicCompressionV1, TopicPerformanceConfigV1, validate` |
| [`crates/astralog-protocol/src/codec.rs`](../crates/astralog-protocol/src/codec.rs) | Bounded primitive little-endian reader/writer and canonical tagged fields. | `TaggedField, Writer, new` |
| [`crates/astralog-protocol/src/decoder.rs`](../crates/astralog-protocol/src/decoder.rs) | Incremental network frame assembler with bounds. | `FrameDecoder, new, buffered_len` |
| [`crates/astralog-protocol/src/error.rs`](../crates/astralog-protocol/src/error.rs) | Protocol parsing/validation failure types. | `ProtocolError` |
| [`crates/astralog-protocol/src/frame.rs`](../crates/astralog-protocol/src/frame.rs) | WireFrame framing, lengths, correlation ID, timeout/CRC validation. | `FrameKind, WireFrame, validate_header_semantics` |
| [`crates/astralog-protocol/src/hash.rs`](../crates/astralog-protocol/src/hash.rs) | Deterministic hash utility for wire identities. | `sha256` |
| [`crates/astralog-protocol/src/hello.rs`](../crates/astralog-protocol/src/hello.rs) | Version and connection handshake. | `ClientHello, v1, encode` |
| [`crates/astralog-protocol/src/lib.rs`](../crates/astralog-protocol/src/lib.rs) | Public crate exports and wire-version constants. | Not applicable |
| [`crates/astralog-protocol/src/partitioner.rs`](../crates/astralog-protocol/src/partitioner.rs) | Murmur3 keyed and sticky partition selection. | `murmur3_x86_32, partition_for_key, PartitionRng` |
| [`crates/astralog-protocol/src/registry.rs`](../crates/astralog-protocol/src/registry.rs) | Public API key, feature advertisement, retry/error taxonomy. | `ApiKey, RetryClass, PublicError` |
| [`crates/astralog-protocol/tests/golden/api_versions_request_v0.hex`](../crates/astralog-protocol/tests/golden/api_versions_request_v0.hex) | Frozen exact binary golden fixture for api versions request v0. | Not applicable |
| [`crates/astralog-protocol/tests/golden/api_versions_response_v0.hex`](../crates/astralog-protocol/tests/golden/api_versions_response_v0.hex) | Frozen exact binary golden fixture for api versions response v0. | Not applicable |
| [`crates/astralog-protocol/tests/golden/client_hello_v1.hex`](../crates/astralog-protocol/tests/golden/client_hello_v1.hex) | Frozen exact binary golden fixture for client hello v1. | Not applicable |
| [`crates/astralog-protocol/tests/golden/create_topic_request_v0.hex`](../crates/astralog-protocol/tests/golden/create_topic_request_v0.hex) | Frozen exact binary golden fixture for create topic request v0. | Not applicable |
| [`crates/astralog-protocol/tests/golden/create_topic_response_v0.hex`](../crates/astralog-protocol/tests/golden/create_topic_response_v0.hex) | Frozen exact binary golden fixture for create topic response v0. | Not applicable |
| [`crates/astralog-protocol/tests/golden/delete_topic_request_v0.hex`](../crates/astralog-protocol/tests/golden/delete_topic_request_v0.hex) | Frozen exact binary golden fixture for delete topic request v0. | Not applicable |
| [`crates/astralog-protocol/tests/golden/delete_topic_response_v0.hex`](../crates/astralog-protocol/tests/golden/delete_topic_response_v0.hex) | Frozen exact binary golden fixture for delete topic response v0. | Not applicable |
| [`crates/astralog-protocol/tests/golden/describe_topic_request_v0.hex`](../crates/astralog-protocol/tests/golden/describe_topic_request_v0.hex) | Frozen exact binary golden fixture for describe topic request v0. | Not applicable |
| [`crates/astralog-protocol/tests/golden/describe_topic_response_v0.hex`](../crates/astralog-protocol/tests/golden/describe_topic_response_v0.hex) | Frozen exact binary golden fixture for describe topic response v0. | Not applicable |
| [`crates/astralog-protocol/tests/golden/error_response_v0.hex`](../crates/astralog-protocol/tests/golden/error_response_v0.hex) | Frozen exact binary golden fixture for error response v0. | Not applicable |
| [`crates/astralog-protocol/tests/golden/fetch_request_v0.hex`](../crates/astralog-protocol/tests/golden/fetch_request_v0.hex) | Frozen exact binary golden fixture for fetch request v0. | Not applicable |
| [`crates/astralog-protocol/tests/golden/fetch_response_v0.hex`](../crates/astralog-protocol/tests/golden/fetch_response_v0.hex) | Frozen exact binary golden fixture for fetch response v0. | Not applicable |
| [`crates/astralog-protocol/tests/golden/list_offsets_request_v0.hex`](../crates/astralog-protocol/tests/golden/list_offsets_request_v0.hex) | Frozen exact binary golden fixture for list offsets request v0. | Not applicable |
| [`crates/astralog-protocol/tests/golden/list_offsets_response_v0.hex`](../crates/astralog-protocol/tests/golden/list_offsets_response_v0.hex) | Frozen exact binary golden fixture for list offsets response v0. | Not applicable |
| [`crates/astralog-protocol/tests/golden/metadata_request_v0.hex`](../crates/astralog-protocol/tests/golden/metadata_request_v0.hex) | Frozen exact binary golden fixture for metadata request v0. | Not applicable |
| [`crates/astralog-protocol/tests/golden/metadata_response_v0.hex`](../crates/astralog-protocol/tests/golden/metadata_response_v0.hex) | Frozen exact binary golden fixture for metadata response v0. | Not applicable |
| [`crates/astralog-protocol/tests/golden/partition_vectors_v1.txt`](../crates/astralog-protocol/tests/golden/partition_vectors_v1.txt) | Frozen exact binary golden fixture for partition vectors v1. | Not applicable |
| [`crates/astralog-protocol/tests/golden/produce_request_v0.hex`](../crates/astralog-protocol/tests/golden/produce_request_v0.hex) | Frozen exact binary golden fixture for produce request v0. | Not applicable |
| [`crates/astralog-protocol/tests/golden/produce_response_v0.hex`](../crates/astralog-protocol/tests/golden/produce_response_v0.hex) | Frozen exact binary golden fixture for produce response v0. | Not applicable |
| [`crates/astralog-protocol/tests/golden/server_hello_v1.hex`](../crates/astralog-protocol/tests/golden/server_hello_v1.hex) | Frozen exact binary golden fixture for server hello v1. | Not applicable |
| [`crates/astralog-protocol/tests/golden_vectors.rs`](../crates/astralog-protocol/tests/golden_vectors.rs) | Frozen protocol V1 golden byte vectors. | Not applicable |
| [`crates/astralog-protocol/tests/p6_golden_vectors.rs`](../crates/astralog-protocol/tests/p6_golden_vectors.rs) | Frozen P6 API byte compatibility. | Not applicable |
| [`crates/astralog-protocol/tests/p7_golden_vectors.rs`](../crates/astralog-protocol/tests/p7_golden_vectors.rs) | Frozen P7 producer byte compatibility. | Not applicable |
| [`crates/astralog-protocol/tests/p9_golden_vectors.rs`](../crates/astralog-protocol/tests/p9_golden_vectors.rs) | Frozen P9 protocol byte compatibility. | Not applicable |

## crates/astralog-raft (12 files)

| File | What it contains / why it exists | Representative public code landmarks |
|---|---|---|
| [`crates/astralog-raft/Cargo.toml`](../crates/astralog-raft/Cargo.toml) | Internal crate dependencies and package manifest. | Not applicable |
| [`crates/astralog-raft/src/configuration.rs`](../crates/astralog-raft/src/configuration.rs) | Stable/joint membership and quorum eligibility. | `StableConfiguration, new, voters` |
| [`crates/astralog-raft/src/engine.rs`](../crates/astralog-raft/src/engine.rs) | Deterministic Raft Input-to-Effect transition engine. | `Input, Effect, RaftCore` |
| [`crates/astralog-raft/src/entry.rs`](../crates/astralog-raft/src/entry.rs) | Term/index-tagged Raft entry types. | `EntryKind, LogEntry, new` |
| [`crates/astralog-raft/src/error.rs`](../crates/astralog-raft/src/error.rs) | Consensus error/fail-closed states. | `RaftError, NodeIdValue` |
| [`crates/astralog-raft/src/formal_model.rs`](../crates/astralog-raft/src/formal_model.rs) | Bounded mechanically checked safety model, test-only. | Not applicable |
| [`crates/astralog-raft/src/lib.rs`](../crates/astralog-raft/src/lib.rs) | Pure consensus API exports. | Not applicable |
| [`crates/astralog-raft/src/message.rs`](../crates/astralog-raft/src/message.rs) | PreVote, RequestVote, AppendEntries, snapshot and transfer messages. | `PreVoteRequest, PreVoteResponse, RequestVoteRequest` |
| [`crates/astralog-raft/src/persistence.rs`](../crates/astralog-raft/src/persistence.rs) | PersistenceOp/Batch and durable completion tokens. | `PersistenceOp, PersistenceBatch, new` |
| [`crates/astralog-raft/src/simulation.rs`](../crates/astralog-raft/src/simulation.rs) | Deterministic fault-schedule/election simulation, test-only. | Not applicable |
| [`crates/astralog-raft/src/state.rs`](../crates/astralog-raft/src/state.rs) | Hard/volatile state, election timing, follower progress and limits. | `TimingConfig, new, PreVoteDecision` |
| [`crates/astralog-raft/src/types.rs`](../crates/astralog-raft/src/types.rs) | Checked Raft term, node/index/transaction identities. | `NodeId, Term, checked_next` |

## crates/astralog-replication (17 files)

| File | What it contains / why it exists | Representative public code landmarks |
|---|---|---|
| [`crates/astralog-replication/Cargo.toml`](../crates/astralog-replication/Cargo.toml) | Internal crate dependencies and package manifest. | Not applicable |
| [`crates/astralog-replication/src/codec.rs`](../crates/astralog-replication/src/codec.rs) | Binary encoding/decoding of Raft messages. | `encode_configuration_change, decode_configuration_change, encode_snapshot_metadata` |
| [`crates/astralog-replication/src/command.rs`](../crates/astralog-replication/src/command.rs) | Versioned application, producer and P9 maintenance commands. | `CommandLimits, ApplicationCommand, ApplicationCommandV2` |
| [`crates/astralog-replication/src/compaction_progress.rs`](../crates/astralog-replication/src/compaction_progress.rs) | P9-G eligible-voter clean proofs and minimum safe bound. | `CleanProgressReport, encode, decode` |
| [`crates/astralog-replication/src/error.rs`](../crates/astralog-replication/src/error.rs) | Replica and transport failure classification. | `ReplicationError` |
| [`crates/astralog-replication/src/lib.rs`](../crates/astralog-replication/src/lib.rs) | Raft/data persistence composition public API. | Not applicable |
| [`crates/astralog-replication/src/peer_transport.rs`](../crates/astralog-replication/src/peer_transport.rs) | Bounded broker-to-broker TCP Raft message routing. | `GroupKey, PeerInboundFrame, PeerDescriptor` |
| [`crates/astralog-replication/src/producer_digest.rs`](../crates/astralog-replication/src/producer_digest.rs) | Canonical producer batch identity hashing. | `canonical_storage_record_set_digest` |
| [`crates/astralog-replication/src/producer_snapshot.rs`](../crates/astralog-replication/src/producer_snapshot.rs) | Durable P7 producer-state checkpoints/reconstruction. | `ProducerSnapshotV1, new, ProducerSnapshotStore` |
| [`crates/astralog-replication/src/producer_state.rs`](../crates/astralog-replication/src/producer_state.rs) | Per-partition epoch/sequence/recent-batch deduplication state. | `ProducerStateError, ProducerBatchIdentityV1, new` |
| [`crates/astralog-replication/src/rebootstrap.rs`](../crates/astralog-replication/src/rebootstrap.rs) | V1 snapshot/file transfer package. | `RebootstrapLimits, RebootstrapPackage, validate` |
| [`crates/astralog-replication/src/rebootstrap_v2.rs`](../crates/astralog-replication/src/rebootstrap_v2.rs) | Canonical bounded V2 retained files/manifest SHA+CRC package. | `RebootstrapLimitsV2, RebootstrapFileV2, RebootstrapPackageV2` |
| [`crates/astralog-replication/src/runtime.rs`](../crates/astralog-replication/src/runtime.rs) | ReplicatedPartition, Raft effects, proposals, acknowledgements, liveness. | `AckRequirement, ProposalOutcome, ProposalReceipt` |
| [`crates/astralog-replication/src/sidecar.rs`](../crates/astralog-replication/src/sidecar.rs) | Raft durable hard-state and snapshot sidecar stores. | `HardStateStore, new, load` |
| [`crates/astralog-replication/src/simulation.rs`](../crates/astralog-replication/src/simulation.rs) | Deterministic replication failure/recovery campaign, test-only. | Not applicable |
| [`crates/astralog-replication/src/storage_adapter.rs`](../crates/astralog-replication/src/storage_adapter.rs) | PartitionPersistence: Raft durable effects, disk and rebootstrap. | `PartitionPersistence, open, execute` |
| [`crates/astralog-replication/src/transport.rs`](../crates/astralog-replication/src/transport.rs) | Internal replication CRC-framed codec and bounds. | `TransportLimits, InternalFrame, encode_internal_frame` |

## crates/astralog-storage (34 files)

| File | What it contains / why it exists | Representative public code landmarks |
|---|---|---|
| [`crates/astralog-storage/Cargo.toml`](../crates/astralog-storage/Cargo.toml) | Internal crate dependencies and package manifest. | Not applicable |
| [`crates/astralog-storage/src/clean_progress.rs`](../crates/astralog-storage/src/clean_progress.rs) | Manifest-generation-bound local P9-G clean-progress sidecar. | `LocalCleanProgress, LocalCleanProgressStore, new` |
| [`crates/astralog-storage/src/compression.rs`](../crates/astralog-storage/src/compression.rs) | None/LZ4/Zstd verified compression and bounded decode. | `compress_data_payload, decompress_data_payload` |
| [`crates/astralog-storage/src/error.rs`](../crates/astralog-storage/src/error.rs) | Disk format, recovery and I/O error types. | `StorageError` |
| [`crates/astralog-storage/src/format.rs`](../crates/astralog-storage/src/format.rs) | Frozen V1 record/frame, segment header and partition metadata codecs. | `FrameType, Compression, ProducerIdentity` |
| [`crates/astralog-storage/src/format_v2.rs`](../crates/astralog-storage/src/format_v2.rs) | Sparse retained-history V2 frame/segment codecs. | `StorageFormatVersion, RetainedSegmentHeaderV2, identity_header` |
| [`crates/astralog-storage/src/index.rs`](../crates/astralog-storage/src/index.rs) | Derived sparse offset/time/Raft indexes and rebuilding. | `IndexKind, IndexEntry, rebuild_segment_indexes` |
| [`crates/astralog-storage/src/io_util.rs`](../crates/astralog-storage/src/io_util.rs) | Sync, atomic file staging and rename (Unix directory fsync; Windows caveat). | `sync_directory, atomic_write, create_new_durable` |
| [`crates/astralog-storage/src/lib.rs`](../crates/astralog-storage/src/lib.rs) | Public disk/storage interfaces and module exports. | Not applicable |
| [`crates/astralog-storage/src/limits.rs`](../crates/astralog-storage/src/limits.rs) | Disk/record/segment bounds. | `StorageLimits` |
| [`crates/astralog-storage/src/maintenance.rs`](../crates/astralog-storage/src/maintenance.rs) | P9 committed log-start, compaction-safe and checkpoint control bodies. | `encode_log_start_offset_body, decode_log_start_offset_body, encode_compaction_safe_offset_body` |
| [`crates/astralog-storage/src/manifest.rs`](../crates/astralog-storage/src/manifest.rs) | CRC-checked generation and immutable/mutable segment inventory. | `ManifestEntry, immutable, mutable_active` |
| [`crates/astralog-storage/src/partition.rs`](../crates/astralog-storage/src/partition.rs) | PartitionLog append, roll, mark commit, recovery and committed reads. | `PartitionLogConfig, StoredRecord, DeleteRetentionSpec` |
| [`crates/astralog-storage/src/partition/compaction.rs`](../crates/astralog-storage/src/partition/compaction.rs) | Copy-on-write keyed compaction and tombstone removal planning. | `CompactionLimits, CompactionPlanSpec, TombstoneRemovalGate` |
| [`crates/astralog-storage/src/segment.rs`](../crates/astralog-storage/src/segment.rs) | Segment scan, append and truncated/corrupted tail detection. | `SegmentFileId, from_header, stem` |
| [`crates/astralog-storage/src/test_support.rs`](../crates/astralog-storage/src/test_support.rs) | Storage fault/fixture helpers, test-only. | `test_directory, decode_hex` |
| [`crates/astralog-storage/tests/golden/control_v1.hex`](../crates/astralog-storage/tests/golden/control_v1.hex) | Frozen exact binary golden fixture for control v1. | Not applicable |
| [`crates/astralog-storage/tests/golden/data_idempotent_multi_v1.hex`](../crates/astralog-storage/tests/golden/data_idempotent_multi_v1.hex) | Frozen exact binary golden fixture for data idempotent multi v1. | Not applicable |
| [`crates/astralog-storage/tests/golden/data_p8_lz4_v1.hex`](../crates/astralog-storage/tests/golden/data_p8_lz4_v1.hex) | Frozen exact binary golden fixture for data p8 lz4 v1. | Not applicable |
| [`crates/astralog-storage/tests/golden/data_p8_zstd_v1.hex`](../crates/astralog-storage/tests/golden/data_p8_zstd_v1.hex) | Frozen exact binary golden fixture for data p8 zstd v1. | Not applicable |
| [`crates/astralog-storage/tests/golden/data_single_v1.hex`](../crates/astralog-storage/tests/golden/data_single_v1.hex) | Frozen exact binary golden fixture for data single v1. | Not applicable |
| [`crates/astralog-storage/tests/golden/manifest_p9_mixed_v1.hex`](../crates/astralog-storage/tests/golden/manifest_p9_mixed_v1.hex) | Frozen exact binary golden fixture for manifest p9 mixed v1. | Not applicable |
| [`crates/astralog-storage/tests/golden/manifest_v1.hex`](../crates/astralog-storage/tests/golden/manifest_v1.hex) | Frozen exact binary golden fixture for manifest v1. | Not applicable |
| [`crates/astralog-storage/tests/golden/offset_index_v1.hex`](../crates/astralog-storage/tests/golden/offset_index_v1.hex) | Frozen exact binary golden fixture for offset index v1. | Not applicable |
| [`crates/astralog-storage/tests/golden/partition_meta_committed_v1.hex`](../crates/astralog-storage/tests/golden/partition_meta_committed_v1.hex) | Frozen exact binary golden fixture for partition meta committed v1. | Not applicable |
| [`crates/astralog-storage/tests/golden/partition_meta_empty_v1.hex`](../crates/astralog-storage/tests/golden/partition_meta_empty_v1.hex) | Frozen exact binary golden fixture for partition meta empty v1. | Not applicable |
| [`crates/astralog-storage/tests/golden/raft_index_v1.hex`](../crates/astralog-storage/tests/golden/raft_index_v1.hex) | Frozen exact binary golden fixture for raft index v1. | Not applicable |
| [`crates/astralog-storage/tests/golden/retained_control_v2.hex`](../crates/astralog-storage/tests/golden/retained_control_v2.hex) | Frozen exact binary golden fixture for retained control v2. | Not applicable |
| [`crates/astralog-storage/tests/golden/segment_header_v1.hex`](../crates/astralog-storage/tests/golden/segment_header_v1.hex) | Frozen exact binary golden fixture for segment header v1. | Not applicable |
| [`crates/astralog-storage/tests/golden/segment_header_v2.hex`](../crates/astralog-storage/tests/golden/segment_header_v2.hex) | Frozen exact binary golden fixture for segment header v2. | Not applicable |
| [`crates/astralog-storage/tests/golden/sparse_lz4_v2.hex`](../crates/astralog-storage/tests/golden/sparse_lz4_v2.hex) | Frozen exact binary golden fixture for sparse lz4 v2. | Not applicable |
| [`crates/astralog-storage/tests/golden/sparse_none_v2.hex`](../crates/astralog-storage/tests/golden/sparse_none_v2.hex) | Frozen exact binary golden fixture for sparse none v2. | Not applicable |
| [`crates/astralog-storage/tests/golden/sparse_zstd_v2.hex`](../crates/astralog-storage/tests/golden/sparse_zstd_v2.hex) | Frozen exact binary golden fixture for sparse zstd v2. | Not applicable |
| [`crates/astralog-storage/tests/golden/time_index_v1.hex`](../crates/astralog-storage/tests/golden/time_index_v1.hex) | Frozen exact binary golden fixture for time index v1. | Not applicable |

## docs (architecture, phases, audit reports) (108 files)

| File | What it contains / why it exists | Representative public code landmarks |
|---|---|---|
| [`docs/ADR_017_PRODUCER_EPOCH_AUTHORITY.md`](./ADR_017_PRODUCER_EPOCH_AUTHORITY.md) | Design/phase/test evidence document: ADR-017 — Producer Epoch Authority Is Controller-Issued and Partition-Enforced. Verify phase date/status before treating as current. | Not applicable |
| [`docs/ADR_019_P9_SPARSE_RETAINED_HISTORY.md`](./ADR_019_P9_SPARSE_RETAINED_HISTORY.md) | Design/phase/test evidence document: ADR-019 — Sparse Retained-History Storage V2 for P9 Compaction. Verify phase date/status before treating as current. | Not applicable |
| [`docs/ADR_P8_E_PERSISTENCE_FRONTIER.md`](./ADR_P8_E_PERSISTENCE_FRONTIER.md) | Design/phase/test evidence document: Proposed ADR — P8-E Persistence Frontier for True Per-Partition Group Commit. Verify phase date/status before treating as current. | Not applicable |
| [`docs/ARCHITECTURE_REVIEW.md`](./ARCHITECTURE_REVIEW.md) | Design/phase/test evidence document: AstraLog Architecture Review — P0. Verify phase date/status before treating as current. | Not applicable |
| [`docs/ASTRALOG_BIBLE.md`](./ASTRALOG_BIBLE.md) | Design/phase/test evidence document: AstraLog Project Bible. Verify phase date/status before treating as current. | Not applicable |
| [`docs/ASTRALOG_GOAL_EXECUTION_GUARDS.md`](./ASTRALOG_GOAL_EXECUTION_GUARDS.md) | Design/phase/test evidence document: AstraLog — Goal-driven engineering execution guards. Verify phase date/status before treating as current. | Not applicable |
| [`docs/ASTRALOG_MASTER_GUIDE.md`](./ASTRALOG_MASTER_GUIDE.md) | This consolidated beginner-to-professional guide and complete tracked-file atlas. | Not applicable |
| [`docs/AUDIT_CLOSURE_MATRIX.md`](./AUDIT_CLOSURE_MATRIX.md) | Design/phase/test evidence document: AstraLog adversarial audit closure matrix (2026-11-03). Verify phase date/status before treating as current. | Not applicable |
| [`docs/CURRENT_STATE.md`](./CURRENT_STATE.md) | Design/phase/test evidence document: AstraLog Current State. Verify phase date/status before treating as current. | Not applicable |
| [`docs/DECISIONS.md`](./DECISIONS.md) | Design/phase/test evidence document: AstraLog Architecture Decision Log. Verify phase date/status before treating as current. | Not applicable |
| [`docs/ENGINEERING_RULES.md`](./ENGINEERING_RULES.md) | Design/phase/test evidence document: AstraLog Engineering Rules. Verify phase date/status before treating as current. | Not applicable |
| [`docs/FEATURE_MATRIX.md`](./FEATURE_MATRIX.md) | Design/phase/test evidence document: AstraLog Feature-to-Phase Matrix. Verify phase date/status before treating as current. | Not applicable |
| [`docs/FORMAT_V1.md`](./FORMAT_V1.md) | Design/phase/test evidence document: AstraLog Storage Format V1. Verify phase date/status before treating as current. | Not applicable |
| [`docs/FORMAT_V2.md`](./FORMAT_V2.md) | Design/phase/test evidence document: AstraLog Retained-History Storage Format V2. Verify phase date/status before treating as current. | Not applicable |
| [`docs/INTERVIEW_READY.md`](./INTERVIEW_READY.md) | Design/phase/test evidence document: AstraLog: runnable SDE interview demo. Verify phase date/status before treating as current. | Not applicable |
| [`docs/INVARIANTS.md`](./INVARIANTS.md) | Design/phase/test evidence document: AstraLog Correctness Invariants. Verify phase date/status before treating as current. | Not applicable |
| [`docs/P0_CLOSURE_REPORT.md`](./P0_CLOSURE_REPORT.md) | Design/phase/test evidence document: P0 Closure Report. Verify phase date/status before treating as current. | Not applicable |
| [`docs/P0_P12_PRODUCTION_TARGET.md`](./P0_P12_PRODUCTION_TARGET.md) | Design/phase/test evidence document: AstraLog P0-P12 Production Target — Execution Charter. Verify phase date/status before treating as current. | Not applicable |
| [`docs/P1_CHECKLIST.md`](./P1_CHECKLIST.md) | Design/phase/test evidence document: P1 Execution Checklist — Single-Node Durable Log Engine. Verify phase date/status before treating as current. | Not applicable |
| [`docs/P1_CLOSURE_REPORT.md`](./P1_CLOSURE_REPORT.md) | Design/phase/test evidence document: P1 Closure Report. Verify phase date/status before treating as current. | Not applicable |
| [`docs/P2_CHECKLIST.md`](./P2_CHECKLIST.md) | Design/phase/test evidence document: P2 — Binary Protocol, Single Broker, Client, Partitioning Checklist. Verify phase date/status before treating as current. | Not applicable |
| [`docs/P2_CLOSURE_REPORT.md`](./P2_CLOSURE_REPORT.md) | Design/phase/test evidence document: P2 Closure Report. Verify phase date/status before treating as current. | Not applicable |
| [`docs/P3_APPEND_HEARTBEAT_REPORT.md`](./P3_APPEND_HEARTBEAT_REPORT.md) | Design/phase/test evidence document: P3 AppendEntries Heartbeat / Prefix Qualification Report. Verify phase date/status before treating as current. | Not applicable |
| [`docs/P3_CHECKLIST.md`](./P3_CHECKLIST.md) | Design/phase/test evidence document: P3 — Raft Core Checklist. Verify phase date/status before treating as current. | Not applicable |
| [`docs/P3_CLOSURE_REPORT.md`](./P3_CLOSURE_REPORT.md) | Design/phase/test evidence document: P3 — Raft Core Closure Report. Verify phase date/status before treating as current. | Not applicable |
| [`docs/P3_FOUNDATION_REPORT.md`](./P3_FOUNDATION_REPORT.md) | Design/phase/test evidence document: P3 Raft Foundation Qualification Report. Verify phase date/status before treating as current. | Not applicable |
| [`docs/P3_LEADER_INIT_REPORT.md`](./P3_LEADER_INIT_REPORT.md) | Design/phase/test evidence document: P3 Leader Initialization Qualification Report. Verify phase date/status before treating as current. | Not applicable |
| [`docs/P3_PREVOTE_REPORT.md`](./P3_PREVOTE_REPORT.md) | Design/phase/test evidence document: P3 Pre-Vote Qualification Report. Verify phase date/status before treating as current. | Not applicable |
| [`docs/P3_REQUESTVOTE_REPORT.md`](./P3_REQUESTVOTE_REPORT.md) | Design/phase/test evidence document: P3 RequestVote Qualification Report. Verify phase date/status before treating as current. | Not applicable |
| [`docs/P4_CHECKLIST.md`](./P4_CHECKLIST.md) | Design/phase/test evidence document: P4 — Replicated Data Partitions and Quorum Acks Checklist. Verify phase date/status before treating as current. | Not applicable |
| [`docs/P4_CLOSURE_REPORT.md`](./P4_CLOSURE_REPORT.md) | Design/phase/test evidence document: P4 — Replicated Data Partitions and Quorum Acks Closure Report. Verify phase date/status before treating as current. | Not applicable |
| [`docs/P4_REPLICATION_V1.md`](./P4_REPLICATION_V1.md) | Design/phase/test evidence document: AstraLog P4 Replicated Partition Contract V1. Verify phase date/status before treating as current. | Not applicable |
| [`docs/P5_BOOTSTRAP_REPORT.md`](./P5_BOOTSTRAP_REPORT.md) | Design/phase/test evidence document: AstraLog P5-E Controller Bootstrap Qualification. Verify phase date/status before treating as current. | Not applicable |
| [`docs/P5_BOUNDARY_REPAIR_REPORT.md`](./P5_BOUNDARY_REPAIR_REPORT.md) | Design/phase/test evidence document: AstraLog Post-P5-O Raft / Movement Boundary Repair. Verify phase date/status before treating as current. | Not applicable |
| [`docs/P5_CHECKLIST.md`](./P5_CHECKLIST.md) | Design/phase/test evidence document: AstraLog P5 Execution Checklist. Verify phase date/status before treating as current. | Not applicable |
| [`docs/P5_CLOSURE_REPORT.md`](./P5_CLOSURE_REPORT.md) | Design/phase/test evidence document: AstraLog P5 Closure Report. Verify phase date/status before treating as current. | Not applicable |
| [`docs/P5_CONTRACT_FREEZE_REPORT.md`](./P5_CONTRACT_FREEZE_REPORT.md) | Design/phase/test evidence document: AstraLog P5-A Contract Freeze Report. Verify phase date/status before treating as current. | Not applicable |
| [`docs/P5_CONTROLLER_V1.md`](./P5_CONTROLLER_V1.md) | Design/phase/test evidence document: AstraLog P5 Controller and Placement Contract V1. Verify phase date/status before treating as current. | Not applicable |
| [`docs/P5_DYNAMIC_INTEGRATION_REPORT.md`](./P5_DYNAMIC_INTEGRATION_REPORT.md) | Design/phase/test evidence document: AstraLog P5-J Dynamic P4 Integration Qualification. Verify phase date/status before treating as current. | Not applicable |
| [`docs/P5_FAILURE_REPORT.md`](./P5_FAILURE_REPORT.md) | Design/phase/test evidence document: AstraLog P5-P Failure Qualification. Verify phase date/status before treating as current. | Not applicable |
| [`docs/P5_FEATURE_LEVEL_REPORT.md`](./P5_FEATURE_LEVEL_REPORT.md) | Design/phase/test evidence document: AstraLog P5-N Active Cluster Feature Level Qualification. Verify phase date/status before treating as current. | Not applicable |
| [`docs/P5_HEARTBEAT_REPORT.md`](./P5_HEARTBEAT_REPORT.md) | Design/phase/test evidence document: AstraLog P5-G Heartbeat / Failure Detector Qualification. Verify phase date/status before treating as current. | Not applicable |
| [`docs/P5_LEADERSHIP_REPORT.md`](./P5_LEADERSHIP_REPORT.md) | Design/phase/test evidence document: AstraLog P5-M Leadership Hook Qualification. Verify phase date/status before treating as current. | Not applicable |
| [`docs/P5_MEMBERSHIP_REPORT.md`](./P5_MEMBERSHIP_REPORT.md) | Design/phase/test evidence document: AstraLog P5-F Broker Membership Qualification. Verify phase date/status before treating as current. | Not applicable |
| [`docs/P5_METADATA_REPORT.md`](./P5_METADATA_REPORT.md) | Design/phase/test evidence document: AstraLog P5-I Metadata Epochs / Deltas + Broker Cache Qualification. Verify phase date/status before treating as current. | Not applicable |
| [`docs/P5_MOVEMENT_REPORT.md`](./P5_MOVEMENT_REPORT.md) | Design/phase/test evidence document: AstraLog P5-K/L Crash-Resumable Replica Movement Qualification. Verify phase date/status before treating as current. | Not applicable |
| [`docs/P5_PERSISTENCE_REPORT.md`](./P5_PERSISTENCE_REPORT.md) | Design/phase/test evidence document: AstraLog P5-C Controller Persistence Qualification. Verify phase date/status before treating as current. | Not applicable |
| [`docs/P5_PLACEMENT_REPORT.md`](./P5_PLACEMENT_REPORT.md) | Design/phase/test evidence document: AstraLog P5-H Topic Metadata / Deterministic Placement Qualification. Verify phase date/status before treating as current. | Not applicable |
| [`docs/P5_PRODUCER_ID_REPORT.md`](./P5_PRODUCER_ID_REPORT.md) | Design/phase/test evidence document: AstraLog P5-O Producer-ID Allocator Qualification. Verify phase date/status before treating as current. | Not applicable |
| [`docs/P5_RUNTIME_REPORT.md`](./P5_RUNTIME_REPORT.md) | Design/phase/test evidence document: AstraLog P5-D Controller Runtime Qualification. Verify phase date/status before treating as current. | Not applicable |
| [`docs/P5_STATE_MACHINE_REPORT.md`](./P5_STATE_MACHINE_REPORT.md) | Design/phase/test evidence document: AstraLog P5-B Deterministic Controller State Machine Qualification. Verify phase date/status before treating as current. | Not applicable |
| [`docs/P6_ASSIGNOR_REPORT.md`](./P6_ASSIGNOR_REPORT.md) | Design/phase/test evidence document: AstraLog P6-E Uniform-Sticky Assignor Qualification. Verify phase date/status before treating as current. | Not applicable |
| [`docs/P6_CHECKLIST.md`](./P6_CHECKLIST.md) | Design/phase/test evidence document: AstraLog P6 Execution Checklist. Verify phase date/status before treating as current. | Not applicable |
| [`docs/P6_CLOSURE_REPORT.md`](./P6_CLOSURE_REPORT.md) | Design/phase/test evidence document: P6 — Consumer Offsets and Cooperative Consumer Groups Closure Report. Verify phase date/status before treating as current. | Not applicable |
| [`docs/P6_CODECS_REPORT.md`](./P6_CODECS_REPORT.md) | Design/phase/test evidence document: AstraLog P6-B Types, Codecs and Golden Bytes Qualification. Verify phase date/status before treating as current. | Not applicable |
| [`docs/P6_CONTRACT_FREEZE_REPORT.md`](./P6_CONTRACT_FREEZE_REPORT.md) | Design/phase/test evidence document: AstraLog P6-A Contract Freeze Report. Verify phase date/status before treating as current. | Not applicable |
| [`docs/P6_COOPERATIVE_REPORT.md`](./P6_COOPERATIVE_REPORT.md) | Design/phase/test evidence document: P6-F Cooperative Revocation / Activation Qualification. Verify phase date/status before treating as current. | Not applicable |
| [`docs/P6_COORDINATOR_REPORT.md`](./P6_COORDINATOR_REPORT.md) | Design/phase/test evidence document: P6-H Replicated Group Coordinator / Recovery Qualification. Verify phase date/status before treating as current. | Not applicable |
| [`docs/P6_FAILURE_CHURN_REPORT.md`](./P6_FAILURE_CHURN_REPORT.md) | Design/phase/test evidence document: P6-K Failure / Churn / Model Qualification. Verify phase date/status before treating as current. | Not applicable |
| [`docs/P6_GROUPS_V1.md`](./P6_GROUPS_V1.md) | Design/phase/test evidence document: AstraLog P6 Consumer Groups and Offsets Contract V1. Verify phase date/status before treating as current. | Not applicable |
| [`docs/P6_INTEGRATION_REPORT.md`](./P6_INTEGRATION_REPORT.md) | Design/phase/test evidence document: P6-J Broker / Client Integration Qualification. Verify phase date/status before treating as current. | Not applicable |
| [`docs/P6_OFFSETS_REPORT.md`](./P6_OFFSETS_REPORT.md) | Design/phase/test evidence document: P6-I Durable Offsets / Fencing Qualification. Verify phase date/status before treating as current. | Not applicable |
| [`docs/P6_ROUTING_REPORT.md`](./P6_ROUTING_REPORT.md) | Design/phase/test evidence document: AstraLog P6-C Internal Topics and Coordinator Routing Qualification. Verify phase date/status before treating as current. | Not applicable |
| [`docs/P6_SESSION_REPORT.md`](./P6_SESSION_REPORT.md) | Design/phase/test evidence document: P6-G Heartbeats / Session Timeout / Static Membership Qualification. Verify phase date/status before treating as current. | Not applicable |
| [`docs/P6_STATE_MACHINE_REPORT.md`](./P6_STATE_MACHINE_REPORT.md) | Design/phase/test evidence document: P6-D Pure Durable Group State Machine Qualification. Verify phase date/status before treating as current. | Not applicable |
| [`docs/P7_CHECKLIST.md`](./P7_CHECKLIST.md) | Design/phase/test evidence document: AstraLog P7 Execution Checklist. Verify phase date/status before treating as current. | Not applicable |
| [`docs/P7_CLOSURE_REPORT.md`](./P7_CLOSURE_REPORT.md) | Design/phase/test evidence document: AstraLog P7 — Idempotent Producer and Duplicate Detection Closure Report. Verify phase date/status before treating as current. | Not applicable |
| [`docs/P7_CODECS_REPORT.md`](./P7_CODECS_REPORT.md) | Design/phase/test evidence document: AstraLog P7-B Types / Codecs / Golden Bytes Qualification Report. Verify phase date/status before treating as current. | Not applicable |
| [`docs/P7_CONTRACT_FREEZE_REPORT.md`](./P7_CONTRACT_FREEZE_REPORT.md) | Design/phase/test evidence document: AstraLog P7-A Contract / Checklist Freeze Report. Verify phase date/status before treating as current. | Not applicable |
| [`docs/P7_CONTROLLER_REPORT.md`](./P7_CONTROLLER_REPORT.md) | Design/phase/test evidence document: AstraLog P7-C Controller Producer-Session Authority / V2 Persistence Qualification. Verify phase date/status before treating as current. | Not applicable |
| [`docs/P7_FAILOVER_REPORT.md`](./P7_FAILOVER_REPORT.md) | Design/phase/test evidence document: AstraLog P7-I Lost-Response / Fencing / Real RF3 Failover Qualification. Verify phase date/status before treating as current. | Not applicable |
| [`docs/P7_INTEGRATION_REPORT.md`](./P7_INTEGRATION_REPORT.md) | Design/phase/test evidence document: AstraLog P7-H Broker / Client Integration Qualification. Verify phase date/status before treating as current. | Not applicable |
| [`docs/P7_J_BOUNDARY_REPAIR_REPORT.md`](./P7_J_BOUNDARY_REPAIR_REPORT.md) | Design/phase/test evidence document: AstraLog P7-J Exact-HEAD Boundary Repair Qualification. Verify phase date/status before treating as current. | Not applicable |
| [`docs/P7_MODEL_STRESS_REPORT.md`](./P7_MODEL_STRESS_REPORT.md) | Design/phase/test evidence document: AstraLog P7-J Deterministic Models / Resource Bounds / Scheduling Stress Qualification. Verify phase date/status before treating as current. | Not applicable |
| [`docs/P7_PRODUCER_V1.md`](./P7_PRODUCER_V1.md) | Design/phase/test evidence document: AstraLog P7 Idempotent Producer Contract V1. Verify phase date/status before treating as current. | Not applicable |
| [`docs/P7_RUNTIME_REPORT.md`](./P7_RUNTIME_REPORT.md) | Design/phase/test evidence document: P7-F Runtime Admission / Pending Duplicate Suppression Qualification. Verify phase date/status before treating as current. | Not applicable |
| [`docs/P7_SNAPSHOT_REPORT.md`](./P7_SNAPSHOT_REPORT.md) | Design/phase/test evidence document: AstraLog P7-G Producer Snapshot / Crash Recovery Qualification. Verify phase date/status before treating as current. | Not applicable |
| [`docs/P7_STATE_MACHINE_REPORT.md`](./P7_STATE_MACHINE_REPORT.md) | Design/phase/test evidence document: P7-D Pure Partition Producer State Machine Qualification. Verify phase date/status before treating as current. | Not applicable |
| [`docs/P7_STORAGE_REPORT.md`](./P7_STORAGE_REPORT.md) | Design/phase/test evidence document: P7-E Storage V1 Producer Metadata / ApplicationCommand V2 Qualification. Verify phase date/status before treating as current. | Not applicable |
| [`docs/P8_BACKPRESSURE_QUOTA_REPORT.md`](./P8_BACKPRESSURE_QUOTA_REPORT.md) | Design/phase/test evidence document: AstraLog P8-G Bounded Backpressure, Watermarks and Quotas Qualification Report. Verify phase date/status before treating as current. | Not applicable |
| [`docs/P8_BENCHMARK_HARNESS_REPORT.md`](./P8_BENCHMARK_HARNESS_REPORT.md) | Design/phase/test evidence document: AstraLog P8-H Benchmark Harness and Deterministic Baseline Tooling Qualification Report. Verify phase date/status before treating as current. | Not applicable |
| [`docs/P8_CHECKLIST.md`](./P8_CHECKLIST.md) | Design/phase/test evidence document: AstraLog P8 Execution Checklist. Verify phase date/status before treating as current. | Not applicable |
| [`docs/P8_CLIENT_BATCHING_REPORT.md`](./P8_CLIENT_BATCHING_REPORT.md) | Design/phase/test evidence document: AstraLog P8-D Client Per-Partition Accumulator and Batching Qualification Report. Verify phase date/status before treating as current. | Not applicable |
| [`docs/P8_CLOSURE_REPORT.md`](./P8_CLOSURE_REPORT.md) | Design/phase/test evidence document: AstraLog P8 Closure Report. Verify phase date/status before treating as current. | Not applicable |
| [`docs/P8_CONTRACT_FREEZE_REPORT.md`](./P8_CONTRACT_FREEZE_REPORT.md) | Design/phase/test evidence document: AstraLog P8-A Contract / Checklist Freeze Report. Verify phase date/status before treating as current. | Not applicable |
| [`docs/P8_CONTROLLER_METADATA_REPORT.md`](./P8_CONTROLLER_METADATA_REPORT.md) | Design/phase/test evidence document: AstraLog P8-B Controller Topic-Performance Metadata Qualification Report. Verify phase date/status before treating as current. | Not applicable |
| [`docs/P8_E_ARCHITECTURE_BLOCKER.md`](./P8_E_ARCHITECTURE_BLOCKER.md) | Design/phase/test evidence document: AstraLog P8-E Architecture Blocker — Persistence Group Commit. Verify phase date/status before treating as current. | Not applicable |
| [`docs/P8_E_REFREEZE_REPORT.md`](./P8_E_REFREEZE_REPORT.md) | Design/phase/test evidence document: AstraLog P8-E Persistence Frontier Architecture Re-Freeze. Verify phase date/status before treating as current. | Not applicable |
| [`docs/P8_PERFORMANCE_V1.md`](./P8_PERFORMANCE_V1.md) | Design/phase/test evidence document: AstraLog P8 — Batching, Compression, Flow Control and Performance Baseline. Verify phase date/status before treating as current. | Not applicable |
| [`docs/P8_PERSISTENCE_GROUP_COMMIT_REPORT.md`](./P8_PERSISTENCE_GROUP_COMMIT_REPORT.md) | Design/phase/test evidence document: AstraLog P8-E Persistence Group Commit Qualification Report. Verify phase date/status before treating as current. | Not applicable |
| [`docs/P8_REPLICATION_FETCH_REPORT.md`](./P8_REPLICATION_FETCH_REPORT.md) | Design/phase/test evidence document: AstraLog P8-F Replication Pipelining and Fetch Tuning Qualification Report. Verify phase date/status before treating as current. | Not applicable |
| [`docs/P8_STORAGE_COMPRESSION_REPORT.md`](./P8_STORAGE_COMPRESSION_REPORT.md) | Design/phase/test evidence document: AstraLog P8-C Storage V1 Compression and Recovery Qualification Report. Verify phase date/status before treating as current. | Not applicable |
| [`docs/P9_CHECKLIST.md`](./P9_CHECKLIST.md) | Design/phase/test evidence document: AstraLog P9 Execution Checklist. Verify phase date/status before treating as current. | Not applicable |
| [`docs/P9_CONTRACT_FREEZE_REPORT.md`](./P9_CONTRACT_FREEZE_REPORT.md) | Design/phase/test evidence document: AstraLog P9-A Contract / Storage V2 Architecture Freeze Report. Verify phase date/status before treating as current. | Not applicable |
| [`docs/P9_CONTROLLER_RETENTION_METADATA_REPORT.md`](./P9_CONTROLLER_RETENTION_METADATA_REPORT.md) | Design/phase/test evidence document: AstraLog P9-B Controller Retention Metadata and Public Codec Qualification. Verify phase date/status before treating as current. | Not applicable |
| [`docs/P9_G_CLOSURE_REPORT.md`](./P9_G_CLOSURE_REPORT.md) | Design/phase/test evidence document: AstraLog P9-G — Local Closure Candidate Qualification Report. Verify phase date/status before treating as current. | Not applicable |
| [`docs/P9_G_WORK_IN_PROGRESS.md`](./P9_G_WORK_IN_PROGRESS.md) | Design/phase/test evidence document: P9-G In-Progress Engineering Ledger — NOT A CLOSURE REPORT. Verify phase date/status before treating as current. | Not applicable |
| [`docs/P9_KEYED_COMPACTION_REPORT.md`](./P9_KEYED_COMPACTION_REPORT.md) | Design/phase/test evidence document: P9-E Copy-on-Write Keyed Compaction Qualification Report. Verify phase date/status before treating as current. | Not applicable |
| [`docs/P9_LOG_START_DELETE_RETENTION_REPORT.md`](./P9_LOG_START_DELETE_RETENTION_REPORT.md) | Design/phase/test evidence document: P9-D Replicated Log Start and Delete-Retention Qualification Report. Verify phase date/status before treating as current. | Not applicable |
| [`docs/P9_PRODUCER_CHECKPOINT_REPORT.md`](./P9_PRODUCER_CHECKPOINT_REPORT.md) | Design/phase/test evidence document: P9-F Producer-State Checkpoint and Compaction-Safe P7 Recovery Qualification. Verify phase date/status before treating as current. | Not applicable |
| [`docs/P9_RETENTION_COMPACTION_V1.md`](./P9_RETENTION_COMPACTION_V1.md) | Design/phase/test evidence document: AstraLog P9 Retention and Log Compaction Contract V1. Verify phase date/status before treating as current. | Not applicable |
| [`docs/P9_STORAGE_V2_MANIFEST_REPORT.md`](./P9_STORAGE_V2_MANIFEST_REPORT.md) | Design/phase/test evidence document: AstraLog P9-C Storage V2 and Manifest Inventory Qualification. Verify phase date/status before treating as current. | Not applicable |
| [`docs/PERSISTENCE_FRONTIER_V2.md`](./PERSISTENCE_FRONTIER_V2.md) | Design/phase/test evidence document: AstraLog Persistence Frontier V2. Verify phase date/status before treating as current. | Not applicable |
| [`docs/PHASE_PLAN.md`](./PHASE_PLAN.md) | Design/phase/test evidence document: AstraLog Phase Plan and Exit Gates. Verify phase date/status before treating as current. | Not applicable |
| [`docs/PROTOCOL_V1.md`](./PROTOCOL_V1.md) | Design/phase/test evidence document: AstraLog Wire Protocol V1. Verify phase date/status before treating as current. | Not applicable |
| [`docs/RAFT_V1.md`](./RAFT_V1.md) | Design/phase/test evidence document: AstraLog Raft Core Contract V1. Verify phase date/status before treating as current. | Not applicable |
| [`docs/REMEDIATION_NOTES.md`](./REMEDIATION_NOTES.md) | Design/phase/test evidence document: Audit remediation — simple interview explanations. Verify phase date/status before treating as current. | Not applicable |
| [`docs/RESEARCH_REFERENCES.md`](./RESEARCH_REFERENCES.md) | Design/phase/test evidence document: AstraLog Research and Design References. Verify phase date/status before treating as current. | Not applicable |

## docs/baselines (3 files)

| File | What it contains / why it exists | Representative public code landmarks |
|---|---|---|
| [`docs/baselines/P8_ENGINEERING_BASELINE.md`](./baselines/P8_ENGINEERING_BASELINE.md) | Performance/Windows process baseline or measurement script; historical platform-specific evidence. | Not applicable |
| [`docs/baselines/P8_ENGINEERING_BASELINE.raw`](./baselines/P8_ENGINEERING_BASELINE.raw) | Performance/Windows process baseline or measurement script; historical platform-specific evidence. | Not applicable |
| [`docs/baselines/P8_WINDOWS_PROCESS_METRICS.ps1`](./baselines/P8_WINDOWS_PROCESS_METRICS.ps1) | Performance/Windows process baseline or measurement script; historical platform-specific evidence. | Not applicable |

## docs/benchmarks/interview_20261103 (5 files)

| File | What it contains / why it exists | Representative public code landmarks |
|---|---|---|
| [`docs/benchmarks/interview_20261103/SUMMARY.md`](./benchmarks/interview_20261103/SUMMARY.md) | Historical/pinned benchmark or reliability evidence; read source SHA/profile before citing. | Not applicable |
| [`docs/benchmarks/interview_20261103/release_raw.csv`](./benchmarks/interview_20261103/release_raw.csv) | Historical/pinned benchmark or reliability evidence; read source SHA/profile before citing. | Not applicable |
| [`docs/benchmarks/interview_20261103/release_test.log`](./benchmarks/interview_20261103/release_test.log) | Historical/pinned benchmark or reliability evidence; read source SHA/profile before citing. | Not applicable |
| [`docs/benchmarks/interview_20261103/reliability_restart_failover.log`](./benchmarks/interview_20261103/reliability_restart_failover.log) | Historical/pinned benchmark or reliability evidence; read source SHA/profile before citing. | Not applicable |
| [`docs/benchmarks/interview_20261103/reliability_transfer.log`](./benchmarks/interview_20261103/reliability_transfer.log) | Historical/pinned benchmark or reliability evidence; read source SHA/profile before citing. | Not applicable |

## docs/benchmarks/remediation_20261103 (4 files)

| File | What it contains / why it exists | Representative public code landmarks |
|---|---|---|
| [`docs/benchmarks/remediation_20261103/SUMMARY.md`](./benchmarks/remediation_20261103/SUMMARY.md) | Post-audit source-pinned benchmark raw samples, log or generated summary; never blend with historical baseline. | Not applicable |
| [`docs/benchmarks/remediation_20261103/release_latency_samples.csv`](./benchmarks/remediation_20261103/release_latency_samples.csv) | Post-audit source-pinned benchmark raw samples, log or generated summary; never blend with historical baseline. | Not applicable |
| [`docs/benchmarks/remediation_20261103/release_raw.csv`](./benchmarks/remediation_20261103/release_raw.csv) | Post-audit source-pinned benchmark raw samples, log or generated summary; never blend with historical baseline. | Not applicable |
| [`docs/benchmarks/remediation_20261103/release_test.log`](./benchmarks/remediation_20261103/release_test.log) | Post-audit source-pinned benchmark raw samples, log or generated summary; never blend with historical baseline. | Not applicable |

**Inventory cross-check:** 312 linked paths, including the new master guide; original checked-in inventory at `d6236c2` contained 311. The generated list is an index, not a code-quality or security audit of every line.

# 21. Final rehearsal — five things that earn trust

1. Start with the **system boundary and honest status** rather than an impressive list of unqualified features.
2. Trace one record **all the way to disk, quorum and read-committed Fetch**; distinguish Raft index, stream offset and commit index.
3. Reproduce one **real failure and exact byte oracle**; say where an ambiguous outcome stays unresolved.
4. Quote one **source-pinned, workload-defined throughput/latency result** and one statistical weakness.
5. End with a **precisely prioritized next qualification**, not “everything is production-ready.”
