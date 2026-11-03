# P2 Closure Report

Phase: **P2 — Binary Protocol, Single Broker, Client, Partitioning**

Result: **PASS — Wire Protocol V1 frozen**

Qualification date: **2026-09-23**

## Scope

P2 makes AstraLog's already-qualified P1 single-node durable log usable through
the real client/broker wire path. It implements the public Wire V1 contract, a
bounded single-broker network runtime, the P2 metadata/admin plane, storage-backed
streaming APIs, stable partitioning, and a Rust client.

P2 deliberately does **not** implement Raft consensus, replicated data
partitions, the controller quorum, consumer groups, producer idempotency, work
queues, compression execution, TLS/ACL enforcement, or other P3+ behavior.

## Frozen artifacts

- `docs/PROTOCOL_V1.md`
- Hello V1 negotiation bytes
- Wire V1 fixed frame header and CRC32C coverage
- primitive and tagged-field encodings
- permanent API-key assignments
- public error-code numbers and retry classes
- P2 API V0 schemas
- MurmurHash3 x86 32 keyed partitioning
- null-key sticky partitioning semantics
- 20 protocol/partition compatibility fixtures under
  `crates/astralog-protocol/tests/golden/`

Future incompatible changes require a new API or wire version according to
`PROTOCOL_V1.md`; existing V1 bytes are not reinterpreted.

## Implementation delivered

### Protocol core

- big-endian bounded primitive codecs
- fixed Hello V1 client/server negotiation
- CRC32C-protected Wire V1 frames
- arbitrary TCP-fragment reassembly
- canonical/skippable tagged fields
- semantic validation for known CLIENT_ID and TRACE_ID header tags
- exact P2 API V0 body codecs
- permanent public API/error registries
- explicit retryable/terminal classification for every frozen public error code
- compatibility golden vectors for requests, responses, errors, and partitioning

### Single-broker runtime

- persistent Tokio TCP listener
- Hello -> ApiVersions -> Ready connection state
- bounded concurrent connections
- bounded frame receive buffer
- bounded per-connection in-flight requests
- response-memory budget tied to maximum frame size and in-flight count
- correlation-ID multiplexing and out-of-order completion
- duplicate active correlation fencing
- request deadlines
- incomplete-frame assembly timeout
- framing corruption closes the connection without scan-ahead resynchronization
- graceful broker shutdown aborts and drains connection tasks

### Durable P2 metadata catalog

- stable cluster ID
- stable broker node UUID
- durable broker ID
- monotonically increasing broker incarnation epoch
- monotonically increasing metadata epoch
- CreateTopic/DeleteTopic journal records
- CRC32C record protection
- incomplete final record truncation only
- complete-record CRC corruption is surfaced
- file and directory durability sync for catalog creation
- bounded configured topic/partition inventory
- serialized topic lifecycle mutation

### Storage-backed streaming

- Produce converts Wire V0 records into P1 Storage Format V1 records
- P1 disk bytes are unchanged
- `acks=0` sends no response and does not advance the committed boundary
- `acks=leader` performs the qualified P1 local durability operation before success
- RF=1 `acks=quorum` uses the same one-replica durable commit boundary without
  implementing Raft early
- committed-only Fetch
- long-poll Fetch with lost-wakeup protection
- Fetch record selection is bounded by record count, byte budget, and negotiated
  response-frame capacity instead of materializing complete partition history
- ListOffsets earliest/latest/timestamp scans committed history without retaining
  the complete history in memory
- concurrent missing-partition open is serialized to avoid duplicate local writers

### Rust client

- bootstrap TCP connection
- Hello V1 + ApiVersions negotiation
- negotiated maximum frame size
- metadata cache
- broker-ID to connection routing abstraction
- persistent multiplexed connections
- bounded in-flight request semaphore
- one deadline budget covering semaphore wait, write, and response wait
- response correlation is strict: an unsolicited response correlation fails the
  connection instead of being silently ignored
- typed public broker errors and unchanged retry classification
- no automatic retry of ambiguous P2 Produce
- protocol-defined Murmur3 keyed partitioning
- sticky null-key partitioning
- metadata refresh on NOT_LEADER/STALE_EPOCH responses

## Qualification evidence

Pinned Linux qualification environment: **Rust 1.98.1**.

- `cargo fmt --all -- --check`: **PASS**
- `cargo clippy --workspace --all-targets --all-features -- -D warnings`: **PASS**
- `cargo test --workspace --all-features`: **PASS**
- `git diff --check`: **PASS**

Test accounting:

- `astralog-broker`: **4 unit + 9 network/integration = 13**
- `astralog-client`: **3**
- `astralog-common`: **2**
- `astralog-protocol`: **33 unit/adversarial + 11 golden-test cases = 44**
- `astralog-storage`: **39**
- **workspace total: 101 passed, 0 failed**
- all doc-test targets: **PASS**

## Adversarial and integration proof

The qualified suite covers:

- every incremental protocol-fragment boundary
- one-byte-at-a-time frame input
- multiple frames in one read buffer
- deterministic arbitrary-byte decoder inputs without panic
- oversized prefix rejection before body allocation
- impossible body/header/tag length rejection
- CRC bit corruption
- unknown frame kinds and reserved flags
- duplicate/unsorted tags
- invalid known header-tag semantics
- duplicate active correlation IDs
- slowloris partial-frame eviction
- allocated-but-unimplemented API-key error classification
- unsupported P2 API-version classification
- persistent-connection multiplexing while a Fetch long poll is outstanding
- bounded multi-partition Fetch response truncation
- stale leader-epoch metadata refresh
- Create/Describe/Delete topic lifecycle remains authoritative across restart,
  and recreating a deleted name receives a new topic UUID
- committed-only visibility across `acks=0`
- `acks=leader` durability across broker restart
- RF=1 quorum durability across broker restart
- ListOffsets earliest/latest/timestamp lookup
- stable cluster/node identity across restart
- increasing broker incarnation and metadata epochs across restart
- catalog incomplete-tail recovery and CRC-corruption failure
- immutable Murmur3 partition compatibility vectors

## Correctness conclusions

### Version compatibility is explicit

Hello selects the Wire version before framed requests are parsed. ApiVersions
then advertises only implemented API versions. Allocated future API keys are not
advertised and return `UNSUPPORTED_API_KEY` in P2.

### Network memory is bounded

Frame length is checked before allocation. Each connection has configured
receive, in-flight, and response bounds. Slow partial frames expire. Fetch
selection also applies one negotiated response-frame budget across all partition
results, preventing a valid multi-partition request from accumulating an
oversized response.

### Durability claims remain honest

P2 does not simulate replication. With RF=1, quorum is one replica, so quorum
success proves local durable commit only. `acks=leader` is locally durable;
`acks=0` makes no success claim. P3/P4 later implement actual Raft majority
commit while preserving these public semantics.

### P2 Produce is intentionally non-idempotent

If a response is lost after a successful append, P2 cannot prove whether the
mutation executed. The Rust client therefore does not silently retry Produce.
P7 owns producer IDs, epochs, sequence numbers, and supported retry
deduplication.

## Known P2 limitations

- single broker only; replication factor other than 1 is rejected
- no Raft engine, leader election, follower replication, or controller quorum
- leader epoch is the P2 single-broker epoch placeholder defined by Wire V1
- topic deletion removes the topic from authoritative P2 metadata immediately,
  while physical orphaned partition-directory reclamation is deferred to later
  lifecycle/retention work
- no authentication, authorization, or TLS policy enforcement yet
- no producer idempotency; ambiguous Produce outcomes may duplicate on an
  application-initiated retry
- no batch compression execution
- no consumer groups or work-queue APIs
- performance benchmarks are not claimed in P2

## Architecture-boundary audit

The P2 implementation contains no Raft state machine, RequestVote,
AppendEntries, replica RPC, controller implementation, consumer-group
coordinator, idempotent-producer state machine, queue lease state machine, or
compression execution.

References to future API keys are registry reservations frozen by Wire V1.
`RaftTerm`/`RaftIndex` values used when writing through P1 are existing P1
storage-format fields and are not a consensus implementation.

## Closure

**P2 is CLOSED. Wire Protocol V1 is FROZEN.**

The qualified implementation, compatibility fixtures, and closure evidence are
committed as the P2 phase snapshot. P3 had not started before P2 closure.
