# P4 — Replicated Data Partitions and Quorum Acks Closure Report

Status: **CLOSED / QUALIFIED**

Qualification date: **2026-09-27**

Normative contract: `docs/P4_REPLICATION_V1.md`

P4 contract-freeze baseline:

```text
eda67713ffa332d254d4369befad688a4a85600e
P4 freeze replicated partition integration contract v1
```

P3 consensus baseline consumed unchanged by P4:

```text
4a3474db75068516c508d99b294f2f28f4de2071
P3 close Raft core with deterministic and formal qualification
```

The final P4 closure commit is the commit containing this report.

## 1. Closure statement

P4 turns the closed P3 Raft state machine and frozen P1 storage format into real
RF=1/3/5 replicated data partitions with broker-to-broker Raft transport,
producer acknowledgement semantics, committed consumer visibility, leader/term
fencing, follower catch-up, and crash-safe rebootstrap.

P4 does not move consensus safety decisions into broker code. Broker/runtime
code executes P3 effects, persists them through the P1 adapter, transports P3
messages, and maps committed state to the public P2 API.

## 2. P1 storage adapter

P4 adds a deterministic adapter between P3 persistence operations and P1 bytes.

Qualified behavior:

- canonical Application Command V1 encoding carries the leader-assigned stream
  offset plus record contents;
- P3 application entries become P1 Data frames with the P3 term/index;
- leader barriers and configuration entries become P1 Control frames;
- control entries consume Raft indexes but no stream offsets;
- hard-state and snapshot sidecars are versioned, CRC32C-protected, and atomically
  replaced;
- P1 `partition.meta.committed_raft_index` remains the durable committed-index
  bridge for consumer visibility;
- a CRC-protected commit-intent journal makes cross-file commit advancement
  recoverable across crashes;
- unexplained hard-state/P1 commit mismatch fails loudly rather than guessing;
- recovery reconstructs a validated P3 `RecoveredState` from P1 frames plus P4
  sidecars.

No P1 frame/header/meta format bytes were changed.

## 3. Raft suffix repair in P1

P4 adds `truncate_raft_suffix(from_index)` to the existing P1 partition-log
implementation without changing the format.

It:

- refuses truncation at/below the committed index;
- truncates inside active or rolled segments;
- deletes later segment/index families;
- rebuilds derived indexes;
- recomputes next Raft index, stream offset, and durable tail;
- preserves committed history;
- survives reopen/recovery.

This is used only when P3 has already decided that an uncommitted suffix may be
replaced.

## 4. Internal broker-pair Raft transport

P4 implements a dedicated bounded internal TCP transport independent from the P2
client listener.

Qualified properties include:

- persistent broker-pair channels;
- multiple partition groups multiplexed on one peer channel;
- explicit broker ID and incarnation fencing;
- topic/partition group fencing;
- bounded frame and byte queues;
- bounded reconnect/backoff;
- CRC32C-protected framing;
- length/version checks before allocation;
- arbitrary TCP fragmentation;
- no silent frame resynchronization after corruption;
- deterministic codecs for every P3 `RaftMessage` variant;
- physical queue/byte saturation returns Busy rather than allocating without
  bound;
- InstallSnapshot may carry a bounded rebootstrap package over the same peer
  channel.

## 5. Replicated partition runtime

Each local replicated partition has exactly one owner for one P3 `RaftCore`.

The owner serializes:

- logical election ticks;
- peer messages;
- client proposals;
- persistence completions/failures;
- apply/snapshot effects.

P3 `PersistenceBatch` operations are executed in emitted order. Completion is
fed back to P3 only after required durable storage work completes. Persistence
failure makes the replica fatal/non-serving rather than manufacturing an ack.

Producer queues, pending proposal count/bytes, runtime Raft outbox, peer frames,
and peer bytes are bounded. Local/peer transport congestion rejects new producer
work while Raft control processing remains live and retryable.

## 6. RF=1, RF=3, and RF=5

The same closed P3 semantics drive every supported replication factor.

- RF=1: election, leader barrier, local durable proposal, local quorum commit,
  committed fetch.
- RF=3: majority replication, follower persistence-before-success, quorum
  producer ack, failover, lag/rejoin, minority fencing.
- RF=5: majority of three; a committed record remains available after two tested
  replica failures and a new election among the remaining three.

No broker-specific quorum arithmetic exists.

## 7. Producer acknowledgements

### `acks=0`

- valid only through the P2 NO_RESPONSE path;
- accepted only on the current partition leader when bounded capacity exists;
- emits no response frame;
- rejection/failure can therefore be invisible to the producer;
- no durability guarantee is advertised.

Both leader acceptance and wrong-node invisible rejection are exercised through
the real TCP/client paths.

### `acks=leader`

Success means the leader's P1 representation of the P3 entry is locally durable.
It does not imply quorum commit.

The deterministic disk-backed test intentionally demonstrates that a locally
durable/acknowledged uncommitted record can be lost after failover and suffix
repair.

### `acks=quorum`

Success is produced only after the P3 entry is quorum committed and the leader's
commit watermark has been durably advanced through P1/P4 persistence.

Tests prove it does not complete at local append alone and survives the tested
minority failures.

If the response is lost after commit, a retry can create a duplicate. A P4 test
commits one logical request, deliberately loses the client-visible completion,
retries after failover, and observes two committed copies. Exact retry
deduplication remains intentionally deferred to P7.

## 8. Committed visibility

Default Fetch remains committed-only.

The visible stream high watermark is one past the highest stream offset in a Data
frame whose Raft index is at/below P1's durable committed Raft index.

Qualified properties:

- uncommitted locally durable tails remain invisible;
- control entries do not advance the stream watermark;
- Fetch high watermark uses committed visibility;
- ListOffsets(latest) uses committed visibility rather than physical tail;
- committed visibility remains correct during follower lag and after failover.

## 9. Routing and leader epoch fencing

P4 defines public leader epoch as the current Raft term.

The broker integration proves:

- non-leader Produce returns `NotLeader` with best-known leader/term;
- explicit lower epoch returns `StaleEpoch`;
- explicit impossible future epoch also returns `StaleEpoch`;
- `UNKNOWN_EPOCH` is accepted as the documented opt-out from client epoch
  checking, but does not turn a follower into a leader;
- metadata exposes the current static P4 replica set and current leader/term;
- failover publishes a different leader with a strictly newer term.

## 10. Ordinary catch-up and conflict repair

AppendEntries remains the ordinary catch-up mechanism.

Disk-backed P4 tests prove:

- a lagging follower may miss several committed writes, rejoin, and catch up;
- an old leader may contain a locally durable but uncommitted suffix;
- after failover, P3 conflict processing authorizes P1 suffix truncation;
- the repaired follower converges to the new committed stream;
- stale old-term AppendEntries delivered after failover cannot regress the new
  leader or its durable history.

## 11. Rebootstrap / InstallSnapshot

P4 carries a deterministic rebootstrap package containing P3 snapshot metadata
and the authoritative P1 prefix through the snapshot boundary.

The package is bounded, CRC-protected, canonical, group-scoped, and validated for
contiguous Raft indexes and exact snapshot boundary identity.

Critically, receiving a package does not authorize disk replacement. The runtime
stashes it and passes the `InstallSnapshotRequest` to P3. Only if P3 emits its
`InstallSnapshot` persistence operation may P4 consume the package.

Installation uses a sibling staging directory:

1. create a fresh P1 partition in staging;
2. materialize/flush all package frames;
3. mark the snapshot commit boundary;
4. persist hard-state and snapshot sidecars;
5. reopen and validate staging;
6. persist a publication intent;
7. close old handles;
8. rename live -> backup and fsync parent;
9. rename staging -> live and fsync parent;
10. reopen/validate the new authoritative partition;
11. only then remove backup/intent.

Crash-state tests prove:

- crash before publication preserves old authoritative data;
- crash after moving old -> backup but before publishing new rolls back old;
- a published valid generation is retained on restart;
- a published corrupt generation restores the previous backup;
- an interrupted transaction never merges generations;
- ordinary AppendEntries resumes at `snapshot_index + 1` after installation.

P9 still owns retention/compaction policy; P4 implements only the correctness
path needed when P3 requires snapshot/rebootstrap catch-up.

## 12. Storage and crash failure qualification

The real P1-backed deterministic P4 harness mechanically exercises:

- leader failure before local persistence;
- local durable append before P3 persistence completion;
- leader failure after local persistence but before quorum;
- committed response loss followed by duplicate retry;
- minority network partition;
- follower disk persistence failure;
- leader disk persistence failure;
- check-quorum loss while a quorum producer request is pending;
- stale internal term after failover/reconnect;
- committed-only visibility during lag;
- RF=1/3/5 election/commit behavior;
- conflicting uncommitted suffix replacement;
- rebootstrap interruption/recovery.

The check-quorum scenario explicitly confirms the pending quorum proposal becomes
`LeadershipLost` rather than success after the isolated leader steps down.

## 13. Broker end-to-end qualification

The RF=3 TCP integration test uses three actual broker runtimes, P2 clients, P4
internal TCP transport, P1 files, and P3 cores.

It covers:

- metadata replica list/leader term;
- NotLeader redirect;
- lower/future StaleEpoch fencing;
- acks=0 NO_RESPONSE behavior;
- quorum Produce;
- committed Fetch;
- committed ListOffsets;
- one follower shutdown while quorum writes continue;
- follower restart/rejoin;
- leader shutdown/failover;
- committed history after failover;
- restart of the former leader from P1 bytes + P4 sidecars;
- removal of the other follower so a new quorum write can succeed only if the
  restarted former leader recovered/caught up correctly.

The full scenario was also executed successfully five consecutive times before
the final workspace qualification to remove a previously observed test-timing
assumption.

## 14. Exit invariants

Mechanical evidence now supports the P4 exit gate:

- every tested quorum-ack success survives the tested minority failures;
- default consumers never observe uncommitted records;
- leader-only acknowledged loss is demonstrated rather than hidden;
- committed-prefix comparison across live replicas finds no split-brain
  committed histories;
- follower success remains downstream of real persistence;
- stale leaders/epochs are fenced from client writes;
- recovery reconstructs durable P3 state without guessing.

## 15. Scope audit

Against P3 closure commit `4a3474d`:

- `crates/astralog-raft` has **no P4 source diff**;
- P3 remains exactly the closed 120-test consensus core;
- P1 `format.rs` and `manifest.rs` have **no diff**;
- P4's storage additions operate through existing P1 Format V1 bytes;
- P5 dynamic controller membership/placement is not implemented;
- P6 groups/offset coordination is not implemented;
- P7 idempotent producer state is not implemented;
- P8 compression/performance claims are not implemented;
- P9 retention/compaction policy is not implemented;
- P10 queue/DLQ semantics are not implemented.

## 16. Qualification evidence

Pinned Linux qualification environment: **Rust 1.98.1**.

- `cargo fmt --all -- --check`: **PASS**
- `cargo clippy --workspace --all-targets --all-features --offline -- -D warnings`: **PASS**
- `cargo test --workspace --all-features --offline`: **PASS**
- workspace tests: **272 passed, 0 failed**
- P3 `astralog-raft`: **120 passed, 0 failed**
- P4 `astralog-replication`: **46 passed, 0 failed**
- P4 RF=3 broker TCP integration: **PASS**
- RF=3 broker TCP integration repeated 5 times: **5/5 PASS**
- P1 storage: **43 passed, 0 failed**
- immutable protocol golden vectors: **11 passed, 0 failed**
- all doc-test targets: **PASS**
- `git diff --check`: **PASS**
- P3 source diff from P3 closure: **empty**
- P1 `format.rs` / `manifest.rs` diff from P3 closure: **empty**

The exact final closure commit is requalified with the same strict workspace gate
before P4 is declared mechanically closed.

## 17. P5 handoff

P4 intentionally ends with static topology. P5 owns the authoritative controller
Raft group, broker membership/incarnation, dynamic replica assignments, placement,
heartbeats, feature levels, and controlled add-learner/catch-up/promote/remove
workflows.

Before any P5 implementation begins, P5 must first be decomposed into the same
base-level implementation actions used for P4.
