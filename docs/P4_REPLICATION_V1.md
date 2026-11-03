# AstraLog P4 Replicated Partition Contract V1

Status: **FROZEN FOR P4 IMPLEMENTATION**

This document freezes the integration contract between the closed P1 storage
format, the closed P3 Raft core, and the P4 replicated-partition runtime.

P4 may add adapters, sidecar metadata, internal transport, and broker runtime
code. P4 must not change P1 Storage Format V1 bytes or reinterpret P3 consensus
safety rules.

## 1. Ownership boundary

P3 owns consensus decisions:

- term/vote rules;
- election/pre-vote;
- leader authority;
- log matching/truncation decisions;
- replication progress;
- commit quorum;
- learners and joint consensus;
- snapshots and transfer safety.

P4 owns execution/integration:

- durable mapping of P3 persistence effects to P1 partition storage plus P4
  sidecars;
- internal Raft RPC transport;
- one P3 core per replicated data partition;
- mapping producer requests to deterministic Raft application commands;
- producer acknowledgement completion;
- committed Raft index to visible stream high watermark;
- leader/term exposure through the existing P2 client protocol fields;
- follower catch-up and snapshot/rebootstrap data movement.

Broker code must never independently decide quorum, vote eligibility, log
truncation safety, or commit advancement.

## 2. Replica identity and group identity

A data Raft group is identified by:

```text
RaftGroupId = (TopicId, PartitionId)
```

P4 maps broker IDs to P3 `NodeId` using:

```text
NodeId = broker_id + 1
```

Broker ID `u32::MAX` is invalid for P4 because the mapping must fit a non-zero
P3 `NodeId` deterministically.

P5 will later make assignments/controller truth dynamic. P4 uses an explicit
static replica assignment supplied by configuration/test harness. P4 must not
invent controller policy.

Supported replication factors are exactly:

```text
RF = 1, 3, or 5
```

All P4 replicas are voters. Learner promotion remains a P3 primitive and is used
for catch-up/rebootstrap tests, but P5 owns dynamic placement workflows.

## 3. Replicated application command

P3 `EntryKind::Application(Vec<u8>)` contains a deterministic P4 command, not an
already encoded P1 frame.

Application Command V1 is:

```text
u16 version = 1
u16 command_kind = 1  // append record batch
u64 base_stream_offset
u32 record_count
repeat record_count:
    i64 timestamp_ms
    i32 key_len      // -1 = null
    i32 value_len    // -1 = null
    u16 header_count
    repeat header_count:
        u16 header_name_len
        i32 header_value_len // -1 = null
        header_name bytes
        header_value bytes when non-null
    key bytes when non-null
    value bytes when non-null
```

All integers are little-endian because this is a local replicated state-machine
encoding, not the P2 network protocol.

The leader assigns `base_stream_offset` before proposing. The assignment is part
of the replicated command, so every replica materializes identical stream
offsets.

P4 rejects malformed, non-canonical, oversized, overlapping, or non-contiguous
commands before writing P1 storage.

P7 may extend the command through a new command version/kind for producer
identity/sequence semantics. P4 does not silently add idempotency.

## 4. Mapping P3 log entries to P1 frames

For a P3 log entry `(term, index, kind)`:

### Application

Decode Application Command V1 and write:

```text
Frame::Data(RecordBatch {
    raft_term  = term,
    raft_index = index,
    base_stream_offset = command.base_stream_offset,
    producer = None,
    compression = None,
    records = command.records,
})
```

### LeaderBarrier

Write a P1 Control frame:

```text
control_kind = 0x0101
control_version = 1
body = empty
```

### Configuration(change)

Write a P1 Control frame:

```text
control_kind = 0x0102
control_version = 1
body = deterministic P4 membership encoding
```

Control entries consume Raft indexes but consume zero stream offsets.

P1 frame/header/meta byte layouts remain unchanged.

## 5. P4 sidecar durable state

P1 `partition.meta` remains authoritative only for the committed Raft index and
its existing metadata epoch. P4 adds sidecars; it does not repurpose reserved P1
bytes.

Each replicated partition directory contains:

```text
raft.hardstate.v1
raft.snapshot.v1
```

Both sidecars are fixed-header, versioned, CRC32C-protected, atomically replaced
files.

`raft.hardstate.v1` stores:

- current term;
- optional voted-for NodeId;
- the same durable commit index represented by P1 `partition.meta`.

On recovery the hard-state commit and P1 committed index must match exactly.
Mismatch is corruption/fatal; P4 never guesses which value is newer.

`raft.snapshot.v1` stores P3 snapshot metadata needed to reconstruct the Raft
core. Snapshot data transfer/rebootstrap uses P1 frames plus this metadata.

## 6. Persistence batch execution

P4 executes a P3 `PersistenceBatch` synchronously and in listed operation order.
It returns `PersistenceCompleted(token)` only after every required durability
operation for the batch has succeeded.

Mapping:

```text
UpdateHardState   -> atomically persist raft.hardstate.v1
TruncateSuffix    -> P1 logical Raft suffix truncation API
Append            -> map entries to P1 frames, append, flush_data
AdvanceCommit     -> P1 mark_committed + hardstate commit sync
InstallSnapshot   -> durable snapshot sidecar + rebootstrap/install transaction
DiscardPrefix     -> P4 snapshot/rebootstrap retention hook; no P1 format change
```

When a batch changes both P1 bytes and a P4 sidecar, completion is emitted only
after all required pieces are durable. Any ambiguous storage error is reported
as persistence failure, never success.

P3 remains the only component allowed to decide whether truncation/commit is
safe. The P1 truncation API independently refuses to truncate committed data as
defense in depth.

## 7. P1 suffix truncation API

P4 adds a storage operation:

```text
truncate_raft_suffix(from_index)
```

Required behavior:

- reject `from_index <= committed_raft_index`;
- remove every frame with Raft index `>= from_index`;
- support truncation inside the active or a rolled segment;
- delete later segments;
- rewrite only the affected segment tail, never preceding valid bytes;
- rebuild derived indexes;
- reopen/create the active segment safely;
- recompute next Raft index and next stream offset from retained authoritative
  frames;
- durable boundary cannot remain above the retained tail;
- crash/reopen yields either the old valid suffix or the new valid suffix, never
  a fabricated committed history.

No frame or segment format version changes.

## 8. Internal Raft transport V1

P4 uses a dedicated broker-to-broker TCP listener, separate from the client P2
listener.

One persistent TCP connection may multiplex many partition groups between a
broker pair.

Transport framing:

```text
u32 frame_len_be
u16 version_be = 1
u16 message_kind_be
u32 source_broker_id_be
u64 source_incarnation_be
16 bytes topic_id
u32 partition_be
u32 payload_len_be
payload
u32 crc32c_be
```

`frame_len` includes all bytes after the length prefix through CRC.

Limits are configured and checked before allocation. Invalid length, CRC,
version, identity, group, or payload closes/rejects the internal frame; the
decoder never attempts byte-stream resynchronization after corrupt framing.

The message payload is a deterministic codec for every P3 `RaftMessage` variant.
P4 transport carries P3 decisions; it does not alter them.

P5 later supplies authoritative broker incarnation membership. In P4 the static
cluster configuration supplies expected broker ID/incarnation and transport
rejects a configured mismatch.

## 9. Runtime scheduling and bounds

Each local replica has exactly one logical owner/driver for its `RaftCore`.

Inputs for a partition are serialized through a bounded queue. The driver:

1. handles one P3 input;
2. drains effects in emitted order;
3. executes persistence effects before delivering their completion input;
4. sends outbound Raft messages through bounded per-peer transport queues;
5. delivers apply effects to partition visibility/proposal completion state;
6. repeats until quiescent.

Required bounds:

- partition input queue;
- per-peer outbound Raft bytes/messages;
- pending producer proposals;
- pending proposal bytes;
- transport frame bytes;
- reconnect/backoff state.

Control traffic must not be starved by producer data. P4 may reject/throttle new
producer proposals with `BrokerBusy`/`NotEnoughReplicas`; it must not block Raft
heartbeats/elections behind an unbounded producer queue.

## 10. Leader epoch

The P2 protocol already carries `leader_epoch: u64`.

For P4:

```text
leader_epoch = current Raft term
```

Rules:

- a non-leader returns `NotLeader`, plus best-known leader broker/term when known;
- a request with explicit epoch lower or higher than the local authoritative
  leader term returns `StaleEpoch`;
- `UNKNOWN_EPOCH` remains accepted as an explicit client opt-out from epoch
  fencing;
- an old-term node never accepts a leader-only produce merely because metadata
  is stale.

P5 may add metadata epochs/incarnation fencing but does not redefine the P4 Raft
leader epoch.

## 11. Producer acknowledgement semantics

### `acks=0`

- accepted only by the current leader;
- proposal may be queued if local bounded capacity exists;
- no response is sent by the P2 NO_RESPONSE path;
- no durability guarantee is advertised;
- rejection/failure after request acceptance may be invisible to the producer.

### `acks=leader`

Success is returned only after the leader's P1 frame for the proposed Raft entry
is durably flushed locally.

It does **not** wait for Raft quorum commit. Therefore the acknowledged record may
later disappear if that uncommitted suffix loses leadership. This loss is an
explicit tested/documented API property.

### `acks=quorum`

Success is returned only after:

1. the leader has proposed the record batch;
2. P3 has quorum-committed the corresponding Raft entry under the current
   configuration; and
3. the leader has durably persisted the commit watermark.

This is the P4 externally visible durability guarantee.

If the producer response is lost after commit, P4/P2 is still non-idempotent:
retry may create a duplicate logical record batch. P7 fixes this with producer
identity/sequence. P4 must document/test the ambiguity rather than silently
deduplicate.

## 12. Proposal identity and response completion

P4 allocates a process-local unique P3 `ProposalId` for each in-flight produce
partition proposal.

The runtime tracks:

- assigned stream offset range;
- Raft index once locally durable;
- requested ack mode;
- completion channel/deadline.

`ProposalResult { accepted: true }` means the leader accepted and durably staged
its local Raft entry; it is sufficient for `acks=leader` but **not** for
`acks=quorum`.

`acks=quorum` completes when the proposal's Raft index is at or below P3's
durable committed index.

Leadership loss before the requested acknowledgement condition returns
`NotLeader` or `RequestTimeout`; P4 never fabricates success.

## 13. Committed visibility and stream high watermark

P1 `partition.meta.committed_raft_index` is the durable bridge from P3 commit to
consumer visibility.

Default Fetch remains `ReadCommitted`.

P4 computes the stream high watermark as:

```text
one past the highest stream offset contained in a Data frame whose raft_index
<= committed_raft_index
```

Control entries do not advance stream offsets.

Fetch must never return a Data frame beyond the committed Raft index.

`ListOffsets(latest)` and Fetch high-watermark reporting use committed stream
visibility, not merely locally appended data.

## 14. Replica catch-up and rebootstrap

Ordinary catch-up is driven entirely by P3 AppendEntries.

If P3 switches a peer to `InstallSnapshot` because required history is below the
retained boundary, P4 transfers a deterministic rebootstrap package containing:

- P3 snapshot metadata;
- authoritative P1 frames representing state through the snapshot boundary;
- any required stable partition metadata;
- integrity hashes/CRC for transferred chunks.

The follower installs into a staging directory and atomically publishes the new
partition generation only after complete validation and durability. It then
continues with ordinary AppendEntries for the suffix.

An interrupted rebootstrap never replaces the previously authoritative local
generation.

P9 later adds retention/compaction and production snapshot generation policy;
P4 implements the correctness path needed to rebootstrap a follower, not P9
retention policy.

## 15. Static P4 topology

Because P5 owns controller membership and placement, P4 integration tests use a
static cluster descriptor containing:

- broker ID;
- node UUID/incarnation;
- client address;
- Raft address;
- topic/partition replica list.

CreateTopic in the ordinary single-broker P2 administrative path remains RF=1
unless the broker was bootstrapped with an explicit P4 static assignment.

P4 must not pretend static test topology is the future production controller.

## 16. Failure semantics

P4 must mechanically exercise at least:

1. leader dies before local persistence;
2. leader dies after local persistence but before quorum;
3. leader dies after quorum commit before producer response;
4. duplicate retry after lost response;
5. minority network partition;
6. lagging follower rejoin;
7. conflicting uncommitted suffix truncation;
8. follower disk persistence failure;
9. leader disk persistence failure;
10. stale leader/epoch client request;
11. stale internal Raft term after reconnection;
12. RF=1/3/5 election and commit paths;
13. committed-only fetch during replication lag;
14. check-quorum loss while producer requests are pending;
15. snapshot/rebootstrap interruption and retry.

## 17. P4 exit invariants

P4 may close only if tests prove:

- every `acks=quorum` success survives any tested minority replica failure;
- default consumers never observe uncommitted records;
- `acks=leader` acknowledged suffix loss is demonstrated/documented;
- no two correct replicas expose different committed records for one partition
  position;
- follower success never precedes local durable persistence;
- stale leaders cannot accept leader-only client operations;
- leader epoch redirects are deterministic;
- RF 1/3/5 use the same P3 safety semantics;
- recovery reconstructs P3 state from durable P4/P1 state without guessing;
- P1 golden bytes remain unchanged;
- P3 consensus source remains unchanged unless a separately reviewed P3
  compatibility fix is proven necessary.

## 18. P4 non-goals

P4 does not implement:

- controller consensus or dynamic placement (P5);
- consumer group coordination (P6);
- producer idempotency/exact retry deduplication (P7);
- compression/performance targets (P8);
- retention/compaction policy (P9);
- work queues/DLQ (P10);
- production authentication/TLS/rolling upgrade policy (P11);
- final system benchmark claims (P12).
