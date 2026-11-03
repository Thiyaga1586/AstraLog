# AstraLog P7 Idempotent Producer Contract V1

Status: **FROZEN FOR P7 IMPLEMENTATION**

This document is the normative contract for P7 — Idempotent Producer and Duplicate Detection.
It extends the exact P6 closure HEAD without weakening any frozen P0-P6 contract. P7 exists to
make supported producer retries safe after ambiguous network/leader failures. It does **not**
claim end-to-end exactly-once processing, cross-partition transactions, or global producer
ordering.

Implementation may begin only from this contract plus `P7_CHECKLIST.md`.

## 1. Ownership boundary

P7 introduces two distinct authorities:

1. **controller producer-session authority**
   - allocates a globally unique ProducerId from the already-qualified P5 monotonic allocator;
   - creates and advances ProducerEpoch values;
   - serializes competing epoch bumps for one ProducerId;
   - persists producer-session authority through controller Raft and controller snapshots;
2. **data-partition producer-sequence authority**
   - validates the ProducerId/ProducerEpoch presented to one partition;
   - owns that producer's expected next sequence for that partition;
   - detects supported committed retries;
   - returns the original committed offset range for an exact retry;
   - reconstructs authority from committed partition history plus a derived producer snapshot.

P3 remains the sole owner of consensus/election/commit safety. P4 remains the owner of data
partition replication, quorum acknowledgement and committed visibility. P5 remains the owner of
cluster metadata and the monotonic ProducerId counter. P7 must not create a second event log,
consensus mechanism, producer database, or broker-local source of durable truth.

The controller is **not** on the hot path of every Produce request. It is consulted only for
InitProducer and for a partition leader that must validate an unseen or higher ProducerEpoch.

## 2. Frozen producer identity types

~~~text
ProducerId    = u64, non-zero
ProducerEpoch = u32, non-zero
Sequence      = u32
~~~

Rules:

- ProducerId `0` is reserved as the InitProducer "allocate new" sentinel and is never issued;
- ProducerEpoch `0` is reserved as the InitProducer new-session sentinel and is never active;
- a newly created producer session starts at ProducerEpoch `1`;
- a successful epoch bump increments by exactly one;
- ProducerEpoch overflow fails closed and cannot wrap;
- per-partition Sequence starts at `0` for each newly accepted ProducerEpoch;
- sequence arithmetic is checked; a range that would exceed `u32::MAX` is rejected before
  proposal;
- sequence numbers never wrap inside one ProducerEpoch.

## 3. Producer-session lifecycle

The controller owns:

~~~text
ProducerSessionRecordV1
  producer_id: ProducerId
  current_epoch: ProducerEpoch
~~~

There is no wall-clock session lease in P7. A producer session does not expire merely because a
client disconnects. P7 intentionally implements no producer-session garbage collection; physical
cleanup remains a later explicitly designed concern.

### 3.1 New session

`InitProducer(producer_id=0, expected_epoch=0)` is the only form that creates a new session.

The controller applies one atomic committed mutation that:

1. reads `next_unallocated_producer_id`;
2. rejects zero/overflow;
3. reserves exactly that ProducerId by advancing the same durable P5 counter by one;
4. inserts `ProducerSessionRecordV1 { producer_id, current_epoch=1 }`;
5. advances controller metadata epoch according to the controller state-machine contract;
6. returns `(producer_id, 1)` only after controller Raft commit and ordered application.

No broker-local ProducerId block may establish public producer authority.

### 3.2 Epoch bump / producer restart

`InitProducer(producer_id=P, expected_epoch=E)` with non-zero values is a compare-and-swap epoch
bump.

It succeeds only when the committed controller record for `P` exists and has exactly epoch `E`.
The committed result is epoch `E+1`. A competing bump from the same expected epoch cannot also
succeed. A request using an older epoch is fenced.

The epoch bump itself does not synchronously contact every data partition. Partition fencing is
therefore deliberately partition-local as defined below.

### 3.3 Lost InitProducer response

InitProducer is commit-before-response but P7 does not pretend that every lost mutation response
is reconstructable from a client-generated request token.

- if a new-session response is lost after commit, the issued ProducerId may be abandoned;
- if an epoch-bump response is lost after commit, the caller must not blindly reuse the old
  producer authority;
- the safe recovery path is to create a fresh producer session unless the client can otherwise
  prove the returned authority;
- wasting a ProducerId or ProducerEpoch is acceptable; reuse/regression is forbidden.

P7 therefore favors monotonic fencing safety over reclaiming ambiguous identities.

## 4. Partition-local fencing semantics

The controller defines which ProducerEpoch is currently issued for one ProducerId. A partition
leader defines which of those issued epochs has become accepted authority on that partition.

For one `(ProducerId, TopicPartition)`:

- an epoch lower than the partition's committed accepted epoch -> `PRODUCER_FENCED`;
- an epoch equal to the partition's accepted epoch -> sequence validation proceeds locally;
- an unseen ProducerId or higher epoch requires fresh controller confirmation before proposal;
- controller confirmation must show that the presented epoch is the controller's current epoch;
- if controller authority cannot be obtained, the unseen/higher epoch is not guessed or accepted;
- once a higher epoch's first batch is quorum-committed on the partition, that committed history
  is the durable partition-local fence against all lower epochs.

This intentionally does **not** claim instantaneous cluster-wide fencing at InitProducer time. An
older epoch already accepted on partition A may continue there until a newer controller-issued
epoch is validated and committed on partition A. Once the newer epoch commits there, the old epoch
cannot append there again.

While a higher-epoch first batch is pending on one leader, that leader must locally withhold lower
epoch proposals for the same ProducerId so the pending transition cannot be interleaved. If the
higher-epoch proposal never commits and leadership changes, recovery returns to committed history;
an uncommitted tentative fence is not durable authority.

## 5. Controller-outage contract

Controller outage alone must not revoke already-valid P4/P7 data-plane authority.

During loss of controller quorum:

- InitProducer is unavailable;
- an idempotent producer whose exact ProducerId/Epoch is already accepted on a partition may
  continue producing there subject to normal partition leadership/quorum availability;
- an unseen ProducerId or higher ProducerEpoch cannot be validated and therefore cannot begin
  authority on that partition;
- an epoch already known locally to be stale remains fenced without controller access.

This preserves I-48's data-plane isolation while keeping producer epoch issuance authoritative.

## 6. Per-partition producer state

For each live producer represented in one partition's derived state:

~~~text
PartitionProducerStateV1
  producer_id
  accepted_epoch
  next_sequence
  recent_committed_batches[<= 5]

RecentBatchV1
  base_sequence
  last_sequence
  record_count
  base_stream_offset
  last_stream_offset
  raft_index
  canonical_digest[32]
~~~

The recent window contains at most the newest **5 committed idempotent batches** for that
ProducerId/partition/epoch. A new accepted epoch clears the previous epoch's sequence window after
the new epoch becomes committed authority.

The canonical digest is SHA-256 over the canonical uncompressed record-set encoding used for
idempotent comparison. It is producer-state metadata, not a security/authentication primitive.
Recovery from ordinary P7 logs may recompute it from committed records. P9 must preserve enough
producer-state metadata to reconstruct this window when physical compaction later removes payload
bytes.

P7-B freezes the digest input bytes exactly as:

~~~text
u32_be record_count
repeat record_count:
    WireRecordV0 canonical body bytes exactly as encoded by Wire V1
~~~

The digest input does **not** include ProducerId, ProducerEpoch, TopicPartition, leader epoch,
base sequence or stream offsets. Those are separate authority/range fields. Record order, explicit
timestamp, key/value nullability and bytes, header order, header names and header values are all
covered by the digest. Two record sets compare equal for retry purposes only when this complete
canonical digest and the frozen producer/range identity both match.

## 7. Sequence admission state machine

Given a validated producer authority and a batch of `record_count > 0`:

~~~text
last_sequence = base_sequence + record_count - 1
~~~

with checked arithmetic.

For the current accepted epoch:

1. `base_sequence == next_sequence`
   - this is the only new-range candidate;
   - if no conflicting pending range exists, it may be proposed;
2. `base_sequence < next_sequence`
   - if the complete sequence range exactly matches a retained committed recent batch **and** the
     canonical digest matches, return the original committed offset range without appending;
   - if the range is known but content differs, return `OUT_OF_ORDER_SEQUENCE`;
   - if the range falls outside the retained window, return `OUT_OF_ORDER_SEQUENCE`; never guess
     whether it was an old success;
3. `base_sequence > next_sequence`
   - return `OUT_OF_ORDER_SEQUENCE`; sequence gaps are never silently accepted.

A partial overlap with an earlier sequence range is always invalid.

For the first accepted batch of a newly validated ProducerEpoch, `base_sequence` must be exactly
`0`.

## 8. Supported retry definition

P7 guarantees duplicate suppression only for a **supported retry**:

- same ProducerId;
- same ProducerEpoch;
- same TopicPartition;
- same base sequence and record count;
- same canonical record bytes/digest;
- original batch is quorum-committed or the retry is reconciling an unresolved local proposal;
- committed batch remains inside the retained five-batch dedupe window when retried.

For such a committed retry, AstraLog returns the original `(base_offset, last_offset)` and creates
zero additional committed logical records for that sequence range.

Retries outside those conditions fail explicitly. P7 never converts an unsupported old retry into
a new append merely because its payload looks plausible.

## 9. Max in-flight / unresolved work

P7 freezes the correctness window to **one unresolved idempotent batch per
`(ProducerId, TopicPartition)`**.

The client must not intentionally pipeline a second sequence range for that producer-partition
until the previous range has a terminal/committed result. The broker independently enforces the
same rule.

An exact duplicate of the one currently pending range may attach to/reuse that pending operation
only when its canonical digest is identical. A conflicting duplicate fails; it never creates a
second proposal.

Different partitions may progress concurrently because each partition owns an independent
sequence stream.

P8 may optimize batching/pipelining only by preserving this P7 contract or by introducing an
explicitly versioned wider in-flight model with equivalent proof.

## 10. Ack-mode contract

Idempotent Produce V1 supports **`acks=quorum` only** in P7.

`acks=0` cannot return authoritative retry information. `acks=leader` may acknowledge a locally
durable but uncommitted suffix that can disappear after failover, which is incompatible with the
P7 guarantee that a successful idempotent result is reconstructable from committed history.

Requests using Produce V1 with another ack mode fail before proposal with the frozen public
invalid-ack/request surface.

Legacy Produce V0 retains all existing P2/P4 ack semantics and is not made idempotent.

## 11. Timestamp rule

Produce V1 idempotent records require explicit timestamps `>= 0`.

The legacy `timestamp_ms = -1` broker-assigned timestamp mode remains valid only for Produce V0 in
P7. P7 forbids it for idempotent Produce because retry identity must not depend on hidden
broker-generated bytes that can differ across attempts.

## 12. Storage V1 activation

P1 already froze the following data-frame fields:

- IDEMPOTENT attribute bit;
- `producer_id: u64`;
- `producer_epoch: u32`;
- `base_sequence: u32`;
- record count, offsets and CRC-protected frame bytes.

P7 activates these existing Storage V1 bytes. **No Storage Format V2 is introduced.**

P7 must preserve exact existing bytes for non-idempotent frames. An idempotent committed batch is
written with the frozen IDEMPOTENT bit set and the exact producer identity/sequence fields.

Only committed frames advance authoritative recovered producer state. Uncommitted surviving tail
bytes may be scanned for storage/Raft recovery but cannot advance P7 dedupe authority.

## 13. P4 application-command versioning

P4 `ApplicationCommand` V1 remains immutable and continues encoding non-idempotent Produce.

P7 introduces `ApplicationCommandV2` for idempotent proposals. Its canonical semantic fields are:

~~~text
version = 2
kind = append idempotent batch
base_stream_offset
producer_id
producer_epoch
base_sequence
record_count
canonical records
~~~

Every replica persists the same producer metadata into the already-frozen P1 frame. V1 and V2
commands may coexist in one Raft history. Decoder/version dispatch must never reinterpret V1 bytes
as V2.

## 14. Producer snapshot

`producer.snapshot` is a **derived acceleration structure**, never competing truth.

ProducerSnapshotV1 is per partition and contains at least:

- magic/version;
- TopicId/PartitionId identity;
- committed Raft index through which the snapshot is valid;
- canonical sorted producer entries;
- accepted epoch and next sequence per entry;
- up to five RecentBatchV1 values per entry;
- CRC32C over canonical bytes.

Snapshot writes use temp-file + file durability + atomic replace + required parent-directory
durability. The complete encoded snapshot has a hard byte bound.

Recovery rules:

1. validate partition identity, version, counts, canonical order and CRC;
2. reject/discard a snapshot whose covered Raft index exceeds the authoritative committed index;
3. restore snapshot state when valid;
4. replay committed idempotent frames after the snapshot boundary;
5. if snapshot is missing, stale or corrupt, rebuild from committed history;
6. never truncate or rewrite committed event history merely to make producer-state recovery pass.

Deleting `producer.snapshot` must not destroy correctness.

## 15. Resource bounds

P7 V1 freezes these correctness/resource bounds:

| Resource | Bound |
|---|---:|
| recent committed batches per producer-partition | 5 |
| unresolved idempotent batches per producer-partition | 1 |
| in-memory producer entries per partition | 4,096 |
| controller ProducerSessionRecord entries | 1,000,000 hard logical cap |
| ProducerSnapshot encoded bytes | 64 MiB |
| InitProducer request producer count | exactly 1 |
| Produce V1 partitions/request | existing Wire-V1 maximum 1,024 |
| records/partition entry | existing Wire-V1 maximum 10,000 |

The controller's existing 64 MiB snapshot envelope is an independent stricter physical bound:
the controller must refuse a producer-session mutation that would make a valid bounded snapshot
impossible even when the logical one-million-record cap has not been reached.

The 4,096-entry partition map is a cache/recovery acceleration bound, not a limit on how many
ProducerIds may ever appear historically. Eviction may discard only reconstructable derived state.
A cache miss rebuilds the required producer state from snapshot/committed history before authority
is granted.

## 16. Controller command/snapshot versioning

Frozen P5 ControllerCommandV1 kinds/bytes and ControllerSnapshotV1 bytes remain immutable.

P7 introduces:

- **ControllerCommandV2** for producer-session initialization/epoch transition;
- **ControllerSnapshotV2** containing all prior authoritative controller fields plus the canonical
  producer-session table.

New controller binaries must continue decoding historical V1 commands and V1 snapshots. Before
P7 feature activation, they must not emit irreversible P7 state. When feature level 3 is active,
new producer-session mutations use V2 authority and snapshots containing P7 state use V2.

Migration from a V1 snapshot creates an empty producer-session table while preserving the exact
P5 `next_unallocated_producer_id`; no previously allocated ProducerId is reused.

## 17. Public wire protocol

Wire V1 remains the connection framing/version. P7 uses the already reserved API/version model.

### 17.1 InitProducer V0 — API key 20

Request body:

~~~text
u64 producer_id
u32 expected_epoch
TaggedFields tags
~~~

Canonical forms:

- `(0, 0)` -> allocate a new ProducerId/session;
- `(nonzero, nonzero)` -> CAS bump existing session;
- exactly one field zero -> `INVALID_REQUEST`.

Response body:

~~~text
u64 producer_id
u32 producer_epoch
TaggedFields tags
~~~

Success is returned only after controller quorum commit/application.

### 17.2 Produce V1 — API key 10 version 1

Request body:

~~~text
u8 acks                         # must be QUORUM
u64 producer_id                 # non-zero
u32 producer_epoch              # non-zero
u32 partition_count
repeat partition_count:
    uuid topic_id
    u32 partition
    u64 leader_epoch
    u32 base_sequence
    u32 record_count
    WireRecordV0 records[record_count]
TaggedFields tags
~~~

The existing WireRecordV0 encoding is reused, with the P7 restriction that timestamps must be
explicit (`>=0`).

ProduceResponse V1 preserves the V0 per-partition response shape: topic/partition, public error,
base/last offset and leader hint. A deduplicated committed retry returns the original offsets.

Produce V0 bytes/semantics remain unchanged.

## 18. Feature activation

Feature key `11 = IDEMPOTENT_PRODUCER` remains the frozen public feature identity.

P7 irreversible behavior requires controller `active_feature_level >= 3`.

Before activation:

- InitProducer key 20 is not advertised/usable;
- Produce advertises only version 0;
- feature 11 is not active;
- P1 writers continue emitting non-idempotent producer fields for ordinary V0 traffic.

After activation, a serving broker advertises InitProducer V0 and Produce V0..V1 only when its
runtime supports the P7 contract. Activation cannot weaken support for legacy Produce V0.

## 19. Public error/retry matrix

P7 reuses the frozen public error registry; no renumbering is allowed.

| Condition | Public error | Client rule |
|---|---|---|
| wrong partition leader / stale leader term | `NOT_LEADER` / `STALE_EPOCH` | refresh metadata; retry same sequence only when result is not known committed |
| broker/controller transient unavailable | `BROKER_NOT_AVAILABLE` / `REQUEST_TIMEOUT` | bounded retry; mutation may be ambiguous only after proposal boundary |
| bounded work saturated | `BROKER_BUSY` | retry with backoff; no sequence advance |
| older producer epoch than accepted authority | `PRODUCER_FENCED` | terminal for that producer session |
| ProducerId absent from controller authority | `UNKNOWN_PRODUCER` | terminal; initialize a new session |
| sequence gap/overlap/content mismatch/too-old retry | `OUT_OF_ORDER_SEQUENCE` | terminal for automatic retry; application must reinitialize producer authority |
| P7 feature inactive | `UNSUPPORTED_FEATURE` / `CLUSTER_FEATURE_TOO_OLD` | do not use idempotent mode |
| non-quorum Produce V1 ack | `INVALID_ACK_MODE` | terminal request error |

The client never advances its local next sequence because a send was attempted. It advances only
after authoritative success for that range, including a deduplicated original-success response.

## 20. Commit-before-response and failure boundaries

For idempotent Produce, success means the sequence range's data batch is P4 quorum-committed.

Required failure behavior:

- leader dies before proposal -> retry may append normally on successor;
- leader dies after local persistence but before quorum -> uncommitted suffix may disappear; retry
  may append normally from the successor's committed producer state;
- leader dies after quorum commit before response -> successor reconstructs the committed recent
  range and exact retry returns original offsets without append;
- response is lost without leader failure -> exact retry on current leader returns/coalesces the
  original result;
- stale leader/old term cannot create committed producer history because ordinary P3/P4 fencing
  remains authoritative.

## 21. Ordering guarantee

For one ProducerId/Epoch/TopicPartition, committed idempotent records follow contiguous accepted
sequence ranges. The sequence state machine cannot skip a range or commit two logical batches for
the same supported sequence range.

There is no global order across partitions. A producer may have independent sequence `0` on many
partitions in the same epoch.

## 22. Recovery and failover

Every new partition leader must derive producer authority from committed state before admitting
idempotent mutations that depend on it.

Recovery must prove:

- committed higher epochs survive leader failover/restart;
- committed `next_sequence` survives restart;
- recent retry results/offsets survive failover;
- an uncommitted higher-epoch tail does not become durable fence authority;
- conflicting uncommitted suffix truncation does not advance producer state;
- a corrupt producer snapshot never overrides valid committed history;
- complete snapshot loss only affects recovery cost, not correctness.

## 23. P9 compaction boundary

P7 implements no retention or physical compaction.

However P7 freezes the producer metadata P9 must preserve: accepted epoch, next sequence, and the
recent committed retry window (including original offsets and canonical digest) for every producer
state still required by the P7 support window.

P9 may change how that metadata is physically retained only if producer-state reconstruction and
P7 retry/fencing proofs remain valid. P7 itself must not pull P9 cleaner/tombstone/segment-rewrite
policy forward.

## 24. Formal/deterministic model obligations

The P7 state model must cover at least:

- new epoch versus old epoch races;
- expected/gap/duplicate/partial-overlap sequence ranges;
- exact duplicate while pending;
- commit then response loss;
- proposal then leader loss before commit;
- higher-epoch uncommitted suffix followed by failover;
- dedupe-window eviction;
- snapshot checkpoint + committed replay;
- controller CAS epoch-bump races.

Model safety properties are I-15, I-16 and I-17 plus I-02/I-05/I-26/I-30/I-45 compatibility.

## 25. Explicit non-goals / later-phase boundaries

P7 does **not** implement:

- cross-partition or consume-transform-produce transactions;
- global exactly-once processing claims;
- transactional producer IDs;
- producer-session TTL/garbage collection;
- batching/compression/performance pipeline changes owned by P8;
- retention/physical compaction owned by P9;
- queue retry/lease/DLQ semantics owned by P10;
- TLS/authn/ACL/authenticated producer tokens owned by P11;
- final chaos/benchmark release claims owned by P12.

## 26. P7 exit contract

P7 may close only when mechanical evidence proves:

1. every supported exact retry creates zero duplicate committed logical records;
2. stale ProducerEpoch cannot append after the newer epoch is committed authority on that
   partition;
3. sequence gaps and partial overlaps are never silently accepted;
4. lost-success retry after real RF3 leader failure returns the original committed offsets;
5. producer state survives restart, snapshot loss/corruption and leader failover;
6. dedupe/controller/runtime state and queues remain bounded;
7. P1 Storage V1 and P2 legacy wire goldens remain byte-compatible;
8. P3/P4/P5/P6 regression suites remain green;
9. no P8-P12 behavior is pulled forward;
10. the exact P7 closure HEAD independently passes the complete qualification gate.
