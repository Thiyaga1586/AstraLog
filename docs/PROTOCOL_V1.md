# AstraLog Wire Protocol V1

Status: **FROZEN — P2 qualified**

Wire protocol version: **1**

This document defines the authoritative AstraLog client/broker wire contract for P2 and the compatibility rules later phases must preserve.

It is deliberately written before networking implementation. P2 networking code must conform to this document rather than inventing bytes ad hoc.

---

## 1. Scope

Wire V1 defines:

- TCP connection negotiation
- persistent multiplexed request/response framing
- exact integer, string, byte-array, UUID, array, and tagged-field encodings
- CRC32C protection for framed messages
- correlation IDs and request deadlines
- API/version negotiation
- public API key assignments
- the P2 API V0 schemas
- public error codes and retry classification
- metadata/leader hints
- bounded-decoder rules
- stable keyed partitioning
- sticky null-key partitioning semantics
- compatibility and evolution rules

P2 implements only:

- ApiVersions
- Metadata
- CreateTopic
- DeleteTopic
- DescribeTopic
- Produce
- Fetch
- ListOffsets

Later API keys are allocated here so their numeric identity cannot drift, but they remain unavailable until their owning phase.

Wire V1 does **not** implement:

- Raft replication RPCs
- controller-Raft RPCs
- consumer-group coordination
- idempotent producer state
- queue leases
- TLS policy/ACLs
- compression execution

Those retain their existing phase ownership.

---

## 2. Transport

### 2.1 TCP

The client protocol runs over a persistent TCP byte stream.

A client may keep one or more connections to a broker. Requests with responses may be simultaneously in flight. Responses may complete out of request order and are matched only by correlation ID.

No V1 server-push messages exist.

### 2.2 TLS

TLS, when enabled in a later phase, wraps the TCP stream before the AstraLog connection preface. The bytes defined by this document are otherwise unchanged.

### 2.3 Byte order

All multi-byte integers in Wire V1 are **big-endian**.

This is independent of Storage Format V1, whose on-disk integers are little-endian.

---

## 3. Connection preface and wire-version negotiation

Version negotiation occurs before any versioned request frame is parsed.

### 3.1 Client hello

The client sends exactly 16 bytes:

| Offset | Size | Field | V1 value / meaning |
|---:|---:|---|---|
| 0 | 8 | magic | ASCII `ASTRALOG` |
| 8 | 2 | hello_version | `1` |
| 10 | 2 | min_wire_version | minimum wire version client can speak |
| 12 | 2 | max_wire_version | maximum wire version client can speak |
| 14 | 2 | flags | `0` in Hello V1 |

Requirements:

- `min_wire_version <= max_wire_version`
- V1 clients send a range containing `1`
- unknown Hello-V1 flag bits are invalid

### 3.2 Server hello

The server returns exactly 16 bytes:

| Offset | Size | Field | Meaning |
|---:|---:|---|---|
| 0 | 8 | magic | ASCII `ASTRALOG` |
| 8 | 2 | hello_version | `1` |
| 10 | 2 | selected_wire_version | selected version, or `0` on failure |
| 12 | 2 | status | Hello status |
| 14 | 2 | flags | `0` in Hello V1 |

Hello status:

| Value | Name |
|---:|---|
| 0 | OK |
| 1 | NO_COMMON_WIRE_VERSION |
| 2 | MALFORMED_HELLO |

The server selects the highest mutually supported wire version.

If no common version exists, it sends `NO_COMMON_WIRE_VERSION` with selected version `0`, then closes the connection.

### 3.3 First framed request

After a successful hello, the first framed request on a connection MUST be `ApiVersions V0`.

Until that request succeeds, no other API is accepted.

This produces two distinct compatibility layers:

1. the fixed connection preface selects the frame grammar;
2. `ApiVersions` selects API-body versions and advertised features.

---

## 4. Wire frame

### 4.1 Hard size limit

`MAX_WIRE_FRAME_LEN_V1 = 67,108,864` bytes (64 MiB).

This value is the maximum value of `frame_len`, excluding the 4-byte length prefix.

A broker or client MAY advertise/configure a lower operational limit. It MUST NOT accept a frame larger than the negotiated operational limit or the V1 hard limit.

The 4-byte length prefix MUST be validated before allocating the full frame.

### 4.2 Layout

Every post-negotiation frame is:

```text
+----------------------+ 4 bytes
| frame_len            | u32 BE
+----------------------+ <- CRC coverage starts
| fixed header         | 40 bytes
+----------------------+
| header tagged fields | tags_len bytes
+----------------------+
| body                 | body_len bytes
+----------------------+
| crc32c               | 4 bytes, u32 BE
+----------------------+ <- CRC coverage ends before trailer
```

`frame_len = header_len + body_len + 4`

`header_len = 40 + tags_len`

Minimum valid `frame_len` is therefore 44.

### 4.3 Fixed header

Offsets below are relative to the first byte after `frame_len`.

| Offset | Size | Field | Meaning |
|---:|---:|---|---|
| 0 | 4 | magic | ASCII `ALV1` |
| 4 | 2 | wire_version | `1` |
| 6 | 2 | header_len | fixed header + header tags |
| 8 | 1 | frame_kind | 0=request, 1=response |
| 9 | 1 | flags | frame flags |
| 10 | 2 | api_key | stable API identifier |
| 12 | 2 | api_version | API-body version |
| 14 | 2 | error_code | response top-level error; zero in requests |
| 16 | 8 | correlation_id | request/response identity |
| 24 | 4 | timeout_ms | request deadline budget; zero in responses |
| 28 | 4 | throttle_ms | response retry delay hint; zero in requests |
| 32 | 4 | body_len | exact body bytes |
| 36 | 4 | tags_len | exact header-tag bytes |

### 4.4 Frame kinds

| Value | Name |
|---:|---|
| 0 | REQUEST |
| 1 | RESPONSE |

Other values are invalid in Wire V1.

### 4.5 Frame flags

| Bit | Name | Meaning |
|---:|---|---|
| 0 | NO_RESPONSE | valid only for Produce with `acks=0` |
| 1-7 | reserved | MUST be zero in V1 |

Response flags MUST be zero in V1.

### 4.6 Correlation IDs

- `0` is reserved for a `NO_RESPONSE` Produce request.
- Every response-bearing request uses a non-zero `u64` correlation ID.
- A correlation ID MUST be unique among currently in-flight response-bearing requests on one connection.
- The response MUST echo the exact request correlation ID.
- A correlation ID may be reused only after its previous request has completed and the response has been fully written or the request has been cancelled by connection close.
- Receiving a duplicate active correlation ID is a protocol violation.

### 4.7 Request timeout

For every response-bearing request:

- `timeout_ms` MUST be in `1..=300_000`
- the broker deadline begins when the complete CRC-valid request frame is admitted for processing
- queueing and execution time count against the deadline
- timeout does not prove whether a mutation executed

For a `NO_RESPONSE` Produce request, `timeout_ms` MAY be zero.

Fetch `max_wait_ms` MUST NOT exceed the request `timeout_ms`.

### 4.8 Throttle hint

`throttle_ms` is zero in P2.

Later quota phases may return a non-zero value. A compliant client treats it as a minimum delay before an automatic retry or additional load for the same throttled scope.

### 4.9 CRC32C

Wire frames use CRC32C/Castagnoli.

CRC input begins at the 4-byte frame magic and ends at the final body byte. The 4-byte length prefix and CRC trailer are excluded.

The stored CRC is big-endian `u32`.

CRC mismatch is connection-fatal because the header/correlation identity can no longer be trusted.

---

## 5. Primitive encodings

### 5.1 Integers

`u8`, `u16`, `u32`, `u64`, `i32`, and `i64` are fixed-width big-endian integers.

### 5.2 Boolean

A boolean is one `u8`:

- `0` = false
- `1` = true

Other values are invalid.

### 5.3 UUID

A UUID is exactly 16 raw bytes in canonical UUID byte order.

It is not sent as text.

### 5.4 `string16`

```text
u16 byte_len
byte[byte_len] UTF-8
```

Rules:

- length counts UTF-8 bytes, not Unicode scalar values
- bytes MUST be valid UTF-8
- `0xFFFF` is reserved for nullable-string null and is invalid for non-null strings

### 5.5 `nullable_string16`

- `0xFFFF` length = null
- otherwise identical to `string16`

### 5.6 `bytes32`

```text
u32 byte_len
byte[byte_len]
```

`0xFFFF_FFFF` is invalid for non-null byte arrays.

### 5.7 `nullable_bytes32`

- `0xFFFF_FFFF` length = null
- otherwise identical to `bytes32`

### 5.8 Arrays

```text
u32 element_count
element[element_count]
```

A decoder MUST validate a count against:

1. the remaining frame bytes;
2. the minimum encoded element width where known;
3. the field-specific element cap;

before allocating a collection.

### 5.9 Tagged fields

Tagged fields provide skippable, semantically optional extensions.

```text
u16 tag_count
repeat tag_count times:
    u16 tag_id
    u32 value_len
    byte[value_len] value
```

Rules:

- tags MUST appear in strictly increasing `tag_id` order
- duplicate tags are invalid
- at most 64 tags may appear in one tagged-field block
- one tag value may not exceed 65,536 bytes
- unknown tags MUST be skipped using `value_len`
- an unknown tag MUST NOT be required to preserve correctness of an older peer
- a new mandatory semantic requires a new API version or wire version, not a supposedly optional tag

Every P2 V0 API body ends with one top-level tagged-field block.

The exception is an error response whose top-level header `error_code` is
non-zero and whose body is explicitly empty as permitted by section 20.1.

### 5.10 Header tags

Header tags use the same tagged-field binary encoding.

Canonical empty-header encoding is `tags_len=0` with no header-tag bytes. If
`tags_len>0`, the encoded block MUST contain `tag_count>=1`. This forbids
two different byte encodings for an empty header-tag set.

Known Wire-V1 header tag IDs:

| Tag | Name | Value |
|---:|---|---|
| 1 | CLIENT_ID | UTF-8 bytes, max 128 bytes |
| 2 | TRACE_ID | exactly 16 opaque bytes |

Both are optional.

Unknown header tags are skipped.

---

## 6. Protocol-wide bounds

These are Wire-V1 hard maxima. Broker/topic configuration may choose smaller limits.

| Item | Hard maximum |
|---|---:|
| frame_len | 64 MiB |
| topic name | 249 bytes |
| client ID | 128 bytes |
| hostname | 255 bytes |
| topics in one request | 1,024 |
| partitions in one request | 1,024 |
| records in one Produce partition entry | 10,000 |
| headers on one record | 64 |
| record-header key | 255 bytes |
| one record key | 1 MiB |
| one record value | 8 MiB |
| tags in one tagged-field block | 64 |
| one tagged-field value | 64 KiB |
| request timeout | 300,000 ms |
| Fetch max_wait_ms | 60,000 ms |

A decoder MUST reject an impossible field before reserving memory proportional to attacker-controlled counts or lengths.

---

## 7. Connection state and bounded decoding

The server connection state machine is:

```text
ACCEPTED
  -> HELLO
  -> API_NEGOTIATION
  -> READY
  -> CLOSED
```

### 7.1 Incremental decoder

The implementation MUST support arbitrary TCP fragmentation:

- a length prefix may arrive one byte at a time
- a header may span reads
- a body may span reads
- multiple complete frames may arrive in one read

No code may assume one socket read equals one frame.

### 7.2 Slow/incomplete frames

Each connection has:

- a bounded receive buffer
- a configured frame-assembly timeout
- a bounded in-flight request count
- a bounded pending-response-byte budget

If a peer begins a frame and fails to complete it within the configured assembly timeout, the server closes the connection.

The assembly timeout is a resource/liveness control and is not a correctness proof.

### 7.3 Invalid framing

The server closes the connection without attempting resynchronization when any of the following occurs:

- invalid hello
- frame length below minimum
- frame length above negotiated/hard maximum
- bad frame magic
- wrong selected wire version
- impossible header/body/tag lengths
- unknown frame kind
- reserved frame flag set
- CRC mismatch
- duplicate active correlation ID

AstraLog does not scan ahead for a plausible next frame after framing corruption.

### 7.4 Valid frame, invalid API body

If the frame header and CRC are trustworthy but the API body is invalid, the server returns `MALFORMED_REQUEST` using the request correlation ID.

For `NO_RESPONSE` Produce, malformed body closes the connection because no response correlation exists.

### 7.5 Backpressure

When per-connection in-flight capacity is reached, the broker MUST either:

- stop reading additional request frames until capacity returns; or
- return `BROKER_BUSY` for a fully decoded request when response-budget capacity exists.

It MUST NOT create an unbounded request queue.

---

## 8. API key registry

API key numbers are permanent once assigned.

| Key | API | First owning phase | P2 availability |
|---:|---|---|---|
| 0 | ApiVersions | P2 | implemented |
| 1 | DescribeCluster | P5 | unavailable |
| 2 | Metadata | P2 | implemented |
| 3 | CreateTopic | P2 | implemented |
| 4 | DeleteTopic | P2 | implemented |
| 5 | DescribeTopic | P2 | implemented |
| 6 | AlterTopicConfig | P5/P9/P11 | unavailable |
| 7 | ReassignPartitions | P5 | unavailable |
| 8 | DrainBroker | P11 | unavailable |
| 10 | Produce | P2 | implemented |
| 11 | Fetch | P2 | implemented |
| 12 | ListOffsets | P2 | implemented |
| 20 | InitProducer | P7 | unavailable |
| 30 | JoinSubscribe | P6 | unavailable |
| 31 | Heartbeat | P6 | unavailable |
| 32 | RevokeAck | P6 | unavailable |
| 33 | Leave | P6 | unavailable |
| 34 | CommitOffsets | P6 | unavailable |
| 35 | FetchOffsets | P6 | unavailable |
| 40 | QueueReceive | P10 | unavailable |
| 41 | QueueAck | P10 | unavailable |
| 42 | QueueNack | P10 | unavailable |
| 43 | ExtendVisibility | P10 | unavailable |

Keys 9, 13-19, 21-29, 36-39, and 44-255 are reserved for AstraLog.

An allocated but unavailable API returns `UNSUPPORTED_API_KEY` in P2.

---

## 9. API version negotiation

Each API evolves independently from the connection wire version.

P2 implements API version `0` for every P2 API.

The request header carries the selected `api_version`.

If an API exists but the requested API version is unsupported, the broker returns `UNSUPPORTED_API_VERSION`.

Clients MUST use the intersection of their supported range and the broker's advertised range.

---

## 10. ApiVersions V0

### 10.1 Request body

```text
string16 client_software_name
string16 client_software_version
TaggedFields tags
```

### 10.2 Response body

```text
string16 server_software_name
string16 server_software_version
u16 selected_wire_version
u32 max_frame_len

u16 api_count
repeat api_count:
    u16 api_key
    u16 min_api_version
    u16 max_api_version

u16 feature_count
repeat feature_count:
    u16 feature_key
    u16 min_supported_version
    u16 max_supported_version
    u16 active_version      # 0xFFFF means supported but not active

TaggedFields tags
```

P2 feature keys:

| Key | Feature |
|---:|---|
| 1 | BASIC_STREAMING |
| 2 | LONG_POLL_FETCH |
| 3 | STICKY_NULL_KEY_PARTITIONING |
| 10 | MULTI_RAFT_REPLICATION |
| 11 | IDEMPOTENT_PRODUCER |
| 12 | CONSUMER_GROUPS |
| 13 | WORK_QUEUES |
| 14 | BATCH_COMPRESSION |
| 15 | RETENTION_COMPACTION |

P2 advertises features 1-3 active at version 1. Future features may be advertised as supported only when implementation exists; a feature is not considered usable unless `active_version != 0xFFFF`.

`ApiVersions` lists only APIs actually usable by the serving broker. Merely
having a permanent key allocated in section 8 does not cause an unavailable
future API to be advertised.

P9 activation is additive and preserves every previously published body:

- feature key `15 = RETENTION_COMPACTION` is supported at version 1;
- its `active_version` becomes 1 only after committed cluster feature level 5 is visible at the broker;
- before that readiness boundary CreateTopic/DescribeTopic advertise at most their already-qualified
  P8 V1 bodies and AlterTopicConfig remains absent from ApiVersions;
- after readiness CreateTopic/DescribeTopic may advertise V2 and AlterTopicConfig may advertise V0;
- codec registration alone never implies runtime readiness.

---

## 11. Metadata V0

### 11.1 Request body

```text
bool all_topics
u32 topic_name_count
repeat topic_name_count:
    string16 topic_name
TaggedFields tags
```

Rules:

- when `all_topics=true`, `topic_name_count` MUST be zero
- when `all_topics=false`, at least one topic name is required

### 11.2 Response body

```text
uuid cluster_id
u64 metadata_epoch

u32 broker_count
repeat broker_count:
    u32 broker_id
    uuid node_uuid
    u64 incarnation_epoch
    string16 host
    u16 port

u32 topic_count
repeat topic_count:
    u16 topic_error
    string16 topic_name
    uuid topic_id
    u32 partition_count
    u16 replication_factor

    u32 partition_metadata_count
    repeat partition_metadata_count:
        u32 partition
        u16 partition_error
        u32 leader_broker_id       # 0xFFFF_FFFF if unknown
        u64 leader_epoch           # 0xFFFF_FFFF_FFFF_FFFF if unknown
        u16 replica_count
        repeat replica_count:
            u32 broker_id

TaggedFields tags
```

In P2:

- one broker is present
- replication factor MUST be 1
- each partition leader is the local broker

`metadata_epoch` is a monotonically increasing version of authoritative
cluster/topic metadata. P2 maintains it in the single-broker catalog; P5 later
derives the authoritative value from controller-committed metadata without
changing this wire field.

The schema intentionally already carries leader and epoch information so client metadata refresh behavior remains valid when P4/P5 introduce real leader movement.

---

## 12. Topic-name rules

P2 topic names:

- contain 1..=249 ASCII bytes
- allowed bytes are `A-Z a-z 0-9 . _ -`
- may not be `.` or `..`
- names beginning with `__astralog_` are reserved for internal topics

Names are case-sensitive.

---

## 13. CreateTopic V0

### 13.1 Request body

```text
string16 topic_name
u32 partition_count
u16 replication_factor
TaggedFields tags
```

P2 rules:

- `partition_count >= 1`
- `replication_factor == 1`
- topic name obeys section 12

### 13.2 Response body

```text
uuid topic_id
u64 metadata_epoch
TaggedFields tags
```

If the topic already exists, the response header error is `TOPIC_ALREADY_EXISTS` and the body is empty.

### 13.3 P9 additive CreateTopic V2

P9 leaves CreateTopic V0/V1 byte-exact and adds V2 only after committed feature level 5.

Request body:

```text
string16 topic_name
u32 partition_count
u16 replication_factor
TopicPerformanceConfigV1 performance
TopicRetentionConfigV1 retention
TaggedFields tags
```

Response body:

```text
uuid topic_id
u64 metadata_epoch
TopicPerformanceConfigV1 performance
TopicRetentionConfigV1 retention
TaggedFields tags
```

The performance substructure is the already-qualified P8 V1 policy. The retention substructure is:

```text
u8 cleanup_policy          # 0=delete, 1=compact, 2=compact+delete
u8 option_flags            # bit0=retention_ms present, bit1=retention_bytes present
u16 reserved               # 0
u64 retention_ms           # 0 when absent
u64 retention_bytes        # 0 when absent
u64 tombstone_grace_ms     # positive
```

Compact-only rejects delete-retention limits. Historical/default P9 policy is delete with both limits
absent and tombstone grace `86_400_000` ms. Subscription-protection state is intentionally not exposed
by the P9 public body.

---

## 14. DeleteTopic V0

### 14.1 Request body

```text
string16 topic_name
TaggedFields tags
```

### 14.2 Response body

```text
uuid deleted_topic_id
u64 metadata_epoch
TaggedFields tags
```

After successful deletion becomes authoritative, new requests using the deleted topic UUID fail with `UNKNOWN_TOPIC`.

---

## 15. DescribeTopic V0

### 15.1 Request body

```text
string16 topic_name
TaggedFields tags
```

### 15.2 Response body

```text
string16 topic_name
uuid topic_id
u32 partition_count
u16 replication_factor
u64 metadata_epoch
TaggedFields tags
```

### 15.3 P9 additive DescribeTopic V2

DescribeTopic V2 retains the V0/V1 request shape and returns both committed controller-owned policies:

```text
string16 topic_name
uuid topic_id
u32 partition_count
u16 replication_factor
u64 metadata_epoch
TopicPerformanceConfigV1 performance
TopicRetentionConfigV1 retention
TaggedFields tags
```

V2 is advertised/accepted only after committed feature level 5.

### 15.4 P9 AlterTopicConfig V0

P9 activates permanent API key 6 version 0 only for retention/compaction policy fields. No P11
security/operations configuration is pulled forward.

Request body:

```text
string16 topic_name
u16 present_flags          # bit0 cleanup, bit1 retention_ms, bit2 retention_bytes, bit3 tombstone_grace
u16 clear_flags            # only bits1/2; must be a subset of present_flags
u8 cleanup_policy          # 0 when absent; otherwise 0/1/2
u8 reserved_0              # 0
u16 reserved_1             # 0
u64 retention_ms           # 0 when absent/cleared
u64 retention_bytes        # 0 when absent/cleared
u64 tombstone_grace_ms     # 0 when absent; positive when present
TaggedFields tags
```

An empty patch is invalid. Omitted fields preserve committed current state; clear bits remove the
corresponding optional delete-retention limit. The controller validates the resulting complete policy
atomically against the current topic lifecycle/policy.

Response body:

```text
uuid topic_id
u64 metadata_epoch
TopicRetentionConfigV1 retention
TaggedFields tags
```

---

## 16. Wire record V0

Produce and Fetch do not expose Storage Format V1 bytes directly.

The public wire record is:

```text
i64 timestamp_ms
nullable_bytes32 key
nullable_bytes32 value
u16 header_count
repeat header_count:
    string16 header_key
    nullable_bytes32 header_value
```

Timestamp:

- `-1` asks the broker to assign its append timestamp
- values `>= 0` are explicit Unix epoch milliseconds
- values `< -1` are invalid

A null value is a tombstone-capable record representation. Whether cleanup policy later compacts it is owned by P9.

---

## 17. Produce V0

### 17.1 Ack mode

| Value | Name | Meaning |
|---:|---|---|
| 0 | NONE | no response; no delivery guarantee |
| 1 | LEADER | local durable append before success |
| 2 | QUORUM | Raft majority commit before success |

In P2, RF=1, so QUORUM is a one-replica quorum and means local durable commit. It does not imply machine-failure tolerance.

P2 does not implement Raft early. For the single RF=1 broker, the one-replica
commit condition is realized by the already-qualified P1 local durable
boundary. P3/P4 later replace this single-node mechanism with the real Raft
commit path while preserving the public ack semantics.

### 17.2 Request body

```text
u8 acks
u32 partition_entry_count
repeat partition_entry_count:
    uuid topic_id
    u32 partition
    u64 leader_epoch      # all-ones means unknown
    u32 record_count
    repeat record_count:
        WireRecordV0 record
TaggedFields tags
```

Rules:

- one request may contain at most 1,024 partition entries
- one partition entry may contain at most 10,000 records
- duplicate `(topic_id, partition)` entries in one request are invalid
- if a non-all-ones leader epoch is stale, return `STALE_EPOCH`

### 17.3 `acks=0`

For `acks=0`:

- frame flag `NO_RESPONSE` MUST be set
- correlation ID MUST be zero
- the broker sends no Produce response
- request loss, server-side rejection, or storage failure cannot be observed by that client request

The client MUST NOT infer success from the TCP connection remaining open.

### 17.4 Response body

No response exists for `acks=0`.

For `acks=leader` or `acks=quorum`:

```text
u32 partition_result_count
repeat partition_result_count:
    uuid topic_id
    u32 partition
    u16 error_code
    u64 base_offset       # all-ones when no offsets were assigned
    u64 last_offset       # all-ones when no offsets were assigned
    u32 leader_broker_id  # all-ones if unknown
    u64 leader_epoch      # all-ones if unknown
TaggedFields tags
```

### 17.5 Retry ambiguity

P2 Produce is not idempotent.

If a Produce response is lost or a `REQUEST_TIMEOUT` occurs after the broker may have appended the records, retrying can create duplicates.

This ambiguity is intentional and must be documented by the client API. P7 removes supported retry duplicates through producer IDs/epochs/sequences; P2 must not pretend those semantics exist early.

---

## 18. Fetch V0

### 18.1 Isolation

Wire V0 defines:

| Value | Name |
|---:|---|
| 0 | READ_COMMITTED |

Other values return `INVALID_ISOLATION_LEVEL`.

### 18.2 Request body

```text
u8 isolation
u32 min_bytes
u32 max_wait_ms
u32 partition_entry_count
repeat partition_entry_count:
    uuid topic_id
    u32 partition
    u64 leader_epoch       # all-ones means unknown
    u64 fetch_offset
    u32 max_bytes
    u32 max_records
TaggedFields tags
```

Rules:

- `max_wait_ms <= 60_000`
- `max_wait_ms <= request timeout_ms`
- each `max_bytes > 0`
- each `max_records > 0`
- total encoded response never exceeds the negotiated frame maximum

### 18.3 Long-poll behavior

The broker returns when the first of these occurs:

1. at least `min_bytes` of eligible encoded record data is available;
2. an error makes immediate return appropriate;
3. `max_wait_ms` expires;
4. the request deadline expires.

`min_bytes=0` requests an immediate response.

Long polling never exposes bytes above the committed high watermark.

### 18.4 Response record

```text
u64 stream_offset
WireRecordV0 record
```

### 18.5 Response body

```text
u32 partition_result_count
repeat partition_result_count:
    uuid topic_id
    u32 partition
    u16 error_code
    u32 leader_broker_id
    u64 leader_epoch
    u64 log_start_offset
    u64 high_watermark
    u64 next_offset
    u32 record_count
    repeat record_count:
        FetchRecordV0 record
TaggedFields tags
```

`high_watermark` is the next stream offset after the highest committed visible record boundary for normal consumption.

`next_offset` is the offset from which a client should continue after the returned records. If no records are returned, it equals the request's fetch offset unless a documented reset/error policy says otherwise.

---

## 19. ListOffsets V0

### 19.1 Timestamp selector

| Value | Meaning |
|---:|---|
| -2 | earliest available stream offset |
| -1 | latest committed next offset |
| >= 0 | first record whose timestamp is at least this Unix-ms timestamp |

Other negative values are invalid.

### 19.2 Request body

```text
u32 query_count
repeat query_count:
    uuid topic_id
    u32 partition
    u64 leader_epoch
    i64 timestamp_selector
TaggedFields tags
```

### 19.3 Response body

```text
u32 result_count
repeat result_count:
    uuid topic_id
    u32 partition
    u16 error_code
    u32 leader_broker_id
    u64 leader_epoch
    u64 offset            # all-ones when no matching offset exists
    i64 timestamp_ms      # -1 when not applicable/not found
TaggedFields tags
```

---

## 20. Public error taxonomy

Error codes are `u16` and permanent once assigned.

Retry class:

- **R** = the client may retry the logical operation with a bounded retry budget, following the stated action
- **T** = do not automatically retry the same logical operation unchanged

An R classification does not mean the previous attempt definitely failed. Mutation outcomes may be ambiguous.

| Code | Name | Class | Required client interpretation |
|---:|---|:---:|---|
| 0 | NONE | - | success |
| 1 | UNKNOWN_SERVER_ERROR | R | bounded backoff; mutation outcome may be ambiguous |
| 2 | MALFORMED_REQUEST | T | fix encoder/request |
| 3 | FRAME_TOO_LARGE | T | reduce request below negotiated limit |
| 4 | UNSUPPORTED_WIRE_VERSION | T | reconnect using mutually supported wire version |
| 5 | UNSUPPORTED_API_KEY | T | API unavailable on this broker/phase |
| 6 | UNSUPPORTED_API_VERSION | T | renegotiate ApiVersions and choose supported version |
| 7 | REQUEST_TIMEOUT | R | bounded retry; mutation outcome may be ambiguous |
| 8 | BROKER_BUSY | R | back off using throttle hint |
| 9 | QUOTA_EXCEEDED | R | wait at least throttle hint |
| 10 | NOT_LEADER | R | refresh metadata and route to returned/current leader |
| 11 | STALE_EPOCH | R | refresh metadata/authority epoch before retry |
| 12 | BROKER_NOT_AVAILABLE | R | refresh metadata and retry another eligible endpoint |
| 13 | UNKNOWN_TOPIC | T | refresh/app must establish topic existence |
| 14 | UNKNOWN_PARTITION | T | refresh metadata; request target is invalid |
| 15 | TOPIC_ALREADY_EXISTS | T | do not repeat create unchanged |
| 16 | INVALID_TOPIC | T | fix topic name/configuration |
| 17 | INVALID_REQUEST | T | fix request semantics |
| 18 | MESSAGE_TOO_LARGE | T | reduce record/batch size |
| 19 | OFFSET_OUT_OF_RANGE | T | application/client offset-reset policy required |
| 20 | CORRUPT_MESSAGE | T | do not automatically resend identical corrupt payload |
| 21 | STORAGE_ERROR | R | bounded backoff/metadata refresh; no success is implied |
| 22 | NOT_ENOUGH_REPLICAS | R | wait for replica availability |
| 23 | AUTHENTICATION_FAILED | T | fix credentials/identity |
| 24 | AUTHORIZATION_FAILED | T | obtain required permission |
| 25 | OUT_OF_ORDER_SEQUENCE | T | producer state recovery required |
| 26 | PRODUCER_FENCED | T | old producer epoch cannot continue |
| 27 | UNKNOWN_PRODUCER | T | producer session reinitialization required |
| 28 | GROUP_EPOCH_STALE | R | rejoin/refresh group authority before retry |
| 29 | UNKNOWN_MEMBER | R | rejoin group |
| 30 | REBALANCE_IN_PROGRESS | R | complete cooperative rebalance then retry |
| 31 | INVALID_OFFSET_COMMIT | T | fix commit target/state |
| 32 | QUEUE_LEASE_STALE | T | receipt no longer owns delivery |
| 33 | QUEUE_LEASE_EXPIRED | T | message must be received again |
| 34 | UNSUPPORTED_FEATURE | T | negotiate/enable supported feature |
| 35 | CLUSTER_FEATURE_TOO_OLD | T | cluster feature level must advance first |
| 36 | DUPLICATE_CORRELATION_ID | T | connection/protocol bug; connection closes |
| 37 | INVALID_ACK_MODE | T | choose defined ack mode |
| 38 | INVALID_ISOLATION_LEVEL | T | choose supported isolation |
| 39 | TOPIC_DELETING | R | wait for deletion state to settle |
| 40 | PARTITION_COUNT_DECREASE_NOT_ALLOWED | T | v1 partitions may not shrink |
| 41 | INVALID_REPLICATION_FACTOR | T | choose supported RF |

Codes 42-255 are reserved for AstraLog V1 evolution.

### 20.1 Top-level versus per-item errors

The frame header `error_code` describes failure of the whole request envelope/API execution.

For multi-topic or multi-partition APIs, item-level errors are carried inside the response body.

A top-level non-zero error MAY have an empty body.

### 20.2 No success on ambiguous durable error

If storage reports an error and the broker cannot prove the requested durability condition was met, the broker MUST NOT return success.

This preserves invariant I-30.

---

## 21. Leader and epoch hints

Partition responses that can encounter stale routing include:

- leader broker ID
- leader epoch

Unknown values use the all-ones sentinel for their unsigned width.

On `NOT_LEADER` or `STALE_EPOCH`, a client:

1. invalidates the affected cached routing entry;
2. refreshes Metadata;
3. retries only if the error class and request semantics permit it.

P2 exercises this path even though all partitions are local, so later P4 leader movement does not require redesigning the client.

---

## 22. Keyed partitioning

The keyed partition function is protocol-defined:

```text
h = murmur3_x86_32(key_bytes, seed=0)
positive = h & 0x7FFF_FFFF
partition = positive % partition_count
```

`partition_count` MUST be positive.

The key bytes are the exact record-key bytes. No UTF-8 normalization, case folding, locale conversion, or string re-encoding occurs inside the partitioner.

### 22.1 MurmurHash3 x86 32-bit definition

Constants:

```text
c1 = 0xCC9E2D51
c2 = 0x1B873593
```

For each complete 4-byte block interpreted little-endian:

```text
k *= c1
k = ROTL32(k, 15)
k *= c2
h ^= k
h = ROTL32(h, 13)
h = h * 5 + 0xE6546B64
```

Tail bytes are accumulated little-endian into `k`, then use the same `c1/ROTL15/c2` mix before XOR into `h`.

Finalization:

```text
h ^= byte_length
h ^= h >> 16
h *= 0x85EBCA6B
h ^= h >> 13
h *= 0xC2B2AE35
h ^= h >> 16
```

All arithmetic is wrapping unsigned 32-bit arithmetic.

### 22.2 Compatibility vectors

| Key bytes | hash u32 | positive | p=3 | p=7 | p=16 |
|---|---:|---:|---:|---:|---:|
| empty | `0x00000000` | 0 | 0 | 0 | 0 |
| ASCII `foo` | `0xF6A5C420` | 1990575136 | 1 | 4 | 0 |
| ASCII `hello` | `0x248BFA47` | 613153351 | 1 | 6 | 7 |
| ASCII `key` | `0xE29C6A44` | 1654417988 | 2 | 6 | 4 |
| ASCII `orders-123` | `0x93E86727` | 333997863 | 0 | 3 | 7 |
| bytes `00 01 ... 0F` | `0x191573DD` | 420836317 | 1 | 6 | 13 |
| UTF-8 bytes for `π` (`CF 80`) | `0xF1954DF6` | 1905610230 | 0 | 6 | 6 |

These vectors become compatibility-golden tests in P2.

The client MUST NOT use Rust's `Hash`, `DefaultHasher`, randomized hash seeds, or another language runtime's native hash.

---

## 23. Null-key sticky partitioning

When a record key is null, the client uses a sticky partition rather than hashing.

Normative behavior:

1. maintain sticky state per `(producer instance, topic_id)`;
2. choose uniformly from currently writable partitions when no sticky partition exists;
3. keep the chosen partition while accumulating the current producer batch;
4. rotate after that batch is closed for send;
5. when more than one writable partition exists, the next choice SHOULD avoid the immediately previous partition;
6. if metadata marks the sticky partition unavailable, invalidate it immediately and choose again.

The random-number-generator algorithm is not part of wire compatibility.

Tests MUST use an injected deterministic RNG.

Null-key sticky behavior provides batching/load distribution, not a key-ordering guarantee.

---

## 24. Ordering implications

For keyed records, all clients implementing Wire V1 and using the same partition count produce the same partition choice.

Increasing a topic's partition count can change the result of `hash % partition_count`.

Therefore key ordering is guaranteed within a fixed partitioning epoch, not transparently across partition-count expansion.

V1 does not support partition-count decrease.

---

## 25. Compatibility rules

Once P2 closes, the following are immutable for Wire V1:

- Hello V1 bytes
- Wire-frame fixed-header bytes and field meanings
- CRC coverage
- primitive encodings
- tagged-field grammar
- API key numbers
- public error-code numbers
- each published `(api_key, api_version)` body schema and semantics
- Murmur3 keyed partition function

### 25.1 Allowed evolution without Wire V2

Later phases may:

- add a previously allocated API implementation
- allocate a new unused API key
- add a new API version for an existing API
- add semantically optional tagged fields
- advertise a newly supported feature

They may not reinterpret existing bytes.

### 25.2 When a new API version is required

A new API version is required when changing:

- mandatory fields
- field type/width
- request/response semantics
- an enum meaning
- error behavior that changes correctness expectations

### 25.3 When Wire V2 is required

A new wire version is required when changing:

- connection preface grammar
- frame fixed-header grammar
- primitive encoding rules
- CRC framing/coverage
- rules needed to parse a frame before its API body is known

### 25.4 Feature-level gating

Later cluster feature levels MUST prevent a node from emitting behavior unavailable to the active cluster level.

Advertising support is not permission to activate an irreversible feature.

---

## 26. P2 implementation obligations derived from this spec

Before P2 can close, implementation must prove:

- Hello wire-version negotiation
- ApiVersions API-version negotiation
- byte-exact protocol golden vectors
- CRC32C detection
- arbitrary TCP fragmentation handling
- multiple concurrent correlation IDs
- out-of-order response matching
- oversized-frame rejection before allocation
- malformed count/length rejection before allocation
- slowloris/incomplete-frame eviction
- bounded receive/in-flight/response memory
- no frame resynchronization guessing
- every public error code has the retry class in section 20
- key partition vectors match section 22.2
- sticky partitioner uses injected deterministic RNG in tests
- single-broker Produce/Fetch/ListOffsets survive durable restart
- default Fetch never returns above the committed high watermark
- lost-response ambiguity is documented for non-idempotent P2 Produce
- metadata refresh path exists before multi-node leadership is introduced

---

## 27. P2 implementation order

After this specification is reviewed/frozen, the implementation order is:

1. `astralog-protocol` primitive codec and CRC
2. Hello + frame decoder/encoder
3. protocol golden vectors and malformed/fuzz tests
4. API V0 body codecs
5. Murmur3 partitioner compatibility tests
6. bounded TCP connection state machine
7. single-broker request dispatcher
8. P2 topic metadata/catalog
9. Produce -> P1 storage integration
10. Fetch/ListOffsets -> P1 storage integration
11. Rust client metadata cache + multiplexed connections
12. sticky null-key partitioner
13. long-poll Fetch
14. durable restart integration suite
15. P2 qualification and closure report

No Raft or later-phase state machine is pulled forward by this order.
