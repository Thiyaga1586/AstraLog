# AstraLog P6 Consumer Groups and Offsets Contract V1

Status: **FROZEN FOR P6 IMPLEMENTATION**

This document freezes the P6 contract for consumer offsets and cooperative consumer groups. It extends the closed P0-P5 system without redefining P1 storage bytes, P2 frame/header semantics, P3 Raft safety, P4 acknowledgement/read guarantees, or P5 controller authority.

## 1. Ownership boundary

P6 owns stream-consumer coordination only:

- durable consumer-group membership and subscription state;
- group/member/session epochs;
- deterministic group-to-coordinator routing;
- uniform-sticky partition assignment;
- cooperative/incremental revocation and activation;
- member heartbeats and session expiry;
- durable committed consumer offsets;
- fencing stale group/member offset commits;
- public API V0 bodies for already-reserved API keys 30 through 35.

P6 does **not** own partition leadership, replica membership, cluster placement, producer idempotency, work-queue leases, physical log compaction, authentication, broker drain, or retention policy.

P3 remains the only owner of Raft election/commit/membership safety. P4 remains the owner of replicated partition log authority. P5 remains the owner of broker/topic/replica metadata and internal-topic placement. P6 coordinators may route to and write normal replicated internal-topic partitions; they may not bypass P3/P4 durability ordering.

## 2. Frozen correctness invariants

P6 is not complete unless all four phase-exit invariants hold:

1. **I-18 Group ownership uniqueness:** one `(topic_id, partition)` has at most one active owner in authoritative group state.
2. **I-19 Cooperative handoff:** movement is `old active -> released -> new active`; old and new owners are never simultaneously active.
3. **I-20 Stale offset fencing:** a stale group/member authority cannot overwrite offsets after group authority has advanced.
4. **Coordinator failover durability:** committed group and offset state survives coordinator leader failure/restart without reconstructing authority from client claims.

Timeouts drive liveness only. Safety comes from committed state, epochs, owner fences, and P3/P4 commit ordering.

## 3. Checked identities and canonical order

P6 V1 uses:

```text
GroupId           UTF-8 string, 1..=255 bytes, byte-exact/case-sensitive, NUL forbidden
MemberId          exactly 16 bytes, all-zero reserved as JOIN allocation sentinel
InstanceId        optional UTF-8 string, 1..=255 bytes when present, NUL forbidden
GroupEpoch        u64, zero reserved for external/member authority, checked increment
MemberEpoch       u64, zero reserved, checked increment
GroupStateVersion u64, zero reserved, checked increment
TopicPartition    (TopicId[16], partition u32)
```

Canonical ordering:

- GroupId by raw UTF-8 bytes;
- MemberId by 16-byte unsigned lexicographic order;
- TopicPartition by `(topic_id bytes, partition)`;
- subscriptions and assignment/revocation sets are sorted, duplicate-free canonical sets.

Any integer overflow, duplicate canonical key, non-canonical order, zero reserved epoch, invalid UTF-8, trailing bytes, or bound violation fails before state mutation.

## 4. Internal replicated topics

P6 uses exactly two reserved internal topics:

```text
__astralog_groups
__astralog_offsets
```

Both topics are ordinary P4/P5 replicated partitions for durability. They are not special consensus systems and do not introduce another database.

P6 V1 topology is frozen as:

```text
partition_count = 64
replication_factor = 3
failure_domain_policy = Preferred
```

Rules:

- P6 serving cannot activate until both topics exist and exactly match the frozen topology.
- creation/validation is routed through P5 controller authority;
- public CreateTopic/DeleteTopic/ExpandTopic cannot create, delete, rename, or expand these names;
- partition count is immutable for P6 V1 because changing it would remap `group_id -> coordinator shard`;
- a later migration may change shard count only through a new explicitly versioned protocol/feature transition;
- `Preferred` may run degraded when topology cannot provide full rack/zone diversity, but degradation is explicit in P5 placement evidence;
- both topics use the same partition count so one group maps to the same numeric shard in each topic.

The architecture calls `__astralog_offsets` compacted. P6 freezes **logical latest-value semantics only**. P9 owns physical segment compaction, retention, tombstone cleanup, and copy-on-write cleanup policy. P6 must not pull those mechanisms forward.

## 5. Coordinator routing

The group shard is:

```text
shard = partition_for_key(group_id UTF-8 bytes, 64)
```

where `partition_for_key` is the already-frozen P2 Murmur3 x86 32 implementation with seed `0`, positive-mask `hash & 0x7FFF_FFFF`, then modulo partition count.

The **group coordinator** is the currently observed P3 leader of `(__astralog_groups, shard)`. Coordinator epoch is that P3 leader term.

Rules:

- API keys 30..35 are externally routed to the group coordinator;
- a non-coordinator never mutates group or offset state on behalf of the client;
- `NOT_LEADER`, `STALE_EPOCH`, `BROKER_NOT_AVAILABLE`, and `REQUEST_TIMEOUT` remain the existing public retry surfaces;
- responses carry coordinator broker/term hints when known;
- clients refresh the coordinator and retry only where the error taxonomy permits;
- group safety never depends on synchronized wall clocks or controller quorum being on the request hot path after internal-topic metadata is known.

The `__astralog_offsets` shard may have a different P3 leader from the group shard. The group coordinator orchestrates the internal offset write/read and returns success only after the offsets shard has committed/applied the required record(s).

## 6. Durable group state

Each group has one authoritative logical `GroupRecordV1` represented by the latest committed canonical record for its GroupId in `__astralog_groups`.

The logical state contains at least:

```text
GroupRecordV1
  group_id
  state_version
  active_group_epoch
  pending_group_epoch optional<u64>
  phase
  members: MemberId -> MemberRecordV1
  active_assignments: TopicPartition -> MemberId
  target_assignments: TopicPartition -> MemberId
  pending_revocations: TopicPartition -> MemberId
```

MemberRecordV1 contains:

```text
member_id
instance_id optional<string>
member_epoch
session_timeout_ms
subscriptions set<TopicId>
```

Heartbeat receive time is **not durable authority**. Session timeout configuration is durable; current receive timestamps/deadlines are coordinator-leader-local observations.

`active_group_epoch=0` is permitted only inside a durable `PREPARING_EPOCH_FENCE` record for a group that has never activated epoch 1. It is an internal bootstrap sentinel, never valid in a member authority tuple. The first externally authoritative GroupEpoch is exactly 1.

`GroupStateVersion` advances exactly once per committed logical group-state transition. A replayed record with an already-applied version is accepted only if canonical bytes match the previously applied version; a conflicting duplicate or a version gap is corruption and fails closed.

Group records are versioned internal application records carried inside ordinary P1/P4 record payloads. P6 does not change P1 frame bytes.

## 6.1 Frozen internal record encoding

P6 internal state payloads use little-endian deterministic encoding. They are application payloads inside the already-frozen P1/P4 record/log machinery and therefore do not alter P1 frame bytes.

Every internal value starts with:

```text
u16 version = 1
u16 record_kind
u32 body_len
body bytes
```

`body_len` must exactly consume the remaining value bytes. No trailing bytes or unknown V1 kind are accepted.

Frozen record kinds:

```text
1 = GROUP_STATE
2 = OFFSET_GROUP_FENCE
3 = OFFSET_OWNER_FENCE
4 = OFFSET_VALUE
```

`GROUP_STATE` body field order:

```text
string16 group_id
u64 state_version
u64 active_group_epoch
optional<u64> pending_group_epoch
u8 phase
u32 member_count
repeat members ascending MemberId:
  uuid member_id
  optional<string16> instance_id
  u64 member_epoch
  u32 session_timeout_ms
  u32 subscription_count
  repeat subscription_count: uuid topic_id   # ascending
u32 active_assignment_count
repeat active assignments ascending TopicPartition:
  uuid topic_id
  u32 partition
  uuid member_id
u32 target_assignment_count
repeat target assignments ascending TopicPartition:
  uuid topic_id
  u32 partition
  uuid member_id
u32 pending_revocation_count
repeat pending revocations ascending TopicPartition:
  uuid topic_id
  u32 partition
  uuid old_member_id
```

`OFFSET_GROUP_FENCE` body field order:

```text
string16 group_id
u64 group_epoch
u64 source_group_state_version
u32 owner_count
repeat owners ascending TopicPartition:
  uuid topic_id
  u32 partition
  uuid member_id
  u64 member_epoch
```

A new group fence must advance GroupEpoch strictly, except the first fence which establishes epoch 1 from no prior fence. A duplicate same-epoch fence is idempotent only when its complete canonical bytes match; a conflicting same-epoch fence is fatal state divergence.

`OFFSET_OWNER_FENCE` body field order:

```text
string16 group_id
uuid topic_id
u32 partition
u64 group_epoch
uuid member_id
u64 member_epoch
u64 source_group_state_version
```

An owner fence is valid only for the current durable group-fence epoch and for a group transition that already released that TopicPartition. Duplicate canonical owner fences are idempotent; conflicting owner authority for the same handoff fails closed.

`OFFSET_VALUE` body field order:

```text
string16 group_id
uuid topic_id
u32 partition
u64 next_offset
string16 metadata        # additionally capped at 4096 UTF-8 bytes
i64 commit_timestamp_ms
u64 group_epoch
uuid member_id
u64 member_epoch
```

Canonical P1 record keys are:

```text
__astralog_groups GROUP_STATE:
  raw UTF-8 GroupId bytes

__astralog_offsets OFFSET_GROUP_FENCE:
  u8 key_kind=1 || string16 group_id

__astralog_offsets OFFSET_OWNER_FENCE:
  u8 key_kind=2 || string16 group_id || topic_id[16] || partition u32 little-endian

__astralog_offsets OFFSET_VALUE:
  u8 key_kind=3 || string16 group_id || topic_id[16] || partition u32 little-endian
```

Internal keys/values are independently bounded by P1 record limits plus the stricter P6 limits. Physical keyed compaction remains P9; these keys define logical replacement identity in P6.
## 7. Group phases

Frozen V1 phases:

```text
EMPTY                  = 0
STABLE                 = 1
PREPARING_EPOCH_FENCE  = 2
REVOKING               = 3
```

Semantics:

- `EMPTY`: no active member owns a partition.
- `STABLE`: no pending revocations; active assignment is authoritative.
- `PREPARING_EPOCH_FENCE`: a new target assignment/epoch is durable in group state, but the new group epoch is not externally active until the matching offsets group fence commits.
- `REVOKING`: new group epoch is active; unchanged owners remain active, moved partitions remain on the old owner until durable release, and target owners are withheld until handoff completes.

Only one authoritative target plan exists at a time. A later membership/subscription change may supersede an unfinished target only by preparing a strictly higher group epoch from the then-current active ownership; it may not reuse or move group epoch backwards.

## 8. Group and member epochs

`GroupEpoch` starts at 1 when a group first becomes externally authoritative. First-group creation commits a PREPARING record with internal active-epoch sentinel 0, then the epoch-1 offsets fence, then activates epoch 1; a crash at any boundary resumes from those committed records.

A new group epoch is required whenever committed membership/subscription changes can alter assignment authority, including:

- dynamic member join;
- member leave/session expiry;
- subscription change;
- topic partition-count change visible to the group;
- static-member replacement.

A static-member replacement always increments MemberEpoch **and** enters the normal strictly higher GroupEpoch path. The uniform-sticky assignor may preserve every assignment when subscription/eligibility is unchanged, but P6 V1 does not introduce a second same-GroupEpoch cross-topic fencing protocol.

`MemberEpoch` starts at 1 for a new logical member and strictly increments when the same static logical member establishes a replacement session. The greater committed MemberEpoch fences the old process.

Dynamic members without InstanceId receive a new MemberId and MemberEpoch=1 on each new JoinSubscribe admission.

Overflow of GroupEpoch, MemberEpoch, or GroupStateVersion is terminal for that attempted transition and cannot wrap.

## 9. Static-membership foundation

P6 V1 supports an optional `InstanceId` foundation:

- at most one committed MemberId per InstanceId in one group;
- a replacement JoinSubscribe carrying the same InstanceId and current MemberId/MemberEpoch creates exactly the next MemberEpoch;
- the old session is fenced immediately after the replacement transition commits;
- replacement always enters the normal next-GroupEpoch fencing path;
- identical subscription and valid ownership should produce zero assignment movement under the uniform-sticky objective;
- changed subscription or invalid ownership uses the same normal rebalance machinery.

P6 does not add long static-member grace windows beyond the ordinary session timeout. Richer operational policies belong to later phases.

## 10. Uniform-sticky assignor

The assignor is a pure deterministic function of:

- canonical members/subscriptions;
- current active assignment;
- current topic partition counts from committed P5 metadata.

Only a member subscribed to a topic is eligible for that topic's partitions.

The chosen assignment minimizes this tuple lexicographically:

1. maximum assigned-partition count across members;
2. number of partitions whose owner changes from a still-eligible current owner;
3. canonical owner vector, obtained by iterating TopicPartition ascending and comparing MemberId bytes.

Therefore balance has priority, movement is minimized among equally balanced solutions, and all remaining ties are deterministic.

Properties required in P6-E:

- identical logical input produces byte-identical assignment regardless of input iteration order;
- every subscribed partition is assigned exactly once when at least one eligible member exists;
- no member receives an unsubscribed topic;
- when every member has the same subscription, load differs by at most one;
- adding/removing partitions/members changes no ownership that is unnecessary under the frozen optimization tuple.

No RNG, wall clock, broker load, rack, or controller liveness input participates in assignment.

## 11. Cooperative rebalance state machine

A rebalance is learner-like in spirit: new ownership is not granted until old ownership is durably relinquished/fenced.

Epoch preparation:

1. compute deterministic target assignment and `next_group_epoch`;
2. commit GroupRecord phase `PREPARING_EPOCH_FENCE` with active old epoch plus pending next epoch/target;
3. commit one atomic `OffsetGroupFenceV1` to the matching `__astralog_offsets` shard that advances `group_epoch=next_group_epoch` and snapshots every then-current active owner with the same MemberId/MemberEpoch but under the new group epoch;
4. only then commit GroupRecord with `active_group_epoch=next_group_epoch`, phase `REVOKING` or `STABLE`.

Step 3 intentionally may create a short availability gap for offset commits from the old epoch. That is allowed; accepting a stale commit is not.

For each moved partition:

1. old owner remains active and receives the partition in its revocation set;
2. `RevokeAck` or session fencing commits a group transition marking the partition **released**; released means no active owner;
3. coordinator commits `OffsetOwnerFenceV1` for the target MemberId/MemberEpoch under the current GroupEpoch;
4. only after that fence is durable may a group transition activate the target owner;
5. after all revocations/handoffs finish, phase becomes `STABLE`.

Crash boundaries are resumable:

- crash after prepare but before group fence -> retry group fence;
- crash after group fence but before epoch activation -> finish epoch activation;
- crash after release but before owner fence -> retry owner fence;
- crash after owner fence but before new-owner activation -> finish activation;
- duplicate exact fence/transition is idempotent; conflicting duplicate fails closed.

At no point may both old and new members appear in `active_assignments` for the same TopicPartition.

## 12. Heartbeats and session timeout

JoinSubscribe supplies `session_timeout_ms` in the inclusive range:

```text
6_000 ..= 300_000 ms
```

The coordinator recommends:

```text
heartbeat_interval_ms = clamp(session_timeout_ms / 3, 1_000, 10_000)
```

Heartbeat admission requires exact current `(group_id, member_id, group_epoch, member_epoch)` authority.

Leader-local runtime maintains the last accepted heartbeat/deadline using injected monotonic time. On coordinator leadership change:

- volatile heartbeat observations are discarded;
- each committed member receives a fresh full `session_timeout_ms` grace interval beginning at new-leader initialization;
- leadership change alone does not change GroupEpoch/MemberEpoch or assignments;
- only a subsequently committed timeout transition removes/fences a member and starts/restarts rebalance.

This may delay crash detection after failover but cannot grant duplicate ownership.

## 13. Offset state and cross-topic fencing

Committed offsets are stored in `__astralog_offsets` under the conceptual key frozen by the Bible:

```text
(group_id, topic_id, partition)
```

Latest logical value:

```text
OffsetValueV1
  next_offset u64
  metadata bytes/string (0..=4096 UTF-8 bytes)
  commit_timestamp_ms i64
  group_epoch u64
  member_id [16]
  member_epoch u64
```

The offsets shard additionally maintains durable fences:

```text
OffsetGroupFenceV1
  group_id
  group_epoch
  owners map<TopicPartition, (MemberId, MemberEpoch)>  # canonical snapshot of current active owners

OffsetOwnerFenceV1
  group_id
  topic_id
  partition
  group_epoch
  member_id
  member_epoch
```

A CommitOffsets entry is accepted only when:

- request group epoch equals the durable OffsetGroupFence group_epoch;
- request member identity/epoch is the current committed group member authority;
- each partition is currently active on that member in group state;
- effective owner authority (the owner snapshot in OffsetGroupFenceV1 overridden by any later same-epoch OffsetOwnerFenceV1) exactly matches `(member_id, member_epoch)`;
- request shape/bounds are valid.

This double fence is required because `__astralog_groups` and `__astralog_offsets` are separate P3 groups. Merely checking group state before proposing an offset write is insufficient: an old proposal could otherwise commit after ownership changes.

Offset commits may move next_offset backward if an authorized application intentionally does so; P6 does not silently impose monotonic application progress. Fencing protects authority, not business intent.

No physical compaction is required for P6 correctness. Recovery applies the latest valid logical value/fence from committed history; P9 may later compact old keyed history without changing logical semantics.

## 14. Public Wire V0 primitives and bounds

All P6 API bodies use the existing P2 Wire V1 framing and **big-endian** body primitives.

Frozen P6 limits:

```text
MAX_GROUP_ID_LEN                 = 255 bytes
MAX_INSTANCE_ID_LEN              = 255 bytes
MAX_GROUP_MEMBERS                = 1_024
MAX_SUBSCRIPTION_TOPICS          = 256 per member
MAX_GROUP_PARTITIONS             = 16_384
MAX_MEMBER_ASSIGNMENT_PARTITIONS = 1_024
MAX_OFFSETS_PER_REQUEST          = 1_024
MAX_OFFSET_METADATA_LEN          = 4_096 bytes
MAX_GROUP_RECORD_BYTES           = 4 MiB
```

An encoded group state exceeding `MAX_GROUP_RECORD_BYTES` is rejected before proposal. All request counts are checked before allocation and must fit the negotiated 64 MiB Wire V1 frame limit.

Reusable public body primitives:

```text
MemberAuthorityV0:
  string16 group_id
  uuid member_id
  u64 group_epoch
  u64 member_epoch

TopicPartitionV0:
  uuid topic_id
  u32 partition

CoordinatorHintV0:
  u32 coordinator_broker_id   # 0xFFFF_FFFF unknown
  u64 coordinator_epoch       # 0xFFFF_FFFF_FFFF_FFFF unknown
  u64 current_group_epoch     # 0 when group unknown
  u64 current_member_epoch    # 0 when member unknown
```

Every P6 body ends in Wire V1 `TaggedFields`.

## 15. JoinSubscribe V0 — API key 30

Request body:

```text
string16 group_id
uuid member_id                 # all-zero asks coordinator to allocate dynamic member
bool instance_id_present
if present: string16 instance_id
u64 expected_group_epoch      # zero means caller has no known group epoch
u64 expected_member_epoch     # zero only for a new member (dynamic or first static instance)
u32 session_timeout_ms
u32 subscription_count
repeat subscription_count:
  uuid topic_id               # strictly ascending, unique
TaggedFields tags
```

Response body:

```text
CoordinatorHintV0 hint
uuid member_id
u8 group_phase
u32 heartbeat_interval_ms
u32 active_assignment_count
repeat: TopicPartitionV0      # ascending
u32 revocation_count
repeat: TopicPartitionV0      # ascending
TaggedFields tags
```

On a non-zero frame error, the hint is populated when known and all success-only fields use canonical zero/empty values. The client retries/heartbeats to obtain current assignment state.

A successful JoinSubscribe returns only committed membership/epoch/assignment state. It never returns a target partition as active before cooperative fencing permits activation.

## 16. Heartbeat V0 — API key 31

Request:

```text
MemberAuthorityV0 authority
TaggedFields tags
```

Response:

```text
CoordinatorHintV0 hint
u8 group_phase
u32 heartbeat_interval_ms
u32 active_assignment_count
repeat: TopicPartitionV0
u32 revocation_count
repeat: TopicPartitionV0
TaggedFields tags
```

Heartbeat success refreshes only leader-local session observation. Assignment/epoch changes in the response are always derived from already-committed group state.

## 17. RevokeAck V0 — API key 32

Request:

```text
MemberAuthorityV0 authority
u32 revoked_count              # 1..=1_024
repeat: TopicPartitionV0       # ascending, unique
TaggedFields tags
```

Response is the same member-state body as Heartbeat V0.

Acknowledge is accepted only for partitions currently pending revocation from that member. Duplicate exact acknowledgements after durable release are idempotent only while they identify the same completed handoff; conflicting or stale authority returns the appropriate group/member error.

## 18. Leave V0 — API key 33

Request:

```text
MemberAuthorityV0 authority
TaggedFields tags
```

Response:

```text
CoordinatorHintV0 hint
TaggedFields tags
```

Success means the member removal/fence transition is committed in `__astralog_groups`; it does not mean all reassigned partitions are already active on replacement members.

## 19. CommitOffsets V0 — API key 34

Request:

```text
MemberAuthorityV0 authority
u32 offset_count               # 1..=1_024
repeat offset_count:
  uuid topic_id
  u32 partition
  u64 next_offset
  string16 metadata            # 0..=4096 UTF-8 bytes
TaggedFields tags
```

Entries are strictly ascending `(topic_id, partition)` and unique. One request belongs to one group and therefore one offsets shard.

Response:

```text
CoordinatorHintV0 hint
u64 offsets_commit_index       # P3 commit/apply index of authoritative offsets write
TaggedFields tags
```

The request is atomic at the P6 offset-state-machine level: either every entry passes authority/fence validation and the canonical batch commits, or none is applied.

Success is returned only after the internal offsets partition has quorum-committed and applied the write. A lost success response is ambiguous; retrying identical values may append an equivalent later write but may never bypass fences or restore stale authority. P7 producer idempotency is not imported into offset commits.

## 20. FetchOffsets V0 — API key 35

Request:

```text
string16 group_id
u32 partition_count            # 1..=1_024
repeat: TopicPartitionV0       # ascending, unique
TaggedFields tags
```

Response:

```text
CoordinatorHintV0 hint
u32 result_count
repeat result_count:
  uuid topic_id
  u32 partition
  bool found
  if found:
    u64 next_offset
    i64 commit_timestamp_ms
    u64 group_epoch
    uuid member_id
    u64 member_epoch
    string16 metadata
TaggedFields tags
```

FetchOffsets returns only committed logical offset state. It is read-only and does not require the caller to be a current group member. P11 authorization may later restrict access without changing P6 durability semantics.

## 21. Error semantics

Existing frozen public errors are reused:

- `NOT_LEADER`: request reached a broker that is not current group coordinator;
- `GROUP_EPOCH_STALE`: caller group epoch is not current;
- `UNKNOWN_MEMBER`: MemberId/InstanceId is not current committed membership;
- `REBALANCE_IN_PROGRESS`: requested operation cannot complete while the caller has unresolved cooperative state;
- `INVALID_OFFSET_COMMIT`: shape/ownership/fence validation fails for an otherwise current member;
- `BROKER_BUSY`: bounded coordinator work capacity saturated;
- `REQUEST_TIMEOUT`: result may be ambiguous only after proposal could have occurred;
- `UNSUPPORTED_FEATURE` / `CLUSTER_FEATURE_TOO_OLD`: P6 feature is not active cluster-wide.

Retry classification remains the frozen P2 registry. P6 must not silently retry a mutation whose result may already have committed unless the operation's own state/version semantics make the retry idempotent.

## 22. Commit-before-response and recovery

Every durable P6 mutation returns success only after the owning internal P3 group has quorum-committed and the P6 state machine has applied it.

For cross-topic workflows, success is returned only after all durability prerequisites specified in this contract are committed. Coordinator process memory is never sufficient authority.

On coordinator failover/restart:

- recover group/offset logical state exclusively from committed internal-topic history;
- reject conflicting/gapped state versions;
- resume `PREPARING_EPOCH_FENCE`, released partitions, and pending revocations idempotently;
- reset only volatile heartbeat observations;
- never synthesize ownership from client heartbeats or cached responses.

## 23. Partition expansion and topic lifecycle

P6 watches committed P5 topic metadata.

When a subscribed user topic expands:

- new partitions enter the deterministic assignor input;
- the group prepares a strictly higher GroupEpoch;
- existing valid ownership is preserved where optimal under the frozen assignor objective;
- new ownership follows the same cooperative fence path.

Topic deletion removes its partitions from target assignment only after P5 metadata makes deletion authoritative. P6 never resurrects deleted topic ownership from stale group history.

The two internal P6 topics themselves cannot expand in P6 V1.

## 24. Resource and scheduling rules

Every P6 coordinator queue, request table, internal write pipeline, pending handoff set, and response buffer has a finite count and byte bound.

Required implementation behavior:

- saturation returns/propagates `BROKER_BUSY` or bounded retryable internal backpressure;
- heartbeat/session-control work cannot be indefinitely starved by offset commits or large JoinSubscribe traffic;
- one pathological group cannot allocate memory proportional to untrusted counts beyond frozen caps;
- decoded lengths are validated before allocation;
- state-machine work is deterministic and does not block on network/disk directly;
- metrics must not use raw GroupId/MemberId as unbounded-cardinality labels.

P6-K/P6-L qualify scheduling and memory bounds under churn.

## 25. Feature activation

Consumer groups correspond to frozen feature key `12 = CONSUMER_GROUPS`.

P6 V1 irreversible group/offset behavior requires controller `active_feature_level >= 2`. Feature level 1 remains the closed P5 baseline without active consumer-group semantics. The level bump is explicit and commit-ordered through the already-qualified P5 feature-level primitive.

P6 APIs 30..35 are advertised by ApiVersions only when:

- P6 implementation is present;
- both internal topics are valid and serving;
- cluster active feature level gates P6 irreversible behavior;
- the broker can route to current group coordinators.

Before activation, allocated P6 API keys continue returning `UNSUPPORTED_API_KEY` exactly as frozen by P2.

## 26. Explicit later-phase boundaries

P6 does **not** implement:

- producer epoch/sequence/deduplication or InitProducer semantics — P7;
- compression/performance qualification — P8;
- physical internal-topic compaction or retention deletion — P9;
- work-queue leases, retries, DLQ, visibility timeout — P10;
- TLS/auth/ACL, operational drain, continuous balancing, rolling upgrades — P11;
- final release chaos/benchmark closure — P12.

Group epochs fence coordinator state and stale offset commits only. They cannot undo arbitrary external side effects already performed by a stale application.

## 27. Implementation checkpoints

- P6-A: contract/checklist freeze only;
- P6-B: checked group types + canonical P6 API/internal-record codecs + goldens;
- P6-C: internal-topic bootstrap/validation + deterministic coordinator routing;
- P6-D: pure durable group state machine and epoch/member fencing;
- P6-E: deterministic uniform-sticky assignor;
- P6-F: cooperative revocation/release/activation state machine;
- P6-G: heartbeats, session timeout and static-membership foundation;
- P6-H: durable replicated group coordinator/recovery;
- P6-I: offset state, cross-topic group/owner fences and Commit/FetchOffsets;
- P6-J: broker/client API routing and bounded runtime integration;
- P6-K: churn/crash/failover/model qualification;
- P6-L: compatibility/scope audit and final closure.

No later checkpoint may weaken P0-P5 contracts or the P6-A contract frozen here.