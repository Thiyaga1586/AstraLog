# AstraLog P9 Retention and Log Compaction Contract V1

Status: **P9-A FROZEN SPECIFICATION — NO P9 RUNTIME IMPLEMENTATION YET**

## 1. Goal and phase boundary

P9 safely reclaims partition disk while preserving the stream, Raft, producer and failover semantics
already frozen by P0-P8.

P9 owns:

- time-based retention;
- size-based retention;
- cleanup policies `delete`, `compact`, and `compact+delete`;
- keyed compaction with stable stream offsets;
- tombstone grace/removal;
- copy-on-write cleaner generations;
- live activation of the P1 `segments.manifest` inventory primitive;
- the sparse retained-history Storage V2 format required by batched compaction;
- a replicated logical `log_start_offset`;
- per-replica clean-compacted progress and replicated `compaction_safe_offset`;
- producer-state preservation across physical cleanup;
- retained-history-aware rebootstrap;
- a bounded internal protected-retention-floor hook for later subscription-aware retention;
- bounded cleaner scheduling/throttling and failure qualification.

P9 does **not** own P10 QueueReceive/Ack/Nack, visibility leases, retries or DLQ semantics; P11
authentication/TLS/ACLs/drain/rolling-upgrade/production observability; or P12 release-grade chaos and
benchmark publication.

The exact entering authority is P8 closure commit:

`37c07a5868950873de6aa250428e4f922a6552ec`

with Git note `P8 exact-HEAD qualification: PASS` declaring P8 authoritatively closed/frozen and P9
authorized next.

## 2. Non-negotiable inherited invariants

P9 must preserve, at minimum:

- I-01/I-02 committed-data and no-false-success durability;
- I-04/I-05 Raft safety and committed-prefix durability;
- I-11 derived indexes;
- I-12 immutable closed segments;
- I-13 stable stream offsets;
- I-15/I-16/I-17 P7 retry/fencing/sequence behavior;
- I-25 controller linearizability;
- I-26 bounded resources;
- I-27 control-plane progress;
- I-28 explicit compatibility/feature activation;
- I-30 no success on ambiguous durable error;
- I-31 no compaction resurrection;
- I-32 no rewrite of the live Raft suffix.

Retention and compaction may make physical bytes disappear. They may not make committed authority,
latest compacted state, producer fencing/retry state, or required protected history disappear.

## 3. Architecture blocker resolved at freeze time

P8 batching means one committed Storage V1 frame may contain many independently keyed records. Storage
V1 requires each record `offset_delta` to equal its ordinal, so deleting one record from the middle of
that frame would force later survivors to be renumbered. I-13 forbids that.

P9 therefore freezes `FORMAT_V2.md`:

- active append and the live Raft suffix stay V1;
- copy-on-write compaction below the committed snapshot floor may emit immutable V2 retained-history
  segments with sparse original stream offsets;
- V2 retained-history frames are never replayed as the live Raft suffix;
- historical V1 bytes remain immutable.

This is a versioned compatibility extension, not a silent reinterpretation of V1.

The second blocker is P4 `RebootstrapPackage` V1: it requires a contiguous Raft prefix from index 1
through the snapshot boundary. Retention/compaction intentionally destroys that prefix. P9 therefore
adds a versioned retained-history rebootstrap package while leaving P4 V1 decoding frozen.

## 4. Feature activation and compatibility

### 4.1 Feature identity

P9 reserves public feature key:

`15 = RETENTION_COMPACTION`

Feature version 1 means the node understands:

- controller P9 retention metadata;
- manifest-governed live partition inventory;
- Storage V2 retained-history files;
- P9 maintenance control state;
- RebootstrapPackage V2;
- Fetch/ListOffsets log-start semantics;
- AlterTopicConfig V0 retention mutation.

### 4.2 Cluster feature level

Irreversible P9 writes require committed cluster feature level **5**.

Before level 5:

- no Storage V2 file is written;
- no existing partition is switched to manifest-governed inventory by P9;
- no P9 maintenance control command is proposed;
- no RebootstrapPackage V2 is emitted;
- no retention policy mutation is advertised or accepted;
- all historical topics behave as infinite-retention, no-physical-compaction topics.

Every active broker/controller voter must advertise level-5 support before the existing P5 feature
level state machine may commit the bump.

### 4.3 Old-format read compatibility

P9-capable binaries continue reading all V1 partitions and V1 rebootstrap packages. Mixed V1/V2
retained-history files are permitted only after level 5 and only through an authoritative manifest.

## 5. Controller-owned topic cleanup policy

Retention semantics are durable controller metadata, never a broker-local failover-sensitive setting.

```text
CleanupPolicyV1 = Delete | Compact | CompactDelete

TopicRetentionPolicyV1 {
    topic_id
    cleanup_policy
    retention_ms: Option<u64>
    retention_bytes: Option<u64>
    tombstone_grace_ms: u64
    subscription_protection: Disabled | ReservedForProtectedFloor
}
```

Rules:

- `Delete` permits time/size logical-start advancement but no keyed cleaner;
- `Compact` permits keyed cleaning/tombstone semantics and requires both delete-retention limits to be
  absent;
- `CompactDelete` permits both mechanisms;
- `None` time means no time deletion;
- `None` bytes means no size deletion;
- both `None` under `Delete` is valid and means historical infinite retention;
- `tombstone_grace_ms` must be finite and positive for compacting policies;
- P9 freezes the default tombstone grace to **86,400,000 ms (24 h)**;
- `ReservedForProtectedFloor` is not publicly activatable until the later subscription owner exists;
  P9 implements and tests the internal protected-floor hook without pulling P10 semantics forward.

Historical topics decode to:

`Delete + retention_ms=None + retention_bytes=None + tombstone_grace_ms=86_400_000 + protection=Disabled`

so feature activation alone deletes nothing.

No historical internal topic is silently changed to compacting policy. P9 may explicitly migrate the
P6 internal topics through ordinary committed policy mutation once the P9 path is independently
qualified.

## 6. Controller persistence/versioning

P9 adds an additive controller metadata version layered after P8 V3.

The P9 controller state contains a canonical TopicId-sorted retention-policy table. Older controller
snapshots/commands decode to the historical infinite-retention default. P9 bytes are emitted only at
feature level 5.

Required mutation semantics:

- CreateTopic with P9 policy commits topic identity, P8 performance policy and P9 retention policy
  atomically;
- AlterTopicConfig changes only explicitly supplied P9 policy fields and is compare/current-state
  validated by controller authority;
- invalid policy changes make no metadata-epoch advance;
- topic deletion removes the retention policy atomically with the topic;
- snapshots encode policies deterministically and reject duplicates, unknown enums, invalid bounds,
  unsorted entries and orphan policy records.

P9 implementation may name the additive command/snapshot envelope V4, but V1/V2/V3 encodings remain
byte-exact.

## 7. Public API boundary

P9 activates the already permanent API key `6 = AlterTopicConfig` at version 0 for retention policy
mutation after feature level 5.

CreateTopic and DescribeTopic gain additive version 2 bodies for P9 policy. Existing Create/Describe
V0/V1 bodies remain unchanged.

Fetch V0 and ListOffsets V0 wire bodies remain unchanged. P9 activates fields/errors they already
carry:

- `FetchPartitionResponseV0.log_start_offset` becomes the replicated P9 logical start instead of the
  historical constant zero;
- a Fetch request below `log_start_offset` returns existing `OFFSET_OUT_OF_RANGE` and the authoritative
  `log_start_offset`/high watermark;
- ListOffsets selector `-2` returns `log_start_offset`;
- timestamp scans ignore records below `log_start_offset` and records physically absent due to
  compaction;
- Fetch from a compacted hole returns the first surviving record at or above the requested offset;
- surviving records keep their original explicit stream offsets.

Compaction never changes Produce ack semantics or the committed high watermark.

## 8. Manifest-governed live partition inventory

P1 already froze `segments.manifest` and `GenerationTransaction`; P9 makes that primitive authoritative
for live partition inventory.

### 8.1 Legacy adoption

If a level-5 partition has no manifest, it first opens through the exact historical generation-0
directory discovery rule. Before its first roll/retention/compaction operation under P9, it atomically
publishes manifest generation 1 referencing the validated existing live files without rewriting their
V1 bytes.

### 8.2 Manifest-mode recovery

Once `segments.manifest` exists:

- the manifest is the sole segment inventory authority;
- missing/corrupt manifest bytes fail closed;
- missing or exact-size-mismatched immutable entries fail closed;
- recovery never falls back to directory globbing;
- unreferenced files are garbage, never candidates for visibility;
- exactly one mutable entry exists for a writable partition and it is V1;
- every roll publishes a new manifest generation because segment-set membership/mutability changed;
- cleaner and roll publication serialize through one partition segment-set generation lock;
- stale generation transactions abort instead of overwriting a newer inventory.

Indexes remain derived and rebuildable.

## 9. Replicated P9 maintenance state

P9 adds versioned application-maintenance commands which persist as Storage control frames and consume
no stream offsets. They are ordinary committed data-partition Raft application entries, so authority
is inherited from P3 commit order without modifying the Raft voting algorithm.

P9 V1 maintenance kinds are:

1. `AdvanceLogStartOffset { new_log_start_offset }`
2. `ProducerStateCheckpoint { covered_raft_index, canonical_p7_states }`
3. `AdvanceCompactionSafeOffset { new_compaction_safe_offset }`

All fields are canonical, bounded and monotonic where applicable. Unknown versions/kinds fail closed.

`log_start_offset` and `compaction_safe_offset` use **exclusive lower-bound semantics**:

- records with offset `< log_start_offset` are no longer logically retained;
- tombstones with offset `< compaction_safe_offset` may pass the replica-coordination portion of the
  physical-removal gate, subject to grace and snapshot safety.

Maintenance control frames survive COW cleaning until a later committed maintenance frame completely
supersedes the same state. They are not returned by Fetch.

## 10. Logical retention authority

Physical deletion is never the first authority transition. The leader first computes a policy-valid
candidate and commits `AdvanceLogStartOffset`. Every replica immediately applies the logical floor;
physical bytes may be reclaimed later.

The floor:

- starts at 0 for historical topics;
- never decreases;
- never exceeds the committed stream high watermark;
- advances only on a current partition leader under current committed topic policy;
- is proposed only at an eligible closed-segment boundary selected by the leader;
- is fenced by term/leader epoch and current policy epoch before proposal;
- never uses an uncommitted tail to justify deletion.

This replicated logical boundary prevents failover from making expired records visible merely because a
new leader has not yet physically deleted the same local files.

## 11. Time retention

P9 time retention uses conservative segment-close age, not user-controlled event timestamps.

For a closed segment, the next segment's durable header `created_at_ms` is its close-time lower bound.
A segment can become time-expired only when:

`now_ms - close_bound_ms >= retention_ms`

with checked arithmetic. Backward clock movement delays cleanup rather than accelerating it.

The active segment is never time-deleted. The leader's current policy-valid decision is replicated via
`log_start_offset`, so physical segmentation differences on followers cannot change logical visibility.

## 12. Size retention

Size retention examines manifest-authoritative `.log` bytes from oldest to newest and selects only
closed logical source ranges. It advances the candidate start until retained bytes are at or below the
configured target, subject to:

- active segment retention;
- snapshot/Raft safety;
- protected-floor input when such a provider is active;
- checked arithmetic and configured planner bounds.

The target is a reclamation goal, not permission to violate safety. A single active/protected range may
legitimately keep usage above the configured target.

## 13. Physical delete cleanup

Deletion removes only files no longer required by the authoritative manifest generation.

After a committed `log_start_offset` advance:

- a fully obsolete immutable segment may be omitted from the next manifest generation;
- a local segment that straddles the logical floor may remain physically present; reads still hide the
  expired prefix;
- later compaction/rebootstrap may rewrite that retained subset, but retention never renumbers it;
- unreachable old files are reclaimed only after the new manifest is durably authoritative.

No cleaner operation targets the mutable active segment or any V1 live-Raft frame above the persisted
snapshot floor.

## 14. Keyed compaction semantics

For `Compact`/`CompactDelete`, P9 compaction considers only committed retained history and keeps the
latest retained record for each non-null key.

Rules:

- null-key records have no compaction identity and are retained until delete-retention removes them;
- values are opaque bytes;
- latest is determined by greatest original stream offset within committed retained history;
- non-tombstone superseded records may be omitted during COW rewrite;
- the latest non-null value for a key is never removed merely because it is old;
- surviving record order is original stream-offset order;
- surviving offsets never change;
- V2 sparse frames represent holes created inside a previously batched frame;
- the active segment is never compacted;
- a cleaner pass may inspect newer committed history to identify the winner, but it only rewrites source
  frames at/below its frozen snapshot-safe cleaning boundary.

Compaction is opportunistic physical cleanup. Before tombstone removal, a less-clean replica may still
contain extra superseded non-tombstone history; it may not expose a value newer than the committed
latest value as something else.

## 15. Tombstones and grace

A record with non-null key and null value is a keyed tombstone. A null-key/null-value record has no
compaction identity and follows ordinary null-key retention rules.

Tombstones are retained while superseded older values are cleaned. Physical tombstone removal requires
all of:

1. compacting cleanup policy is active;
2. tombstone source frame is committed and at/below the persisted snapshot floor;
3. conservative grace time has elapsed;
4. tombstone offset is `< committed compaction_safe_offset`;
5. the exact cleaner plan still matches current manifest/policy/maintenance epochs.

Grace uses conservative closed-segment age for the same reason as time retention: user event timestamps
must not permit immediate delete-marker disappearance.

## 16. Per-replica clean-compacted progress

Each replica tracks a local monotonic `clean_compacted_offset` meaning:

> for every keyed record below this exclusive offset, the replica's current authoritative generation
> has removed superseded older values consistently while retaining required latest values, tombstones,
> and P9/P7 recovery anchors.

The local watermark is acceleration/progress state, not cluster authority. It is persisted in a small
CRC-protected sidecar bound to the current manifest generation. Missing/corrupt/stale local progress
falls back to a conservative lower value and triggers re-cleaning; it never advances authority.

Replicas report progress to the current leader through a bounded P9 sideband message fenced by topic,
partition, current term/leader epoch, broker identity/incarnation and assignment generation.

## 17. Replicated compaction safety boundary

The leader computes the safe candidate as the minimum accepted `clean_compacted_offset` across every
replica in the current effective voting configuration that remains controller-eligible for the
partition.

Safety policy is deliberately conservative:

- an eligible voter with no current report blocks advancement;
- an offline voter remains blocking until it catches up or P5 authority commits its exclusion from the
  eligible voting set;
- learner progress does not reduce the voter minimum;
- joint-consensus configurations include every voter from the effective joint set;
- stale term/incarnation/assignment reports are ignored;
- `compaction_safe_offset` advances only by a committed maintenance command and never decreases.

This makes the Bible's "currently leader-eligible" rule mechanically precise without adding automatic
membership changes to P9.

A replica whose local clean state is below the committed safe boundary after disk loss/rejoin is
**rebootstrap-required** before it may regain leadership eligibility for that partition.

## 18. Producer-state preservation

P7 requires accepted producer epoch, exact next sequence, and the newest five committed retry records
with original offsets/Raft indexes/digests to survive physical compaction.

Before P9 removes any committed idempotent source frame still needed to reconstruct P7 state, the leader
must first commit `ProducerStateCheckpoint` containing the canonical bounded P7 partition producer map
through a declared committed Raft boundary.

Rules:

- checkpoint state is derived from committed P7 state only;
- the checkpoint command itself must commit before source history becomes reclaimable;
- source history and the checkpoint must both be below the cleaning snapshot floor before source frames
  disappear;
- recovery loads the latest valid retained checkpoint then replays later committed idempotent V1 frames;
- P7 `producer.snapshot` remains a derived acceleration sidecar;
- deleting/corrupting `producer.snapshot` after P9 compaction still reconstructs identical authority
  from retained committed P9 checkpoint history plus later data;
- exact supported retry continues returning original offsets without appending a duplicate.

The checkpoint payload is bounded by the existing per-partition P7 producer-entry and retry-window
limits and the application command byte ceiling. If a canonical checkpoint cannot fit, cleanup fails
closed instead of discarding source producer history.

## 19. Retained-history-aware rebootstrap V2

P4 RebootstrapPackage V1 remains valid for partitions whose required contiguous prefix still exists.

P9 adds RebootstrapPackage V2 for a replica that must cross a retention/compaction boundary. V2 carries
at minimum:

- TopicId/PartitionId;
- P3 SnapshotMetadata;
- committed P9 logical maintenance state;
- the authoritative retained-history manifest generation;
- bounded immutable V1/V2 retained-history file bytes with exact lengths and hashes/CRC;
- enough canonical P7 checkpoint state to preserve producer recovery;
- the next live stream offset needed to create/resume the mutable V1 suffix;
- package-level canonical integrity protection and hard file/byte/count bounds.

Install happens in a staging directory. The target validates every file/header/frame, manifest,
identity, maintenance boundary and producer checkpoint, durably syncs the staged generation, and only
then atomically publishes it under P3 InstallSnapshot authorization. Interrupted install never replaces
the old authoritative generation.

After install, the follower resumes ordinary V1 AppendEntries strictly above the P3 snapshot floor.

## 20. Fetch and concurrent cleaning

Readers take an immutable view of one authoritative manifest generation. A COW cleaner builds another
generation without mutating files held by readers.

Publication swaps the manifest atomically; old files are reclaimed only after no active reader can
reference them. Implementation may use Arc/reference-counted generation handles or an equivalent safe
ownership mechanism; unsafe lifetime tricks are not permitted.

A Fetch either sees the old complete generation or the new complete generation. It never observes a
partially staged cleaner output.

## 21. Protected-retention-floor hook

P9 implements a bounded internal input:

`protected_log_start_ceiling: Option<StreamOffset>`

When present, ordinary retention may not advance `log_start_offset` beyond that ceiling.

P9 does not define queue subscriptions or derive this value from P10 state. Public activation of
subscription-aware protection remains disabled until P10 owns durable subscription semantics. P9 tests
the hook with deterministic synthetic providers so later P10 binding cannot require a storage redesign.

Absence, corruption or timeout of a required protected-floor provider fails toward retaining more data,
not deleting uncertain required data.

## 22. Cleaner bounds and scheduling

Every cleaner work source is finite. P9 freezes configurable hard bounds for at least:

- partitions queued for cleanup;
- concurrent cleaner jobs;
- source segments per plan;
- source bytes per plan;
- decoded records/keys per plan;
- key-index memory bytes;
- replacement output bytes;
- staged temp files;
- progress reports in flight;
- rebootstrap V2 file count/package bytes;
- garbage files reclaimed per sweep.

One-over-limit work is rejected/deferred before unbounded allocation.

Cleaner scheduling is background data-plane maintenance. Raft, controller heartbeats, metadata control,
P6 group control and health-critical work retain reserved scheduling capacity. Cleaner pressure cannot
indefinitely starve control progress.

P9 "compaction throttling" means bounded work slices plus configurable byte/work budgets and yields;
it is not a benchmark claim and it does not change durability semantics.

## 23. Crash and corruption behavior

P9 fails closed on:

- manifest CRC/version/shape failure;
- manifest reference to missing or wrong-size immutable file;
- V2 header/frame/CRC/version/schema failure;
- non-monotonic sparse offsets or duplicate retained offsets;
- V2 frame above the captured snapshot floor;
- stale cleaner plan after manifest/policy/snapshot/maintenance change;
- malformed maintenance control state;
- producer checkpoint inconsistency;
- rebootstrap V2 identity/hash/manifest mismatch;
- local clean watermark ahead of the generation it claims;
- attempted tombstone removal beyond committed safe offset;
- attempted retention below an uncertain protected floor.

Crash after replacement files are durable but before manifest switch leaves the old manifest
authoritative. Crash after manifest switch leaves the new generation authoritative; unreachable files
are garbage only.

## 24. Deterministic/failure qualification obligations

P9 qualification includes at least:

- crash at every generation publication stage;
- legacy no-manifest adoption and restart;
- mixed immutable V1/V2 + mutable V1 recovery;
- Fetch held open while cleaner swaps generations;
- duplicate keys across many segments and inside one batched frame;
- sparse survivors from the middle of compressed P8 batches;
- null-key records under compact policy;
- tombstone grace before/after boundary;
- offline voter blocking safe advancement;
- voter removal then safe advancement;
- replica restart/disk rollback behind safe boundary requiring rebootstrap;
- leader change with unequal local clean progress;
- proof that removed tombstone cannot resurrect an older value;
- producer-state exact retry/fencing after compaction with `producer.snapshot` deleted;
- time retention with backward clock movement;
- size retention unable to meet target because active/protected bytes remain;
- retention plus lagging replica/rebootstrap interaction;
- corrupted/stale local cleaner progress;
- malformed/corrupt V2 and rebootstrap package rejection;
- deterministic seeded model of policy/clean/swap/failover operations;
- sustained cleaner pressure with control-plane progress and bounded memory.

## 25. Phase exit gate

P9 closes only when all of the following are mechanically proven:

- compaction never renumbers surviving offsets;
- no committed latest value disappears incorrectly;
- failover cannot resurrect a value whose tombstone was safely removed;
- crash cannot leave ambiguous live segment generations;
- retention never deletes data required by its configured protected policy;
- logical `log_start_offset` cannot regress or outrun committed visibility;
- no cleaner rewrites the live Raft suffix;
- producer-state recovery/retry/fencing is identical before and after cleanup;
- old V1 Storage/protocol behavior remains compatible and exact goldens remain unchanged;
- all P9 queues/buffers/work slices are bounded and control progress survives pressure;
- no P10-P12 runtime behavior is pulled forward;
- the complete P0-P8 compatibility regression remains green;
- exact clean P9 closure HEAD repeats the required gates and receives the P9 PASS Git note.

## 26. Ordered implementation checkpoints

P9 implementation is intentionally decomposed:

- **P9-A** — contract/checklist/Storage V2 architecture freeze;
- **P9-B** — controller retention metadata, feature level 5 and public config codecs;
- **P9-C** — Storage V2 codec plus manifest-governed live partition inventory;
- **P9-D** — replicated log-start state and time/size delete retention;
- **P9-E** — copy-on-write keyed compaction and sparse retained-history reads;
- **P9-F** — producer-state checkpoint/recovery preservation;
- **P9-G** — coordinated tombstone safety and RebootstrapPackage V2;
- **P9-H** — bounded cleaner runtime, throttling, protected-floor hook and pressure qualification;
- **P9-I** — compatibility/failure/model audit and exact P9 closure.

No checkpoint may begin until the previous checkpoint is independently qualified. P10 remains disabled
until P9-I exact-HEAD authority exists.
