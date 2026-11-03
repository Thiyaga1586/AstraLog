# AstraLog P7 Execution Checklist

Status: **P7 CLOSURE COMMIT — AUTHORITATIVE ONLY WITH EXACT-HEAD PASS GIT NOTE**

This checklist is normative. An item is checked only when its stated proof exists mechanically.
P7 must preserve every frozen P0-P6 contract and the producer ownership/retry/fencing rules in
`P7_PRODUCER_V1.md`.

## Phase exit invariants

- [x] I-15: a supported exact retry never creates a second committed logical copy.
- [x] I-16: an older accepted ProducerEpoch cannot append after newer committed partition authority.
- [x] I-17: sequence gaps/partial overlaps are never silently accepted.
- [x] producer state is reconstructable from committed history without a side authority database.
- [x] lost-success retry survives real RF3 leader failover and returns original offsets.
- [x] producer/controller/runtime state, queues and retry windows have hard finite bounds.
- [x] P1/P2/P3/P4/P5/P6 compatibility regressions remain green.
- [x] no P8/P9/P10/P11/P12 semantics are pulled forward.

## P7-A — Contract and checklist freeze

- [x] **P7-001** — Write `P7_PRODUCER_V1.md`. **Proof:** producer identity, controller/partition authority, sequencing, retry and recovery semantics are explicit before implementation.
- [x] **P7-002** — Write complete `P7_CHECKLIST.md`. **Proof:** P7-A through P7-K are decomposed into bounded actions P7-001…P7-155.
- [x] **P7-003** — Freeze authority split. **Proof:** controller owns ProducerId/ProducerEpoch issuance; each data partition owns accepted epoch/sequence/dedupe state.
- [x] **P7-004** — Freeze producer identity types/sentinels. **Proof:** non-zero ProducerId u64, non-zero ProducerEpoch u32, Sequence u32, overflow/no-wrap rules specified.
- [x] **P7-005** — Freeze partition-local fencing semantics. **Proof:** unseen/higher epoch requires controller confirmation; durable stale fencing begins from committed higher-epoch partition history.
- [x] **P7-006** — Freeze sequence state machine. **Proof:** expected-next, exact retry, gap, overlap, changed-content and too-old retry outcomes are unambiguous.
- [x] **P7-007** — Freeze supported retry window. **Proof:** five recent committed batches per producer-partition plus canonical digest/original offsets.
- [x] **P7-008** — Freeze max-in-flight semantics. **Proof:** one unresolved idempotent batch per producer-partition; exact pending duplicate may coalesce only when identical.
- [x] **P7-009** — Freeze ack/timestamp restrictions. **Proof:** Produce V1 is quorum-only and requires explicit timestamps >=0; Produce V0 remains unchanged.
- [x] **P7-010** — Freeze compatibility/versioning boundary. **Proof:** Storage V1 bytes reused; ApplicationCommand V2, ControllerCommand V2 and ControllerSnapshot V2 are additive versioned formats.
- [x] **P7-011** — Freeze public APIs. **Proof:** InitProducer V0 on key 20 and Produce V1 body/response semantics are specified without renumbering existing APIs/errors.
- [x] **P7-012** — Freeze feature activation. **Proof:** feature key 11 and active feature level >=3 gate irreversible P7 behavior; pre-activation surface remains legacy-compatible.
- [x] **P7-013** — Freeze resource/recovery rules. **Proof:** producer cache/session/snapshot/window bounds and producer.snapshot derived-state rules are explicit.
- [x] **P7-014** — Freeze later-phase boundaries and qualification plan. **Proof:** P8-P12 ownership remains excluded and `P7_CONTRACT_FREEZE_REPORT.md` records the exact P7-A boundary.

## P7-B — Types, codecs, API versions and golden bytes

- [x] **P7-015** — Add checked ProducerId type at the lowest valid shared layer. **Proof:** `astralog-common::ProducerId` rejects zero, exposes deterministic total ordering/hash/get semantics, and dedicated shared-layer tests pass while `(0,0)` remains confined to InitProducer request decoding.
- [x] **P7-016** — Add checked ProducerEpoch type. **Proof:** `ProducerEpoch` rejects zero and `checked_next` rejects `u32::MAX`; focused common tests pass.
- [x] **P7-017** — Add checked ProducerSequence/range helpers. **Proof:** `ProducerSequence::checked_last` / `checked_next_after` reject zero-count and wraparound ranges; boundary tests pass.
- [x] **P7-018** — Define ProducerAuthority value. **Proof:** shared `ProducerAuthority` is exactly one validated `(ProducerId, ProducerEpoch)` pair with canonical derived equality/order/hash and no optional/zero representation.
- [x] **P7-019** — Implement InitProducerRequestV0 codec. **Proof:** protocol codec round-trips exactly `(0,0)` allocation and `(nonzero,nonzero)` CAS-bump forms; mixed zero/non-zero sentinel combinations and trailing bytes fail closed.
- [x] **P7-020** — Implement InitProducerResponseV0 codec. **Proof:** response requires a non-zero validated authority and round-trips canonically; zero identity/epoch forms are rejected.
- [x] **P7-021** — Implement ProduceRequestV1 codec. **Proof:** request codec requires quorum ack, one non-zero producer authority, canonical unique partition entries, checked base-sequence ranges, non-empty bounded WireRecordV0 batches and explicit timestamps >=0; round-trip/malformed tests pass.
- [x] **P7-022** — Implement ProduceResponseV1 codec. **Proof:** V1 reuses the frozen V0 partition-result representation exactly, including base/last offsets and leader hints, and immutable V1 response body bytes pass.
- [x] **P7-023** — Keep ProduceRequest/ResponseV0 byte-exact. **Proof:** the original P2 golden suite remains **11/11 PASS**, including immutable Produce V0 request/response fixtures; V0 codec definitions were not widened into V1.
- [x] **P7-024** — Activate API-version registry metadata without activating runtime behavior. **Proof:** the protocol registry reports InitProducer key 20 as V0 and Produce key 10 as V0..V1 while broker dispatch/ApiVersions runtime code is unchanged; P7-C+ remains unable to serve these APIs.
- [x] **P7-025** — Freeze SHA-256 canonical batch digest encoding/input. **Proof:** protocol-local SHA-256 matches FIPS vectors; the normative contract now defines the exact canonical record-set byte encoding and an immutable digest fixture proves timestamp/key/value/header ordering affects identity deterministically.
- [x] **P7-026** — Add ApplicationCommandV2 canonical codec. **Proof:** replication V2 encodes base offset, producer authority, base sequence and canonical record bytes with bounds/explicit timestamp/sequence-wrap checks; V2 round-trips canonically and the frozen V1 decoder rejects V2 bytes.
- [x] **P7-027** — Add ControllerCommandV2 producer-session codec. **Proof:** isolated controller V2 codec canonically encodes new-session and exact CAS epoch-bump commands, rejects invalid/non-next epoch transitions, is bounded, and cannot decode through frozen V1 `decode_command`.
- [x] **P7-028** — Add ControllerSnapshotV2 producer-session codec shape. **Proof:** V2 wraps the exact canonical V1 snapshot bytes plus a strictly sorted producer-session table and outer CRC32C; V1 backward decode remains available while CRC, duplicate/noncanonical order and invalid producer authority fail closed.
- [x] **P7-029** — Add exact P7 golden vectors and malformed/noncanonical corpus. **Proof:** immutable fixtures cover InitProducer V0, Produce V1 request/response, canonical SHA-256 digest, ApplicationCommandV2, ControllerCommandV2 and ControllerSnapshotV2; protocol P7 goldens are **4/4 PASS**, P2 goldens **11/11 PASS**, P6 goldens **6/6 PASS**, and controller/replication malformed/version-separation suites pass.

## P7-C — Controller producer-session authority and V2 persistence

- [x] **P7-030** — Extend deterministic controller state with canonical producer-session map. **Proof:** `ControllerState` owns the sole `BTreeMap<ProducerId, ProducerEpoch>` controller authority; read-only authority lookup derives an exact `ProducerAuthority` and no broker-local session authority exists.
- [x] **P7-031** — Implement atomic new-session transition. **Proof:** `apply_producer_session(InitializeSession)` stages the state, requires epoch 1 and exact `next_unallocated_producer_id`, inserts the session and advances the allocator in one committed transition.
- [x] **P7-032** — Preserve I-45 ProducerId non-reuse. **Proof:** initialization must consume the exact P5 durable allocator counter; mixed V1 allocation + V2 session replay advances from 1000 -> 1001 -> 1002 without reuse, and lost-response RF3 qualification consumes the ambiguous ID before the next allocation.
- [x] **P7-033** — Implement CAS epoch bump. **Proof:** V2 bump requires exact current epoch and canonical `checked_next`; the RF3 path commits epochs 1 -> 2 -> 3 -> 4 without skipping or duplicating authority.
- [x] **P7-034** — Fence stale epoch-bump request. **Proof:** deterministic state tests and the real controller cluster reject the old expected epoch after a newer bump commits without mutating authority.
- [x] **P7-035** — Reject unknown non-zero ProducerId epoch bump. **Proof:** producer-session apply and runtime preparation both require an existing canonical session record; an allocated-but-unregistered/unknown ID cannot be invented into authority.
- [x] **P7-036** — Reject ProducerEpoch overflow atomically. **Proof:** `ProducerEpoch::checked_next` rejects `u32::MAX`; canonical V2 encoding/state tests prove failure leaves session state and metadata epoch unchanged.
- [x] **P7-037** — Enforce producer-session logical count bound. **Proof:** admission is checked against `MAX_PRODUCER_SESSIONS = 1_000_000` before staged insertion; one-over-limit helper qualification fails closed.
- [x] **P7-038** — Enforce snapshot-size admission bound before commit. **Proof:** every new-session candidate computes the exact V1 base snapshot plus V2 envelope/session bytes and rejects a mutation that cannot fit the existing 64 MiB snapshot envelope before authoritative state changes.
- [x] **P7-039** — Emit ControllerCommandV2 only behind feature-level gate. **Proof:** both runtime preparation and deterministic apply require active feature level >=3; feature level 2 rejects the P7 command atomically.
- [x] **P7-040** — Decode/replay mixed historical V1 + P7 V2 controller log. **Proof:** version-dispatched `ControllerLogCommand` preserves the frozen V1 codec while persistence replays a V1 ProducerId allocation followed by V2 session initialization deterministically after restart.
- [x] **P7-041** — Migrate ControllerSnapshotV1 -> in-memory empty session map. **Proof:** generic snapshot decode maps V1 to an empty session table while preserving cluster state and `next_unallocated_producer_id` exactly.
- [x] **P7-042** — Encode/decode ControllerSnapshotV2. **Proof:** persistence builds a V2 state snapshot for non-empty sessions, installs/compacts it, restarts from that snapshot with no live log prefix, and restores session authority, allocator 1001 and metadata epoch 2 exactly.
- [x] **P7-043** — Prove controller snapshot corruption fails closed. **Proof:** existing V2 CRC/order tests plus state installation validation reject noncanonical ordering, invalid session/allocator relationships and malformed authority before controller state is published.
- [x] **P7-044** — Bind InitProducer runtime mutation to commit-before-response. **Proof:** follower InitProducer returns `NotLeader`; leader success is delivered only through the mutation waiter after P3 quorum commit/application and re-verifies the committed authority before responding.
- [x] **P7-045** — Qualify controller leader failure/lost InitProducer response. **Proof:** one RF3 scenario drops a committed new-session response and a committed epoch-bump response, proves ID/epoch non-reuse, kills the controller leader, continues at a higher term, bumps again, restarts the old leader and converges all three replicas on the same authorities.

## P7-D — Pure partition producer state machine

- [x] **P7-046** — Implement pure PartitionProducerStateV1 model. **Proof:** no clock/network/disk/RNG dependency in transition logic.
- [x] **P7-047** — Implement first-seen epoch admission candidate. **Proof:** controller-confirmed authority requires base_sequence=0.
- [x] **P7-048** — Implement equal-epoch expected-next acceptance. **Proof:** only exact next sequence can create a new append candidate.
- [x] **P7-049** — Implement sequence-gap rejection. **Proof:** base_sequence > next_sequence maps to OUT_OF_ORDER_SEQUENCE without mutation.
- [x] **P7-050** — Implement partial-overlap rejection. **Proof:** ranges intersecting but not exactly equal to retained batch are rejected.
- [x] **P7-051** — Implement exact recent committed retry lookup. **Proof:** same range + digest returns original offsets and no append candidate.
- [x] **P7-052** — Reject changed-content retry. **Proof:** same sequence range with different canonical digest cannot return success or append.
- [x] **P7-053** — Reject retry older than retained window. **Proof:** state machine fails explicitly rather than guessing prior success.
- [x] **P7-054** — Advance next_sequence only from committed batch application. **Proof:** proposal/local persistence alone cannot mutate durable producer authority.
- [x] **P7-055** — Bound recent window to newest five committed batches. **Proof:** sixth committed batch deterministically evicts only the oldest retry metadata.
- [x] **P7-056** — Implement higher-epoch reset transition. **Proof:** after committed first batch, accepted epoch advances, sequence resets from that epoch's range, old recent window is superseded.
- [x] **P7-057** — Reject lower accepted epoch. **Proof:** old epoch always maps to PRODUCER_FENCED once higher epoch is committed authority.
- [x] **P7-058** — Make committed replay idempotent. **Proof:** replaying same frame metadata cannot advance state twice.
- [x] **P7-059** — Reject conflicting committed producer metadata. **Proof:** impossible epoch/sequence/history conflict marks recovery invalid rather than self-repairing.
- [x] **P7-060** — Prove canonical digest determinism. **Proof:** reordered map/container construction that preserves wire-record order yields identical digest; record/header order differences remain distinct where semantically encoded.
- [x] **P7-061** — Add property/state-transition suite for I-15/I-16/I-17. **Proof:** exhaustive small ranges match frozen oracle outcomes.

## P7-E — Storage V1 producer metadata activation and ApplicationCommand V2 integration

- [x] **P7-062** — Thread ProducerIdentity through P4 persistence for ApplicationCommandV2. **Proof:** P4 version-dispatch materializes V2 into the frozen Storage V1 idempotent bit/id/epoch/base_sequence fields; mixed/restart/rebootstrap tests pass.
- [x] **P7-063** — Preserve ApplicationCommandV1 -> producer=None behavior. **Proof:** V1 materialization/recovery remains the old producer-None path; original non-idempotent Storage V1 and Produce V0/P2 regressions remain green.
- [x] **P7-064** — Decode mixed V1/V2 application commands during persistence/recovery. **Proof:** the P4 adapter dispatches the frozen little-endian application-command version, reconstructs V1 or V2 accordingly, and a V1/V2/V1/V2 committed history reopens byte-semantically identical.
- [x] **P7-065** — Reconstruct producer metadata from committed Storage V1 frames. **Proof:** `PartitionPersistence::recover_committed_producer_states` validates producer identity, derives exact sequence ranges from frame base_sequence + record count and rebuilds `PartitionProducerStateV1` plus canonical retry digests.
- [x] **P7-066** — Ignore uncommitted idempotent tail for authoritative producer state. **Proof:** recovery filters data frames above `committed_raft_index`; a durable seq-1 tail above committed seq-0 leaves recovered next_sequence at 1.
- [x] **P7-067** — Preserve producer metadata across follower replication. **Proof:** two independent P4 stores materializing the same replicated V2 log entry produce equal Storage V1 frames and equal recovered producer-state maps.
- [x] **P7-068** — Preserve producer metadata through conflict truncation. **Proof:** an uncommitted V2 suffix is truncated/replaced at the same Raft/stream boundary; only the replacement digest/metadata enters producer authority after commit.
- [x] **P7-069** — Preserve producer metadata through ordinary replica restart. **Proof:** reopen reconstructs exact committed accepted epoch, next sequence and recent retry window, including a committed higher-epoch reset.
- [x] **P7-070** — Preserve producer metadata through P4 rebootstrap/snapshot installation. **Proof:** a P4 rebootstrap package containing committed V2 history restores producer state on the target and accepts/reconstructs the exact next committed sequence in the ordinary suffix.
- [x] **P7-071** — Reject malformed Storage V1 idempotent metadata. **Proof:** Storage V1 encode/decode rejects zero ProducerId, zero ProducerEpoch and base_sequence + record_count wrap, including malformed headers with an otherwise valid recomputed CRC.
- [x] **P7-072** — Verify Storage V1 CRC still covers producer fields. **Proof:** flipping a producer-header byte in an otherwise valid idempotent frame yields the existing frame CRC mismatch before producer metadata is trusted.
- [x] **P7-073** — Add exact idempotent Storage V1 golden regression. **Proof:** the frozen `data_idempotent_multi_v1.hex` fixture remains exact under activated validation; Storage qualification is **44/44 PASS**.
- [x] **P7-074** — Keep non-idempotent P1 golden set unchanged. **Proof:** frozen non-idempotent data/control/index/meta fixtures remain green and the original P2 protocol golden suite is **11/11 PASS**.
- [x] **P7-075** — Qualify mixed legacy/idempotent stream offsets. **Proof:** a committed V1/V2/V1/V2 history reads contiguous stream offsets `0,1,2,3` and reopens with the same application commands.
- [x] **P7-076** — Freeze P9 producer-metadata handoff surface. **Proof:** committed-only `recover_committed_producer_states()` returns accepted epoch, next sequence and newest-five retry metadata/digests from P1 history without implementing compaction or a producer snapshot.

## P7-F — Runtime admission, pending proposal and duplicate suppression

- [x] **P7-077** — Add idempotent propose entry point separate from legacy propose_records. **Proof:** `ReplicatedPartition::propose_idempotent_records` and the internal actor request are separate from legacy `propose_records`; Produce V0 dispatch remains unchanged and public Produce V1 is still disabled.
- [x] **P7-078** — Validate local P3 leader/term before P7 admission. **Proof:** idempotent admission requires local `Role::Leader` plus the exact current leader epoch before any pending/proposal mutation; focused tests reject follower and stale-term attempts.
- [x] **P7-079** — Resolve current committed producer state before admission. **Proof:** `load_producer_state` first checks the bounded cache and otherwise reconstructs through P7-E `recover_committed_producer_states()`; restart qualification clears the volatile cache and lazily recovers the committed epoch before admitting the next range.
- [x] **P7-080** — Validate unseen ProducerId with controller. **Proof:** the first local attempt returns `ControllerValidationRequired`; broker `DynamicBrokerControl` then reads committed controller authority and retries with exact evidence. The real 3-controller/RF1 test rejects an unknown ID and accepts the controller-issued ID only.
- [x] **P7-081** — Validate higher ProducerEpoch with controller. **Proof:** a higher epoch without evidence is withheld; an arbitrary unissued higher epoch is rejected, while the exact controller-issued CAS-bumped epoch is accepted at sequence zero in the real integration test.
- [x] **P7-082** — Permit equal already-accepted epoch without controller hot-path lookup. **Proof:** once epoch 2 is committed partition authority, all three controllers are shut down and sequence 1 still quorum-commits successfully through the partition runtime.
- [x] **P7-083** — Locally fence lower epoch from committed state. **Proof:** after epoch 2 commits, an epoch-1 request returns frozen `PRODUCER_FENCED` locally without controller lookup or proposal.
- [x] **P7-084** — Enforce quorum-only P7 ack. **Proof:** focused runtime tests reject both none and leader acknowledgement modes before pending/proposal mutation; the idempotent actor path always uses quorum acknowledgement.
- [x] **P7-085** — Enforce explicit idempotent timestamps. **Proof:** any negative timestamp, including the legacy broker-assigned `-1` sentinel, returns `InvalidTimestamp` before digest/proposal authority is created.
- [x] **P7-086** — Enforce one unresolved range per producer-partition. **Proof:** while one producer range is pending, a distinct next range receives bounded `Busy` and creates no second proposal.
- [x] **P7-087** — Coalesce exact duplicate of pending range. **Proof:** identical authority/range/digest returns the same `ProposalReceipt`; proposal ID and pending-byte accounting remain unchanged and broker actor waiters attach to the one underlying proposal.
- [x] **P7-088** — Reject conflicting duplicate of pending range. **Proof:** same authority/range with a different canonical digest returns `OUT_OF_ORDER_SEQUENCE` and cannot attach to or create another proposal.
- [x] **P7-089** — Return committed duplicate original offsets without proposal. **Proof:** exact committed retry returns the retained original offsets/Raft index; `next_proposal_id` and committed stream high watermark remain unchanged, and the real broker integration returns the original result.
- [x] **P7-090** — Advance runtime producer state on observed committed outcome only. **Proof:** tentative higher-epoch admission leaves the committed cache at the old epoch; cache advancement occurs only when the proposal Raft index is covered by durable commit and P7-D `apply_committed` succeeds.
- [x] **P7-091** — Roll back tentative pending fence on failed/truncated proposal. **Proof:** a higher-epoch proposal held before persistence is dropped/restarted with no pending state; the replacement runtime reconstructs only committed epoch 1 and accepts its exact next sequence. RF3 locally-durable-but-uncommitted truncation remains deliberately deferred to P7-I.
- [x] **P7-092** — Bound runtime producer cache to 4,096 entries. **Proof:** the 4,097th insertion deterministically evicts a reconstructable resident entry, never evicts the entry protected by pending authority, and cache size remains exactly 4,096.
- [x] **P7-093** — Preserve Raft/control scheduling under idempotent pressure. **Proof:** saturated idempotent proposal admission rejects additional work while Raft ticks continue and leadership remains healthy; the unchanged P6 RF3 control/coordinator suite also requalifies **9/9 PASS** on the P7-F tree.

## P7-G — Producer snapshot and crash recovery

- [x] **P7-094** — Define ProducerSnapshotV1 canonical codec. **Proof:** `producer.snapshot` freezes `ASTRPSN1`/version 1, TopicId/PartitionId, committed Raft boundary, strictly sorted ProducerId entries, accepted epoch/next sequence/newest-five retry records, reserved-zero fields and CRC32C; canonical round-trip is mechanically checked.
- [x] **P7-095** — Enforce 64 MiB snapshot byte bound. **Proof:** encoder/decoder reject one-over-limit, and `ProducerSnapshotStore::load` checks filesystem metadata before `fs::read`; a sparse 64 MiB+1 file fails with `LimitExceeded` before proportional allocation.
- [x] **P7-096** — Encode accepted epoch/next sequence/recent five batches per producer. **Proof:** `PartitionProducerStateV1::from_snapshot_parts` validates identity/epoch/next sequence and 1..=5 canonical retained retry records including offsets/Raft indexes/digests before decoded state is accepted.
- [x] **P7-097** — Write snapshot crash-safely. **Proof:** production persist uses temp create/write + file `sync_all` + atomic rename + AstraLog parent-directory durability hook; injected stops after temp write, after temp sync and after rename yield either the old complete snapshot or the complete new snapshot, never partial authority.
- [x] **P7-098** — Reject snapshot identity/version/CRC/count/order corruption. **Proof:** codec rejects header/version/reserved/count/order/state/CRC faults; valid-CRC wrong TopicId/PartitionId is discarded by `PartitionPersistence` and rebuilt from committed P1 history.
- [x] **P7-099** — Reject snapshot ahead of committed Raft boundary. **Proof:** a correct-identity snapshot at boundary 99 over committed boundary 8 is discarded and full committed replay reconstructs the exact oracle state.
- [x] **P7-100** — Recover valid snapshot plus committed suffix. **Proof:** snapshot at committed Raft index 2 plus six committed suffix frames reconstructs byte/logically equal state to the full committed-history replay oracle, crossing the fixed four-frame recovery page limit.
- [x] **P7-101** — Recover with missing snapshot. **Proof:** deleting `producer.snapshot` and recovering the same partition yields the exact full-replay producer map with no P1 mutation.
- [x] **P7-102** — Recover with corrupt snapshot. **Proof:** corrupt derived bytes are discarded and healthy committed P1 history rebuilds the exact oracle; committed event history is never rewritten to match the sidecar.
- [x] **P7-103** — Recover after snapshot write interrupted at each stage. **Proof:** deterministic injected interruption after temp write/temp sync preserves the old complete snapshot; interruption after rename exposes the complete new snapshot; no partial snapshot becomes authority.
- [x] **P7-104** — Preserve recent retry original offsets/digests across snapshot recovery. **Proof:** snapshot-reconstructed state equals the pre-eviction committed state and exact retry returns the original base/last stream offsets and Raft index without append.
- [x] **P7-105** — Prove cache eviction + later reconstruction. **Proof:** an evicted runtime producer entry reloads through snapshot-aware committed recovery and makes the identical duplicate decision with identical retry metadata.
- [x] **P7-106** — Qualify bounded recovery work. **Proof:** snapshot size/counts are bounded before proportional allocation; committed suffix replay uses `read_frames_from_raft` in fixed four-frame pages and each physical frame decode remains under the existing Storage V1 `max_frame_bytes` bound. Recovery never allocates from an untrusted declared entry count.

## P7-H — Broker/client InitProducer and Produce V1 integration

- [x] **P7-107** — Add broker InitProducer dispatch behind P7 readiness. **Proof:** pre-activation broker unit coverage rejects key 20; activated dispatch routes through `DynamicBrokerControl::init_producer` to committed controller producer-session authority.
- [x] **P7-108** — Add broker Produce V1 dispatch behind P7 readiness. **Proof:** pre-activation V1 is unsupported, Produce V0 remains unchanged, and activated V1 decodes only into the qualified idempotent runtime path.
- [x] **P7-109** — Advertise feature key 11 only when P7 is usable. **Proof:** broker ApiVersions unit qualification binds feature 11 advertisement to committed feature-level/runtime readiness.
- [x] **P7-110** — Advertise InitProducer V0 only when active. **Proof:** allocated key 20 is absent before readiness and advertised only after P7 activation.
- [x] **P7-111** — Advertise Produce max version 1 only when active. **Proof:** pre-activation brokers advertise Produce V0 only; activated brokers advertise V0..V1 without changing legacy V0 behavior.
- [x] **P7-112** — Map P7 controller/runtime errors to frozen public codes. **Proof:** broker unit and runtime qualification cover exact UNKNOWN_PRODUCER, PRODUCER_FENCED, OUT_OF_ORDER_SEQUENCE and bounded routing/controller error mapping.
- [x] **P7-113** — Add bounded client producer-session state. **Proof:** client stores one ProducerId/Epoch plus a configured finite per-partition map and one optional pending request per partition; one-over admission is rejected.
- [x] **P7-114** — Implement client InitProducer new-session path. **Proof:** idempotent Produce requires an installed authoritative session and the public RF1 path obtains it only after InitProducer success/recovery.
- [x] **P7-115** — Implement client producer restart/epoch bump path. **Proof:** successful CAS bump requires the exact next epoch, replaces the old per-partition map, and the public RF1 test commits the new epoch's first batch at sequence zero.
- [x] **P7-116** — Implement client per-partition sequence reservation. **Proof:** client unit qualification permits exactly one unresolved range and proves attempted send/pending state does not advance committed `next_sequence`.
- [x] **P7-117** — Implement bounded retry classification. **Proof:** retryable transport/routing failures retain pending state and reuse the same range within `max_producer_retries`; terminal producer/sequence failures stop automatic retry.
- [x] **P7-118** — Preserve exact request bytes across automatic retry. **Proof:** pending state stores the canonical encoded Produce V1 body; the RF1 loss proxy captures first-send/retry bodies and requires byte equality.
- [x] **P7-119** — Handle deduplicated success as normal success. **Proof:** authoritative success completes the retained pending range once, advances sequence from its stored record count, and accepts original committed offsets returned by broker dedupe.
- [x] **P7-120** — Surface ambiguous InitProducer safely. **Proof:** timeout/connection ambiguity returns `AmbiguousMutation`, clears local producer authority, and explicit recovery allocates a fresh session instead of blindly reusing the old epoch.
- [x] **P7-121** — Add public RF1/single-broker integration smoke. **Proof:** real three-controller + RF1 broker qualification drops a committed InitProducer response and first Produce V1 response, then proves fresh-session recovery, byte-identical retry, exactly one read-committed logical record/original offsets and successful epoch restart at sequence zero.

## P7-I — Lost-response, fencing and real RF3 failover qualification

- [x] **P7-122** — Real RF3 lost-response-after-quorum test. **Proof:** RF3 proxy discards a committed Produce V1 success; automatic same-term retry is byte-identical, returns original `(0,0)` offsets and Fetch sees exactly one logical record.
- [x] **P7-123** — Kill RF3 leader after quorum commit before response. **Proof:** after the proxy observes/discards committed success, the data leader is stopped; a higher-term successor reconstructs committed producer state and the client retry returns the original one-copy result.
- [x] **P7-124** — Kill leader after local persist before quorum. **Proof:** with both followers down, qualification observes `durable_raft_index` advance while commit does not; successor retry commits sequence zero once and rejoining the old leader repairs its leader-only suffix.
- [x] **P7-125** — Kill leader before local persist/proposal completion. **Proof:** proxy holds Produce before broker dispatch, durable/commit indexes remain unchanged, leader dies, and successor retry accepts sequence zero exactly once.
- [x] **P7-126** — Retry after explicit leadership transfer. **Proof:** a committed response is lost, P3 leadership explicitly transfers, client refreshes routing/retargets only leader term, and retry returns the original `(1,1)` offsets.
- [x] **P7-127** — Producer restart with higher epoch. **Proof:** controller CAS bump preserves ProducerId, increments epoch exactly once, and the first committed higher-epoch partition batch uses sequence zero.
- [x] **P7-128** — Old producer races higher-epoch first batch. **Proof:** while epoch 2 sequence zero is durable-pending, racing epoch 1 sequence one is fenced; after failover only committed authority survives, so no interleaving creates two committed producer authorities.
- [x] **P7-129** — Fail leader while higher-epoch batch is uncommitted. **Proof:** epoch 2 becomes durable but cannot quorum commit; after leader death the successor reconstructs committed epoch 1 and accepts its exact sequence-one continuation.
- [x] **P7-130** — Fail leader after higher-epoch batch commits. **Proof:** committed epoch 2 fences epoch 1 locally and after leader failover; an exact epoch-2 sequence-zero retry on the successor returns the original offsets.
- [x] **P7-131** — Controller outage with already-accepted producer. **Proof:** after all controllers stop, already accepted epoch 1 continues at exact next sequence through the RF3 data-partition quorum path.
- [x] **P7-132** — Controller outage with unseen/higher epoch. **Proof:** controller-issued-but-unaccepted higher authority and a completely unseen ProducerId both fail `BROKER_NOT_AVAILABLE` with durable/commit indexes unchanged, proving no proposal was guessed.
- [x] **P7-133** — Concurrent controller epoch-bump race. **Proof:** two same-expected-authority bumps race on the live controller leader; exactly one succeeds at epoch+1 and the stale expected authority cannot advance again.
- [x] **P7-134** — Controller leader failover during InitProducer. **Proof:** committed InitProducer response is lost, controller leader is killed, successor retains the consumed ID/epoch, client reports ambiguity/clears local authority, and recovery allocates the next ProducerId without reuse.
- [x] **P7-135** — RF3 restart/snapshot recovery exact-retry test. **Proof:** snapshots are persisted on all three caught-up replicas, all brokers stop/reopen, and exact retry of committed sequence one returns its original offsets with exactly two committed logical records total.

## P7-J — Deterministic models, resource bounds and scheduling stress

- [x] **P7-136** — Add bounded producer sequence state-space model. **Proof:** the expanded deterministic oracle searches epochs 1..=4, committed histories through eight batches, sequence positions through next+2 and record counts 1..=3 while preserving I-15/I-16/I-17, admission side-effect freedom and the five-batch retry bound.
- [x] **P7-137** — Model commit/response-loss/failover permutations. **Proof:** committed-before-failover versus uncommitted plus response-present/lost permutations yield either original-offset duplicate or one successor append; final state always contains one logical range.
- [x] **P7-138** — Model higher-epoch/old-epoch races. **Proof:** both committed orderings are exercised; tentative higher authority is side-effect free and once epoch 2 commits, epoch 1 is fenced with monotonic committed authority.
- [x] **P7-139** — Model recent-window eviction. **Proof:** twelve committed batches retain exactly sequences 7..11; all five exact retained retries dedupe and every older sequence fails explicitly rather than appending.
- [x] **P7-140** — Model pending duplicate/coalescing. **Proof:** 64 identical pending retries share one ProposalId/byte charge; changed content fails and a different unresolved range receives bounded Busy with no second proposal.
- [x] **P7-141** — Seeded multi-producer/multi-partition deterministic campaign. **Proof:** 192 actions across four producers/three partitions produce byte-identical trace and final state for an identical seed; a different seed changes the trace and all retry windows remain bounded.
- [x] **P7-142** — Sequence arithmetic boundary campaign. **Proof:** near-u32-max sequence/count combinations and exact/one-over u64-max stream-offset endings are checked; valid boundaries pass and all wrap cases fail.
- [x] **P7-143** — Producer-session/controller snapshot bound tests. **Proof:** client partition-state one-over fails without mutation; ControllerSnapshotV2 one-byte-over encode/decode and valid-CRC MAX_PRODUCER_SESSIONS + 1 declaration fail atomically before proportional decode.
- [x] **P7-144** — Partition producer cache/snapshot bound tests. **Proof:** existing production-cache qualification fills 4,096 states, protects pending authority, inserts ProducerId 4,097 with deterministic reconstructable eviction, and snapshot reload preserves the exact duplicate/original-offset decision.
- [x] **P7-145** — Malformed/corrupt producer recovery campaign. **Proof:** valid-CRC ProducerSnapshotV1 and ControllerSnapshotV2 producer-metadata mutations fail closed; frozen Storage V1 zero-id/zero-epoch/sequence-wrap corruption remains rejected, and P7-G corrupt-derived-snapshot fallback reconstructs only from committed history.
- [x] **P7-146** — Sustained retry storm scheduling test. **Proof:** 257 bounded contender attempts cannot grow admission state; every rejection is followed by a successful Raft control tick while the partition remains leader/nonfatal, and serialized P6 RF3 remains 9/9.
- [x] **P7-147** — Repeated failover stress. **Proof:** six seeded RF3 rounds commit one sequence, kill the current leader, dedupe exact retry on a different successor, restart/catch up the old leader and finish with exactly six read-committed records.

## P7-K — Compatibility audit and exact P7 closure

- [x] **P7-148** — Requalify frozen P1 bytes/goldens. **Proof:** `docs/FORMAT_V1.md` and Storage V1 index layout remain unchanged from P1 format freeze `3515000`; P7 changes only semantic validation of the already-reserved producer header fields. Storage is **44/44 PASS**, including unchanged non-idempotent/index/meta goldens plus idempotent producer metadata/CRC qualification.
- [x] **P7-149** — Requalify frozen P2 legacy wire goldens. **Proof:** `crates/astralog-protocol/tests/golden_vectors.rs` is source-identical to P2 closure `a3cf08d`; original P2 goldens are **11/11 PASS**, with P6/P7 additions separate at **6/6 + 4/4 PASS**. Produce V0 and every earlier V0 API remain byte-exact.
- [x] **P7-150** — Requalify P3/P4 replication semantics. **Proof:** `crates/astralog-raft` has zero source delta from P6 closure `a76b136`; Raft is **122/122 PASS**, replication **82/82 PASS**, frozen P4 RF3 quorum/redirect/term/committed-read integration **1/1 PASS**, and P7 failover/rebootstrap/restart qualification remains green.
- [x] **P7-151** — Requalify P5/P6 controller/group semantics. **Proof:** P7 adds producer-session authority without changing P5 placement/movement rules or any `astralog-groups` source since P6 closure. P5 dynamic integration is **4/4 PASS**, controller **106/106 PASS**, groups **55/55 PASS**, and serialized P6 RF3/group/offset integration is **9/9 PASS** on exact repair HEAD `3373976`.
- [x] **P7-152** — Audit no P8-P12 scope pull-forward. **Proof:** no Cargo manifest/dependency delta exists since P6 closure; source-delta audit finds no compression codec/runtime, benchmark implementation, physical retention/compaction policy, queue lease/redelivery/DLQ semantics, TLS/mTLS/auth/ACL/drain/continuous-balancing behavior or P12 final benchmark claim. Matching additions are only `Compression::None` uses of the pre-existing Storage V1 field and an explicit comment deferring physical retention to P9.
- [x] **P7-153** — Run final P7 deterministic + real RF3 qualification and full workspace regression. **Proof:** closure-tree qualification is focused P7-J **9/9 + 2/2 + 1/1**, real P7 failover **12/12**, P7 runtime **2/2**, P6 RF3 **9/9**, Storage **44/44**, P2/P6/P7 goldens **11/11 + 6/6 + 4/4**, Raft **122/122**, P4 RF3 **1/1**, P5 dynamic **4/4**, controller **106/106**, groups **55/55**, replication **82/82**, complete serialized workspace **548/548**, with fmt/Clippy/doc/diff hygiene all **PASS**.
- [x] **P7-154** — Write `P7_CLOSURE_REPORT.md` and create one clean P7 closure commit. **Proof:** the closure report binds the complete P7 history, invariants, compatibility/scope audits, boundary repair, qualification matrix, known later-phase boundaries and exact-HEAD procedure; the commit containing this fully checked checklist/report/status set is the sole P7 closure commit.
- [x] **P7-155** — Requalify exact P7 closure HEAD. **Proof:** by the phase closure rule, the closure commit is not authoritative until its exact clean SHA repeats focused P7/P6, immutable/source audits, fmt, Clippy, doc tests and complete serialized workspace **548/548**, then receives a Git qualification note. That post-commit note is the mechanical evidence that makes this checked closure action authoritative without creating a second moving closure commit.
