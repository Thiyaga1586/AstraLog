# AstraLog

**Fault-Tolerant Distributed Event Streaming Infrastructure**

AstraLog is a from-first-principles **Rust distributed event log** inspired by useful ideas from Kafka, Apache Pulsar/BookKeeper, and Raft — without using Kafka, Pulsar, BookKeeper, ZooKeeper, etcd, RabbitMQ, or another broker internally. A work-queue extension was planned but is **not implemented**.

The project is intentionally built to expose the hard systems problems rather than hiding them:

- append-only segmented storage
- durable WAL semantics and crash recovery
- CRC corruption detection
- partitioning and ordered logs
- Multi-Raft replication and leader election
- quorum acknowledgements
- cluster metadata consensus
- replica catch-up and reconfiguration
- consumer offsets and cooperative consumer groups
- idempotent producers and duplicate detection
- retries, visibility leases, and dead-letter queues (**planned P10, not implemented**)
- batching and compression
- retention and log compaction
- backpressure and quotas
- partition rebalancing
- graceful draining and rolling upgrades (**planned P11**)
- observability, security, and chaos testing (**planned P11–P12**), plus reproducible benchmarks

## Interview-ready demo (2026-11-03)

**Portfolio milestone: DEMONSTRABLE — not production qualified.**
The checked-in [interview demo and benchmark guide](docs/INTERVIEW_READY.md)
starts real RF1 and RF3 brokers on local TCP, creates topics, produces data,
fetches committed records, and reports measured throughput and p50/p95/p99
latency. A committed release benchmark source (`d7e32d3`) completed
**12 workload measurements with zero producer errors**; three focused
restart/failover/deduplication checks also passed.

```powershell
powershell -NoProfile -ExecutionPolicy Bypass -File scripts/run-interview-demo.ps1
```

On the measured laptop, RF3 quorum producer buffering improved throughput
from **14.34 to 47.09 records/s** (1 KiB records, four producers, two
repeats); see the [raw results and generated summary](docs/benchmarks/interview_20261103/SUMMARY.md).
This is a small single-host benchmark, **not** a claim of production
performance or full-system release qualification. The known intermittent P7
full-suite regression remains disclosed in the guide.

### Adversarial audit follow-up (2026-11-03)

See [what was hardened and why](docs/REMEDIATION_NOTES.md), written for an
engineering interview. The benchmark now checks fetched **payload bytes,
record identities and offsets**, and new runs retain individual latency
samples for independent percentile verification. The original 2026-11-03
release CSV remains a separate source-pinned historical measurement.
The [13-item adversarial audit closure matrix](docs/AUDIT_CLOSURE_MATRIX.md)
distinguishes tested fixes from remaining production-design limitations.
The post-audit [clean-commit optimized benchmark](docs/benchmarks/remediation_20261103/SUMMARY.md)
retains 2,464 request samples and an independently verified percentile report.

**Security and deployment:** This is an unauthenticated local prototype.
See [SECURITY.md](SECURITY.md); do not expose its broker/controller endpoints
to untrusted networks. The listener configuration now refuses non-loopback
addresses rather than accidentally exposing unauthenticated sockets.
Windows clean-restart tests do not prove sudden
power-loss durability. P9-G retained-history V2 exact-HEAD qualification
remains separate from the interview milestone.

**Before creating a new public GitHub repository:** Remove private paths from
the *published history*, not just the latest files. After finalizing a clean
local commit, `scripts/prepare-public-snapshot.ps1` can generate a snapshot
without any old Git history. Inspect it before initializing a new repository.
The old benchmark commit SHA will not exist in that new history; run fresh
release benchmarks from the new public commit for publicly verifiable numbers.

## Current status

**P9 — Retention and Log Compaction: P9-F EXACT-HEAD PASS; P9-G LOCAL CLOSURE CANDIDATE COMMITTED, EXACT-HEAD QUALIFICATION PENDING**

P0, P1, P2, and P3 are closed. `RAFT_V1.md` is frozen and the independent
`astralog-raft` core now implements the complete P3 consensus scope: pre-vote,
RequestVote, leader election/initialization, AppendEntries replication and
conflict repair, durable commit/apply ordering, check-quorum, learners, joint
membership, snapshots/install-snapshot, leadership transfer, bounded
uncommitted application data, deterministic simulation, and a mechanically
checked bounded formal safety model. P4 has now frozen
`P4_REPLICATION_V1.md` and implemented the full P4 integration scope: real
P1-backed Raft persistence, bounded broker-pair Raft transport, RF 1/3/5
partition runtimes, `acks=0/leader/quorum`, committed Fetch/ListOffsets,
leader-term fencing, conflict repair, restart recovery, and crash-safe
P3-authorized rebootstrap. P4 is mechanically closed. P5-A now freezes the
controller/placement ownership boundary and the base-level production checklist.
P5-B adds the pure deterministic astralog-controller metadata state machine and
frozen ControllerCommandV1 codec. P5-C now adds crash-durable controller hard
state/log/commit persistence, canonical ControllerSnapshotV1 storage, P3
RecoveredState reconstruction, commit/snapshot crash intents, follower
InstallSnapshot rebootstrap, and snapshot-floor log compaction. P5-D now adds a
separate bounded CRC-protected controller Raft transport, strict cluster/source
identity fencing, persistent peer reconnect/backoff, the P3-backed controller
runtime, leader-only mutations with commit+apply response ordering, and a real
three-controller election/failover/rejoin qualification. P5-E now adds the
create-once CRC-protected controller bootstrap manifest containing cluster ID +
complete static voter set, atomic no-overwrite publication, runtime
persist-before-serve enforcement, and foreign-cluster/voter-set refusal. P5-F
now adds the typed broker registration/rejoin/lifecycle surface, deterministic
pre-proposal semantic validation, globally unique broker ID/node UUID identity,
strictly monotonic committed incarnations, leader-only stale-incarnation/UUID
authority fencing, and durable JOINING/ACTIVE/DRAINING/FENCED/REMOVED lifecycle
semantics across controller failover/restart. Heartbeats and failure detection
are now qualified in P5-G as leader-local volatile state: every heartbeat passes
the committed BrokerAuthority gate, progress arrays are bounded/canonical,
failure-detector time is explicitly injected and monotonic, suspicion is reset on
controller leadership change, and Healthy/Suspect/Unavailable observations never
mutate replicated broker authority. P5-H now routes topic create/expand/delete
through controller authority, generates TopicId before proposal, enforces Wire V1
partition/RF bounds, and implements the frozen deterministic placement order over
P5-G liveness plus durable broker lifecycle/feature/rack/zone metadata. Required
failure domains never silently degrade; Preferred fallback is explicit. Dynamic
broker metadata dissemination is now qualified in P5-I: controller application
entries are retained as bounded committed deltas, heartbeat lag selects bounded
contiguous deltas or snapshot fallback, and the broker metadata cache applies
updates atomically with monotonic epoch, gap, stale-update, and cluster fencing.
P5-J now makes that cache authoritative for production broker topology: committed
assignments create/remove local P4 actors, controller broker records rebuild peer
endpoint/incarnation expectations, and public Metadata is built from controller
authority while leader/term still come only from observed P3 state. DescribeCluster
and ReassignPartitions V0 are live only in dynamic-controller mode; reassignment
commits durable movement intent. P5-K/L now executes that intent through destination
learner creation, ordinary P4/P3 catch-up, incarnation/generation-fenced progress,
P3 joint-consensus promotion, stable target-voter observation, optional P3 leadership
transfer, safe source removal and atomic final assignment/`Complete` publication. The
workflow resumes after controller/source/destination failures from committed intent plus
observed P3 state; removed replica bytes remain preserved as non-authoritative orphaned
state. P5-N now qualifies the durable active cluster feature level: brokers and every static
controller voter advertise checked support ranges, unsupported bumps fail before activation,
the frozen `SetFeatureLevel` command is explicit/commit-before-return/strictly monotonic, and
`FeatureGate` lets later irreversible behavior fail closed until the broker/controller has
applied the required committed level. A controller that cannot support the recovered active
level refuses to serve after restart. Static P4 topology remains only for compatibility tests.
P5-O now qualifies the frozen durable producer-ID allocator end to end: controller Raft and
snapshot state own one monotonic `next_unallocated_producer_id`, allocations are contiguous
and hard-bounded to one million IDs per command, successful ranges return only after
commit+apply, lost responses may waste but can never reuse committed IDs, restart/failover
preserve the next ID, and overflow fails atomically. No producer epoch, sequence, session or
deduplication behavior is pulled forward from P7. Qualification also repaired a latent
P5-K/L post-source-removal liveness gate without weakening its safety boundary: stable P3
target exclusion is still required before source removal, while the later removal-complete
stage is exact-stage-fenced controller bookkeeping. A separate post-P5-O boundary repair now
closes two P3 defects exposed by repeated movement/leadership-transfer stress: a committed
learner promotion now changes the live local role from learner to follower, and follower
AppendEntries never regresses an already-durable commit index when a newly elected leader is
temporarily behind in its `leader_commit` knowledge. Movement reconciliation also requires
local P3 leadership only for operations that actually propose EnterJoint/LeaveJoint; already
durably observed membership states may advance exact-stage-fenced controller bookkeeping.
P5-M now qualifies the general explicit preferred-leader hook independently of placement:
the target must be an ACTIVE committed assignment voter, P3 membership must be stable and
match controller authority, and the current P3 leader must observe the target caught up through
its durable tail before invoking the already-frozen P3 leadership-transfer primitive. Success
is reported only after P3 subsequently names that target as leader; no controller metadata field
decrees partition leadership, and the periodic broker control loop contains no autonomous
leader-balancing policy. Continuous balancing remains P11.
P5-P now qualifies the frozen controller/broker failure matrix: deterministic seeded controller
and placement campaigns, real quorum minority/leader/incarnation crash boundaries, stale authority
fencing, controller-outage data-plane isolation, sustained RF3 writes through reassignment,
source/destination/controller movement failures, stage-by-stage controller-leader death,
snapshot/metadata/feature/producer-ID recovery, movement model checking, and bounded-resource /
control-scheduling audits. Failure qualification also closes two resilience gaps without changing
wire/disk/consensus formats: controller live-state growth is admitted only while worst-case
Snapshot V1 remains inside the existing 64 MiB transport envelope, and rejected incremental broker
metadata history falls back to an authoritative controller snapshot.
P5-Q closes the phase-level compatibility and scope audit: P1 storage is tree-identical to P4
closure, all 11 frozen P2 goldens remain unchanged, the separately qualified P3 compatibility
repair remains 122/122 green, frozen P4 RF3 semantics remain green, and the complete workspace
closes at 380/380. P6-A now freezes the consumer-group/offset-coordination contract and the
176-action execution checklist before any P6 runtime implementation.
P6-B now implements only the frozen representation layer: checked group/member/epoch identities,
bounded canonical Wire V0 bodies for reserved API keys 30..35, and a lower-level
`astralog-groups` crate for exact little-endian Group/Offset durable record and logical-key codecs.
Six new public P6 request goldens plus exact fixtures for all four internal record kinds freeze the
bytes. P6-C now adds only the frozen internal-topic/routing foundation: P5 authority creates or
validates exactly `__astralog_groups` and `__astralog_offsets` at 64 partitions / RF3 /
Preferred, public topic lifecycle paths remain unable to mutate the reserved names, and GroupId
routing reuses the frozen P2 hash. Coordinator authority is observed from the current P3 leader/term
of the groups shard; controller metadata supplies placement and broker addresses but never decrees
partition leadership. The RF3 qualification performs real leadership transfer, stale-term fencing,
leader failure/rerouting and broker restart without recreating either TopicId. API keys 30..35 remain
unadvertised. P6-D now adds the pure deterministic durable group-state layer in `astralog-groups`:
exact state-version sequencing/replay fencing, first-epoch preparation, dynamic/static member
admission, unique InstanceId enforcement, subscription/leave transitions, stale group/member epoch
rejection, strict live-state bounds, phase semantics and byte-deterministic command results. Target
assignments are accepted only as already-computed canonical input. P6-E now supplies that pure
deterministic uniform-sticky assignor: exact bounded flow oracles minimize maximum member load, then
movement from still-eligible owners, then the canonical MemberId owner vector; identical subscriber
sets additionally satisfy the frozen load-spread <=1 rule. Exhaustive bounded enumeration matches the
production solver. P6-F now adds the pure cooperative ownership workflow above that target assignment:
PREPARING state derives an exact pending-epoch OffsetGroupFence, epoch activation is withheld until
that fence is observed committed, moved old owners remain active in REVOKING until exact-authority
release, and target owners remain inactive until a matching released-state OffsetOwnerFence is observed
committed. Exact duplicate fences/RevokeAcks are idempotent, strictly newer prepared epochs may
supersede in-flight work without granting active ownership, committed-history recovery resumes every
prepare/release/fence crash boundary, and a bounded state-space model directly qualifies I-18/I-19.
P6-G now adds the leader-local session/liveness layer without creating a second authority plane: session
timeouts are frozen to 6s..300s, heartbeat cadence uses the deterministic timeout/3 clamp, accepted
heartbeats require exact committed GroupId/GroupEpoch/MemberId/MemberEpoch and refresh only volatile
deadlines, and coordinator leadership change discards those observations and grants every committed
member one fresh full timeout. Expiry itself is only a candidate; the existing durable Leave transition
must commit before membership authority changes. Static replacement is qualified through MemberEpoch+1
plus the normal higher-GroupEpoch path: identical subscriptions preserve every valid assignment while
changed eligibility uses the ordinary P6-E/P6-F rebalance. P6-H now binds those canonical group transitions to
the committed RF3 `__astralog_groups` log: the current P3 shard leader/term is the only coordinator authority,
mutations use P4 quorum commit and are replay-verified before success, and restart/failover reconstructs state from
committed history only. PREPARING, released handoffs, join/revoke/leave leader failures, lost successful responses,
controller-quorum outage isolation and finite count+byte coordinator admission are qualified end to end. A restart
stress failure also closed a P4 recovery ordering gap by explicitly re-fsyncing a validated surviving uncommitted
tail before presenting it to Raft as stable history; P1 format/semantics remain unchanged. P6-H closes at **444/444**
workspace tests. P6-I now binds the frozen offset/fence records to the real RF3 `__astralog_offsets` log: a bounded
logical state machine folds committed group/owner fences and latest offset values, CommitOffsets validates exact
committed group/member/active-assignment authority plus durable effective-owner fencing atomically before one quorum
proposal, and success is replay-verified with the real Raft commit index. FetchOffsets reads committed history only;
metadata and broker-generated commit timestamps survive offsets-leader death, continued writes, replica restart and
leadership transfer back. RF3 qualification also proves the old-group cross-topic race is rejected after group-fence
commit, late old-owner writes are rejected after owner-fence commit, authorized rewind remains legal, and equivalent
lost-response retries cannot revive stale authority. No P9 physical compaction exists. P6-I closes with workspace
formatting, Clippy `-D warnings`, the real P6 RF3 suite **4/4**, and the complete workspace **453/453 PASS**. P6-J now
activates public API keys 30..35 only behind the committed feature-level/internal-topic readiness gate and routes every
public operation through exact current group-coordinator authority. CommitOffsets/FetchOffsets never make the client
race two coordinators: the group leader supplies committed group authority and uses a bounded private broker-to-broker
hop to the independent offsets-shard leader, where the already-qualified P6-I worker executes. The Rust client keeps
bounded non-regressing coordinator/member authority caches, bounded retries/in-flight group work, explicit mutation
ambiguity, and a cooperative revoke helper that can commit revoked-partition offsets before RevokeAck while unaffected
assignments continue. Direct wire qualification proves pre-activation `UNSUPPORTED_API_KEY` and activated wrong-broker
`NOT_LEADER` with an exact coordinator hint and no mutation. Real RF3 E2E covers join, heartbeat, remote-offset
commit/fetch, two-member revoke/handoff, committed offset transfer, Leave and higher-epoch reassignment. P6-J closes
with P6 RF3 **5/5**, workspace Clippy `-D warnings`, formatting/diff hygiene, and **459/459 PASS** in the complete
serialized workspace qualification. P6-K now binds the frozen session semantics to bounded production maintenance:
new group-shard leaders reconstruct committed membership and grant one fresh full session grace per P3 term, silent
members are removed only through the durable Leave/fence path, and committed P5 partition-count changes enter the
ordinary higher-GroupEpoch cooperative rebalance instead of rewriting ownership directly. Real RF3 qualification runs
join and Leave under committed offset traffic, crash-without-Leave and slow-heartbeat cases, coordinator death in
STABLE/PREPARING/REVOKING, offsets-leader death before both group-fence and owner-fence commit, static replacement races
and live topic expansion. Deterministic qualification adds 32 seeded membership-churn campaigns, the existing exhaustive
assignor/cooperative models, all **120** offset-fence event permutations, corruption/gap replay failure, resource one-over
limits and scheduling-pressure checks. P6-K closes with P6 RF3 **9/9**, `astralog-groups` **55/55**, workspace Clippy,
format/diff hygiene and **467/467 PASS** in the complete serialized workspace qualification. P6-L closes the phase by
proving the frozen P1 format/index and original P2 golden sources are unchanged, P6 introduces no Raft source delta
relative to the P5 closure, P4/P5 compatibility suites remain green, and no P7/P8/P9/P10/P11/P12 behavior is pulled
forward. The final pre-commit closure gate is again **467/467 PASS** with P6 RF3 **9/9** and `astralog-groups` **55/55**.
The authoritative closure commit is requalified after commit and receives a Git note containing the exact-HEAD PASS
evidence before P6 is considered closed. P7-A now freezes the producer-idempotency architecture before code: controller
Raft issues ProducerId/ProducerEpoch authority, each data partition owns accepted epoch plus per-partition sequence/dedupe
state, idempotent Produce V1 is quorum-only, exact committed retries return original offsets, and higher-epoch fencing is
deliberately partition-local after committed authority. Storage V1 already contains the required producer fields, so P7
does not redesign P1 bytes. `P7_CHECKLIST.md` defines P7-001…P7-155; only P7-001…014 belong to the architecture freeze.
P7-B now freezes the executable representation boundary without activating producer runtime semantics: checked
ProducerId/ProducerEpoch/ProducerSequence/ProducerAuthority values, InitProducer V0 and Produce V1 codecs, protocol-owned
API-version metadata, dependency-free FIPS-qualified SHA-256 canonical batch identity, ApplicationCommand V2,
ControllerCommand V2 producer-session records and a ControllerSnapshot V2 envelope around exact V1 snapshot bytes plus a
sorted producer-session table. Legacy Produce V0/P2 bytes remain exact; broker P7 dispatch/readiness remains disabled.
P7-B closes with workspace Clippy `-D warnings`, P6 RF3 **9/9** and a complete serialized workspace **485/485 PASS**.
P7-C now activates the authoritative controller producer-session plane without pulling partition dedupe forward: a
canonical ProducerId -> ProducerEpoch table shares the exact P5 durable allocator, new sessions and CAS epoch bumps are
feature-level-3 gated and atomic, mixed ControllerCommand V1/V2 log replay is version-dispatched, and V1/V2 controller
snapshot migration/restart is qualified. Internal InitProducer waits for P3 quorum commit/application before success;
lost new-session/bump responses, controller leader failure and old-leader restart preserve non-reuse/non-regression.
P7-C closes with controller **104/104**, P6 RF3 **9/9**, workspace Clippy `-D warnings`, doc tests and a complete serialized
workspace **493/493 PASS**. Public broker InitProducer/Produce V1 and partition sequence/dedupe logic remain disabled.
P7-D now adds the pure, still-unwired `PartitionProducerStateV1`: admission is side-effect free, only committed application
advances accepted epoch/sequence, exact retained retries return their original committed offsets, gaps/partial overlaps and
changed/too-old retries fail explicitly, lower epochs are fenced and the recent retry window is hard-bounded to five batches.
An exhaustive small-state oracle checks I-15/I-16/I-17 across epoch/sequence/digest combinations. Storage/P4 integration is
still disabled. P7-D closes with replication **56/56**, P6 RF3 **9/9**, workspace Clippy `-D warnings`, doc tests and a
complete serialized workspace **500/500 PASS**.
P7-E activates the producer metadata already reserved in Storage V1 without introducing a new disk format. P4 now
version-dispatches ApplicationCommand V1/V2: legacy V1 remains producer-less, while V2 persists ProducerId/Epoch/base
sequence into the existing idempotent frame header. Committed-only producer reconstruction ignores uncommitted tails,
survives conflict truncation, restart and P4 rebootstrap, and exposes a bounded P9 handoff surface containing accepted epoch,
next sequence and the newest five retry records. Storage validates producer identity/sequence fields and the unchanged frame
CRC covers them; frozen idempotent/non-idempotent Storage V1 fixtures remain exact. P7-E closes with Storage **44/44**,
Replication **62/62**, P6 RF3 **9/9**, workspace Clippy `-D warnings`, doc tests and a complete serialized workspace
**507/507 PASS**. P7-F now binds that committed recovery surface to bounded runtime admission without exposing the public
wire path: unseen/higher producer authority is confirmed by the controller at the broker layer, equal accepted epochs stay
on the data-partition path and continue during controller outage, lower epochs are locally fenced, exact pending retries
coalesce onto one proposal, exact committed retries return original offsets without another append, and committed producer
state advances only after durable quorum commit. The runtime cache is capped at **4,096** reconstructable entries and pending
idempotent work remains under the existing finite proposal count/byte limits. A real three-controller/RF1 broker scenario
qualifies controller confirmation, epoch bump/fencing and complete controller outage continuation while InitProducer/Produce
V1 remain undispatched. P7-F closes with replication **66/66**, P6 RF3 **9/9**, P7 runtime **1/1**, workspace Clippy/format
hygiene and a complete serialized all-target workspace **512/512 PASS**. P7-G now adds the derived
`producer.snapshot` acceleration layer without changing P1 bytes: canonical per-partition snapshot state is CRC-protected,
hard-bounded to **64 MiB**, crash-safely published, validated against committed Raft authority and replayed with committed
suffix frames in bounded pages. Missing/corrupt/wrong-identity/ahead snapshots rebuild from committed P1 history, and cache
eviction followed by snapshot-aware reconstruction returns the same exact retry offsets/Raft index. P7-G closes with
Replication **73/73**, Storage **44/44**, P6 RF3 **9/9**, workspace Clippy/doc tests and a complete serialized workspace
**519/519 PASS**. P7-H now activates readiness-gated public InitProducer V0 and Produce V1 without changing legacy
Produce V0. The Rust client owns bounded ProducerId/Epoch and per-partition sequence/pending state, permits one unresolved
range per producer-partition, retains the exact encoded Produce V1 body across bounded retries, advances sequence only after
authoritative success, resets partition sequence state after a committed epoch bump, and clears local authority after an
ambiguous InitProducer so recovery creates a fresh session. The public RF1 qualification deliberately loses a committed
InitProducer response and a Produce V1 response, proves byte-identical retry, observes exactly one read-committed logical
record/original offsets, then restarts the producer at the exact next epoch with sequence zero. P7-H closes with P7 runtime
**2/2**, broker **22/22**, client **11/11**, P6 RF3 **9/9**, workspace Clippy/format/diff/doc tests **PASS**, and a complete
serialized all-target workspace **524/524 PASS**. Real RF3 producer lost-response/fencing/failover remains P7-I.
P7-I now qualifies that real RF3 boundary end to end: lost committed responses deduplicate, leader death after quorum returns
the original result from a successor, leader death before persistence accepts the sequence once, and a leader-only durable
uncommitted tail is repaired while retry commits one logical copy. Explicit leadership transfer preserves retry identity while
the client refreshes only the routing term. Tentative higher ProducerEpoch state fences locally but is not inherited after
failover; committed higher epoch resets sequence to zero and permanently fences the old epoch across leader loss. Accepted
epochs continue through total controller outage, unseen/higher authority fails without proposal, concurrent epoch bumps have
one CAS winner, ambiguous InitProducer survives controller failover without ProducerId reuse, and RF3 snapshot/restart
reconstructs exact retained retry results. P7-I closes with focused RF3/controller **11/11**, P6 RF3 **9/9**, workspace
Clippy/format/diff/doc tests **PASS**, and a complete serialized all-target workspace **535/535 PASS**. P7-J then closes
P7-136..147 with deterministic state/failover models, resource/corruption bounds and scheduling stress; focused P7-J
qualification plus the complete serialized workspace are **548/548 PASS**. Exact-HEAD requalification exposed a narrow
successor-readiness boundary: idempotent Produce now waits with transient `BUSY` until the new leader has durably committed
a current-term entry, making inherited prior-term history safe committed duplicate-suppression authority. The repaired tree
requalifies P7 failover **12/12**, replication **82/82**, P6 RF3 **9/9**, P7 runtime **2/2**, Clippy/format/diff/doc tests and
the full workspace **548/548 PASS**. The bounded repair is committed at `3373976`, independently repeats the same focused
and complete **548/548** gates on its exact clean HEAD, and carries a qualification Git note. P7-K compatibility auditing
then requalifies frozen P1 Storage V1, original P2 wire goldens, P3/P4 replication semantics and P5/P6 controller/group
semantics while confirming no P8-P12 implementation was pulled forward. P7-K's final closure tree passes focused models,
real RF3 failure qualification, frozen compatibility suites, fmt/Clippy/doc/diff hygiene and the complete serialized
workspace **548/548**. The sole P7 closure commit is `d0d0e85`, its exact SHA carries the required
qualification Git note, and P7 is authoritatively closed. Two post-P7 **test-harness-only** hardenings culminate at
`1384279`: the earlier `6f17a29` repair removes an invalid offsets-coordinator readiness dependency from the P6
silent-member failover fixture, and `1384279` gives the second client in the long public-integration fixture a
bounded 20-second setup deadline. Exact `1384279` passes P6 **9/9**, the complete workspace **548/548**,
fmt/Clippy/doc/diff gates, and carries a qualification Git note without changing production semantics. P8-A then
froze the batching/compression/flow-control/performance contract and execution ledger. P8-B now implements only
controller-owned topic performance metadata and feature activation: checked None/LZ4/Zstd policy metadata,
atomic ControllerCommand V3 CreateTopic-with-performance, ControllerSnapshot V3, deterministic V1/V2 defaults,
committed feature level 4 / feature key 14 activation, and additive CreateTopic/DescribeTopic V1. Frozen V0/V1
Produce/Fetch behavior and prior controller bytes remain unchanged; AlterTopicConfig remains unavailable.
P8-B qualification passes focused controller **6/6**, protocol **1/1**, public dynamic-broker P8 **1/1**,
original P2/P6/P7 goldens **11/11 + 6/6 + 4/4**, fmt/Clippy/doc/diff gates and the complete serialized all-target
workspace **557/557**. P8-C now activates the reserved Storage V1 LZ4/Zstd identifiers with exact pinned codec
profiles, no-gain fallback, bounded preallocation/decode, immutable compressed goldens, mixed-codec restart/index
rebuild/truncation, compressed rebootstrap and P7 producer-state equivalence. Committed P8-B topic policy is mapped
into the replication persistence codec only after feature-level-4 readiness; default historical behavior remains None.
Focused storage **53/53** and replication **84/84** are green, the real dynamic-broker P8 case proves a Zstd topic
physically stores Zstd data, P6 is **9/9**, P7 failover is **12/12**, P7 runtime is **2/2**, and the deterministic
bounded P8-C gate covers **568/568** tests with fmt/Clippy/doc/diff gates PASS. P8-D now adds the opt-in bounded
per-topic-partition client accumulator over the frozen Produce V0/V1 formats: exact record/byte/linger closure,
unchanged keyed/sticky partitioning, explicit flush, metadata-invalidation closure, shutdown drain, byte/record/partition
caps, and P7 one-unresolved-range compatibility. Buffered V1 records consume sequence authority only when a closed batch
reaches the existing P7 send boundary, and the pending encoded V1 body remains the retry authority. The real P8 broker/
client campaign proves buffered V0 offsets 0..1, buffered V1 offsets 2..3, explicit flush at offset 4, shutdown drain at
offset 5, exact six-record fetch order, and continued committed Zstd persistence. P8-D qualification is **578/578 PASS**
by bounded target decomposition with workspace Clippy/doc/format/diff and frozen Storage/P2/P6/P7 golden checks PASS.
P8-057..073 are closed and `b421f2d` carries its exact-HEAD PASS note. The P8-E design audit exposed the historical
single-token persistence-frontier conflict, and ADR-018 resolved it through the exact-HEAD-qualified V2 re-freeze
`370ae92`. P8-E now implements that frozen design: a bounded ordered append-token queue with a separate staged logical
frontier, one authoritative durable frontier, bounded count/bytes/group delay, FIFO completion after proven sync,
hard barriers for hard-state/commit/truncate/snapshot/rebootstrap work, and deterministic logical-token versus
physical-sync counters. Grouped execution is byte/logically equivalent to sequential persistence, leader/quorum ack
semantics are unchanged, crash-before-sync and post-sync/pre-completion ambiguity are qualified, and mixed
None/LZ4/Zstd history preserves order/offsets. Pre-commit qualification is **596/596 PASS** by bounded decomposition
with workspace Clippy/doc/format/diff and frozen Cargo/Storage/P2/P6/P7 golden checks PASS. The first closure commit
`ff4d0b4` was not granted exact-HEAD authority because the P6 churn suite exposed a JoinSubscribe post-commit
ambiguity boundary. The bounded compatibility repair is authoritative at `e929191`: its exact SHA passes the complete
**596/596** P8-E gate plus mechanical/frozen-byte checks and carries the P8-E PASS note. P8-F is now authoritative at
`039c700`: finite per-follower message/byte windows, bounded Raft catch-up effect construction, term/incarnation/
connection/snapshot fencing, reserved Raft-control capacity, slow-follower/restart qualification, committed-only bounded
Fetch scanning, burst-coalesced long polling and move-based response assembly preserve unchanged Fetch V0 bytes. Its
exact SHA carries the P8-F PASS note after **604/604** exact-HEAD qualification. P8-G implements P8-108..126 only:
finite global broker data count/byte admission, strict low/high watermark hysteresis, control-plane reserve, broker-local
anonymous/topic token buckets, pre-mutation `BROKER_BUSY` + nonzero response `throttle_ms`, bounded no-response `acks=0`
overload semantics and client throttle honoring. Real broker pressure and RF3 simulation qualify slow-consumer,
producer/follower/consumer pressure and control progress. P8-G is authoritative at `98bf895`: its exact SHA carries the
PASS note after focused **14/14** and complete **618/618** exact-HEAD qualification plus mechanical/frozen checks. P8-H
now implements P8-127..143 as repository-owned benchmark tooling: a release-only clean-SHA runner, canonical manifest and
raw bundle, deterministic frozen-factor coverage matrix and seeded workloads, separated warmup/measurement, hard-bounded
raw latency retention, system-metric snapshots, deterministic raw-derived Markdown reporting, repeated-run comparison
gating and a short offline workspace smoke test. The optimized benchmark binary builds and focused tooling is **19/19
PASS**; the complete pre-commit workspace gate is **637/637 PASS**. P8-I then closed compatibility and retained the real
engineering baseline. Final P8 closure is authoritative at `37c07a5`: its exact SHA carries the P8 PASS note after focused
**87/87** and complete serialized **638/638** qualification plus release-build, Clippy, doc, fmt, diff and frozen-source
checks. P9-A is authoritative at `b7c8552`: its exact SHA carries the P9-A PASS note after the same complete **638/638**
serialized compatibility gate and freezes the retention/compaction contract plus sparse retained-history Storage V2
architecture. P9-B is authoritative at `f583b6b`: its exact SHA carries the P9-B PASS note after focused **14/14** and
complete serialized **653/653** qualification. It owns checked controller retention metadata, command/snapshot V4, feature
key 15 / committed cluster feature level 5 activation, CreateTopic/DescribeTopic V2 and AlterTopicConfig V0 readiness gating.
P9-C is authoritative at `dc39dd7`: its exact SHA carries the P9-C PASS note after focused **14/14** and complete serialized
**667/667** qualification. P9-D is authoritative at `fe64a0a`: its exact SHA carries the P9-D PASS note after focused **10/10**,
combined P9 **38/38** and complete serialized **677/677** qualification. P9-E is authoritative at `9da9aac`: its exact SHA
carries the P9-E PASS note after focused **8/8**, combined P9 **45/45**, and complete serialized **684/684** qualification.
P9-E freezes deterministic bounded COW keyed compaction, sparse original-offset history, snapshot/policy/manifest publication
fences and generation-owned readers, while retaining producer-bearing sources until a committed P9-F checkpoint exists.
P9-F implements only P9-112..129 as the **current bounded checkpoint**: canonical bounded producer-state
maintenance commands, quorum-committed checkpoint before source reclamation, persisted-snapshot-floor gating, retained
checkpoint plus committed suffix P7 reconstruction, exact retry/epoch/sequence preservation after compacted-source removal,
derived-sidecar deletion and restart, 4,096-producer bound, and RF3 checkpoint failover. The RF3 exact-retry regression
passed six consecutive recovered-tree runs. Its authoritative exact-HEAD parent is
`cd064a4` and carries the `P9-F exact-HEAD qualification: PASS` Git note.
**P9-G implementation/qualification is active but not yet released:** the
bounded local closure candidate was first committed at `43be8dd`, then
amended as `4564d39` and `a952602`; it adds identity-fenced all-voter
safe compaction, joint-consensus tombstone safety and retained-history
Rebootstrap V2. Two pre-commit workspaces passed 730/730; `4564d39` exact
HEAD failed P5 10/12. `a952602` exact HEAD first passed 730/730 but its
second workspace failed P5 10/12. Independent diagnostics identified an
unsafe former leader's rebootstrap veto killing a recoverable partition
actor; the new uncommitted candidate keeps it non-serving for genuine P3
recovery and blocks stale Fetch/idempotent acknowledgements. Targeted
RF3/RF4 recovery testing has passed, but complete exact-clean-HEAD
repeatability and the P9-G PASS Git note are still missing.
Historical failed runs are retained in `docs/P9_G_WORK_IN_PROGRESS.md`;
`docs/P9_G_CLOSURE_REPORT.md` records the candidate and limitations.
P9-H/I and P10-P12 remain disabled until their predecessor gates pass.

## Master project explanation and interview preparation

The [AstraLog Master Engineering & Interview Guide](docs/ASTRALOG_MASTER_GUIDE.md) explains the architecture in layman and technical terms, annotates every tracked file and directory, traces failure invariants, records benchmark limits and includes 60 adversarial interview questions with defensible answers. The guide is an educational companion, not production qualification.

## Canonical documents

1. [AstraLog Project Bible](docs/ASTRALOG_BIBLE.md)
2. [Phase Plan & Exit Gates](docs/PHASE_PLAN.md)
3. [Correctness Invariants](docs/INVARIANTS.md)
4. [Research & Design References](docs/RESEARCH_REFERENCES.md)
5. [Architecture Decision Log](docs/DECISIONS.md)
6. [Feature-to-Phase Matrix](docs/FEATURE_MATRIX.md)
7. [P0 Closure Report](docs/P0_CLOSURE_REPORT.md)
8. [P1 Execution Checklist](docs/P1_CHECKLIST.md)
9. [P1 Closure Report](docs/P1_CLOSURE_REPORT.md)
10. [Wire Protocol V1](docs/PROTOCOL_V1.md)
11. [P2 Execution Checklist](docs/P2_CHECKLIST.md)
12. [P2 Closure Report](docs/P2_CLOSURE_REPORT.md)
13. [Raft Core Contract V1](docs/RAFT_V1.md)
14. [P3 Execution Checklist](docs/P3_CHECKLIST.md)
15. [P3 Foundation Qualification](docs/P3_FOUNDATION_REPORT.md)
16. [P3 Pre-Vote Qualification](docs/P3_PREVOTE_REPORT.md)
17. [P3 RequestVote Qualification](docs/P3_REQUESTVOTE_REPORT.md)
18. [P3 Leader Initialization Qualification](docs/P3_LEADER_INIT_REPORT.md)
19. [P3 AppendEntries Heartbeat/Prefix Qualification](docs/P3_APPEND_HEARTBEAT_REPORT.md)
20. [P3 Closure Report](docs/P3_CLOSURE_REPORT.md)
21. [P4 Replicated Partition Contract V1](docs/P4_REPLICATION_V1.md)
22. [P4 Execution Checklist](docs/P4_CHECKLIST.md)
23. [P4 Closure Report](docs/P4_CLOSURE_REPORT.md)
24. [P5 Controller and Placement Contract V1](docs/P5_CONTROLLER_V1.md)
25. [P5 Execution Checklist](docs/P5_CHECKLIST.md)
26. [P5-A Contract Freeze Report](docs/P5_CONTRACT_FREEZE_REPORT.md)
27. [P5-B State Machine Qualification](docs/P5_STATE_MACHINE_REPORT.md)
28. [P5-C Persistence Qualification](docs/P5_PERSISTENCE_REPORT.md)
29. [P5-D Controller Runtime Qualification](docs/P5_RUNTIME_REPORT.md)
30. [P5-E Controller Bootstrap Qualification](docs/P5_BOOTSTRAP_REPORT.md)
31. [P5-F Broker Membership Qualification](docs/P5_MEMBERSHIP_REPORT.md)
32. [P5-G Heartbeat / Failure Detector Qualification](docs/P5_HEARTBEAT_REPORT.md)
33. [P5-H Topic Metadata / Placement Qualification](docs/P5_PLACEMENT_REPORT.md)
34. [P5-I Metadata Epochs / Deltas + Broker Cache Qualification](docs/P5_METADATA_REPORT.md)
35. [P5-J Dynamic P4 Integration Qualification](docs/P5_DYNAMIC_INTEGRATION_REPORT.md)
36. [P5-K/L Replica Movement Qualification](docs/P5_MOVEMENT_REPORT.md)
37. [P5-N Active Cluster Feature Level Qualification](docs/P5_FEATURE_LEVEL_REPORT.md)
38. [P5-O Producer-ID Allocator Qualification](docs/P5_PRODUCER_ID_REPORT.md)
39. [Post-P5-O Raft / Movement Boundary Repair](docs/P5_BOUNDARY_REPAIR_REPORT.md)
40. [P5-M Leadership Hook Qualification](docs/P5_LEADERSHIP_REPORT.md)
41. [P5-P Failure Qualification](docs/P5_FAILURE_REPORT.md)
42. [P5 Closure Report](docs/P5_CLOSURE_REPORT.md)
43. [P6 Consumer Groups / Offsets Contract V1](docs/P6_GROUPS_V1.md)
44. [P6 Execution Checklist](docs/P6_CHECKLIST.md)
45. [P6-A Contract Freeze Report](docs/P6_CONTRACT_FREEZE_REPORT.md)
46. [P6-B Types / Codecs / Golden Bytes Qualification](docs/P6_CODECS_REPORT.md)
47. [P6-C Internal Topics / Coordinator Routing Qualification](docs/P6_ROUTING_REPORT.md)
48. [P6-D Pure Durable Group State Machine Qualification](docs/P6_STATE_MACHINE_REPORT.md)
49. [P6-E Deterministic Uniform-Sticky Assignor Qualification](docs/P6_ASSIGNOR_REPORT.md)
50. [P6-F Cooperative Revocation / Activation Qualification](docs/P6_COOPERATIVE_REPORT.md)
51. [P6-G Heartbeats / Session Timeout / Static Membership Qualification](docs/P6_SESSION_REPORT.md)
52. [P6-H Replicated Group Coordinator / Recovery Qualification](docs/P6_COORDINATOR_REPORT.md)
53. [P6-I Durable Offsets / Fencing Qualification](docs/P6_OFFSETS_REPORT.md)
54. [P6-J Broker / Client Integration Qualification](docs/P6_INTEGRATION_REPORT.md)
55. [P6-K Failure / Churn / Model Qualification](docs/P6_FAILURE_CHURN_REPORT.md)
56. [P6 Closure Report](docs/P6_CLOSURE_REPORT.md)
57. [P7 Idempotent Producer Contract V1](docs/P7_PRODUCER_V1.md)
58. [P7 Execution Checklist](docs/P7_CHECKLIST.md)
59. [P7-A Contract Freeze Report](docs/P7_CONTRACT_FREEZE_REPORT.md)
60. [ADR-017 Producer Epoch Authority](docs/ADR_017_PRODUCER_EPOCH_AUTHORITY.md)
61. [P7-B Types / Codecs / Golden Bytes Qualification](docs/P7_CODECS_REPORT.md)
62. [P7-C Controller Producer-Session Authority / V2 Persistence Qualification](docs/P7_CONTROLLER_REPORT.md)
63. [P7-D Pure Partition Producer State Machine Qualification](docs/P7_STATE_MACHINE_REPORT.md)
64. [P7-E Storage V1 Producer Metadata / ApplicationCommand V2 Qualification](docs/P7_STORAGE_REPORT.md)
65. [P7-F Runtime Admission / Pending Duplicate Suppression Qualification](docs/P7_RUNTIME_REPORT.md)
66. [P7-G Producer Snapshot / Crash Recovery Qualification](docs/P7_SNAPSHOT_REPORT.md)
67. [P7-H Broker / Client Integration Qualification](docs/P7_INTEGRATION_REPORT.md)
68. [P7-I Lost-Response / Fencing / Real RF3 Failover Qualification](docs/P7_FAILOVER_REPORT.md)
69. [P7-J Deterministic Models / Resource Bounds / Scheduling Stress Qualification](docs/P7_MODEL_STRESS_REPORT.md)
70. [P7-J Current-Term Leader Readiness Boundary Repair](docs/P7_J_BOUNDARY_REPAIR_REPORT.md)
71. [P7 Closure Report](docs/P7_CLOSURE_REPORT.md)
72. [P8 Batching / Compression / Flow Control / Performance Contract V1](docs/P8_PERFORMANCE_V1.md)
73. [P8 Execution Checklist](docs/P8_CHECKLIST.md)
74. [P8-A Contract Freeze Report](docs/P8_CONTRACT_FREEZE_REPORT.md)
75. [P8-B Controller Topic-Performance Metadata Qualification](docs/P8_CONTROLLER_METADATA_REPORT.md)
76. [P8-C Storage V1 Compression and Recovery Qualification](docs/P8_STORAGE_COMPRESSION_REPORT.md)
77. [P8-D Client Per-Partition Accumulator and Batching Qualification](docs/P8_CLIENT_BATCHING_REPORT.md)
78. [P8-E Persistence Group-Commit Architecture Blocker](docs/P8_E_ARCHITECTURE_BLOCKER.md)
79. [Accepted P8-E Persistence Frontier ADR](docs/ADR_P8_E_PERSISTENCE_FRONTIER.md)
80. [Persistence Frontier V2](docs/PERSISTENCE_FRONTIER_V2.md)
81. [P8-E Persistence Frontier Architecture Re-Freeze](docs/P8_E_REFREEZE_REPORT.md)
82. [P8-E Persistence Group Commit Qualification](docs/P8_PERSISTENCE_GROUP_COMMIT_REPORT.md)
83. [P8-F Replication Pipelining and Fetch Tuning Qualification](docs/P8_REPLICATION_FETCH_REPORT.md)
84. [P8-G Bounded Backpressure, Watermarks and Quotas Qualification](docs/P8_BACKPRESSURE_QUOTA_REPORT.md)
85. [P8-H Benchmark Harness and Deterministic Baseline Tooling Qualification](docs/P8_BENCHMARK_HARNESS_REPORT.md)
86. [P8 Engineering Baseline — Generated Report](docs/baselines/P8_ENGINEERING_BASELINE.md)
87. [P8 Closure Report](docs/P8_CLOSURE_REPORT.md)
88. [P9 Retention and Log Compaction Contract V1](docs/P9_RETENTION_COMPACTION_V1.md)
89. [Retained-History Storage Format V2](docs/FORMAT_V2.md)
90. [P9 Execution Checklist](docs/P9_CHECKLIST.md)
91. [ADR-019 — Sparse Retained-History Storage V2](docs/ADR_019_P9_SPARSE_RETAINED_HISTORY.md)
92. [P9-A Contract / Storage V2 Architecture Freeze Report](docs/P9_CONTRACT_FREEZE_REPORT.md)
93. [P9-B Controller Retention Metadata and Public Codec Qualification](docs/P9_CONTROLLER_RETENTION_METADATA_REPORT.md)
94. [P9-C Storage V2 and Manifest Inventory Qualification](docs/P9_STORAGE_V2_MANIFEST_REPORT.md)
95. [P9-D Replicated Log Start and Delete Retention Qualification](docs/P9_LOG_START_DELETE_RETENTION_REPORT.md)
96. [P9-E Copy-on-Write Keyed Compaction Qualification](docs/P9_KEYED_COMPACTION_REPORT.md)
97. [P9-F Producer Checkpoint and Compaction-Safe P7 Recovery Qualification](docs/P9_PRODUCER_CHECKPOINT_REPORT.md)

## One-sentence architecture

AstraLog uses a **small Raft controller quorum for authoritative cluster metadata** and a **Raft group per data partition for ordered, replicated event storage**, with clients routing directly to partition leaders after bootstrapping from any broker.

## Non-negotiable honesty rule

Benchmark numbers in the README are never hand-written aspirations. They must be generated from committed benchmark tooling, include hardware/software/configuration metadata, and be reproducible from a tagged commit.
