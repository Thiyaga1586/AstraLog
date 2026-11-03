# AstraLog Current State

## Project stage

**P9 — Retention and Log Compaction**

Status: **INTERVIEW DEMO COMPLETED; P9-F AUTHORITATIVELY CLOSED at cd064a4 (exact-HEAD PASS); P9-G LOCAL CLOSURE CANDIDATE 563ede6 COMMITTED, EXACT-HEAD NOT YET QUALIFIED. P9-H/I AND P10–P12 WERE NOT IMPLEMENTED.**

The 2026-11-03 portfolio-only milestone provides a real, runnable RF1/RF3
demo with release-mode p50/p95/p99 and throughput measurements, three focused
restart/failover/deduplication passes, and a one-command launcher. See
`docs/INTERVIEW_READY.md`. This does not supersede the P9-G production
qualification gate or erase the known previously failing P7 full-suite repeat.

**2026-11-03 adversarial remediation (not a release gate):** See
`docs/REMEDIATION_NOTES.md`. Test-only P7 logical-clock and P9-G election
timing fixes, stronger benchmark data/latency oracles, CI/tooling provenance,
and publication hygiene were added. A new serialized all-features full-workspace
run passed **732/732** with zero failures, including P5/P9-G and P7, but an
intermittent regression cannot be ruled out by a single green run. P9-G
production qualification and P9-H/I/P10–P12 remain outside this milestone.

**2026-11-03 follow-up adversarial rerun:** A later serialized workspace run
reproduced a P9-G follower fatal during a V1 InstallSnapshot package whose
boundary was *already committed* locally. The V1 runtime now mirrors the
existing V2 committed-prefix guard: discard that obsolete replacement package
but allow P3 to validate/persist snapshot metadata. A new exact-byte unit
regression and the previously failing focused P9-G integration test passed.
See `docs/AUDIT_CLOSURE_MATRIX.md`. This is a narrowly scoped product fix,
not evidence of full P9-G production release qualification.

A subsequent full-workspace run passed P5/P9 **12/12** but failed the last
P6 integration case **8/9** when the initial `JoinSubscribe` response became
ambiguous. The client correctly rejected blind mutation replay. A test-only
read-from-committed-history reconciliation was added; its focused case passed
and the entire serialized P6 target subsequently passed **9/9**. That still
does not convert the earlier failed full-workspace attempt into a PASS. A
further serialized **all-features full-workspace** local run on the corrected
source tree subsequently passed **737/737**, with zero failures: P5/P9-G
**12/12**, P6 **9/9**, P7 **12/12**, and separate-process static RF3
leader-kill coverage. This is strong local regression evidence, not P9-G
exact-HEAD production qualification or Linux-hosted CI proof.

## Qualified implementation

- P0 architecture remains closed/frozen
- Rust workspace initialized and Rust 1.98.1 pinned
- `astralog-common` and `astralog-storage` implemented
- Storage Format V1 frozen
- CRC32C data/control frame codec with bounded decoding
- checksummed segment and partition metadata headers
- append-only segment writer with partial-write rollback
- sequential segment scanner and frame-at-position reader
- sparse offset/time/Raft indexes rebuilt from authoritative log bytes
- partition log with size/time roll hooks and logical stream/Raft positions
- durable local boundary separated from merely visible uncommitted bytes
- committed-boundary placeholder persisted without implementing Raft early
- crash recovery truncates only invalid uncommitted active tails
- committed corruption is surfaced and never silently repaired
- copy-on-write generation manifest with monotonic generation fencing
- V1 golden byte fixtures
- deterministic corruption/truncation/fuzz-style adversarial tests
- CI and local qualification scripts
- Wire Protocol V1 frozen before implementation at commit `20da30c`
- `astralog-protocol` implemented with bounded big-endian primitive codecs
- Hello V1 negotiation and CRC32C-protected frame codec
- incremental frame decoder for arbitrary TCP fragmentation
- all P2 API V0 body codecs
- permanent public API/error registries and retry classification
- protocol-owned Murmur3 keyed partitioner and sticky null-key partitioner
- bounded single-broker TCP runtime with Hello -> ApiVersions -> Ready gating
- bounded connections, frame buffers, in-flight requests, and response bytes
- multiplexed correlation IDs with out-of-order completion
- request/assembly deadlines and slowloris eviction
- durable CRC32C single-broker metadata catalog
- stable cluster/node identity, broker incarnation, and monotonic metadata epoch
- Create/Delete/DescribeTopic and Metadata
- P1-backed Produce, committed-only Fetch, long polling, and ListOffsets
- explicit RF=1 `acks=leader` / `acks=quorum` local durability semantics
- Rust client with metadata cache, broker routing abstraction, and bounded in-flight requests
- non-idempotent P2 Produce ambiguity documented rather than silently retried
- 20 immutable protocol/partition compatibility fixtures
- Raft Core V1 safety/persistence contract frozen at commit `de0e93d`
- `astralog-raft` foundation crate with no external dependencies
- checked Raft-native `NodeId`, `Term`, and synthetic-zero-capable `LogIndex`
- deterministic stable/joint membership model with sorted member iteration
- persistent hard state, snapshot metadata, retained log, and recovery validation
- volatile follower/learner state and deterministic leader-progress containers
- explicit persistence batch/token abstraction with one in-flight durability barrier
- deterministic input/effect engine skeleton with no disk, socket, clock, RNG, or thread I/O
- persistence failure is fatal/non-serving rather than reported as success
- deterministic election timing contract with injected timeout samples
- PreVote, RequestVote, and AppendEntries request/response message models implemented
- voter timeout enters `PreCandidate` without changing durable term/vote state
- prospective pre-vote term is checked `current_term + 1`
- pre-vote request emission follows sorted `NodeId` order
- grant logic enforces voter eligibility, prospective term, log freshness, and recent-leader suppression
- snapshot boundary participates in log-freshness comparison
- learners neither campaign nor grant pre-votes
- stable pre-vote quorum counts a strict voter majority
- joint pre-vote quorum independently requires old and new voter majorities
- duplicate/stale/foreign responses cannot inflate pre-vote quorum
- pre-vote quorum is reported once but does not start a real election
- valid same-current-term leader contact cancels/suppresses pre-vote; mismatched/synthetic contact does not
- AppendEntries response-side durable peer progress and deterministic conflict backtracking implemented
- explicit `BeginElection` gate requires a matching qualified pre-vote quorum
- campaign term/self-vote is staged as one hard-state persistence action
- RequestVote requests are emitted only after durable self-vote completion
- RequestVote request/response message model implemented
- granted votes are persisted before positive VoteResponse
- higher-term RequestVote handling durably advances term and clears/sets vote before response
- higher-term vote responses durably fence candidates before follower step-down is exposed
- one durable vote per term survives restart; crash-before-fsync creates no acknowledged vote
- learners persist higher terms but never grant or count voting authority
- foreign/non-voter RequestVote identities cannot raise or fence the group term
- stable and joint-consensus election vote tracking implemented deterministically
- election quorum emits `ElectionQuorumReached` once but leaves the node `Candidate`
- campaign persistence failure is fatal and emits no RequestVote
- hard-state persistence staging is core-owned; embedding code cannot bypass consensus ordering
- explicit `WinElection` gate consumes only a qualified current-term candidate quorum
- legal `Candidate -> Leader` transition sets `leader_id=self`
- campaign/pre-vote state is cleared on victory
- deterministic progress is initialized for voters and learners
- peers start at `match_index=0` and `next_index=pre-barrier durable last + 1`
- self progress initially reflects only the already-durable pre-barrier log tail
- current-term `LeaderBarrier` is staged as the only local append on victory
- barrier index follows retained durable tail or snapshot boundary
- leader victory emits no AppendEntries/heartbeat
- barrier is absent from stable log until persistence completion
- self `match_index` advances to the barrier only after its persistence completes
- barrier durability does not advance commit index
- barrier persistence failure is fatal/non-serving and never fabricates self replication
- AppendEntries lower-term requests reject using the local current term
- foreign/non-voter leader identities cannot raise the local term
- higher-term AppendEntries requests persist the new term before dependent processing/response
- higher-term AppendEntries persistence failure emits no response
- valid current-term AppendEntries establishes one leader authority per term
- current-term candidate steps down before heartbeat prefix validation
- competing same-term leader authority is rejected without replacing accepted authority
- learners accept valid heartbeat authority while remaining non-voting
- prefix matching covers the snapshot boundary and retained durable suffix
- prefix below the snapshot floor rejects with `snapshot_index + 1`
- missing tail rejects with `local_last_index + 1`
- retained term mismatch returns conflict term plus first retained index for that term
- snapshot-boundary term mismatch returns snapshot term/index hint
- empty heartbeat success reports the durable matched prefix index
- same-term AppendEntries responses advance durable peer progress monotonically
- higher-term AppendEntries responses durably fence a leader/candidate and clear leader progress
- non-empty AppendEntries retains identical entries, truncates only conflicting uncommitted suffixes, and persists before success
- committed-prefix conflict is fatal and never truncated
- follower `leader_commit` advancement is persisted before authoritative commit/apply/success
- leader conflict-term/index hints deterministically backtrack peer `next_index`
- leader sends replication in stable `NodeId` order and switches lagging peers to snapshot mode below retained history
- leader quorum commit requires a current-term entry and persists the commit watermark before apply/visibility
- prior-term entries commit only by prefix implication through a committed current-term entry
- check-quorum steps down leaders that lose recent voter quorum; joint configurations require old+new recent majorities
- application proposals are leader-only and bounded by configured uncommitted application bytes
- learners replicate but never vote, campaign, or count toward quorum
- learner promotion requires durable catch-up through current commit
- EnterJoint becomes effective on durable storage; joint elections/commit require independent old/new majorities
- LeaveJoint is accepted only after the joint entry commits; membership changes are serialized
- leader self-removal steps down once the new stable configuration commits
- recovery reconstructs effective membership from snapshot plus durable configuration log
- local snapshots require an applied+committed index and stable resolved membership
- InstallSnapshot preserves matching suffixes, discards conflicting suffixes, and responds only after durability
- snapshot state-machine installation is ordered before later log application
- leadership transfer requires an eligible caught-up voter, pauses new proposals, and uses legal `TimeoutNow` election
- failed leadership transfer times out and resumes ordinary proposal service if leadership is retained
- deterministic simulator supports seeded logical time, packet drop/dup/delay/reorder, crash/restart, and persistence delay/failure
- all mandatory deterministic scenarios 1–25 are represented and green
- seeded simulation traces are exactly reproducible; 64 seeded campaigns × 300 steps preserve safety
- bounded mechanically checked formal model covers election/log/commit/joint-membership/crash safety invariants

## P1 qualification

Pinned Linux qualification environment: `rust:1.98.1`.

- `cargo fmt --all -- --check`: **PASS**
- `cargo clippy --workspace --all-targets --all-features -- -D warnings`: **PASS**
- `cargo test --workspace --all-features`: **PASS**
- `astralog-common`: **2 passed, 0 failed**
- `astralog-storage`: **39 passed, 0 failed**
- total unit tests: **41 passed, 0 failed**
- doc tests: **PASS**
- `git diff --check`: **PASS**

See `P1_CLOSURE_REPORT.md` for detailed proof obligations.

## P2 qualification

Pinned Linux qualification environment: `rust:1.98.1`.

- `cargo fmt --all -- --check`: **PASS**
- `cargo clippy --workspace --all-targets --all-features -- -D warnings`: **PASS**
- `cargo test --workspace --all-features`: **PASS**
- `astralog-common`: **2 passed, 0 failed**
- `astralog-broker`: **4 unit + 9 network/integration = 13 passed, 0 failed**
- `astralog-client`: **3 passed, 0 failed**
- `astralog-protocol`: **33 unit/adversarial + 11 golden tests = 44 passed, 0 failed**
- `astralog-storage`: **39 passed, 0 failed**
- workspace total: **101 passed, 0 failed**
- doc tests: **PASS**
- `git diff --check`: **PASS**

## P3 pre-vote checkpoint qualification

Pinned Linux qualification environment: `rust:1.98.1`.

- `cargo fmt --all -- --check`: **PASS**
- `cargo clippy --workspace --all-targets --all-features -- -D warnings`: **PASS**
- `cargo test --workspace --all-features`: **PASS**
- `astralog-raft`: **27 passed, 0 failed**
- frozen P1/P2 tests: **101 passed, 0 failed**
- workspace total: **128 passed, 0 failed**
- all doc-test targets: **PASS**
- `git diff --check`: **PASS**
- final qualification rerun offline from the pinned quality container

See `P3_PREVOTE_REPORT.md` for the checkpoint proof obligations.

## P3 RequestVote checkpoint qualification

Pinned Linux qualification environment: `rust:1.98.1`.

- `cargo fmt --all -- --check`: **PASS**
- `cargo clippy --workspace --all-targets --all-features -- -D warnings`: **PASS**
- `cargo test --workspace --all-features`: **PASS**
- `astralog-raft`: **42 passed, 0 failed**
- frozen P1/P2 tests: **101 passed, 0 failed**
- workspace total: **143 passed, 0 failed**
- all doc-test targets: **PASS**
- `git diff --check`: **PASS**
- final qualification rerun offline from the pinned quality container

See `P3_REQUESTVOTE_REPORT.md` for the checkpoint proof obligations.

## P3 leader-initialization checkpoint qualification

Pinned Linux qualification environment: `rust:1.98.1`.

- `cargo fmt --all -- --check`: **PASS**
- `cargo clippy --workspace --all-targets --all-features -- -D warnings`: **PASS**
- `cargo test --workspace --all-features`: **PASS**
- `astralog-raft`: **48 passed, 0 failed**
- frozen P1/P2 tests: **101 passed, 0 failed**
- workspace total: **149 passed, 0 failed**
- all doc-test targets: **PASS**
- `git diff --check`: **PASS**
- final qualification rerun offline from the pinned quality container

See `P3_LEADER_INIT_REPORT.md` for the checkpoint proof obligations.

## P3 AppendEntries heartbeat/prefix checkpoint qualification

Pinned Linux qualification environment: `rust:1.98.1`.

- `cargo fmt --all -- --check`: **PASS**
- `cargo clippy --workspace --all-targets --all-features -- -D warnings`: **PASS**
- `cargo test --workspace --all-features`: **PASS**
- `astralog-raft`: **69 passed, 0 failed**
- frozen P1/P2 tests: **101 passed, 0 failed**
- workspace total: **170 passed, 0 failed**
- all doc-test targets: **PASS**
- `git diff --check`: **PASS**
- final qualification rerun offline from the pinned quality container

See `P3_APPEND_HEARTBEAT_REPORT.md` for the checkpoint proof obligations.

## P3 closure qualification

Pinned Linux qualification environment: `rust:1.98.1`.

- `cargo fmt --all -- --check`: **PASS**
- `cargo clippy --workspace --all-targets --all-features -- -D warnings`: **PASS**
- `cargo test --workspace --all-features`: **PASS**
- `astralog-raft`: **120 passed, 0 failed**
- frozen P1/P2 tests: **101 passed, 0 failed**
- workspace total: **221 passed, 0 failed**
- all doc-test targets: **PASS**
- deterministic mandatory scenarios 1–25: **PASS**
- exactly reproducible seeded trace test: **PASS**
- large deterministic campaign: **64 seeds × 300 steps, PASS**
- bounded formal state-space safety checker: **PASS**
- `git diff --check`: **PASS**
- no P1/P2 source changes in the P3 closure diff
- `astralog-raft` remains independent of broker/client/storage/network runtime code

See `P3_CLOSURE_REPORT.md` for the complete P3 proof map.

## P4 closure qualification

Pinned Linux qualification environment: `rust:1.98.1`.

- `cargo fmt --all -- --check`: **PASS**
- `cargo clippy --workspace --all-targets --all-features --offline -- -D warnings`: **PASS**
- `cargo test --workspace --all-features --offline`: **PASS**
- workspace total: **272 passed, 0 failed**
- `astralog-raft`: **120 passed, 0 failed**
- `astralog-replication`: **46 passed, 0 failed**
- RF=3 broker TCP integration: **PASS**
- RF=3 broker TCP integration repeated 5 consecutive times: **5/5 PASS**
- `astralog-storage`: **43 passed, 0 failed**
- protocol golden vectors: **11 passed, 0 failed**
- all doc-test targets: **PASS**
- `git diff --check`: **PASS**
- P3 consensus source diff from P3 closure: **empty**
- P1 `format.rs` and `manifest.rs` diff from P3 closure: **empty**

See `P4_CLOSURE_REPORT.md` for the complete implementation and failure-proof map.

## P5 qualified implementation

- P5 controller/placement contract V1 frozen at commit ad265118e5e6e422f7f3c6fb7baf367b1b56907e
- astralog-controller pure deterministic state-machine crate added
- controller crate has no disk, socket, timer, async-runtime, broker, replication, or Raft runtime dependency
- checked non-zero ControllerNodeId, AssignmentGeneration, and MovementId types
- all 12 frozen ControllerCommandV1 kinds implemented with canonical bounded little-endian encoding
- command decoder rejects malformed, oversized, non-canonical, duplicate/unsorted assignment, invalid UTF-8, invalid enum, body-length mismatch, and trailing data
- exact command golden vectors established for frozen V1 mutation bytes
- deterministic ControllerState uses ordered maps for brokers, topics, assignments, and movements
- metadata epoch advances only after successful pure apply at a strictly newer committed/apply index
- failed commands are atomic: no state or metadata-epoch partial mutation
- broker registration/rejoin and lifecycle transition rules implemented at state-machine level
- topic creation is atomic with initial assignments and requires active compatible brokers
- movement intent is serialized per partition and cannot skip frozen safety stages
- movement finalization requires next assignment generation and exact target voters
- feature-level mutation is monotonic and rejects unsupported active/draining brokers
- producer-ID allocation is monotonic, non-overlapping, checked for overflow, and intentionally permits gaps after ambiguous responses
- dedicated P5-B controller suite: **19 passed, 0 failed**
- P4 RF=3 broker test helper is CI-stabilized to retry transient metadata refresh timeouts and uses a 15-second outer election/failover observation window; P4 production source is unchanged
- stabilized P4 RF=3 broker E2E repeated **10/10 PASS** before workspace qualification
- P5-B workspace qualification: **291 passed, 0 failed**
- P5-C controller persistence is implemented without networking, timers, heartbeat logic, broker runtime membership or placement execution
- controller hard state uses a fixed-width versioned CRC32C sidecar for P3 term/vote/durable commit
- controller Raft history uses bounded versioned CRC-protected atomic log storage for canonical controller application entries and leader barriers
- dynamic controller-voter configuration entries are rejected because P5 controller membership remains statically bootstrapped
- controller durable commit is mirrored by an independent CRC-protected commit marker and crash-recoverable commit intent
- ControllerSnapshotV1 is encoded canonically with frozen field ordering and CRC32C, then atomically published together with P3 SnapshotMetadata in the controller snapshot bundle
- snapshot decode reconstructs derived topic-name indexes and rejects CRC-valid but logically impossible broker/topic/assignment/movement state
- restart reconstructs P3 RecoveredState from hard state + snapshot + retained suffix and then reconstructs ControllerState by replaying committed ControllerCommandV1 entries only
- unexplained hard-state/commit divergence, foreign cluster ID, foreign static controller voter set, corruption and committed-boundary truncation fail loudly
- local snapshot build requires the state to be no newer than the committed Raft snapshot index and the boundary term/static membership to match durable history
- InstallSnapshot uses a durable snapshot-install intent so crash recovery cannot expose a new snapshot alongside an incompatible old suffix
- matching snapshot boundaries retain the post-snapshot suffix; conflicting boundaries discard it; ordinary append resumes at snapshot+1
- physical controller-log prefix compaction is permitted only at/below the persisted snapshot floor and preserves the retained suffix
- dedicated P5-C controller suite: **33 passed, 0 failed**
- P5-C workspace qualification: **305 passed, 0 failed**
- P3 regression remains **120 passed, 0 failed**; P4 replication remains **46 passed, 0 failed**; storage remains **43 passed, 0 failed**; protocol goldens remain **11 passed, 0 failed**
- P1-P4 production-source diff from the P5-B checkpoint is **empty**
- controller-only ASTCTRP1 transport provides a disjoint cluster/group namespace from P4 data-partition transport, canonical bounded Raft message encoding, CRC32C framing, fragmentation support and snapshot-package carriage
- controller peer transport has bounded inbound frames, per-peer frame queues and byte semaphores, persistent TCP writers, bounded reconnect backoff, and startup-safe shutdown signaling
- foreign cluster frames, unknown controller sources and mixed connection identities are rejected before P3 delivery; ControllerRuntime separately verifies embedded Raft sender identity
- ControllerRuntime is the single owner of P3 core + P5-C persistence + applied ControllerState and converts P3 persistence/send/apply/election effects without reimplementing consensus decisions
- follower mutations return NotLeader with no hidden forwarding; leader-side mutation responses remain pending after local durability and complete only after P3 quorum commit plus ordered ControllerState apply
- real three-node controller TCP quorum elects, commits, fails over to a higher term after leader shutdown, restarts the old controller from P5-C disk state, reconnects and converges
- P5-D three-node election/commit/failover/rejoin scenario repeated **5/5 PASS**
- lost committed responses are explicitly ambiguous: retries can intentionally leave producer-ID gaps across leadership changes, but committed ranges are never reused
- dedicated P5-E controller suite: **49 passed, 0 failed**
- P5-E workspace qualification: **321 passed, 0 failed**
- `controller.bootstrap.v1` is now the durable create-once source of cluster ID + complete static controller voter identity
- bootstrap publication uses an fsynced staging file plus atomic create-once hard-link publication, so concurrent conflicting attempts cannot overwrite each other
- identical bootstrap is idempotent; conflicting cluster IDs or static voter sets fail closed
- ControllerRuntime refuses to start before the bootstrap manifest is durable and refuses to retroactively adopt controller persistence that predates the manifest
- runtime configuration must match the manifest's cluster ID and complete static P3 voter set before P5-C persistence or P3 may start
- P1-P4 production-source diff from the P5-C checkpoint is **empty**
- dedicated P5-F controller suite: **54 passed, 0 failed**
- P5-F workspace qualification: **326 passed, 0 failed**
- typed broker membership APIs route registration, rejoin and lifecycle mutations only through the current controller leader and preserve P5-D quorum-commit + ordered-apply response semantics
- controller runtime now prevalidates accepted metadata proposals against a deterministic speculative state so duplicate/stale membership commands are rejected before entering the controller Raft log; replicated ControllerState remains the final Apply validator
- first registration fixes incarnation 1; broker ID and node UUID are globally unique in committed controller state
- rejoin requires the same node UUID and derives exactly expected incarnation + 1; old incarnation authority is already fenced when a successful rejoin response becomes visible
- leader-only BrokerAuthority validation checks broker ID + node UUID + exact greatest committed incarnation and rejects FENCED/REMOVED identities; this is the mandatory shared gate that P5-G heartbeat handling will reuse
- lifecycle transitions JOINING -> ACTIVE, ACTIVE -> DRAINING, explicit FENCED and terminal REMOVED are committed controller mutations and remain correct across controller failover/restart
- P5-F membership/failover scenario repeated **5/5 PASS**
- dedicated P5-G controller suite: **60 passed, 0 failed**
- P5-G workspace qualification: **332 passed, 0 failed**
- bounded BrokerHeartbeatRequest carries committed BrokerAuthority, applied metadata epoch, feature range, and strictly ordered bounded replica/movement progress summaries
- every accepted heartbeat first passes the P5-F committed broker-ID/node-UUID/current-incarnation authority gate and committed feature/metadata consistency checks
- heartbeat last-seen observations live only in leader-local FailureDetector memory and never enter ControllerCommand, P3 persistence, snapshots, or metadata_epoch
- broker-liveness time is injected explicitly as monotonic logical ticks; backward ticks fail closed and the P3 election timer/wall clock are not broker-liveness authority
- validated thresholds deterministically classify Healthy, Suspect and Unavailable; Unknown is used before the current controller leader has observed the broker
- controller leadership changes clear detector clock + last-seen state, so a replacement leader starts Unknown and cannot inherit another leader's suspicion age
- real three-controller P5-G scenario proves an ACTIVE broker may become Suspect/Unavailable without any replicated lifecycle/incarnation/metadata mutation and without losing BrokerAuthority
- planner-facing liveness snapshots expose available_for_new_placement only for Healthy observations; P5-G does not implement placement itself
- P5-G heartbeat/liveness/failover scenario repeated **5/5 PASS**
- dedicated P5-H controller suite: **70 passed, 0 failed**
- P5-H workspace qualification: **342 passed, 0 failed**
- controller-owned CreateTopic generates a fresh UUIDv4 TopicId before proposal, then commits the exact TopicRecord plus complete initial assignment set through the current controller leader; followers never rerun placement during replay
- topic names are unique while current, RF is exactly 1/3/5, and partition count is bounded to Wire V1's 1..=1024 contract in both typed API and canonical ControllerCommand validation
- DeleteTopic is a replicated ACTIVE -> DELETING -> removed lifecycle; partition expansion is strict increase only and reports an explicit keyed-partitioning-change consequence without changing frozen TopicRecord bytes
- PlacementInputSnapshot freezes durable broker lifecycle/feature/rack/zone metadata, current P5-G liveness, global replica counts and topic-local counts before planning
- new placements require ACTIVE + feature-compatible + Healthy current-incarnation brokers; JOINING/DRAINING/FENCED/REMOVED and Unknown/Suspect/Unavailable brokers are excluded
- deterministic scoring applies failure-domain constraints/diversity first, then total replica count, topic-local concentration and broker ID; emitted voter sets are distinct and canonical
- Required placement requires present pairwise-distinct rack and zone labels and fails closed when unsatisfiable; Preferred fallback is surfaced with degraded_failure_domain=true
- leader preference is deliberately absent from P5-H placement output, preserving P3 leadership authority and the later P5-M hook boundary
- P5-H topic/placement/failover scenario repeated **5/5 PASS**
- frozen P4 E2E qualification harness now uses legal non-overlapping per-broker election windows instead of overlapping random windows; production Raft timing is unchanged and the scenario passed **5/5** consecutive stability runs before the full workspace pass
- dedicated P5-I controller suite is now **73 passed, 0 failed**; broker unit coverage is **8 passed, 0 failed** including four metadata-cache safety tests
- each committed controller application entry now produces a `MetadataDelta { epoch, command }` retained in a volatile dissemination window bounded to **256 deltas / 64 MiB**
- heartbeat metadata synchronization returns at most **64 contiguous deltas / 8 MiB** per response; the broker-reported applied metadata epoch is the sole incremental-sync cursor
- because metadata epochs are controller Raft log indexes, any missing numeric epoch — including a leader-barrier gap — deterministically selects a full committed controller metadata snapshot instead of pretending the delta stream is contiguous
- controller snapshot installation clears retained delta history so no pre-snapshot local history can be replayed across a replacement state boundary
- `BrokerMetadataCache` holds committed controller metadata independently of the legacy P2 local catalog, tracks a monotonic applied metadata epoch, rejects stale/gapped updates, accepts exact same-epoch duplicates idempotently, and fences foreign-cluster/non-regressing snapshots
- broker metadata delta/snapshot application is clone-stage-commit atomic: a later failure in a multi-delta batch cannot expose a half-applied topic/assignment/controller state
- P5-I full workspace qualification: **349 passed, 0 failed**; workspace Clippy with `-D warnings`, formatting and doc tests are clean
- P5-I real three-controller heartbeat/delta/snapshot/failover scenario repeated **5/5 PASS**; broker metadata-cache safety suite repeated **5/5 PASS**
- P5-I does **not** create/remove partition actors, rewrite live peer maps, replace `StaticReplicationConfig`, or change P4 data-plane consensus/ack/read semantics; those became P5-J responsibilities
- P5-J adds a production dynamic-controller broker mode whose `DynamicReplicationManager` projects committed P5 broker/topic/assignment metadata onto the existing P4 runtime; `StaticReplicationConfig` remains only as the frozen P4 compatibility path
- committed assignments now create/remove local P4 actors and rebuild internal peer endpoint/incarnation expectations; no P3 consensus, quorum, acknowledgement, read, rebootstrap or storage semantics were modified
- dynamic reconciliation is staged: controller metadata is cloned/applied first, a replacement P4 runtime must open successfully, and only then are the broker metadata cache and stable routing handle published; a failed replacement attempts rollback to the prior serving topology
- FENCED/REMOVED controller broker identities are blocklisted at the broker embedding boundary for internal transport/local actor admission while their identifiers may remain structurally present until safe data-Raft membership change; stale incarnations remain rejected by the existing P4 transport fence
- controller outage is non-authoritative for already-running partitions: failed refreshes perform no reconciliation or teardown; the real three-controller/broker P5-J E2E shut down the entire controller quorum and then completed RF=1 quorum Produce plus ReadCommitted Fetch
- movement intent is now a public bounded controller mutation but movement execution remains P5-K/L: `ReassignPartitions` commits only `MovementStage::Planned`, leaves the current assignment untouched, and therefore cannot create/promote a learner or remove the source replica in P5-J
- local actor authority is removed only when the committed controller assignment itself changes after safe movement completion; actor removal never deletes the partition directory, preserving old replica bytes as non-authoritative orphaned state
- dynamic Metadata is controller-authoritative for cluster ID, metadata epoch, broker identity/incarnation/endpoints, topic lifecycle and replica assignments; partition leader/term comes only from observed local P3 actor status and is UNKNOWN when no P3 leader observation exists
- reserved DescribeCluster V0 and ReassignPartitions V0 bodies are now implemented with bounded canonical codecs and exact byte fixtures, but are advertised/served only in dynamic-controller mode; legacy P2 continues to return UnsupportedApiKey for those future keys
- CreateTopic/DeleteTopic/ReassignPartitions dynamic admin work is routed through a deterministic bounded controller-leader router; broker pending admin work is fail-fast bounded by a semaphore and controller actor/mutation limits remain bounded independently
- DrainBroker workflow and AlterTopicConfig remain deliberately absent; no P5-K learner/catch-up/promotion/removal orchestration was pulled forward
- P5-J qualification: **354 passed, 0 failed** across the workspace; formatting and workspace Clippy `-D warnings` are clean and all 11 frozen P2 protocol golden tests still pass unchanged
- P5-J real three-controller/dynamic-broker/controller-outage E2E repeated **5/5 PASS**; dynamic topology/fencing/orphan suite repeated **5/5 PASS**; frozen P4 RF=3 TCP E2E repeated **5/5 PASS**
- P5-K/L now executes durable movement intent as a state-driven workflow from `Planned` through learner creation/catch-up, P3 joint-consensus promotion, stable target voters, optional leadership transfer, safe source actor removal, final assignment publication and `Complete`
- destination actor existence is deliberately separated from voting authority: movement stage may create the runtime early, but the first P3 membership change adds the destination only as a learner and P3 remains the sole authority that can later promote it
- P4/P3 ordinary AppendEntries/conflict repair/snapshot rebootstrap remain the only learner data-copy correctness path; P5 adds no second record/file/snapshot transfer format
- dynamic broker heartbeats now publish bounded P3-derived replica progress; controller movement observations are leader-local volatile state fenced by broker node UUID/incarnation, exact assignment generation and movement identity, and are reset on controller leadership change
- `CatchingUp -> PromotionReady` requires both live P3 leader progress and a fresh incarnation/generation-fenced destination report through the relevant durable commit boundary; P3 independently rechecks learner catch-up before accepting the actual voter transition
- promotion is performed only through P3 `EnterJoint`/`LeaveJoint`; controller stages distinguish request from committed joint/stable observation, so response loss cannot be mistaken for durable membership change
- optional movement-specific preferred-leader transfer invokes the existing P3 transfer primitive only from the actual partition leader and advances only after P3 reports the preferred target as leader
- source serving authority is removed only after P3 has committed the stable target voter set excluding it; the old partition directory is preserved as orphaned/non-authoritative state and final controller assignment publication advances assignment generation atomically with `Complete`
- active movement admission is now hard-bounded to **64 globally / 8 per broker by default** against speculative controller validation state; per-partition overlap remains independently rejected
- the broker movement loop performs bounded metadata refresh, heartbeat/progress publication and at most one durable reconciliation action per tick; every action is reconstructed from committed controller intent plus observed P3 state and is safe to retry after lost responses
- the RF=3 movement E2E proves `[1,2,3] -> [2,3,4]`, destination learner catch-up, committed joint/stable configurations, observation of preferred leader 4 before source removal, bounded quorum writes while the movement is active, generation `1 -> 2`, source-byte preservation and post-movement quorum Produce/ReadCommitted Fetch; ordinary P3 may elect another target-set leader after completion because preference is not a permanent pin
- the failure E2E proves source death during bootstrap, destination death, destination rejoin at incarnation 2 and controller-leader death; fresh observations on surviving authority resume the same durable movement to `Complete` and post-recovery quorum Produce succeeds
- P5-K/L qualification: **358 passed, 0 failed** across the workspace; formatting, workspace Clippy `-D warnings`, `git diff --check` and all **11/11** frozen protocol goldens are clean
- full controller suite repeated **5/5 PASS**; three-scenario P5 dynamic movement/failure suite repeated **5/5 PASS**; frozen P4 RF=3 TCP E2E repeated **5/5 PASS**
- P5-N qualifies the already-frozen `ControllerState.active_feature_level` as durable cluster authority: it remains controller-Raft committed, snapshot-persistent, metadata-epoch ordered and visible through DescribeCluster without changing ControllerCommand/Snapshot wire bytes
- broker feature support remains durable `BrokerRecord.min_feature..=max_feature` metadata; registration/rejoin now fail closed on zero minimums, every heartbeat must exactly match the committed range, ACTIVE/DRAINING brokers must support a requested bump, and an incompatible JOINING broker cannot later become ACTIVE
- every static controller voter now has a checked `FeatureRange` compatibility advertisement in `ControllerRuntimeConfig`; the map must exactly cover the frozen voter set, `ControllerStatus` exposes the local advertised min/max, and runtime recovery refuses to serve when any required controller range cannot support the recovered durable active level
- feature bumps use the frozen explicit `SetFeatureLevel { expected_feature_level, new_feature_level }` command and the ordinary leader-only controller commit/apply response path; there is no heartbeat-, placement-, timeout- or startup-driven automatic activation
- bump admission is defense-in-depth: the leader rejects a proposed level unsupported by any required static controller voter before Raft proposal, while the replicated controller state independently rejects a level unsupported by any ACTIVE/DRAINING broker
- feature level is strictly monotonic in P5 at both codec and deterministic state-machine layers; same-level/downgrade attempts fail atomically, failover preserves the committed level, and a controller binary/config advertising only levels below the recovered active level refuses startup
- checked `FeatureGate`, `ControllerState::require_feature` and `BrokerMetadataCache::require_feature` APIs qualify invariant I-28: future irreversible behavior remains disabled below its required committed level, and the broker gate opens only after the corresponding committed metadata delta/snapshot has been applied
- the real three-controller P5-N test proves controller ranges `1..=3 / 1..=2 / 1..=2`, rejection of unsupported level 3, successful committed `1 -> 2`, JOINING broker incompatibility fencing, same-level/downgrade rejection, controller-leader failover/restart preservation and refusal of an old controller range `1..=1` against recovered active level 2
- P5-N qualification: **364 passed, 0 failed** across the workspace; formatting, workspace Clippy `-D warnings`, `git diff --check` and all **11/11** frozen protocol goldens are clean
- dedicated P5-N feature/failover/restart E2E repeated **5/5 PASS**; full controller suite repeated **5/5 PASS**; P5 dynamic controller/movement suite repeated **5/5 PASS**; frozen P4 RF=3 TCP E2E repeated **5/5 PASS**
- P5-O qualifies the already-frozen `next_unallocated_producer_id` as the sole P5 producer-ID authority: it remains in controller Raft state and ControllerSnapshotV1, survives leader changes and complete three-controller restart, and never depends on broker-local or volatile state
- one allocation is hard-bounded to **1..=1,000,000 IDs** while preserving the frozen ControllerCommandV1 `u32 allocation_count` bytes; a successful command returns one exact contiguous inclusive range and advances the durable counter by exactly that count
- `ControllerClient`/`ControllerNodeHandle::allocate_producer_ids` expose a typed allocator API but inherit the existing leader-only controller mutation waiter, so a range is returned only after quorum commit and ordered state-machine apply
- lost allocation responses are deliberately non-idempotent and safe: qualification commits `[1007,1011]`, drops the response, kills the observing controller and proves the replacement allocator begins at `1012`; after all controller voters restart from disk the next block continues at `1017`, so committed IDs can be wasted but never reused
- allocator overflow uses checked arithmetic and fails atomically without advancing metadata epoch or durable counter; the command-count bound independently rejects zero and oversized logical blocks before proposal
- P5-O does **not** introduce producer epochs, sessions, sequences, retry identities or deduplication; those remain P7 and no public Produce wire semantics change in this checkpoint
- P5-O qualification exposed and repaired a latent P5-K/L liveness defect: once `OldReplicaRemoveRequested` is committed, P3 stable-target exclusion has already been proven and the dynamic loop has already applied source-removal topology, so `OldReplicaRemoved` is now an exact-stage-fenced bookkeeping transition rather than requiring a fresh local membership/leader observation that may no longer exist after source actor removal; the RF3 movement E2E passes **10/10** on this repaired path
- the preferred-leader movement assertion was corrected to the frozen semantic boundary: broker 4 must be observed before advancing past `LeadershipTransfer`, while post-completion metadata accepts any current leader from final voters `[2,3,4]`; quorum Produce/ReadCommitted Fetch still prove serving continuity
- P5-O qualification: **366 passed, 0 failed** across the workspace; formatting, workspace Clippy `-D warnings`, `git diff --check` and all **11/11** frozen protocol goldens are clean
- dedicated producer-ID allocator tests repeated **5/5 PASS**; full controller suite repeated **5/5 PASS**; P5 dynamic controller/movement suite repeated **5/5 PASS**; frozen P4 RF=3 TCP E2E repeated **5/5 PASS**
- post-P5-O repeated movement stress exposed two lower-layer defects before P5-M began: a promoted learner could retain live `Role::Learner` after its stable voter configuration committed, and a follower receiving new entries from a freshly elected leader could attempt to regress its already-durable commit index when that leader advertised an older `leader_commit`
- P3 now reconciles a non-leader local role after a committed stable membership transition: learner-only members remain `Learner`, committed voters become `Follower`, and the transition clears stale election/progress/transfer state while preserving observed leader identity
- follower AppendEntries commit calculation is now monotonic: `max(local_commit, min(leader_commit, match_index))`; a new leader may be temporarily behind in commit knowledge, but it can never force a follower's durable committed prefix backward while appending new entries
- movement reconciliation now distinguishes observation from authority: any local replica may confirm an already-durable matching stable/joint configuration and advance exact-stage-fenced controller bookkeeping, while only the actual P3 leader may submit EnterJoint/LeaveJoint configuration proposals
- direct Raft regressions prove both learner-to-voter live-role reconciliation and stale-new-leader commit clamping; the full Raft suite is now **122 passed, 0 failed**
- post-P5-O boundary-repair qualification: **368 passed, 0 failed** across the workspace; formatting, workspace Clippy `-D warnings`, `git diff --check` and all **11/11** frozen protocol goldens are clean
- repaired three-scenario P5 movement/controller suite repeated **10/10 PASS**; frozen P4 RF=3 E2E repeated **5/5 PASS**; full controller suite repeated **5/5 PASS**
- P5-M adds an explicit dynamic-controller-only `PreferredLeaderRequest` / `BrokerHandle::request_preferred_leader` hook with bounded admin admission and timeout; it is separate from placement and does not mutate assignment generation, movement intent or controller metadata epoch
- preferred-target eligibility comes from a fresh committed controller snapshot: the target must be an ACTIVE committed assignment voter, no reassignment may be active, and local P3 must expose stable membership exactly matching controller assignment; the current P3 leader additionally requires target `match_index >= durable_last_index` before requesting transfer
- the hook reuses the existing P3 leadership-transfer primitive and introduces no second election mechanism; after transfer request, success is returned only when P3 observation names the target as leader, and `AlreadyPreferred` is idempotently returned if P3 already reports that target
- P5-M qualification caught and fixed a completion bug in the new hook: once P3 transfer succeeds the initiator normally becomes a follower, so post-transfer observation must not re-require old-leader role; pre-transfer authority remains leader-only while post-transfer completion is observation-only
- the periodic `DynamicControlRuntime` does not call the general preferred-leader hook, so no autonomous continuous leader balancer exists in P5; that policy remains P11
- P5-M qualification: **370 passed, 0 failed** across the workspace; formatting, workspace Clippy `-D warnings` and all **11/11** frozen protocol goldens are clean; full P5 dynamic suite repeated **5/5 PASS**, frozen P4 RF=3 E2E repeated **5/5 PASS**, and full controller suite repeated **5/5 PASS**
- P5-P adds a deterministic controller-state fault simulator over the real `ControllerState`/canonical command codec, seeded broker-incarnation/producer-ID churn, a 128-permutation multi-partition placement determinism campaign, and a bounded movement model that snapshot/restarts at every durable stage and rejects stage skips, duplicate stale-stage delivery and premature finalization
- real three-controller P5-P faults now include minority isolation with no metadata mutation, broker incarnation crash-before-commit versus commit-before-lost-response authority, explicit rejection of old-process heartbeat after rejoin, bounded admin pressure while heartbeat/Raft remain live, and controller-leader kill/restart at all 13 durable movement stages
- the dynamic broker outage scenario now proves control-plane admin failure plus unchanged cached Metadata while quorum Produce/ReadCommitted Fetch continue with all three controllers down
- the RF3 movement scenario now performs eight paced quorum writes while movement is active, concurrently kills source broker 1 only after destination catch-up reaches `PromotionReady` or later safe pre-removal state, completes on `[2,3,4]`, and reads every acknowledged record from the final voter set; focused qualification passed **3/3**
- P5-P resource audit adds deterministic live-state broker/topic/assignment/movement count caps plus a projected worst-case Snapshot V1 byte budget, preserving the existing **64 MiB** snapshot/controller-transport envelope; metadata-delta application failure now falls back to a fresh authoritative snapshot instead of retrying unusable incremental history forever
- P5-P qualification: **380 passed, 0 failed**; controller **91/91**, broker library **13/13**, P5 dynamic **4/4**, protocol **35/35 + 11/11 frozen goldens**, Raft **122/122**, replication **46/46**, storage **43/43**; workspace Clippy `-D warnings`, formatting and `git diff --check` are clean
- focused P5-P stability: sustained RF3 writes + caught-up source failure **3/3 PASS**; controller minority isolation **3/3 PASS**; bounded admin-pressure scheduling **3/3 PASS**; broker-incarnation crash boundaries **3/3 PASS**; controller-leader failure at every movement stage **3/3 PASS**
- P5-Q closure proves the P1 storage tree is identical to P4 closure, frozen P2 golden source is unchanged and **11/11** pass, the only P3 source delta is the separately qualified `e19bbcc` repair, frozen P4 RF3 semantics pass **5/5**, and source audits find no P6 group, P7 idempotency, P8 benchmark, P9 retention-policy, P10 queue or P11 security/drain implementation pulled forward
- P5-Q closure qualification: formatting PASS, workspace Clippy `-D warnings` PASS, controller **91/91**, deterministic P5 failure/model **5/5**, P5 dynamic **4/4**, Raft **122/122**, replication **46/46**, storage **43/43**, frozen P4 RF3 **5/5**, full workspace **380/380**; the admin-pressure observation harness was hardened to reinitialize leader-local volatile failure-detector time after leadership changes and passes **5/5**
- P5-Q exact-HEAD qualification also eliminated a Windows bind-then-release collision in the replication peer-transport test harness by using process-salted loopback addresses; this is test-only and does not change production P4 transport or acknowledgement/read semantics

- P6-A freezes `P6_GROUPS_V1.md`: 64-shard RF3 internal group/offset topics, Murmur3 coordinator routing, checked group/member epochs, deterministic uniform-sticky assignment, cooperative release/fence/activate ordering, cross-topic offset fencing, session-timeout semantics and exact API 30..35 V0 bodies
- P6-A freezes `P6_CHECKLIST.md` with exactly 176 unique actions across P6-A through P6-L; only P6-001..018 are checked and no P6 runtime implementation exists yet
- post-P6-A exact-HEAD qualification exposed a pre-existing P5 RF3 test-harness pacing/cache-convergence race; separate test-only repair `419205e` preserves the frozen >=3 quorum-write proof, passes the repaired exact workspace **380/380**, and changes no production P5/P6 semantics
- P6-B adds checked GroupId/MemberId/GroupEpoch/MemberEpoch/GroupStateVersion plus canonical TopicPartition ordering in the protocol representation layer
- reserved API keys 30..35 now have bounded canonical V0 request/response body codecs, shared MemberAuthority/CoordinatorHint primitives and strict order/count/text validation, but are not yet advertised or routed by a P6 runtime
- new lower-level `astralog-groups` crate implements only the frozen little-endian GroupRecordV1 / OffsetGroupFenceV1 / OffsetOwnerFenceV1 / OffsetValueV1 durable codecs and logical key layouts; it depends only on common + protocol and contains no coordinator/state-machine runtime
- P6-B exact fixtures cover six public API requests and all four internal record kinds; old P2 goldens remain **11/11**, protocol unit tests are **38/38**, `astralog-groups` is **5/5**, and the full workspace is **394/394**
- P6-C adds the exact two reserved internal-topic descriptors and frozen `group_shard` mapping in `astralog-groups`; compatibility vectors freeze `g->22`, `orders->9`, `consumer-a->21` and the long test ID `->59`, while both internal namespaces share the same numeric shard
- P6 internal-topic bootstrap is bounded and P5-authoritative: the broker routes create/validate through the existing controller leader, controller command/snapshot validation admits only the two exact reserved names with 64 partitions / RF3 / Preferred, and controller-derived dynamic P4 topology may carry those names without weakening user/static P4 topic-name validation
- public CreateTopic/DeleteTopic continue rejecting the reserved namespace before mutation; Wire V1 exposes no public ExpandTopic API, and the real P6-C scenario proves rejected public lifecycle attempts leave both authoritative TopicIds unchanged
- group coordinator routing probes only committed RF3 replicas of `(__astralog_groups, shard)` using targeted ordinary Metadata, selects one leader only at the highest concrete observed P3 term, rejects conflicting/unknown highest-term observations and exposes broker/term routing hints; controller metadata never supplies partition leadership
- broker coordinator authority is freshly fenced by local P3 role/broker/term; the RF3 qualification transfers leadership, proves the previous leader/term is rejected, kills the new leader and follows the next P3 leader at a strictly higher term
- P6-C bootstrap/failover/restart qualification uses three controllers + four dynamic brokers + both real 64-partition RF3 internal topics, re-runs bootstrap idempotently, restarts/rejoins a broker at a higher committed incarnation and proves both TopicIds remain unchanged; focused scenario **3/3 PASS**
- P6-C pre-commit qualification: formatting PASS, workspace Clippy `-D warnings` PASS, `git diff --check` PASS, broker library **14/14**, client **6/6**, controller **91/91**, `astralog-groups` **7/7**, protocol **38/38**, frozen P2 goldens **11/11**, P6 goldens **6/6**, P4 RF3 **1/1**, P5 dynamic **4/4**, Raft **122/122**, replication **46/46**, storage **43/43**, complete workspace **401/401**
- P6-D adds a pure `GroupStateMachine` inside `astralog-groups`; it owns only one GroupId/current canonical GroupRecordV1 and has no clock, RNG, network, disk or broker dependency
- committed replay is exact-version fenced: first record must be version 1, later records must be exactly `current+1`, byte-identical same-version replay is idempotent, conflicting duplicates/gaps fail closed, and replay rejects regressing active/pending group epochs or retained member epochs
- first member admission creates state version 1 with active epoch sentinel 0, PREPARING_EPOCH_FENCE and pending GroupEpoch 1; dynamic members start at MemberEpoch 1, static replacement increments the same logical member's epoch exactly once, and duplicate InstanceIds fail atomically
- subscription change, member admission/replacement and leave prepare strictly higher pending GroupEpochs; unfinished prepared epochs may be superseded only by a higher pending epoch, while P6-D never computes assignment ownership itself
- stale GroupEpoch mutations map to frozen `GROUP_EPOCH_STALE`; stale MemberEpoch/unknown-member mutations map to frozen `UNKNOWN_MEMBER`; all command paths use clone/validate/commit so failures preserve the previous record byte-for-byte
- group/member/subscription/partition/per-member-assignment/4 MiB record bounds are enforced before publication; dedicated over-member and per-member-target tests prove no partial mutation
- P6-D focused `astralog-groups` qualification is **17/17 PASS**; workspace fmt and Clippy are clean, frozen P2 goldens remain **11/11**, P6 goldens **6/6**, P6-C routing **1/1**, P5 dynamic **4/4**, and complete workspace is **411/411**
- P6-E adds only the pure `uniform_sticky_assign` function in `astralog-groups`; inputs are canonical members/subscriptions, current active ownership and committed topic partition counts, with no clock/RNG/network/disk/broker/controller-liveness input
- assignment is solved exactly rather than heuristically: max-flow finds the minimum feasible maximum member load, min-cost flow minimizes movement from still-eligible current owners at that load, and canonical ascending TopicPartition/MemberId feasibility fixing produces the lexicographically smallest owner vector among those optima
- the separate frozen identical-subscription balance requirement is enforced as a validity constraint: every identical subscriber receives the floor/ceil load envelope, so a sticky but unbalanced 2/2/0 result is rejected in favor of a <=1 spread result before movement/tie-break optimization
- an exhaustive bounded two-topic/three-member oracle enumerates every legal assignment for heterogeneous subscription/current-owner cases and matches the production solver exactly; dedicated tests also cover ineligible old ownership, reordered input, join/leave movement, partition expansion and unsatisfiable/bound failures
- P6-E focused `astralog-groups` qualification is **24/24 PASS**; workspace fmt and Clippy are clean, frozen P2 goldens remain **11/11**, P6 goldens **6/6**, P6-C routing **1/1**, P5 dynamic **4/4**, controller **91/91**, protocol **38/38**, Raft **122/122**, replication **46/46**, storage **43/43**, and complete workspace is **418/418**
- one combined sequential compatibility process stalled once inside the pre-existing P5 dynamic harness; no code changed, the isolated P5 suite immediately passed **4/4**, and the full 418-test workspace also passed the same P5 suite **4/4**, so no semantic repair was required
- P6-F adds a pure `CooperativeStateMachine` in `astralog-groups`; it has no clock, network, disk, broker or Raft dependency and treats `OffsetGroupFenceV1` / `OffsetOwnerFenceV1` only as externally committed durability evidence rather than pretending that constructing a record commits it
- PREPARING now derives the exact pending-epoch group fence from the committed source state version and complete active-owner/MemberEpoch snapshot; active GroupEpoch cannot advance until that exact fence is observed committed, with exact duplicate fences idempotent and conflicts/regressions rejected
- epoch activation enters REVOKING whenever work remains: moved old owners stay active and alone populate canonical `pending_revocations`, unchanged owners continue uninterrupted, and brand-new/changed target owners remain withheld; REVOKING semantic validation rejects any pending-revocation map other than the exact moved-active set
- RevokeAck requires exact GroupId/GroupEpoch/MemberId/MemberEpoch plus a canonical bounded partition set pending from that member; durable release removes active ownership before an owner fence can be emitted, and an `OffsetOwnerFenceV1` is accepted only for the exact current target authority and exact released `source_group_state_version`
- target activation occurs only after matching committed owner-fence evidence and refuses to overwrite an active owner; STABLE is reached only with zero pending revocations and exact `active_assignments == target_assignments`, directly mechanically qualifying I-18 and I-19
- exact duplicate RevokeAck remains idempotent with no state-version advance even after its handoff reaches target activation/STABLE; that completion identity is GroupEpoch-scoped so a later epoch cannot inherit an earlier handoff's duplicate-ack status; supersession requires a strictly newer pending epoch and cannot use PREPARING to add or transfer active ownership
- P6-F crash recovery cross-validates committed group/fence histories: prepare regenerates the same group fence, committed group fence resumes epoch activation, released state regenerates its owner fence, and committed owner fence resumes target activation; a target appearing active without an earlier matching fence is rejected
- the bounded cooperative model explores legal release/fence/activation interleavings and never reaches dual active ownership; P6-F focused `astralog-groups` qualification is **35/35 PASS**, frozen P2 goldens remain **11/11**, P6 goldens **6/6**, P6-C routing **1/1**, P5 dynamic **4/4**, controller **91/91**, protocol **38/38**, Raft **122/122**, replication **46/46**, storage **43/43**, and complete workspace is **429/429**
- final closure also hardens only the P6-C failover qualification harness: after a coordinator shutdown it now waits for a different broker at a strictly higher P3 term instead of accepting the first transient stale metadata route; production routing semantics are unchanged and the hardened scenario passed **2/2** focused runs
- P6-G adds only leader-local `SessionTracker` liveness observations plus pure transition planning in `astralog-groups`; no heartbeat timestamp/deadline enters GroupRecordV1 or any durable codec, and injected monotonic time rejects regression/overflow
- session timeout admission is exactly **6_000..=300_000 ms** and the recommendation is exactly `clamp(timeout/3, 1_000, 10_000)`; public JoinSubscribe validation and the pure session layer share the same frozen constants
- heartbeat refresh requires exact current GroupId/active GroupEpoch/MemberId/MemberEpoch and changes only the volatile deadline; leadership reset discards observations and rebuilds each committed member with a fresh full timeout without changing any durable group authority
- expiry is deliberately non-authoritative: it yields a `SessionTimeoutCandidate`; only a still-current/still-expired candidate may produce the existing durable `GroupCommand::Leave`, and applying that transition removes the member. Real P4/P3 commit-before-authority binding remains P6-H
- crash-without-Leave qualification removes the expired member, preserves surviving ownership, computes the normal P6-E target and completes the P6-F cooperative fence path to STABLE; a slow member at deadline-1 remains authoritative and can refresh its session
- static replacement on the same leader increments MemberEpoch exactly once and immediately fences the old heartbeat after committed-state reconciliation; identical subscriptions preserve the target byte-for-byte while still preparing/activating the next GroupEpoch, whereas changed subscriptions follow the normal cooperative rebalance
- P6-G freezes a pure control-priority scheduling rule only: whenever session-control work exists it wins over ordinary join/offset pressure; the 10,000-cycle pressure model proves non-starvation without pulling P6-H/P6-J runtime queues forward
- P6-G focused `astralog-groups` qualification is **44/44 PASS**; workspace fmt/Clippy/diff are clean, protocol **38/38**, frozen P2 goldens **11/11**, P6 goldens **6/6**, P6-C routing **1/1**, frozen P5 dynamic **4/4**, controller **91/91**, Raft **122/122**, replication **46/46**, storage **43/43**, and complete workspace is **438/438**
- P6-H adds the broker-local replicated `GroupCoordinatorRuntime` without creating a new authority plane: canonical GroupRecord mutations route only to the current P3 leader/term of the GroupId's `__astralog_groups` RF3 shard, use P4 quorum acknowledgement, and return success only after a second committed-history replay reconstructs the candidate as current
- coordinator recovery is deterministic and bounded: committed records are fetched in bounded pages from stream offset zero, only the exact canonical group key is replayed through `GroupStateMachine::apply_committed`, and matching-key tombstones, malformed values, wrong record kinds or conflicting state-machine histories fail closed rather than being guessed/repaired
- real RF3 failover qualification now covers PREPARING_EPOCH_FENCE recovery, released-partition handoff recovery, committed and uncommitted join boundaries, revoke/release failover, durable Leave without member resurrection, exact duplicate lost-response retry, and continued group mutation after the entire P5 controller quorum is unavailable
- coordinator runtime admission is finite by construction: independent bounded control/mutation request channels share a hard byte semaphore, fail-fast saturation returns `Busy`, and broker shutdown closes and awaits the worker; no offset-value queue or public API dispatch has been pulled forward
- P6-H stress exposed a P4 restart durability-ordering gap without changing any frozen P1 bytes: P1 correctly forgets whether a recovered uncommitted tail had been fsynced, while P4 previously reconstructed that surviving tail as Raft-stable immediately; P4 recovery now explicitly re-fsyncs the validated recovered tail before constructing stable Raft state, preventing a first post-restart `AdvanceCommit` from outrunning the local durability watermark while preserving the P1 safety contract
- the P4 recovery repair has a dedicated restart regression and preserves the lower-layer test that P1 itself never assumes an uncommitted tail was durable; three consecutive post-fix full coordinator recovery/failover stress runs passed
- P6-H qualification is clean: formatting and affected-crate Clippy are **PASS**, P6 routing/recovery **3/3**, controller **91/91**, `astralog-groups` **44/44**, protocol **38/38**, frozen P2 goldens **11/11**, P6 goldens **6/6**, Raft **122/122**, replication **47/47**, storage **43/43**, and complete workspace **444/444 PASS**
- P6-I adds the pure `OffsetStateMachine`: one GroupId's committed `OffsetGroupFenceV1`, same-epoch `OffsetOwnerFenceV1` overrides and latest `OffsetValueV1` map are reconstructed deterministically from committed history with monotonic/conflict/source-version validation and frozen logical-state bounds
- atomic CommitOffsets validation now completes before proposal: canonical order/uniqueness, group/member epochs, current member existence, active assignment, durable effective-owner authority, metadata/record encoding and projected live-state growth must all pass or the entire request is rejected without a partial append
- durable offset mutations use the real RF3 `__astralog_offsets` P4 actor with `AckRequirement::Quorum`; P4 exposes the proposal Raft index internally, committed history is replay-verified after the proposal, and success returns that authoritative `offsets_commit_index` only after every requested logical value is current
- the offsets coordinator generates the server commit timestamp itself, preserves metadata plus GroupEpoch/MemberId/MemberEpoch in `OffsetValueV1`, and committed-only FetchOffsets reconstructs through bounded P4 committed Fetch pages without exposing uncommitted tail bytes
- P6-I directly qualifies the cross-topic fence races: once the epoch-2 offsets group fence commits, an epoch-1 CommitOffsets is rejected before the group log activates epoch 2; once a released partition's owner fence commits, a late old-owner write is rejected before target activation
- authorized offset rewind is intentionally legal: exact current authority can move next_offset backward because P6 fences stale writers rather than imposing application-progress policy
- equivalent CommitOffsets retry after a hypothetical lost response may append another replacement record because P7 request idempotency is absent, but it cannot regress group/member/owner authority and the effective logical value remains the requested current-authority value
- real offsets-shard RF3 qualification kills the current leader, recovers fences/latest value on a different higher-term broker, continues committing, rejoins the failed replica and transfers leadership back; metadata plus broker-generated commit timestamp survive the complete failure/restart path
- P6-I runtime work is bounded by separate control/mutation queues, a shared byte semaphore and bounded replay pages; unrelated-group garbage/tombstones do not poison another group's replay, while matching-group tombstones fail closed until P9 defines physical compaction semantics
- P6-I P9 audit is clean: no storage compactor, retention timer, tombstone lifecycle, segment rewrite or physical keyed cleanup is introduced; public API 30..35 dispatch/advertisement remains absent and is still P6-J
- P6-I qualification is clean: formatting, `git diff --check` and workspace Clippy `--all-targets -D warnings` are **PASS**; broker units **19/19**, P2 network **9/9**, P4 RF3 **1/1**, P5 dynamic **4/4**, P6 RF3 routing/failover **4/4**, client **6/6**, common **2/2**, controller **91/91**, `astralog-groups` **50/50**, protocol **38/38**, frozen P2 goldens **11/11**, P6 goldens **6/6**, Raft **122/122**, replication **47/47**, storage **43/43**, and the complete workspace is **453/453 PASS**
- P6-J activates public JoinSubscribe/Heartbeat/RevokeAck/Leave/CommitOffsets/FetchOffsets V0 only when committed feature level >=2 and both frozen P6 internal topics are valid in committed metadata/local replicated topology; before activation a raw key-30 request still returns the frozen `UNSUPPORTED_API_KEY` path
- public group operations always enter the current `__astralog_groups` P3 leader/term; a raw request sent to an activated non-coordinator returns `NOT_LEADER` plus the exact current broker/term hint before mutation, and RF3 qualification proves authoritative group history is unchanged
- CommitOffsets/FetchOffsets preserve the two-authority architecture without a client-side race: the client talks only to the group coordinator, which supplies committed group state and uses a bounded private broker hop to the independently elected `__astralog_offsets` leader; that receiver validates committed broker identity/incarnation and then enters the qualified P6-I worker
- the implementation-private offset hop uses key 65000 outside the public `ApiKey` registry/ApiVersions and does not alter the frozen P4 Raft peer transport; P11 cryptographic broker transport security is not pulled forward
- P6-J reuses the exact P6-F PREPARING -> group fence -> epoch activation -> revoke/release -> owner fence -> target activation ordering. Bounded committed group recovery validates all history while retaining only the current workflow suffix; `recover_workflow_suffix` accepts only a PREPARING snapshot plus canonical later transitions, while the older full-history recovery contract is unchanged
- the Rust client now owns bounded coordinator-route and member-state caches, exact MemberId/GroupEpoch/MemberEpoch authority, a hard group in-flight semaphore and finite retry count. Coordinator hints cannot regress term; member authority cannot regress epochs or silently change identity at the same epoch tuple
- public mutation transport timeout/connection loss is surfaced as explicit ambiguity rather than blindly retried; retryable routing failures refresh coordinator discovery within the finite retry budget, and the client regression binds the frozen P6 public retry/terminal classifications
- cooperative client handling separates `keep_processing` from the exact revoke set, optionally commits only revoked-partition offsets before RevokeAck, and accepts new ownership only after the committed fence workflow. The RF3 two-member test preserves one unaffected partition continuously, transfers the other with committed offset 77, then performs public Leave and safely returns both partitions to the surviving member at the next GroupEpoch
- P6-J resource-bound closure adds the last public/client bounds: route/member cache caps, group in-flight semaphore, retry cap, 1,024-group volatile server session map and bounded private-hop attempts; P6-H/P6-I coordinator queue/byte/replay bounds remain unchanged
- P6-J qualification is clean: formatting, `git diff --check` and workspace Clippy `--all-targets -D warnings` are **PASS**; broker units **19/19**, P2 network **9/9**, P4 RF3 **1/1**, P5 dynamic **4/4**, P6 RF3 routing/public integration **5/5**, client **10/10**, common **2/2**, controller **91/91**, `astralog-groups` **51/51**, protocol **38/38**, frozen P2 goldens **11/11**, P6 goldens **6/6**, Raft **122/122**, replication **47/47**, storage **43/43**, and the complete workspace is **459/459 PASS** using serialized Cargo compilation (`-j 1`) to stay inside the local Windows paging-file limit
- P6-K binds the previously pure P6-G session rules to bounded broker maintenance without creating a new authority plane: only a local committed `__astralog_groups` P3 leader/term may maintain a group, the ticker interval is validated to 10ms..=5s with a 100ms default, and one round awaits at most the hard 1,024 tracked session groups before another round begins
- new group-shard leadership is discovered from local P3 status; on each new leader term the broker scans committed group-shard history in bounded P4 pages, reconstructs canonical GroupState machines, installs one fresh `SessionTracker` grace from committed membership, and remembers shard+term so transient routing observation cannot repeatedly reset that grace
- session expiry remains non-authoritative: maintenance turns an expired candidate into the already-frozen durable timeout-removal/Leave command, then uses the normal P6-F/P6-H group-fence/cooperative path before authority changes. A heartbeat roughly 5.3s into a 6s session remains valid, while a later silent member is durably removed and its stale session is fenced
- a dedicated coordinator-failover RF3 case proves a silent 6s member survives the successor's fresh two-second grace observation without heartbeat refresh, while a stable successor term still removes it after the full grace; later coordinator term changes each grant one new full committed-membership grace rather than inheriting an old leader's volatile deadline
- P6-K adds only one new durable logical group command, `RebalanceAssignments`, for committed P5 topic-count changes. It is legal only from STABLE, preserves members and active ownership, requires a different deterministic target, and reuses `prepare_next_epoch`, so topic expansion cannot bypass PREPARING -> group fence -> epoch activation -> release -> owner fence -> target activation
- real RF3 churn qualification keeps CommitOffsets/FetchOffsets traffic moving while a second member joins and while that member later Leaves; moved ownership never overlaps, unaffected ownership continues, and the survivor converges to STABLE through the normal fenced workflow
- real RF3 crash/slow-heartbeat qualification covers crash-without-Leave, just-before-timeout heartbeat preservation, durable timeout removal, stale-session fencing and subsequent topic expansion from two to four RF3 partitions while the surviving group remains active; the sticky target preserves all still-valid ownership and activates only the two new partitions at a higher GroupEpoch
- static membership race qualification submits the same replacement concurrently from two clients at the same old MemberEpoch; committed state contains one logical member at MemberEpoch 2, any duplicate successful response describes that exact authority, and the original static session is fenced
- coordinator-failure qualification now explicitly covers committed STABLE, PREPARING_EPOCH_FENCE and REVOKING-after-release boundaries: higher-term successors reconstruct the exact committed record/workflow before further progress and never invent missing fence durability or resurrect released ownership
- offsets-failure qualification kills the actual `__astralog_offsets` leader before group-fence commit and separately after durable release but before owner-fence commit; group epoch/target activation remain withheld until the higher-term offsets successor quorum-commits the exact required fence
- the real RF3 stale-write races remain green under P6-K: an old GroupEpoch cannot overwrite after OffsetGroupFence advances, and an old owner at the same GroupEpoch cannot overwrite after OffsetOwnerFence switches effective authority
- deterministic P6-K qualification runs 32 seeds x 32 join/leave transitions twice and requires byte-identical traces, reruns the exhaustive assignor objective oracle and bounded cooperative handoff model, and adds an offset-fence state model covering all 120 permutations of group-fence/old-epoch-write/owner-switch/old-owner-write/new-owner-write events
- corruption/resource/scheduling qualification now includes explicit committed group state-version-gap rejection alongside tombstone/malformed/conflicting replay failures; max members, per-member assignments, subscriptions and 1,025-offset batch one-over-limit cases fail without partial state change; the 10,000-cycle control-priority model plus real RF3 load prove session work continues under join/offset pressure
- P6-K qualification is clean: formatting, `git diff --check` and workspace Clippy `--all-targets --all-features -D warnings` are **PASS**; broker units **19/19**, P2 network **9/9**, P4 RF3 **1/1**, P5 dynamic **4/4**, P6 RF3 routing/failover/churn/public integration **9/9**, client **10/10**, common **2/2**, controller **91/91**, `astralog-groups` **55/55**, protocol **38/38**, frozen P2 goldens **11/11**, P6 goldens **6/6**, Raft **122/122**, replication **47/47**, storage **43/43**, doc tests **PASS**, and the complete serialized workspace is **467/467 PASS**
- P6-L compatibility audit binds closure to the frozen earlier-phase boundaries: P1 `FORMAT_V1.md` plus storage format/index source are unchanged from `3515000`; the original P2 golden source is unchanged from `a3cf08d`; P6 adds zero Raft source delta relative to P5 closure `ea6c311`; the only P6 replication-core delta is the independently qualified recovered-tail re-fsync repair; controller changes only reserve/validate the frozen P6 internal-topic topology
- explicit compatibility regressions are green at closure: storage **43/43**, original P2 goldens **11/11**, Raft **122/122**, replication **47/47** plus frozen P4 RF3 **1/1**, controller **91/91** and P5 dynamic integration **4/4**
- P6-L later-phase audit is clean: Produce V0 still has no producer-session/epoch/sequence/dedup fields; there is no compression/benchmark implementation, no P9 retention/physical compaction policy, no P10 lease/retry/DLQ semantics, and no P11 TLS/auth/drain/continuous-balancing policy. Logical group/fence idempotence and bounded internal work queues remain strictly P6 correctness machinery
- final P6 correctness-debt audit finds zero `TODO|FIXME|XXX|HACK` markers in the P6 broker/client/groups/protocol/test sources
- the authoritative P6-L pre-commit gate is formatting **PASS**, workspace Clippy `-D warnings` **PASS**, P6 RF3 **9/9**, P6 deterministic group/model suite **55/55**, complete serialized workspace **467/467 PASS**, and doc tests **PASS**; exact closure HEAD must pass the same gate and receive a qualification Git note before P7 planning is allowed
- P7-C adds the sole controller producer-session authority as a canonical `BTreeMap<ProducerId, ProducerEpoch>` inside deterministic ControllerState; new sessions consume the exact P5 durable `next_unallocated_producer_id`, begin at epoch 1, enforce the one-million logical bound plus the existing 64 MiB snapshot envelope before mutation, and never create a broker-local competing allocator
- producer epoch changes are committed compare-and-swap transitions: exact current authority is required, the next epoch is checked/non-wrapping, unknown IDs and stale epochs fail atomically, and both runtime preparation plus committed apply require active feature level >=3
- controller Raft application payloads are now version-dispatched without changing frozen V1 bytes: historical ControllerCommand V1 and P7 ProducerSessionCommandV2 coexist in one log; P7-C qualification replays V1 ProducerId allocation followed by V2 session creation and reconstructs the exact allocator/session state after restart
- controller snapshot persistence keeps the existing outer Raft snapshot package and version-dispatches only the embedded controller state: V1 snapshots migrate to an empty session map while retaining the durable allocator; non-empty authority encodes through ControllerSnapshotV2 and a compacted V2 snapshot restart restores producer session, epoch, allocator and metadata epoch exactly
- internal `ControllerClient::init_producer` is commit-before-response controller authority only, not public broker API activation: followers return `NotLeader`; leaders hold a bounded waiter until P3 quorum commit/application and verify the exact committed authority before returning success
- real three-controller P7-C failure qualification deliberately loses a committed new-session response and a committed epoch-bump response, proves the ambiguous ID/epoch remains consumed, kills the active leader, continues at a higher term, advances authority again, restarts the old leader and converges all replicas without ID reuse or epoch regression
- P7-C qualification is clean: focused P7-C tests **8/8**, controller **104/104**, formatting and `git diff --check` **PASS**, workspace Clippy `--all-targets -D warnings` **PASS**, P2 network **9/9**, P4 RF3 **1/1**, P5 dynamic **4/4**, P6 RF3 **9/9**, client **10/10**, common **3/3**, groups **55/55**, protocol **44/44**, P2/P6/P7 goldens **11/11 + 6/6 + 4/4**, Raft **122/122**, replication **49/49**, storage **43/43**, doc tests **PASS**, and the complete serialized workspace is **493/493 PASS**
- P7-D adds the pure `PartitionProducerStateV1` in `astralog-replication` without wiring it into P4 runtime/storage: state is only ProducerId, committed accepted epoch, exact next sequence and the newest five committed retry records; there is no clock, disk, network, RNG or side authority dependency
- producer admission is side-effect free: unseen/controller-confirmed authority and higher epochs require sequence zero, equal epoch accepts only exact `next_sequence`, lower epochs are fenced, and no admission path advances committed state
- exact retained retry identity is sequence range plus canonical P7-B digest; a supported retry returns the original committed stream-offset range/Raft index without append, while changed content, partial overlap, sequence gap and retries older than the five-batch window fail explicitly instead of being guessed
- only `apply_committed` advances `next_sequence`; the sixth committed batch evicts exactly the oldest retained retry record. A higher epoch clears the old window only when its first sequence-zero batch is committed, and old epochs are fenced thereafter
- committed replay is idempotent for identical retained metadata and fails closed on epoch regression, gaps, conflicting metadata, non-zero first range for a higher epoch or unverifiable old/partial history; sequence and stream-offset arithmetic are checked/non-wrapping
- the P7-D exhaustive small-state oracle independently classifies epoch/sequence/count/digest combinations and mechanically checks I-15 exact-retry dedupe, I-16 old-epoch fencing and I-17 no silent gaps/overlaps while also requiring every admission decision to leave committed state unchanged
- P7-D requalifies the frozen P7-B canonical digest rather than duplicating SHA logic: the direct digest test and immutable P7 golden corpus remain green
- P7-D qualification is clean: focused producer-state tests **7/7**, replication **56/56**, canonical digest **1/1**, P7 goldens **4/4**, formatting and `git diff --check` **PASS**, workspace Clippy `--all-targets --all-features -D warnings` **PASS**, P2 network **9/9**, P4 RF3 **1/1**, P5 dynamic **4/4**, P6 RF3 **9/9**, broker **19/19**, client **10/10**, common **3/3**, controller **104/104**, groups **55/55**, protocol **44/44**, P2/P6/P7 goldens **11/11 + 6/6 + 4/4**, Raft **122/122**, storage **43/43**, doc tests **PASS**, and the complete serialized workspace is **500/500 PASS**
- P7-E activates the already-frozen Storage V1 idempotent producer header without changing its byte layout: ApplicationCommand V1 still materializes producer=None, while ApplicationCommand V2 version-dispatches into ProducerId/ProducerEpoch/base_sequence and recovers back into V2 from those existing fields
- `PartitionPersistence::recover_committed_producer_states()` is the P7-E/P9 logical handoff: it scans only data frames at or below the committed Raft boundary, validates ProducerId/Epoch/sequence range, recomputes the frozen canonical batch digest and rebuilds `PartitionProducerStateV1` per producer; uncommitted P7 tails never acquire authority
- mixed V1/V2 persistence is qualified in one partition history with contiguous stream offsets 0,1,2,3; conflict truncation removes tentative producer metadata, ordinary restart restores exact epoch/next-sequence/recent-window state, and P4 rebootstrap transfers committed producer history then continues at the exact next sequence
- follower-equivalent qualification materializes the same V2 entry into independent replica stores and requires equal Storage V1 frames plus equal recovered producer maps; legacy V1 entries continue to reconstruct through the unchanged ApplicationCommand V1 path
- Storage V1 producer validation now rejects zero ProducerId, zero ProducerEpoch and sequence-range wrap on encode/decode without changing any field or version; producer-header bit flips are caught by the existing frame CRC, and malformed producer values remain rejected even after recomputing an otherwise valid CRC
- lower-layer dependency direction remains intact: replication does not depend on protocol. A private recovery-only canonical record re-encoder is mechanically bound to the immutable P7-B SHA-256 digest golden `090e5db72735ab045bd82af0dc1fc927465bced52a5e59e617559c3b7966af4a`; the public P7-B digest definition remains authoritative
- frozen P1 idempotent and non-idempotent Storage V1 fixtures remain exact; original P2/P6/P7 protocol goldens remain **11/11 + 6/6 + 4/4 PASS**. P7-E qualification is formatting/diff hygiene **PASS**, workspace Clippy `--all-targets --all-features -D warnings` **PASS**, Storage **44/44**, Replication **62/62**, P2 network **9/9**, P4 RF3 **1/1**, P5 dynamic **4/4**, P6 RF3 **9/9**, broker **19/19**, client **10/10**, common **3/3**, controller **104/104**, groups **55/55**, protocol **44/44**, Raft **122/122**, doc tests **PASS**, and complete serialized workspace **507/507 PASS**
- P7-F adds a separate internal idempotent proposal path above the frozen P4 runtime: exact local P3 leader/term, quorum ack, explicit timestamps, checked ProducerAuthority/sequence and bounded proposal resources are required before any P7 proposal authority is created; legacy Produce V0 remains on the unchanged non-idempotent path
- producer authority lookup preserves layering: replication has no controller dependency. Unseen ProducerId or a higher ProducerEpoch returns `ControllerValidationRequired`; `DynamicBrokerControl` performs the fresh committed controller lookup and retries with exact evidence, while equal committed epochs remain controller-independent and continue during complete controller outage
- runtime producer state is committed-history-derived and bounded: cache misses replay P7-E committed P1 history, committed duplicates return original offsets/Raft index without another proposal, the cache is hard-capped at 4,096 reconstructable entries and deterministic eviction never removes pending authority
- one unresolved idempotent range is permitted per producer-partition; exact pending duplicates coalesce onto one proposal/waiter set without additional pending-byte charge, changed-content duplicates fail explicitly and a different range receives bounded Busy
- accepted ProducerEpoch/next sequence advance only after the proposal Raft index is covered by durable P3 commit and P7-D committed apply succeeds; tentative higher-epoch authority is separate volatile state and disappears after conclusive failure/leadership loss or crash before persistence
- the real three-controller/RF1 dynamic-broker P7-F qualification proves unknown producer rejection, epoch-1 commit/exact dedupe, changed-content rejection, exact-next commit, rejection of an unissued higher epoch, committed controller bump to epoch 2, local old-epoch fencing and continued equal-epoch production after all controllers are shut down
- P7-F does not advertise or dispatch InitProducer/Produce V1; its broker entry point is internal qualification/runtime plumbing only. ProducerSnapshotV1 remains P7-G and RF3 lost-response/failover qualification remains P7-I
- P7-F qualification is clean: focused replication runtime **4/4**, real P7 broker/controller integration **1/1**, broker **19/19**, P2 network **9/9**, P4 RF3 **1/1**, P5 dynamic **4/4**, P6 RF3 **9/9**, client **10/10**, common **3/3**, controller **104/104**, groups **55/55**, protocol **44/44**, P2/P6/P7 goldens **11/11 + 6/6 + 4/4**, Raft **122/122**, replication **66/66**, storage **44/44**, workspace Clippy/format/diff hygiene **PASS**, and the complete serialized all-target workspace is **512/512 PASS**
- P7-G adds canonical per-partition `ProducerSnapshotV1` as rebuildable acceleration only: `ASTRPSN1`/version 1, exact partition identity, committed Raft boundary, strictly sorted ProducerIds, accepted epoch/next sequence, newest-five retry metadata and CRC32C are frozen; decoded producer state is revalidated through the P7-D invariants before publication
- snapshot resource work is bounded before trust: the hard **64 MiB** limit is checked from filesystem metadata before `fs::read`, declared counts must fit actual entry bytes, and committed suffix replay uses fixed four-frame pages with every physical decode still bounded by Storage V1 `max_frame_bytes`
- snapshot publication uses the existing AstraLog crash-safe sidecar sequence (temporary complete write, file durability, atomic rename, parent-directory durability hook); deterministic interruption tests prove only an old complete or new complete snapshot is ever observed
- snapshot-aware committed producer recovery accepts a sidecar only when TopicId/PartitionId match and its boundary is not newer than the committed P1/P3 boundary; missing, corrupt, noncanonical, wrong-identity, over-limit and ahead snapshots are discarded/rebuilt from committed P1 history without rewriting that history
- qualification takes a snapshot at Raft index 2 and replays six committed suffix frames through index 8, forcing more than one four-frame page and producing the exact same map as the full committed-history oracle; deletion/corruption/wrong-identity/ahead cases produce that same oracle state
- P7-F cache eviction now has a qualified P7-G reconstruction path: persisted snapshot state reloads exactly, and an exact retry after eviction returns the original stream-offset range and Raft index without another append
- P7-G changes no Storage V1 bytes/indexes/retention semantics; P1 only gains a read-only bounded frame-page helper. Public InitProducer/Produce V1 dispatch/advertisement and client producer sessions remain disabled for P7-H
- P7-G qualification is clean: focused snapshot/recovery **7/7**, broker **19/19**, P2 network **9/9**, P4 RF3 **1/1**, P5 dynamic **4/4**, P6 RF3 **9/9**, P7 runtime **1/1**, client **10/10**, common **3/3**, controller **104/104**, groups **55/55**, protocol **44/44**, P2/P6/P7 goldens **11/11 + 6/6 + 4/4**, Raft **122/122**, replication **73/73**, storage **44/44**, workspace Clippy/format/diff hygiene and doc tests **PASS**, and the complete serialized all-target workspace is **519/519 PASS**
- P7-H activates the public producer surface only behind committed P7 readiness: InitProducer V0, Produce V1 and feature key 11/Produce max-version advertisement remain unavailable before activation, while legacy Produce V0 remains unchanged
- broker InitProducer delegates only to controller producer-session authority; Produce V1 delegates only to the already-qualified idempotent partition runtime. Frozen public UNKNOWN_PRODUCER/PRODUCER_FENCED/OUT_OF_ORDER_SEQUENCE plus bounded routing/controller error mappings are unit-qualified
- the Rust client now owns a bounded producer session containing authoritative ProducerId/Epoch plus finite per-partition next-sequence/pending state; it permits one unresolved range per partition, advances sequence only after authoritative success, and keeps the exact encoded Produce V1 body for bounded retry
- ambiguous InitProducer transport outcomes clear local producer authority and require explicit fresh-session recovery rather than assuming the old epoch remains safe. Producer restart uses the controller CAS bump and replaces old partition sequence state only after the exact next epoch succeeds
- the real public P7-H RF1 test deliberately drops a committed InitProducer response and the first Produce V1 response, then proves fresh-session recovery, byte-identical automatic Produce retry, exactly one read-committed logical record with the original offset range, and an epoch bump whose first partition batch starts at sequence zero
- P7-H qualification is clean under serialized RF3/workspace execution: focused public RF1 **1/1**, P7 runtime **2/2**, broker **22/22**, client **11/11**, P2 network **9/9**, P4 RF3 **1/1**, P5 dynamic **4/4**, P6 RF3 **9/9**, common **3/3**, controller **104/104**, groups **55/55**, protocol **44/44**, P2/P6/P7 goldens **11/11 + 6/6 + 4/4**, Raft **122/122**, replication **73/73**, storage **44/44**, doc tests/Clippy/format/diff hygiene **PASS**, and the complete serialized all-target workspace is **524/524 PASS**
- P7-I qualifies real RF3 idempotent-producer failure boundaries: lost committed response, leader death after quorum/before client success, leader death after local durable persistence without quorum, leader death before broker persistence, and explicit leadership transfer all preserve one committed logical range or explicit failure; cross-term retry refreshes routing and retargets only the pending request's leader epoch while producer/sequence/record identity remains fixed
- tentative higher ProducerEpoch authority is volatile: while an uncommitted epoch-2 range is pending it locally fences epoch 1, but killing that leader makes the successor reconstruct only committed epoch 1 and accept its exact continuation. Once epoch 2 sequence zero commits, epoch 1 is fenced both locally and after failover, and exact epoch-2 retry returns original offsets
- controller/data-plane separation is mechanically qualified: an already accepted producer epoch continues during total controller outage, while unseen or higher unaccepted authority fails without changing durable/committed partition indexes; concurrent controller epoch bumps have one CAS winner, and ambiguous InitProducer across controller-leader death consumes but never reuses the committed ProducerId
- RF3 restart qualification persists the derived P7-G producer snapshot on every caught-up replica, stops/reopens all brokers and proves an exact retained retry returns the original committed offsets with no extra logical record; committed P1/P4 history remains authoritative
- P7-I also hardens only older integration-test convergence boundaries exposed by complete serialized RF3 execution (bounded health/election/read-only readiness waits); no production P5/P6 mutation semantics are weakened
- P7-I qualification is clean: focused RF3/controller campaign **11/11**, P7 runtime **2/2**, broker **22/22**, client **11/11**, P2 network **9/9**, P4 RF3 **1/1**, P5 dynamic **4/4**, P6 RF3 **9/9**, common **3/3**, controller **104/104**, groups **55/55**, protocol **44/44**, P2/P6/P7 goldens **11/11 + 6/6 + 4/4**, Raft **122/122**, replication **73/73**, storage **44/44**, doc tests/Clippy/format/diff hygiene **PASS**, and the complete serialized all-target workspace is **535/535 PASS**
- the final committed P7-I checkpoint is independently requalified on its exact clean HEAD: focused P7-I **11/11**, complete serialized all-target workspace **535/535**, doc tests, workspace Clippy `-D warnings`, formatting and diff hygiene all **PASS**
- P7-J adds qualification only, not production behavior: the producer state machine is searched across deeper histories/window edges; commit/response-loss/failover and old/new epoch orderings are modeled; recent-window eviction and 64-way pending coalescing are checked without changing the frozen P7 runtime
- a fixed-seed 192-action campaign spans four producers and three logical partitions and requires byte-identical trace/final state for repeated seeds; near-maximum sequence and stream-offset arithmetic is mechanically checked for exact-bound acceptance and one-over rejection
- producer resource/corruption bounds remain finite: client producer-partition one-over fails without mutation, ControllerSnapshotV2 byte/count one-over fails closed, ProducerSnapshotV1/controller producer metadata valid-CRC corruption is rejected, the 4,097th resident producer triggers reconstructable eviction rather than growth, and Storage V1 producer metadata remains CRC/semantic fenced
- P7-J scheduling stress subjects a one-slot runtime to 257 bounded idempotent contenders while executing Raft control ticks after every rejection; the partition remains leader/nonfatal and P6 RF3 remains fully green
- the real RF3 stress campaign performs six seeded leader-kill/restart rounds with one producer session; every successor exact retry returns the original committed range, rejoined leaders catch up, leadership changes each round and final read-committed history contains exactly six logical records
- P7-J qualification is clean: focused P7-J replication **9/9**, controller **2/2**, client **1/1**, repeated RF3 **1/1**, complete P7 failover **12/12**, broker **22/22**, P2 network **9/9**, P4 RF3 **1/1**, P5 dynamic **4/4**, P6 RF3 **9/9**, P7 runtime **2/2**, client complete **12/12**, common **3/3**, controller complete **106/106**, groups **55/55**, protocol **44/44**, P2/P6/P7 goldens **11/11 + 6/6 + 4/4**, Raft **122/122**, replication **82/82**, storage **44/44**, doc tests/Clippy/format/diff hygiene **PASS**, and the complete serialized all-target workspace is **548/548 PASS**
- P7-J exact-HEAD requalification exposed one narrow P3/P7 leadership boundary: a successor may be local leader before its current-term barrier is durably committed, so prior-term producer history is not yet safe to use as committed duplicate-suppression authority. P7 idempotent admission now returns bounded transient `BUSY` until the durable commit boundary belongs to the current Raft term; no producer state/proposal is mutated before that gate
- the companion P7/P6 qualification changes are harness-only: successor retries tolerate only transient Busy/routing errors, P6 offset fences use their existing idempotent retry helpers, ambiguous setup/static-join outcomes require committed-state proof, the long churn fixture gets a larger request-delivery budget, and serialized Windows RF3 fixtures get a 250 ms teardown-drain boundary
- the finished boundary-repair tree is mechanically green: P7 failover **12/12**, replication **82/82**, P7 runtime **2/2**, P6 RF3 **9/9**, format/diff/Clippy/doc tests **PASS**, and complete serialized all-target workspace **548/548 PASS**. `docs/P7_J_BOUNDARY_REPAIR_REPORT.md` records the exact repair and evidence

## P7 closure and P8-A/P8-B/P8-C/P8-D execution

- P7-K compatibility audit P7-148..152 is complete: Storage V1 **44/44**, original P2 goldens **11/11** with the golden source identical to P2 closure, P6/P7 goldens **6/6 + 4/4**, Raft **122/122** with zero source delta since P6 closure, frozen P4 RF3 **1/1**, P5 dynamic **4/4**, controller **106/106**, groups **55/55** with zero source delta since P6 closure, and exact repair-head P6 RF3 **9/9**
- P7-K scope audit confirms no P8-P12 implementation was pulled forward: no Cargo manifest delta since P6 closure, no compression codec/runtime or benchmark implementation, no physical retention/compaction policy, no queue lease/redelivery/DLQ semantics, no TLS/mTLS/auth/ACL/drain/continuous-balancing behavior, and no new P7 correctness `TODO|FIXME|XXX|HACK` markers
- P7-153 final closure-tree qualification is green: focused P7-J **9/9 + 2/2 + 1/1**, P7 failover **12/12**, P7 runtime **2/2**, P6 RF3 **9/9**, Storage **44/44**, P2/P6/P7 goldens **11/11 + 6/6 + 4/4**, Raft **122/122**, P4 RF3 **1/1**, P5 dynamic **4/4**, controller **106/106**, groups **55/55**, replication **82/82**, complete serialized workspace **548/548**, and fmt/Clippy/doc/diff hygiene **PASS**
- `docs/P7_CLOSURE_REPORT.md` binds the complete closure evidence; all phase-exit invariants and P7-001..155 are mechanically checked. Sole closure commit `d0d0e85` carries the exact-HEAD PASS Git note, so **P7 is authoritatively closed**
- post-P7 qualification-harness hardening culminates at `1384279` and changes only `crates/astralog-broker/tests/p6_routing.rs`: parent `6f17a29` hardens the silent-member RF3 fixture and removes `FetchOffsets` as an invalid group-coordinator readiness proxy; `1384279` gives the second client in the long public-integration fixture a bounded 20-second setup deadline before any second JoinSubscribe mutation is issued. Exact `1384279` passes public integration **1/1**, P6 RF3 **9/9**, complete serialized workspace **548/548**, fmt/Clippy/doc/diff gates, and carries its own exact-HEAD PASS Git note. Production P0-P7 semantics remain unchanged, and `1384279` is the clean execution base for P8-A
- P8-A is planning/specification only: `docs/P8_PERFORMANCE_V1.md` freezes controller-owned topic compression/batch policy, feature key 14 + committed feature level 4 activation, Storage V1 LZ4/Zstd semantics, per-partition client batching, bounded group commit/replication/fetch optimization, data-plane backpressure/watermarks, broker-local anonymous/topic load shaping and provenance-bound baseline benchmarking
- P8 V1 deliberately preserves P7's one-unresolved-idempotent-range per producer-partition rule; compression is a storage representation detail and the P7 digest remains over uncompressed canonical logical records
- P8-A froze additive controller evolution: ControllerCommand/Snapshot V1/V2 stay unchanged; V3 topic-performance metadata was reserved for P8-B. Produce V0/V1 and Fetch V0 stay byte-exact, and P8 does not take ownership of AlterTopicConfig key 6
- `docs/P8_CHECKLIST.md` defines exactly P8-001..P8-160 across P8-A..P8-I; P8-A through P8-G now check P8-001..126, while P8-H P8-127..143, P8-I P8-144..160 and all phase-exit invariants remain open
- P8-A contained no Rust/Cargo implementation. P8-B adds controller/protocol/broker topic-performance metadata activation, P8-C activates Storage V1 compression/recovery plus the committed-policy-to-persistence handoff, P8-D adds the opt-in bounded client accumulator/batching path, P8-E adds Persistence Frontier V2 plus bounded per-partition group commit, P8-F adds replication pipelining/fetch tuning, and P8-G adds bounded broker data admission, watermark hysteresis, broker-local shaping and client throttle honoring; P8-H benchmark tooling remains disabled
- exact P8-A authority is `5652821d6ef1c7a4e129dab3e265fff1daf6f589` plus its PASS Git note; that boundary authorizes P8-B only
- P8-B P8-019..038 adds checked controller-owned `CompressionPolicyV1` and `TopicPerformancePolicyV1`, deterministic historical-topic defaults, additive ControllerCommand V3 / ControllerSnapshot V3 and one atomic CreateTopic-with-performance transition without changing frozen V1/V2 bytes
- P8-B feature activation remains solely the P5 committed feature-level mechanism: V3 mutation requires level 4, ACTIVE/DRAINING broker and static-controller compatibility is enforced before activation, and feature key 14 version 1 is supported/inactive before convergence and active only after committed level 4
- public CreateTopic/DescribeTopic V1 carry/read the validated committed P8 policy while V0 stays byte-compatible; permanent key 6 `AlterTopicConfig` remains unadvertised and undispatched
- controller RF3 qualification commits level 4 plus an LZ4 topic policy, verifies identical policy on every controller, kills the leader, verifies the higher-term successor, restarts the stopped controller and recovers identical feature/policy state
- real dynamic-broker qualification advertises topic V1 only after committed P8 readiness, commits a Zstd policy through CreateTopic V1, reads the identical policy through DescribeTopic V1 and still rejects `AlterTopicConfig`
- P8-B introduced no Cargo/dependency change and no P8-C storage/compression runtime; `astralog-storage`, `astralog-raft`, `astralog-groups`, `astralog-replication`, workspace manifests and original P2/P6/P7 golden sources remained unchanged from `5652821` at the P8-B boundary
- P8-B pre-commit qualification is clean: focused controller **6/6**, protocol P8 **1/1**, dynamic-broker P8 **1/1**, broker readiness **1/1**, original P2/P6/P7 goldens **11/11 + 6/6 + 4/4**, fmt/Clippy/doc/diff gates **PASS**, and the complete serialized all-target workspace **557/557 PASS**. `docs/P8_CONTROLLER_METADATA_REPORT.md` binds the evidence
- P8-C P8-039..056 pins exact `lz4_flex=0.14.0` raw-block and `zstd=0.14.0` level-3 profiles, compresses only the canonical Storage V1 data payload, keeps control frames None-only, falls back to None unless compressed bytes are strictly smaller, binds stored/logical length fields, checks the logical decompression ceiling before decode allocation, validates CRC over physical bytes and rejects unknown/truncated/trailing/length-mismatched codec data
- immutable `data_p8_lz4_v1.hex` and `data_p8_zstd_v1.hex` now bind exact compressed Storage V1 bytes while every historical None Storage fixture remains tree-identical to P8-B authority `339d87f`
- mixed None/LZ4/Zstd history qualifies restart/scan, sparse offset/time/Raft index rebuild and committed-boundary-safe suffix truncation; compressed P4 rebootstrap preserves physical codecs and logical history and the selected in-process persistence codec survives the rebootstrap replacement
- P7 producer authority is representation-independent: LZ4/Zstd history reconstructs the same ProducerId/Epoch, next sequence, recent committed retry window, offsets and canonical retry digests as None history
- committed P8-B topic compression policy is mapped into dynamic replication topology only after feature level 4 and applied to partition persistence; default persistence remains None. The real dynamic-broker P8 integration creates a Zstd topic, produces/fetches the same logical value and proves the committed on-disk data frame is physically Zstd
- P8-C qualification is green by bounded target decomposition: storage **53/53**, replication **84/84**, broker **22/22**, client **12/12**, common **3/3**, controller **113/113**, groups **55/55**, protocol **45/45**, Raft **122/122**, P2 network **9/9**, P4 RF3 **1/1**, P5/P8 dynamic **5/5**, P6 **9/9**, P7 runtime **2/2**, P7 failover **12/12**, original P2/P6/P7 goldens **11/11 + 6/6 + 4/4**; the split gate covers **568/568** tests and fmt/Clippy/doc/diff gates pass. One earlier monolithic P7 broker-health deadline failure was rejected as qualification; the exact case immediately passed **1/1** unchanged and full P7 failover passed **12/12** unchanged. `docs/P8_STORAGE_COMPRESSION_REPORT.md` binds this evidence
- P8-D P8-057..073 adds finite client batch/accumulator limits, an exact logical record-size helper over the frozen encoder, a per-topic-partition accumulator, unchanged keyed Murmur3/sticky partition behavior, exact record/byte/linger closure, explicit flush, metadata-invalidation closure, bounded shutdown drain and explicit accumulator-full/closed/record-too-large errors
- the optimized producer surface is additive: `produce_buffered`, `produce_idempotent_buffered`, `flush_producer_batches` and `close_with_drain`; existing immediate Produce V0/V1 APIs keep their prior behavior. Buffered production resolves the committed TopicPerformanceConfigV1 via DescribeTopic V1 before admission
- P7's one-unresolved-range rule is preserved: buffered records consume no sequence while open, a V1 range is reserved only when the closed batch reaches the existing P7 send boundary, a second V1 batch waits behind an unresolved pending range, and retry preserves the same logical records/base sequence while route refresh may change only the leader epoch
- real dynamic-broker qualification produces two-record buffered V0 and V1 batches, proves offsets 0..3, proves explicit flush at offset 4 and shutdown drain at offset 5, reconnects/fetches all six values in order, and proves every physical data frame still uses the committed Zstd policy without constraining P4 physical frame coalescing
- an intermediate global delivery-budget validation was rejected because it broke three legacy P6 clients at connect time. The retry-budget check now occurs only before opt-in buffered admission; all three affected P6 cases then passed independently and full P6 **9/9** plus P7 failover **12/12** passed without weakening frozen tests
- P8-D qualification is green by bounded target decomposition: broker **22/22**, P2 network **9/9**, P4 RF3 **1/1**, P5/P8 dynamic **5/5**, P7 runtime **2/2**, client **22/22**, common **3/3**, controller **113/113**, groups **55/55**, protocol **45/45**, original P2/P6/P7 goldens **11/11 + 6/6 + 4/4**, Raft **122/122**, replication **84/84**, storage **53/53**, P6 **9/9** and P7 failover **12/12**; the bounded qualification covers **578/578** tests and fmt/Clippy/doc/diff/frozen-byte gates pass. `docs/P8_CLIENT_BATCHING_REPORT.md` binds this evidence
- the P8-E design audit proved the historical P3/P4 combination could not support true same-partition group commit: P3 permitted exactly one outstanding persistence batch per Raft group, while P4 completed that token only after durability, leaving no legal window for a second token to share the same physical sync
- `docs/P8_E_ARCHITECTURE_BLOCKER.md` records that contradiction. ADR-018 now accepts Option A, and `docs/PERSISTENCE_FRONTIER_V2.md` defines a bounded ordered multi-token append queue with a separate non-authoritative staged logical frontier, one monotonic durability frontier, strict ordered completion and hard barriers for hard-state/commit/truncate/snapshot/rebootstrap work
- the P8-E V2 re-freeze is exact-HEAD authoritative at `370ae92`; it supersedes only historical single-outstanding-token scheduling while preserving PersistenceCompleted/PersistenceFailed meanings and all durability/ack invariants
- P8-E P8-074..090 implements a bounded ordered append-only persistence queue with a separate non-authoritative staged logical suffix, finite count/byte/group-delay bounds, queue-depth-one compatibility, FIFO token completion, and a per-partition runtime executor that stages append frames then performs one proven physical sync for each eligible contiguous group
- only leader application append tokens are groupable. UpdateHardState, AdvanceCommit, truncation, leader/configuration barriers, snapshot/install, discard/rebootstrap, mixed batches and non-Propose control inputs are hard barriers; focused proofs show commit cannot outrun grouped durability and snapshot request processing drains an open group first
- failure/crash qualification proves ambiguous grouped fsync never creates success, pre-sync staged history does not become committed after restart, post-sync/pre-completion history is recoverable without inventing client success, and leader/quorum ack semantics remain frozen
- mixed None/LZ4/Zstd grouping preserves logical order/offsets and grouped execution matches sequential recovered state/frames. Deterministic counters distinguish logical persistence tokens from actual data syncs without making benchmark claims
- P8-E qualification is green by bounded target decomposition: broker **23/23**, P2 network **9/9**, P4 RF3 **1/1**, P5/P8 dynamic **5/5**, P7 runtime **2/2**, client **22/22**, common **3/3**, controller **113/113**, groups **55/55**, protocol **45/45**, original P2/P6/P7 goldens **11/11 + 6/6 + 4/4**, Raft **127/127**, replication **96/96**, storage **53/53**, P6 **9/9** and P7 failover **12/12**; bounded qualification covers **596/596** tests and fmt/Clippy/doc/diff/frozen-byte gates pass. `docs/P8_PERSISTENCE_GROUP_COMMIT_REPORT.md` binds this evidence
- the first closure commit `ff4d0b4` did not receive authority because exact-HEAD qualification exposed one P6 churn failure in JoinSubscribe post-commit completion. A one-file compatibility repair now converts failures after committed group authority exists into the frozen P6 ambiguous RequestTimeout/reconciliation outcome rather than replayable fresh mutation failure; the exact failed churn case passes **1/1** and the full repaired P6 suite passes **9/9**
- exact P8-E authority is the repaired commit `e929191`; its Git note records clean exact-HEAD **596/596**, fmt/Clippy/doc/diff and frozen-byte qualification and authoritatively closes P8-074..090
- P8-F P8-091..107 adds finite per-follower AppendEntries message/byte windows, a bounded Raft replication-effect history slice so a slow follower cannot cause an unbounded retained-suffix clone, term/incarnation/inbound-connection/snapshot fences, and reserved outbound capacity for Raft control while data replication is saturated
- follower catch-up keeps P3 acknowledgement authority unchanged: only P3 responses advance durable peer progress, failures/reset boundaries discard speculative window accounting, and RF3 sustained-write plus follower restart qualification converges without gaps or duplicates
- Fetch V0 remains byte-compatible and read-committed-only. Storage now page-scans committed frames under the exact request record/byte ceiling, broker response assembly moves owned record payloads instead of cloning them, and long-poll notifications coalesce bursts without changing `min_bytes` / `max_wait_ms` semantics
- the optional deeper plaintext zero-copy path was evaluated and remains **disabled** for P8-F: the proven move-based path removes record-level clones, while true segment-buffer borrowing would couple immutable Storage buffer lifetimes to framed/CRC response ownership without a measured benefit. No unsafe/lifetime-sensitive path or wire/storage format change was introduced
- P8-F pre-commit qualification is green: focused P8-F **8/8**, broker **26/26**, client **22/22**, common **3/3**, controller **113/113**, groups **55/55**, protocol **45/45**, Raft **128/128**, replication **100/100**, storage **53/53**, P2 network **9/9**, P4 RF3 **1/1**, P5/P8 dynamic **5/5**, P6 **9/9**, P7 runtime **2/2**, P7 failover **12/12**, original P2/P6/P7 goldens **11/11 + 6/6 + 4/4**, and the complete serialized all-target workspace is **604/604 PASS**. fmt/Clippy `-D warnings`/doc/diff and frozen Cargo/protocol/Storage-byte gates pass
- exact P8-F authority is `039c700`; its Git note records clean exact-HEAD **604/604**, focused **8/8**, fmt/Clippy/doc/diff and frozen compatibility checks and authoritatively closes P8-091..107
- P8-G P8-108..126 adds a global broker data-admission gate with independent count/byte ceilings, strict low/high watermark hysteresis and a data/control classification that leaves Raft/heartbeat/admin work outside the saturated data budget while preserving the existing per-connection frame/in-flight/response bounds
- broker-local anonymous and TopicId token buckets use monotonic refill, finite burst/rate/cardinality bounds and check response-bearing CreateTopic/Produce/Fetch/ListOffsets quota before dispatch. Quota rejection returns retryable `BROKER_BUSY` with nonzero response `throttle_ms`; request `throttle_ms` stays frozen at zero. Real tests prove throttled CreateTopic and quorum Produce leave no authoritative mutation
- no-response `acks=0` Produce remains outside response quota because no authoritative throttle can be returned; it is bounded by frame, connection and global data-admission limits and makes no success claim when overload admission drops work. The Rust client records nonzero broker throttle hints per connection and delays later sends until at least the advertised monotonic deadline without escaping the caller request timeout
- P8-G pressure qualification composes the already-qualified P8-D accumulator bounds, P8-E persistence token/byte/deadline bounds, the new broker count/byte/watermark gate, frozen P8-F Fetch response ceilings and RF3 replication windows. Real broker slow-consumer/no-response-producer pressure preserves Metadata control progress and committed history; RF3 sustained writes with an isolated follower preserve replication/read bounds and converge after healing
- P8-G pre-commit qualification is green by bounded target decomposition: focused P8-G **14/14**, broker **37/37**, client **24/24**, common **3/3**, controller **113/113**, groups **55/55**, protocol **45/45**, original P2/P6/P7 goldens **11/11 + 6/6 + 4/4**, Raft **128/128**, replication **101/101**, storage **53/53**, P2 network **9/9**, P4 RF3 **1/1**, P5/P8 dynamic **5/5**, P6 **9/9** serialized, P7 runtime **2/2** and P7 failover **12/12** serialized; the complete bounded gate covers **618/618 PASS** and fmt/Clippy `-D warnings`/doc/diff plus frozen Cargo/protocol/Storage surfaces pass. `docs/P8_BACKPRESSURE_QUOTA_REPORT.md` binds this evidence
- exact P8-G authority is `98bf895`; its Git note records clean exact-HEAD **618/618**, focused **14/14**, fmt/Clippy/doc/diff and frozen compatibility checks and authoritatively closes P8-108..126
- P8-H P8-127..143 introduces the release-only `astralog-benchmark` workspace crate and CLI. The long-run path refuses debug builds and dirty Git worktrees, captures exact commit/toolchain/build plus required host/config provenance, validates a canonical manifest, writes canonical raw evidence before report generation, and keeps normal workspace qualification on a short offline smoke path rather than executing a performance run
- the deterministic workload matrix covers 100 B/1 KiB/10 KiB, keyed/unkeyed, RF1/RF3, leader/quorum ack, None/LZ4/Zstd committed topic policy, single/configured-multi producer concurrency and producer-only/backlog-consumer/simultaneous modes while mechanically covering the frozen critical factor pairs. Deterministic seeds reproduce record and partition choices; CreateTopic/DescribeTopic V1 verifies committed performance policy before measurement
- warmup cannot enter reported accounting: producer/simultaneous use separate warmup topics and backlog-consumer uses a non-retaining warmup accumulator. Measurement retains hard-bounded raw producer/Fetch latency samples plus event/byte, CPU/RSS, disk, fsync, network, replication-lag and error/throttle fields. Report percentiles/rates are generated from raw evidence, identical raw bundles regenerate byte-identical Markdown, and comparisons reject fewer than two complete repeats or mismatched matrices
- P8-H focused qualification on the final source candidate is **19/19 PASS** with package Clippy `-D warnings`, optimized benchmark-binary build, fmt and diff hygiene green. No throughput/latency number is claimed by qualification itself; any future performance claim must come from the committed release runner and retained raw bundle. `docs/P8_BENCHMARK_HARNESS_REPORT.md` binds the implementation and closure evidence
- P8-H complete pre-commit regression is **637/637 PASS**: benchmark **19/19**, broker **37/37**, P2 network **9/9**, P4 RF3 **1/1**, P5/P8 dynamic **5/5**, P6 **9/9** serialized, P7 failover/runtime **12/12 + 2/2** serialized, client **24/24**, common **3/3**, controller **113/113**, groups **55/55**, protocol **45/45**, original P2/P6/P7 goldens **11/11 + 6/6 + 4/4**, Raft **128/128**, replication **101/101** and storage **53/53**. Workspace Clippy/doc/fmt/diff and frozen protocol/Storage/golden-source checks pass; the optimized benchmark binary builds successfully
- exact P8-H authority is `58f529f`; its Git note records clean exact-HEAD **637/637**, benchmark **19/19**, release build, fmt/Clippy/doc/diff and frozen compatibility checks and authoritatively closes P8-127..143
- the first P8-I engineering-baseline execution exposed a benchmark-only topic-readiness race: committed CreateTopic/DescribeTopic policy could precede concrete P3 leader readiness in Metadata. Repair `c7e7a1b` adds a request-timeout-bounded Metadata convergence wait requiring the exact TopicId/partition/RF shape plus concrete leader broker/epoch for every partition; it changes no broker production path, wire body, Storage byte or authority rule. Benchmark tooling is now **20/20 PASS** and the real baseline completes on that clean source SHA
- P8-I compatibility audit proves the historical Storage golden directory differs from P7 closure only by the two P8 compressed fixtures, original P2/P6/P7 golden sources remain unchanged, I-26 bounded-resource and I-27 control-progress surfaces are explicitly bounded/stress-qualified, and feature-level-4 remains the only activation authority for irreversible P8 topic-performance behavior. Added-source/dependency audit finds no P9-P12 implementation pull-forward
- the retained P8 engineering baseline is `docs/baselines/P8_ENGINEERING_BASELINE.raw` plus generated `docs/baselines/P8_ENGINEERING_BASELINE.md`, bound to exact source `c7e7a1b7aee964e8ace2dd45be294d87c31f1265`, Rust 1.98.1 release and complete host/config provenance. The deterministic matrix contains 9 cells with two complete repeats; reparsing the raw bundle regenerates byte-identical Markdown. No handwritten status prose copies throughput/latency values. The closure report records the single-host/loopback, process-I/O proxy and unavailable fsync/lag telemetry limitations; this remains an engineering baseline rather than a P12 release claim
- P8-I focused deterministic stress is **87/87 PASS**. The complete pre-commit closure tree is **638/638 PASS**: benchmark **20/20**, broker **37/37**, P2 **9/9**, P4 **1/1**, P5/P8 dynamic **5/5**, P6 **9/9**, P7 failover/runtime **12/12 + 2/2**, client **24/24**, common **3/3**, controller **113/113**, groups **55/55**, protocol **45/45**, P2/P6/P7 goldens **11/11 + 6/6 + 4/4**, Raft **128/128**, replication **101/101** and Storage **53/53**. Workspace Clippy/doc/release-build/fmt/diff and immutable/source audits pass. `docs/P8_CLOSURE_REPORT.md` binds the evidence and exact-head procedure
- final P8 closure is authoritative at exact commit `37c07a5868950873de6aa250428e4f922a6552ec`; its `P8 exact-HEAD qualification: PASS` Git note repeats focused **87/87**, benchmark **20/20**, complete serialized **638/638**, release build, Clippy/doc/fmt/diff and frozen compatibility gates and authorizes P9 as the next phase
- P9-A is authoritative at exact commit `b7c8552646f7e3cd766bcba469c9bc9e0ce766d7`; its `P9-A exact-HEAD qualification: PASS` note records a docs-only freeze, complete **638/638** serialized compatibility regression and authorizes P9-B only
- P9-A discovered a real V1/P8 interaction before implementation: V1 requires `offset_delta == record ordinal`, so keyed removal from the middle of a P8 batched frame cannot preserve later survivor offsets in V1. ADR-019 therefore freezes an explicit Storage V2 used only for immutable sparse retained history at/below the committed snapshot floor; active append and the live Raft suffix remain byte-exact V1
- the same freeze audit found P4 RebootstrapPackage V1 requires a contiguous Raft prefix that physical P9 cleanup may no longer possess. P4 V1 remains frozen; P9 specifies a retained-history-aware versioned rebootstrap package instead of synthesizing discarded history or weakening old validation
- P9-B P9-021..042 now adds the closed controller-owned Delete/Compact/CompactDelete policy model, optional time/size retention limits, positive 24-hour historical tombstone-grace default and an internal Disabled/ReservedForProtectedFloor protection enum; historical V1/V2/V3 controller state derives infinite delete retention so level-5 activation alone deletes nothing
- public feature key `15 = RETENTION_COMPACTION` is additive; committed cluster feature level 5 remains the only runtime activation authority. An ACTIVE max-4 broker or controller voter whose range ends at 4 blocks level 5, while old feature meanings remain unchanged
- controller command/snapshot V4 are additive after P8 V3. V4 CreateTopic atomically commits frozen V1 topic identity/assignments plus P8 performance policy plus P9 retention policy; V4 AlterTopicRetentionPolicy uses exact expected/current compare plus ACTIVE lifecycle validation; FinalizeDeleteTopic removes P8/P9 policy rows atomically
- Snapshot V4 wraps a canonical V3 base plus a CRC-protected TopicId-sorted P9 policy table; legacy snapshots reconstruct historical defaults and V4 rejects unsorted, duplicate, orphan, invalid or noncanonical policy state. Real RF3 qualification commits level 5, creates/alters a P9 policy, kills the leader, verifies the successor, restarts the old leader and recovers identical altered state
- public CreateTopic/DescribeTopic V2 carry/return P8 performance plus P9 retention policy; AlterTopicConfig V0 owns only partial P9 retention mutation using present/clear masks. Subscription protection is intentionally absent from the public body. ApiVersions advertises V2/key6 only after the broker cache observes committed level 5; the existing level-4 P8 integration still advertises only V1 and rejects AlterTopicConfig
- new P9 public body goldens are isolated in `crates/astralog-protocol/tests/p9_golden_vectors.rs`; original P2/P6/P7 golden sources and Storage V1 bytes are untouched. Focused P9-B qualification is **14/14 PASS** (controller **8/8**, protocol/registry **3/3**, P9 wire goldens **2/2**, dynamic broker **1/1**)
- the final P9-B pre-commit candidate is complete serialized **653/653 PASS**: benchmark **20/20**, broker **37/37**, P2 **9/9**, P4 **1/1**, P5/P8/P9 dynamic **6/6**, P6 **9/9**, P7 failover/runtime **12/12 + 2/2**, client **24/24**, common **3/3**, controller **122/122**, groups **55/55**, protocol **48/48**, P2/P6/P7/P9 goldens **11/11 + 6/6 + 4/4 + 2/2**, Raft **128/128**, replication **101/101**, Storage **53/53**. fmt/Clippy/doc/diff/frozen-source gates pass
- two pre-existing long P6 failover observations transiently failed under qualification-host load, each passed independently unchanged, the full P6 suite then passed **9/9**, and the final complete workspace also passed P6 **9/9**; no P6 source was modified. A Clippy large-enum finding caused by the larger P9 controller state was fixed structurally by boxing only the in-process full metadata-sync snapshot payload, with no wire/disk/authority change
- `docs/P9_CONTROLLER_RETENTION_METADATA_REPORT.md` binds the authoritative P9-B implementation/proof boundary at `f583b6b8e8a4196c22a6b36ae946c8d5501065f1`; its exact-head PASS note authorizes P9-C only
- P9-C now implements a parallel retained-history Storage V2 decoder/encoder without editing frozen V1 format/compression bytes: 72-byte retained-history segment headers, sparse schema-3 u64 offset deltas, None/LZ4/Zstd reuse, retained P9 control schema 2, zero producer fields and strict corruption/truncation/overflow validation
- new immutable P9-C Storage fixtures lock V2 header, sparse None/LZ4/Zstd frames, retained control and mixed-manifest bytes while every historical V1/P8 Storage golden remains unchanged
- `PartitionLog` now consumes a valid `segments.manifest` as exclusive live inventory authority; no-manifest partitions preserve exact generation-0 discovery until explicit byte-preserving generation-1 adoption. Live manifests require one last mutable V1 active segment; referenced identity/version/size errors fail closed without directory fallback
- manifest-mode roll publishes the next P1 generation; immutable-target Raft suffix repair uses copy-on-write manifest publication while mutable-tail repair stays V1 in-place. Mixed immutable V1/V2 history rebuilds derived indexes and Fetch returns exact sparse offsets without changing committed high watermark semantics
- V2 retained frames are explicitly excluded from `read_frames`/`read_frames_from_raft`. A replication-adapter proof below snapshot floor confirms retained V2 remains Fetch-visible but never appears in P3 `RecoveredState.log`
- P9-C is authoritative at exact commit `dc39dd7cf8ca278ec0d1a4a4b04fa965ae0636ee`; its exact-head PASS note records focused **14/14**, Storage **66/66**, replication **102/102**, combined P9 **28/28**, complete serialized **667/667**, frozen-V1/golden checks and authorizes P9-D only
- P9-D P9-067..087 now introduces application-maintenance V3 `AdvanceLogStartOffset` mapped to dedicated Storage control kind `0x0901`/version 1. The committed logical floor reconstructs from Raft-ordered controls, defaults to zero for historical partitions, consumes no stream offset, cannot regress and cannot exceed committed high watermark
- delete-retention planning is manifest-authoritative and source-boundary-only: legacy inventory is adopted before planning; sources must be closed, committed and at/below the persisted snapshot floor. Time retention uses successor durable creation time as close lower-bound, and a backward roll clock explicitly stops time cleanup so clock regression cannot accelerate deletion
- size retention uses checked authoritative `.log` byte arithmetic, never deletes the mutable active segment, and accepts that active/protected bytes may keep usage over target. The internal protected-floor ceiling caps ordinary advancement; required-but-missing protection fails toward retaining data
- physical omission happens only after the logical floor commits. Generation-fenced manifest publication omits wholly obsolete immutable prefix files, publishes authority before unlink, pins the latest still-required floor-control segment, and leaves straddling V1 bytes unchanged while hiding expired records logically
- Fetch V0 now returns the committed `log_start_offset` and returns `OFFSET_OUT_OF_RANGE` with no stale records below it. ListOffsets `-2` returns the floor; timestamp scans skip logically expired/physically absent history without changing the V0 wire layout
- P9-D RF1/RF3 qualification covers restart, stale leader epoch, lagging-voter rejoin, leader failure and post-failover floor preservation. Focused P9-D is **10/10 PASS**, combined P9 **38/38**, Storage **72/72**, replication **104/104**, broker **38/38**, dynamic integration **7/7**, and the complete serialized pre-commit workspace is **677/677 PASS** with fmt/Clippy/doc/diff gates green
- P9-D is authoritative at exact commit `fe64a0a23166840b90e81ef3e820952490627483`; its exact-head PASS note records focused P9-D **10/10**, combined P9 **38/38**, complete serialized **677/677** and authorizes P9-E only
- P9-E P9-088..111 now adds a deterministic bounded cleaner plan bound to manifest generation, controller policy epoch, persisted P3 snapshot floor, committed Raft/log-start maintenance state and finite source/decode/key-index/output ceilings. The active mutable segment and any source above the snapshot floor are rejected
- committed latest-key selection indexes only non-null keys at/above the P9-D logical floor. Null-key records remain, keyed non-tombstones are removed only when superseded by a greater committed original offset, surviving records retain exact bytes/headers/timestamps and sparse V2 retains original ordering/offsets across None/LZ4/Zstd history
- ordinary P9-E compaction conservatively keeps tombstones and the latest `AdvanceLogStartOffset` control anchor. Any V1 source carrying P7 producer identity is deliberately ineligible until P9-F commits an authoritative producer-state checkpoint; future P9-F/G retained control kinds are rejected rather than executed
- cleaner output uses the frozen P1 `GenerationTransaction`: complete replacement bytes are fsynced and finally named before atomic manifest publication. Manifest/policy/snapshot/maintenance staleness is revalidated before switch, and a broker metadata-epoch publication guard closes locally observed policy drift during the final manifest swap
- `PartitionReadGeneration` gives Fetch one complete generation; old unreachable files move to a retired generation and remain on disk while any reader owns that generation. A focused concurrency proof reads the complete old record set while the live log reads the complete compacted set, then reclaims only after the old reader drops
- P9-E focused qualification is **8/8 PASS**, combined P9 **45/45**, Storage **78/78**, replication **105/105**, broker **38/38** and dynamic integration **7/7** with fmt/Clippy/doc/diff green. The complete serialized documented candidate is **684/684 PASS**. P9-E is **authoritatively closed** at exact commit `9da9aac850344a1a3ee4412bdb0026c409689800` with its exact-HEAD PASS note
- P9-F P9-112..129 now adds V3/kind-2 `ProducerStateCheckpoint` using canonical bounded P7 producer-state snapshot bytes in a Raft-committed, offset-free application-maintenance control. The encoding preserves partition identity, committed Raft coverage, producer epochs/next sequences and newest-five exact retry identities; validation enforces the P7 4,096-entry bound and existing command/control byte ceilings
- checkpoint proposal completion is quorum-commit gated in the broker partition actor. P9-E's cleaner now admits producer-bearing closed sources only after committed checkpoint coverage **and** a later persisted P3 snapshot that covers the checkpoint control; no local derived sidecar grants cleanup authority
- recovery prefers the latest valid retained committed producer checkpoint and replays later committed idempotent V1 frames; `producer.snapshot` remains discardable derived acceleration. RF1 proves exact retry/old-epoch/changed-content/overlap/gap/too-old behavior and unchanged committed high watermark after physical compaction, sidecar deletion and restart
- the 4,096-producer resource-bound test compacts and restarts from a full canonical map. Real RF3 three-broker failover proves the compacted checkpoint survives leader replacement and an exact duplicate returns the first committed offset/Raft range, without a new append. The interrupted RF3 reproduction now passes **six consecutive standalone runs** on the recovered tree
- `docs/P9_PRODUCER_CHECKPOINT_REPORT.md` maps P9-112..129 to implementation and tests. The documented P9-F closure candidate passes combined P9 **49/49**, serialized workspace **688/688**, replication **108/108**, Storage **78/78**, dynamic integration **8/8**, P7 **12/12 + 2/2**, and fmt/Clippy/doc/diff/frozen-source checks. This is candidate evidence only; exact-HEAD closure authority requires a PASS Git note on the same closure SHA before any P9-G work
- P9 retention/compaction policy
- P10 work queues/retries/DLQ
- P11 production security/operations/upgrades
- P12 final end-to-end chaos/benchmark closure

These are frozen later-phase responsibilities, not missing P4 replication
correctness behavior.

## Next action

P9-F is **authoritatively qualified** at commit
`cd064a4fe60141590b6062957475aa15ee560717` with Git note
`P9-F exact-HEAD qualification: PASS`. P9-G's bounded local
candidate was initially committed as `43be8dd`, amended as
`4564d39` then `a952602`. The first two exact-HEAD workspaces were
**RED P5 11/12 and RED P5 10/12**. `a952602` passed 730/730 once but
failed P5 10/12 on its unchanged repeat. There is **no P9-G PASS Git
note**. Its P9-130..150 implementation and tests include
RF3/RF4 all-voter safe authority, committed Joint voter set and exclusion,
tombstone non-resurrection, P3-authorized V2 rebootstrap and interrupted
real child-process publication oracles. The initial source passed two full
**730/730** offline-locked serialized pre-commit workspace runs and static
gates; none supersedes the failed exact-HEAD evidence. A subsequent P5
run was contaminated by host idle sleep; an independent new awake focused
run was **RED 3/4**, with one eligible voter left at durable safe 0 while
the other two were at 4. A test-only diagnostic addition now prints that
failure's live three-voter actor/membership/manifest/clean/safe evidence.
An isolated awake RF3 tombstone rerun passed 1/1 and full awake P5 12/12
passed on the updated uncommitted source. All P3/snapshot/quorum and ambiguous
P2 mutation fences remain intact. Exact-clean-HEAD complete workspace,
Clippy/docs/fmt, frozen-source and repeatability gates must qualify before
attaching a P9-G PASS note. Subsequent diagnostic full-P5 runs exposed the
actual defect: a recovering voter with unverified retained history still
reported a volatile P3 Leader role, and its safe fail-closed step rejection
was treated as fatal by the broker actor. The current uncommitted runtime
candidate instead quarantines such old-leader authority, remains alive for
newer-term P3-authorized repair, blocks all client Fetch/maintenance calls
and idempotent cached/coalesced acknowledgements until current-generation
history is verified. No fabricated P3 commit or skipped tombstone gate is
allowed. Focused qualification is underway, not exact-HEAD release authority.
Full chronological RED and GREEN evidence is in
`docs/P9_G_WORK_IN_PROGRESS.md` and `docs/P9_G_CLOSURE_REPORT.md`.
P9-H/I and P10–P12 remain blocked until their predecessor gates.
