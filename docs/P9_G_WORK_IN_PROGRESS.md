# P9-G In-Progress Engineering Ledger — NOT A CLOSURE REPORT

Status: **P9-G IMPLEMENTATION IN PROGRESS / NO QUALIFICATION AUTHORITY**

Authoritative parent: P9-F exact-HEAD PASS
`cd064a4fe60141590b6062957475aa15ee560717`.

Scope is only **P9-130 through P9-151**. P9-H/I and P10-P12 must not
start. Every P9-G checklist checkbox remains unchecked until the associated
end-to-end proof exists; P9-151 requires a separate clean exact-HEAD PASS note.

## Recovered and implemented local sub-boundaries

1. **P9-130..132 verified immutable-prefix cleaning implemented, awaiting
   independent multi-node qualification:** a CRC32C local
   `clean.progress.v1` sidecar records TopicId, PartitionId, manifest
   generation and exclusive local clean-compacted offset. Missing, corrupt,
   wrong-identity, wrong-generation or absent-manifest evidence yields zero.
   Same-generation regression and high-watermark violations are rejected.
   The sidecar writer is now invoked only **after successful COW manifest
   publication**, under the actual authoritative manifest generation. A
   bounded whole-inventory verifier uses the latest-key index to find any
   surviving older superseded keyed value in *all* immutable files, including
   unselected files, and limits its exclusive upper bound to the first mutable
   V1 active offset and the committed high watermark. Every immutable source
   must be at/below the captured persisted snapshot floor; a count/byte-budget
   overflow, stale generation, damaged frame or incomplete proof publishes no
   progress (or zero). Partial-source and complete-COW tests verify the
   distinction, restart binding, generation invalidation and bounded fallback.
   Future append/clean cycles must not borrow stronger authority from this
   derived sidecar. **Only committed maintenance plus an explicitly enabled
   current-policy CompactDelete gate may permit physical tombstone deletion.**
2. **P9-133..138 transport and broker-reporting path implemented; end-to-end
   P5/RF3 qualification still outstanding:**
   fixed-size 80-byte CRC32C report and bounded `CleanProgressTracker`
   reject stale term/epoch, claimed source identity versus observed transport
   identity, incarnation, assignment generation, learner-only reports,
   offset above committed high watermark and regressions. Missing eligible
   voter blocks the candidate; joint old/new voters are unioned. A new
   committed assignment requires a fresh tracker and discards old reports.
   Transport message kind 3 carries exactly one fixed-size progress report,
   preserving all historical Raft kind 1/kind 2 encodings. Identity is checked
   against transport source/header and controller-committed P5 voter/assignment
   records. The dynamic broker actor emits bounded best-effort reports on
   periodic ticks **only when explicitly opted in** with
   `DynamicReplicationConfig.enable_p9g_progress_reports` and controller P9
   Feature Level 5 active. The opt-in is **disabled by default** so new
   sideband frames cannot disconnect historical P9-F peers in a mixed rollout.
   Operators must first establish homogeneous peer capability. Only effective
   P3 voter sets equal to committed P5 voters
   are eligible. The current leader collects reports in a term-fenced tracker;
   a missing/offline voter continues blocking progress. Static P4-only
   clusters do not activate the P9 sideband. Optional progress packets use
   `try_send` and are dropped when the actor control queue fills, reserving
   the blocking path for Raft traffic. A previously accepted high report is
   **revoked**, not reused, when its voter produces a lower valid clean
   offset. Deterministic expiry after 10 broker ticks revokes reports from
   offline/paused eligible voters. A real TCP RF3 peer network test confirms
   missing/expired voters block advancement; an RF3 committed P5 controller
   topology test verifies explicit feature opt-in and incarnation refresh.
   Remaining multi-broker actor and controller joint-consensus failure
   qualification is NOT complete.
3. **P9-139..140 broker safe proposal wired behind dual opt-in:**
   command V3/kind 3 `AdvanceCompactionSafeOffset` persists as Storage
   control kind `0x0903` without consuming stream offsets. Committed
   V1/V2 retained-history reconstruction starts at zero and rejects
   nonmonotonic/high-watermark-invalid controls. RF1 requires complete
   current-voter report evidence, leadership/assignment fences, quorum
   commit, then verifies restart recovery and unchanged stream high watermark.
   Broker collects transport-authenticated and P5-fenced report evidence and
   only proposes the all-voter minimum when both P9 sideband and V2
   rebootstrap capability are enabled, the P3 current-term leader barrier
   commits, and no maintenance proposal is already pending. Terminal proposal
   outcomes are consumed and cleared. Default opt-ins remain **false** to
   protect prior P9-F clusters. Full RF3 commit/failover and joint P3/P5
   voter-change proof is still required before activation/closure.
4. **COW safe-control anchor rewrite primitives added; qualification still
   incomplete:** latest committed V1/V2 `AdvanceCompactionSafeOffset` control
   is recognized and preserved in the keyed-compaction reconstruction and
   V2 rewrite branches rather than mistaken for a P4 control. Ordinary P9-E
   COW value compaction remains allowed after a positive safe offset; absence
   of a CompactDelete gate never permits tombstone omission. Latest safe
   anchor is rebuilt on restart and tested in V1->V2 rewrite. P9-D physical
   retention now identifies the latest committed safe-control anchor as a
   mandatory manifest-retained file alongside the latest log-start anchor.
   Its bounded test proves an older obsolete data file can be reclaimed but
   the positive safe-control segment remains committed and readable after
   restart. Real failover/retention accounting proof remains pending.
5. **P9-141..145 tombstone and rebootstrap-required partial:** explicit
   `TombstoneRemovalGate` captures conservative monotonic successor-created
   closed-segment age, controller cleanup-policy epoch, grace, verified local
   whole-prefix cleaning, persisted P3 snapshot floor, and committed safe
   offset. COW selection and publication re-check manifest/maintenance/
   policy epoch and preserve recovery anchors. Tests prove backward-clock
   grace suppression, too-young tombstone retention, stale policy rejection,
   proper marker omission and restart with no older value resurrection.
   A three-replica retained-history model rewrites the marker on one replica
   and then its logical successor, and reopens all replicas without older
   value resurrection. **Not a live network RF3 failover proof.**
   `ReplicatedPartition::tick` now prevents local election/campaigning when
   verified sidecar is below committed safe, revalidating once per manifest/
   boundary, and fails current leaders closed. This still requires RF3 test.
6. **P9-146..150 bounded V2 package + P3-authorized atomic installation
   implemented, not yet RF3-qualified:** new
   `RebootstrapPackageV2` and independent `ASTRBTP2` codec carry the
   authoritative manifest generation, ordered immutable and mutable retained
   files, P3 SnapshotMetadata, committed P9 maintenance offsets, next stream
   offset and optional canonical P7 producer checkpoint. The codec enforces
   configured total bytes, file count, per-file byte, snapshot/manifest/
   producer-checkpoint bounds before allocation; each file is protected by
   SHA-256 and CRC32C, and the whole package by canonical CRC32C. The old
   `ASTRBTP1` decoder remains unchanged and rejects the new version.
   The source extractor copies only immutable/mutable files in the exact
   authoritative manifest generation with limits and validates before send.
   The receiver checks version/CRC/SHA, exact P3 InstallSnapshot snapshot,
   Storage V1/V2 history, producer checkpoint presence/reconstruction rules,
   committed maintenance floors, Raft snapshot boundary, next stream offset
   and independent whole-prefix clean proof, then stages/fsyncs and publishes
   using the P4 intent + backup + atomic directory rename protocol. A
   focused test starts with V2 sparse retained Raft index 2 **without index
   1**, installs under P3 authorization, rejects mismatched P3 snapshot and
   corrupted staged V2 bytes without replacing old authority, appends an
   ordinary V1 Raft/stream suffix, restarts and exercises interrupted
   backup/intent cleanup after V2 publication. The peer broker dispatch uses
   a separate P9-only opt-in `enable_p9g_rebootstrap_v2` (default false),
   controller feature-level 5 and homogeneous advertised peer support.
   Live RF3 InstallSnapshot, interrupted-install matrix and producer-bearing
   V2 retained checkpoint failover proof remain **pending**.

## 2026-10-14 continuation: bounded qualification evidence (not closure)

- Resumed exact parent `cd064a4fe60141590b6062957475aa15ee560717`;
  no checklist action marked, no P9-G commit, no qualification Git note.
- Fixed a keyed-tombstone bypass: `log_start_offset` alone cannot erase a
  tombstone. Physical omission still requires offset strictly below the
  committed safe bound plus the explicit grace/CompactDelete gate. A new
  focused regression proves log-start advancement does not bypass this rule.
- Extended real controller-managed RF3 integration: with both P9-G sideband
  and V2 transport explicitly opted in across all three feature-level-5
  brokers, quorum production, real P3 broker-leader failover and successor
  quorum production with an offline eligible voter passed. This does NOT
  claim a successful all-voter safe-offset advance or live V2 rebootstrap.
- Added direct `ReplicatedPartition` P3 authority test: V2 InstallSnapshot
  accepted under P3, stale repeat was a no-op, ordinary V1 AppendEntries
  consumed `snapshot + 1`, and re-open recovered its suffix and snapshot.
- Extended V2 storage-adapter crash matrix with real V2 bytes: mismatched
  snapshot, self-consistent package containing corrupt Storage bytes, crash
  before intent (old survives; orphan stage discarded), crash after durable
  intent and moving old to backup but before publishing new (backup restored),
  and crash after new publication before backup/intent retirement (validated
  new generation retained), then ordinary V1 suffix and restart.
- Added explicit P9-F producer checkpoint protection to physical P9-D
  retention: latest committed producer checkpoint segment is a retained
  anchor independently of the latest P9-G safe or P9-D log-start anchor.
  A new Storage restart regression exercises the relative ordering.
- Added a V2 producer-checkpoint round-trip regression whose source
  physically removes the producer-bearing original frame, transports the
  committed P9-F control without `producer.snapshot`, reconstructs the
  original retry window and appends/recovers a new idempotent V1 suffix.
  **Focused checkpoint/V1 suffix test PASS (1/1).** This does not substitute
  for the full live RF3 rebootstrap-after-compaction test.

All flags for P9-G reporting and V2 dispatch remain **disabled by default**;
P9-H/I and P10–P12 are disabled. The required live RF3 no-resurrection,
behind-safe rejoin/V2 snapshot network install and complete exact-HEAD
failure matrix remain blockers to authoritative P9-G closure.

## Further bounded regressions and remaining P6 qualification

- Broker manager reconciliation originally compared only P5/Raft routing
  fields. A pure P9 Feature Level activation or P5 assignment-generation
  authority change could leave the running partition actor using an obsolete
  progress tracker. The comparison now includes both the exact committed
  `progress_authorities` map and V2 rollout flag in its no-op and routing-only
  branches, forcing actor rebuild when P9 authority changes. A deterministic
  manager test switches Feature Level 1 -> 5 without any broker/topic/assignment
  routing change and proves the new actor authority is installed and a later
  unchanged reconciliation is a no-op. **Focused test PASS (1/1).**
- P3 tick and inbound packet paths initially reconstructed the full
  manifest-authoritative compaction safe offset on every call, including P6
  groups that have never enabled P9-G. They now cache the safe authority by
  `(committed_raft_index, manifest_generation)`, re-probing after committed
  Raft or manifest changes. The sidecar is read only if the committed bound
  is positive. Restart always begins with an empty cache. The public
  correctness probe remains an independently checked read.
- Physical retention size reporting and the size-target loop now preserve
  the same P9-G safe and P9-F producer checkpoint segment anchors that
  publication must retain. The regression compares the projected retained
  byte count with actual on-disk retained file bytes after publication;
  **focused test PASS (1/1)**. Ordinary P9-D behavior with no such anchors
  uses the prior rules.
- The new V2 committed P9-F checkpoint test passed (1/1), preserving exact
  original retry ranges and later V1 idempotent suffix after restart without
  a derived producer snapshot. V2 P3-authorized install, stale-install no-op,
  ordinary suffix and restart passed (1/1). Real controller-managed RF3 with
  opted-in P9-G peer reporting/V2 transport and quorum leader failover passed
  (1/1). Workspace Clippy with all features/all targets and denied warnings
  passed after these changes.
- An attempted complete `cargo test --workspace -- --test-threads=1` on an
  *earlier pre-cache* worktree exposed timeouts/failures in legacy P6 routing
  tests; it was terminated without claiming a workspace PASS. The isolated
  P6 churn/timeout case **passed (1/1)** after caching (87.63 s); the
  P6 leader-fence case passed individually (1/1, 27.15 s), and the P6
  group-history/recovery case passed individually (1/1, 63.60 s). Thus the
  three earlier failures are individually green, but the complete serialized
  workspace still needs requalification on a stable worktree.
  **No P9-G commit or exact-HEAD note is authorized yet.**

### Last bounded validation on the recovered P9-G worktree

- `cargo test -q -p astralog-storage --lib -- --test-threads=1`:
  **PASS 88/88**.
- `cargo test -q -p astralog-replication --lib -- --test-threads=1`:
  **PASS 122/122**.
- `cargo test -q -p astralog-broker --lib -- --test-threads=1`:
  **PASS 40/40**.
- `cargo clippy -q --workspace --all-targets --all-features -- -D warnings`:
  **PASS**.
- `cargo fmt --all`, `git diff --check`: **PASS**.
- RF3 controller-managed feature-5 sideband+V2-opt-in broker-failover
  integration: **PASS 1/1**.
- Isolated pre-cache P6 regression failures on the corrected tree:
  **churn 1/1**, **leader fencing 1/1**, **history/recovery 1/1** PASS.

This is **not full-workspace or exact-HEAD authority**. Remaining required
P9-G proof: a genuinely behind-safe retained-history replica crossing a
P3/P5 RF3 change by network-installed Rebootstrap V2, including interrupted
publication, resumed sparse retained Fetch, exact P7 producer retry and V1
suffix convergence, plus the live no-resurrection tombstone gate across
leadership/membership churn. Then run the complete serialized workspace,
independent exact-HEAD tests, and only then update `docs/P9_CHECKLIST.md`,
commit P9-G and attach an exact-HEAD PASS note. P9-H/I/P10-P12 remain off.

## 2026-10-14 subsequent bounded RF3 V2 dispatch audit — NOT CLOSURE

On the preserved uncommitted P9-G tree, an expanded actual-controller RF3
test exposed a **real rollout fault**: when `enable_p9g_rebootstrap_v2=true`
but a leader had not yet adopted any authoritative retained-history
manifest, broker outbound InstallSnapshot handling attempted V2 export and
failed. This terminated the actor during a normal P9-E snapshot/cleaning
sequence, yielding `BrokerNotAvailable`. The dispatch now uses the frozen
P4 V1 codec for contiguous pre-manifest partitions and V2 only after an
authoritative manifest exists. A separate new guard recognizes obsolete P3
InstallSnapshot requests when the leader has committed a later prefix: it
requests a current-term P3 snapshot refresh and defers the stale outbound
packet; pending/uncommitted tails are also deferred. No stale V2 generation
is installed as a substitute.

The expanded RF3 test now exercises explicit P9-G opt-in, an offline
eligible follower, quorum appends with repeated keys, source COW rewriting,
simulated local partition-directory loss after follower shutdown, restart,
and controller-observed P3 committed-index convergence across all three
brokers, followed by leader failover and a fresh quorum suffix. In the
diagnostic run all three voters converged to committed Raft index **3**
without a V2 manifest appearing on the rejoined voter. This is a legal
ordinary P3 AppendEntries recovery path, NOT evidence of an actual V2
package on the wire. Do not count the case as the required forced-V2
InstallSnapshot network qualification. Temporary diagnostic logging used to
separate actor failure from legal catch-up was removed from broker code.
The corrected RF3 opt-in/compaction/disk-loss/failover/quorum-suffix
regression subsequently **PASSED 1/1**. The same bounded command also passed
workspace-wide `cargo clippy --workspace --all-targets --all-features --
-D warnings` and `git diff --check`. This proves compatible P4 V1 dispatch
before manifest adoption, healthy P3 incremental rejoin when it is sufficient,
and no unnecessary fatal actor termination; it is **not V2 transfer proof**.

**Remaining mandatory closure gate:** force an RF3/P5/P3 scenario where the
current leader cannot reconstruct/send the old contiguous prefix, observe
actual `ASTRBTP2` transport plus P3-authorized staging/swap on a recovering
eligible replica, then independently prove retained Fetch and exact producer
retry/suffix after P3 leadership change, interruptions and tombstone grace.
This gate remains open regardless of passing opt-in/regular-catchup tests.

## 2026-10-14 deterministic RF3 natural-P3 V2 selection — qualified focused proof

- Added `p9g_rf3_p3_naturally_requests_v2_snapshot_after_sparse_clean_and_follower_disk_loss`
  in `crates/astralog-replication/src/simulation.rs` with a small-segment
  RF3 disk-backed test harness. The test elects a real P3 leader, commits
  two different values of the same key, loses one eligible follower, and
  compacts its old closed segment with a persisted P3 snapshot floor.
- The offline follower's partition directory is then deleted and reopened.
  Rather than fabricating a Raft `InstallSnapshotRequest`, the test delivers
  normal P3 AppendEntries/probe responses until P3 itself emits the snapshot
  request. The source exports only its authoritative manifest inventory as
  `ASTRBTP2`; the test decodes the exact package and calls the receiver's
  P3-fenced V2 install path. The follower recovers only the newest keyed
  record (the physical old value is not exposed).
- A fresh V1 application suffix is committed by quorum and received by the
  rebootstrap follower. That follower restarts, preserves its V2 manifest
  and exact Fetch values, and joins a new RF3 leader election after the
  original leader crashes. The successor commits a further V1 suffix;
  retained Fetch on the recovered voter has exactly the winner plus two
  suffix values, and does not resurrect the obsolete keyed value.
- **Focused regression PASS 1/1** after correcting the test harness to
  advance all surviving voters' logical lease/election clocks instead of
  ticking only one `PreCandidate`. This is not a production P3 change.

This natural P3 selector proof complements the earlier actual authenticated
RF3 TCP ASTRBTP2 delivery and staged crash-recovery tests. It **does not**
yet exercise the full combined dynamic P5-controller actor/connection/
incarnation boundary with a forced automatic V2 transfer, nor a tombstone
removed after the full multi-voter safe/grace gate. Do not check P9-130..151
or attach exact-HEAD authority until those remaining proofs are complete.

## LATEST 2026-10-14 — controller-managed RF3 post-COW V2 install verified

This section **supersedes the older statement above** that the dynamic
P5-controller RF3 rejoin never installed V2. Stronger live instrumentation
and the corrected test exposed two false-positive traps and established a
real post-COW network installation, without granting final P9-G authority.

1. The original controller-driven test treated cached controller Raft
   progress as proof of the rejoined broker's current state. That could
   report an index from before local disk loss. The corrected test requires
   a **live observation of the restarted follower's local partition actor**
   and a persisted authoritative V2 manifest on that follower.
2. An offline follower could be sent an earlier manifest generation 1 while
   the leader was adopting inventory prior to keyed COW. Installing it is
   legal for the P3 snapshot index but would not prove later sparse history:
   a repeated InstallSnapshot at the same boundary cannot upgrade an already
   accepted generation 1. The test now first brings all three P5 eligible
   voters to the same committed Raft boundary, then publishes a COW source
   generation before shutting down and wiping the selected follower.
3. Initially the old segment contained only a superseded value, and COW
   legitimately erased the entire segment without creating a V2 sparse
   replacement. The test fixture now writes a second, independently live
   keyed record in that segment. P9-E COW must omit the old overwritten
   value but retain the independent key in an immutable **Storage V2**
   sparse segment; `compaction.outcome.replaced_segments > 0` alone is not
   sufficient evidence.
4. The test requires source P3 snapshot floor == committed Raft index, source
   leader still authoritative, source manifest generation >= 2, the **exact
   same manifest** installed on the restarted follower, direct local follower
   committed progress at least the source snapshot boundary, and **at least
   one immutable Storage V2 retained file whose bytes equal the source**.
   It then shuts down the original P3 leader, waits for a new RF3 leader and
   commits an ordinary quorum V1 suffix in the new term.
5. `cargo test -q -p astralog-broker --test p5_dynamic
   p9g_rf3_sideband_and_v2_opt_in_survive_broker_leader_failover -- --exact
   --test-threads=1`: **PASS 1/1** on the stricter generation-and-exact-file
   proof. The prior failed strict tests were real qualification findings,
   corrected by strengthening the sequencing and fixture, not by removing
   the guard. Temporary `P9G_OUTBOUND_SNAPSHOT`, `P9G_SEND_V2`, and receiver
   traces were removed from product code after diagnosis.

The remaining P9-G closure requirements now concentrate on independent
**live** joint P3/P5 voter membership and broker-incarnation fencing under
RF3 failure, all-voter replicated compaction-safe offset advancement,
grace/snapshot-gated **tombstone** removal with no resurrection on a newly
elected rebootstrap voter, network-installed P9-F producer retry recovery,
full crash/restart interruption matrix, and exact-HEAD postcommit authority.
The previous complete serialized workspace PASS predated this final stronger
integration fixture; it must be repeated on the final closure tree.

No P9-G checklist box, commit, or PASS Git note is authorized by this
evidence alone. P9-F `cd064a4fe60141590b6062957475aa15ee560717`
remains the authoritative frozen HEAD; P9-H/I/P10–P12 remain disabled.

## Required before P9-G can be called complete

### 2026-10-15: resumed P5 incarnation-rotated retained V2 RF3 proof

- Continued from the preserved `cd064a4...` P9-F HEAD and the uncommitted
  P9-G implementation, without editing any P9-130..151 checklist box.
- Extended the live P5-controller RF3 source-COW/V2 rebootstrap regression:
  the previously eligible follower shuts down, then a controller Raft-committed
  `RejoinBroker` rotates its broker incarnation **1 to 2** before the local
  partition is erased/restarted. The test independently reads the committed
  P5 broker record and rejects an uncommitted incarnation. The real peer
  transport and actor must re-establish authority under that new identity.
- The first isolated incarnation-rotation attempt failed its bounded V2
  convergence gate; the subsequent diagnostic retry and **three further
  consecutive standalone runs all PASS (3/3)**. The initial intermittent
  failure is not silently treated as impossible; the final full workspace
  and failure matrix remain independent gates.
- Existing same test retains strict **source post-COW manifest generation**,
  exact authoritative target manifest, byte-for-byte immutable Storage V2
  file, live restarted follower committed index, P3 leader-failover and
  successor quorum-write checks. It cannot pass on stale P5 controller
  progress, an early pre-COW package, or incremental-only catch-up.
- No product routing/fencing mechanism was weakened to obtain this proof.

### 2026-10-15: final-worktree regression gate — FAILED, NOT CLOSURE

- Clean-build `cargo fmt --all -- --check` and workspace-wide
  `cargo clippy --workspace --all-targets --all-features -- -D warnings`
  passed with `CARGO_BUILD_JOBS=1`, `CARGO_INCREMENTAL=0`.
- `cargo test -q --workspace -- --test-threads=1` **FAILED** at the
  existing `astralog-broker --test p6_routing` integration suite: **8/9
  passed, 1 failed** after 592.49s. The failing
  `p6_silent_member_gets_fresh_grace_then_times_out_after_coordinator_failover`
  case timed out waiting for initial offsets-shard leader readiness in
  `p6_routing.rs:565`. The full-workspace run therefore exited code 101 and
  no clean-workspace PASS exists for this final P9-G tree.
- Re-running only the exact P6 case (serialized, `CARGO_INCREMENTAL=0`)
  also **FAILED**, but at a different boundary after ~84.84s: the initial
  offsets leader became ready, then a `JoinSubscribe` response was ambiguous;
  the client correctly refused a blind replay at `p6_routing.rs:1992`.
  This is not yet proven a deterministic P9-G regression or an environment
  flake. It is a **mandatory open P6 compatibility qualification blocker**;
  never weaken P6 commit-before-response/ambiguous-mutation safety to make
  this test green.
- The verified P9-G live RF3 incarnation-rotated V2 test separately passed
  three consecutive runs; a pass there does not supersede the failed P6
  workspace gate. No checklist action, Git commit or P9-G PASS note is
  justified by this checkpoint.
- A second unchanged-tree standalone rerun of that exact P6 silent-member
  failure subsequently **PASSED 1/1 in 38.69s**. The sequence is therefore
  full-suite offsets-shard readiness timeout, isolated ambiguous JoinSubscribe
  (no unsafe retry), then isolated PASS. Treat as **intermittent/unresolved
  qualification**, not as a proven product fix or an authoritative workspace
  PASS. Keep P6 protocol behavior, safety checks, and bounded client retry
  semantics unchanged pending an evidence-based root cause.


- Qualify post-COW whole-prefix progress and bounded sidecar behavior with
  RF3 failover, read generations, concurrent appends, corruption and disk
  rollback. Confirm no stale reporter survives a controller refresh.
- Independently qualify actual peer-sideband reporting with controller-managed
  RF3, joint P3 configuration, P5 broker exclusion/incarnation rotation and
  restart. Only after this proof, activate commit-before-use of candidate
  safe offsets, with bounded completion/failure handling.
- Qualify the COW safe-control anchor, physical retention publication and
  tombstone-policy gates over leader changes with actual RF3 brokers. The
  physical safe-anchor retention logic is now implemented but still needs a
  full retained-bytes accounting and concurrent publication failure matrix.
- Enforce rebootstrap-required before local replicas behind committed safe
  state become leadership eligible, with the existing controller/P3
  fencing boundaries intact.
- Run the full RF3 V2 snapshot-install interruption matrix (stage-only,
  intent-only, backup moved, published-but-not-retired, invalid stage) and
  recover real producer-bearing checkpoint authority through failover.
  Confirm P3 snapshot-authorized install and later live V1 suffix convergence
  under network reassignments. V2 is still disabled by default.
- Independently qualify the focused P9-G failure matrix, unchanged historical
  protocol/Storage goldens, complete serialized workspace, fmt, Clippy,
  documentation tests and diff checks. Only then check P9-130..151, produce
  the closure report, commit, repeat tests on exact HEAD and attach
  `P9-G exact-HEAD qualification: PASS`.

**No progress-sidecar value and no volatile candidate-safe offset is
replicated authority.** No latest tombstone may be physically removed until
the full committed eligibility + grace + snapshot + safe-offset gate is in
place and independently tested.

## 2026-10-14 authenticated RF3 ASTRBTP2 transfer proof — NOT CLOSURE

Extended two independent P9-G replication test fixtures to use the actual
three-peer TCP transport and its incarnation-authenticated inbound headers,
not a direct in-process delivery of an already decoded package:

1. `p9g_v2_install_snapshot_is_p3_authorized_and_followed_by_normal_v1_suffix`:
   canonical `ASTRBTP2` bytes were transmitted from broker-id 0 to broker-id
   1 in an RF3 connection set; the receiver verified source incarnation,
   partition identity and byte codec, then the P3 `InstallSnapshotRequest`
   accepted the V2 generation, rejected the stale repeated installation,
   and accepted/read/recovered an ordinary V1 suffix. **PASS 1/1.**
2. `p9g_v2_rebootstrap_installs_sparse_manifest_under_p3_and_catches_up_live_suffix`:
   source retained manifest includes actual V2 sparse segment bytes whose
   frame at Raft index 1 is physically absent and Raft index 2 is retained;
   it was transmitted as `ASTRBTP2` through authenticated RF3 peer channels,
   canonically decoded, then independently validated/staged under the
   authorized P3 snapshot. Existing test assertions cover rejected mismatch,
   corrupt Storage V2, abandoned staging, intent+backup rollback, completed
   publication, subsequent V1 suffix and restart. **PASS 1/1.**

These prove actual network transport plus P3/Storage integration and sparse
history across a three-voter peer configuration. They are NOT an end-to-end
P5-controller-driven automatic snapshot selection/InstallSnapshot of a
behind-safe eligible voter. The controller-managed RF3 compaction/rejoin test
continues to demonstrate legal incremental P3 catch-up and cannot substitute
for that separate closure gate. Exact-HEAD authority remains P9-F.

### Full serialized workspace run on this uncommitted P9-G tree

`cargo test -q --workspace -- --test-threads=1` completed **PASS (exit 0)**
after the two V2 RF3-network test extensions. All reported Rust test groups
passed with zero failures, including the serialized controller/broker RF3
integrations, Replication (122/122) and Storage (88/88). This supersedes the
earlier **pre-cache** interrupted full-workspace run recorded above. The
successful command is still a **pre-commit** run and does not establish the
required independently tested **exact committed HEAD** authority.

**Explicit non-closure:** The P5 controller-managed rejoining voter in the
separate dynamic-broker integration recovered via valid incremental P3
AppendEntries. Even though actual sparse-history ASTRBTP2 now crossed the
authenticated three-peer TCP transport and was installed under P3 effect
authorization in focused tests, a real controller-fenced automatic RF3
InstallSnapshot against a behind-safe eligible voter (and its complete
member-exclusion, interruption, retained-Fetch, producer, and tombstone
matrix) is still outstanding. No P9-G checkbox, commit or note is permitted.

## 2026-11-01 continuation: RF3 all-voter safe evidence and failover blocker — NOT CLOSURE

This continuation targets the existing P9-G implementation only; P9-H/I and P10–P12 remain off. No checklist item, phase commit, exact-HEAD PASS note or rollout activation is authorized.

- Added deterministic P3/RF3 negative regression `p9g_rf3_missing_voter_progress_cannot_commit_safe_offset_across_failover` in `crates/astralog-replication/src/simulation.rs`. A quorum producer write and later majority leadership change do not transform 2/3 clean reports into all-voter evidence; previous-term tracker is refused. **Targeted PASS 1/1; replication library PASS 124/124**, formatting and diff hygiene pass.
- Strengthened existing live controller-managed RF3 integration `p9g_rf3_sideband_and_v2_opt_in_survive_broker_leader_failover` to attempt third-voter independently led COW, inspect each manifest-bound clean sidecar, require the exact all-voter minimum in real durable committed V1/V2 safe controls on all three, and recheck after failover. **Original extension FAILED twice** when broker-1 had authoritative manifest generation 3 but stale generation-2 sidecar (conservative read zero). This is a legitimate generation fence; test was not relaxed.
- Added a P9-G opt-in-only bounded refresh path: when the current manifest/snapshot pair has no verified local clean progress, `ReplicatedPartition::revalidate_local_clean_progress_for_current_generation` invokes the existing whole-prefix verifier at most once per pair in the broker actor before reporting. No stale sidecar is promoted without fresh source-file proof. New methods expose the persisted snapshot-floor key and revalidation; historic/P6 paths do not opt in to this *new* refresh.
- The stricter live RF3 case then got past all-voter current-generation evidence and the durable safe-minimum observation, **but FAILED at a later leader shutdown/failover**: Metadata continued to report `UNKNOWN_BROKER_ID` for partition leader (term 4), and quorum Produce could not recover within its bounded deadline (`p5_dynamic.rs:360`). Retained crash fixture diagnostics showed one surviving broker's authoritative manifest at generation **4** while its `clean.progress.v1` sidecar remained bound to generation **3** (offset 3). Its conservative local clean bound is therefore zero, making a positive committed safe bound block campaigning. This is a **real open availability/liveness failure** under the safety gate; it must not be bypassed by trusting the stale sidecar, disabling behind-safe election fencing, or weakening P3 authority.
- The earlier P6 silent-member intermittent readiness/ambiguous-response failure has no proven P6 safety regression. Independent source review identified a plausible P9-G legacy-path I/O pressure cause: `ReplicatedPartition::tick` reads committed safe controls from the whole segment inventory when cache is invalidated by ordinary Raft commits. A correct remedy must cache exact authority across ordinary commits with proper durable-control application and open/rebootstrap corruption validation; do not skip safe checks purely because current opt-in is disabled. This remains an **unproven performance/liveness hypothesis**, not a validated source repair.

**Mandatory next acceptance work:** independently explain and correct the manifest-generation / persisted snapshot-floor interaction after replicated safe-offset advancement, so every eligible surviving voter can re-establish verified clean history without stale proof and legal P3 elections resume after failover. Then re-run the strict live RF3 test, add tombstone removal/no-resurrection, producer-bearing V2 interruption/joint-voter matrices, and requalify P6 and the complete serialized workspace. No P9-G closure claim exists.
## 2026-11-01 follow-up: bounded safe-prefix revalidation restores RF3 elections — PRE-COMMIT, NOT CLOSURE

The previously recorded live RF3 failure and its crash fixture were reproduced and analyzed at the level of current generation and actual segment frames. The two surviving replicas could have `clean.progress.v1` for generation N while a normal roll published manifest generation N+1. Their committed P9-G safe control was valid, but the verifier refused all progress due to (a) committed control-only frames with Raft index above the earlier snapshot floor, (b) a newer same-key append outside the previously certified stream prefix, and (c) a non-quorum-committed suffix frame rolled into an immutable V1 file outside that prefix. These events cannot confer new authority, but must not revoke a previously *independently provable* exclusive clean prefix.

Bounded fix in `crates/astralog-storage/src/partition/compaction.rs`:

- Compute clean evidence from the authoritative, fully validated current manifest and finite `CompactionLimits` only. The history's latest-key index is restricted to records with stream offset **strictly below the tested exclusive bound**, so a later append cannot retroactively turn a previous certified value into an unclean one.
- Prove the current active-segment prefix and, if needed, separately re-derive the *existing quorum-committed* safe prefix from the same current inventory. Never infer a proof from the stale local sidecar or a volatile candidate.
- A closed V1 maintenance/config control (which carries no stream data) may lie above the old P3 snapshot floor. It cannot invalidate an earlier data-only clean prefix. Likewise, an uncommitted closed V1 data batch with **base offset >= the tested bound**, or an uncommitted control, cannot confer progress or invalidate data below the bound. Uncommitted data that intersects the tested bound remains an explicit fail-closed zero. V2 history above snapshot/commit remains conservatively rejected.
- The broker's P9-G-only revalidation still requires explicit homogeneous opt-in and runs at most once per `(manifest generation, persisted P3 snapshot floor)` attempt; failed proof leaves the election fence in place. P3/P5 authority and committed safe-offset monotonicity are unchanged.

New focused regression: `p9g_proven_safe_prefix_survives_control_roll_and_uncommitted_suffix` verifies a committed safe control, later V1 rolls, uncommitted out-of-prefix newer keyed data, exact current-generation revalidation/sidecar replacement, and restart reconstruction. **PASS 1/1.** The earlier negative `p9g_rf3_missing_voter_progress_cannot_commit_safe_offset_across_failover` remains **PASS**. Existing focused P9-G Storage suite **PASS 10/10 before the new regression** and new standalone regression PASS 1/1; final serialized workspace includes Storage **89/89**.

The previously failing strict live controller-managed RF3 test `p9g_rf3_sideband_and_v2_opt_in_survive_broker_leader_failover` now **PASSES 1/1** both as a targeted run and inside the complete serialized workspace. It independently checks all-voter manifest-bound sidecars, the minimum replicated safe control on all three durable inventories, broker incarnation-rotated V2 transfer, leader shutdown, successor election, continued quorum write, and durable safe recovery. This test is *not* the separate physical tombstone-no-resurrection matrix or interrupted V2 publication campaign.

Current **pre-commit complete serialized workspace**:

- `CARGO_BUILD_JOBS=1; CARGO_INCREMENTAL=0; cargo test -q --offline --locked --workspace -- --test-threads=1` — **PASS 718/718 (exit 0)**; includes P6 routing **9/9**, P5 dynamic **9/9**, P7 failover **12/12**, Raft **128/128**, Replication **124/124**, Storage **89/89**, and P2/P6/P7/P9 frozen wire goldens.
- `cargo test -q --offline --locked --workspace --doc` — **PASS** (zero runnable doctests across workspace).
- `cargo clippy -q --offline --locked --workspace --all-targets --all-features -- -D warnings` — **PASS**.
- `cargo fmt --all -- --check` — **PASS**; `git diff --check` — **PASS**.

**Qualification scope remains strictly limited:** This is positive pre-commit evidence for one P9-G liveness gate, *not* P9-G closure or an exact clean committed HEAD. The historical intermittent P6 readiness issue passed this serialized run, but one pass does not establish that the underlying intermittent scheduling sensitivity is permanently fixed. Outstanding P9-G gates include full RF3 physical tombstone-grace/snapshot/no-resurrection after failover, joint-P3/P5 membership and missing/offline voter exclusion, hostile and interrupted producer-bearing V2 rebootstrap/restart, negative/positive corruption and publication matrix, and independent exact-HEAD qualification. No P9-G checklist items were checked and no commit, push, feature activation, P9-H/I or P10–P12 development occurred.
## 2026-11-01 next gate: live RF3 physical tombstone nonresurrection and bounded recovery

**Scope:** Continued from the separately passing RF3 all-voter safe-offset/failover baseline. No P9-G closure, release activation, commits, staging, push or next-phase work. All local source edits retain the existing uncommitted P9-G implementation and preserve earlier P9-F exact-HEAD authority.

- Added **`p9g_rf3_tombstone_removal_after_safe_commit_cannot_resurrect_on_failover`** in `crates/astralog-broker/tests/p5_dynamic.rs`, reusing the same real three-controller/three-broker opt-in RF3 harness as the previous `p9g_rf3_sideband_and_v2_opt_in_survive_broker_leader_failover`. The original test remains separate and passes. The new test uses real Quorum-acknowledged old keyed data, a null-value keyed tombstone at stream offset 2, a large independent append to close the tombstone-bearing source, controller-authoritative `CompactDelete` with **positive 1ms grace** (test fixture, not production default), real authenticated P3/P5 V2 rebootstrap and reincarnation, independently current-manifest-bound clean progress from three voters, and an exact durable committed safe minimum before attempting physical deletion. The marker is verified to remain on disk after an earlier pre-safe COW when safe was still zero. The positive post-safe COW must replace an immutable source and the current manifest must have **neither tombstone nor any older same-key value**. The committed safe anchor must remain durable after COW.
- After physically removing the marker, kill the cleaner leader, observe a successful successor election and new unrelated quorum write, and check committed disk inventory **and** public read-committed Fetch for resurrection. Then rejoin the tombstone-free cleaner under a new committed P5 incarnation, require P3 committed catch-up without reintroducing the discarded marker/source, transfer leadership back to **that exact physically cleaned broker**, and recheck read-committed Fetch plus manifest inventory. These steps passed in focused and serialized integration runs. This is a genuine physical-cleaner failover/rejoin/re-election proof, not only a storage model.
- Intermediate repeated runs exposed (1) a **test-harness race** when `request_preferred_leader` was invoked on a broker that was no longer the P3 leader after V2 rejoin; fixed the test to discover the current local P3 leader and use the normal controller-authorized `request_preferred_leader` contract with bounded retries, never fabricating election state or retrying ambiguous producer writes; and (2) a **real P9-G liveness failure** when a follower rolled to manifest generation 4 but `clean.progress.v1` remained bound to generation 3 after physical removal, leaving only one eligible survivor and no election quorum. Failed intermediate repeats were preserved as **RED** evidence, not counted as passes.
- Corrected the previous one-shot failure-starvation risk: `ReplicatedPartition::tick` and the P9-G opt-in broker actor may retry a failed fresh current-generation clean-prefix verification at most every **51 ticks** for the same manifest / safe-offset or manifest / P3 snapshot pair. A current-generation proof remains mandatory; stale sidecars are never trusted, and the non-electable behind-safe gate remains enforced if revalidation cannot succeed. An existing RF1 runtime gate test now checks the 50-tick cooldown, attempted retry at tick 51, and that repeated unavailable proof **cannot** make a behind-safe replica electable. This does not enable cleaner scheduling, bypass controller member fencing, or alter P6's ambiguous mutation semantics.

**Current verified pre-commit commands and observations:**

- Both live `p9g_rf3` broker integration tests together **PASS 2/2** in two successive separate focused runs following the bounded retry change (and also **PASS 2/2** within the complete serialized workspace). Earlier individual passes and RED intermediate repetitions are retained above and must not be misrepresented as deterministic release qualification.
- P9-G storage **PASS 11/11**; targeted RF1 behind-safe/retry and RF3 missing-voter regressions each **PASS 1/1** (run without a misleading `--exact` filter; the first probe with `--exact` selected zero tests, and is explicitly not evidence).
- `CARGO_BUILD_JOBS=1; CARGO_INCREMENTAL=0; cargo test -q --offline --locked --workspace -- --test-threads=1` **PASS 719/719, exit 0**, including P5 dynamic **10/10**, historically intermittent P6 routing **9/9**, P7 failover **12/12**, Raft **128/128**, Replication **124/124**, Storage **89/89** and existing protocol/golden vectors.
- `cargo test -q --offline --locked --workspace --doc` **PASS** (no runnable doctests); `cargo clippy -q --offline --locked --workspace --all-targets --all-features -- -D warnings` **PASS**; `cargo fmt --all -- --check` **PASS**; `git diff --check` **PASS**. No stage, commit, push, merge or feature rollout performed; authoritative HEAD remains `cd064a4fe60141590b6062957475aa15ee560717` (P9-F).

**Still OPEN for P9-G:** dedicated long-grace/negative-time/future-clock cross-replica oracle and offline voter excluded/not excluded under controller identity fencing, P3/P5 joint-membership and reassignments, interrupted producer-bearing V2 rebootstrap and crash publication, corruption and exact manifest/tail failures, repeated/adversarial liveness qualification, independent source audit, and new exact-HEAD qualification before P9-H/I. A single full serialized workspace run and finite focused repeats do not close these gates.
## 2026-11-01 next P9-G gate: offline P5 voter, incarnation rotation and joint-set minimum

**Status: pre-commit targeted acceptance evidence, P9-G OPEN.** The authoritative P9-F exact-HEAD Git note on `cd064a4fe60141590b6062957475aa15ee560717` remains unchanged. These edits continue the existing uncommitted P9-G work; no checklist box was marked, no commit/stage/push/merge/rollout or work from later phases.

**Bug addressed — identity rotation under unchanged P3 voter IDs and P5 assignment generation.** `CleanProgressTracker::is_current_for` compared only topic, partition, Raft term, P5 assignment generation and current effective voter *set*, so it could consider old volatile reports current after a broker RejoinBroker incarnation rotation that retained the same NodeId/assignment. Added `is_current_for_voter_identities` to require the exact broker ID + committed incarnation of **each effective P3 voter** before the actor retains the tracker. The broker's actor now calls this strict check, requiring fresh reports under the new identity; stale reports may not renew another generation's safe minimum. The Raft runtime's separate `is_current_for` continues to verify term/assignment/effective voter set against the current core; the broker supplies independent P5 incarnation authority.

**Joint-membership identity pool:** The controller can have committed an in-flight P5 `BeginMovement` with stable old assignment while the partition's durable effective P3 configuration has entered joint consensus. The old topology exposed identities only for `assignment.voters`, causing the broker to reset its tracker for any P3 joint union including the new destination and preventing a lawful candidate. `build_dynamic_topology` now uses the committed old-voter assignment **plus** target voters named in the committed active P5 movement as the authoritative identity *pool*. `observe_p9_clean_progress_on_tick` verifies every actually effective P3 voter exists in that pool; `CleanProgressTracker::from_committed_assignment` selects only the actual P3 effective voter set, never the entire potential pool. A learner/movement destination is not eligible until P3 commits its promotion. Missing or unknown committed voter identities fail closed. The tracker resets across term, assignment, actual P3 voter set or observed broker-incarnation changes; no report or old generation is inherited.

**New adversarial tests:**

- `p9g_unchanged_voters_and_assignment_must_revoke_old_incarnation_reports` in `crates/astralog-replication/src/compaction_progress.rs`: all three old identities have positive reports; same P3 voters/term/assignment but one newly committed broker incarnation immediately makes the old tracker stale; new tracker starts with no reports, and only freshly authenticated new incarnation can complete the candidate. PASS.
- `p9g_committed_movement_identity_pool_covers_joint_union_and_post_exclusion` in `crates/astralog-broker/src/replication.rs`: four real committed controller broker records, a P5 BeginMovement from old voters `[1,2,3]` to new `[2,3,4]`, explicit committed progress through JointCommitted, and a P3 joint EffectiveConfiguration. Requires *four* reports even though both Raft majorities have three; stale/forged destination incarnation is rejected. Cannot reuse joint evidence after P3 enters stable. Advances through controller's full movement workflow, commits `FinalizeMovement` with generation 2, verifies removed voter 1 drops from eligible pool only then, and refuses both removed-voter and previous-assignment reports until every current new voter supplies fresh proof. PASS.
- Extended `p9g_rf3_tombstone_removal_after_safe_commit_cannot_resurrect_on_failover`: a real controller-managed voter is shut down after pre-safe COW; a 350ms hold exceeds the bounded sideband report lifetime (10 ticks at 20ms). The surviving P3 majority may continue quorum traffic but cannot commit safe>0 or physically remove the latest tombstone while an eligible P5 voter remains unverified. Existing independent V2 rebootstrap, all-voter positive safe proof, physical deletion, P3 leader failover, P5 rejoin, return election to physically tombstone-free broker, and client no-resurrection Fetch oracles remain intact. PASS.

**Fresh qualification commands:** `cargo test -q --offline --locked -p astralog-replication --lib p9g_ -- --test-threads=1`: PASS 17/17; `cargo test -q --offline --locked -p astralog-broker --lib p9g_ -- --test-threads=1`: PASS 3/3 (before the final extended exclusion assertion, which passed as a standalone 1/1); live `cargo test -q --offline --locked -p astralog-broker --test p5_dynamic p9g_rf3 -- --test-threads=1`: PASS 2/2. Full `CARGO_BUILD_JOBS=1; CARGO_INCREMENTAL=0; cargo test -q --offline --locked --workspace -- --test-threads=1`: **PASS 721/721**, exit 0. Historical P6 routing 9/9, dynamic P5 10/10, P7 failover 12/12; Replication lib 125/125, Storage lib 89/89, Raft lib 128/128. `cargo test -q --offline --locked --workspace --doc`: PASS (zero runnable doctests). `cargo clippy -q --offline --locked --workspace --all-targets --all-features -- -D warnings`: PASS. `cargo fmt --all -- --check`: PASS. `git diff --check`: PASS.

**Open before P9-G closure:** This test matrix verifies tracker/controller membership and a genuine live RF3 offline-voter tombstone oracle, but it does **not yet** execute a real *four-broker* P3 joint membership transition with simultaneous physical tombstone reclaim: the joint case constructs the effective joint configuration from the controller's committed movement intent and exercises the actual tracker. Still required: live joint topology / report sideband / failover qualification, negative/future-clock grace matrix, interrupted and hostile producer-bearing V2 install/crash publication / corruption or torn tails, repeat liveness/fault campaigns, independent adverse review, then user-approved commit and exact-HEAD qualification. P9-H/I and P10-P12 remain pending.
## 2026-11-01 action-audited P9-G continuation: zero-grace and pre-finalize P5 voter fences

**Pre-commit scope only; P9-G is ACTIVE and OPEN.** User requests continuation toward the entire numbered P0-P12 specification and explicit contradiction monitoring. `docs/ASTRALOG_GOAL_EXECUTION_GUARDS.md` records action-level scope, concurrency, safety, command and release fences. No stage, commit, push, merge, deployment, paid inference or P9-H/I/P10/P11/P12 pull-forward occurred.

1. **P9-G conservative grace (P9-141, P9-143):** Added `p9g_real_segment_clock_rollback_and_future_successor_never_establish_grace` to `crates/astralog-storage/src/partition/compaction.rs`. It uses actual on-disk segment creation/roll times, exact boundary `now=close+grace`, one millisecond short, a future successor, a reversed-clock successor, an enormous grace, timestamp subtraction overflow and absent committed safe authority. All fail toward retaining the marker. Audit also found that ControllerRetentionSpecV1 and public TopicRetentionConfigV1 reject zero grace but the lower-level `source_tombstone_safe_bound` previously accepted zero. The explicit zero-grace negative assertion initially **FAILED as intended**: targeted cargo test exit 101, left `Some(StreamOffset(2))` rather than `None`. Added a two-line fail-closed guard at the ultimate storage gate; reran targeted test **PASS 1/1** and full P9-G storage filter **PASS 12/12**, both exit 0. No ordinary positive-grace semantics or earlier V1 formats changed. First attempt with an `--exact` substring filter selected 0 tests; that is expressly **not** counted as evidence.
2. **P5 pre-finalization exclusion fence (P9-136 through P9-138):** During an in-progress movement old P5 assignment [1,2,3] -> target [2,3,4], P3 can finish LeaveJoint and have effective stable voters [2,3,4] before Controller `FinalizeMovement` commits the replacement assignment generation. Previously the P9-G broker tracker could use the new P3 set alone and prematurely exclude old P5 voter 1 even while P5 still committed it. Added exact `committed_p5_voters` to opt-in `P9ProgressAuthority` and require that the current effective P3 voter set **contains all still-committed P5 voters**. Joint P3 union [1,2,3,4] remains eligible for all-four proof; new stable P3 during old P5 assignment is **blocked**; after committed P5 generation 2 finalization, new stable [2,3,4] may report. Added regression with a complete premature three-new-voter candidate demonstrating it **must not** bypass committed old P5 authority. Existing joint+finalization controller-backed test now **PASS 1/1** under this stronger fence. This is a conservative temporary hold, not weakened Raft quorum or a shortcut to authority.

**Further current-turn work:** A worker owns live four-broker P3/P5 joint transition tests in broker p5_dynamic only, and a separate worker owns producer-bearing V2 crash/restart/corruption tests in replication source only; final reports and full combined workspace requalification remain **PENDING**, not implicitly green. Independent exact-HEAD closure remains blocked. Current main authoritative HEAD is the P9-F PASS commit `cd064a4fe60141590b6062957475aa15ee560717`, not a P9-G closure commit.
## 2026-11-01 P9-G producer-bearing V2 crash-state and corruption matrix

**Scope: P9-146..150 focused pre-commit evidence only; not P9-G closure.** Worker-5 changed ONLY replication test fixtures/helpers in `crates/astralog-replication/src/rebootstrap_v2.rs` and `crates/astralog-replication/src/storage_adapter.rs`; no production V2 decode/install implementation edits.

- Codec uses an actual bounded `ProducerSnapshotV1` with producer epoch, sequence, retry digest and exact offsets; V2 roundtrip checks strict checkpoint size, topic/partition identity, and P3 covered-snapshot boundary. Hostile nested manifest CRC, nested producer checkpoint CRC, outer CRC rewrite, and forged producer-identity with both inner and outer CRCs recalculated must still fail decode.
- Crash-state matrix derives its V2 generation from a **real successful P3-authorized retained-history install** seeded by a storage partition with committed P9-F producer checkpoint and physically reclaimed producer-bearing V1 source. Tested filesystem states: staged only -> old generation retained; commit-intent+backup but no new publish -> old recovered; published valid V2 with corrupted derived producer.snapshot -> new retained checkpoint authority and exact recovered producer state; published new with corrupted authoritative manifest -> old restored; published new with corrupted immutable retained segment -> old restored. These are read/write test-fixture directories only, not user workspace cleanup.
- `CARGO_BUILD_JOBS=1 CARGO_INCREMENTAL=0 cargo test -p astralog-replication --offline --locked p9g_v2_ -- --nocapture`: **PASS 8/8**. `cargo fmt --package astralog-replication -- --check` PASS; focused diff check PASS. First iteration failed due test-only assumption that COW must emit a Storage-format-V2 immutable file; the invariant being qualified is the actual encoded ASTRBTP2+retained checkpoint, not that unrequired storage encoding. The erroneous assertion was removed without weakening the real package/rollback or producer authority oracles, and final matrix passed.
- **Explicit remaining limitation:** Crash points are deterministic reconstructions of filesystem states, **not** actual process-kill, torn fsync/rename, disk-pressure or multi-host chaos injection. Production package builder intentionally exports `producer_checkpoint: None`; the optional acceleration snapshot receiver branch was exercised by attaching a canonical snapshot, while the retained committed P9-F control remained the independently authoritative producer state. This is not an end-to-end joint controller-qualified replay of producer retries across kill/restart. Preserve P9-G OPEN until those missing gates and full combined serialized workspace/independent review pass.
## 2026-11-01 independent contradiction audit: pre-epoch broker clock fails closed

**P9-141 defense-in-depth correction, pre-commit.** An independent READ-ONLY reviewer challenged >16 P3/P5 identity and Storage grace boundaries. No observed bypass of the new committed-P5-voter subset fence or the Storage zero-grace guard. The reviewer found one genuine broker-to-Storage integration hole: `crates/astralog-broker/src/state.rs::now_ms` intentionally falls back to `i64::MAX` when system time is before the UNIX epoch or unrepresentable, and the prior P9-G `run_keyed_compaction_once` used this fallback as the `TombstoneRemovalGate.now_ms`. Under a real backward/pre-epoch clock failure, committed safe >0 and a closed tombstone segment could appear arbitrarily aged, contradicting conservative grace. The lower storage helper itself only checked elapsed from the supplied clock and could not detect that the broker invented MAX.

Smallest safe repair: `run_keyed_compaction_once` now uses new `p9g_tombstone_gate_from_clock(SystemTime, grace_ms)->Option<TombstoneRemovalGate>` for CompactDelete only. Invalid/pre-epoch, unrepresentable, or nonpositive-grace inputs yield None, allowing ordinary keyed value compaction while **retaining tombstones**; valid clock values pass their true milliseconds to the existing Storage gate. The legacy `now_ms()` helper and every P6/P7/other timestamp path remain unchanged. Deterministic test injects a pre-epoch clock and normal epoch/positive-grace and zero-grace values, WITHOUT ever adjusting the host clock. `cargo test -q --offline --locked -p astralog-broker --lib p9g_tombstone_clock_gate_fails_closed_before_epoch_and_for_zero_grace -- --test-threads=1`: **PASS 1/1**; broker `cargo test -q --offline --locked -p astralog-broker --lib p9g_ -- --test-threads=1`: **PASS 4/4**. Rustfmt targeted to state.rs only. The independent audit also flagged an unresolved *forward* clock jump (grace may appear elapsed early) as a contract specification/fault-injection gap, not an established implementation defect, and a safe liveness stall if controller finalization never completes. Neither can be marked qualified by these tests alone.

**Release status unchanged:** HEAD remains P9-F `cd064a4`, P9-G remains ACTIVE, worktree uncommitted, no stage/commit/push/feature rollout, combined full workspace and live RF4 pass still PENDING.

## 2026-11-01 live RF4 joint-movement positive-phase qualification — PRE-COMMIT, NOT CLOSURE

**Latest focused result:** the existing true four-broker / three-controller
`p9g_joint_rf4_new_voter_incarnation_and_stable_safe_minimum` integration in
`crates/astralog-broker/tests/p5_dynamic.rs` completed **PASS 1/1**, zero failed,
ten filtered, in **225.47 seconds** using offline locked Cargo,
`CARGO_BUILD_JOBS=1`, `CARGO_INCREMENTAL=0`, and one test thread. The corrected
test source was frozen after this run; targeted Rustfmt check and
`git diff --check` passed. This is one successful focused run, **not** repeated
fault-campaign qualification or full P9-G closure.

The prior observed `StaleEpoch` in the positive stable-new COW arose in a test
harness that checked finalized broker IDs but had not proved broker-local
metadata was current, and captured a target's leader epoch without proving
that target was itself currently leader. The minimal **test-only** repair now
requires a stable controller metadata epoch across the finalized `[2,3,4]`
broker-local observations, checks target-local P3 leader identity/epoch after
every preferred transfer, and reacquires broker 4 leadership before the
post-safe physical tombstone-removal attempt. Production Raft quorum, P5
assignment, metadata epoch, tombstone-grace and COW publication fences were
**not** weakened. An independent source review found `StaleEpoch` exits before
COW publication in this path; earlier separately committed producer checkpoint
or retention snapshot steps do not imply that a COW occurred.

**Remaining P9-G proof obligations:** the live test does not directly inspect
the committed P3 `EffectiveConfiguration::Joint` union. Its current
controller-`JointCommitted` negative step stops the **new-only broker 4**, not
the old-only broker 1; it therefore does **not** prove the required absent
old-only-voter case while P3 has actually committed the joint four-voter
union. This exact independent live oracle remains unqualified; one focused
pass does not eliminate
intermittency. P9-150 real process-kill V2 installation is also absent: the
existing producer-bearing crash-state tests reconstruct rather than interrupt
the installer. A read-only audit identified three practical crash barriers
(after publication intent, after old-to-backup rename, after new publication)
for a future bounded external child-process kill/recovery test. Windows
directory sync is a no-op, so a child kill would show process-death recovery,
**not** power-loss or torn-fsync durability. Neither missing test is counted
as completed merely because a design has been reviewed.

### Current complete-workspace attempt: RED — P9-G NOT QUALIFIED

After the single focused RF4 PASS, prime ran
`CARGO_BUILD_JOBS=1; CARGO_INCREMENTAL=0; cargo test -q --offline --locked --workspace -- --test-threads=1`
on the frozen combined P9-G source. **Exit 101 / FAIL.** Earlier test groups
passed, but `crates/astralog-broker/tests/p5_dynamic.rs` finished **10 passed,
1 failed** in **492.78 seconds**. The failing test was
`p9g_joint_rf4_new_voter_incarnation_and_stable_safe_minimum` at
`p5_dynamic.rs:4636:18`: `stable target voter COW must be accepted: NotLeader`.
This is a *different* RED from the previously observed `StaleEpoch` and does
not invalidate the separate focused PASS; the latter simply does not prove
reliability under serialized workspace pressure. A concrete harness race is
possible because target-local leadership was confirmed **before** a bounded
up-to-1500ms revalidation wait, then an earlier leader epoch was used for COW.
This diagnosis is **not yet proven** and must not be used to weaken P3 leader
checks or excuse an actual liveness defect.

Worker-4 was re-engaged for exclusive source-scoped inspection and the smallest
safe, freshly leader-fenced test correction. No full-workspace success, strict
post-patch Clippy/doc qualification or exact-HEAD gate can be claimed from
this run. No checklist closure, staging, commit, push, rollout or phase
advancement is authorized.

**Subsequent focused attempt after the minimum NotLeader harness repair was
also RED.** The source retains the initial preferred transfer for actor clean
revalidation, then reacquires target-local P3 leadership and fresh epoch
immediately **after** the up-to-1500ms wait and before its single COW call.
No retry of an ambiguous mutation was added. The exact focused RF4 test ran
offline/locked with one test thread and exited **101: 0/1 PASS, 10 filtered,
72.81 seconds**. It failed **earlier** at `p5_dynamic.rs:4540`, because the
controller movement remained `MovementStage::JointCommitted` through the
30-second completion deadline after destination broker 4 incarnation-2
rejoin/fresh progress. It **never reached the modified COW call**, so this
result neither validates nor refutes the original `NotLeader` repair. Do
not convert this stage stall into a pass by assuming controller completion
or arbitrarily weakening the deadline. P5 reconciliation and actual committed
P3 role/configuration need independent liveness diagnosis first.

### Independent exact-joint qualification gap (read-only source audit)

The committed broker replication actor already exposes a
`MembershipObservation` including `effective_configuration`,
`pending_configuration_index`, leader/term information and durable commit
index (`astralog-replication/src/runtime.rs`). Broker internal
`ReplicationClient::membership_status` reaches that observation, but the
existing integration-facing `BrokerHandle::observe_partition_internal`
does not expose it. A minimal future **internal diagnostic** extension in
broker `state.rs` / `server.rs` (similar to its existing hidden observation
method) could expose the already-computed P3 observation without introducing
any new network protocol, Raft mutation or public request contract. Do **not**
use library `#[cfg(test)]` alone for integration tests, where the crate is
normally compiled as a dependency.

The required real joint oracle must prove the **committed**
`EffectiveConfiguration::Joint` old `[1,2,3]`, new `[2,3,4]`, effective union
`[1,2,3,4]`, and pending config index at/below durable commit. A controller
`MovementStage::JointCommitted` observation alone is insufficient. Transfer
leadership to a shared voter (2 or 3) before shutting down the old-only voter
1, re-observe that live shared leader's still-joint committed P3 membership,
and assert safe remains zero and the physical latest tombstone remains. If P3
has already reached stable new, do **not** count the run as joint proof.

The separate all-other-voters-positive/old-only-report-expiry oracle also
remains open: the test's existing 150ms hold does not establish expiration
past the ten-tick TTL at 20ms/tick, and controller reconciliation may finish
LeaveJoint during a longer hold. This requires a controlled live boundary,
not a manufactured progress candidate or an unbounded pause. This section
is an **implementation plan from independent source review**, not a passing
test or authorization to advance the P9-G checklist.

### Follow-up: bounded failure-only JointCommitted liveness diagnostics

The independent P5-stall reviewer could not reconnect, so prime inspected
the existing production paths without changing their behavior. In
`crates/astralog-broker/src/controller.rs`, each dynamic broker control tick
does one metadata refresh, heartbeat and movement reconciliation; at
`JointCommitted`, `reconcile_leave_joint` requires local committed P3 Joint
membership and an actual `Role::Leader` before requesting LeaveJoint.
The RF4 fixture currently uses a 2-second metadata-refresh interval on
brokers 1/4 and 10 seconds on brokers 2/3. This suggests a scheduling/role
observation hypothesis, but **does not establish that the 30-second RED was
harmless host load rather than real P3/P5 liveness loss**.

Prime added a **failure-path-only** diagnostic to
`p9g_joint_rf4_new_voter_incarnation_and_stable_safe_minimum`: when the same
unmodified 30-second movement-completion deadline is reached, poll all four
brokers with at most 300ms each via the existing `observe_partition_internal`
read-only endpoint and include their leader/epoch/durable commit observations
plus the controller epoch in the failing assertion. The success path,
deadline, movement conditions, committed-voter gates and Raft authority are
unchanged. Source was formatted with edition-2024 rustfmt and `git diff
--check` succeeded before the focused run.

Exact focused command with `CARGO_BUILD_JOBS=1`, `CARGO_INCREMENTAL=0`:
`cargo test -q --offline --locked -p astralog-broker --test p5_dynamic
p9g_joint_rf4_new_voter_incarnation_and_stable_safe_minimum -- --exact
--test-threads=1 --nocapture` — **PASS 1/1, 10 filtered, 58.59 seconds, exit 0**.
The diagnostic did not fire because the movement completed in this run.
The earlier 492.78s full-workspace `NotLeader` RED and 72.81s focused
`JointCommitted` RED **remain evidence of intermittency**, not erased by
this single green repeat. A new combined serialized workspace regression
is in progress; its outcome is not presumed here. P9-G remains OPEN.

### Current P6 backward-compatibility gate: explicit ambiguous RevokeAck RED and test-only authority oracle

A subsequent full serialized offline-locked workspace run completed the P5
dynamic integration group **11/11 PASS** (including the revised live RF4 test),
but the P6 routing group finished **8/9, RED exit 101**, with
`p6_churn_timeout_static_and_partition_expansion_are_authoritative` failing
at `crates/astralog-broker/tests/p6_routing.rs:1620`: the client's single
cooperative `RevokeAck` returned `AmbiguousMutation { api: RevokeAck }`.
This is a required **fail-safe result** for an uncertain mutation response,
not proof of a data-plane safety breach. The complete workspace therefore
**remains RED**, despite the earlier P5 focused/combined PASS. The same test
already provisions a 20s transport response timeout; blind mutation replay
or loosening P6 epoch/owner fences is prohibited.

Prime verified an existing historical P6 test in that same file already
resolves an ambiguous `JoinSubscribe` solely by re-reading exact committed
group history. A minimal **test-only** analogous `RevokeAck` branch was added
to the P6 long scenario, without changing production client/broker code:
after `AmbiguousMutation { api: RevokeAck }`, obtain the live authoritative
coordinator, recover committed `GroupRecordV1`, and accept **without sending
another mutation** only when epoch **exactly 4** is Stable, pending epoch and
revocations are empty, both expected members exist, there are exactly two
assigned partitions, the newly joined crash member owns each formerly
revoked partition, and the original survivor owns every unaffected one.
If that exact evidence cannot be produced under a bounded deadline, the
test fails. Unexpected errors still fail immediately. This does not relax
P6 `AmbiguousMutation` client behavior or any ownership fence.

`rustfmt --edition 2024` and `git diff --check` passed; the exact focused
offline-locked single-thread test
`p6_churn_timeout_static_and_partition_expansion_are_authoritative`
**PASS 1/1, 8 filtered, 125.44s, exit 0**. An independent read-only
adversarial review was requested; focused PASS does not establish that the
complete combined worktree is qualified, and no release authorization follows.

## 2026-11-01 P9-G V2 real child-process termination at publication barriers — NOT CLOSURE

**Scoped P9-146..150 pre-commit evidence.** Added a test-only external-kill
barrier and `p9g_v2_external_process_kill_preserves_committed_authority` in
`crates/astralog-replication/src/storage_adapter.rs`. The parent builds real
committed producer-bearing V2 retained history from the existing P9-F checkpoint
fixture and a distinct old committed partition, launches the same Rust test
binary as a child, then uses the operating system's `Child::kill` while the
actual P3-authorized V2 installer is stopped at one of three fsynced marker
barriers. The child cannot synthesize a passing crash state; a missing barrier,
unexpected child exit or timeout fails the parent. The pause hook is
`#[cfg(test)]` only and does not exist in production builds.

- After durable **publication intent** but before old-directory rename:
  restart must retain exact old commit index 1 and the original old data.
- After old-directory **backup rename** but before new-directory publication:
  restart must restore the exact old generation, not accept uncommitted stage.
- After the complete **new V2 generation is published** but before backup
  retirement: restart must accept P3 snapshot/commit index 3 and recover the
  exact original P7 producer window from committed retained history.
- Each recovered state has no leftover swap staging, backup or intent artifacts.

The initial focused attempt correctly failed **RED** because the new child
test fixture incorrectly reset an already committed old hard-state index 1 to
zero through `UpdateHardState`, which existing Raft persistence validation
rejected *before* the first crash barrier. Only the fixture was corrected to
retain commit index 1; no production Raft/Storage/P9 fence changed. The
actual three-stage parent-controlled kill test then **PASS 1/1**. Follow-up
`CARGO_BUILD_JOBS=1; CARGO_INCREMENTAL=0; cargo test -q --offline --locked -p
astralog-replication --lib p9g_v2_ -- --test-threads=1`: **PASS 9/9**;
`cargo fmt --package astralog-replication -- --check`, focused
`cargo clippy -q --offline --locked -p astralog-replication --all-targets
--all-features -- -D warnings`, and `git diff --check`: **PASS**.

**Hard limits:** Windows directory sync is a no-op; this is genuine
external *process termination*, **not** physical power loss, torn-fsync,
disk-full, multi-host crash, nor automatic controller-managed live V2 rejoin.
Full current-worktree qualification remains **RED** due separate live RF4
JointCommitted and RF3 post-tombstone-removal election failures reported
above. P9-G and its checklist stay OPEN; no staging, Git commit, push,
release, or P9-H/P10 implementation follows from this focused result.

## 2026-11-01 P9-G RF4 direct committed P3-joint observation — PRE-COMMIT

**Scope P9-136..138; strict live oracle, not phase closure.** The earlier
controller-`JointCommitted` milestone could be true even when the specific
broker observed by the harness had not yet durably installed the actual P3
Joint configuration. Added a read-only `observe_p9g_membership_internal`
path through `BrokerHandle`/`DynamicControlRuntime`/`DynamicBrokerControl` to
the existing `ReplicationClient::membership_status`; it is internal Rust
diagnostic access, not a new wire API, network mutation or quorum rule. The
RF4 integration now requires an actual locally committed
`EffectiveConfiguration::Joint` with old P5 voters [1,2,3] plus learner 4,
new voters [2,3,4], all four effective P3 voters and a pending joint index
at/below durable Raft commit. An uncommitted proposal or already-stable P3
cannot pass merely because the controller still says `JointCommitted`.

The first focused run **failed RED** in the new test assertion because the
new fixture mistakenly compared public broker IDs to actual P3 `NodeId`s.
The frozen P4 mapping is `NodeId(broker_id + 1)` (NodeId 1 reserved), proven
in broker replication's `broker_to_node`/`node_to_broker`; the observed
P3 old configuration had voters NodeId 2,3,4 and learner NodeId 5. Corrected
the *test-only expected identities* to that frozen mapping. No production
membership or P3/P5 behavior changed. The full live
`p9g_joint_rf4_new_voter_incarnation_and_stable_safe_minimum` then
**PASS 1/1, 10 filtered, 80.06 seconds**, offline locked single-threaded.

This is direct live committed joint proof plus the previously implemented
new-only-voter negative, incarnation rotation, post-finalization all-voter
safe minimum and physical tombstone/failover path. **Still missing:** an
independent true joint *old-only broker 1 absent while other reports are
positive and old report expires* oracle, repeated adverse campaigns,
reliable full-workspace P3/P5/RF3 liveness and exact-HEAD qualification.
Do not mark P9-G or P9-151 PASS on this focused test.

## 2026-11-01 full current-worktree P9-G liveness RED and non-idempotent test-helper audit

The serialized offline/locked full-workspace run started around 18:51 IST
against the new process-kill V2 and actual live P3-joint oracle completed
**RED exit 101** in the P5 dynamic group: **10/11 PASS, 1 FAIL**,
`p9g_rf3_sideband_and_v2_opt_in_survive_broker_leader_failover` at
`p5_dynamic.rs:362`. The post-V2-rebootstrap successor quorum suffix could
not be sent because refreshed client metadata continued to report
`leader_broker_id=UNKNOWN_BROKER_ID` at P3 epoch **4** and metadata epoch
**10**. This is a separate observed RF3 election/liveness failure under
serialized combined-worktree load, consistent with earlier RED observations
of unknown successor leadership after P9-G committed safe/tombstone COW.
An isolated PASS cannot erase this RED: **complete workspace remains OPEN**.

Read-only `astralog-client::AstraClient::produce` audit proved the existing
`quorum_produce_with_refresh` test helper in `p5_dynamic.rs` retried *every*
error for a P2 **non-idempotent** Produce. This could replay an ambiguously
committed write and manufacture a successful acceptance; it contradicts the
frozen P2/P7 ambiguous-write policy. Narrowed test-only retry to three
source-confirmed **pre-dispatch** metadata paths:
`topic not present in metadata` from `topic_metadata`, or
`partition leader is unknown` / `partition leader authority is unknown`
from `leader_for` / `leader_route`. Every other error now panics rather
than retrying an uncertain mutation. The initial stricter RF3 focused probe
failed because topic metadata was not yet propagated; static client source
confirmed that specific path is pre-dispatch, so it was explicitly added.
The corrected strict helper then passed the two real
`p9g_rf3` broker tests **2/2**, exit 0. This is *not* a resolution of the
intermittent full-workspace leader-election blocker.

Added a separate P9-G post-failover probe, using the same strict pre-send
retry classification and deadline, to collect on **failure only** each live
broker's real P3 `MembershipObservation`, manifest generation, conservative
current-manifest clean sidecar and actual committed safe-control inventory.
Its current focused compile/run result is **pending**; no probe output or
pass claim is inferred before the command completes. No production
client/Raft/controller/compaction behavior was changed by either test-only
diagnostic.

## 2026-11-01 19:23 IST recovered interrupted-run evidence — PRE-COMMIT, NOT CLOSURE

The earlier chat response stream was interrupted, but the retained terminal
session `80291` was re-read directly rather than inferring test success from a
chat summary. It launched at **19:13:36 IST** with
`CARGO_BUILD_JOBS=1`, `CARGO_INCREMENTAL=0`, and
`cargo test -q --offline --locked --workspace -- --test-threads=1` and
completed **exit 0, 728/728 passing tests**, counted across the 20 nonempty
workspace test groups. This includes the live P5 dynamic **11/11**, P6 routing
**9/9**, replication lib **129/129** (the three real external process-kill V2
barriers run in its child test binaries), Storage lib **90/90**, Raft lib
**128/128** and P7 failover **12/12**. No failing/ignored tests were reported.
This is an evidence-backed **single current-worktree run**, not proof that the
previously observed joint/RF3 liveness RED is no longer intermittent.

Separate `cargo test -q --offline --locked --workspace --doc` and
`cargo clippy -q --offline --locked --workspace --all-targets --all-features
-- -D warnings` both passed. The first follow-up `cargo fmt --all -- --check`
identified only formatting in the P5 sustained-write test's fail-safe match
arm; a later retained terminal session `21493` ran file-scoped edition-2024
rustfmt, strict Clippy, whole-workspace formatting check and `git diff --check`
with **exit 0**. The sustained-write test never repeats an uncertain payload:
any ambiguous response abandons that key and counts only separately
quorum-acknowledged writes. No production safety fence or public protocol was
changed to obtain these results.

**Unclosed:** live joint old-only-voter disappearance with complete positive
reports from all remaining effective P3 voters and expiry of old identity;
repeatable RF3/RF4 liveness under fault/host load; broader real interruption,
disk-pressure and clock-fault matrix; an independently checked final P9-G
candidate; and user-authorized closure commit followed by exact-HEAD gates.
P9-G/P9-151 are not qualified; P9-H/I and P10–P12 remain pending. No staging,
Git commit, push, release or rollout was performed.

## 2026-11-01 19:37 IST additional missing P9-G fault proofs — PRE-COMMIT

**P9-136..138 live RF4 old-only-voter absence:** Added a second independent
live joint-membership test
`p9g_joint_rf4_old_only_voter_absent_until_committed_exclusion`, sharing the
existing controller-managed RF4 source/positive test machinery but reversing
the fault. After verifying the *actual committed P3 Joint* old
`[broker 1,2,3]` / new `[2,3,4]` effective four-voter union and old committed
P5 generation 1, the test shuts down old-only broker 1. Old and new P3
majorities remain possible, yet broker 1 remains an eligible *all-voter*
clean-minimum participant. After a 350 ms hold (beyond 10 tick sideband
expiry), asserts controller still at JointCommitted and committed old
assignment, surviving broker 2's live P3 still Joint, surviving brokers 2/3/4
each have durable safe offset **zero** and retain the latest real tombstone at
stream offset 2. Only after P5 committed `FinalizeMovement` generation 2 and
actual new `[2,3,4]` identity proof may the positive three-voter current
manifest clean minimum commit, physical marker disappear, and surviving
leader fail over without resurrecting the older keyed value. Old broker 1
remains shut down and its orphan disk cannot manufacture positive safe
authority. **Focused PASS 1/1, 11 filtered, 68.17 s.** Existing opposite
new-only-voter/stale-incarnation test remains independent; both live tests
were run together with filter `p9g_joint_rf4_`, **PASS 2/2, 10 filtered,
146.32 s**, offline locked serialized. Still missing: targeted proof that
*all* three other sideband reports are positive and refreshed during old-only
outage; repeated fault/host-pressure campaigns and complete exact-HEAD gate.

**P9-150 real child-kill plus post-restart P7 suffix:** Extended existing
`p9g_v2_external_process_kill_preserves_committed_authority` (which kills
actual installer child at three distinct publication boundaries). After the
new-generation-published kill, the parent independently reopens the
V2 retained-history generation with original producer source bytes physically
compacted away, commits a genuine idempotent sequence-1 V1 suffix at Raft
index 4, checks the original recovered sequence-0 base offset 0 and next
sequence base offset 2 and expected next sequence 2, then restarts once more
and requires exact producer map equality and durable commit index 4. The two
prepublication kill cases still preserve old committed history unchanged.
**Focused PASS 1/1**; complete V2 focused family **PASS 9/9**. This is real
OS child termination and durable logical recovery, **not** OS power loss,
Windows directory-fsync authority, torn write, disk full, or automatic
controller-rejoined behind-safe producer retry.

**These changes postdate the previous 728/728 workspace PASS** and are not
included in that historical test count. Full current-worktree serial P5/P6,
Clippy/docs/fmt and P9-G closure qualification must be rerun. No feature
activation, staging, commit, push or release authorization was inferred.

## 2026-11-01 19:46 IST independent joint old-only logical-expiry oracle — PRE-COMMIT

The live RF4 old-only outage uses a 350 ms sleep, but sideband report validity
is counted in **leader observation ticks**, not wall-clock milliseconds.
Therefore an elapsed 350 ms under host load cannot independently prove ten
logical expirations, even though the live test genuinely proves controller
old-P5 and committed P3 joint membership plus retained physical tombstones.
Added a separate negative unit regression
`p9g_joint_old_only_report_expires_while_all_three_survivors_remain_positive`
in `crates/astralog-replication/src/compaction_progress.rs` (test only).
It starts with real typed reports from all four effective P3 joint voters,
then advances **exactly ten observation ticks** while continuously refreshing
the three surviving current-incarnation voters' positive reports. On tick ten
the old-only voter expires, the tracker retains three current positive
reports, and `candidate_safe_offset()` is **None** despite both joint Raft
majorities; a forged old incarnation is rejected, while a newly valid old
report can restore the exact minimum. No P3/P5 quorum, identity, TTL or
production code was changed. Targeted test **PASS 1/1**; complete focused
replication-library `p9g_` filter **PASS 22/22**, exit 0, with strict
replication Clippy (`-D warnings`), package fmt check and `git diff --check`
all PASS. This is deterministic sideband-expiration proof complementary to
the live RF4 outage, **not** a claim of a full all-survivors-positive live
network interval. P9-G remains OPEN pending full combined requalification and
the rest of the adverse matrix.

The separately added V2 external-kill child pause was also made finite:
the parent still has its 30s kill deadline, while an orphaned child fails
after an independent 45s watchdog if its parent is terminated unexpectedly.
This is `#[cfg(test)]` only; focused V2 **9/9** and format/diff passed after
this correction, and the later producer-suffix revision separately passed
its own focused 9/9 per the recorded source owner's result. It is not
production crash behavior or an OS power-loss claim.

## 2026-11-02 08:15 IST resumed qualification — full-suite RED remains OPEN

The interrupted serialized offline/locked `cargo test -q --offline --locked
--workspace -- --test-threads=1` command started on 2026-11-01 at 19:46:59
IST and eventually returned **exit 101**, despite earlier broker integration
groups having passed: `p5_dynamic` **12/12**, `p6_routing` **9/9** and
`p7_failover` **12/12**. The controller library ended **119/122**, with the
three exact failures:

- `runtime::tests::controller_leader_failure_at_every_movement_stage_preserves_durable_workflow` — P5-D leader election timed out at `crates/astralog-controller/src/runtime.rs:5268`;
- `runtime::tests::p9_retention_policy_commits_alters_fails_over_and_recovers_on_rf3` — P5-D mutation did not obtain a committed response at `runtime.rs:5341`;
- `runtime::tests::producer_id_allocator_is_bounded_commit_ordered_non_reusing_and_restart_durable` — P5-D leader election timed out at `runtime.rs:5268`.

The anomalous reported controller-test duration was **43,276.63 seconds**
across the overnight workspace run. No claim is made that host sleep, load,
Raft liveness or test timeouts caused the failures without supporting traces.
No exact-HEAD PASS or P9-G release follows from the successful preceding
groups. A separate unchanged offline/locked serialized
`cargo test -q --offline --locked -p astralog-controller --lib
-- --test-threads=1` recheck on November 2 **PASS 122/122, 33.12 seconds**;
this confirms nondeterminism of the prior observed RED, not its resolution.

Current-worktree `cargo test -q --offline --locked --workspace --doc`,
`cargo clippy -q --offline --locked --workspace --all-targets --all-features
-- -D warnings`, `cargo fmt --all -- --check` and `git diff --check` all
completed **PASS** on November 2. No source patch, weakening of P3/P5/P6/P7
fences or blind retry was used to obtain the focused recheck. Working tree
remains unstaged and uncommitted on `main` at `cd064a4`. P9-G/P9-151,
P9-H/I and P10 remain **OPEN/PENDING**; no later-phase release status should
be claimed on a resume.

## 2026-11-02 08:36 IST second full current-worktree attempt — RF4 RED, manually interrupted

An additional serialized, offline, locked full-workspace command was started
at **08:27:19 IST** with `CARGO_BUILD_JOBS=1`, `CARGO_INCREMENTAL=0` and
`cargo test -q --offline --locked --workspace -- --test-threads=1`; its raw
terminal output was tee'd to
`%TEMP%\astralog_p9g_full_20261102_082719.log`. The earlier small groups
reported **20/20, 42/42, 9/9 and 1/1 PASS**. The subsequent twelve-test P5
dynamic group emitted **two distinct FAIL markers**:

- `p9g_joint_rf4_new_voter_incarnation_and_stable_safe_minimum`;
- `p9g_joint_rf4_old_only_voter_absent_until_committed_exclusion`.

These tests had previously passed together **2/2** in focused isolated
qualification but did **not** survive this combined workspace attempt.
After the already-RED P5 executable remained active without producing a
completed failure summary, the known test-process tree (PIDs 29276/28096/
33588) was intentionally terminated at **08:35:58 IST** to conserve time.
The resulting `CARGO_EXIT=-1` reflects **manual interruption, not completion**
of the full workspace; the raw quiet-mode output contains the two FAIL
markers, **not their underlying panic messages**. Do not fabricate their
root causes, infer another test's result, or weaken Raft/P5/epoch/producer
fences to force a pass. The correct follow-up is a bounded fail-on-first,
`--nocapture`/`RUST_BACKTRACE=1` live RF4 reproduction with failure-only
P3/P5 actor, controller movement, local manifest/clean/safe observations,
followed by focused and combined qualification after a narrow confirmed fix.

No serving source was changed in response. Staging is empty; branch remains
`main`, HEAD `cd064a4fe60141590b6062957475aa15ee560717`. P9-G is **RED/
OPEN**, and P9-H, P9-I and P10 are **NOT IMPLEMENTED OR QUALIFIED**.

## 2026-11-02 10:10-10:45 IST: live RF4 failure isolated to obsolete V2 publication

With the P9-G worktree preserved, two exact offline/locked serialized RF4
tests initially gave different results: new-voter incarnation/stable safe
minimum **PASS 1/1 in 78.57s**, but old-only-voter absent until committed
exclusion **FAIL 0/1 in 74.01s**, reporting movement `JointCommitted`,
controller epoch 18, surviving brokers with no elected P3 leader and broker
3's partition actor unavailable (`p5_dynamic.rs:4893`). Another unchanged
old-only run passed **1/1 in 68.09s**. No unsafe safe-offset advance or marker
resurrection was seen; the failure is intermittent liveness, not proof of
data deletion safety. An experimental all-four-voter joint readiness test
was **reverted** because the controller legitimately commits `LeaveJoint`
before every follower has separately observed joint; requiring all four to
stay at that stage is not a valid P3/P5 invariant.

Debug-build-only diagnostics in `crates/astralog-broker/src/replication.rs`
showed broker 3's actor could exit with `replica is fatal/non-serving`, leaving
a closed actor client queue. A later old-only diagnostic run emitted no result
for over four minutes and was intentionally terminated **only after verifying
the owned process IDs/command lines**; its exit `-1` is an **interruption, not
a failed/passing test verdict**. Phase markers were added to the live RF4
integration test, after which old-only **PASS 1/1 in 76.32s**.

The full P5 integration group subsequently ended **11/12**, exit 101:
new-only RF4 **PASS**, old-only RF4 **FAIL** while the remaining historical
tests passed. The failure-only actor trace confirmed the exact fatal reason:

```
control=Err(Replication(Invalid("replica is fatal/non-serving")))
replica_fatal=Some("invalid replication data: P9-G V2 package targets wrong partition or obsolete committed snapshot")
```

The V2 storage gate in `storage_adapter.rs::install_rebootstrap_package_v2`
correctly forbids replacing committed local history when the incoming
snapshot's index is at or behind the follower's durable commit index. But
`ReplicatedPartition::step_with_rebootstrap_v2` previously submitted such a
package to P3 unchanged; P3 can legitimately advance *snapshot metadata*
even when local committed log data already covers that index. The rejected
V2 storage replacement was interpreted as a fatal persistence failure,
terminating a live voter and stalling controller joint movement.

Added an adversarial existing-history fixture to
`runtime::tests::p9g_v2_install_snapshot_is_p3_authorized_and_followed_by_normal_v1_suffix`:
a follower commits the Raft leader barrier at index 1 but retains snapshot
floor 0, then receives a V2 package for snapshot index 1. The first focused
run was deliberately **RED 0/1 exit 101** (`assertion failed: !ahead.is_fatal()`).
The minimal production correction in `runtime.rs::step_with_rebootstrap_v2`
checks the current persisted durable commit boundary *after* authenticating
and matching the package, then routes already-covered snapshot requests to
ordinary `self.step(...)` without attaching the V2 replacement. The frozen
storage rejection of V2-over-committed-history is unchanged. P3 still
authorizes any snapshot metadata advancement, and all current fail-closed
clean-progress/election checks remain. With this correction, the exact
regression is **PASS 1/1**, and replication P9-G library tests **PASS 22/22**
including external-process-kill and producer-state cases. Formatting and
`git diff --check` also passed.

This diagnosis and targeted result do **not** close P9-G. The combined P5
group and complete workspace still need repeated post-fix verification, and
the historical controller-election nondeterminism remains. Debug-only
diagnostics and test phase markers are **temporary** and must be reviewed or
removed before any release candidate. No staging, commit, push, deployment
or feature rollout was authorized or performed. P9-H/I and P10 remain pending.

### 2026-11-02 post-fix combined P5: V2 fatal fixed in targeted case, additional liveness RED

The first post-V2-patch serialized `cargo test --offline --locked -p
astralog-broker --test p5_dynamic -- --test-threads=1 --nocapture` completed
**10/12 PASS, exit 101** (`%TEMP%\astralog_p5_after_v2_fix_20261102.log`).
The previously fatal old-only RF4 movement **PASS** end-to-end, including
committed exclusion and positive all-voter clean minimum. However:

- `p9g_joint_rf4_new_voter_incarnation_and_stable_safe_minimum` **FAIL** at
  `p5_dynamic.rs:4676`: `P9-G P3 joint observation timed out: Elapsed(())`.
  This is an actual bounded 3s actor-observation timeout, not proof that
  a Raft quorum or safe-offset invariant failed. Do not silently increase
  or omit the actual P3 joint authority oracle; first distinguish actor
  scheduling/backpressure from partition fatal/restart and stage racing.
- `p9g_rf3_tombstone_removal_after_safe_commit_cannot_resurrect_on_failover`
  **FAIL** at `p5_dynamic.rs:487`: post-failover non-idempotent quorum
  Produce returned `Broker(NotLeader)` after an apparent new leader had
  been observed. No subsequent blind retry was performed. The failure
  diagnostic showed remaining live actors in term 4 and durable
  manifest/clean/safe values, with no observed tombstone resurrection.

The V2 current-commit test was further strengthened to carry an independently
committed application suffix at index 2 past incoming snapshot index 1,
checking its exact value and durable boundary after P3 metadata-only
acceptance and after process restart. Its exact focused test passed.
The diagnostic-only production `eprintln!` instrumentation was **removed**
after isolating the fatal; the intentionally narrow `P9G RF4 phase=...`
test-only milestones remain to localize intermittent failures. Do not call
the first combined post-fix run a PASS or an exact-HEAD qualification.

### 2026-11-02 subsequent conservative harness fixes and focused live PASS

Added **test-only error instrumentation** at the RF4 broker-1 P3 joint
observation: its fixed 3s authority timeout remains intact, but on timeout
the failure includes the latest actual controller stage and bounded direct
actor observations of all available brokers. This is not a substitute for
proof of actual committed joint consensus.

The RF3 non-idempotent post-failover quorum-Produce probe now waits for
**three consecutive** matching metadata and *target-local P3 leader/epoch*
observations before its single Produce dispatch. The wait has a 12s
bounded deadline. Once Produce is dispatched, **NotLeader, timeout or other
ambiguous response still immediately fails**: no retry or second mutation
is attempted; this only removes a known stale-authority pre-send race.

The rebootstrap committed-prefix regression now additionally proves the
exact application suffix at Raft index 2 survives the incoming snapshot
index 1, P3 metadata advancement and actual filesystem reopen (no V2 file
replacement). Focused V2 test **PASS 1/1**; replication library P9-G
**PASS 22/22** after strengthening, plus workspace format and Git diff check.

On this combined candidate, the exact offline/locked serialized
`cargo test -p astralog-broker --test p5_dynamic p9g_ -- --test-threads=1
--nocapture` **PASS 4/4 in 166.14s**, including both live four-broker joint
negative/positive acceptance tests and the RF3 failover/tombstone recovery
tests. The complete twelve-test P5 group is **running separately** and is
not yet qualified by this narrower result. Earlier RED tests remain in the
ledger, since one clean run cannot prove their intermittency resolved.

### 2026-11-02 subsequent full P5 intermittent timeout and timing audit

The subsequent complete offline-locked serialized P5 group returned
**RED 11/12**, `%TEMP%\astralog_p5_full_after_recovery_fix_20261102.log`:
only `p9g_joint_rf4_new_voter_incarnation_and_stable_safe_minimum` failed.
The original 3s P3 membership observation timed out. A bounded diagnostic
showed controller `JointCommitted`, broker 1's local actor unresponsive
(`Err(Elapsed(()))`), while brokers 2, 3 and 4 all reported P3 leader
broker 1, term/leader epoch 3 and committed index 11. This proves an
intermittent **local actor response liveness** gap, not a demonstrated
quorum breach, corrupted committed membership or tombstone resurrection.

Temporary debug-only runtime timing instrumentation then recorded complete
manager rebuilds and slow actor control/V2 export paths while running the
full 12-test P5 group again. This run **PASS 12/12 in 186.85s**, exit 0,
`%TEMP%\astralog_p5_runtime_restart_trace_20261102.log`. Observed runtime
restart times were generally **50–210ms**, no control handler exceeded the
1500ms trace threshold, and no V2 source export exceeded 200ms in this
successful run. Thus the source of the earlier 3s local-actor unresponsiveness
remains unproven. Do not attribute it to runtime restart, snapshot export or
host load without matching failing-run evidence; no P3/P5 safety fence was
changed. Debug timing prints were removed after the trace. Complete
workspace and intermittent repeat qualification are still pending.

## 2026-11-02 P9-G combined-liveness release candidate — NOT EXACT-HEAD CLOSURE

This section supersedes the prior *current status* without erasing historical
RED runs. The last authoritative committed parent is still `cd064a4` and no
P9-G commit, feature rollout, or P9-H/I/P10 implementation has been authorized.

1. **Reproduced real V2 failure and fixed fail-closed recovery:** a follower
   already holding a P3 snapshot index in its committed V1 log was receiving
   an `ASTRBTP2` package targeting that obsolete committed index. Storage
   correctly refused the file replacement, but the error terminated the
   partition actor. The receiver now passes that same-or-older committed
   snapshot request to ordinary authenticated P3 InstallSnapshot **without
   attaching a V2 file swap**. The exact regression was RED (`is_fatal`)
   before this change and GREEN after. It also preserves an independently
   committed application suffix past the incoming index across snapshot
   metadata advancement and filesystem reopen. Invalid/wrong-identity
   packages are still refused and cannot replace acknowledged history.
2. **RF4/RF3 actor liveness:** the broker's biased Tokio actor loop formerly
   could starve queued client observations indefinitely during continuous
   Raft/tick/progress control traffic. It now admits a pending client after
   at most 16 consecutive control turns, while preserving shutdown priority
   and ordinary control priority at all other times. Separately, the two
   read-only P7/P9 P3 observation hooks use a long-lived ReplicationClient
   clone instead of waiting on the manager mutex across reconciliation;
   existing atomically swapped routing lets an observation see a complete old
   or new topology, with a closed actor returning unavailable. No mutation
   authority, election/term fence, or ambiguous Produce retry was changed.
3. **Unrelated P6 compatibility test:** `p6_routing.rs` adds a bounded
   *read-only* oracle for a lost RevokeAck response: accept only independently
   recovered committed group epoch 4, STABLE, no pending revocations, exactly
   the intended partition owners; never blindly re-send the ambiguous
   mutation. Frozen production P6 groups/offsets remain unchanged.
4. **Post-fix source tests:** full offline locked serialized
   `cargo test -q --offline --locked --workspace -- --test-threads=1`
   **PASS 730/730, exit 0** at
   `%TEMP%\astralog_workspace_fair_cache_20261102.log`: P5 dynamic 12/12,
   P6 routing 9/9, P7 failover 12/12, controller 122/122, Raft 128/128,
   replication 130/130 and Storage 90/90. Focused `p9g_` live dynamic
   test **PASS 4/4** at `%TEMP%\astralog_p9g_fair_cache_20261102.log`;
   a previous complete P5 run also passed 12/12 in 189.40s. Past
   intermittent full-suite RED results remain relevant and are not erased.
5. **Static gate and second workspace run:** a later warning-denied Clippy
   run found one style-only `len() > 0` expression in the actor fairness
   change; it was replaced by behaviorally identical `!is_empty()`.
   Subsequent offline-locked Clippy `--all-targets --all-features
   -- -D warnings`, workspace no-deps rustdoc, fmt and `git diff --check`
   **all PASS, exit 0**. The 730/730 run is consequently **pre-final-source**
   evidence. A second full serial workspace run on the corrected source
   and completed at `%TEMP%\astralog_workspace_post_clippy_20261102.log`:
   **PASS 730/730 across 31 test-result groups, exit 0, zero failed**.
   Its P5 group **12/12** and P6 group **9/9** passed; the latter took
   **455.72s** under host load versus **317.05s** in the preceding full run.
   P7 **12/12**, controller **122/122**, Raft **128/128**, replication
   **130/130** including real V2 child-kill, and Storage **90/90** passed.
   Thus there are **two complete 730/730 workspace PASS runs**, one before
   and one after the style-only correction. No current source test failure
   remains observed; historical intermittent REDs remain in this ledger.

Independent read-only audit mapped P9-130..150 to concrete implementation,
negative tests, live RF3/RF4 tests and child-process-kill V2 publication
barriers. The checklist records those pre-commit evidence items. P9-150
does **not** claim physical power-loss/fsync-torn/disk-full chaos; broader
generation-fault and power-failure campaigns belong to P9-I/P12. P9-151
is still OPEN: prepare a sole bounded P9-G
closure commit only with the separately required Git authorization, then
run exact-clean-HEAD checks and attach the PASS note. The two untracked
governance documents must be handled explicitly, never by `git add -A`.
P9-H/I and P10 cannot be declared released or started under the
authoritative dependency sequence until that exact-HEAD gate is satisfied.

## 2026-11-02 exact-HEAD attempted closure — RED retained, not qualified

The user explicitly authorized bounded LOCAL stage/commit/exact-HEAD work for
P9 and P10 without any push or deployment. The exact thirty-file P9-G closure
candidate was committed as `43be8dda8215bf0b51d070e966a2dc1183d500c6`
on `main`, parent `cd064a4`. Immediately after the commit the main worktree was
clean, there were zero staged paths, and the earlier P9-F PASS note was intact.
**This G commit did not pass its exact-HEAD gate and has NO P9-G PASS note.**

1. First exact-clean-HEAD offline locked serialized workspace run FAILED
   `p5_dynamic` **11/12**, log
   `%TEMP%\astralog_p9g_exact_43be8dd_workspace_20261102.log`.
   `p9g_rf3_tombstone_removal_after_safe_commit_cannot_resurrect_on_failover`
   had already proven source COW, durable all-voter safe, physical marker
   removal, successor quorum suffix and P5 rejoin; then an immediate
   read-only successor-local observation `.unwrap()` failed during broker
   restart/topology reconciliation: `local partition actor is unavailable`.
   A *test-only* fix now polls the actual survivor P3 actor under the existing
   bounded twenty-second rejoin deadline, additionally requiring a committed
   post-failover suffix beyond the original snapshot and a known leader epoch.
   No source mutation or Raft/tombstone/lease fencing was relaxed. Its focused
   RF3 tombstone test **PASS 1/1** in 8.73s, log
   `%TEMP%\astralog_p9g_rf3_transient_rejoin_20261102_v2.log`.
2. An initial post-fix P5 rerun FAILED **11/12** at
   `%TEMP%\astralog_p9g_rf3_rejoin_12_p5_20261102.log` with a 40-second
   sustained-write campaign overdue immediately after a first ambiguous
   Produce (`acked=0, unacknowledged=1`). Windows event 42 recorded **idle
   suspension 12:27:59 IST**, and an RTC correction at wake 14:29:33
   advanced **7,293.002 seconds**; run lasted 7,477.31 seconds.
   Subtracting suspension gives 184.31 active seconds, consistent with earlier
   P5 full-run durations. The same focused P5 reassignment test **PASS 1/1
   in 9.14s** under a scoped temporary `SetThreadExecutionState` guard, log
   `%TEMP%\astralog_p9g_focused_p5_reassign_awake_20261102.log`.
   Never retry ambiguous P2 mutations or count them as acknowledged merely
   to hide a host sleep event.
3. A new **awake** complete P5 serialized run still FAILED **8/12** in
   262.84 seconds, log `%TEMP%\astralog_p9g_p5_12_awake_20261102.log`:
   RF4 new-voter scenario failed five-second final successor election,
   RF4 old-only scenario got an ambiguous `NotLeader` during its active-tail
   quorum Produce, RF3 sideband/V2 failover got `BrokerNotAvailable` with
   an unavailable survivor actor, and RF3 tombstone scenario found a live
   follower at committed index 6 but no V2 target manifest after its bounded
   thirty-second rebootstrap wait. Host idle sleep is **not** an explanation
   for these fresh RED results. Read-only independent RF3/RF4 audits and
   further safety-preserving product/harness diagnosis are required.

Current working tree has exactly one known test-only uncommitted fix in
`crates/astralog-broker/tests/p5_dynamic.rs` (until evidence ledger edits).
Do NOT stage, amend, attach a G PASS note, or begin P9-H/I/P10 serving
integration merely because isolated tests pass. Exact-clean-SHA full
qualification is still BLOCKED and historical RED receipts remain retained.

### Later 2026-11-02 recovery campaign: focused and complete P5 green

The preceding awake 8/12 RED was investigated without increasing client
timeouts, weakening P3/P5/Raft membership or quorum, or replaying an
ambiguous ordinary P2 Produce. In the RF3 V2 case the lost follower could
advance through ordinary V1 catch-up when a different un-compacted voter
was P3 leader; that result did not establish a retained-history V2 install.
A test-only source-restart experiment was RED 1/1 and **reverted**. Instead,
after P5 rejoin-incarnation commit and before erased-follower startup, the
harness now transfers genuine P3 leadership to the physically compacted
manifest holder; the original exact manifest/V2-file/on-disk catch-up
assertions remain unchanged. The RF3 tombstone removal test **PASS 1/1**
in **17.76s**, log
`%TEMP%\astralog_p9g_v2_after_rejoin_transfer_focus_20261102.log`.

RF4's source setup now reconfirms actual source leadership before the
non-idempotent active-tail write and again after catch-up for the COW epoch
fence. The final successor election asserts genuine P3 leadership within
a bounded 20s window with per-survivor read-only diagnostics; it does not
allow a leaderless result or stale term. Both adversarial RF4 cases passed:

- Old-only committed-Joint voter absent: **PASS 1/1 in 69.26s**,
  `%TEMP%\astralog_p9g_rf4_old_only_refenced_leader_20261102.log`.
- New voter incarnation/stable all-voter minimum: **PASS 1/1 in 79.01s**,
  `%TEMP%\astralog_p9g_rf4_new_voter_leader_fence_20261102.log`.
- RF3 sideband and V2 opt-in after leader failover: **PASS 1/1 in 6.15s**,
  `%TEMP%\astralog_p9g_rf3_sideband_rejoin_fenced_20261102.log`.

The final **complete P5 dynamic serialized 12/12 PASS in 196.86s** with
zero failures is preserved at
`%TEMP%\astralog_p9g_p5_full_fenced_20261102.log`.
The Windows power guard was limited to the command, then cleared in a
`finally` block; no persistent power-plan changes were made. The two old
complete-workspace 730/730 PASS runs predate these test-only changes. The
first exact clean closure SHA `43be8dd` remains **BLOCKED** with no PASS
note pending the sole bounded local amendment and independent complete
exact-HEAD workspace/static/frozen-source requalification. P9-H/I and
P10 remain disabled meanwhile.

## 2026-11-02 evening exact-HEAD repeat — independent failures retained

- Clean `4564d39` full offline-locked serialized workspace: **RED P5 10/12**,
  `%TEMP%\astralog_p9g_exact_4564d39_workspace_20261102.log`. RF3 sideband
  physical clean `[3,3,3]` but durable safe `[0,0,0]`; RF3 tombstone
  erased follower P3 index 7 without target V2 manifest.
- Exact-`4564d39` four-test repeat log
  `%TEMP%\astralog_p9g_4564d39_focused_20261102_evening.log` ended **RED
  2/4** after 3143.20s, with RF4 learner-stage deadline and RF3 failover
  `BrokerNotAvailable`. Windows system power events show a ~50-minute sleep
  ending 18:37 IST during this execution; that run is not clean liveness proof.
- New awake four-test repeat: **RED 3/4** in 173.17s, log
  `%TEMP%\astralog_p9g_4564d39_focused_awake_20261102_1928.log`. Both RF4
  and RF3 sideband passed; the RF3 tombstone case failed the actual all-voter
  safe replication oracle with clean `[4,4,4]` and durable `[0,4,4]`.
  This independently awake failure remains open.
- Added test-only diagnostic capture to that exact safe-offset assertion:
  live membership/actor, manifest generation, current-generation clean
  proof and durable safe on each of three voters; no deadline or authority
  relaxation. Awake isolated tombstone **PASS 1/1 in 9.88s**,
  `%TEMP%\astralog_p9g_rf3_tombstone_diagnostic_awake_20261102_1932.log`.
  Complete awake P5 dynamic suite **PASS 12/12 in 191.22s**,
  `%TEMP%\astralog_p9g_p5_12_diagnostic_awake_20261102_1933.log`.

The newest green evidence is neither a proof of intermittent immunity nor
an exact-HEAD full workspace qualification. Keep P9-G release BLOCKED;
do not attach its PASS Git note or unlock P9-H/I/P10 based on these runs.

## 2026-11-02 19:38-20:29 IST: repeated exact-HEAD RED and unsafe-leader actor fix

- Exact-clean-HEAD `a952602a2cbad059dd3d6976c4a94fda0a18a5ba`
  initially passed all **730/730 across 31 groups**, log
  `%TEMP%\astralog_p9g_a952602_exact_workspace_20261102.log`.
  A *second unchanged exact-HEAD* full workspace RED in P5 **10/12**:
  `%TEMP%\astralog_p9g_a952602_exact_workspace_repeat_20261102.log`.
  RF4 target leadership changed between a transferred-leader observation and
  a COW; RF3 post-safe COW received explicit `StaleEpoch`. Neither error
  was evidence of permission to bypass committed P3/P5 authority.
- A new test-only bounded leader rediscovery and post-safe COW stale-epoch
  recovery retains all non-idempotent Produce no-retry fences and verifies
  the tombstone remains on disk before any new maintenance attempt. Focused
  P9-G 4-test rerun was **RED 3/4** under this correction, and captured a
  different persistent defect: a voter actor was unavailable with local
  disk safe zero while two other voters committed the safe offset, log
  `%TEMP%\astralog_p9g_staleepoch_transfer_harness_4_20261102.log`.
- The actor error probe reproduced the defect: broker 1 returned
  `P9-G leader is behind committed safe history; rebootstrap is mandatory`
  while P3 still observed it as Leader after a physical recovery/leadership
  handoff. The broker formerly treated that *recoverable safety fence* as a
  fatal actor result. Repeated complete P5 reruns **3/3 x 12/12 PASS** on
  pre-product-fix source still logged this very actor-fatal error, so a
  green test alone was insufficient. Log
  `%TEMP%\astralog_p9g_p5_actor_root_full_20261102.log`.
- The replication-runtime candidate now keeps an unverified old leader
  non-serving, discards its prior leader outbound effects, blocks new
  local proposals/configuration/snapshot/transfer authority, and admits only
  higher-term authenticated P3 Append/InstallSnapshot that P3 itself must
  authorize. A follower still cannot campaign with insufficient retained
  history. No synthetic committed index, quorum shortcut, stale reader or
  ambiguous P2 retry is introduced.
- The augmented RF1 safe-offset regression **PASS 1/1** and the four live
  RF3/RF4 P9-G scenarios **PASS 4/4 in 164.60s** on the new working tree,
  log `%TEMP%\astralog_p9g_nonfatal_recovery_4_20261102.log`. This is
  product-fix candidate evidence, not release authority.

Outstanding: repeat entire serialized workspace against the final clean
amended SHA, verify no old-leader actor fatal and safe floor convergence,
run warning-denied Clippy/doc/fmt/frozen-source gates, and attach the exact
SHA P9-G PASS note only when those gates truly pass. P9-H/I/P10-P12 remain
blocked meanwhile.

### 2026-11-02 20:40 IST final working-tree quarantine probe

The fail-closed broker actor quarantine also blocks Fetch and every client
mutation/maintenance request when the required current-generation retained
proof is unavailable; the producer idempotent Duplicate/Coalesced paths are
vetoed *before* cache admission. The existing P3 membership observation
remains available for an authorized higher-term successor. With these
additional guards, the focused RF1 unsafe-leader regression **PASS 1/1** and
live P9-G RF3/RF4 **PASS 4/4 in 167.80s**. The captured log contains
**zero** `P9G_REPLICATION_ACTOR_FATAL` records:
`%TEMP%\astralog_p9g_safe_quarantine_final_focused_20261102.log`.
No public P9-G feature has been activated, and no closure PASS is inferred.
