# AstraLog P9-G — Local Closure Candidate Qualification Report

**State:** COMMITTED CANDIDATE ONLY; **P9-G NOT YET QUALIFIED**. The last
qualified source commit is the P9-F parent
`cd064a4fe60141590b6062957475aa15ee560717`. P9-G candidate revisions
`43be8dd` and `4564d39` have recorded exact-HEAD RED results. There is
no P9-G exact-HEAD PASS Git note as of this report.

## Scope and non-goals

P9-G owns only `P9-130..151` in `P9_CHECKLIST.md`: manifest-bound local
clean-progress proof, authenticated voter progress, quorum-committed
compaction-safe authority, grace/snapshot-gated marker omission, and retained
Storage V2 rebootstrap under ordinary P3 authorization. It is not a global
cleaner scheduler (P9-H), full release fault/compatibility closure (P9-I),
work queue (P10), security/rolling upgrade (P11), or chaos/performance
release (P12). Default P9-G peer-report/V2 opt-ins remain false; activation
requires committed feature level 5 and homogeneous supporting peers.

## Implemented evidence boundaries

| Items | Authoritative source and required negative case |
| --- | --- |
| P9-130..132 | `astralog-storage/src/clean_progress.rs`: 52-byte CRC32C topic/partition/manifest-generation-bound local sidecar; zero on missing, corrupt, foreign or stale evidence, verified complete immutable prefix and monotonic exclusive local bound |
| P9-133..138 | `astralog-replication/src/compaction_progress.rs`, P9 peer transport, broker actor and live RF4: bounded 80-byte report, current term/leader/incarnation/assignment/P3 effective-voter fencing, missing/expired voter blocks safe floor even with three other positives, current committed P5 exclusion permits later new-stable minimum |
| P9-139..143 | Versioned `AdvanceCompactionSafeOffset` quorum-committed maintenance, anchor recovery and a strict persisted snapshot/grace/`tombstone_offset < committed_safe` marker-deletion gate; pre-safe marker is retained |
| P9-144..145 | Live RF3 physical omission/restart/leader change and RF4 old/new voter lifecycle; replica behind replicated safe is ineligible to campaign/lead but remains available for authorized catch-up/rebootstrap |
| P9-146..149 | `astralog-replication/src/rebootstrap_v2.rs` and `storage_adapter.rs`: `ASTRBTP2` file/manifest/checkpoint/snapshot package with bounded counts and bytes, SHA-256 and CRC32C validation, staged durable publication and P3 InstallSnapshot authorization; no silent replacement of equal-or-older committed history |
| P9-150 | V2 synthetic fault/corruption matrix plus real child-process termination at durable intent, old-generation backup and new-publication boundaries; prepublication recovers old, fully published recovers new; producer checkpoint, exact retry and a V1 idempotent suffix survive restart |

The detailed per-item acceptance evidence is in `docs/P9_CHECKLIST.md`;
chronological failures, test names, log excerpts and bounded limitations are
retained in `docs/P9_G_WORK_IN_PROGRESS.md`. This report is **not** permission
to mark P9-151 or move to P9-H.

## Reproduced RED and repair

1. **Obsolete V2 package killed a valid joint voter:** live RF4 showed a V2
   package whose snapshot index was already committed locally. The storage
   swap rejection was correct, but its propagation killed the partition
   actor and obstructed elections. The authenticated P3 request now uses
   normal snapshot handling without V2 swap when the complete local durable
   commit index already covers the package. The focused regression was
   independently RED before repair, then GREEN after, preserving the local
   committed Raft/application suffix through disk reopen.
2. **Control-biased actor could starve local observations:** two historical
   full P5 RED cases timed out on actual P3 local membership observation
   while surviving voters reported current committed Raft state. Bounded
   client service after 16 consecutive control turns resolves the unbounded
   scheduling opportunity without weakening shutdown, election, commit or
   tombstone authority. The two read-only P7/P9 observations now also use an
   already-supported atomically switched ReplicationClient routing clone
   instead of blocking behind manager reconciliation. Writes, leadership
   transfers and controller mutations retain their prior paths.
3. **P6 lost-response test harness:** a historical P6 RevokeAck response can
   be ambiguous after a successful durable commit. The changed P6 test
   accepts only a bounded independently recovered exact committed STABLE
   group state; it never retries that non-idempotent mutation and fails if
   the state is not provable. No production P6 source changed.

No previous RED run is deleted from the chronological ledger. A passing
campaign limits the observed reproduction but does not constitute a proof of
immunity to all scheduling or host-load failures.

## Candidate gates already observed

- Live P9-G `p9g_` filtered broker dynamic integrations: **4/4 PASS**, log
  `%TEMP%\astralog_p9g_fair_cache_20261102.log`.
- Complete broker P5 dynamic integrations: **12/12 PASS** on the bounded
  fairness candidate, log `%TEMP%\astralog_p5_fairness_20261102.log`.
- Full offline/locked serialized workspace, `cargo test -q --offline
  --locked --workspace -- --test-threads=1`: **730/730 PASS, exit 0**, log
  `%TEMP%\astralog_workspace_fair_cache_20261102.log`. It contains broker
  P5 12/12, P6 9/9, P7 12/12, controller 122/122, Raft 128/128,
  replication 130/130, Storage 90/90 and all other workspace groups.
- Following a behavior-equivalent Clippy style correction (`len() > 0` to
  `!is_empty()`), warning-denied offline-locked Clippy on all workspace
  targets/features, offline-locked workspace `cargo doc --no-deps`,
  `cargo fmt --all -- --check`, and `git diff --check` each **PASS**.
- The first 730/730 is **pre-final-source** evidence. A second complete
  offline-locked serialized run **PASS 730/730, exit 0, zero failed** on the
  post-Clippy-correction source, log
  `%TEMP%\astralog_workspace_post_clippy_20261102.log`. In that repeat,
  P5 **12/12**, P6 **9/9** (455.72s), P7 **12/12**, controller **122/122**,
  Raft **128/128**, replication **130/130** (including V2 child-kill),
  and Storage **90/90** passed. Both complete runs have 31 test-result
  groups. This is current dirty-working-tree evidence, **not** clean
  exact-commit qualification.

## Frozen-compatibility and resource checks

No source changes under the original `astralog-protocol`, `astralog-raft`,
`astralog-controller` or `astralog-groups` crates. Their wire/golden, Raft,
controller and group suites are included in complete workspace regression.
Storage V1 bytes remain immutable; retained-history compaction writes
versioned V2 only below the P3 snapshot-safe boundary. Broker-side P9
progress is explicitly opt-in, bounded and fenced by actual P3/P5 authority.
The V2 package validates file count, individual/total bytes, CRC/SHA and
recoverable committed state before publication. Absent/outdated proof
retains data; it never authorizes tombstone removal.

## Release boundary and limitations

P9-G tests include actual child-process termination and recovery but do not
represent an electrical power cut, torn filesystem sync, disk-full chaos or
multi-host partition campaign. The latter are independent later P9-I/P12
requirements, not substitutes for this P9-G gate. There is no benchmark,
throughput, rolling-upgrade or production-rollout claim.

The user explicitly authorized a bounded **local P9-G closure commit** and
completion of P9-H/I and P10 in dependency order on 2026-11-02. This approval
does **not** authorize a push, deployment or premature feature activation.
Before P9-151 is *authoritatively* closed: (a) review every checklist proof,
(b) synchronize README/CURRENT_STATE and governance documents,
(c) preserve/explicitly select all intended files, (d) make the bounded
locally authorized P9-G commit, (e) re-run exact-clean-HEAD focused/serialized
compatibility, Clippy, docs, fmt, frozen-source and diff checks, then (f)
attach `P9-G exact-HEAD qualification: PASS` Git note only if all pass. The
pre-commit report does not claim that this note already exists; post-commit
test results must be recorded as a separate later evidence section. P9-H/I
and P10 remain blocked until that exact-HEAD note exists.

## 2026-11-02 local commit and postcommit qualification repair — still candidate

The initial reviewed P9-G candidate was committed locally as
`43be8dda8215bf0b51d070e966a2dc1183d500c6` with thirty explicitly
selected paths; it was not pushed or deployed. The first exact-clean-HEAD
serialized workspace failed in P5 dynamic **11/12** on a transient read-only
local actor observation immediately following P5 broker rejoin, so **that SHA
is NOT release-qualified and carries no P9-G PASS note**. Additional runs
found a 7,293.002-second Windows idle sleep in an unrelated 40-second P5
Produce campaign and separately reproduced awake P9-G RF3/RF4 integration
readiness failures. Do not erase or reinterpret these recorded RED cases.

The *test-only* repair now requires an actual committed post-failover P3
suffix after rejoin before reading successor recovery authority, transfers
leadership to the manifest-governed compacted source before deliberately
erasing a follower to force genuine P3-authorized V2 rebootstrap, rechecks
actual source P3 leadership/epoch before non-idempotent RF4 source writes/COW,
and gives final RF4 successor election a bounded twenty-second observation
window with diagnostic peer state (rather than a five-second scheduling
deadline). **No Raft product authority, quorum/safe-offset, tombstone,
producer idempotency or ambiguous P2 retry semantics were modified.**

The four individually focused P9-G RF3/RF4 live integration cases now pass.
The complete updated offline-locked serialized P5 dynamic suite passed
**12/12** in **196.86 seconds**, log
`%TEMP%\astralog_p9g_p5_full_fenced_20261102.log`, without host sleep.
That is working-tree evidence, **not** exact-clean-HEAD full workspace
qualification. The sole bounded local closure candidate will be amended with
the test-only fix and evidence; only a subsequent clean exact-SHA workspace,
static/frozen-source gate and explicit PASS Git note can qualify P9-G.

## 2026-11-02 evening exact-HEAD falsification and diagnostics

The amended clean candidate `4564d39` was independently tested in a full
offline-locked serialized workspace; P5 returned **10/12 RED**. The RF3
sideband scenario had verified physical clean `[3,3,3]` but durable safe
`[0,0,0]`, and the tombstone recovery scenario reached P3 index 7 without
installing the required retained V2 manifest. No prior green run supersedes
these failures.

The subsequent four-test run was interrupted by Windows sleep for about
50 minutes (verified via Windows power logs); its **2/4 RED** is not clean
liveness evidence. A new awake four-test run passed both RF4 cases and the
RF3 sideband case but failed the RF3 tombstone scenario **3/4**, with physical
clean `[4,4,4]` and durable safe `[0,4,4]`. The replicated control had
not converged on all three eligible voter disks within the unchanged 15s
bound. No safety assertion or quorum requirement was relaxed.

The test-only diagnostic improvement now captures live P3 membership,
partition actor availability, manifest generation, local clean sidecar and
disk-side safe offset for all three physical voters on that precise RED.
An awake isolated RF3 tombstone rerun passed **1/1** in **9.88s**, log
`%TEMP%\astralog_p9g_rf3_tombstone_diagnostic_awake_20261102_1932.log`;
the complete awake P5 integration suite then passed **12/12** in **191.22s**,
log `%TEMP%\astralog_p9g_p5_12_diagnostic_awake_20261102_1933.log`.
These runs are *working-tree* evidence, not exact-HEAD workspace authority.
P9-G remains BLOCKED pending repeatable exact-clean-HEAD full qualification,
static/compatibility checks, and its PASS Git note.

## 2026-11-02 reproducible unsafe-leader actor failure and bounded repair

The first clean `a952602` workspace **PASS 730/730** did not establish
repeatability: its second exact-HEAD run failed P5 **10/12**. Additional
instrumentation showed that a physically recovering voter can temporarily
hold P3 Leader role after losing verified current-generation clean history,
even while another voter has a valid higher-term replacement. The existing
runtime rejected the unsafe leader's incoming replication with an error;
the broker propagated that nonfatal recovery fence as a fatal partition
actor shutdown. The post-failover replicated safe control then committed
on other voters while the killed actor retained disk safe zero.

The new runtime repair retains the safety veto but not the actor kill:
unverified leaders cannot propose new records, reconfiguration, snapshots
or transfers and must drop stale outbound authority. In addition, the broker
quarantines Fetch and all client mutation/maintenance commands while the
retained generation lacks verification; idempotent cached duplicate and
coalesced responses are fenced before admission as well. Only higher-term
authenticated P3 Append/InstallSnapshot can advance them toward follower
repair. The P3 engine retains sole membership, term and persistence
authority, and rebootstrap still requires a genuine P3 InstallSnapshot.
This is *not* a change to no-resurrection or all-voter minimum requirements.

An augmented runtime regression passed, followed by **4/4 live P9-G
RF3/RF4** integrations (164.60s), but complete new exact-HEAD qualification
and independent review remain outstanding. P9-G stays BLOCKED.

On the final working-tree quarantine candidate, the bounded RF1 regression
passed 1/1 and all four focused live P9-G cases passed 4/4 in 167.80s,
with **zero actor-fatal diagnostic markers**. The log is
`%TEMP%\astralog_p9g_safe_quarantine_final_focused_20261102.log`.
This evidence is still pre-commit; do not substitute it for exact-HEAD
workspace, static, repeatability or compatibility qualification.
