# Audit remediation — simple interview explanations

This file explains bounded changes after the November 3, 2026 audit. These
changes do **not** upgrade AstraLog to a production Kafka replacement.

| Concept | What changed | What it proves (and does not prove) |
| --- | --- | --- |
| Failure detector | The P7 readiness loop holds synthetic time constant and reports each broker's observed health. | Startup is no longer racing a fabricated 1,000-tick jump. It does not prove fault detection is perfect. |
| Correctness oracle | The benchmark checks each fetched key, full payload, sequence identity and contiguous offset against successful Produce calls. | It now detects duplicate, substituted and missing benchmark records. It is not a formal consensus proof. |
| Idempotency | P7 failover, epoch, controller-outage and repeated-failover cases now compare actual expected committed payload bytes and contiguous offsets, not only record counts. | An old response may be lost without creating duplicate logical records in these tested scenarios. |
| Tail latency | New runs can save individual request samples (`latency_samples.csv`) and the summary script verifies nearest-rank percentiles. | A sample p99 based on fewer than 100 observations remains statistically weak. |
| Reproducibility | New CSV rows declare the executable profile, dirty worktree and sample count. The launcher saves native Cargo stdout/stderr separately. | Results can be independently checked; do not mix new runs with the original pinned `d7e32d3` evidence. |
| CI | The checkout action is pinned, Cargo uses `--locked`, and Rust 1.98.1 is explicit. | A GitHub-hosted Ubuntu run still needs to execute after publishing; Windows tests do not stand in for Linux. |
| Publication | Added Apache-2.0 license, sanitized committed interview log username and provided a history-free export script. | Public GitHub should use a fresh snapshot; pushing the **existing** `main` history will still reveal older file versions. |
| Security & durability | Broker public, broker-Raft and controller-Raft listeners fail config validation for wildcard/non-loopback addresses; `SECURITY.md` documents Windows directory-sync limitations. | No TLS, authenticated multi-machine deployment, or Windows power-loss durability is claimed. |
| Failure injection | Existing storage-adapter tests cover process-kill recovery; an additional static RF3 test starts three broker OS processes, kills the leader after a quorum commit, fetches the exact retained value and commits a new record on the successor. | This is stronger than graceful shutdown but still does not qualify controller-managed P9-G multi-process failover or power loss. |

## P9-G retained-history V2 qualification

The full audit run failed to observe an exact V2 manifest on one rejoined
follower, despite its Raft commit index converging. The isolated rerun passed.
**A later November 3 full-workspace run caught a more specific correctness
defect** in the same P9-G case: a follower went fatal with
`rebootstrap package does not advance the durable snapshot/commit boundary`.
Independent tracing found that the package was **V1**, not V2. A follower can
have the Raft prefix durably committed at index N while its snapshot metadata
still trails N. An InstallSnapshot request at N may validly update metadata,
but it must **not replace the already committed files**. V2 already checked
this situation; the V1 runtime path did not, so the storage no-overwrite
guard correctly rejected the stale package and the runtime wrongly made the
follower fatal. The V1 runtime now discards the obsolete *file package* and
lets Raft handle the InstallSnapshot metadata. The strict storage guard,
sender/term/leader checks and exact payload assertions are unchanged.

`runtime::tests::v1_snapshot_of_already_committed_prefix_preserves_local_bytes`
passes and asserts that the snapshot metadata advances, committed bytes stay
identical, and the follower does not become fatal. The previously failing
`p9g_rf3_sideband_and_v2_opt_in_survive_broker_leader_failover` focused test
also passed after this fix. A subsequent complete serialized source-tree
regression passed **737/737**, including P9-G **12/12**. The original distinct
missing-V2-manifest symptom still needs exact branch-level attribution, and
one green run cannot establish production liveness under all schedules.

For the disk-heavy P9-G fixture, election timeouts were widened **in the test
only**, and failures now show each peer's current local state. The strict
manifest equality and byte comparison remain unchanged. This change reduces
timing-induced test races; it is **not** a demonstrated production bug fix.
If that case fails again, capture the full diagnostics and investigate whether
the current leader has the authoritative retained manifest and whether the
follower received V2 InstallSnapshot or ordinary V1 AppendEntries.

To distinguish these cases without changing product behavior, set
`ASTRALOG_P9G_TRACE=1` while running the specific debug test. It records
whether a snapshot request was **sent as V1**, **sent as V2**, or **deferred**
because its exact committed boundary was stale or an uncommitted suffix
remained. The option is off by default. A legal V1 catch-up from an
uncompacted leader is not by itself proof of data loss; prolonged V2
defer/retry liveness under sustained writes remains unqualified.

## How to explain it

Raft decides **which log entries are committed**. Storage decides **which bytes
survive a restart**. A test oracle checks **whether the right bytes came back**.
These three are different: checking only a commit index or record count is not
enough to validate the contents. Likewise, the failure detector's logical time
is not wall-clock time; a test should not jump time merely to wait for startup.

**Ambiguous P6 join:** A timeout after `JoinSubscribe` means the server might
have committed the member but lost the response. Retrying the mutation could
create another member. A later full workspace run surfaced this correctly
fail-closed client result in the last P6 case (**8/9**), while the same test
passed focused. The test now checks the *committed group record* for one
uniquely matching epoch-1 member before proceeding and fails if it cannot
prove the join. The client retains its no-blind-retry safety behavior. The
focused case passed 1/1 and a subsequent serialized P6 rerun passed **9/9**.

## Minimal commands

```powershell
cd D:\Workspace\AstraLog
cargo fmt --all -- --check
cargo clippy --workspace --all-targets --all-features --offline --locked -- -D warnings
cargo test -p astralog-broker --test p7_failover --offline --locked -- --test-threads=1
powershell -NoProfile -ExecutionPolicy Bypass -File scripts/run-interview-demo.ps1 -Fast
```

`-Fast` produces **debug smoke**, not publishable performance figures. A
clean-commit release run is needed for new optimized measurements. The old
report in `docs/benchmarks/interview_20261103` remains tied to its original
source and workload; do not silently replace it.

### Bounded verification of these changes (November 3, 2026)

On the local Windows/Rust 1.98.1 workspace, the following finished successfully:

- `cargo fmt --all -- --check` and warning-denied full-workspace Clippy.
- Complete P5 dynamic integration target: **12/12 PASS**.
- Complete P7 failover integration target: **12/12 PASS**.
- Separate-process static RF3 leader-kill test: **1/1 PASS**, with a quorum-committed record preserved and a successor quorum append.
- One-command `-Fast` smoke: both RF1/RF3 tests and three focused restart/failover checks passed; raw latency percentiles independently recalculated.
- One serialized `cargo test --workspace --all-features --offline --locked -- --test-threads=1`: **732 tests passed, zero failed**, across 32 test-result groups.
- After the additional V1 committed-prefix and P6 ambiguity corrections, the **final local source-tree** `cargo test --workspace --all-features --offline --locked -- --test-threads=1` passed **737/737**, zero failed, across 32 test-result groups. Format and warning-denied Clippy also passed. This supersedes the older 732-test count **for the current local source tree**, not as a production release-gate decision.
- A separate, clean-HEAD release benchmark after the fixes: **12/12 workload measurements**, **1,232 acknowledged and subsequently fetched records**, **zero producer errors**, and **2,464 independently checked raw produce/fetch latency samples**, stored in `docs/benchmarks/remediation_20261103/`. Benchmark source commit: `35190af265ad5ce9c4e23278785e0a3bd0a7ecdf`.

These are executed regression observations, **not an exact-HEAD production
release qualification**. P9-G previously failed intermittently; a green
repeat does not mathematically eliminate that risk. Native Cargo output from
the remediation runs is kept in local `%TEMP%/astralog_remediation_*` folders,
not added to GitHub as personal-path-bearing logs.

The new RF3 quorum release measurement was **10.71 records/s unbuffered** and
**33.31 records/s buffered** (about **3.11x**). The earlier 3.28x value remains
in its own historical baseline; the short, single-host runs are not a capacity
SLA and should never be combined into a favorable best-of benchmark.

The Windows launcher now waits on Cargo's direct process handle with a 15-minute
bound. `Start-Process -Wait` can otherwise remain blocked on a lingering
console-host descendant after the benchmark process has already exited. The
release benchmark itself completed 2/2 before this wrapper issue was identified;
the raw evidence and independent report are retained.

**Public-history tradeoff:** A history-free export intentionally cannot
contain the old private commit `d7e32d3`, even if the historical CSV mentions
it. The old CSV remains an archived baseline, **not publicly SHA-verifiable**
in a brand-new GitHub repository. For publicly reproducible claims, first
initialize and commit the clean export, then rerun the release benchmark
against that *public* source commit and retain its fresh evidence separately.
