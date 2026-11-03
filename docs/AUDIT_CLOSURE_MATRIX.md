# AstraLog adversarial audit closure matrix (2026-11-03)

**Scope:** A modest Rust distributed-log interview demo, not a production
Kafka replacement. **Closure** below means the *specific audit item* is
supported by source plus relevant executed evidence. It does **not** mean all
possible distributed-system faults have been eliminated.

| Audit ID | Status | What was done / remaining evidence |
| --- | --- | --- |
| A01 P9-G manifest convergence | **Reproduced V1 fatal fixed; broader V2 liveness still unqualified** | A subsequent serialized full-workspace run reproduced a fatal follower: a V1 rebootstrap file package was attached to a snapshot already covered by durable commit. The runtime now processes the Raft InstallSnapshot **without replacing committed files**, mirroring the existing V2 guard. Exact-data unit regression and previously failing P9-G focused integration test pass; the strict storage no-overwrite fence remains. Test-only timing, all-peer diagnostics and opt-in `ASTRALOG_P9G_TRACE=1` make other V1/V2 branches observable. The earlier separate missing-V2-manifest symptom and continuous-write convergence are not conclusively explained. |
| A02 P7 logical clock | **Fixed / tested** | Readiness uses one logical timestamp (not a new +1,000 tick per poll); failed readiness prints each broker's liveness observation. P7 full suite passed 12/12 after remediation. |
| A03 benchmark Fetch oracle | **Fixed / tested** | Verify record identity, full value bytes, uniqueness, offset continuity and absent unexpected headers against successful Produce calls. RF1/RF3 debug and release tests passed. |
| A04 P7 content assertions | **Fixed / tested** | Selected ambiguous retry/failover cases and all remaining count-only P7 callers now use exact expected payload and offset checks; six seeded rounds compare the full ordered payload sequence. |
| A05 tail sample provenance | **Fixed capture; statistical limit remains** | Save each request's elapsed nanoseconds, independently recompute p50/p95/p99, reject missing/extra rows and warn that with fewer than 100 samples p99 is the observed maximum, not reliable capacity evidence. |
| A06 private Windows path | **Public-export guarded** | Sanitized committed release logs; a newly initialized public repository must use `scripts/prepare-public-snapshot.ps1` so older private Git commits are not published. Existing private Git history is deliberately retained locally. |
| A07 missing license | **Fixed** | Full Apache-2.0 `LICENSE` added in the repository root. |
| A08 Windows directory durability | **Accurately scoped, not Windows power-loss qualified** | `File::sync_all` before rename; directory sync exists on Unix and is a no-op on Windows. Linux-first durability assumptions and limits documented in `SECURITY.md`. Windows crash-restart tests do not prove sudden power-loss persistence. |
| A09 unauthenticated network exposure | **Local access restricted; remote auth unimplemented** | Public broker, broker Raft and controller Raft configs now reject wildcard and non-loopback listeners with targeted negative tests. TLS, per-client identity and same-machine process isolation remain outside the local demo. |
| A10 CI portability | **Configured, hosted verification pending** | GitHub Actions uses pinned checkout, Ubuntu 24.04, Rust 1.98.1, Cargo `--locked`, fmt, Clippy and serialized tests. No hosted Actions result exists before publication; local Ubuntu WSL has no Rust toolchain installed. |
| A11 benchmark profile/dirty state | **Fixed / tested** | Capture full Git SHA, `debug`/`release`, dirty-worktree flag and per-case sample counts; release reports reject dirty profiles. The clean-commit source `35190af` has 12 validated release rows and 2,464 raw request samples. |
| A12 separate-process RF3 crash | **Fixed / focused test passed** | New `rf3_separate_process_leader_kill_preserves_committed_quorum` spawns three distinct broker OS processes, kills the elected leader with `Child::kill`, reads the already committed exact payload on a successor, then confirms a new quorum write without unsafe retries. Static RF3 scope; does not qualify controller-managed P9-G process death. |
| A13 native test log capture | **Fixed / tested** | Windows launcher collects Cargo's native stdout/stderr with a direct .NET process handle, bounded wait and reliable exit code; the Fast demo and its three reliability tests passed. |

### Additional failure discovered by repeating the full workspace

After fixing the stale V1 snapshot race, the serialized workspace run passed
P5/P9 **12/12** but failed the last P6 integration case **8/9**:
`p6_silent_member_gets_fresh_grace_then_times_out_after_coordinator_failover`
received an **ambiguous** `JoinSubscribe` result during initial admission.
That was **not proof of duplicate data**; the client correctly refused to
blindly retry an operation that may already have committed.

The P6 **test harness only** now confirms group-coordinator readiness before
issuing its first join and, on an ambiguous result, reads the authoritative
committed group history. Only a unique Stable epoch-1 member matching the
original anonymous-member contract (timeout, subscriptions and member epoch)
proves success. If that evidence is absent, the test still **fails**; no
uncertain mutation is replayed. The focused test **passed 1/1**, and the
complete serialized P6 target **passed 9/9** after this change. A subsequent
full-workspace run, with all these fixes present, passed **737/737** tests in
32 result groups, including P6 9/9, P5/P9-G 12/12, P7 12/12 and the separate
OS-process RF3 leader-kill test. This is a passing local *source-tree*
regression, not an exact-HEAD production release gate. Prior failed runs
remain part of the evidence and still motivate continued liveness testing.

### Reproducible local gates

```powershell
cargo fmt --all -- --check
cargo clippy --workspace --all-targets --all-features --offline --locked -- -D warnings
cargo test --workspace --all-features --offline --locked -- --test-threads=1
```

All three passed in the final local audit. Public GitHub-hosted Ubuntu CI,
multi-machine TLS, physical Windows power-loss behavior and sustained P9-G
V2 liveness are **not** qualified by this run.

## Failure-invariant gaps (read before an interview)

| Injection | Evidence | Still missing |
| --- | --- | --- |
| Metadata leader change | Controller/Raft tests | Wider real multi-process controller failure campaign |
| Broker leader shutdown / OS kill | P7 idempotent retries and exact payload checks; separate-process static RF3 leader-kill 1/1 PASS | Controller-managed P9-G process-kill and endurance campaign |
| Minority network partition | Raft model and replication tests | Sustained multi-process partition/fault-network campaign |
| Stale follower rejoin | P9 strict manifest and retained-byte oracles | Continuous-write V2 liveness/root-cause diagnosis |
| Corrupted storage | CRC, recovery and external process-kill tests | Physical power-loss/FS crash matrix on intended platform |
| Concurrent client retries | Idempotent producer and seeded P7 tests | Long endurance and extreme sequence-boundary campaign |
| Compaction + membership | P9-G RF4 joint-membership tests | Full P9 production release gate |
| Malformed network requests | Decoder bounds and negative unit tests | Independent fuzz/security qualification |

## Interview mental model

**Commit** means the required consensus replicas have accepted the log entry;
**durability** is the storage promise about bytes surviving failures; **Fetch
oracle** means the test verifies the same ordered bytes came back. All three
need separate evidence. A **logical tick** is controlled test time, not
30 milliseconds or one real-world second. **Process-kill** is an abrupt
operating-system failure and is stronger than calling `shutdown()` in one
Tokio task. **Loopback-only** protects against accidental remote exposure,
not malicious local processes or authenticated clients.
