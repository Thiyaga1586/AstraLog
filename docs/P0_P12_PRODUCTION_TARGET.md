# AstraLog P0-P12 Production Target — Execution Charter

Decision recorded 2026-10-31: deliver the **complete production-oriented P0-P12 specification**, not a subset optimized only for a resume demonstration.

This document is a status/navigation aid, **not** a replacement for `ASTRALOG_BIBLE.md`, `INVARIANTS.md`, `PHASE_PLAN.md`, the phase-specific frozen contracts, ADRs, checklists, or their exact-HEAD closure gates. Later-phase planning here does not grant implementation/activation authority before earlier gates pass.

## Current authorized boundary

- Active source: `D:\Workspace\AstraLog`, branch `main`.
- Last authoritative clean commit: `cd064a4fe60141590b6062957475aa15ee560717` (`P9-F exact-HEAD qualification: PASS`). P0-P8 and P9-A..F are closed by their own phase evidence.
- **P9-G P9-130..151**: active *uncommitted* qualification/closure work; `P9_G_WORK_IN_PROGRESS.md` now records two complete 730/730 workspace PASS runs, an independent P9-130..150 audit and the unresolved exact-HEAD release gate P9-151. Earlier intermittent P5/P6/controller failures remain part of the evidence.
- P9-H/I, P10, P11 and P12 remain **not authorized to start** until predecessor qualification.

## Gate-by-gate delivery sequence

| Gate | Required result before advancing | Evidence to retain |
| --- | --- | --- |
| P9-G | Every eligible P3/P5 voter establishes identity-fenced complete local clean progress; a quorum-committed minimum controls tombstone removal; grace/snapshot/producer authority remain intact; behind-safe voters rebootstrap through authenticated retained-history V2; failover never resurrects removed keys | Live RF3 multi-voter/joint membership, lagging voter, incarnation, tombstone and producer retry tests; V2 crash interruption matrix; historical compatibility and serialized workspace; clean exact-HEAD PASS note |
| P9-H | Finite cleaner concurrency, pending work, decode/index/output/disk staging budgets and deterministic yields; no Raft/P5/P6 control-plane starvation; conservative protected-floor interface | Pressure/overload, slow/offline voter, boundedness, starve/no-starve and restart tests; exact-HEAD gate |
| P9-I | Full P9 end-to-end fault, model, compatibility and golden-byte audits | P9-168..180, deterministic reproducibility, full workspace and final P9 exact-HEAD closure |
| P10 | At-least-once durable queue subscriptions with receive/ack/nack, fenced visibility leases, re-delivery, delayed retry, poison handling and idempotent DLQ transfer | Lease crash/failover, stale receipt, ambiguous ack/transfer, retry load, queue history/restart tests; frozen P10 contract and phase closure |
| P11 | Operable multi-broker cluster: launchable services/admin, safe drain and rebalance, TLS/mTLS/auth/ACLs, readiness, bounded metrics/traces/logs, feature-gated N/N+1 upgrades | Deployment/runbooks, authentication/rotation/ACL matrix, drain/rolling-upgrade drills, observable control and recovery proof |
| P12 | Production release correctness and measured performance, not merely a working demo | Multi-process/multi-host chaos/soak/disk pressure, acknowledged-record and idempotency oracles, raw reproducible benchmark provenance with latency percentiles and resource metrics, release build, runbooks and tagged exact SHA |

## Unchanged acceptance discipline

1. Preserve P0 frozen architecture: Rust, independent Raft controller quorum, one data Raft group per partition, direct-to-leader clients, broker-local segmented WAL, cooperative groups and two planned subscription modes.
2. Preserve P1 Storage V1 and P2 Wire V1 bytes; use versioned compatibility extensions where explicitly frozen (e.g., sparse retained Storage V2).
3. Never treat derived sidecars, volatile telemetry, benchmark estimates or controller liveness observations as quorum-committed authority. No false success, silent committed-data repair, or blind retry of ambiguous mutations.
4. Any failing full-suite regression is an open blocker until its root cause is addressed or independently qualified; an isolated passing rerun is not proof of determinism.
5. Full closure requires appropriate focused/adversarial integrations, complete serialized regression, formatting, warning-denied Clippy, doc and frozen-source compatibility checks; then clean exact-commit requalification and recorded PASS authority.
6. Preserve all unrelated working-tree state. No commit, push, merge or release without the user's explicit approval, and no paid API/model dependencies.
7. Never substitute a code path being implemented for a property being measured. Publication numbers must be generated from retained raw benchmark evidence with exact hardware, runtime, durability and configuration provenance.

## Immediate engineering order

1. Preserve the historical P6/P5/controller intermittent RED receipts; current P6-K test-only ambiguous RevokeAck recovery must prove the exact committed state without retrying the mutation.
2. P9-G P9-130..150 now have independently reviewed proof in live RF3/RF4 and interrupted V2 recovery, with both complete workspace repeats passing 730/730 and Clippy/docs/fmt/frozen compatibility green. Reconcile the reviewable closure candidate; even two passing runs do not erase intermittent historical failures.
3. Close P9-151 only via an authorized bounded commit plus exact-clean-HEAD qualification and PASS Git note. Completed proof items 130..150 may be checked with precise evidence; item 151 stays unchecked until the note is attached. Do not start P9-H/I or P10 production implementation before this predecessor gate passes.
