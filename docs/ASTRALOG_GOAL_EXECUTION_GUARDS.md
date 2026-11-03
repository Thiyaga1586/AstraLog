# AstraLog — Goal-driven engineering execution guards

Status: **PROCESS CONTRACT ONLY**. The end goal is the complete published P0–P12
specification, not a reduced resume demo. The current development gate is P9-G,
with P0–P8 and P9-A–F previously qualified. This file authorizes **no** release,
feature activation, checklist closure, staging, commit, or push.

## Mandatory check before every engineering action

1. **Confirm the exact repository and phase.** Read branch, HEAD, git status,
   docs/P0_P12_PRODUCTION_TARGET.md, docs/P9_CHECKLIST.md, and the latest
   active-phase evidence. Do not confuse a current-worktree test with an
   exact-HEAD release qualification.
2. **Name the phase requirement and threat model.** Identify the numbered
   acceptance item, observed failure or missing qualification, and the
   invariant the action must preserve. If the action belongs to a future phase
   or broadens the public contract, **stop**.
3. **Confirm file ownership and concurrent activity.** No simultaneous edits
   to the same source, no conflicting full-suite test runs, no wholesale
   formatting while another worker is editing, and no destruction of unrelated
   dirty/untracked content. Preserve unrelated changes verbatim.
4. **Verify before changing.** Inspect the actual source and persisted
   evidence; distinguish implementation bugs from test-harness races, real
   liveness failures, and unsupported inferences. For a defect, reproduce
   independently or state explicitly which proof remains unavailable.
5. **Keep the safety contract.** Do not weaken P3 Raft quorum/term/epoch or
   snapshot eligibility, P5 committed identity/assignment fencing, P6
   ambiguous-mutation semantics, P7 idempotent producer retries, P9 replicated
   safe-offset authority, immutable manifest hashing, or CRC/corruption
   rejection. Stale/absent progress is **not** positive authority. A returned
   None, timeout, or transient result is not success.
6. **Use the smallest necessary patch and adverse oracle.** Test the
   counterexample first, require an actual positive path and a fail-closed
   negative path, and include crash/restart where the requirement depends on
   durability. A skipped or zero-selected test is **not** test evidence.
7. **Qualify, then record.** Run focused offline locked tests, relevant
   P3/P5/P6/P7 compatibility, and the serialized workspace gates before
   reporting a qualified working-tree milestone. Clippy with warnings denied,
   formatting, documentation tests, and git diff --check are also required.
   Record exact command, count, output, failure and limitations in the
   active-phase ledger.
8. **Respect the release boundary.** No staging, commit, push, merge, deployment,
   paid API, later-phase feature implementation, destructive cleanup, or
   purported production release without separate user authorization and the
   appropriate phase evidence. Independently re-run gates on the exact future
   closure SHA if/when a commit is authorized.

If an automation or external worker runs without access to the connected
workspace, or another agent is actively changing the same subsystem, it must
**report the blocker and avoid competing edits**. An online laptop by itself
is not evidence that any unattended code changes were executed.

## Current gate scoreboard

| Gate | Evidence required | State |
| --- | --- | --- |
| P9-G local progress, RF3 safe-min and failover | Current manifest proof, all effective voters, durable safe control, successor write | Pre-commit PASS; phase OPEN |
| P9-G physical tombstone non-resurrection | Retain marker before quorum-safe floor; remove only after safe/grace/snapshot; restart and re-elect cleaner | Pre-commit PASS; phase OPEN |
| P9-G offline voter and membership | Offline-voter fence, exact broker incarnation, joint old/new union, pre/post-P5-finalize exclusion | Live RF4 positive/negative proof PASS 2/2, including actual P3 Joint authority; exact-HEAD release still OPEN |
| P9-G clock/grace boundary | Real segment-close clock rollback, elapsed-age overflow, strictly positive grace | Focused PASS 12/12; zero-grace RED reproduction then GREEN fix; broader matrix still required |
| P9-G interrupted producer-bearing V2 | Independent corruption, staged/atomic crash and idempotent producer recovery under P3 authority | Real child-process kill at three V2 publication barriers, recovery and idempotent suffix PASS; physical power-cut/torn fsync not claimed or required by P9-150 |
| P9-G independent exact-HEAD closure | Complete adverse matrix, full regression, independent review and authorized closure SHA | Independent checklist audit, two full serialized workspace runs 730/730 each, Clippy/docs/fmt/diff PASS; commit/exact-HEAD note NOT attempted |
| P9-H/I and P10–P12 | Numbered requirements in canonical plan and dependency order | **PENDING** |

## Resume claim fence

Describe only implemented and tested capabilities. Quantitative claims must
identify their actual command, hardware, and workload or be limited to verified
test counts. Do **not** claim production deployment, millions of messages per
second, tested horizontal scalability, final security qualification, or the
entire P0–P12 target while those gates remain open.
