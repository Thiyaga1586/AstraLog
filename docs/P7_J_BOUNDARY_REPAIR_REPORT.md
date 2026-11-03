# AstraLog P7-J Exact-HEAD Boundary Repair Qualification

Date: 2026-10-04

Status: **PASS — BOUNDED P7 SAFETY REPAIR QUALIFIED / EXACT REPAIR HEAD REQUALIFICATION NEXT**

## Why this repair exists

The first exact-HEAD requalification attempt after P7-J exposed a narrow RF3 leadership boundary
that the pre-commit P7-J campaign had not forced reliably enough.

A newly elected partition leader can be locally `Role::Leader` and have the expected leader term
before its current-term `LeaderBarrier` is durably committed. Under the frozen P3 current-term
commit rule, prior-term log entries become committed on the new leader only through prefix
implication once a current-term entry commits. P7 duplicate suppression, however, must make its
decision from committed producer history only.

Therefore P7 idempotent admission now requires all of the already-frozen leader/term checks plus a
durably committed entry from the leader's current term. Until that point Produce V1 returns bounded
transient `BUSY`; it does not allocate a sequence, create a proposal, mutate producer authority or
reinterpret controller authority.

This is a safety repair inside the existing P3/P7 contract, not a new architecture choice.

## Production change

`astralog-replication::ReplicatedPartition` now has a bounded leader-readiness predicate used only
by idempotent Produce admission:

- the local replica must be P3 leader;
- durable commit index must be non-zero;
- the entry at the durable commit boundary (or the snapshot boundary when equal) must belong to the
  current Raft term;
- otherwise idempotent admission returns `Busy("current-term leader barrier is not durably committed yet")`.

All existing P7 admission checks remain in force after this gate: exact leader epoch, quorum ack,
canonical producer identity/sequence, controller evidence when required, pending-range bounds and
duplicate detection from committed history.

No P1 bytes, P2 wire bytes, controller producer authority, P6 group/offset authority, client retry
identity or snapshot format changed.

## RF3 qualification hardening

The P7 failover harness now treats only `BROKER_BUSY`, `NOT_LEADER` and `STALE_EPOCH` as transient
while waiting for a successor to become safe for idempotent admission. Terminal producer errors are
still returned immediately. This keeps the RF3 scenarios aligned with the new readiness boundary
without hiding fencing or sequence failures.

The repaired RF3 campaign proves the same frozen outcomes after the gate:

- lost response after quorum commit returns the original committed range;
- leader death after quorum commit deduplicates on the successor;
- leader death before persistence accepts the sequence once;
- locally durable but uncommitted work is repaired and retry commits one copy;
- explicit leadership transfer preserves retry identity;
- tentative higher epoch is not inherited after failover;
- committed higher epoch fences the old epoch after failover;
- snapshot/restart reconstructs the exact retained duplicate decision;
- six seeded repeated failovers preserve unique committed producer sequence ranges.

## P6 test-harness stabilization

Whole-workspace serialization also exposed older P6 harness timing/ambiguity boundaries. The P6
changes in this repair are test-only and do not alter broker, controller, group or offset production
semantics:

- setup mutation duplicate/stale-CAS outcomes are accepted only when a fresh committed controller
  snapshot proves the exact requested broker setup effect;
- group/owner fence commits use the already-existing idempotent bounded retry helpers across
  transient offsets-leader changes;
- an ambiguous static bootstrap JoinSubscribe is resolved only from committed group history rather
  than blindly retried;
- the long churn/timeout/expansion fixture uses a 20-second request deadline for its own clients so
  host scheduling pressure does not manufacture a 5-second transport ambiguity before a dynamic
  member session is locally established;
- each serialized P6 RF3 fixture gives the previous Windows Tokio/socket/file-handle teardown a
  250 ms bounded drain interval before allocating the next process-salted loopback topology.

All authoritative P6 assertions and deadlines remain. No ambiguous mutation is declared successful
without committed-state proof.

## Mechanical qualification on the finished repair tree

- P7-I + P7-J RF3/controller failover suite, serialized: **12/12 PASS**
- replication complete suite: **82/82 PASS**
- P7 runtime integration: **2/2 PASS**
- P6 RF3/integration, serialized after final harness stabilization: **9/9 PASS**
- broker unit tests inside the complete workspace: **22/22 PASS**
- P2 network compatibility: **9/9 PASS**
- P4 RF3 compatibility: **1/1 PASS**
- P5 dynamic compatibility: **4/4 PASS**
- client: **12/12 PASS**
- common: **3/3 PASS**
- controller: **106/106 PASS**
- groups: **55/55 PASS**
- protocol: **44/44 PASS**
- P2/P6/P7 golden suites: **11/11 + 6/6 + 4/4 PASS**
- Raft: **122/122 PASS**
- storage: **44/44 PASS**
- complete serialized all-target workspace: **548/548 PASS**
- workspace doc tests: **PASS**
- workspace Clippy `--all-targets --all-features -D warnings`: **PASS**
- `cargo fmt --all -- --check`: **PASS**
- `git diff --check`: **PASS**

## Boundary

This repair must be committed as its own bounded checkpoint and then requalified on that exact clean
HEAD. Only after the exact repair HEAD repeats the focused P7/P6 safety gates, complete serialized
workspace, doc tests, Clippy, formatting and diff hygiene may execution advance to:

> **P7-K only — P7-148 through P7-155: compatibility audit and exact P7 closure**

P8 and later remain disabled until P7-K closes P7.
