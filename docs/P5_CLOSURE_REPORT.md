# AstraLog P5 Closure Report

Date: 2026-09-29

Status: **P5 CLOSED — FINAL CLOSURE QUALIFIED / P6 PLANNING NEXT**

## Scope

This report closes **P5 — Controller Quorum, Cluster Membership, Placement**. It covers final
actions P5-201 through P5-224 and binds the evidence from all prior P5 checkpoints into one
phase-level closure. It does **not** start P6. Consumer groups and offset coordination remain
unimplemented until a separate P6 contract/checklist is decomposed and frozen.

The authoritative P5 base is P4 closure `54aa8d5`; P5 began at contract-freeze commit
`ad26511` and ends at the commit containing this report. The exact closure SHA is therefore the
result of `git rev-parse HEAD` after this bounded closure commit is created. Post-commit
qualification is attached to that SHA as a Git note so the evidence can be recorded without
changing the closure commit itself.

## Qualified P5 history

- `ad26511` — freeze controller and placement contract V1
- `e4538fb` — deterministic controller state machine
- `b4b49cb` — controller persistence and snapshots
- `85a956c` — controller transport and election
- `8910b32` — durable controller cluster bootstrap
- `a8bfc51` — broker membership and incarnation fencing
- `2ed1690` — heartbeat and failure detection
- `60e8c43` — topic metadata and deterministic placement
- `eaed1b9` — metadata epochs/deltas and broker cache
- `c6aaec0` — dynamic P4 integration
- `e3264ac` — crash-resumable replica movement
- `24df48d` — active cluster feature level
- `38316c9` — producer-ID allocator
- `e19bbcc` — separately qualified P3/movement compatibility repair discovered by movement stress
- `3cb462a` — general leadership-transfer hooks
- `7b1b728` — P5 failure matrix qualification
- final P5-Q closure — the commit containing this report

## Phase-exit invariants

All P5 exit invariants are mechanically closed:

1. Controller Raft owns one committed/applied metadata history and `metadata_epoch` is its applied
   application-log index.
2. Broker authority is exact broker ID + node UUID + greatest committed incarnation + lifecycle;
   stale processes are fenced from heartbeats and internal replication traffic.
3. Replica movement creates a learner first, requires catch-up, and relies on P3 joint consensus
   before voter authority changes.
4. Required rack/zone policy never silently weakens; Preferred degradation is explicit.
5. Durable movement intent resumes idempotently after controller/source/destination failures.
6. Controller outage blocks new control-plane mutations but does not tear down already-running
   healthy P4 partitions.
7. Producer IDs are monotonic and never reused, even after lost responses/failover/restart.
8. Feature level changes only by explicit compatible committed mutation.
9. Commands, snapshots, metadata retention/responses, heartbeats, control channels, mutation
   queues, movement concurrency and live controller state all have finite bounds.
10. P1/P2/P3/P4 compatibility regressions remain green at closure.

## Cross-phase compatibility audit

### P1 storage

The `astralog-storage` tree hash at P4 closure and P5 closure is identical:

```text
5b7b2bd34dd6ecf981205e0768882dfd1e5094e6
```

No Storage V1 bytes changed during P5. The full storage suite remains 43/43 PASS.

### P2 wire protocol

P5 activates bounded bodies for already-reserved cluster/admin surfaces, but the frozen P2 golden
vector source is unchanged from P4 closure. All 11 immutable P2 goldens pass. Legacy brokers still
return `UnsupportedApiKey` for P5-only reserved operations when dynamic-controller mode is absent.

### P3 Raft

P5 does not redesign Raft. One separately bounded post-P5-O repair (`e19bbcc`) changed
`astralog-raft/src/engine.rs` after movement stress exposed two lower-layer defects:

- committed learner promotion now reconciles the local live role from Learner to Follower; and
- follower AppendEntries commit advancement clamps monotonically so a newly elected leader with
  stale `leader_commit` knowledge cannot regress an already-durable committed prefix.

That repair was independently qualified before P5-M and added two direct regressions. The current
Raft suite is 122/122 PASS.

### P4 replication

P5 replication-layer changes expose control/observation seams needed by dynamic topology and
leadership transfer. They do not redefine `acks=0/leader/quorum`, committed Fetch/ListOffsets,
leader-term fencing or durability ordering. The 46 replication tests remain green and the frozen
RF3 TCP quorum/redirect/epoch/failover E2E passes 5/5 at closure.

## Later-phase scope audit

No later-phase semantics were pulled forward:

- **P6:** no consumer-group coordinator, JoinGroup/SyncGroup, group membership state or offset
  coordinator exists.
- **P7:** P5 provides only producer-ID block allocation. There is no producer session, active
  producer epoch/sequence protocol, retry token, deduplication table or idempotent Produce path.
  The `producer_epoch` field already present in frozen P1 record bytes remains inactive format.
- **P8:** no throughput/latency benchmark claim is made; timing knobs in correctness tests are
  explicitly not performance qualification.
- **P9:** no data-retention or compaction policy is implemented.
- **P10:** no work-queue visibility/retry/DLQ semantics exist.
- **P11:** reserved DrainBroker/auth registry values have no runtime handler; there is no TLS,
  authentication/authorization/ACL plane or drain orchestration.

## Failure and model qualification

P5-P plus P5-Q exercise both deterministic and real-network failure boundaries:

- deterministic seeded controller state-machine churn and canonical trace replay;
- deterministic 64-partition placement under 128 candidate-order permutations;
- bounded movement model with snapshot/restart at every durable stage;
- controller leader kill, minority isolation and stale source fencing;
- crash-before-incarnation-commit and commit-before-lost-response incarnation boundaries;
- failure-detector reset across leader changes;
- controller-outage Metadata/admin behavior while the data plane continues;
- source/destination/controller failures through RF3 movement;
- controller leader death at every durable movement stage;
- sustained quorum writes during reassignment with all acknowledged values readable afterward;
- stale metadata snapshot/delta recovery;
- feature-level failover and incompatible participant rejection;
- producer-ID lost response/failover/restart non-reuse;
- bounded admin pressure while heartbeat and Raft control stay live.

P5-Q found one qualification-harness race: admin-pressure heartbeat setup could initialize the
volatile failure-detector clock on a controller that lost and later regained leadership. Because
failure-detector state intentionally resets on every leadership transition, the test now reacquires
the current leader and initializes that leader-local clock immediately before heartbeat
observation. The hardened test passes 5/5. Production failure-detector semantics were not changed.

The first exact-HEAD workspace rerun also exposed a Windows bind-then-release port collision in the
existing replication peer-transport test harness (`os error 10048`). The test-only address allocator
now uses process-salted loopback IPs, matching the already-qualified controller/broker harness
strategy. Production peer transport and P4 acknowledgement/read semantics are unchanged.

## Resource bounds

P5-P closed the last controller-memory gap by aligning live replicated-state admission with the
frozen Snapshot V1 envelope. Canonical limits remain:

- brokers: 100,000;
- topics: 100,000;
- assignments: 1,000,000;
- movements: 100,000;
- controller snapshot / snapshot transport package: 64 MiB.

Growth is rejected before state mutation if projected worst-case canonical snapshot bytes would
cross the existing envelope. Independent count/byte limits remain for command decoding, heartbeat
progress arrays, metadata delta retention/responses, pending mutations, control requests,
transport queues, active movements and broker admin work.

## Closure qualification matrix

Pre-commit P5-Q qualification on the closure bytes:

- `cargo fmt --all -- --check`: PASS
- workspace Clippy `-D warnings`: PASS
- full workspace: **380 passed, 0 failed**
- controller: **91/91**
- deterministic P5 failure/model module: **5/5**
- broker library: **13/13**
- P2 broker network: **9/9**
- P5 dynamic multi-process integration: **4/4**
- client: **3/3**
- common: **2/2**
- protocol: **35/35**
- frozen protocol goldens: **11/11**
- Raft: **122/122**
- replication: **46/46**
- storage: **43/43**
- frozen P4 RF3 E2E: **5/5** in the dedicated closure campaign
- hardened bounded-admin-pressure scenario: **5/5**

The same closure HEAD is requalified after commit. P5 is not considered closed unless that exact
SHA passes and a Git note recording the result is attached to the closure commit.

## Correctness debt audit

`TODO|FIXME|XXX|HACK` search over controller, broker and project documentation finds no unresolved
P5 correctness implementation marker. The only matches are engineering rules/checklist prose that
state the no-correctness-TODO policy itself.

## Closure boundary

P5 is closed only after the post-commit exact-HEAD gate succeeds. P6 remains disabled here. The
next allowed activity is **P6 planning/decomposition only**: read the frozen project architecture,
split consumer groups/offset coordination into bounded checkpoints, define entry/exit invariants,
and freeze a P6 contract/checklist before any P6 implementation begins.
