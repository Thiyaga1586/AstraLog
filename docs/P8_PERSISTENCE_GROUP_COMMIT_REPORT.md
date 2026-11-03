# AstraLog P8-E Persistence Group Commit Qualification Report

Date: 2026-10-07

Status: **P8-E CLOSURE CANDIDATE — EXACT-HEAD PASS NOTE REQUIRED FOR AUTHORITY**

## Scope

P8-E closes actions **P8-074 through P8-090** only.

The implementation is governed by Persistence Frontier V2, frozen and exact-HEAD qualified at:

    370ae9285be0de4a3b735dac7bacacf0563d10c6

P8-F and later checkpoints remain disabled.

## V2 Raft persistence frontier

Historical queue-depth-one behavior remains the compatibility mode.

When explicitly enabled, Raft maintains a finite ordered queue of append-only persistence tokens.
Queued application entries form a non-authoritative staged logical suffix used only to assign and
validate subsequent append positions. Durable stable_log remains the sole durability authority.

Staged entries do not advance self-match, durable commit, leader acknowledgement or quorum
authority. Persistence completion is accepted only for the exact queue head.

Queue admission is bounded by both token count and logical bytes. Exact count/byte one-over
conditions fail before further staging.

Only ordinary application append tokens may coexist. Configuration/leader-barrier entries,
hard-state work, commit persistence, follower conflict/truncation, snapshot/install,
discard/rebootstrap and durability-dependent control transitions remain barriers.

## Physical group commit

Partition persistence now separates append staging from the physical data flush. The replication
runtime owns a bounded per-partition group containing the original P3 tokens, staged logical byte
count and injected-monotonic opening time.

The group is flushed at the earliest of:

- token-count boundary;
- byte boundary;
- maximum group-delay deadline;
- explicit flush;
- non-Propose/control input;
- snapshot/control barrier.

After one successful physical sync, the runtime feeds PersistenceCompleted for the exact eligible
tokens back to Raft in FIFO order. It never synthesizes a combined token.

## Barrier proofs

Focused qualification proves:

- grouped execution is logically identical to sequential persistence;
- only application append batches are groupable;
- AdvanceCommit cannot move past an unsynced append group;
- a snapshot request drains the open append group before the snapshot barrier executes;
- suffix truncation remains follower/non-groupable and all existing P3/P4 truncation proofs pass;
- hard-state, InstallSnapshot, DiscardPrefix and mixed Append+AdvanceCommit batches are non-groupable.

## Failure and crash semantics

Staging failure or ambiguous physical-sync failure never fabricates success. The first unproven
token is failed into P3, the replica becomes fatal/non-serving, and later unproven tokens are not
reported complete.

Crash qualification covers both:

- crash after staged append before sync: restart exposes no unjustified committed data;
- crash after successful sync before completion delivery: durable log bytes are recovered while
  response ambiguity remains governed by existing P4/P7 retry semantics.

## Ack semantics

Leader acknowledgement remains pending until the grouped sync proves local durability.

Quorum acknowledgement remains P3 majority-commit authority. A single-replica quorum proposal
does not advance durable commit while its group is unsynced; after the group flush, it becomes
quorum committed and durable commit advances to its exact Raft index.

No quorum mathematics or follower acknowledgement rule changed.

## Liveness and bounds

P8-E introduces finite limits for staged token count, staged logical bytes and maximum group delay.
Broker startup validates that the replication tick interval does not exceed the configured group
deadline, preserving deterministic deadline progress through the existing injected monotonic clock.

A sustained 64-append pressure campaign proves control ticks remain live while physical sync count
stays positive and lower than logical-token count.

## Compression and recovery compatibility

Mixed None/LZ4/Zstd grouped history preserves logical order and stream offsets. Grouped and
sequential execution recover identical state, Storage frames and next stream offset.

Segment roll syncs are counted as real physical syncs rather than hidden from the metric.
No Storage V1 or protocol golden bytes changed.

## Deterministic sync counters

P8-E exposes deterministic counters for logical persistence tokens and actual physical data syncs.
They are qualification instrumentation only and are not benchmark claims.

Proven examples include:

- two compatible tokens -> one physical sync;
- three grouped tokens -> one sync versus three in depth-one sequential mode;
- segment roll contributes its actual sync;
- sustained pressure amortizes physical syncs below logical-token count.

## Qualification evidence

Focused P8-E:

- Raft V2 queue: **3/3 PASS**;
- V2 bounded formal model: **2/2 PASS**;
- replication group-commit campaign: **12/12 PASS**.

Complete core suites:

- broker: **23/23 PASS**;
- client: **22/22 PASS**;
- common: **3/3 PASS**;
- controller: **113/113 PASS**;
- groups: **55/55 PASS**;
- protocol: **45/45 PASS**;
- Raft: **127/127 PASS**;
- replication: **96/96 PASS**;
- storage: **53/53 PASS**.

Frozen integration/compatibility:

- P2 network: **9/9 PASS**;
- P4 RF3: **1/1 PASS**;
- P5/P8 dynamic: **5/5 PASS**;
- P7 runtime: **2/2 PASS**;
- P6 RF3/group/offset/fencing/churn/public integration: **9/9 PASS**;
- P7 failover: **12/12 PASS**;
- original P2/P6/P7 goldens: **11/11 + 6/6 + 4/4 PASS**.

Bounded workspace total:

    575 fast/core + 9 P6 + 12 P7 failover = 596/596 PASS

One pre-commit P7 runtime run hit a controller-leadership ambiguity before producer assertions.
No P8-E source changed. The exact failed case immediately passed **1/1** unchanged and the complete
P7 runtime file then passed **2/2** unchanged. That transient run is not qualification authority.

Mechanical/frozen-byte gate:

- cargo fmt --all -- --check: **PASS**;
- workspace Clippy with -D warnings: **PASS**;
- workspace doc tests: **PASS**;
- git diff --check: **PASS**;
- Cargo manifests/lockfile unchanged from the V2 re-freeze;
- protocol P2/P6/P7 golden sources unchanged;
- all Storage golden fixtures unchanged.

## Closure boundary

P8-074 through P8-090 are mechanically satisfied on this closure candidate.

The first closure commit, `ff4d0b4`, was **not** granted authority: its exact-HEAD qualification
hit the P6 churn integration case after the P8-E implementation had already been committed.
The failure occurred in the existing JoinSubscribe post-commit completion path, where a group
mutation could already be quorum committed but a later fencing/activation/tracking failure was
surfaced as a fresh terminal mutation outcome.

The bounded compatibility repair preserves the frozen P6 ambiguity rule: once committed group
authority exists, a failure while finishing the prepared epoch or tracking the joined member is
reported as RequestTimeout together with the committed group/member observation so the caller
reconciles from committed history instead of blindly replaying into GroupEpochStale.

On the repaired tree:

- the exact previously failing churn case: **1/1 PASS**;
- the complete serialized P6 integration suite: **9/9 PASS**.

`ff4d0b4` therefore remains historical closure-candidate evidence only. The subsequent bounded
repair commit must re-pass the complete exact-HEAD **596/596** qualification and mechanical/frozen
byte gate before P8-E can become authoritative.

P8-E becomes authoritative only after the exact closure commit is clean, re-passes the same
bounded **596/596** qualification plus mechanical/frozen-byte gate, and carries the exact-HEAD
P8-E PASS Git note.

Only then may **P8-F — P8-091 through P8-108** begin. P8-G and later remain disabled.
