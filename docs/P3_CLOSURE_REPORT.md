# P3 — Raft Core Closure Report

Status: **CLOSED / QUALIFIED**

Qualification date: **2026-09-24**

Frozen contract: `docs/RAFT_V1.md`

P3 baseline before the final closure campaign:

```text
2765309  P3 qualify AppendEntries heartbeat prefix validation
3008877  P3 qualify leader initialization
8f36820  P3 qualify RequestVote campaigning
3321435  P3 qualify pre-vote state machine
b5ec47a  P3 add qualified raft foundation state model
de0e93d  P3 freeze raft core safety contract v1
```

The final P3 closure commit is the commit containing this report.

## 1. Closure statement

P3 implements and qualifies the AstraLog Raft consensus core independently from
broker, transport, and partition-storage integration.

The closed P3 core now owns:

- deterministic follower / pre-candidate / candidate / leader / learner roles;
- pre-vote and real RequestVote elections;
- persistence-before-message term/vote ordering;
- legal election victory and leader initialization;
- current-term leader barrier;
- AppendEntries heartbeat and non-empty replication;
- prefix matching and conflict-term/index optimization;
- committed-prefix protection;
- follower durable acknowledgement;
- leader peer progress and deterministic replication scheduling;
- stable and joint replication quorums;
- current-term-only leader commit advancement;
- durable commit watermark before apply/visibility;
- ordered application;
- check-quorum leader step-down;
- bounded uncommitted application data;
- learner replication and promotion validation;
- joint-consensus membership transitions;
- membership reconstruction on recovery;
- snapshot creation and InstallSnapshot;
- snapshot-versus-log catch-up selection;
- leadership transfer through legal election;
- deterministic simulation and failure injection;
- bounded mechanically checked formal safety model.

P3 does **not** own broker transport, partition/P1 storage adaptation, producer
quorum acknowledgement plumbing, or replicated partition high-watermark
integration. Those begin in P4.

## 2. Deterministic core boundary

`astralog-raft` remains a pure state/effect core:

- no direct filesystem access;
- no sockets;
- no wall-clock reads;
- no sleeping/threads;
- no hidden RNG;
- deterministic ordered peer containers/iteration;
- all persistence goes through explicit `PersistenceBatch` effects;
- all network actions are explicit `Send` effects;
- all time enters through logical `Tick` input;
- election timeout samples are injected by the runtime/simulator.

The crate has no dependency on broker/client/storage/network runtime code for
consensus safety.

## 3. Persistence safety

The core enforces one ordered persistence barrier per group.

Correctness-dependent external actions are delayed until persistence completion,
including:

- new election term + self-vote before RequestVote;
- newly granted vote before positive VoteResponse;
- higher-term fencing before dependent responses/role exposure;
- leader barrier before self durable match advancement;
- follower truncate/append before successful AppendEntries response;
- commit watermark before authoritative `commit_index` / Apply;
- local snapshot publication before changing the snapshot boundary;
- InstallSnapshot success before acknowledgement/state-machine installation.

Persistence failure is fatal/non-serving rather than converted into success.

## 4. Elections and term fencing

Qualified properties include:

- pre-vote never changes durable term/vote state;
- recent valid leader contact suppresses disruptive pre-vote;
- learners never campaign or vote;
- RequestVote uses the frozen last-term/last-index freshness rule;
- one durable vote per term survives restart;
- a crash before vote persistence creates no acknowledged vote;
- higher-term valid messages durably fence stale candidate/leader state;
- foreign/non-member identities cannot manufacture authority;
- stable elections require a strict voter majority;
- joint elections independently require old and new majorities;
- legal leadership requires a matching qualified current-term election quorum.

## 5. Replication and log matching

Follower AppendEntries processing follows the frozen order:

1. validate term/leader authority;
2. validate the snapshot/live-log prefix;
3. validate incoming entry contiguity;
4. retain identical `(index, term, payload/kind)` entries;
5. reject/fail rather than truncate at or below committed history;
6. truncate only conflicting uncommitted suffix;
7. append replacement/new suffix;
8. persist the required truncate/append operations;
9. return success only after the resulting match index is durable.

Follower conflict hints are deterministic:

- missing history -> first locally missing index;
- retained term mismatch -> conflict term + first retained index of that term;
- snapshot boundary mismatch -> snapshot term/index;
- below retained snapshot floor -> snapshot boundary + 1.

Leader response handling consumes these hints to skip conflicting terms, keeps
`match_index` monotonic, updates `next_index` deterministically, and switches to
snapshot catch-up when required history is no longer retained.

Duplicate/reordered AppendEntries is idempotent with respect to durable history.

## 6. Commit and apply

Stable configuration commit requires a strict majority of voters with durable
`match_index >= N`.

Joint configuration commit requires both:

- majority of old voters; and
- majority of new voters.

Learners never contribute to commit quorum.

Leader quorum-based advancement chooses only an `N` whose log term equals the
leader's current term. Prior-term entries become committed only through prefix
implication when a current-term entry is committed.

Commit advancement is persisted before `commit_index` becomes authoritative.
Apply effects contain only entries in strictly increasing order above
`last_applied` and at/below `commit_index`.

Recovery restores the durable commit boundary and re-emits committed retained
entries requiring application replay.

## 7. Leader liveness and bounded proposals

Check-quorum uses recent voter responses over logical election-timeout windows.

- stable configurations require one recent majority;
- joint configurations require recent majorities of both old and new voters;
- loss of quorum steps the leader down;
- Raft safety does not depend on timer accuracy.

Only leaders accept proposals. Followers/candidates/learners do not silently
forward them.

Application payloads are bounded by configurable uncommitted application bytes.
Control traffic, membership traffic, elections, and snapshot traffic are not
blocked by that application-data bound.

## 8. Learners and membership

Learners:

- replicate durable log state;
- receive snapshots;
- report progress;
- never vote;
- never campaign;
- never count toward election or commit quorum.

A learner may be promoted only when leader progress proves it is caught up
through the current committed index.

Membership changes use explicit log entries:

```text
EnterJoint { old, new }
LeaveJoint { new }
```

`EnterJoint` becomes the effective quorum configuration as soon as the entry is
durably stored. `LeaveJoint` is accepted only after the joint entry is committed
and becomes the new stable configuration when durably stored. Only one unresolved
configuration change may exist at once.

If the committed final stable configuration removes the leader, that node steps
down and stops accepting proposals as leader.

Recovery reconstructs effective membership and pending-transition state from the
snapshot's stable configuration plus retained durable configuration entries.

## 9. Snapshots

Local snapshot creation is allowed only when:

- requested index is committed;
- requested index is applied;
- its term is known;
- effective membership is stable/non-joint;
- there is no unresolved membership transition.

Snapshot build completion is validated against the requested index/term and the
stable configuration at that boundary. Snapshot metadata is persisted before the
new snapshot boundary is published.

InstallSnapshot behavior includes:

- stale snapshot acknowledge/ignore without moving backward;
- committed-history incompatibility rejection;
- matching-boundary suffix retention;
- conflicting suffix discard;
- snapshot/log/commit durability before success;
- state-machine snapshot installation before subsequent suffix apply;
- monotonic commit/applied boundaries.

Leader catch-up switches to snapshot mode once peer progress falls below retained
Raft history and returns to ordinary AppendEntries after snapshot acknowledgement.

## 10. Leadership transfer

Leadership transfer enforces:

- target is an eligible voter;
- target is not self;
- one active transfer at a time;
- target is caught up through leader durable tail before `TimeoutNow`;
- new application/configuration proposals are rejected while transfer is active;
- `TimeoutNow` begins a real higher-term election with durable self-vote;
- no direct leader assignment exists;
- failed/timed-out transfer restores ordinary proposal service if the old leader
  still satisfies leadership/check-quorum conditions.

## 11. Deterministic simulator

`simulation.rs` supplies:

- seeded deterministic RNG;
- logical clock/ticks;
- stable NodeId scheduling;
- explicit network queue;
- packet drop;
- packet duplication;
- packet delay;
- packet reordering;
- crash/restart from durable state only;
- persistence hold-before-apply;
- persistence hold-after-apply-before-completion;
- persistence failure injection;
- bounded trace capture containing the seed and deterministic event sequence.

The same seed/campaign produces the same trace.

All mandatory scenarios from `RAFT_V1.md` are explicitly represented and pass:

1. single-node election and commit;
2. three-node normal election;
3. split vote and retry;
4. pre-vote stale-isolate protection;
5. durable vote across restart;
6. stale-log candidate rejection;
7. current-term replication and commit;
8. indirect prior-term commit;
9. conflict-term skip;
10. committed-prefix truncation protection;
11. minority cannot commit;
12. stale leader loses service after quorum loss;
13. crash after local append before follower quorum;
14. crash after follower durable append before response;
15. crash after durable quorum/commit persistence boundary;
16. duplicate/reordered AppendEntries;
17. learner catch-up without voting;
18. learner promotion only after catch-up;
19. independent old/new joint majority;
20. leader self-removal;
21. matching-boundary snapshot suffix retention;
22. conflicting snapshot suffix discard;
23. snapshot crash boundaries;
24. leadership transfer through legal election;
25. failed transfer resumes ordinary service.

Large campaign qualification executes **64 deterministic seeds × 300 steps** and
checks safety after every generated action.

## 12. Mechanically checked formal model

P3 uses the `formal_model.rs` bounded state-space checker as the explicitly
allowed "TLA+ (or equivalent)" mechanism from `RAFT_V1.md`.

The model contains:

- node terms;
- durable votes;
- roles;
- indexed `(term, value-id)` logs;
- commit indexes;
- in-flight messages;
- crash/restart state;
- stable voter configuration;
- joint old/new voter configuration;
- learner membership.

It explores deterministic bounded successors including elections, vote message
loss/duplication/reordering, appends, commits, and crash/restart. The main search
uses depth 7 and a 40,000-expansion bound, with separate exhaustive/specialized
checks for joint quorum intersection, learner/minority quorum exclusion, durable
vote immutability within a term, higher-term fencing, crash durability, and
learner promotion/joint transition.

The model/tests cover the frozen safety obligations R-01 through R-07 plus R-09,
R-10, and R-13 directly or through stronger equivalent committed-prefix checks.
R-08 follower durable acknowledgement, R-11 learner non-authority, and R-12
snapshot preservation are additionally enforced by production-core adversarial
tests and deterministic simulation.

## 13. Scope audit

The P3 closure diff from `2765309` touches only:

- `crates/astralog-raft/...`
- P3/project documentation.

No P1/P2 implementation source is modified.

`astralog-raft` has no broker/client/storage runtime dependency and contains no
direct disk/network/clock/RNG/thread APIs.

P3 does not pull forward:

- broker-to-broker transport;
- per-partition P1 adapter implementation;
- replicated partition runtime;
- controller placement policy;
- consumer groups/offsets;
- producer idempotency;
- compression/performance work;
- retention/compaction;
- queue semantics.

## 14. Final qualification evidence

Pinned environment: **Rust 1.98.1**, Linux container.

- `cargo fmt --all -- --check`: **PASS**
- `cargo clippy --workspace --all-targets --all-features --offline -- -D warnings`: **PASS**
- `cargo test --workspace --all-features --offline`: **PASS**
- `astralog-raft`: **120 passed, 0 failed**
- frozen P1/P2 suite: **101 passed, 0 failed**
- workspace total: **221 passed, 0 failed**
- all doc-test targets: **PASS**
- deterministic scenarios 1–25: **PASS**
- 64 × 300-step seeded campaigns: **PASS**
- bounded formal checker: **PASS**
- `git diff --check`: **PASS**

The final committed HEAD is requalified with the same strict gate before P3 is
declared mechanically closed.

## 15. P4 handoff

P4 may consume P3 through its deterministic input/effect and persistence/log
abstractions. P4 must not move consensus safety decisions into broker code.

P4 owns:

- P1 storage adapter for Raft hard/log/commit/snapshot operations;
- broker-pair Raft RPC transport and multiplexing;
- RF 1/3/5 replicated partitions;
- leader routing/redirect integration;
- producer `acks=quorum` integration;
- Raft commit -> partition committed/high-watermark visibility mapping;
- replica catch-up/rebootstrap integration.

Any change to P3 consensus semantics requires an explicit compatibility/safety
review against `RAFT_V1.md`; P4 integration inconvenience is not sufficient
reason to silently change the closed core contract.
