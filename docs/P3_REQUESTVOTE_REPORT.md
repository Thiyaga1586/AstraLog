# P3 RequestVote Qualification Report

Checkpoint: **P3 RequestVote campaigning only**

Result: **PASS**

Qualification date: **2026-09-24**

This is a P3 checkpoint, not P3 phase closure.

## Delivered scope

The qualified RequestVote layer adds only the real-election campaigning behavior
required after pre-vote and before leader initialization:

- explicit `BeginElection { prospective_term }` input
- validation that real campaigning follows a matching successful pre-vote round
- checked new term `current_term + 1`
- durable self-vote before any outbound RequestVote
- `RequestVoteRequest`
- `RequestVoteResponse`
- deterministic candidate request emission in sorted `NodeId` order
- RequestVote log-freshness reuse from the qualified pre-vote comparator
- same-term durable `voted_for` fencing
- repeated same-candidate vote acknowledgement from durable hard state
- higher-term hard-state transition with old vote cleared or new vote recorded
- durable positive vote before VoteResponse
- durable higher-term fencing before candidate step-down becomes externally visible
- deterministic stable and joint election-vote tracking
- one-shot `ElectionQuorumReached` notification

## Persistence-before-message proof

Starting a real campaign does not immediately modify the durable hard state or
send RequestVote.

The core first emits exactly one hard-state persistence batch containing:

```text
current_term = prospective_term
voted_for    = self
```

Only after the matching `PersistenceCompleted(token)` input does the core:

1. publish the new hard state;
2. enter `Candidate`;
3. count its self-vote;
4. emit RequestVote to the remaining voters.

The same ordering is enforced for vote grants. A newly granted vote is not sent
as `vote_granted=true` until `voted_for=candidate` is durable.

## One-vote-per-term crash safety

The checkpoint proves both sides of the crash boundary:

- crash **before** vote persistence: no positive response was emitted, so the
  restarted node may legally vote for another candidate;
- restart **after** a durable vote: `voted_for` is recovered and a different
  candidate in the same term is rejected.

Repeated RequestVote from the same candidate in the same term may be granted
again without another persistence operation because the original vote is
already durable.

## Higher-term fencing

For an eligible higher-term RequestVote request, the new term is persisted
before any response using one hard-state update. If the candidate is eligible
and sufficiently up to date, the new term and granted vote are persisted
atomically in that update.

A higher-term stale candidate still forces the legitimate term advance, but its
rejection is delayed until that term is durable.

A candidate receiving a higher-term RequestVote response similarly stages a
hard-state update clearing `voted_for`; only after durability does it expose the
follower/learner step-down.

Non-voter/foreign identities cannot raise or fence the group term in this core
checkpoint.

## Learner behavior

Learners remain non-voting:

- they never campaign;
- they never grant RequestVote;
- they never count toward election quorum.

An eligible voter message with a higher term still causes a learner to persist
the newer term before returning a negative vote response.

## Election quorum behavior

Stable configuration:

- strict voter majority required;
- each voter contributes at most once.

Joint configuration:

- strict majority of old voters required; and
- independently, strict majority of new voters required.

Duplicate/stale/foreign RequestVote responses cannot inflate quorum.

Reaching quorum emits exactly one:

```text
ElectionQuorumReached { term }
```

and **does not**:

- transition to `Leader`;
- initialize leader replication progress;
- append `LeaderBarrier`;
- emit AppendEntries;
- advance commit index.

Those actions belong to later separately qualified checkpoints.

## Persistence barrier ownership

While a correctness-critical hard-state persistence action is outstanding, the
core rejects new consensus-driving inputs with `PersistenceAlreadyInFlight`.
This prevents a second vote/campaign decision from racing the first durability
frontier.

The generic persistence-staging helper exists only in test builds. Production
embedding/broker code cannot manually stage hard state and bypass the core's
continuation ordering.

Persistence failure during campaign/vote hard-state update is fatal/non-serving
and never produces the dependent RequestVote or positive VoteResponse.

## Adversarial/unit coverage

The `astralog-raft` suite now proves, in addition to all earlier foundation and
pre-vote invariants:

- real campaign rejected without matching pre-vote quorum
- wrong prospective campaign term rejected
- self-vote persistence before outbound RequestVote
- persistence barrier blocks concurrent consensus input
- single-voter election quorum without leader transition
- positive vote persistence before response
- repeated same-candidate vote from durable state
- one durable vote per term across restart
- crash-before-vote-fsync produces no acknowledged vote
- higher-term stale candidate rejection after durable term fence
- higher-term eligible candidate persists term+vote atomically
- higher-term vote response fences candidate durably
- learner higher-term update without vote grant
- foreign request cannot raise term
- foreign higher-term response cannot fence candidate
- stable election quorum emitted once
- joint election quorum requires independent old/new majorities
- campaign hard-state persistence failure emits no RequestVote
- election quorum leaves role at `Candidate`

## Strict qualification evidence

Pinned Linux qualification environment: **Rust 1.98.1**.

- `cargo fmt --all -- --check`: **PASS**
- `cargo clippy --workspace --all-targets --all-features -- -D warnings`: **PASS**
- `cargo test --workspace --all-features`: **PASS**
- `astralog-raft`: **42 passed, 0 failed**
- frozen P1/P2 suite: **101 passed, 0 failed**
- workspace total: **143 passed, 0 failed**
- all doc-test targets: **PASS**
- `git diff --check`: **PASS**

The final qualification runs with dependencies cached and `--offline` in the
pinned Rust quality container.

## Source/scope audit

No production source path in this checkpoint performs:

- `Candidate -> Leader`
- leader replication-progress initialization
- current-term `LeaderBarrier` append
- AppendEntries / heartbeat emission
- follower log matching or truncation
- conflict-term/index backtracking
- quorum commit advancement

Existing `Role::Leader` and `EntryKind::LeaderBarrier` types remain frozen
foundation representations only. Their appearances in the RequestVote diff are
limited to assertions/test log fixtures, not production transitions.

P1 storage and P2 protocol/broker/client source files are unchanged.

## Next checkpoint

The next deterministic P3 step is **election victory + leader initialization
only**:

1. consume a qualified election-quorum state;
2. legally transition `Candidate -> Leader` for the same current term;
3. initialize deterministic per-peer `next_index` / `match_index` state;
4. clear transfer/old campaign state;
5. stage the local current-term `LeaderBarrier` through the persistence
   boundary;
6. qualify that the barrier is locally durable before it can count as self
   replication progress.

AppendEntries, heartbeat replication, follower append processing, conflict
optimization, and quorum commit advancement remain out of scope until later
checkpoints.
