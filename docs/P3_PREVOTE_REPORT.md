# P3 Pre-Vote Qualification Report

Checkpoint: **P3 pre-vote only**

Result: **PASS**

Qualification date: **2026-09-24**

This is a P3 checkpoint, not P3 phase closure.

## Delivered scope

The `astralog-raft` core now implements only the pre-vote behavior required by
`RAFT_V1.md` before a real election may exist:

- validated logical timing configuration
- injected election-timeout samples
- follower timeout -> `PreCandidate`
- `PreVoteRequest`
- `PreVoteResponse`
- checked prospective term `current_term + 1`
- deterministic request emission in sorted `NodeId` order
- self pre-vote for an eligible local voter
- local-voter eligibility check
- requester-voter eligibility check
- candidate log-freshness comparison
- snapshot-boundary log freshness
- recent valid current-term leader suppression
- stable-configuration majority counting
- joint old/new majority counting
- deterministic response tracking using ordered maps/sets
- duplicate/stale/foreign response fencing
- one-shot `PreVoteQuorumReached` notification
- deterministic pre-vote round retry after timeout

## Non-persistence proof

Starting, granting, rejecting, counting, and completing a pre-vote round do not:

- increment `current_term`
- change `voted_for`
- create/update hard state
- emit a persistence batch
- enter `Candidate`
- enter `Leader`

Tests snapshot `HardState` before pre-vote operations and prove it is unchanged.
Tests also prove the effects emitted by pre-vote contain no `Effect::Persist`.

The only persistence code reachable in `engine.rs` remains the generic qualified
foundation persistence-barrier API; it is not invoked by the pre-vote paths.

## Pre-vote grant rule qualified

A local voter grants a pre-vote only when all are true:

1. the local node is a voter;
2. the requester is a voter in the effective configuration;
3. the request's prospective term is greater than local `current_term`;
4. the requester log is at least as up to date as local durable history;
5. the node has not recently accepted valid current-term leader authority.

Log freshness uses `(last_log_term, last_log_index)` and falls back to the
snapshot `(last_included_term, last_included_index)` when the retained suffix is
empty.

## Recent-leader guard

Because AppendEntries is intentionally not implemented yet, the core exposes a
narrow P3 pre-vote liveness hook for already-validated leader contact.

The hook only accepts suppression when:

- leader identity is an effective voter;
- local term is non-zero; and
- supplied leader term exactly equals local current term.

A mismatched or synthetic term-0 contact cannot suppress pre-vote. A valid
contact resets election elapsed time, cancels an active pre-vote round, and
returns a pre-candidate to follower state.

This hook does not implement higher-term processing; that remains part of the
real-election/RequestVote checkpoint.

## Quorum behavior qualified

Stable configuration:

- strict majority of voters required
- learners never count
- each voter contributes at most once

Joint configuration:

- strict majority of old voters required; and
- independently, strict majority of new voters required

The voter union is emitted/processed in stable `NodeId` order. Duplicate
responses do not inflate quorum. Responses from another prospective term or a
non-voter are ignored.

## Timeout behavior qualified

The frozen timing constraints are enforced:

```text
heartbeat_interval > 0
election_timeout_min >= 5 * heartbeat_interval
election_timeout_max > election_timeout_min
```

Timeout samples are injected by the embedding runtime and checked against the
configured inclusive range. The core reads no wall clock and owns no RNG.

On a pre-candidate timeout, a fresh pre-vote round is constructed with a newly
injected timeout sample. The durable Raft term is unchanged.

## Adversarial/unit coverage

The `astralog-raft` suite now proves:

- timing-ratio validation
- invalid timeout sample rejection before state change
- deterministic timeout -> pre-candidate transition
- deterministic voter request ordering
- single-voter pre-vote quorum without real election
- stale-log rejection even when candidate index is larger
- snapshot-boundary log-freshness comparison
- prospective-term rejection
- non-voter requester rejection
- learner no-campaign/no-grant behavior
- recent valid leader suppression
- valid leader contact cancels an active round
- term-0/mismatched leader contact cannot suppress
- stable quorum counts each voter once
- joint quorum requires old and new majorities independently
- stale prospective-term responses ignored
- foreign/non-voter responses ignored
- quorum notification emitted once
- pre-candidate timeout starts a fresh round
- hard state remains unchanged by pre-vote
- pre-vote emits no persistence effect
- non-pre-vote algorithms remain disabled
- all previously qualified foundation invariants remain green

## Strict qualification evidence

Pinned Linux qualification environment: **Rust 1.98.1**.

- `cargo fmt --all -- --check`: **PASS**
- `cargo clippy --workspace --all-targets --all-features -- -D warnings`: **PASS**
- `cargo test --workspace --all-features`: **PASS**
- `astralog-raft`: **27 passed, 0 failed**
- frozen P1/P2 suite: **101 passed, 0 failed**
- workspace total: **128 passed, 0 failed**
- all doc-test targets: **PASS**
- `git diff --check`: **PASS**

The final workspace qualification ran with dependencies cached and `--offline`,
so the green result did not depend on network availability.

## Source/scope audit

Source audit of `crates/astralog-raft` found no implementation symbols for:

- `RequestVote`
- `AppendEntries`
- `TimeoutNow`
- candidate campaigning
- leader election transition
- replication prefix matching
- conflict-term backtracking

There is still no direct filesystem, socket, thread, wall-clock, Tokio, or RNG
dependency in `astralog-raft`.

P1 storage and P2 protocol/broker/client source files were not modified by this
checkpoint.

## Next checkpoint

The next deterministic P3 step is **RequestVote campaigning only**:

1. higher-term handling/fencing;
2. durable new-term + self-vote before outbound campaigning;
3. RequestVote request/response messages;
4. log-freshness reuse from the qualified pre-vote helper;
5. durable granted vote before VoteResponse;
6. one durable vote per term across restart;
7. deterministic stable/joint election vote tracking;
8. candidate state may record that election quorum has been reached, but it does
   not yet transition to leader.

Election victory, leader initialization/`LeaderBarrier`, AppendEntries, and
replication remain out of scope until later separately qualified checkpoints.
