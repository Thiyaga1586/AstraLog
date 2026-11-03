# P3 Leader Initialization Qualification Report

Checkpoint: **P3 election victory + leader initialization only**

Result: **PASS**

Qualification date: **2026-09-24**

This is a P3 checkpoint, not P3 phase closure.

## Delivered scope

This checkpoint implements the narrow transition that was deliberately excluded
from the RequestVote checkpoint:

- explicit `WinElection { term }` input
- validation of a qualified current-term candidate election quorum
- legal `Candidate -> Leader` transition
- `leader_id = self`
- deterministic per-member replication-progress initialization
- campaign/pre-vote state cleanup
- current-term `LeaderBarrier` construction
- local barrier persistence staging
- self progress update only after barrier persistence completion

No replication message exists yet.

## Victory gate

`WinElection` succeeds only when all are true:

1. local role is `Candidate`;
2. requested winning term equals durable `current_term`;
3. durable `voted_for` is the local node;
4. local node is still an effective voter;
5. the current election round is for the same term; and
6. that election round has already reached the qualified stable/joint quorum.

Follower, pre-candidate, learner, wrong-term, or candidate-without-quorum victory
attempts fail with `InvalidLeaderTransition`.

## Deterministic progress initialization

Let `L` be the greatest already-durable logical log index before the leader
barrier. `L` comes from the retained stable suffix, or from the snapshot boundary
when the suffix is empty.

On legal election victory, progress is recreated from the effective
configuration in sorted `NodeId` order:

- every member gets `next_index = L + 1`;
- every non-self member starts with `match_index = 0`;
- self starts with `match_index = L`;
- self is marked recently active and in `Replicate` mode;
- other members begin not-recently-active in `Probe` mode.

Learners receive replication progress because they must catch up later, but
their voter authority remains unchanged: they still do not vote, campaign, or
count toward quorum.

## LeaderBarrier persistence boundary

The leader immediately constructs:

```text
LogEntry {
    term: current_term,
    index: L + 1,
    kind: LeaderBarrier,
}
```

Election victory emits exactly the leader role-change effect and one local
append persistence effect. It emits no network replication effect.

Before the barrier persistence completes:

- the barrier is not in `stable_log`;
- self `match_index` remains `L`;
- self `next_index` remains `L + 1`;
- peer progress is unchanged from initialization;
- commit index is unchanged.

After the matching `PersistenceCompleted(token)`:

- the barrier enters `stable_log`;
- self `match_index = L + 1`;
- self `next_index = L + 2`;
- commit index still does not advance.

This enforces the frozen contract that unstable local entries cannot count as
self replication progress.

## Snapshot boundary behavior

When the retained live suffix is empty, the snapshot
`last_included_index/last_included_term` defines the durable tail used for
leader progress and barrier placement. A snapshot at index 8 therefore produces
a new leader barrier at index 9, not at index 1.

## Failure behavior

Leader-barrier persistence failure is fatal/non-serving.

The failure path does not:

- append the barrier to stable log;
- advance self match;
- advance commit;
- emit AppendEntries or any other replication success.

The volatile role may already be `Leader`, but the stopped/fatal core is not a
serving leader and must be recovered/replaced according to the frozen
persistence-failure contract.

## Adversarial/unit coverage

The `astralog-raft` suite now proves, in addition to all prior foundation,
pre-vote, and RequestVote invariants:

- election victory rejected from follower state
- election victory rejected before election quorum
- wrong-term election victory rejected
- deterministic progress initialization from a non-empty durable log
- progress includes learners without granting voting authority
- leader transition emits no network messages
- current-term leader barrier staged as the only append
- self match remains at pre-barrier durable tail before persistence completion
- self match advances only after barrier persistence completion
- peer match/next state does not falsely include unstable barrier bytes
- barrier persistence leaves commit index unchanged
- barrier persistence failure is fatal and does not advance self match
- barrier index follows snapshot boundary when retained suffix is empty
- campaign/pre-vote state is cleared on legal victory

## Strict qualification evidence

Pinned Linux qualification environment: **Rust 1.98.1**.

- `cargo fmt --all -- --check`: **PASS**
- `cargo clippy --workspace --all-targets --all-features -- -D warnings`: **PASS**
- `cargo test --workspace --all-features`: **PASS**
- `astralog-raft`: **48 passed, 0 failed**
- frozen P1/P2 suite: **101 passed, 0 failed**
- workspace total: **149 passed, 0 failed**
- all doc-test targets: **PASS**
- `git diff --check`: **PASS**

The final qualification runs with dependencies cached and `--offline` in the
pinned Rust quality container.

## Source/scope audit

Source audit confirms this checkpoint adds no implementation of:

- AppendEntries request/response
- heartbeat transmission
- follower prefix validation
- follower log truncation or append
- conflict-term/index optimization
- leader next-index backtracking
- quorum commit advancement
- application of committed entries

The only production `Effect::Send` paths remain the already-qualified PreVote
and RequestVote paths. Election victory itself emits no `Send` effect.

P1 storage and P2 protocol/broker/client source files are unchanged.

## Next checkpoint

The next deterministic P3 checkpoint is **AppendEntries heartbeat + prefix
validation only**:

1. add AppendEntries request/response message types;
2. enforce lower/current/higher-term handling;
3. establish valid current-term leader authority;
4. match `prev_log_index/prev_log_term` against snapshot or retained log;
5. accept empty heartbeats only after valid prefix matching;
6. return deterministic missing-prefix / term-mismatch hints;
7. preserve all persistence-before-response ordering where a higher term must be
   stored first.

Non-empty entry append/truncation, conflict replacement, leader replication
scheduling, and commit advancement remain out of scope until later checkpoints.
