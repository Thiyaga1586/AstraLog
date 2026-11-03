# P3 AppendEntries Heartbeat / Prefix Qualification Report

Checkpoint: **P3 AppendEntries heartbeat + prefix validation only**

Result: **PASS**

Qualification date: **2026-09-24**

This is a P3 checkpoint, not P3 phase closure.

## Delivered scope

This checkpoint introduces the first AppendEntries behavior without introducing
follower log mutation or leader replication scheduling:

- `AppendEntriesRequest`
- `AppendEntriesResponse`
- lower/current/higher-term request handling
- higher-term response fencing
- current-term leader-authority establishment
- candidate step-down on valid current-term leader contact
- same-term competing-leader rejection
- snapshot/live-prefix validation
- deterministic follower conflict hints
- empty-heartbeat success responses

The request structure already carries `entries[]` and `leader_commit` because
they are part of the frozen conceptual Raft message. This checkpoint deliberately
does not make those mutation paths authoritative yet.

## Term and authority ordering

Lower-term AppendEntries never changes local term, vote, log, leader authority,
or progress. The follower rejects using its current term.

A higher-term request from an eligible voter first emits a hard-state persistence
effect for:

```text
current_term = request.term
voted_for    = none
```

Only after the matching persistence completion may the request establish leader
authority, validate its prefix, and emit a response. Persistence failure is
fatal/non-serving and emits no AppendEntries response.

Foreign/non-voter leader identities are not accepted as valid term authority.

For the current term, a candidate/pre-candidate accepts a valid voter leader,
steps down to follower, clears campaign state, and resets its election timer
before prefix validation. A learner keeps `Role::Learner` while recording the
leader and resetting the timer.

Once a correct node has accepted leader identity `L` in a term, another identity
cannot replace it in that same term. A local leader similarly rejects a competing
same-term authority and retains its progress state.

## Prefix validation

The prefix matches only when either:

- `prev_log_index == snapshot.last_included_index` and the term equals the
  snapshot term; or
- a retained stable-log entry exists at `prev_log_index` with the same term.

The deterministic rejection hints are:

- prefix below snapshot floor: `conflict_term = none`,
  `conflict_index = snapshot.last_included_index + 1`;
- prefix beyond local durable tail: `conflict_term = none`,
  `conflict_index = local_last_index + 1`;
- retained-index term mismatch: `conflict_term = local_term_at_prev`,
  `conflict_index = first retained index with that term`;
- snapshot-boundary term mismatch: `conflict_term = snapshot term`,
  `conflict_index = snapshot boundary`.

This checkpoint implements the follower-side hints only. Leader-side conflict
backtracking is still deliberately absent.

## Empty heartbeat success

When term/authority/prefix all validate and no later mutation is required, an
empty AppendEntries heartbeat succeeds immediately and reports:

```text
success     = true
match_index = prev_log_index
```

That index is already durable because it is either the durable snapshot boundary
or an entry in the retained stable log.

No stable-log bytes are changed by heartbeat success.

## Explicit checkpoint guards

Two operations are intentionally blocked rather than partially implemented:

- a matching request with non-empty `entries[]` returns
  `AppendEntriesEntriesUnavailable`;
- a matching heartbeat whose `leader_commit` would require advancing the local
  authoritative commit index returns
  `AppendEntriesCommitAdvancementUnavailable`.

These are internal P3 checkpoint guards, not production wire errors. They exist
to prevent a partial implementation from falsely acknowledging mutation or
commit semantics before those layers are separately qualified.

A non-empty request with an invalid prefix can still be rejected normally,
because prefix validation precedes suffix mutation in the frozen contract.

## AppendEntries responses

Same/lower-term AppendEntries responses are deliberately ignored by the leader
in this checkpoint. They do not mutate `match_index`, `next_index`, activity, or
replication mode.

A higher-term response from an effective voter or learner still participates in
the generic Raft term rule: the higher term is persisted and the leader/candidate
steps down only after durability. Leader-only progress is cleared when fenced.

Foreign/non-member higher-term responses cannot fence the group.

## Scope proof

Relative to leader-initialization commit `3008877`, the production diff contains
no new:

- `stable_log.push`
- log truncation
- `PersistenceOp::TruncateSuffix`
- `PersistenceOp::AdvanceCommit`
- peer `match_index` mutation
- peer `next_index` mutation
- leader replication scheduling

The only `match_index`/`next_index` mutation in production remains the previously
qualified local `LeaderBarrier` persistence path from the prior checkpoint.

P1 storage and P2 protocol/broker/client source files are unchanged.

## Adversarial/unit coverage

The Raft suite now additionally proves:

- stale heartbeat rejection with current term
- current-term candidate step-down on matching heartbeat
- candidate step-down before bad-prefix rejection
- same-term competing leader rejection
- local leader rejection of competing same-term authority
- higher-term term persistence before success response
- higher-term term persistence before prefix rejection
- higher-term persistence failure emits no response
- higher-term fencing clears stale leader progress
- learner heartbeat acceptance without voting authority
- snapshot-boundary heartbeat success
- below-snapshot deterministic hint
- missing-tail deterministic hint
- retained-term mismatch deterministic hint
- snapshot-term mismatch deterministic hint
- foreign leader cannot raise term
- non-empty matching request cannot mutate log
- non-empty bad-prefix request returns prefix rejection
- heartbeat commit advancement remains blocked
- same-term response cannot advance peer progress
- higher-term response durably fences a leader
- foreign higher-term response cannot fence a leader

## Strict qualification evidence

Pinned Linux qualification environment: **Rust 1.98.1**.

- `cargo fmt --all -- --check`: **PASS**
- `cargo clippy --workspace --all-targets --all-features -- -D warnings`: **PASS**
- `cargo test --workspace --all-features`: **PASS**
- `astralog-raft`: **69 passed, 0 failed**
- frozen P1/P2 suite: **101 passed, 0 failed**
- workspace total: **170 passed, 0 failed**
- all doc-test targets: **PASS**
- `git diff --check`: **PASS**

The final qualification runs with dependencies cached and `--offline` in the
pinned Rust quality container.

## Next checkpoint

The next deterministic P3 checkpoint is **non-empty AppendEntries log mutation
+ durable acknowledgement only**:

1. validate incoming entry contiguity from `prev_log_index + 1`;
2. retain already-present entries with identical `(index, term)`;
3. treat conflict at or below `commit_index` as fatal safety/corruption;
4. truncate only a conflicting uncommitted suffix;
5. append only the necessary replacement/new entries;
6. persist truncate + append in one ordered persistence batch;
7. emit `success=true` only after persistence completion;
8. report the resulting durable `match_index`.

Leader-side response processing/backtracking, replication scheduling, follower
commit advancement, and quorum-based leader commit advancement remain out of
scope until later checkpoints.
