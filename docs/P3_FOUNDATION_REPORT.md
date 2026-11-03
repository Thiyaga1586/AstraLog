# P3 Raft Foundation Qualification Report

Checkpoint: **P3 foundation — types/state/persistence/effect shell**

Result: **PASS**

Qualification date: **2026-09-23**

This is a P3 checkpoint, not P3 phase closure.

## Delivered scope

The `astralog-raft` crate now contains only the foundation required by
`RAFT_V1.md` before consensus behavior begins:

- checked `NodeId` with node zero rejected
- checked `Term` and `LogIndex` arithmetic with overflow errors
- synthetic Raft log index zero supported by the consensus model
- deterministic `BTreeSet`-backed stable voter/learner configuration
- joint old/new configuration representation
- duplicate membership and voter/learner overlap rejection
- application, leader-barrier, and configuration log-entry representations
- persistent `HardState`
- stable snapshot metadata
- retained-log recovery image and validation
- follower/pre-candidate/candidate/leader/learner role representation
- volatile commit/apply/timer/configuration/progress state
- persistence operation/batch/token abstraction
- one in-flight persistence barrier per Raft group
- deterministic input/effect enums
- deterministic `RaftCore` shell
- fatal/non-serving transition on persistence failure
- no direct disk/network/clock/RNG/thread access

## Intentional non-implementation

This checkpoint deliberately contains **no consensus algorithm behavior**.

The source audit found no implementation of:

- PreVote request/response processing
- RequestVote
- vote counting
- candidate campaigning
- leader election
- AppendEntries
- heartbeat processing
- log conflict/backtracking replication logic
- quorum commit advancement
- snapshot-install state machine
- leadership transfer behavior

`Tick`, inbound-message, and proposal inputs intentionally produce no consensus
effects in the foundation checkpoint. A regression test locks this boundary.

## Foundation invariants qualified

### Membership input is not silently normalized

Stable configurations reject duplicate voter/learner IDs and reject any node
present in both roles. Member iteration is sorted by `NodeId` for deterministic
future traces.

### Recovery does not accept impossible durable boundaries

Recovery validation rejects:

- non-contiguous retained log indexes
- term zero on real retained entries
- durable commit below the snapshot boundary
- durable commit beyond stable history
- pending configuration indexes outside retained history
- `voted_for` identities that are not voters in the effective configuration

### Persistence has one explicit frontier

At most one persistence batch may be outstanding. A mismatched completion token
cannot clear that barrier. This gives later crash-boundary simulation an exact
durability frontier.

### Persistence errors never become success

A persistence failure moves the foundation core to stopped/non-serving state and
emits a fatal effect. It does not generate any successful consensus effect.

## Qualification evidence

Pinned environment: **Rust 1.98.1**.

- `cargo fmt --all -- --check`: **PASS**
- `cargo clippy --workspace --all-targets --all-features -- -D warnings`: **PASS**
- `cargo test --workspace --all-features`: **PASS**
- `astralog-raft`: **12 passed, 0 failed**
- prior P1/P2 tests: **101 passed, 0 failed**
- workspace total: **113 passed, 0 failed**
- all doc-test targets: **PASS**
- `git diff --check`: **PASS**

The final workspace qualification was rerun from the persistent P3 qualification
container with dependencies cached and `--offline`, so the final green result
did not depend on external network availability.

## Dependency/scope audit

`astralog-raft/Cargo.toml` has no dependencies.

The crate does not depend on:

- `astralog-broker`
- `astralog-client`
- `astralog-protocol`
- `astralog-storage`
- Tokio
- a Raft library
- filesystem/network/random/time libraries

P1 storage bytes and P2 Wire V1 are unchanged.

## Next checkpoint

The next deterministic implementation step is **pre-vote only**.

Pre-vote must be implemented and deterministically qualified before real
RequestVote campaigning, election victory, or AppendEntries replication is
introduced.
