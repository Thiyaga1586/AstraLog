# AstraLog Raft Core Contract V1

Status: **FROZEN FOR P3 IMPLEMENTATION**

Contract version: **1**

This document is the normative P3 contract for AstraLog's reusable Raft core.
It is frozen before election, replication, membership, snapshot, or leadership-
transfer implementation begins.

P3 implements this contract independently from broker networking and P4
partition integration.

---

## 1. Purpose and scope

Raft Core V1 defines:

- the deterministic state-machine boundary
- persistent versus volatile Raft state
- persistence ordering and crash semantics
- term and vote safety
- pre-vote and election behavior
- RequestVote and AppendEntries semantics
- conflict optimization
- commit advancement
- learner behavior
- joint-consensus membership transition
- snapshot creation/install semantics
- leadership transfer
- monotonic-timer rules
- deterministic simulation hooks
- formal safety properties that must be model checked before P3 closes

Raft Core V1 deliberately does **not** define:

- broker/client TCP wire messages
- P4 replica-RPC transport bytes
- partition placement policy
- controller metadata policy
- producer acknowledgements
- consumer visibility/high-watermark mapping
- compression
- retention/compaction policy
- authentication or authorization

Those remain owned by later phases.

---

## 2. Design boundary

The Raft core is a pure deterministic state machine.

It performs no direct:

- disk I/O
- socket I/O
- wall-clock reads
- random-number generation
- thread creation
- sleeping

Instead, the embedding runtime supplies deterministic inputs and executes
explicit effects emitted by the core.

For identical durable state, volatile state, configuration, and ordered input
sequence, the core MUST emit the same ordered effects.

This separation is mandatory for deterministic simulation and crash-boundary
testing.

---

## 3. Fundamental types

### 3.1 Node identity

`NodeId` is a non-zero `u64`.

Zero is reserved as `NONE` and never represents a real member.

The P4 broker adapter may losslessly map a broker identifier into a `NodeId`.

### 3.2 Terms

`Term` is `u64`.

- term 0 means no election term has yet been established
- real elections use terms >= 1
- terms only increase
- overflow MUST produce a terminal local fault; terms never wrap

### 3.3 Log indexes

`LogIndex` is `u64`.

- index 0 is the synthetic pre-log/snapshot origin
- first real log entry is index 1
- indexes are contiguous above the current snapshot boundary
- arithmetic is checked; indexes never wrap

This matches the P1 rule that real Raft indexes begin at 1.

### 3.4 Entry identity

The identity of a log entry is:

```text
(term, index)
```

Two entries with the same `(term, index)` MUST represent the same accepted log
history prefix.

### 3.5 Entry kinds

The core recognizes three semantic entry kinds:

```text
Application(payload)
LeaderBarrier
Configuration(change)
```

`Application(payload)` is opaque to P3.

`LeaderBarrier` is an empty current-term entry appended when a node becomes
leader.

`Configuration(change)` is interpreted by the Raft core and is not delegated to
the application state machine as an ordinary application command.

P3 does not freeze production serialization bytes for these entries. P4 maps
them onto the already-frozen P1 storage representation without changing P1
bytes.

---

## 4. Persistent state

The following state survives restart.

### 4.1 Hard state

```text
current_term: Term
voted_for: Option<NodeId>
durable_commit_index: LogIndex
```

Rules:

- `voted_for` is either none or one voter for `current_term`
- changing `current_term` clears `voted_for` unless the same persistence batch
  records a new vote in the new term
- `durable_commit_index` is monotonic
- `durable_commit_index <= stable_last_log_index`
- `durable_commit_index >= snapshot.last_included_index`

For this contract, `stable_last_log_index` means the greatest durable logical
Raft index represented either by the retained stable suffix or, when that
suffix is empty, by the snapshot boundary.

Persisting the commit index is an AstraLog V1 recovery rule. Raft safety does
not depend on losing no volatile commit progress, but AstraLog must never move a
known committed boundary backwards after restart.

### 4.2 Stable log

Persistent log state consists of an ordered sequence of entries above the
snapshot boundary.

For every real entry:

```text
entry.index > snapshot.last_included_index
entry.index is contiguous with its predecessor
entry.term > 0
```

The storage adapter MUST support:

- append contiguous entries
- truncate a conflicting suffix
- term lookup by index
- first/last index lookup
- range reads
- durable completion acknowledgement

It MUST NOT silently invent or repair an authoritative log entry.

### 4.3 Snapshot metadata

Persistent snapshot metadata is:

```text
last_included_index: LogIndex
last_included_term: Term
configuration: StableConfiguration
snapshot_id: opaque stable identifier
```

The initial empty snapshot origin is:

```text
last_included_index = 0
last_included_term  = 0
```

A non-empty snapshot index always refers to a committed/applied prefix.

### 4.4 Membership persistence

Membership is replicated through configuration log entries and is recoverable
from:

1. snapshot configuration; plus
2. configuration entries after the snapshot boundary.

The implementation MUST NOT rely on a separate mutable membership database for
Raft safety.

---

## 5. Volatile state

Volatile state may be reconstructed after restart.

All nodes maintain:

```text
role
leader_id
commit_index
last_applied
election_elapsed
randomized_election_timeout
effective_configuration
pending_configuration_index
```

Leaders additionally maintain per replication target:

```text
match_index
next_index
recently_active
replication_mode
```

`commit_index` starts at `durable_commit_index` after restart and may advance
when valid leader information proves further commitment.

`last_applied` starts at least at the snapshot index. Application replay above
that boundary is driven by explicit apply effects.

---

## 6. Roles

V1 roles are:

```text
Follower
PreCandidate
Candidate
Leader
Learner
```

`Learner` is non-voting. A learner receives log replication and snapshots but:

- never starts an election
- never grants a vote that counts toward quorum
- never counts toward election or commit quorum

A voter that is currently follower/candidate/leader is not simultaneously a
learner.

---

## 7. Deterministic input interface

The core accepts an ordered stream of inputs conceptually equivalent to:

```text
Tick
Step(message)
Propose(application_payload)
ProposeConfiguration(target_configuration)
TransferLeadership(target_node)
PersistenceCompleted(token)
PersistenceFailed(token, error)
AppliedThrough(index)
SnapshotBuilt(snapshot)
SnapshotPersisted(token)
Shutdown
```

Production APIs may use different Rust names, but semantics must match this
contract.

### 7.1 Injected time

The core receives logical ticks. It never reads wall clock or `Instant`
directly.

### 7.2 Injected randomness

The runtime supplies election-timeout randomness at initialization/reset.

Tests and simulation MUST be able to supply a deterministic seeded source.

---

## 8. Effect interface

The core emits ordered effects conceptually equivalent to:

```text
Persist(batch, token)
Send(message)
Apply(entries)
BuildSnapshot(index)
InstallSnapshotToStateMachine(snapshot)
RoleChanged(role, term, leader)
ProposalResult(id, result)
Fatal(reason)
```

The embedding runtime executes effects and feeds completion events back to the
core.

### 8.1 One ordered persistence barrier per group

AstraLog Raft V1 has at most one unacknowledged `Persist` effect per Raft group.

Multiple hard-state/log/commit operations may be coalesced into that batch, but
the core does not issue a second persistence batch whose correctness depends on
the first until the first receives `PersistenceCompleted` or
`PersistenceFailed`.

This bounds unstable state and gives crash/restart simulation one explicit
durability frontier. Later performance work may batch more aggressively without
changing the safety ordering defined here.

### 8.2 Durability dependency rule

An effect whose correctness depends on newly persistent state MUST NOT become
externally visible before the required `PersistenceCompleted(token)` input.

At minimum this means:

- a real RequestVote campaign is not sent before the new term/self-vote is
  durable
- a granted VoteResponse is not sent before term/vote state is durable
- a successful AppendEntriesResponse is not sent before the accepted log
  mutation is durable
- a successful InstallSnapshotResponse is not sent before the snapshot
  installation is durable
- the local node does not count a newly proposed entry as its own durable match
  for commit calculation before local persistence completes

The implementation may buffer dependent messages internally while persistence
is outstanding.

---

## 9. Persistence batch semantics

A persistence batch may contain:

```text
HardState update
Log suffix truncate
Log append entries
Commit-index update
Snapshot install/update
Log-prefix discard after snapshot durability
```

### 9.1 Completion contract

`PersistenceCompleted(token)` means every operation in that batch has reached
the durability boundary required by its storage adapter.

It is not merely an in-memory write completion.

### 9.2 Crash prefix safety

The storage adapter need not provide one giant filesystem transaction, but a
crash may expose only a state that recovery can interpret as a safe prefix of
the persistence protocol.

Required ordering:

1. hard-state term/vote changes needed to authorize an action
2. any required log truncate
3. replacement/appended log entries
4. commit-index advancement, never beyond stable log

For snapshot installation:

1. snapshot bytes and metadata become durable
2. required retained suffix decision becomes durable
3. obsolete prefix may then be reclaimed

No response that claims successful persistence may be emitted before the whole
required sequence is durable.

### 9.3 Persistence failure

A persistence failure is never translated into protocol success.

For correctness-critical local persistence failure, the Raft instance enters a
non-serving/fatal state until the embedding runtime explicitly replaces or
recovers it.

---

## 10. Recovery contract

On restart, before processing messages or ticks:

1. load and validate hard state
2. load and validate snapshot metadata
3. load stable log suffix
4. prove log contiguity above the snapshot boundary
5. verify the snapshot boundary term against any retained matching entry
6. verify `durable_commit_index` is within durable history
7. reconstruct membership from snapshot plus retained configuration entries
8. set role to follower or learner according to recovered membership
9. set volatile commit index to durable commit index
10. make committed entries above the durable application/snapshot point
   available for replay

Recovery MUST fail loudly on an impossible committed-history state.

It MUST NOT silently truncate committed state to make startup succeed.

---

## 11. Term handling

### 11.1 Higher term

Except for the special pre-vote rules in section 14, receiving a valid Raft
message with term greater than `current_term` causes the node to:

1. become follower (or remain learner if non-voting)
2. clear known leader
3. set `current_term` to the received term
4. clear `voted_for`
5. persist the hard-state change before sending a response that depends on the
   new term

### 11.2 Lower term

Messages from terms lower than `current_term` never cause log or vote changes.

Where the protocol has a response, the node rejects using its current term.

### 11.3 Same term leader contact

A candidate receiving valid AppendEntries or InstallSnapshot from the current
term steps down to follower before processing the request.

There is at most one accepted leader authority per term from the perspective of
a correct node.

---

## 12. Log comparison rule

Candidate log freshness is compared using:

```text
(last_log_term, last_log_index)
```

Candidate A is at least as up to date as local log B when:

```text
A.last_term > B.last_term
OR
(A.last_term == B.last_term AND A.last_index >= B.last_index)
```

The snapshot boundary participates in last-log term/index lookup when the live
suffix is empty.

---

## 13. Election timers

Consensus safety never depends on synchronized clocks.

V1 uses logical monotonic ticks:

```text
heartbeat_interval > 0
election_timeout_min >= 5 * heartbeat_interval
election_timeout_max > election_timeout_min
```

The production defaults are chosen later from measured environments, but the
ratio constraint is validated by the core configuration.

Each election timeout reset samples a deterministic injected value in the
inclusive configured range.

Receiving valid leader replication traffic resets the election timer.

---

## 14. Pre-vote

Pre-vote is **always enabled** in AstraLog V1.

Its purpose is to prevent an isolated/rejoining node from needlessly increasing
the cluster term.

### 14.1 Pre-vote request

A voter whose election timer expires first enters `PreCandidate` and requests
pre-votes for prospective term:

```text
current_term + 1
```

Starting or granting a pre-vote:

- does not increment `current_term`
- does not modify `voted_for`
- requires no persistent hard-state update

PreVoteRequest and PreVoteResponse carry a prospective election term for vote
comparison, but receiving that prospective term does **not** by itself advance
the receiver's persistent `current_term`.

### 14.2 Pre-vote grant

A voter may grant pre-vote only when:

- requester is a voter in the effective configuration
- request uses a prospective term greater than local current term
- requester log is at least as up to date
- local node has not recently accepted valid leader authority within its
  election timeout

The recent-leader condition is a disruption/liveness guard, not a safety proof.

### 14.3 Transition to real election

Only after the pre-candidate obtains election quorum does it begin a real
election.

---

## 15. Real election

To start an election, a voter:

1. increments `current_term`
2. becomes candidate
3. votes for itself
4. clears leader identity
5. persists `(current_term, voted_for=self)`
6. only after persistence completion sends RequestVote to the other voters

The candidate's own vote counts toward election quorum only after that
self-vote persistence completes.

A candidate wins only after receiving an election quorum under the effective
configuration.

On election win it becomes leader and immediately creates a `LeaderBarrier`
entry in its current term.

The barrier is replicated through the ordinary log path.

---

## 16. RequestVote

Request:

```text
term
candidate_id
last_log_index
last_log_term
```

Response:

```text
term
vote_granted
```

A voter grants a vote only when all are true:

1. request term is not stale
2. candidate is a voter permitted by the effective configuration
3. local `voted_for` is none or already that candidate
4. candidate log is at least as up to date as local log

Repeated RequestVote from the same candidate in the same term may be granted
again after the original vote is durable.

One correct voter never durably votes for two different candidates in one
term.

Learners never grant voting authority.

---

## 17. Leader initialization

On becoming leader:

- `leader_id = self`
- each peer `next_index = local_last_index + 1`
- each peer `match_index = 0`, except self
- self match reflects only locally durable entries
- replication progress is reset for the new term
- leadership transfer state is cleared
- a current-term `LeaderBarrier` entry is proposed

The leader may send empty AppendEntries heartbeats immediately after its
leadership term/vote is durable, but it cannot count an unstable local log
entry toward commitment.

---

## 18. AppendEntries request

Conceptual request:

```text
term
leader_id
prev_log_index
prev_log_term
entries[]
leader_commit
```

Entries are contiguous and start at `prev_log_index + 1`.

An empty `entries[]` request is a heartbeat.

`leader_commit` never exceeds the leader's own authoritative commit index; a
leader does not advertise a newly proven commit boundary until its local
commit-index persistence completes.

---

## 19. AppendEntries validation

The follower/learner processes AppendEntries in this order:

1. term validation
2. leader-authority update
3. `prev_log_index/prev_log_term` matching
4. conflicting suffix analysis
5. persistence of required term/log changes
6. derive follower commit target as
   `min(leader_commit, last locally matched log/snapshot index)`
7. persist any higher commit target before making it authoritative/applyable
8. commit advancement bounded by local matched history
9. success response only after required log persistence

### 19.1 Prefix match

Prefix matches when either:

- `prev_log_index == snapshot.last_included_index` and terms match snapshot
  metadata; or
- local retained log contains `prev_log_index` with `prev_log_term`

### 19.2 Missing prefix

If follower does not contain `prev_log_index`, it rejects without modifying the
log.

### 19.3 Term conflict

If the index exists but term differs, it rejects without modifying the log.

Log truncation occurs only when the prefix matches and a subsequently supplied
entry conflicts with the local uncommitted suffix.

---

## 20. Conflict handling and log matching

When incoming entry `(index, term)` conflicts with a local entry at the same
index:

- if `index <= commit_index`, this is a fatal safety violation/corruption path;
  committed history is never truncated
- otherwise truncate the local suffix beginning at that index and append the
  leader's entries

If local entry at an incoming index already has the same term, the existing
entry is retained and need not be rewritten.

After a successful append, the Raft Log Matching Property must hold.

---

## 21. Conflict optimization

A rejected AppendEntries response contains:

```text
term
success = false
conflict_term: Option<Term>
conflict_index: LogIndex
```

If `prev_log_index` is beyond follower's last index:

```text
conflict_term  = none
conflict_index = follower_last_index + 1
```

If `prev_log_index` exists but has a different term:

```text
conflict_term  = follower_term_at(prev_log_index)
conflict_index = first retained index having conflict_term
```

Leader backtracking:

- if leader has `conflict_term`, set next index to one past its last index in
  that term
- otherwise set next index to `conflict_index`
- never set next index below `snapshot.last_included_index + 1`; use snapshot
  installation when ordinary catch-up cannot satisfy the prefix

---

## 22. Append acknowledgement durability

A follower/learner returns `success=true` only after:

- any required higher-term hard state is durable
- required conflict truncation is durable
- all newly accepted entries covered by the response are durable

The response reports a durable `match_index`.

This directly enforces invariant I-09.

---

## 23. Commit rule

### 23.1 Stable configuration

For a stable voter set `C`, an index has replication quorum when durable match
indexes show a strict majority of `C` at or above that index.

Learners do not count.

### 23.2 Joint configuration

For joint configuration `(C_old, C_new)`, an index has replication quorum only
when it is present on:

- a strict majority of `C_old`; and
- independently, a strict majority of `C_new`

### 23.3 Current-term restriction

A leader advances `commit_index` using quorum counting only to an index `N`
where:

```text
log[N].term == current_term
```

When such `N` becomes committed, all preceding entries in the leader's log are
committed by prefix implication.

This rule is mandatory.

### 23.4 Durable commit state

When quorum logic or a valid leader-commit value proves a higher commit index,
the core first requests persistence of the monotonic durable commit boundary.
Only after that persistence completes does the local authoritative
`commit_index` advance and become eligible for `Apply` effects or higher-layer
crash-stable visibility.

P4 maps this to partition visibility/high-watermark behavior.

---

## 24. Application ordering

Committed entries are emitted to the application state machine strictly in
increasing index order.

The core never emits an entry above `commit_index` for application.

Configuration entries update Raft membership according to section 30 and are
also reflected in apply progress.

`last_applied` never exceeds `commit_index`.

Application failure is surfaced to the embedding runtime; the core does not
pretend the entry was applied.

---

## 25. Leader liveness check

AstraLog V1 enables **check-quorum** behavior.

The leader tracks recent responses from voters using logical ticks. If it
cannot confirm contact with an election quorum during an election-timeout
window, it steps down and stops accepting new proposals as leader.

In joint configuration, recent-contact quorum requires a majority of both old
and new voter sets.

This reduces stale-leader exposure and improves recovery behavior.

Raft safety MUST NOT depend on check-quorum timing.

---

## 26. Proposal behavior

Only a leader accepts new application/configuration proposals.

A follower/candidate/learner returns a not-leader outcome containing known
leader identity when available.

The core does not silently forward proposals.

This avoids hidden transport behavior inside the consensus engine.

---

## 27. Uncommitted-log bound

P3 core state must support a configured bound on uncommitted application bytes
accepted by a leader.

When the bound is reached, new application proposals are rejected/backpressured
until commitment frees capacity.

Heartbeats, vote traffic, configuration progress, snapshot traffic, and other
control traffic are not blocked by this application-data bound.

This enforces bounded-resource and control-plane-starvation invariants.

---

## 28. Membership model

A configuration contains disjoint sets:

```text
voters
learners
```

Requirements:

- voter set is never empty in a valid committed stable configuration
- a node cannot be voter and learner simultaneously
- node 0 is never a member
- duplicate node IDs are invalid

---

## 29. Learners

New replicas join as learners.

A learner:

- receives AppendEntries/snapshot replication
- maintains durable log state
- reports replication progress
- does not vote
- does not campaign
- does not count toward commit quorum

The leader may propose promotion to voter only when the learner is caught up
through at least the current committed index at proposal validation time.

This catch-up rule prevents an empty replica from immediately weakening voter
availability.

---

## 30. Joint-consensus membership transition

AstraLog V1 uses two explicit configuration entries:

```text
EnterJoint { old, new }
LeaveJoint { new }
```

Only one membership transition may be pending at a time.

### 30.1 Entering joint consensus

When changing stable `C_old` to stable `C_new`, the leader appends:

```text
EnterJoint(C_old, C_new)
```

As soon as a server durably stores this entry, it uses the joint configuration
for subsequent Raft quorum/election decisions; it does not wait for the entry
to commit.

Joint decisions require independent majorities from old and new voter sets.

### 30.2 Leaving joint consensus

Only after `EnterJoint` is committed may the leader append:

```text
LeaveJoint(C_new)
```

As soon as a server durably stores `LeaveJoint`, it uses `C_new` as its latest
configuration according to the Raft membership algorithm.

When `LeaveJoint` commits, `C_old` is no longer authoritative.

### 30.3 Pending-change serialization

The core rejects a new membership proposal while another configuration entry
is pending/unresolved.

This avoids overlapping membership transitions.

### 30.4 Self-removal

If committed `C_new` removes the current leader, that node steps down once the
new stable configuration becomes authoritative and does not accept new
proposals as leader.

### 30.5 Snapshot restriction

P3 V1 creates local snapshots only from a **stable, non-joint committed
configuration**.

The log prefix containing an unresolved joint transition is not compacted away.

This intentionally simplifies recovery and preserves the evidence needed to
finish or reject the transition safely.

---

## 31. Election quorum under configuration change

Election quorum uses the effective configuration associated with the latest
configuration entry known under the joint-consensus algorithm.

In joint configuration, winning an election requires:

- majority of old voters; and
- majority of new voters

A learner never contributes a vote.

A node not eligible as a voter in the effective configuration cannot become
leader.

---

## 32. Snapshot creation

Snapshot creation is requested only at an applied index `S` satisfying:

```text
S <= last_applied
S <= commit_index
```

Snapshot metadata records:

- `S`
- term at `S`
- committed stable membership at `S`
- opaque application snapshot identity/data reference

Snapshot creation MUST NOT mutate the live Raft suffix before the snapshot is
durably persisted.

After durable snapshot publication, entries strictly below the retained
snapshot/log safety boundary may be reclaimed by the storage adapter.

P3 does not define P9 event-history retention or compaction.

---

## 33. InstallSnapshot

Conceptual request:

```text
term
leader_id
last_included_index
last_included_term
stable_configuration
snapshot_id
snapshot_payload/chunk
```

P3 may implement chunk transport abstractly; transport framing is P4-owned.

### 33.1 Stale snapshot

If `last_included_index <= local_snapshot.last_included_index`, the node may
acknowledge/ignore the already-covered snapshot without moving state backward.

### 33.2 Retaining suffix

After installing snapshot `(S,T)`:

- if `S <= local commit_index` and the locally committed history proves a term
  at `S` different from `T`, reject/fail the installation rather than replacing
  committed history
- if local log contains entry `S` with term `T`, retain the suffix strictly
  after `S`
- otherwise discard the conflicting retained suffix

Committed history is never replaced by an incompatible snapshot.

### 33.3 Durability

Successful InstallSnapshotResponse is emitted only after:

- snapshot bytes/metadata are durable
- resulting retained/discarded log state is durable
- hard/commit state is consistent with the new snapshot boundary

### 33.4 State-machine install

The application state-machine snapshot is installed before entries after the
snapshot are applied.

`commit_index` and `last_applied` never move below the installed snapshot index.

---

## 34. Snapshot versus ordinary catch-up

Leader uses ordinary AppendEntries while follower `next_index` remains within
leader's retained Raft suffix.

If required prefix history is no longer retained, leader sends a snapshot.

After snapshot acknowledgement, follower progress advances to at least the
snapshot index and ordinary AppendEntries resumes.

---

## 35. Leadership transfer

Leadership transfer is a liveness extension; it never bypasses normal election
safety.

Rules:

1. target must be a voter in the effective configuration
2. target cannot be self
3. only one transfer is active at a time
4. leader first catches target up through the leader's durable last log index
5. while transfer is active, leader rejects/backpressures new application
   proposals rather than extending the target indefinitely
6. when caught up, leader sends `TimeoutNow` to target
7. `TimeoutNow` causes the target to begin an immediate **real election**;
   leadership transfer bypasses both the ordinary election-timeout wait and the
   pre-vote phase, but does not bypass term increment, durable self-vote,
   RequestVote log-freshness checks, or quorum
8. transfer completes only if target wins that legal election
9. if transfer times out or target becomes ineligible, old leader resumes normal
   proposal service if it still holds leadership

Transfer never grants leadership directly.

---

## 36. Message duplication, delay, and reordering

The core MUST remain safe under arbitrary:

- duplicate messages
- dropped messages
- delayed messages
- reordered messages
- stale messages from old terms

within the non-Byzantine Raft fault model.

Message processing must therefore be idempotent where logically possible and
must fence stale terms/indexes explicitly.

---

## 37. Crash/restart model

A node may crash:

- before a Persist effect begins
- during any persistence operation
- after persistence completes but before dependent Send
- after Send but before receiving a response
- during membership transition
- during snapshot publication/install
- during leadership transfer

Recovery may lose volatile state and unsatisfied effects.

It MUST preserve every state transition previously acknowledged as durable.

The simulator must exercise these boundaries deterministically.

---

## 38. Safety invariants

P3 implementation and formal model must prove at least:

### R-01 Election safety

At most one leader is elected in a term.

### R-02 Log matching

If two logs contain an entry with the same index and term, all preceding log
entries have the same index/term history.

### R-03 Leader completeness

If an entry is committed in term T, every leader elected in a later term
contains that entry.

### R-04 State-machine safety

No two correct nodes apply different entries at the same log index.

### R-05 Vote durability

A node never grants durable votes to two different candidates in one term,
including across crashes.

### R-06 Commit durability

A durable committed index never moves backward across restart.

### R-07 Committed-prefix preservation

No AppendEntries or snapshot operation replaces a committed entry with a
different entry at the same index.

### R-08 Follower durable acknowledgement

Append success implies the acknowledged suffix is locally durable.

### R-09 Minority cannot commit

In a stable configuration, a leader isolated with fewer than a voting majority
cannot newly commit entries.

### R-10 Joint quorum intersection

During joint consensus, no entry/election decision succeeds without majority
approval from both old and new voter sets.

### R-11 Learner non-authority

Learners never create quorum authority by voting or counting toward commit.

### R-12 Snapshot preservation

Installing/creating snapshots never discards evidence required to preserve the
committed prefix or unresolved membership transition.

### R-13 Higher-term fencing

After a correct node establishes a higher term, stale lower-term authority
cannot cause that node to accept a conflicting commit or vote.

---

## 39. Formal model requirements

P3 includes a small TLA+ model (or equivalent mechanically checkable model)
covering the essential protocol, not production implementation details.

Model state must include at least:

- node terms
- durable votes
- roles
- logs `(index, term, value-id)`
- commit indexes
- messages in flight
- crash/restart
- stable voter configuration
- joint old/new voter configuration
- learners where needed for membership safety

Bounded model checks must exercise:

- simultaneous candidacies
- lost/duplicated/reordered vote messages
- stale leaders
- conflicting uncommitted suffixes
- crash after vote persistence
- crash before/after append persistence
- leader crash before/after quorum
- joint-configuration transition
- learner promotion

The model checks R-01 through R-07, R-09, R-10, and R-13 directly or via
stronger equivalent invariants.

---

## 40. Deterministic simulator requirements

`astralog-sim` or the P3 test harness supplies:

- seeded deterministic RNG
- logical clock/ticks
- explicit node scheduling
- packet drop
- packet duplication
- packet delay
- packet reordering
- node crash/restart
- persistence delay/failure injection
- reproducible trace capture

A simulation failure must print enough information to rerun exactly the same
seed and schedule.

P3 closure requires both hand-authored scenarios and large seeded campaigns.

---

## 41. Mandatory deterministic scenarios

At minimum P3 tests include:

1. single-node election and commit
2. three-node normal election
3. split vote and retry
4. pre-vote prevents isolated stale node from term disruption
5. one durable vote per term across restart
6. candidate with stale log cannot win
7. leader replicates and commits current-term entry
8. prior-term entry commits only indirectly through a current-term entry
9. follower conflict hint skips an entire conflicting term
10. committed prefix cannot be truncated
11. minority partition cannot commit
12. stale leader cannot commit after losing quorum/higher term
13. crash after local append before follower quorum
14. crash after follower durable append before response
15. crash after quorum replication before leader commit notification
16. duplicate/reordered AppendEntries is safe
17. learner catches up but does not vote
18. learner promotion only after catch-up rule
19. old/new joint majorities required independently
20. leader self-removal through configuration change
21. snapshot install with matching retained suffix
22. snapshot install with conflicting suffix
23. snapshot install crash boundaries
24. leadership transfer to caught-up voter
25. failed leadership transfer resumes ordinary service

---

## 42. Stable iteration order

Any algorithm whose output order affects deterministic traces MUST iterate peers
in stable `NodeId` order rather than hash-map iteration order.

This applies to at least:

- vote request emission
- AppendEntries emission when multiple peers become ready simultaneously
- quorum/debug trace rendering
- simulator scheduling choices after seeded RNG selection

---

## 43. No hidden safety dependency on broker code

The Raft core owns all consensus-critical decisions.

The broker/P4 adapter may:

- persist requested state
- transport emitted messages
- expose committed entries
- provide timers/RNG/configuration requests

It may not decide:

- whether to grant a vote
- whether a log prefix matches
- whether to truncate a suffix
- whether quorum exists
- whether an entry is committed
- whether a membership transition is safe
- whether a learner counts as voter

P3 exit gate requires that no broker code be necessary to make Raft safe.

---

## 44. Relationship to P1 storage

P3 does not change Storage Format V1.

P1 already carries Raft term/index on stored frames and persists a committed
Raft boundary. P4 will provide the production Raft-storage adapter that maps
this P3 contract to partition files.

P3 tests may use an in-memory deterministic persistence adapter and a dedicated
test/reference durable adapter, but those do not create an alternate production
event-log format.

---

## 45. Relationship to P4

P3 proves the consensus engine in isolation.

P4 owns:

- one Raft group per partition
- broker-pair Raft transport/multiplexing
- RF 1/3/5 partition integration
- follower P1 persistence adapter
- client-facing leader redirects
- mapping commit index to stream high watermark
- quorum producer acknowledgements
- replica catch-up using real partition storage

P3 MUST NOT implement those prematurely.

---

## 46. Compatibility/freeze rule

After P3 closes, incompatible changes to the following require the architecture
change process and a contract-version update:

- persistent hard-state semantics
- persistence-before-message ordering
- vote/log freshness rules
- commit rule
- learner authority rules
- joint-consensus semantics
- snapshot safety semantics
- deterministic state-machine effect contract

Rust type names and internal module organization are not compatibility
contracts unless separately documented.

---

## 47. P3 implementation order

Only after this document is frozen:

1. `astralog-raft` fundamental types/configuration
2. persistent/volatile state and storage abstraction
3. deterministic input/effect engine skeleton
4. term handling and pre-vote
5. RequestVote election path
6. AppendEntries/log matching/conflict optimization
7. leader replication progress and commit calculation
8. apply progression
9. learners
10. joint-consensus membership
11. snapshots/install snapshot
12. leadership transfer
13. deterministic simulator/fault scheduler
14. TLA+ safety model
15. seeded campaigns and crash-boundary qualification
16. P3 closure report

No broker integration is required for these steps.

---

## 48. Research basis

The contract follows the core safety rules from the extended Raft paper and
Ongaro dissertation, including log freshness, current-term commit restriction,
and joint consensus. Production experience from deterministic I/O-separated
Raft cores is used as an implementation-quality reference, not as a dependency.

Primary references:

- Diego Ongaro and John Ousterhout, *In Search of an Understandable Consensus
  Algorithm (Extended Version)*
- Diego Ongaro, *Consensus: Bridging Theory and Practice*
- etcd-io/raft design and documentation for deterministic core/persistence
  integration patterns
