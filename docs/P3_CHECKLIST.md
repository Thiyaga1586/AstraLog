# P3 — Raft Core Checklist

Status: **P3 CLOSED — RAFT CORE QUALIFIED**

This checklist is the mechanical closure gate for P3.

## A. Contract freeze

- [x] `RAFT_V1.md` written before Raft implementation
- [x] deterministic I/O-free core boundary fixed
- [x] persistent hard-state semantics fixed
- [x] persistence-before-message ordering fixed
- [x] vote/log freshness rule fixed
- [x] current-term commit rule fixed
- [x] learner authority rule fixed
- [x] joint-consensus transition fixed
- [x] snapshot safety rules fixed
- [x] leadership-transfer safety boundary fixed
- [x] contract freeze commit created

## B. Core crate

- [x] `astralog-raft` crate created with no broker/client dependency
- [x] checked `NodeId`, `Term`, `LogIndex` types
- [x] stable/joint configuration types
- [x] persistent hard state
- [x] volatile state
- [x] deterministic input enum
- [x] deterministic effect enum
- [x] persistence tokens/barriers
- [x] no direct disk/network/clock/RNG access in core

## C. Elections

- [x] follower/pre-candidate/candidate/leader/learner roles
- [x] randomized injected election timeout
- [x] pre-vote always enabled
- [x] RequestVote
- [x] durable term/self-vote before campaigning
- [x] durable granted vote before response
- [x] log up-to-date rule
- [x] one vote per term across restart
- [x] higher-term step-down
- [x] current-term leader barrier entry
- [x] check-quorum step-down

### P3 pre-vote checkpoint

- [x] timeout moves eligible voter to `PreCandidate`
- [x] prospective term is checked `current_term + 1`
- [x] pre-vote does not mutate `current_term`
- [x] pre-vote does not mutate `voted_for`
- [x] pre-vote emits no persistence effect
- [x] PreVote request/response message types implemented
- [x] requester must be voter in effective configuration
- [x] local learner never grants or campaigns
- [x] log freshness includes snapshot boundary
- [x] recent valid current-term leader contact suppresses grant
- [x] mismatched/synthetic leader contact cannot suppress grant
- [x] stable voter quorum counting deterministic
- [x] joint old/new majorities required independently
- [x] duplicate/stale/foreign responses cannot inflate quorum
- [x] quorum notification emitted exactly once per round
- [x] single-voter pre-vote quorum supported without starting real election
- [x] pre-candidate timeout starts a fresh deterministic round
- [x] valid leader contact cancels active pre-vote round
- [x] RequestVote/election/AppendEntries source-scope audit clean
- [x] pre-vote checkpoint qualification report written

### P3 RequestVote checkpoint

- [x] real campaign can start only from a matching qualified pre-vote quorum
- [x] `BeginElection` stages checked `current_term + 1` and self-vote
- [x] hard state remains old until persistence completion
- [x] outbound RequestVote waits for durable term/self-vote
- [x] candidate self-vote counts only after persistence completion
- [x] RequestVote request/response message types implemented
- [x] RequestVote candidate identity must be an effective voter
- [x] learner persists a higher term but never grants voting authority
- [x] same-term grant persists `voted_for` before positive response
- [x] repeated same-candidate vote may be granted from durable hard state
- [x] one durable vote per term survives restart
- [x] crash before vote persistence cannot create an acknowledged vote
- [x] higher-term request persists new term and clears/sets vote atomically
- [x] higher-term stale candidate is rejected only after new term is durable
- [x] higher-term vote response durably fences a candidate before step-down effect
- [x] foreign/non-voter request/response identity cannot raise or fence term
- [x] stable election vote tracking deterministic
- [x] joint old/new election majorities required independently
- [x] duplicate/stale/foreign vote responses cannot inflate quorum
- [x] election quorum notification emitted exactly once
- [x] election quorum does not transition `Candidate` to `Leader`
- [x] campaign hard-state persistence failure is fatal and sends no RequestVote
- [x] consensus inputs are fenced while a hard-state persistence barrier is active
- [x] core-owned persistence staging cannot be invoked by embedding code
- [x] leader transition/LeaderBarrier/AppendEntries source-scope audit clean
- [x] RequestVote checkpoint qualification report written

### P3 leader-initialization checkpoint

- [x] `WinElection` requires current `Candidate` role
- [x] winning term must equal durable `current_term`
- [x] durable self-vote must belong to local node
- [x] matching election round must have qualified quorum
- [x] local node must still be a voter in effective configuration
- [x] legal `Candidate -> Leader` transition implemented
- [x] `leader_id` becomes local node
- [x] pre-vote/election campaign state cleared on victory
- [x] per-member progress initialized in deterministic `NodeId` order
- [x] peers start with `match_index = 0`
- [x] peers start with `next_index = pre-barrier durable last + 1`
- [x] self match starts at pre-barrier durable last only
- [x] learners receive replication progress without voting authority
- [x] current-term `LeaderBarrier` created at next logical index
- [x] snapshot boundary supplies barrier predecessor when suffix is empty
- [x] leader transition emits no AppendEntries/heartbeat
- [x] barrier append is staged through persistence effect only
- [x] stable log excludes barrier before persistence completion
- [x] self match advances to barrier only after persistence completion
- [x] self next index advances only after barrier persistence
- [x] barrier persistence does not advance commit index
- [x] barrier persistence failure is fatal/non-serving
- [x] AppendEntries/conflict/commit source-scope audit clean
- [x] leader-initialization checkpoint qualification report written

## D. Replication

- [x] AppendEntries heartbeat
- [x] prefix term/index matching
- [x] conflicting uncommitted suffix truncation
- [x] committed suffix truncation impossible/fatal
- [x] follower append persistence before success
- [x] duplicate AppendEntries idempotence
- [x] conflict-term/index optimization
- [x] stable per-peer next/match progress
- [x] bounded uncommitted application bytes

### P3 AppendEntries heartbeat/prefix checkpoint

- [x] AppendEntries request/response conceptual message types
- [x] lower-term request rejected with local current term
- [x] foreign/non-voter leader identity cannot raise term
- [x] higher-term request stages hard-state persistence before response
- [x] higher-term persistence failure emits no AppendEntries response
- [x] higher-term request clears stale vote through hard-state update
- [x] current-term candidate steps down before prefix validation
- [x] valid current-term leader authority resets election timer
- [x] one accepted leader identity per term is enforced locally
- [x] leader rejects competing same-term authority without losing progress
- [x] learner accepts valid heartbeat while remaining learner
- [x] snapshot-boundary prefix match
- [x] retained-log prefix match
- [x] prefix below snapshot floor returns `snapshot + 1` hint
- [x] missing prefix beyond local tail returns `last + 1` hint
- [x] retained term mismatch returns conflict term + first retained index
- [x] snapshot-boundary term mismatch returns snapshot term/index hint
- [x] empty heartbeat success reports matched durable prefix
- [x] heartbeat success does not mutate stable log
- [x] heartbeat success does not advance commit in this checkpoint
- [x] non-empty matching AppendEntries blocked before log mutation
- [x] bad-prefix non-empty AppendEntries still returns deterministic prefix rejection
- [x] heartbeat requiring commit advancement blocked by explicit checkpoint guard
- [x] same-term AppendEntries response cannot advance peer progress yet
- [x] higher-term AppendEntries response durably fences leader/candidate
- [x] foreign higher-term AppendEntries response cannot fence group
- [x] term fencing clears stale leader-only peer progress
- [x] no new truncate/append/commit/progress mutation in checkpoint diff
- [x] heartbeat/prefix checkpoint qualification report written

## E. Commit/apply

- [x] stable majority calculation
- [x] joint old+new majority calculation
- [x] current-term entry required for quorum-based commit advancement
- [x] durable commit watermark monotonic
- [x] apply strictly in log order
- [x] never apply above commit index
- [x] prior-term entries commit indirectly through current-term prefix

## F. Learners and membership

- [x] learner replication
- [x] learners do not vote/campaign/count toward quorum
- [x] learner promotion catch-up validation
- [x] EnterJoint
- [x] independent old/new election quorum
- [x] independent old/new commit quorum
- [x] LeaveJoint only after joint entry committed
- [x] one pending configuration change maximum
- [x] leader self-removal step-down
- [x] recovery reconstructs configuration from snapshot+log

## G. Snapshots

- [x] snapshot only at committed/applied boundary
- [x] snapshot includes stable membership
- [x] no local snapshot while unresolved joint configuration exists
- [x] InstallSnapshot stale handling
- [x] matching-boundary suffix retention
- [x] conflicting suffix discard
- [x] snapshot durability before success response
- [x] snapshot/apply ordering
- [x] ordinary catch-up switches to snapshot below retained boundary

## H. Leadership transfer

- [x] target must be eligible voter
- [x] target caught up before TimeoutNow
- [x] new proposals paused/rejected during transfer
- [x] target wins through legal election, not direct assignment
- [x] failed transfer resumes service safely

## I. Deterministic simulation

- [x] seeded RNG
- [x] logical clock
- [x] deterministic peer iteration
- [x] packet drop
- [x] packet duplicate
- [x] packet delay
- [x] packet reorder
- [x] node crash/restart
- [x] persistence delay/failure
- [x] exact failing-seed reproduction
- [x] mandatory scenarios 1–25 from `RAFT_V1.md`
- [x] large seeded campaigns

## J. Formal model

- [x] TLA+ (or equivalent) model checked
- [x] election safety checked
- [x] log matching checked
- [x] leader completeness checked
- [x] state-machine safety checked
- [x] committed-prefix preservation checked
- [x] minority cannot commit checked
- [x] joint quorum intersection checked
- [x] higher-term fencing checked
- [x] crash/restart persistence boundaries represented

## K. Scope audit

- [x] no broker dependency needed for Raft safety
- [x] no P4 partition transport/integration pulled forward
- [x] no controller policy pulled forward
- [x] no consumer groups/idempotency/queues/compression pulled forward
- [x] P1 disk bytes unchanged

## L. Qualification and closure

- [x] `cargo fmt --all -- --check`
- [x] `cargo clippy --workspace --all-targets --all-features -- -D warnings`
- [x] `cargo test --workspace --all-features`
- [x] deterministic campaign qualification green
- [x] formal model qualification green
- [x] P3 closure report written
- [x] clean working tree
- [x] P3 phase commit created
