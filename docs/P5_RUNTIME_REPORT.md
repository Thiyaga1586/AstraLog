# AstraLog P5-D Controller Runtime Qualification

Status: **QUALIFIED**

P5-D implements actions P5-035 through P5-044 only. P5-030 through P5-034
remain intentionally open for the separate P5-E bootstrap checkpoint. Broker
join/rejoin, broker heartbeats, placement and dynamic data-partition assignment
remain outside this checkpoint.

## Delivered scope

### Recovered P3 runtime

ControllerRuntime opens the qualified P5-C persistence adapter, constructs the
closed P3 RaftCore from RecoveredState, acknowledges only P3's deterministic
recovery Apply replay because ControllerState was already reconstructed from the
same committed history, and then becomes the sole owner of:

- the controller P3 core;
- controller persistence;
- applied ControllerState;
- pending controller mutations;
- bounded outbound controller Raft effects.

It does not decide elections, prefixes, votes, quorum commit, snapshot safety or
leadership independently from P3.

## Controller transport V1

P5-D introduces a controller-only transport frame with magic ASTCTRP1. The
controller namespace is physically distinct from the P4 data-partition frame
format and carries:

- cluster_id;
- source controller ID;
- one canonical P3 Raft message;
- optional controller snapshot package only for InstallSnapshot;
- CRC32C over frame header and payload.

The decoder is bounded before allocation/use, handles arbitrary TCP
fragmentation, accepts multiple sequential frames without resynchronizing after
corruption, rejects unknown flags/reserved fields, and rejects oversized Raft
entries or snapshot packages.

## Identity fencing

The peer listener checks that:

- cluster_id equals the configured controller group;
- source_controller_id is one of the statically configured peers;
- one TCP connection never changes source identity.

ControllerRuntime then independently verifies that the embedded P3 message
sender (candidate, voter, leader or follower ID depending on message type)
matches the transport source before delivering the message to P3.

Tests inject foreign-cluster and unknown-source frames and prove they never
reach the inbound controller consumer.

## Resource bounds and reconnect

The transport has:

- bounded inbound mpsc capacity;
- bounded outbound frame queue per peer;
- bounded outbound bytes per peer using a semaphore;
- bounded runtime outbound messages/bytes;
- bounded pending mutation count/bytes;
- bounded reconnect delay with exponential growth capped at the configured
  maximum.

Queue/byte exhaustion returns Busy or drops retryable Raft sends instead of
growing memory without bound. Raft ticks/control processing continue.

A startup shutdown race discovered during qualification was fixed by creating
the listener shutdown subscription before task spawn. The bounded-queue test
now exits deterministically even when its target peer is intentionally absent.

## Election and failover

The integration suite starts three real controller processes/tasks with:

- separate P5-C persistence directories;
- real TCP listeners and persistent peer writers;
- one static 3-voter P3 configuration;
- injected logical election timeout samples;
- the closed P3 election/check-quorum/replication logic.

The scenario proves:

1. a controller leader is elected by P3;
2. a follower rejects a controller mutation with NotLeader;
3. the leader replicates and quorum-commits a controller command;
4. the response completes only after ordered state-machine apply;
5. the committing leader is stopped;
6. the surviving majority elects a replacement in a higher term;
7. further metadata mutation commits on the replacement leader;
8. the old controller restarts from P5-C durable state at the same address;
9. persistent peer writers reconnect;
10. the restarted controller catches up to the committed metadata epoch.

The complete scenario passed **5 consecutive runs** before workspace
qualification.

## Mutation completion semantics

P3 ProposalResult(accepted=true) means the local proposal is durably accepted;
P5-D deliberately leaves the external mutation result Pending at that point.

External success occurs only when:

1. P3 has durably quorum-committed the log entry;
2. P3 emits Apply for the entry;
3. ControllerState successfully applies ControllerCommandV1 at that Raft index.

Followers never silently forward mutations.

## Ambiguous lost responses

A committed mutation response may be lost or leadership may change before the
caller observes the response. Retrying is a new command. For producer-ID range
allocation this can consume multiple ranges before one response is observed.
The safe rule is therefore:

- gaps are allowed;
- reuse is forbidden.

The P5-D test intentionally drops a committed allocation response, retries
through possible additional leadership changes and proves the eventual range is
strictly beyond the lost range and aligned to the requested allocation size.

## Qualification evidence

Pre-documentation qualification from the P5-D source state:

- cargo fmt --all -- --check: PASS
- workspace Clippy with -D warnings: PASS
- full workspace tests: **314 passed, 0 failed**
- astralog-controller: **42 passed, 0 failed**
- astralog-raft: **120 passed, 0 failed**
- astralog-replication: **46 passed, 0 failed**
- astralog-storage: **43 passed, 0 failed**
- protocol goldens: **11 passed, 0 failed**
- all doc tests: PASS
- P1-P4 production-source diff from P5-C: empty
- git diff --check: PASS

## Scope audit

P5-D does not implement:

- cluster bootstrap manifest or first-cluster creation;
- broker membership registration/rejoin runtime;
- broker incarnation heartbeat fencing;
- broker heartbeat/failure detector;
- topic placement planner;
- metadata dissemination to brokers;
- dynamic P4 partition assignments;
- replica movement orchestration.

The next checkpoint is P5-E cluster bootstrap: actions P5-030 through P5-034.
