# P6-G Heartbeats / Session Timeout / Static Membership Qualification

Date: 2026-10-01

Status: **PASS — P6-G QUALIFIED / P6-H NEXT**

This report closes actions **P6-085 through P6-096** only. P6-G qualifies the leader-local heartbeat/session-timeout foundation and the pure timeout/static-membership transitions that reuse the already-qualified P6-D/E/F state machines. It does **not** bind group mutations to replicated `__astralog_groups` history, implement offset-value state, activate broker/client APIs 30..35, or create coordinator runtime queues.

## Authority boundary

`SessionTracker` is deliberately volatile coordinator-leader state. Its observations contain only current MemberEpoch, the committed timeout configuration and an injected-monotonic deadline. No receive timestamp or deadline is encoded into `GroupRecordV1`, internal record bytes, P1 storage, controller metadata or any public protocol body.

Session expiry is therefore **liveness evidence, not authority**. `expired_members` produces a bounded deterministic `SessionTimeoutCandidate`. `prepare_timeout_removal` revalidates that the candidate still names the exact current GroupId/GroupEpoch/MemberId/MemberEpoch and that its observed deadline has not been refreshed, then emits the already-qualified durable `GroupCommand::Leave`. Only applying/committing that group transition removes member authority. P6-H remains responsible for binding that command to real P4/P3 commit-before-response semantics.

## Frozen timeout and heartbeat rules

- session timeout is accepted only in the inclusive range **6_000..=300_000 ms**;
- heartbeat recommendation is exactly `clamp(session_timeout_ms / 3, 1_000, 10_000)`;
- injected monotonic time may advance or remain equal but may never regress;
- deadline arithmetic is checked and never wraps;
- heartbeat admission requires exact current GroupId, active GroupEpoch, MemberId and MemberEpoch;
- a successful heartbeat refreshes only the leader-local deadline and leaves canonical durable GroupRecord bytes unchanged.

The protocol's existing JoinSubscribe V0 validator already enforces the same timeout range. P6-G only re-exports those frozen constants through the protocol API module so the group/session layer cannot drift from the wire contract; no request/response bytes changed.

## Coordinator leadership change

`reset_leadership` discards every old volatile observation and reconstructs one observation for every currently committed member. Each member receives its full committed `session_timeout_ms` beginning at the new leader's injected initialization time. No GroupEpoch, MemberEpoch, membership or assignment changes as a side effect of leadership change.

This intentionally favors safety over fast failure detection: failover can delay expiry by one full timeout, but it cannot create duplicate ownership or synthesize durable authority from an old timer.

## Timeout and crash-without-Leave

The timeout qualification proves the boundary in both directions:

- at deadline minus one millisecond, the member remains authoritative and may refresh its deadline;
- at/after the deadline, expiry creates only a candidate and does not mutate GroupRecord state;
- a heartbeat accepted before the durable removal commits invalidates the stale timeout candidate;
- once the existing Leave transition is applied, the expired member disappears from committed membership/active authority and the remaining members continue;
- P6-E computes the deterministic replacement target and P6-F completes the usual group-fence/owner-fence activation path to STABLE.

Thus crash without Leave uses the ordinary durable membership/rebalance machinery rather than introducing a timer-owned authority shortcut.

## Static membership

Static replacement reuses the existing P6-D `ReplaceStaticMember` command and P6-E assignor:

- the existing InstanceId/MemberId must already be committed;
- replacement requires the exact current MemberEpoch;
- the replacement record advances MemberEpoch by exactly one and prepares the normal strictly higher GroupEpoch;
- same-leader reconciliation detects the new MemberEpoch and gives that replacement session a fresh full timeout;
- the prior process's heartbeat is rejected immediately by the exact MemberEpoch gate.

When subscriptions/eligibility are unchanged, uniform-sticky assignment returns the existing ownership exactly, so the higher GroupEpoch activates to STABLE with **zero unnecessary ownership movement**. When subscriptions change, the replacement uses the ordinary higher-GroupEpoch P6-E target and P6-F revoke/release/fence/activate sequence.

## Scheduling-pressure foundation

P6-G freezes only a **pure scheduling decision**, not a coordinator queue: if session-control work is pending it is selected before ordinary join/offset work. A 10,000-cycle deterministic pressure model continuously grows ordinary backlog while injecting control work and proves every control arrival is selected immediately.

Actual bounded coordinator queues, byte/count admission and shutdown behavior remain P6-H/P6-J scope. This checkpoint therefore proves the priority rule required by P6-096 without prematurely implementing later runtime machinery.

## Mechanical qualification

Focused P6-G qualification:

- formatting — **PASS**
- workspace Clippy `--all-targets --all-features --offline -D warnings` — **PASS**
- `git diff --check` — **PASS**
- `astralog-groups` — **44/44 PASS**
- protocol — **38/38 PASS**
- frozen P2 protocol goldens — **11/11 PASS**
- P6 protocol goldens — **6/6 PASS**
- P6-C real RF3 routing/bootstrap/failover/restart — **1/1 PASS**
- frozen P5 dynamic integration — **4/4 PASS**
- controller — **91/91 PASS**
- Raft — **122/122 PASS**
- replication — **46/46 PASS**
- storage — **43/43 PASS**
- complete workspace — **438/438 PASS**

No P1 disk bytes, P2 request/response body bytes, P3 consensus semantics, P4 replication semantics or P5 controller authority changed. P6 API keys 30..35 remain unadvertised and undispatched.

## Boundary

P6-H and later remain untouched. In particular P6-G does not add:

- a replicated group-coordinator log/state runtime or commit-before-response network path;
- recovery of group authority from real committed `__astralog_groups` P4/P3 history;
- durable OffsetValue application or CommitOffsets/FetchOffsets execution;
- broker/client handlers or ApiVersions advertisement for API keys 30..35;
- P7+ behavior.

The next bounded checkpoint is **P6-H only — actions P6-097 through P6-108: replicated group coordinator and recovery**.
