# AstraLog P5-M Leadership Hook Qualification

Date: 2026-09-28

Status: **PASS — P5-M QUALIFIED**

## Scope

This checkpoint implements and qualifies exactly P5-M actions P5-158 through P5-162:

1. an explicit preferred-leader target hook;
2. committed-assignment and broker-lifecycle eligibility validation;
3. P3 catch-up validation before transfer request;
4. reuse of the existing P3 leadership-transfer primitive;
5. observation of P3's result rather than controller leadership decree; and
6. an explicit boundary that autonomous continuous leader balancing remains P11.

P5-P failure qualification and P5-Q phase closure remain outside this checkpoint.

## Ownership boundary

Replica placement and partition leadership remain separate concerns.

`PreferredLeaderRequest` identifies only `(topic_id, partition_id, target_broker_id)`. The
hook does not create or mutate an `AssignmentRecord`, does not advance assignment generation,
does not create a `MovementRecord`, and does not write a preferred leader into controller
Raft state.

Eligibility is read from a fresh controller-authoritative metadata snapshot. Actual
leadership remains exclusively P3 authority.

The explicit hook is available only on a broker running in dynamic-controller mode:

`BrokerHandle::request_preferred_leader(PreferredLeaderRequest)`

It is bounded by the existing pending-admin semaphore and by
`DynamicControllerConfig.preferred_leader_timeout` (default five seconds). Zero timeout is
rejected during configuration validation.

## Eligible target contract

Before a transfer request can reach P3, P5-M requires all of the following:

- the controller has a committed assignment for the topic/partition;
- no non-complete reassignment exists for that partition;
- the target broker is present in committed controller membership;
- the target broker lifecycle is `ACTIVE`;
- the target is in the committed assignment voter set and is not a learner;
- local P3 membership is stable with no pending configuration entry;
- P3 stable membership exactly matches the controller assignment; and
- on the current P3 leader, the target's match index is at least the leader's durable last
  index.

The controller-authoritative snapshot is read again immediately before the hand-off to P3.
This second validation fences a lifecycle, assignment or movement change that occurs while a
target is catching up. Reading that snapshot is observation-only: the explicit leadership
hook does not reconcile or rebuild local replica actors.

## P3 transfer reuse

P5-M introduces no new election algorithm.

The request path is:

```text
explicit PreferredLeaderRequest
    -> fresh committed controller metadata validation
    -> local P3 stable-membership observation
    -> target caught up through leader durable tail
    -> ReplicationClient::transfer_leadership
    -> ReplicatedPartition::transfer_leadership
    -> frozen P3 Input::TransferLeadership
    -> ordinary P3 TimeoutNow / election semantics
    -> observe P3 leader_id == requested target
    -> return TransferObserved
```

If P3 already reports the requested target as leader, the hook returns `AlreadyPreferred`
without initiating another transfer.

## Observe, never decree

`PreferredLeaderOutcome` contains the requested partition, target, observed leader epoch and
one of two dispositions:

- `AlreadyPreferred`; or
- `TransferObserved`.

Neither result is derived from RPC-send success. `TransferObserved` is returned only after a
subsequent P3 membership observation names the target as leader.

Qualification exposed one important completion bug while building this hook: after a valid
P3 transfer is initiated, the old leader is expected to stop being `Role::Leader`. The first
implementation incorrectly re-required old-leader role before it could observe the result,
causing a successfully initiated transfer to be reported as `NotLeader` and retried by the
test. The final implementation separates pre-transfer authority/catch-up validation from
post-transfer observation. Once the request has been issued, the old leader may be a follower
and can still observe `leader_id == target` as the successful P3 result.

This preserves the frozen ownership rule: the control plane requests and observes; P3 elects.

## No autonomous balancing policy

P5-M does not add a background leader balancer, scoring loop, preferred-leader table or
periodic transfer scheduler.

`DynamicControlRuntime` continues to run only:

1. controller metadata refresh;
2. broker heartbeat/progress reporting; and
3. bounded crash-resumable movement reconciliation.

It never calls the general preferred-leader hook. Continuous balancing, operational policy,
drain orchestration and fleet-wide leader distribution remain P11.

## Qualification scenario

The real RF3 P5-M integration scenario starts three controllers plus four dynamic brokers and
creates one RF3 partition assigned to voters `[1,2,3]`.

It proves:

- public P3 leadership exists and quorum Produce succeeds before transfer;
- broker 4 is rejected as a non-voter preferred target;
- rejected preference does not advance controller metadata epoch and creates no movement;
- a current P3 leader accepts only a fully caught-up committed voter;
- the existing P3 transfer primitive performs the real leadership change;
- completion waits for P3 to observe the requested target as leader;
- public Metadata subsequently exposes that P3 leader/term;
- repeating the same explicit preference returns `AlreadyPreferred`;
- controller metadata epoch remains unchanged and movement state remains empty; and
- quorum Produce plus ReadCommitted Fetch continue after the transfer.

The integration harness probes committed voters concurrently only to find the transient local
P3 leader. Followers reject locally; there is no hidden forwarding and no controller decree.

## Mechanical qualification

On the final pre-commit P5-M bytes:

- `cargo fmt --all -- --check`: **PASS**;
- workspace Clippy with `-D warnings`: **PASS**;
- full workspace: **370 passed, 0 failed**;
- broker library: **12/12**;
- P5 dynamic integration: **4/4**;
- controller: **82/82**;
- protocol: **35/35**;
- frozen protocol goldens: **11/11**;
- Raft: **122/122**;
- replication: **46/46**;
- storage: **43/43**;
- full P5 dynamic suite repeated **5/5 PASS**;
- frozen P4 RF3 E2E repeated **5/5 PASS**;
- full controller suite repeated **5/5 PASS**.

## Frozen-boundary audit

P5-M changes only broker control/integration surfaces plus P5 documentation/tests. It does not
change Storage V1 bytes, public Wire V1 protocol bytes, controller command/snapshot formats,
P3 consensus source, replication semantics, producer-ID allocation or feature-level state.

The broker crate only widens visibility of its existing `broker_to_node` mapping helper so
P5-M can validate the exact P3 voter identity before invoking the already-existing transfer
primitive. The `astralog-replication` crate itself is unchanged.

P5-P actions P5-163 through P5-200 and all P5-Q closure actions remain unchecked.

## Next checkpoint

The next bounded checkpoint is **P5-P only — failure qualification, actions P5-163 through
P5-200**. P5-Q closure remains disabled until P5-P is independently qualified.
