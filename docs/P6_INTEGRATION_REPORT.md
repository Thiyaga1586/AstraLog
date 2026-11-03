# P6-J Broker / Client Integration Qualification

Date: 2026-10-01

Status: **PASS — P6-J QUALIFIED / P6-K NEXT**

This report closes actions **P6-125 through P6-138 only**. P6-J activates the already-frozen public consumer-group APIs behind the committed feature/readiness gate, routes all public group authority through the P6-H coordinator, routes offset operations from that coordinator to the independently authoritative P6-I offsets shard, and adds bounded Rust-client group state/retry/cooperative-revoke behavior. P6-K churn/failure/model campaigns and P6-L compatibility/closure remain intentionally open.

## Public activation boundary

Public API keys 30..35 are advertised only when all of the following are true on the broker:

- the dynamic P6 service exists;
- committed controller `active_feature_level >= 2`;
- `__astralog_groups` and `__astralog_offsets` match the frozen 64-partition RF3 active topology in committed metadata;
- the local replicated topology contains the same frozen internal-topic shape.

Before that gate opens, the existing public dispatcher still treats allocated P6 keys as unsupported. A raw Wire V1 JoinSubscribe request before activation returns exact `UNSUPPORTED_API_KEY` and an empty legacy error body. The RF3 qualification then commits feature level 2 and observes the APIs becoming usable without changing their frozen V0 wire layouts.

## Group-coordinator authority

JoinSubscribe, Heartbeat, RevokeAck, Leave, CommitOffsets and FetchOffsets all enter `P6Service` and first resolve the GroupId's current `__astralog_groups` P3 leader/term. A broker that is not that exact leader returns `NOT_LEADER` plus the current coordinator hint before group mutation.

The direct wire qualification sends JoinSubscribe to a deliberately wrong activated broker, verifies the exact leader broker/term hint, then reconstructs the authoritative group state on the real coordinator and proves it is unchanged. The public socket layer therefore does not create a forwarding authority or client-selected coordinator plane.

Group mutations continue to use the already-qualified P6-H bounded worker and quorum/replay commit-before-response contract. Heartbeat refreshes only the leader-local P6-G session tracker.

## Cross-broker offsets routing

Group and offsets shards are separate Raft groups and may have different leaders. P6-J therefore does not ask the client to discover or race an offsets coordinator.

The group coordinator performs a bounded broker-internal P6 offset request to brokers in the committed offsets-shard assignment. The receiver validates the source broker ID/node UUID/incarnation against committed P5 broker authority, requires itself to be the current P3 offsets-shard leader, and then enters the already-qualified P6-I worker.

This private hop uses implementation-private key `65000`; it is not part of the public `ApiKey` registry and is never advertised by ApiVersions. It does not alter the frozen P4 Raft peer transport. P11 remains responsible for production cryptographic transport authentication/security; P6-J uses the project's current committed broker-identity trust boundary.

Mutation routing is bounded to a small finite candidate/leader-hint retry set. Idempotent group/owner fence writes may safely repeat their exact durable record. CommitOffsets is different: if transport loss occurs after it may have been sent, P6-J returns mutation ambiguity rather than blindly issuing a second public commit.

## Cooperative orchestration and bounded recovery

The server orchestration reuses the frozen P6-F ordering:

1. commit `PREPARING_EPOCH_FENCE` group state;
2. commit the matching offsets group fence;
3. activate the prepared GroupEpoch;
4. keep moved old ownership active while REVOKING;
5. durably release acknowledged old ownership;
6. commit the target owner fence;
7. activate the durably fenced target owner.

P6-H committed-history replay remains authoritative. P6-J adds a bounded workflow-history view that still validates every committed group record from offset zero but retains only the current suffix beginning at the latest `PREPARING_EPOCH_FENCE`. `CooperativeStateMachine::recover_workflow_suffix` accepts only such a validated PREPARING snapshot followed by exact canonical transitions. The existing full-history `recover` behavior is unchanged.

A dedicated regression reconstructs PREPARING -> activated -> released state from that bounded suffix and regenerates the required owner fence. A suffix that starts after PREPARING fails closed.

## Rust client authority and retry behavior

`AstraClient` now owns bounded per-group state:

- coordinator route: GroupId, shard, groups TopicId, broker ID and coordinator P3 term;
- member authority: MemberId, GroupEpoch and MemberEpoch;
- current active assignments and revocation set;
- heartbeat recommendation.

Coordinator cache entries never regress term. Member state never regresses the `(GroupEpoch, MemberEpoch)` tuple, and an identical tuple cannot change MemberId.

The group request path has a hard semaphore bound plus a finite retry count. Retryable routing failures refresh coordinator discovery. Network timeout/connection loss on a mutation is surfaced as `AmbiguousMutation`; it is not silently replayed. A broker `NOT_LEADER` is a routing observation and remains distinguishable from transport ambiguity.

The public retry-matrix regression binds the frozen registry classifications for P6 routing/session-progress errors and terminal invalid-offset/feature/API errors.

## Cooperative client workflow

`cooperative_revoke_plan` explicitly separates partitions that remain active from those that must stop. `complete_cooperative_revoke` may commit offsets for the exact revoked set, sends exact RevokeAck, then accepts the committed post-fence assignment response.

The RF3 E2E uses a two-partition topic. After a second member joins, one partition moves while one remains continuously owned by the first member. The first member commits offset `77` for the revoked partition before acknowledgement; after release/owner-fence/activation, the second member owns that partition and committed FetchOffsets returns `77`. The unaffected partition never leaves the first member's processing set.

The same scenario then performs public Leave from the second member. The local client authority is removed after success, GroupEpoch advances again, and the remaining member becomes STABLE with both partitions only through the ordinary fence workflow.

## Public RF3 qualification

The real scenario uses three controllers, four dynamic brokers, the real 64-partition RF3 P6 internal topics, and a separate RF3 user topic. It proves:

- raw key-30 `UNSUPPORTED_API_KEY` before activation;
- feature-level activation of public 30..35;
- non-coordinator JoinSubscribe -> hinted `NOT_LEADER` with no mutation;
- dynamic member admission and exact local member authority;
- Heartbeat through exact group-coordinator authority;
- group coordinator and offsets leader deliberately on different brokers;
- CommitOffsets group-coordinator -> remote offsets-leader execution;
- committed FetchOffsets through the same two-stage route;
- second-member cooperative rebalance;
- unaffected partition continues processing;
- optional pre-revoke offset commit, durable release, owner fence and target activation;
- committed handoff offset recovered by the new owner;
- public Leave and higher-epoch reassignment back to the surviving member.

The focused P6-J scenario passes **1/1**. The complete serialized P6 routing/failover suite passes **5/5**.

## Resource bounds

No unbounded P6-J request/task/cache path was introduced:

- client coordinator cache: hard configured count bound;
- client member cache: same hard configured count bound;
- client group requests: hard semaphore bound;
- client retries: finite configured maximum capped during configuration validation;
- server volatile session groups: hard bound of 1,024;
- P6-H group and P6-I offsets workers: existing bounded control/mutation channels, byte semaphores and replay pages;
- private offsets routing: finite committed-assignment candidates and bounded attempts;
- wire/request collections retain the frozen P6 protocol caps.

This closes the phase-exit hard-bound invariant. P6-K still owns stress/scheduling qualification under sustained churn.

## Mechanical qualification

- `cargo fmt --all -- --check` — **PASS**
- `git diff --check` — **PASS**
- `cargo clippy --workspace --all-targets -- -D warnings` — **PASS**
- broker unit tests — **19/19 PASS**
- P2 network integration — **9/9 PASS**
- P4 RF3 integration — **1/1 PASS**
- P5 dynamic integration — **4/4 PASS**
- P6 RF3 routing/failover/public integration — **5/5 PASS**
- client — **10/10 PASS**
- common — **2/2 PASS**
- controller — **91/91 PASS**
- `astralog-groups` — **51/51 PASS**
- protocol — **38/38 PASS**
- frozen P2 golden vectors — **11/11 PASS**
- P6 golden vectors — **6/6 PASS**
- Raft — **122/122 PASS**
- replication — **47/47 PASS**
- storage — **43/43 PASS**
- complete workspace — **459/459 PASS** using `cargo test -j 1 --workspace --all-targets -- --test-threads=1`

The first full-workspace attempt used Cargo's default parallel compilation and exhausted the local Windows paging file (`os error 1455`) before tests could run. That was an environment resource failure, not a source/test assertion. Re-running the identical all-target test set with Cargo build concurrency fixed to one completed cleanly at **459/459**.

## Boundary

P6-K/P6-L and P7+ remain disabled. P6-J does not claim:

- sustained join/leave/crash/slow-heartbeat churn qualification;
- coordinator/offsets-leader failure at every active group workflow boundary;
- seeded churn/fence model campaigns beyond the already-qualified lower-layer models;
- resource/scheduling stress under concurrent join/offset pressure;
- the P6 compatibility/closure audit;
- P7 producer/request idempotency or deduplication;
- P9 physical compaction/retention;
- P10 queue lease/retry/DLQ semantics;
- P11 security/drain/continuous balancing policy.

The next bounded checkpoint is **P6-K only — actions P6-139 through P6-158: failure/churn/model qualification**.
