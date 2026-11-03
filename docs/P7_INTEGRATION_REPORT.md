# AstraLog P7-H Broker / Client Integration Qualification

Date: 2026-10-04

Status: **PASS — P7-H QUALIFIED / P7-I NEXT**

## Scope

P7-H closes actions **P7-107 through P7-121** only. It is built on qualified P7-G commit
`ef99267d6deaddfcf405f9ba416e73c962da67cc` and exposes the already-qualified P7 controller and
partition runtime through readiness-gated broker Wire V1 dispatch plus bounded client producer
session/retry behavior.

P7-I and later remain disabled. This checkpoint does not claim RF3 producer failover, global
exactly-once processing, transactions, P8 batching/compression or any later-phase behavior.

## Broker public activation

The broker public surface preserves the P7-G feature gate:

- `InitProducer` V0 dispatch is accepted only when the broker's committed dynamic metadata reports
  P7 feature level 3 or newer;
- Produce V1 is rejected before that same readiness boundary while Produce V0 remains available;
- ApiVersions advertises feature key 11 only when P7 is usable;
- ApiVersions advertises `InitProducer` V0 only when P7 is usable;
- ApiVersions raises Produce's maximum version from 0 to 1 only when P7 is usable;
- public InitProducer routes through the existing bounded dynamic-controller path rather than
  creating broker-local producer authority;
- public Produce V1 routes through the already-qualified idempotent partition runtime rather than
  duplicating sequence/fencing logic in the socket layer;
- the public P7 error surface maps controller/session/routing failures onto the frozen registry,
  including `UNKNOWN_PRODUCER`, `PRODUCER_FENCED`, `OUT_OF_ORDER_SEQUENCE`, routing errors and
  bounded transient errors.

The server also contains an explicit pre-activation unit proof that InitProducer remains unusable
and Produce V1 remains unsupported before the committed P7 readiness gate.

## Bounded client producer session

`astralog-client` contains a dedicated idempotent-producer session separate from the legacy
Produce V0 path:

- authoritative ProducerId/ProducerEpoch state established only by successful InitProducer;
- a finite per-producer partition-state map with a configured hard bound;
- per-partition `next_sequence` beginning at zero for each accepted producer epoch;
- at most one unresolved Produce V1 range per producer-partition;
- sequence reservation before send without advancing committed `next_sequence` merely because a
  send was attempted;
- bounded automatic retries using the exact encoded Produce V1 body retained in pending state;
- terminal producer/sequence errors ending automatic retry rather than inventing a new range;
- authoritative success, including a broker dedupe success, advancing sequence exactly once and
  returning the broker's original committed offset range;
- producer restart through InitProducer compare-and-swap epoch bump, with per-partition sequence
  state reset lazily by replacing the old session map after authoritative success;
- ambiguous InitProducer handling that clears local producer authority instead of assuming the old
  epoch remains valid;
- an explicit recovery operation that creates a fresh producer session after an InitProducer
  outcome cannot be proven.

Client unit qualification checks the producer-state bound, one
pending range, exact retained Produce V1 body, no pre-success sequence advance, terminal/retryable
classification and next-sequence advancement only after success.

## Public RF1 lost-response proof

`crates/astralog-broker/tests/p7_runtime.rs` contains
`p7_public_rf1_init_produce_restart_and_lost_response_retry_commit_once`.

The test topology is a real three-controller quorum plus one dynamic RF1 broker with P7 feature
level 3. A bounded TCP proxy is used only to inject client-visible response loss. The scenario is
designed to prove:

1. the first committed new-session InitProducer response is discarded;
2. the client reports an ambiguous mutation and retains no assumed producer authority;
3. explicit recovery allocates a fresh producer session rather than reusing the ambiguous one;
4. the first successful Produce V1 response is discarded after the broker processed it;
5. the client automatically retries the same pending request body;
6. the proxy captures the first and retry Produce V1 bodies and requires byte equality;
7. the returned success contains the original `(0, 0)` offset range;
8. a read-committed Fetch observes exactly one logical record and high watermark 1;
9. a producer epoch bump preserves ProducerId, advances ProducerEpoch exactly once and accepts the
   new epoch's first partition sequence at zero.

This is the P7-121 RF1/public smoke. It does not substitute for the RF3 leader-loss and fencing
campaign owned by P7-I.

## Qualification discipline

The first broad P2/P4/P5/P6 compatibility command allowed Rust's default parallel test execution.
One long P6 churn test reported a failure while several other RF3 tests were simultaneously running.
That exact test then passed **1/1** in isolation, and the complete P6 integration suite passed
**9/9** under the project's canonical serialized `--test-threads=1` discipline. No P7-H source
change was required. The final complete workspace gate was also run serialized so RF3 timing tests
did not compete for the same machine resources.

## Mechanical qualification

- exact public RF1 lost-response P7-H smoke: **1/1 PASS**
- complete P7 runtime integration: **2/2 PASS**
- broker unit tests: **22/22 PASS**
- client unit tests: **11/11 PASS**
- P2 network compatibility: **9/9 PASS**
- P4 RF3 compatibility: **1/1 PASS**
- P5 dynamic compatibility: **4/4 PASS**
- P6 RF3 integration, serialized: **9/9 PASS**
- common: **3/3 PASS**
- controller: **104/104 PASS**
- groups: **55/55 PASS**
- protocol: **44/44 PASS**
- P2/P6/P7 goldens: **11/11 + 6/6 + 4/4 PASS**
- Raft: **122/122 PASS**
- replication: **73/73 PASS**
- storage: **44/44 PASS**
- complete serialized all-target workspace: **524/524 PASS**
- workspace doc tests: **PASS**
- workspace Clippy `--all-targets --all-features -D warnings`: **PASS**
- `cargo fmt --all -- --check`: **PASS**
- `git diff --check`: **PASS**

## Boundary

P7-H is qualified. The next clean checkpoint is:

> **P7-I only — P7-122 through P7-135: lost-response, fencing and real RF3 failover qualification**

The P7-H checkpoint containing this report is authoritative for phase advancement only after that
exact commit HEAD is independently requalified. P7-J and later remain disabled until P7-I
independently qualifies.
