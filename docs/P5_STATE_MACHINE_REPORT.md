# AstraLog P5-B Deterministic Controller State Machine Qualification

Date: 2026-09-27

Status: **PASS — P5-B STATE MACHINE QUALIFIED**

P5-A contract base:

ad265118e5e6e422f7f3c6fb7baf367b1b56907e — P5 freeze controller and placement contract v1

## Scope

P5-B implements only actions P5-013 through P5-020:

- new astralog-controller crate;
- checked controller-native identity/generation types;
- all frozen ControllerCommandV1 variants;
- bounded/canonical command codec;
- command golden vectors;
- deterministic ControllerState;
- pure ordered state-machine apply;
- impossible-transition rejection.

P5-B deliberately contains no controller disk persistence, socket transport, timers, async runtime, P3 driver, broker runtime integration, placement planner, heartbeat detector or replica-reconciliation loop.

## Dependency proof

Normal dependency tree:

~~~text
astralog-controller
├── astralog-common
└── astralog-protocol
    ├── astralog-common
    └── crc32c
~~~

The controller state machine therefore cannot perform disk, broker, replication or consensus-runtime I/O.

## ControllerCommandV1 implementation

All twelve frozen V1 kinds are implemented:

1. RegisterBroker
2. RejoinBroker
3. SetBrokerLifecycle
4. CreateTopic
5. BeginDeleteTopic
6. FinalizeDeleteTopic
7. ExpandTopic
8. BeginMovement
9. AdvanceMovement
10. FinalizeMovement
11. SetFeatureLevel
12. AllocateProducerIds

Encoding is canonical little-endian with exact envelope body length. Decode rejects malformed/truncated input, unsupported command kinds, configured limit violations, invalid UTF-8, invalid option markers/enums, non-canonical replica ordering, duplicate replica identities, body-length mismatch and trailing bytes. Decode re-encodes the logical value and rejects any alternate byte representation.

Exact byte goldens are asserted for representative frozen mutation commands, while all twelve variants have canonical round-trip coverage.

## Pure state-machine semantics

ControllerState uses deterministic BTreeMap state for broker, topic, assignment and movement tables. A command is first structurally validated, then applied to a staged clone. Live state and metadata_epoch change only when the complete transition succeeds.

Mechanically tested state rules include:

- same ordered commands produce identical state;
- metadata log/apply index must strictly advance;
- duplicate broker ID or node UUID is rejected atomically;
- first broker incarnation is exactly one;
- rejoin requires the same UUID and exact next incarnation;
- removed/fenced authority cannot be implicitly resurrected;
- topic creation is atomic with assignments;
- initial/expanded assignment targets must be known ACTIVE feature-compatible brokers;
- assignment voters are canonical distinct RF 1/3/5 sets and learners are disjoint;
- movement is serialized per partition;
- movement stages cannot skip the frozen safety sequence;
- final movement assignment generation is exactly previous + 1;
- feature-level bump is monotonic and rejected when an ACTIVE/DRAINING broker lacks support;
- producer-ID allocation returns monotonic non-overlapping ranges;
- producer-ID overflow is rejected atomically.

## P4 test-harness stabilization

During full-workspace qualification the existing P4 RF=3 TCP test exposed a pre-existing helper race: wait_for_leader had an outer five-second retry deadline but unwrapped a single metadata refresh whose client timeout was three seconds. A transient timeout therefore aborted instead of retrying.

The test helper now treats a transient metadata refresh error as a retry within its already-defined outer deadline. No P4 broker/replication production source or semantics changed.

After stabilization the RF=3 E2E passed **10 consecutive isolated runs** before the complete workspace gate.

## Qualification

Pinned Rust qualification environment: 1.98.1.

- cargo fmt --all -- --check: **PASS**
- workspace Clippy, all targets/features, offline, -D warnings: **PASS**
- cargo test --workspace --all-features --offline: **PASS**
- workspace total: **291 passed, 0 failed**
- astralog-controller: **19 passed, 0 failed**
- astralog-raft: **120 passed, 0 failed**
- astralog-replication: **46 passed, 0 failed**
- astralog-storage: **43 passed, 0 failed**
- protocol golden vectors: **11 passed, 0 failed**
- RF=3 P4 broker E2E: **PASS**, plus **10/10** isolated stabilization runs
- all doc-test targets: **PASS**
- git diff --check: **PASS**
- P3 source diff from P5-A base: **empty**
- P4 broker/replication production source diff from P5-A base: **empty**
- P1/P2/client/common source diff from P5-A base: **empty**

## Next checkpoint

P5-C only: durable controller P3 adapter + recovery/snapshots. Networking and broker/runtime integration remain out of scope until later checkpoints.
