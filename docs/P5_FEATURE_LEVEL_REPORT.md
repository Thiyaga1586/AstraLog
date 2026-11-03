# AstraLog P5-N Active Cluster Feature Level Qualification

Date: 2026-09-27

Status: **PASS — P5-N QUALIFIED**

## Scope

This checkpoint implements and qualifies exactly P5-N actions P5-145 through P5-151:

- durable active cluster feature-level authority;
- broker supported feature-range advertisement;
- controller-voter supported feature-range advertisement;
- compatibility rejection before a feature bump can enter controller Raft;
- explicit commit/apply-only feature activation;
- monotonic P5 feature-level semantics;
- fail-closed feature gates for later irreversible behavior.

P5-O producer-ID allocator closure, P5-M general leadership hooks and P5-P/Q remaining
failure/phase-closure work are deliberately outside this checkpoint.

## Durable active feature level

The active cluster feature level was already part of the frozen P5 controller state and
ControllerSnapshot V1 format. P5-N does not change those bytes. Instead, this checkpoint
qualifies the primitive as production authority:

```text
explicit bump request
        -> current controller leader
        -> controller/broker compatibility validation
        -> frozen SetFeatureLevel command
        -> controller Raft commit
        -> deterministic ControllerState apply
        -> metadata delta/snapshot dissemination
        -> broker committed metadata cache
        -> FeatureGate becomes eligible
```

`SetFeatureLevel` remains ControllerCommand V1 kind 11 and its frozen exact command fixture
is unchanged. A successful bump therefore advances the same authoritative controller Raft
history and metadata epoch as the rest of P5 metadata.

## Broker supported feature ranges

Broker feature support remains durable broker metadata. Registration and rejoin carry
`min_feature` and `max_feature`; P5-N tightens canonical validation so both bounds must be
non-zero and `min_feature <= max_feature`.

Every heartbeat continues to advertise the broker range and must exactly match the current
committed `BrokerRecord`. This prevents a running broker process from silently claiming a
different capability set after registration.

Compatibility is lifecycle-aware:

- ACTIVE and DRAINING brokers must support any requested new active feature level;
- an incompatible ACTIVE/DRAINING broker rejects the bump atomically;
- a JOINING broker may remain recorded while the cluster advances, but it cannot transition
  to ACTIVE if it does not support the current committed active level;
- FENCED/REMOVED brokers do not block a bump because they have no serving authority.

The real P5-N scenario proves that a broker limited to level 1 may remain JOINING after a
committed level-2 bump but cannot become ACTIVE.

## Controller supported feature ranges

`ControllerRuntimeConfig.controller_feature_ranges` is the P5-N controller compatibility
input. It is keyed by `ControllerNodeId` and must cover exactly the frozen static controller
voter set. Missing, extra or invalid advertisements fail runtime startup.

`ControllerStatus` exposes the local controller's advertised `min_feature` and
`max_feature`, along with the current committed active level. This makes the controller's
capability range observable without changing controller transport framing or the P5-E
bootstrap manifest.

Before a leader accepts `SetFeatureLevel`, it verifies that every required static controller
voter advertises support for the proposed level. This happens before command encoding or
Raft proposal. The deterministic replicated state machine then independently checks all
ACTIVE/DRAINING brokers, giving separate controller-side and broker-side compatibility
barriers.

The runtime also validates capability against the **recovered durable** active level. This
is critical for restarts: after the cluster commits level 2, a controller restarted with a
range ending at level 1 refuses to serve rather than participating below the active feature
contract.

P11 still owns rolling-upgrade orchestration and policy. P5-N only provides the durable
primitive and compatibility guard required by later upgrade workflows.

## Explicit commit-only activation

`ControllerNodeHandle::bump_feature_level(new_level)` obtains current leader status and
constructs the frozen command:

```text
SetFeatureLevel {
    expected_feature_level: committed_current,
    new_feature_level,
}
```

The operation uses the existing bounded leader-only controller mutation path and returns
only after Raft commit plus state-machine apply. There is no heartbeat-driven, placement-
driven, startup-driven or timeout-driven automatic feature bump.

The expected-current field also acts as a compare-and-set fence: a stale admin decision
cannot overwrite a newer committed feature level.

## Monotonicity

P5 feature level is strictly monotonic.

Defense exists at two layers:

1. the canonical ControllerCommand codec rejects `new <= expected`;
2. `ControllerState` independently rejects `new <= expected` before mutating state.

Therefore same-level replays and downgrade attempts fail atomically even if code tries to
bypass the normal command-construction path. Controller failover and restart recover the
committed level from durable state/snapshot and do not derive it from process-local config.

## I-28 irreversible-behavior gate

P5-N introduces the checked `FeatureGate` primitive. A gate names one required active
feature level and exposes a fail-closed `require(active_feature_level)` operation.

The gate is wired at both authority consumers:

- `ControllerState::require_feature` checks the committed controller state;
- `BrokerMetadataCache::require_feature` checks only the broker's applied committed metadata
  state and refuses use while the cache is uninitialized.

The broker qualification test proves that a level-2 irreversible behavior remains disabled
at committed level 1 and becomes enabled only after the broker applies the committed
`SetFeatureLevel 1 -> 2` metadata delta. Local configuration, heartbeat claims or an
uncommitted intent cannot open the gate.

P5-N intentionally does not introduce any new irreversible disk or public-wire behavior;
it creates the guard future phases must use before doing so.

## Real failover/restart qualification

The three-controller P5-N scenario uses:

- controller 1 support: levels 1..=3;
- controller 2 support: levels 1..=2;
- controller 3 support: levels 1..=2;
- ACTIVE broker 30 support: levels 1..=3;
- JOINING broker 31 support: level 1 only.

The scenario mechanically proves:

1. all controller statuses start at active level 1 and expose their configured range;
2. level 3 is rejected because required controller voters 2/3 cannot support it, even though
   the ACTIVE broker can;
3. explicit level 2 commit succeeds and converges to all controllers;
4. FeatureGate(2) succeeds only after the committed state reports level 2;
5. broker 31 cannot activate after the bump;
6. same-level and downgrade requests are rejected;
7. controller-leader death elects a replacement that retains committed level 2;
8. level 3 remains rejected after failover;
9. the old leader restarts and converges at level 2;
10. after shutdown, attempting to restart controller 1 with advertised support only through
    level 1 fails against the recovered durable level-2 state.

## Qualification-harness determinism

During full-workspace qualification, pre-existing network E2Es exposed transient leadership
handoffs in observation-only test paths. P5-N hardens the P5 dynamic test harness without
changing production behavior:

- leader observation windows are generous correctness windows rather than latency targets;
- dynamic broker startup retries only explicit `NotLeader`/controller-busy observations;
- CreateTopic test setup retries only a pre-proposal `NotLeader`, never an ambiguous mutation;
- synthetic failure-detector ticks are routed to the current controller leader because they
  are volatile observations, not replicated authority.

The frozen P4 data-plane implementation is unchanged.

## Compatibility boundary

P5-N preserves:

- P1 disk bytes and storage-generation semantics;
- all P2 public protocol bodies and frozen golden fixtures;
- P3 Raft consensus and membership algorithms;
- P4 replication, acknowledgement/read, peer-transport and rebootstrap semantics;
- P5 controller command numbering and frozen SetFeatureLevel bytes;
- P5-J/K/L broker metadata, dynamic topology and movement semantics.

No P5-N production change exists in `astralog-storage`, `astralog-protocol`,
`astralog-raft`, or `astralog-replication`.

## Qualification evidence

Mechanical qualification on the P5-N implementation bytes before commit:

- `cargo fmt --all -- --check`: PASS;
- `cargo clippy --workspace --all-targets --all-features --offline -- -D warnings`: PASS;
- `cargo test --workspace --all-features --offline`: **364 passed, 0 failed**;
- broker library: **11 passed, 0 failed**;
- legacy P2 network suite: **9 passed, 0 failed**;
- frozen P4 RF=3 TCP E2E: **1 passed, 0 failed**;
- P5 dynamic integration/movement suite: **3 passed, 0 failed**;
- client/common: **5 passed, 0 failed**;
- controller suite: **80 passed, 0 failed**;
- protocol suite: **35 passed, 0 failed** plus **11/11** frozen golden tests;
- Raft suite: **120 passed, 0 failed**;
- replication suite: **46 passed, 0 failed**;
- storage suite: **43 passed, 0 failed**;
- dedicated real P5-N feature/failover/restart scenario: **5/5 PASS**;
- full controller suite: **5/5 PASS**;
- P5 dynamic controller/movement suite: **5/5 PASS**;
- frozen P4 RF=3 TCP E2E: **5/5 PASS**;
- `git diff --check`: PASS.

## P5-N closure

P5-N is qualified only when this report, the normative checklist and implementation are
committed together and the exact committed HEAD re-passes formatting, Clippy, the complete
workspace suite, repeated feature/failover qualification, repeated P5 dynamic qualification,
repeated frozen P4 E2E and the frozen-crate boundary audit.

The next bounded checkpoint is **P5-O only — producer-ID allocator closure, actions P5-152
through P5-157**. P5-M general leadership hooks and P5-P/Q remaining failure/closure work
remain outside that checkpoint unless explicitly selected later.
