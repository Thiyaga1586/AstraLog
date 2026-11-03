# AstraLog P5-I Metadata Epochs / Deltas + Broker Cache Qualification

Date: 2026-09-27

Status: **PASS — P5-I QUALIFIED**

## Scope

This checkpoint implements exactly P5-I actions P5-121 through P5-127:

- committed metadata-delta representation;
- broker `applied_metadata_epoch` tracking;
- heartbeat-driven lag reporting;
- bounded incremental delta responses;
- authoritative snapshot fallback;
- stale/gapped update rejection;
- atomic broker metadata-cache application.

P5-J dynamic partition/runtime integration, public controller-backed Metadata routing,
replica movement, and any change to P4 consensus/ack/read semantics remain outside this
checkpoint.

## Metadata delta contract

`MetadataDelta` contains the exact controller Raft application index (`epoch`) and the
canonical `ControllerCommand` committed at that index. The broker therefore reuses the
same deterministic state-machine semantics rather than maintaining a second mutation
language.

`MetadataSyncResponse` is explicit:

1. `UpToDate { controller_epoch }` when broker and controller epochs match;
2. `Deltas { controller_epoch, deltas }` for a contiguous retained suffix;
3. `Snapshot { controller_epoch, snapshot }` when incremental replay is unsafe or
   unavailable.

The dissemination history is volatile cache state, not replicated authority. Every
controller applies the authoritative command through ordinary P3/P5 state-machine
ordering first; only then is the resulting `(epoch, command)` retained for broker
dissemination.

## Hard bounds

The dissemination cache has two independent hard limits:

- at most 256 retained metadata deltas;
- at most 64 MiB of retained canonical command bytes.

One heartbeat response returns at most:

- 64 contiguous metadata deltas;
- 8 MiB of encoded command bytes.

Old entries are evicted deterministically from the lowest epoch. The bounds are on both
item count and encoded payload bytes so a small number of large commands cannot bypass
the memory contract.

## Raft-index gaps and snapshot fallback

The frozen P5 contract defines `metadata_epoch` as a controller committed/applied Raft
log index. Controller leader barriers are real Raft entries but are not metadata
commands, so valid metadata epochs can contain numeric gaps across leadership events.

P5-I does not weaken the broker rule `incoming epoch == local + 1` for incremental
application. If the exact next epoch is absent from retained delta history — because of
a barrier, eviction, restart, snapshot replacement, or any other history gap — the
controller returns a full committed metadata snapshot. This prevents a broker from
silently skipping an unseen epoch.

After controller InstallSnapshot replaces the local controller state, retained delta
history is cleared. Subsequent lagging brokers receive a snapshot until a new safe
contiguous suffix has accumulated.

## Heartbeat integration

`BrokerHeartbeatRequest.applied_metadata_epoch` already existed as a bounded P5-G
field. P5-I makes it operational:

- an epoch ahead of controller authority is rejected;
- equal epoch returns UpToDate;
- a retained exact suffix returns bounded deltas;
- a missing/gapped suffix returns the authoritative snapshot.

The heartbeat remains a liveness observation. Returning metadata does not convert the
heartbeat itself into a controller Raft mutation and does not advance controller
`metadata_epoch`.

The real three-controller heartbeat/failover test additionally proves:

- follower heartbeat handling still returns `NotLeader`;
- same-leader one-epoch lag receives exactly one metadata delta;
- bootstrap lag across the initial controller leader barrier receives snapshot fallback;
- existing P5-G liveness/failover fencing remains intact.

## Broker metadata cache

`astralog-broker` now contains `BrokerMetadataCache`, deliberately separate from the
legacy P2 `Catalog`. P5-I does not yet route data-plane topology through this cache.

The cache owns an optional committed `ControllerState` snapshot. Its
`applied_metadata_epoch` is derived from that state rather than maintained as an
independent counter.

Delta rules are mechanical:

~~~text
incoming epoch < local      -> reject stale
incoming epoch == local     -> idempotent duplicate
incoming epoch == local + 1 -> apply through ControllerState atomically
incoming epoch > local + 1  -> reject gap; snapshot required
~~~

An uninitialized broker cache requires a snapshot before deltas. Snapshot install:

- validates embedded state epoch equality;
- cannot move the local epoch backwards;
- cannot cross cluster ID;
- treats identical same-epoch state as idempotent;
- rejects a different same-epoch state.

## Atomicity proof

`BrokerMetadataCache::apply_sync` clone-stages the complete cache, validates and applies
every delta/snapshot to the staged copy, then replaces the live cache only after the
whole response succeeds.

The dedicated failure test applies a valid first delta followed by an out-of-order
second delta in one response and proves that the original state and epoch remain
unchanged. No half-created topic, half-updated assignment set, or partially advanced
metadata epoch can escape a failed batch.

## Action evidence map

- **P5-121:** `metadata.rs` defines `MetadataDelta`, `ControllerMetadataSnapshot` and
  `MetadataSyncResponse`; exact controller commands are the mutation representation.
- **P5-122:** `BrokerMetadataCache::applied_metadata_epoch` is monotonic and state-derived.
- **P5-123:** heartbeat lag cursor drives metadata synchronization and ahead-of-authority
  claims fail closed.
- **P5-124:** `MetadataDisseminator` returns bounded contiguous retained deltas with count
  and byte caps.
- **P5-125:** retention miss / numeric gap / unavailable suffix returns the committed full
  metadata snapshot; controller snapshot replacement clears old retained deltas.
- **P5-126:** broker cache rejects stale and gapped deltas plus stale/foreign snapshots.
- **P5-127:** clone-stage-commit batch application proves no partial metadata visibility.

## Mechanical qualification

Pre-commit qualification on the P5-I source tree:

- `cargo fmt --all -- --check`: **PASS**
- `cargo clippy --workspace --all-targets --all-features --offline -- -D warnings`: **PASS**
- `cargo test --workspace --all-features --offline`: **PASS**
- workspace total: **349 passed, 0 failed**
- `astralog-controller`: **73 passed, 0 failed**
- `astralog-broker`: **8 unit + 9 P2 network + 1 P4 E2E = 18 passed, 0 failed**
- `astralog-raft`: **120 passed, 0 failed**
- `astralog-replication`: **46 passed, 0 failed**
- `astralog-storage`: **43 passed, 0 failed**
- protocol golden vectors: **11 passed, 0 failed**
- all doc-test targets: **PASS**
- real three-controller heartbeat/delta/snapshot/failover scenario repeated **5/5 PASS**
- broker metadata-cache safety suite repeated **5/5 PASS**

## Frozen-boundary audit

P5-I changes controller/broker control-plane code only. It does not modify:

- P1 storage format bytes;
- P2 public protocol codecs/golden vectors;
- P3 Raft consensus source or safety semantics;
- P4 replication source, quorum acknowledgement, committed-read, rebootstrap, or leader
  semantics.

`astralog-broker` now depends on `astralog-controller` only to consume the qualified
controller metadata model/state machine. The dependency is one-way; the controller does
not depend on the broker, so no crate cycle or authority inversion is introduced.

## Next bounded checkpoint

**P5-J only — actions P5-128 through P5-144.**

P5-J may make production broker topology consume the P5-I cache, dynamically manage
local replica/peer configuration, publish controller-authoritative Metadata, and route
admin operations. P5-K/L movement orchestration remains a separate later checkpoint and
P4 consensus semantics remain frozen.
