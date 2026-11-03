# P6-F Cooperative Revocation / Activation Qualification

Date: 2026-09-30

Status: **PASS — P6-F QUALIFIED / P6-G NEXT**

This report closes actions **P6-071 through P6-084** only. It qualifies the pure cooperative ownership state/fence orchestration layer in `astralog-groups`. It does not implement heartbeat/session expiry, replicated coordinator runtime, offset-value CommitOffsets/FetchOffsets state, broker/client API 30..35 dispatch, or later P6 scheduling/failure closure.

## Scope and authority boundary

P6-F adds `CooperativeStateMachine` above the already-qualified P6-D `GroupStateMachine` and P6-B fence record types. It remains a pure deterministic state machine: there is no clock, RNG, socket, disk, broker, controller, Raft or P4 runtime dependency.

The machine does **not** claim that constructing an `OffsetGroupFenceV1` or `OffsetOwnerFenceV1` makes it durable. Instead it separates required durable records from authority transitions:

- `required_group_fence` and `required_owner_fences` derive the exact records that later integration must commit;
- `apply_committed_group_fence` / `apply_committed_owner_fence` accept those records only as externally committed durability evidence;
- group-epoch or target-owner activation is refused until the corresponding committed fence evidence has been observed.

P6-H/P6-I remain responsible for binding these pure transitions to real committed internal-topic history and offset-state application.

## Qualified cooperative ordering

The frozen handoff is implemented as:

```text
PREPARING_EPOCH_FENCE
  -> exact committed OffsetGroupFence
  -> REVOKING
  -> durable group-state release
  -> exact committed OffsetOwnerFence
  -> target activation
  -> STABLE
```

### Prepare and group fence

A PREPARING record contains the already-durable target plan and strictly newer pending GroupEpoch. The required group fence is derived from exactly that record:

- GroupId and pending GroupEpoch must match;
- `source_group_state_version` is the prepared record version;
- every current active TopicPartition is snapshotted with its exact active MemberId and current MemberEpoch;
- first epoch-1 bootstrap produces an empty owner snapshot when no partition was previously active.

`activate_prepared_epoch` refuses to advance authority until that exact fence is observed committed. Exact duplicates are idempotent. Same-epoch byte conflicts and epoch regression fail closed.

### REVOKING and canonical revocations

After the group fence is durable, the pending epoch becomes active. A partition whose current active owner still equals its target remains uninterrupted. A partition whose target changed or disappeared stays active on its old owner and enters `pending_revocations`. A target partition with no prior active owner remains inactive.

The canonical revocation map is therefore exactly:

```text
active TopicPartitions where target_owner != active_owner
```

`GroupRecordV1` live semantic validation now rejects a REVOKING record whose pending-revocation map differs from that set. This makes stale, missing or fabricated cooperative work fail before publication.

### Release and owner fence

`RevokeAck` is accepted only when:

- GroupId matches;
- GroupEpoch is the exact active epoch;
- MemberId exists and MemberEpoch is exact;
- the request is canonical, sorted, unique and contains 1..=1024 TopicPartitions;
- every newly acknowledged partition is still active and pending revocation from that exact member authority.

An accepted release first commits a new group-state version with the partition removed from both `active_assignments` and `pending_revocations`. Until that release exists, an owner fence is rejected.

The subsequently required `OffsetOwnerFenceV1` must cite the exact released group-state version, active GroupEpoch, target MemberId and current target MemberEpoch. A target cannot activate without that committed fence evidence.

P6-F deliberately serializes the narrow same-epoch boundary more strictly than the minimum contract: once an owner fence is observed committed, that fenced target must activate (or the workflow must be superseded by a strictly newer prepared epoch) before another release changes the same-epoch group state. This keeps `source_group_state_version` an exact, unambiguous proof that release preceded the fence without changing any frozen bytes or weakening concurrency safety.

## I-18 / I-19 ownership safety

Target activation inserts ownership only for partitions carrying an exact committed owner fence and refuses to overwrite an existing active owner. Therefore a moved partition follows only:

```text
old active -> no active owner -> new active
```

The bounded model explores legal release/fence/activation interleavings for multiple handoffs and asserts that each changed partition is always in one of the old-active, released or new-active states. It also requires eventual canonical STABLE state in explored progress paths. No explored state contains two active owners for the same TopicPartition.

This directly qualifies phase invariants **I-18** and **I-19** at the pure authoritative group-state layer.

## Idempotence and supersession

P6-F qualifies:

- exact duplicate group fences: idempotent;
- exact duplicate owner fences: idempotent;
- exact duplicate RevokeAck for an already released same handoff: idempotent with no GroupStateVersion advance, including after target activation/STABLE; the retained completion key includes GroupEpoch + MemberId + MemberEpoch so a later epoch cannot accidentally inherit an older handoff's idempotence;
- conflicting fence bytes/authority: fail closed;
- in-flight supersession: only a strictly newer pending GroupEpoch is accepted;
- superseding PREPARING records may not add or transfer active ownership; removing active ownership is accepted only when the previous owner's member authority has itself been removed by the committed membership transition.

Obsolete owner-fence observations are discarded when a strictly newer target epoch supersedes the workflow; the newer epoch must establish its own group fence before authority advances.

## Crash-boundary recovery

`CooperativeStateMachine::recover` accepts committed group/fence histories and mechanically cross-validates their ordering:

- **after prepare / before group fence:** the exact required group fence is regenerated;
- **after group fence / before epoch activation:** the canonical epoch activation is resumed;
- **after release / before owner fence:** release is reconstructed from consecutive group-state versions, duplicate RevokeAck remains idempotent, and the exact owner fence is regenerated;
- **after owner fence / before target activation:** the fence source version is checked against released group state and target activation resumes;
- target ownership appearing in committed group history without an earlier matching owner fence is rejected as invalid recovery history.

Recovery derives authority exclusively from the supplied committed histories. It never reconstructs ownership from a client heartbeat or response cache.

## Mechanical qualification

Focused P6-F qualification:

- formatting — **PASS**
- workspace Clippy `--all-targets --all-features --offline -D warnings` — **PASS**
- `astralog-groups` — **35/35 PASS**
- bounded cooperative handoff model — **PASS**

Compatibility qualification:

- frozen P2 protocol goldens — **11/11 PASS**
- P6 protocol goldens — **6/6 PASS**
- P6-C real RF3 routing/bootstrap/failover/restart — **1/1 PASS**
- frozen P5 dynamic integration — **4/4 PASS**
- broker library — **14/14 PASS**
- client — **6/6 PASS**
- common — **2/2 PASS**
- controller — **91/91 PASS**
- protocol — **38/38 PASS**
- Raft — **122/122 PASS**
- replication — **46/46 PASS**
- storage — **43/43 PASS**
- complete workspace — **429/429 PASS**

During final closure, one pre-existing P6-C test-harness race was exposed under the full workspace: immediately after shutting down the current group coordinator, surviving replicas could still transiently report that dead broker in the old P3 term. The production resolver was unchanged. The P6-C qualification helper now waits for the property that the test already intends to prove — a **different coordinator broker at a strictly higher observed P3 term** — rather than accepting the first stale-but-well-formed metadata route. The hardened routing scenario passed **2/2** focused runs before final workspace qualification.

No frozen P1 disk bytes, P2 wire bodies/goldens, P3 consensus behavior, P4 replication semantics or P5 controller authority were changed.

## Boundary

P6-G and later remain untouched. In particular P6-F does not add:

- heartbeat receive timestamps, deadline calculations or session expiry;
- automatic timeout-driven membership changes;
- replicated group-coordinator proposal/recovery runtime;
- logical OffsetValue state or CommitOffsets/FetchOffsets execution;
- broker/client handlers or ApiVersions advertisement for API keys 30..35;
- P7+ behavior.

The next bounded checkpoint is **P6-G only — actions P6-085 through P6-096: heartbeats, timeout and static membership**.
