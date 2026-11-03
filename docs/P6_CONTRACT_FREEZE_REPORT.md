# AstraLog P6-A Contract Freeze Report

Date: 2026-09-29

Status: **PASS — P6-A CONTRACT FROZEN / P6-B NEXT**

## Scope

P6-A is specification/checklist work only. It introduces no consumer-group runtime, no offset state machine, no API handler, no client group session, and no internal-topic creation code.

Frozen artifacts:

- `P6_GROUPS_V1.md` — consumer-group/offset authority, durable record formats, cooperative handoff, offset fencing, API V0 bodies and phase boundaries;
- `P6_CHECKLIST.md` — exactly 176 unique actions P6-001 through P6-176 with no gaps;
- P6 implementation checkpoints P6-A through P6-L;
- synchronized README and CURRENT_STATE status/canonical-document references.

## Architecture sources audited

P6-A is derived from the already-frozen project sources rather than replacing them:

- `ASTRALOG_BIBLE.md` sections 17-18: internal offset/group topics, group epochs, pending revocations, session deadlines and cooperative rebalancing;
- `PHASE_PLAN.md` P6 goal/features/tests/exit gate;
- `DECISIONS.md` ADR-007 cooperative incremental groups and ADR-011 internal replicated topics;
- `INVARIANTS.md` I-05, I-18, I-19 and I-20;
- `PROTOCOL_V1.md` permanent API keys 30-35, feature key 12 and frozen public group error numbers;
- closed P5 placement/feature-level/controller authority.

## Frozen P6-A decisions

### Ownership

P6 owns only stream-consumer coordination and committed offsets. P3 remains sole consensus authority, P4 remains replicated partition/log authority, and P5 remains cluster metadata/placement authority.

### Internal state plane

- `__astralog_groups` and `__astralog_offsets` are normal replicated internal topics;
- P6 V1 freezes both at 64 partitions, RF=3, Preferred failure-domain policy;
- shard count cannot expand in P6 V1 because coordinator hashing would remap group authority;
- physical keyed compaction remains P9; P6 requires only logical latest-value semantics.

### Coordinator routing

GroupId UTF-8 bytes reuse the already-frozen P2 Murmur3 seed-0 keyed partitioner. The current P3 leader/term of the corresponding `__astralog_groups` partition is the only group coordinator/epoch observation.

### Group/session authority

- external GroupEpoch and MemberEpoch are non-zero checked u64 values;
- first group creation may use internal `active_group_epoch=0` only while a durable PREPARING_EPOCH_FENCE record exists; externally authoritative epoch starts at 1;
- dynamic members receive a fresh MemberId/epoch 1;
- static replacement increments MemberEpoch and takes the normal strictly-higher GroupEpoch path, allowing sticky assignment to preserve ownership without inventing a same-epoch fence protocol.

### Deterministic assignment

Uniform-sticky assignment minimizes, in order: maximum member load, ownership movement among equally balanced solutions, then canonical owner-vector order. Inputs are canonical and contain no RNG/clock/load signal.

### Cooperative ownership

Moved partitions follow old-active -> released -> new-active. New ownership is withheld until the offsets-shard owner fence is committed. This directly encodes I-18/I-19 rather than relying on timeout timing.

### Cross-topic offset fencing

Because group and offset state are separate P3 groups, group-state validation before an offset proposal is insufficient by itself. P6 therefore freezes:

- one OffsetGroupFence per group epoch, atomically carrying the current active-owner snapshot under the new epoch;
- later same-epoch OffsetOwnerFence records for released/moved partitions;
- CommitOffsets acceptance only when both group epoch and effective per-partition owner/member epoch match.

This orders stale-commit fencing before new authority activation and closes the late-cross-group-commit race required by I-20.

### Heartbeats and timeouts

Session timeout is 6,000..=300,000 ms. Heartbeat receive time remains coordinator-leader-local monotonic observation. New coordinator leadership discards observations and grants a full timeout window to committed members; only a committed timeout transition removes authority.

### Durable bytes

P6 internal values freeze a version/kind/body-length envelope and four record kinds: GROUP_STATE, OFFSET_GROUP_FENCE, OFFSET_OWNER_FENCE and OFFSET_VALUE. Primitive ordering and canonical key identity are specified in `P6_GROUPS_V1.md`. No P1 frame bytes change.

### Wire API

Existing permanent keys are retained:

- 30 JoinSubscribe;
- 31 Heartbeat;
- 32 RevokeAck;
- 33 Leave;
- 34 CommitOffsets;
- 35 FetchOffsets.

V0 fields, canonical array order, bounds, coordinator hints and commit-before-response semantics are frozen before implementation. Existing public errors GroupEpochStale, UnknownMember, RebalanceInProgress and InvalidOffsetCommit are reused; no registry renumbering is introduced.

### Feature activation

Feature key 12 remains CONSUMER_GROUPS. P6 V1 requires controller active feature level >=2 plus valid serving internal topics before P6 APIs are advertised/activated. Before that, existing P2 UNSUPPORTED_API_KEY behavior remains authoritative.

## Bounds frozen

- GroupId: 255 UTF-8 bytes;
- InstanceId: 255 UTF-8 bytes;
- group members: 1,024;
- subscription topics/member: 256;
- group partitions: 16,384;
- assignments/member: 1,024;
- offsets/request: 1,024;
- offset metadata: 4,096 UTF-8 bytes;
- encoded GroupRecordV1: 4 MiB;
- all bodies remain additionally bounded by Wire V1 64 MiB frame limits and P1 record limits.

## Later-phase boundary

P6-A explicitly keeps out:

- P7 producer epochs/sequences/deduplication;
- P8 compression/performance claims;
- P9 physical retention/compaction;
- P10 work-queue leases/retries/DLQ;
- P11 TLS/auth/ACL/drain/continuous balancing/rolling upgrades;
- P12 release chaos/benchmark closure.

Group epochs fence coordinator state and stale offset writes only; they do not roll back external application side effects.

## Checklist audit

`P6_CHECKLIST.md` contains exactly 176 unique action IDs P6-001 through P6-176 with no missing numbers.

- P6-001 through P6-018 are checked for P6-A contract freeze.
- P6-019 through P6-176 remain unchecked.
- all P6 phase-exit implementation invariants remain unchecked because no implementation exists yet.

## Mechanical qualification

Pre-commit qualification on the exact documentation-only P6-A bytes:

- documentation/checklist audit: **PASS** — exactly 176 unique actions P6-001..P6-176, only P6-001..018 checked;
- source-boundary audit: **PASS** — no Rust/Cargo/source file modified by P6-A;
- `git diff --check`: **PASS**;
- `cargo fmt --all -- --check`: **PASS**;
- workspace Clippy `-D warnings`: **PASS**;
- full workspace: **380 passed, 0 failed**;
- controller: **91/91**;
- P5 dynamic integration: **4/4**;
- protocol: **35/35**;
- frozen P2 protocol goldens: **11/11**;
- Raft: **122/122**;
- replication: **46/46**;
- storage: **43/43**;
- all doc-test targets: **PASS**.

The bounded P6-A commit containing this report is the authoritative freeze commit. After commit,
fmt/Clippy/full-workspace/golden/boundary checks are rerun on that exact SHA. The exact-HEAD PASS
record is attached to the freeze commit as a Git note so qualification evidence does not alter the
frozen commit bytes.

## Next checkpoint

After exact-HEAD P6-A qualification, the next allowed implementation checkpoint is **P6-B only — checked group types, canonical public/internal codecs, strict bounds and golden vectors**.

P6-C internal-topic creation/routing and every runtime behavior remain disabled until P6-B independently qualifies.