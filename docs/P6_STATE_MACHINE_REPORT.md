# P6-D Pure Durable Group State Machine Qualification

Date: 2026-09-30

Status: **PASS — P6-D QUALIFIED / P6-E NEXT**

This report closes actions **P6-045 through P6-060** only. It qualifies the pure deterministic durable group-state layer in `astralog-groups`. It does not implement the P6-E assignor, P6-F cooperative handoff execution, P6-G session runtime, P6-H replicated coordinator, P6-I offsets state machine, or P6-J public API activation.

## Scope

P6-D adds a pure `GroupStateMachine` over the canonical `GroupRecordV1` representation frozen in P6-A/P6-B.

The state machine owns exactly one `GroupId` and its current canonical durable record. It has no clock, RNG, disk, network, broker, controller, Raft, or P4 dependency. All authoritative mutation results are ordinary canonical `GroupRecordV1` values that later checkpoints may replicate through the already-qualified internal-topic machinery.

Assignment ownership is deliberately not computed here. P6-D accepts an already-computed canonical `GroupMutationPlan` only so membership/subscription/leave mutations can prepare the next durable group epoch. The uniform-sticky assignment algorithm remains entirely P6-E.

## Durable replay contract

`GroupStateMachine::apply_committed` provides the recovery/replay gate:

- an empty machine accepts only `state_version = 1`;
- later committed records must use exactly `current_state_version + 1`;
- byte-identical replay of the current version is idempotent;
- a same-version record with different canonical bytes is a fatal state-version conflict;
- version gaps fail closed;
- active GroupEpoch may never regress;
- pending GroupEpoch may never regress relative to already-prepared authority;
- retained members may never regress MemberEpoch;
- every candidate is revalidated through the canonical durable-record encoder before publication.

This means recovery never reconstructs authority from client claims and never guesses through a missing state version.

## Membership and epoch transitions

P6-D qualifies these pure transitions:

- **first admission:** creates state version 1 in `PREPARING_EPOCH_FENCE`, with internal active-epoch sentinel 0 and pending GroupEpoch exactly 1; the admitted member starts at MemberEpoch 1;
- **dynamic admission:** a caller-supplied checked MemberId is admitted at MemberEpoch 1 and a strictly newer pending GroupEpoch is prepared;
- **static replacement:** requires the current MemberId/MemberEpoch plus an existing InstanceId, increments MemberEpoch exactly once, preserves the static identity, and prepares a higher GroupEpoch;
- **subscription update:** requires exact group/member authority, rejects unchanged subscriptions as a non-transition, and prepares a higher GroupEpoch;
- **leave:** requires exact group/member authority, removes the member and any active/revocation ownership still attributed to it, and prepares a higher GroupEpoch;
- **prepared supersession:** an unfinished prepared target may be replaced only by a strictly higher pending GroupEpoch, matching the frozen P6-A contract.

P6-D does not activate pending epochs, compute target assignments, execute revocations, install offset fences, or grant new partition ownership. Those are later checkpoint responsibilities.

## Fencing and atomicity

All command paths are clone → mutate candidate → canonical validation/encode → publish.

Therefore failures leave the current authoritative record byte-for-byte unchanged.

Qualified fencing includes:

- stale expected GroupEpoch → `GroupStateError::GroupEpochStale` → frozen public `GROUP_EPOCH_STALE` taxonomy;
- stale MemberEpoch → `GroupStateError::MemberEpochStale` → frozen public `UNKNOWN_MEMBER` taxonomy;
- removed/unknown MemberId → `UNKNOWN_MEMBER` taxonomy;
- duplicate MemberId admission rejection;
- duplicate InstanceId rejection on both mutation and replay paths;
- static replacement may reuse an InstanceId only for the same logical MemberId.

No broker API is activated by this mapping; it is only the pure error-classification seam for later integration.

## Canonical bounds and semantic validation

Before publication, P6-D reuses the frozen canonical durable-record validation and strengthens it as a live state invariant:

- `MAX_GROUP_MEMBERS = 1_024`;
- `MAX_SUBSCRIPTION_TOPICS = 256` per member;
- `MAX_GROUP_PARTITIONS = 16_384` per assignment/revocation map;
- `MAX_MEMBER_ASSIGNMENT_PARTITIONS = 1_024` independently for active, target, and pending-revocation ownership;
- session timeout remains `6_000..=300_000 ms`;
- duplicate InstanceIds are forbidden;
- `STABLE` contains no pending epoch/revocations and its target equals active ownership;
- `PREPARING_EPOCH_FENCE` carries no pending revocations and has a pending epoch strictly above the active epoch;
- encoded state remains within `MAX_GROUP_RECORD_BYTES = 4 MiB`.

Dedicated over-cap tests prove rejection occurs before the live state is replaced.

## Determinism

The machine uses canonical checked identities plus `BTreeMap`/sorted-set representation only. There is no time, randomness, iteration-order, broker-load, or network input.

The determinism test applies the same logical mutation with different insertion order for the supplied target map and asserts byte-identical canonical `GROUP_STATE` output.

## Mechanical qualification

Focused P6-D qualification:

- `cargo fmt --all -- --check` — **PASS**
- `cargo clippy -p astralog-groups --all-targets --all-features --offline -- -D warnings` — **PASS**
- `cargo test -p astralog-groups --all-features --offline` — **17 passed, 0 failed**

Compatibility qualification:

- frozen P2 goldens — **11/11 PASS**
- P6 public goldens — **6/6 PASS**
- P6-C real RF3 routing/bootstrap/failover/restart — **1/1 PASS**
- frozen P5 dynamic integration — **4/4 PASS**
- controller — **91/91 PASS**
- protocol — **38/38 PASS**
- Raft — **122/122 PASS**
- replication — **46/46 PASS**
- storage — **43/43 PASS**
- complete workspace — **411 passed, 0 failed**

## Boundary audit

P6-D changes only the pure `astralog-groups` state layer plus P6 status/checklist/report documentation.

It does **not**:

- modify P1 disk bytes;
- change P2 Wire V1 framing or frozen public golden bytes;
- change P3 Raft consensus;
- change P4 replication/acknowledgement/read semantics;
- change P5 controller metadata/placement/movement authority;
- advertise or dispatch P6 API keys 30..35;
- compute uniform-sticky assignments;
- execute cooperative release/fence/activation;
- run heartbeat/session timers;
- write replicated group records to internal topics;
- implement offset fencing/CommitOffsets/FetchOffsets runtime behavior.

## Closure

Actions **P6-045 through P6-060** are mechanically qualified. Actions **P6-061 onward remain untouched**.

The next bounded checkpoint is therefore:

> **P6-E only — P6-061 through P6-070: deterministic uniform-sticky assignor.**

P6-E must remain a pure assignment function and must not start cooperative handoff, session/runtime, replicated-coordinator, or offset-state-machine work.
