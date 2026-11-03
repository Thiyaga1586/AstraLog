# AstraLog P6-B Types, Codecs and Golden Bytes Qualification

Date: 2026-09-29

Status: **PASS — P6-B QUALIFIED / P6-C NEXT**

## Scope

P6-B closes actions **P6-019 through P6-032** only. It implements the checked identities,
public Wire V0 bodies and internal durable record encodings frozen by `P6_GROUPS_V1.md`.

P6-B deliberately does **not** create either internal topic, route a GroupId to a live
coordinator, advertise APIs 30..35 as usable, implement a group state machine, execute a
rebalance, maintain session deadlines, write offsets, or add client group-session behavior.
Those begin in later P6 checkpoints.

## Dependency boundary

Public protocol identities and Wire V0 bodies live in `astralog-protocol`. Durable P6
record types/codecs live in a new lower-level `astralog-groups` crate with dependencies only on:

- `astralog-common`;
- `astralog-protocol`.

No broker, client, controller, storage, Raft or replication crate is required by the P6-B
durable codec layer. This keeps later P6 state-machine work below broker embedding and avoids an
upward dependency cycle.

## Checked public identities

P6-B adds:

- `GroupId`: UTF-8, 1..=255 bytes, case-sensitive/byte-exact, NUL forbidden;
- `MemberId`: 16 bytes, all-zero reserved as JoinSubscribe allocation sentinel;
- `GroupEpoch`: non-zero `u64`, checked increment;
- `MemberEpoch`: non-zero `u64`, checked increment;
- `GroupStateVersion`: non-zero `u64`, checked increment;
- `TopicPartitionV0`: canonical total order by TopicId bytes, then partition number.

Canonical sets are strictly ascending and duplicate-free. Integer wrap, zero reserved authority,
invalid text, non-canonical order and bound violations fail closed.

## Public API V0 codecs

The permanent API registry is unchanged. P6-B supplies bodies for the already-reserved keys
without activating or advertising them:

- 30 `JoinSubscribe`;
- 31 `Heartbeat`;
- 32 `RevokeAck`;
- 33 `Leave`;
- 34 `CommitOffsets`;
- 35 `FetchOffsets`.

All bodies reuse existing big-endian Wire V1 `Reader`/`Writer`, `TaggedFields`, bounded-count and
frame rules. Shared `MemberAuthorityV0`, `CoordinatorHintV0` and `TopicPartitionV0` layouts match
the frozen contract exactly. Join session timeout, subscription count/order, revocation count,
offset count/order and offset metadata bounds are validated before authoritative use.

P6-B does not modify `ApiVersions` availability. Until later P6 activation, allocated P6 APIs
continue to use the existing unavailable-key gate.

## Internal durable record codec

`astralog-groups` implements the frozen little-endian envelope:

```text
u16 version = 1
u16 record_kind
u32 body_len
body
```

Kinds are exactly:

1. `GROUP_STATE`;
2. `OFFSET_GROUP_FENCE`;
3. `OFFSET_OWNER_FENCE`;
4. `OFFSET_VALUE`.

`body_len` must consume the remaining bytes exactly. Unknown version/kind, trailing bytes,
non-canonical maps/sets, invalid optional markers and count claims that cannot fit remaining bytes
fail closed.

`GroupRecordV1` validates the P6-A bootstrap sentinel rule, exact pending-next epoch in
`PREPARING_EPOCH_FENCE`, member key/identity agreement, member/session/subscription bounds,
assignment references, revocation/current-owner consistency and per-group/per-member partition
bounds. No transition/state-machine behavior is introduced yet.

Internal logical keys also match the frozen layouts for group state, group fences, owner fences and
offset values. P1 record/frame bytes themselves are not changed.

## Golden-byte qualification

P6-B adds immutable exact fixtures for:

- JoinSubscribe V0 request;
- Heartbeat V0 request;
- RevokeAck V0 request;
- Leave V0 request;
- CommitOffsets V0 request;
- FetchOffsets V0 request;
- `GROUP_STATE` internal value;
- `OFFSET_GROUP_FENCE` internal value;
- `OFFSET_OWNER_FENCE` internal value;
- `OFFSET_VALUE` internal value;
- frozen internal logical key layouts.

The existing P2 golden-vector source is not modified and its **11/11** fixtures remain green.

## Malformed and bounds qualification

Focused tests cover:

- empty/oversized/NUL GroupId;
- all-zero MemberId;
- zero/wrapping checked epochs;
- duplicate/unsorted TopicId and TopicPartition collections;
- new-member epoch-shape errors;
- oversized offset batches and metadata;
- trailing public body bytes;
- invalid GroupRecord bootstrap epoch and dangling assignment authority;
- non-canonical subscriptions/internal maps;
- internal envelope trailing bytes, unknown kind and body-length mismatch;
- exact internal key byte order.

All decoded counts are bounded before allocation by either existing protocol helpers or the
internal codec's remaining-byte/count checks.

## Qualification matrix

On the P6-B closure bytes:

- `cargo fmt --all -- --check`: PASS;
- workspace Clippy, all targets/features, offline, `-D warnings`: PASS;
- `git diff --check`: PASS;
- `astralog-protocol` unit tests: **38/38 PASS**;
- frozen P2 protocol goldens: **11/11 PASS**;
- P6 public V0 goldens: **6/6 PASS**;
- `astralog-groups`: **5/5 PASS**;
- full workspace: **394 passed, 0 failed**;
- P5 dynamic regression: **4/4 PASS**;
- controller: **91/91 PASS**;
- Raft: **122/122 PASS**;
- replication: **46/46 PASS**;
- storage: **43/43 PASS**.

The full workspace is rerun once more after documentation synchronization and then against the exact
committed SHA. P6-B is not mechanically frozen until that exact-SHA gate succeeds.

## Boundary

The next allowed checkpoint after exact-SHA P6-B qualification is **P6-C only — actions P6-033
through P6-044: internal topics and coordinator routing**.

P6-C has not started here. No internal-topic creation/validation, coordinator resolution, term
fencing, client routing or failover behavior belongs in this checkpoint.
