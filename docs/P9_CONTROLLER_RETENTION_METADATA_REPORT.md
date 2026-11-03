# AstraLog P9-B Controller Retention Metadata and Public Codec Qualification

Date: 2026-10-08

Status: **P9-B PRE-COMMIT QUALIFIED — CLOSURE COMMIT / EXACT-HEAD AUTHORITY PENDING**

## 1. Boundary

P9-B implements only checklist actions **P9-021 through P9-043** on top of authoritative P9-A:

`b7c8552646f7e3cd766bcba469c9bc9e0ce766d7 — P9-A freeze retention compaction architecture`

The P9-A Git note declares that exact SHA authoritatively closed/frozen and authorizes P9-B only.

P9-B does not implement Storage V2, manifest-mode live inventory, logical log-start execution, physical
retention, keyed cleaner generations, producer-state cleanup checkpoints, tombstone-removal watermarks,
retained-history rebootstrap or cleaner scheduling. Those remain P9-C and later.

## 2. Controller retention policy

`astralog-controller::retention` adds the frozen checked policy model:

- `CleanupPolicyV1::{Delete, Compact, CompactDelete}`;
- `TopicRetentionPolicyV1`;
- `TopicRetentionSpecV1`;
- `SubscriptionProtectionV1::{Disabled, ReservedForProtectedFloor}`;
- `P9_RETENTION_FEATURE_LEVEL = 5`;
- default tombstone grace `86_400_000` ms.

Historical/default topic policy is deterministic:

```text
Delete
retention_ms = None
retention_bytes = None
tombstone_grace_ms = 86_400_000
subscription_protection = Disabled
```

This means feature activation alone deletes or compacts no historical topic data.

Validation is closed/fail-fast. Unknown cleanup/protection values fail; tombstone grace must be positive;
Compact cannot carry delete-retention limits; Delete and CompactDelete accept independently optional
u64 time/size limits.

## 3. Feature activation

P9-B reserves protocol feature key **15 = RETENTION_COMPACTION**, version 1.

Irreversible P9 behavior remains gated by the existing committed controller feature-level mechanism at
cluster level 5. No new side authority is introduced:

- controller voters must all advertise support for level 5;
- every ACTIVE broker must support the requested level;
- an active max-4 broker blocks level 5;
- a controller voter whose range ends at 4 blocks level 5;
- level 4 continues to qualify when only P8 capability exists.

The real P9 RF3 and dynamic-broker tests use explicit 1..5 capability ranges rather than silently
changing historical tests/configuration.

## 4. Controller command V4

P9-B adds `ControllerLogCommand::RetentionV4` after the existing V1/V2/V3 envelopes. It has two
canonical command kinds:

1. `CreateTopicWithPerformanceAndRetention`
2. `AlterTopicRetentionPolicy`

Create wraps the frozen V1 CreateTopic command and carries one validated P8 performance policy plus one
validated P9 retention policy for the same TopicId. The controller applies all three pieces to a cloned
state and publishes them under one committed Raft metadata index, so no partially created topic/policy
state can become authoritative.

Alter carries TopicId, the expected current full policy and the new full policy. Apply requires:

- committed cluster level 5;
- existing topic;
- ACTIVE topic lifecycle;
- exact expected/current policy equality;
- valid new policy.

Any failure leaves the metadata epoch and all policy state unchanged.

## 5. Topic lifecycle ownership

Frozen V1 CreateTopic continues to install historical default P8/P9 policy rows. P9 V4 create then
atomically replaces those defaults with the requested committed policies.

Committed FinalizeDeleteTopic removes:

- topic record/name;
- assignments;
- P8 performance policy;
- P9 retention policy.

The state validator rejects missing or orphan policy rows, preventing a deleted topic from leaving
authoritative retention metadata behind.

## 6. Controller Snapshot V4

Snapshot V4 is additive after V3. It wraps a canonical V3 base plus a bounded TopicId-sorted P9 policy
table and protects the complete V4 bytes with CRC32C.

The nested V3 base is deliberately canonicalized to historical P9 defaults because V3 bytes cannot
encode P9 policy. The outer V4 policy table is the sole P9 snapshot authority.

Decoder/encoder validation proves:

- exact topic coverage;
- TopicId/policy identity match;
- strict TopicId ordering;
- no duplicates;
- no orphan policy rows;
- valid policy fields;
- count/byte bounds;
- no trailing/noncanonical bytes.

V1/V2/V3 snapshot decoding reconstructs the exact historical P9 default. Old snapshot decoders reject
V4 instead of silently interpreting its bytes.

## 7. Persistence and RF3 failover

Controller persistence/replay now understands RetentionV4. The real RF3 P9 test proves:

1. three controllers advertise 1..5 support;
2. an ACTIVE broker advertises 1..5 support;
3. feature level 5 commits;
4. CreateTopic-with-performance-and-retention commits and converges on all controllers;
5. AlterTopicRetentionPolicy commits and converges;
6. the current leader is stopped;
7. a successor in a higher term retains level 5 and the altered policy;
8. the stopped node restarts from durable state and recovers the identical altered policy.

This is controller metadata durability/failover qualification only. It makes no P9-C storage-cleaner
claim.

## 8. Public protocol additions

P9-B keeps every prior published body unchanged and adds only:

- CreateTopic request/response V2;
- DescribeTopic request/response V2;
- AlterTopicConfig V0;
- feature key 15.

CreateTopic V2 carries the already-frozen P8 `TopicPerformanceConfigV1` plus a new
`TopicRetentionConfigV1`. DescribeTopic V2 returns both committed policies.

`TopicRetentionConfigV1` contains only:

- cleanup policy;
- optional retention_ms;
- optional retention_bytes;
- positive tombstone_grace_ms.

The internal subscription-protection field is intentionally absent from public P9 wire bodies.

AlterTopicConfig V0 is limited to P9 retention fields. Its present/clear masks distinguish:

- omitted field: preserve committed current value;
- present optional limit with clear bit: set it to None;
- present optional limit without clear: set the provided u64;
- present cleanup/grace: replace that field.

Empty patches, unknown/reserved bits, noncanonical absent bytes and invalid resulting policies fail
before controller mutation.

## 9. Broker readiness and dispatch

The broker derives `p9_ready` only from its authoritative controller metadata cache observing committed
feature level >=5.

Before P9 readiness:

- CreateTopic/DescribeTopic advertise at most P8 V1 when P8 is ready;
- CreateTopic/DescribeTopic V2 return UnsupportedApiVersion;
- AlterTopicConfig is absent from ApiVersions and dispatch returns UnsupportedApiKey;
- feature key 15 may be advertised supported but inactive (`0xFFFF`).

After P9 readiness:

- CreateTopic/DescribeTopic advertise versions 0..2;
- AlterTopicConfig advertises V0;
- feature key 15 active version is 1.

The real dynamic-broker P9 test commits level 5 and exercises public Create V2 -> Describe V2 -> Alter
V0 -> Describe V2. It proves the altered controller policy is returned through both broker response and
controller state. The existing P8 level-4 integration remains green and still rejects AlterTopicConfig.

## 10. Metadata dissemination

Frozen controller metadata delta V1 cannot encode P9 policy. Therefore a committed RetentionV4 command
clears the retained incremental-delta window, forcing lagging brokers onto the existing authoritative
full-state snapshot path. This mirrors the compatibility-safe P8 V3 strategy and avoids inventing an
unfrozen delta reinterpretation in P9-B.

## 11. Golden/compatibility evidence

New immutable P9 public-body vectors live in:

`crates/astralog-protocol/tests/p9_golden_vectors.rs`

They fix exact bytes for:

- CreateTopicRequest V2;
- CreateTopicResponse V2;
- DescribeTopicResponse V2;
- AlterTopicConfigRequest V0;
- AlterTopicConfigResponse V0.

No historical P2/P6/P7 golden source is modified. Storage V1/FORMAT_V1 is untouched by P9-B.

Current focused evidence on the implementation candidate:

- `cargo test --workspace p9 -- --test-threads=1`: **14/14 PASS**;
- P9 controller-focused tests: **8/8 PASS**;
- P9 protocol/registry-focused unit tests: **3/3 PASS**;
- P9 public-body goldens: **2/2 PASS**;
- real P9 dynamic broker integration: **1/1 PASS**;
- complete protocol package: **48/48 unit PASS** plus historical P2 **11/11**, P6 **6/6**, P7 **4/4** goldens and P9 **2/2** goldens;
- complete controller unit suite: **122/122 PASS**;
- broker units: **37/37 PASS**;
- existing real P8 dynamic topic-policy integration: **1/1 PASS**.

## 12. P9-C exclusion audit

P9-B contains no implementation for:

- `FORMAT_V2.md` disk codec;
- manifest-authoritative live PartitionLog inventory;
- log-start maintenance command;
- time/size physical deletion;
- keyed COW cleaner;
- producer-state cleanup checkpoint;
- clean-compacted/safe watermarks;
- tombstone removal;
- RebootstrapPackage V2;
- protected-floor provider runtime;
- cleaner queues/throttling.

Those remain P9-C through P9-H. P10-P12 remain disabled.

## 13. Remaining P9-B closure gate

The final P9-B candidate passed the complete pre-commit closure gate:

- `cargo fmt --all -- --check`: PASS;
- `cargo clippy --workspace --all-targets --all-features -- -D warnings`: PASS;
- `cargo test --workspace --doc`: PASS;
- `git diff --check`: PASS;
- focused `cargo test --workspace p9 -- --test-threads=1`: **14/14 PASS**;
- complete serialized `cargo test --workspace --all-targets -- --test-threads=1`: **653/653 PASS**.

Complete serialized component evidence:

- benchmark tooling: **20/20**;
- broker units: **37/37**;
- P2 network: **9/9**;
- P4 RF3: **1/1**;
- P5/P8/P9 dynamic integration: **6/6**;
- P6 routing/group/offset/fencing/churn/public integration: **9/9** serialized;
- P7 real RF3 failover: **12/12** serialized;
- P7 runtime: **2/2**;
- client: **24/24**;
- common: **3/3**;
- controller: **122/122**;
- groups: **55/55**;
- protocol: **48/48**;
- original P2/P6/P7 protocol goldens: **11/11 + 6/6 + 4/4**;
- new P9 public-body goldens: **2/2**;
- Raft: **128/128**;
- replication: **101/101**;
- Storage: **53/53**.

During qualification, two pre-existing long-running P6 coordinator-failover observations transiently
failed on the loaded Windows host. Neither failure reproduced in isolation; the complete P6 suite then
passed **9/9** unchanged, and the final complete serialized workspace run also passed P6 **9/9**. No
P6 production/test source was changed to obtain the green result.

The Clippy candidate also exposed that adding the P9 policy map made the in-process
`MetadataSyncResponse::Snapshot` enum variant materially larger than its delta variants. P9-B fixes the
Rust memory-layout issue structurally by boxing only the full metadata snapshot payload. No wire format,
controller command/snapshot bytes or authority semantics change; metadata-cache qualification remains
green.

Frozen-source audit confirms no change from authoritative P9-A to:

- `docs/FORMAT_V1.md`;
- `crates/astralog-storage/**`;
- original P2 `golden_vectors.rs`;
- P6 `p6_golden_vectors.rs`;
- P7 `p7_golden_vectors.rs`.

The Rust scope audit finds no P9-C Storage V2/manifest-mode cleaner/log-start/tombstone/rebootstrap
implementation pull-forward.

This evidence permits one bounded P9-B closure commit. P9-B is not authoritative until that exact clean
SHA repeats the required gates and receives the P9-B PASS Git note. Only after that note exists may
P9-C begin.
