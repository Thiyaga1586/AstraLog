# AstraLog P8-B Controller Topic-Performance Metadata Qualification Report

Date: 2026-10-05

Status: **P8-B CLOSURE COMMIT — EXACT-HEAD QUALIFICATION NOTE REQUIRED FOR AUTHORITY**

## Scope

P8-B closes actions **P8-019 through P8-038** only.

It activates controller-owned topic performance metadata and its public feature gate without
activating Storage V1 compression, client batching, persistence group commit, replication
pipelining, quotas or benchmarking.

The qualified entering authority is:

    5652821d6ef1c7a4e129dab3e265fff1daf6f589

That exact P8-A HEAD carries the P8-A qualification note authorizing P8-B only.

## Implemented controller model

P8-B introduces the checked controller-only performance model:

- CompressionPolicyV1 with exactly None, Lz4 and Zstd;
- TopicPerformancePolicyV1;
- checked topic maximum record bytes;
- checked topic maximum batch records;
- checked maximum logical uncompressed batch bytes;
- deterministic legacy-topic defaults of None plus frozen global limits.

Unknown compression values fail closed. Topic policy cannot exceed the frozen 8 MiB record
maximum, 10,000-record batch maximum, or the bounded P8 logical batch/decompression ceiling.

Historical V1/V2 controller state is not rewritten. It derives the deterministic default policy
when decoded.

## Additive controller persistence

Frozen ControllerCommand V1 and producer-session V2 bytes remain unchanged.

P8-B adds a separate V3 command envelope whose CreateTopic-with-performance mutation carries:

- the exact nested V1 CreateTopic command;
- the validated topic performance policy.

Topic creation and policy publication are one atomic committed controller transition. Feature
level below 4 rejects the V3 mutation without advancing metadata epoch.

Frozen ControllerSnapshot V1/V2 bytes remain unchanged.

P8-B adds ControllerSnapshotV3 as:

- exact nested V2 snapshot bytes;
- one strictly ordered fixed-width topic-performance table;
- outer CRC32C.

Malformed V3 snapshots reject duplicate/unsorted topics, unknown codecs, invalid policy bounds,
non-zero reserved bytes and noncanonical encodings before state publication.

V1 and V2 snapshot decoders derive default P8 policy while preserving existing allocator,
metadata, broker, assignment, movement and producer-session authority.

## Feature activation

P8-B uses the already-frozen P5 feature-level state machine.

Irreversible P8 controller metadata requires committed cluster feature level **4**.

Feature-level bump remains explicit, monotonic, commit-before-use, rejected if any ACTIVE/DRAINING
broker cannot support the requested level, and rejected if a static controller voter cannot support
the requested level.

Feature key **14**, version **1**, is advertised as supported/inactive before P8 readiness and
active only after committed level 4 has converged to the broker.

## Public topic API boundary

P8-B adds:

- CreateTopic V1;
- DescribeTopic V1.

Both carry the checked P8 topic performance policy. V0 remains available and retains its frozen
bytes.

The original P2 topic-admin golden vectors remain byte-identical.

Permanent API key 6 AlterTopicConfig remains unadvertised and undispatched. P8-B does not
introduce dynamic topic performance mutation.

## Metadata dissemination

Controller V3 state is authoritative. A V3 metadata epoch is not misrepresented as an old V1
incremental metadata delta.

When V3 state commits, retained V1 delta history is cleared so a broker behind that epoch receives
an authoritative full controller snapshot. Broker readiness and ApiVersions activation are derived
from the committed metadata state.

## RF3 and restart qualification

The real three-controller P8-B campaign proves:

- all controller voters advertise feature range 1..4;
- one ACTIVE broker advertises feature range 1..4;
- committed feature level 4 converges across the controller quorum;
- CreateTopic-with-performance commits an LZ4 policy atomically;
- every controller replica observes the identical committed policy;
- controller leader death elects a higher-term successor with the same policy;
- the stopped controller restarts and recovers the same feature level and policy.

The real dynamic-broker public campaign additionally proves:

- feature key 14 is active only after committed P8 readiness;
- CreateTopic/DescribeTopic advertise V0..V1 only after readiness;
- CreateTopic V1 commits a Zstd topic policy;
- DescribeTopic V1 returns the identical committed policy;
- AlterTopicConfig remains UNSUPPORTED_API_KEY.

## Frozen compatibility evidence

No Cargo manifest or dependency changes exist in P8-B.

No P8-C storage/compression-runtime work exists in this checkpoint.

The following source boundaries are tree-identical to entering P8-A authority 5652821:

- workspace/root Cargo manifests;
- astralog-storage;
- astralog-raft;
- astralog-groups;
- astralog-replication;
- original P2/P6/P7 golden-vector source files.

Focused P8-B qualification:

- controller P8 tests: **6/6 PASS**;
- protocol P8 codec test: **1/1 PASS**;
- real dynamic-broker P8 topic V1 integration: **1/1 PASS**;
- broker ApiVersions/readiness qualification: **1/1 PASS**.

Frozen protocol compatibility:

- original P2 goldens: **11/11 PASS**;
- P6 goldens: **6/6 PASS**;
- P7 goldens: **4/4 PASS**.

Mechanical gates:

- cargo fmt --all -- --check: **PASS**;
- cargo clippy --workspace --all-targets --all-features -- -D warnings: **PASS**;
- cargo test --workspace --doc -- --test-threads=1: **PASS**;
- git diff --check: **PASS**.

The authoritative pre-commit serialized all-target workspace run is:

    cargo test -j 1 --workspace --all-targets -- --test-threads=1

Result: **557 passed, 0 failed**.

Suite totals in that run:

- broker units: 22;
- P2 network: 9;
- P4 RF3: 1;
- P5/P8 dynamic broker integration: 5;
- P6 RF3 routing/failover/churn/public integration: 9;
- P7 failover: 12;
- P7 runtime: 2;
- client: 12;
- common: 3;
- controller: 113;
- groups: 55;
- protocol unit: 45;
- P2/P6/P7 goldens: 11 + 6 + 4;
- Raft: 122;
- replication: 82;
- storage: 44.

## Closure boundary

P8-019 through P8-038 are mechanically satisfied.

P8-C and every later checkpoint remain disabled until the exact P8-B closure commit:

1. is clean;
2. passes the exact-HEAD P8-B qualification gate;
3. carries the exact-HEAD PASS Git note.

Only then may **P8-C — P8-039 through P8-056** begin.

P8-C is the first checkpoint allowed to add pinned LZ4/Zstd dependencies or activate Storage V1
compression/runtime behavior.
