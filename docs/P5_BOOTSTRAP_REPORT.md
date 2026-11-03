# AstraLog P5-E Controller Bootstrap Qualification

Status: **QUALIFIED**

P5-E closes actions P5-030 through P5-034 only. Broker membership,
join/rejoin, heartbeats, placement and dynamic partition assignment remain outside
this checkpoint.

## Durable bootstrap manifest

Controller bootstrap authority is persisted in `controller.bootstrap.v1` before
ControllerRuntime may open P5-C persistence or serve control-plane work.

The canonical V1 manifest is:

```text
8 bytes   magic = ASTCBT01
u16 LE    version = 1
u16 LE    header_len = 32
16 bytes  cluster_id
u16 LE    voter_count = 3 or 5
u16 LE    reserved = 0
u64 LE[]  controller voter IDs, strictly ascending and distinct
u32 LE    CRC32C over all preceding bytes
```

Decoding rejects zero cluster IDs, unsupported versions, malformed lengths,
non-zero reserved bits, invalid voter cardinality, duplicate/non-canonical voter
ordering, CRC corruption and trailing data.

## Create-once publication

Bootstrap publication does not overwrite an existing manifest.

1. The canonical manifest is written to a unique staging file.
2. The staging file is fsynced.
3. Publication uses an atomic create-once hard link to
   `controller.bootstrap.v1`.
4. The parent directory is fsynced.
5. The staging file is removed.
6. The published manifest is decoded and compared with the requested identity.

Concurrent conflicting bootstraps therefore cannot last-writer-win. Exactly one
manifest becomes authoritative; a losing bootstrap must validate against the
winner and fails if cluster ID or voter set differs.

Repeated initialization with the identical cluster ID and voter set is
idempotent and does not rewrite authority.

## Persistence-before-serving gate

ControllerRuntime now validates the bootstrap manifest before opening P5-C
persistence. A controller directory with no bootstrap manifest cannot start a
controller runtime.

Likewise, bootstrap refuses to retroactively adopt a directory containing
controller persistence files without a manifest. This prevents pre-existing
Raft bytes from being assigned a cluster identity after the fact.

## Split-bootstrap and foreign-cluster fencing

The complete static controller voter set is part of the durable manifest.
Runtime startup compares both:

- configured cluster ID; and
- configured static P3 voter set

against the manifest before serving.

A different cluster ID or a different controller voter set fails closed.

The P5-D transport continues to fence foreign cluster IDs at the network
boundary; P5-E adds the durable local identity source used before that transport
can start.

## Qualification evidence

Controller checkpoint suite after P5-E:

- **49 passed, 0 failed**
- canonical/CRC bootstrap round trip
- create-once + identical-bootstrap idempotence
- conflicting cluster-ID rejection
- conflicting voter-set rejection
- concurrent conflicting bootstrap create-once race
- persistence-without-manifest rejection
- runtime refusal before bootstrap
- runtime foreign-cluster/static-voter-set refusal
- existing P5-D three-node controller election/failover/rejoin regression

Full pre-commit workspace qualification:

- `cargo fmt --all -- --check` — PASS
- workspace Clippy with `-D warnings` — PASS
- workspace tests — **321 passed, 0 failed**
- P3 regression — **120 passed, 0 failed**
- P4 replication — **46 passed, 0 failed**
- storage — **43 passed, 0 failed**
- protocol goldens — **11 passed, 0 failed**
- all doc tests — PASS
- P1-P4 production-source diff from the P5-C baseline — empty

## Scope boundary

P5-E does not implement or activate:

- broker registration or rejoin;
- broker incarnation runtime fencing;
- heartbeats or failure detection;
- placement;
- metadata dissemination;
- dynamic P4 assignments;
- replica movement orchestration.

Those remain later P5 checkpoints.