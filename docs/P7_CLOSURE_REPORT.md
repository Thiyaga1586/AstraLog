# AstraLog P7 — Idempotent Producer and Duplicate Detection Closure Report

Date: 2026-10-04

Status: **PASS — P7 CLOSURE COMMIT; AUTHORITY REQUIRES EXACT-HEAD PASS GIT NOTE**

## 1. Closure statement

P7 adds exactly-once duplicate suppression for a single AstraLog producer session without changing
the frozen P1 Storage V1 byte layout, legacy Produce V0 behavior, P3 consensus authority, P4 quorum
visibility, P5 placement authority or P6 consumer-group/offset ownership rules.

The phase closes around three explicit authorities:

- the P5/P7 controller Raft state owns ProducerId allocation and canonical ProducerEpoch;
- each data partition owns committed accepted ProducerEpoch, exact next sequence and a bounded recent
  committed retry window derived from committed P1/P4 history;
- the client owns only bounded local session/routing/pending state and never manufactures durable
  producer authority.

An exact committed retry returns the original committed offset range and does not append another
logical record. A changed retry, partial overlap, gap, stale producer epoch or unverifiable evicted
retry fails explicitly. Producer state advances only from durable committed history.

P7 is not considered closed until the commit containing this report and the fully checked
`P7_CHECKLIST.md` passes the exact-HEAD procedure in section 15 and receives a qualification Git
note. P8 and later remain disabled until then.

## 2. Qualified P7 history

P7 was implemented and independently qualified in bounded checkpoints:

- **P7-A** — producer-idempotency contract, invariants and checklist freeze;
- **P7-B** — checked producer types, InitProducer V0 / Produce V1 codecs, canonical SHA-256 identity,
  command/snapshot version envelopes and immutable P7 goldens;
- **P7-C** — controller ProducerId/ProducerEpoch authority, V2 controller persistence/snapshots and
  lost-response/failover qualification;
- **P7-D** — pure partition producer state machine and exhaustive small-state oracle;
- **P7-E** — activation of the already-reserved Storage V1 producer metadata and committed-history
  reconstruction;
- **P7-F** — bounded runtime admission, controller validation boundary and pending duplicate
  coalescing;
- **P7-G** — rebuildable bounded producer snapshot acceleration and crash recovery;
- **P7-H** — readiness-gated public broker/client InitProducer and Produce V1 integration;
- **P7-I** — real RF3 lost-response, fencing, controller-outage, leader-loss, transfer and restart
  qualification;
- **P7-J** — deterministic models, resource/corruption bounds and scheduling/failover stress;
- **P7-J boundary repair** — current-term leader-readiness gating discovered by exact-HEAD
  requalification and independently qualified at `3373976`;
- **P7-K** — compatibility/scope audit and exact P7 closure.

The final implementation checkpoints immediately before P7-K are:

- `069b635` — controller producer-session authority;
- `9ffc744` — pure partition producer state machine;
- `de9d11a` — Storage V1 producer metadata integration;
- `28024d0` — runtime admission and duplicate suppression;
- `ef99267` — producer snapshot and crash recovery;
- `06906e2` — public broker/client integration;
- `4b28545` — RF3 failover and fencing;
- `f2afbd1` — deterministic models/resource bounds/stress;
- `3373976` — idempotent leader-readiness boundary repair.

## 3. Frozen P7 safety invariants

P7 closes with these producer invariants mechanically qualified:

1. **I-15 — exact committed retry deduplication.** An exact retained retry resolves to its original
   committed stream-offset range/Raft index without a second append.
2. **I-16 — old epoch fencing.** Once a higher ProducerEpoch is committed for a partition, every
   lower epoch is rejected there; tentative uncommitted higher-epoch state is not inherited as
   committed authority after failover.
3. **I-17 — no silent sequence gaps/overlaps.** New work is accepted only at exact next sequence;
   gaps, partial overlap, changed content and too-old unverifiable retries fail explicitly.
4. **Committed-history authority.** Accepted epoch, next sequence and retry results are reconstructed
   from committed P1/P4 history; caches and producer snapshots are rebuildable accelerators only.
5. **Commit-before-success.** Produce V1 success requires quorum commit and committed producer-state
   application. InitProducer success requires committed controller authority.
6. **Bounded operation.** Recent retry history, runtime caches, pending work, client partition state,
   controller session tables, snapshots, retry budgets and all public/internal messages have finite
   enforced bounds.

## 4. Controller ProducerId / ProducerEpoch authority

The canonical controller state contains a deterministic ProducerId -> ProducerEpoch table. New
producer sessions consume the existing durable P5 ProducerId allocator, begin at epoch 1 and never
reuse a committed/lost-response ID. Restarting a producer is a compare-and-swap epoch bump requiring
the exact current authority and checked next epoch.

Controller commands are version-dispatched so historical V1 metadata commands remain byte-compatible
while producer-session mutations use V2. Controller snapshots similarly retain V1 backward decode and
use V2 only when producer-session state must be represented.

Three-controller qualification proves commit-before-response, concurrent epoch bump single-winner
semantics, ambiguous InitProducer across controller leader loss, restart durability and ProducerId
non-reuse.

## 5. Partition producer state and sequence semantics

`PartitionProducerStateV1` contains only committed producer authority, exact next sequence and the
five newest committed retry records. Admission itself is side-effect free.

Equal committed epoch accepts only exact next sequence or an exact retained duplicate. Higher epoch
requires sequence zero and resets sequence/retry state only when that first batch commits. Lower
epochs are fenced. Committed replay is deterministic and fails closed on conflicting metadata,
regression, gaps or unverifiable overlap.

P7-J expands the oracle across deeper histories/window edges and adds deterministic commit/response
loss/failover permutations, epoch races, near-maximum arithmetic and a fixed-seed multi-producer /
multi-partition campaign.

## 6. Storage V1 and committed recovery

P7 intentionally does **not** introduce Storage V2. P1 already reserved:

- the idempotent attribute bit;
- ProducerId;
- ProducerEpoch;
- base producer sequence.

P7 activates those fields and validates zero/reserved/wrap conditions without moving any byte,
changing any header version or changing index formats. The existing frame CRC covers producer
metadata.

ApplicationCommand V1 remains producer-less. ApplicationCommand V2 materializes the existing producer
fields. Recovery scans committed frames only, recomputes canonical record digests and rebuilds
partition producer state. Uncommitted tails never become producer authority.

## 7. Runtime admission and current-term leader readiness

The idempotent runtime requires:

- local P3 leader role;
- exact current leader epoch;
- quorum acknowledgements;
- bounded proposal capacity;
- valid producer authority/sequence;
- committed producer-state reconstruction.

Exact pending duplicates coalesce onto one proposal. Changed-content duplicates fail; a different
range for the same producer-partition receives bounded Busy while one range is unresolved.

Exact-HEAD P7-J requalification exposed one narrow Raft boundary: immediately after election a node
can be local leader before a current-term entry is durably committed. Prior-term history is not yet
safe as committed duplicate-suppression authority under the P3 current-term commit rule. The bounded
repair at `3373976` therefore returns transient Busy until the durable commit boundary belongs to the
current term. No producer state or proposal is mutated before this gate.

The repair exact HEAD is independently qualified **548/548 PASS** and carries its own Git note before
P7-K begins.

## 8. Producer snapshot acceleration

`ProducerSnapshotV1` is a CRC-protected, canonical, hard-bounded per-partition sidecar containing only
derived committed producer state. It is validated against partition identity and committed Raft
boundary and is crash-safely published.

Missing, corrupt, noncanonical, wrong-identity, over-limit or ahead snapshots are discarded and
rebuilt from committed P1 history. Committed suffix replay is bounded in pages. Snapshot/cache
eviction cannot change retry decisions or create authority.

## 9. Public broker/client integration

InitProducer V0, Produce V1 and their ApiVersions exposure activate only after committed P7 readiness.
Legacy Produce V0 remains available with its frozen semantics.

The Rust client stores one bounded producer authority plus finite per-partition next-sequence/pending
state. It allows at most one unresolved range per producer-partition, retains exact Produce V1 bytes
for same-route retries, retargets only the leader term across a routing-term change and advances local
sequence exactly once after authoritative success.

An ambiguous InitProducer clears local authority. Recovery obtains a fresh committed session rather
than assuming the old authority remains safe.

## 10. Real RF3 failure qualification

P7-I/J qualification covers:

- lost Produce response after quorum commit;
- data leader death after quorum commit before client success;
- leader death after local durable persistence without quorum;
- leader death before request persistence;
- explicit leadership transfer;
- tentative higher-epoch local fencing followed by failover;
- committed higher epoch and permanent old-epoch fencing;
- complete controller outage for already-accepted partition authority;
- rejection of unseen/higher authority during controller outage;
- concurrent controller epoch-bump CAS;
- ambiguous InitProducer across controller failover;
- full broker restart from committed producer snapshots;
- six repeated seeded RF3 leader-kill/restart rounds.

Every exact committed retry returns the original range and the final committed history contains one
logical copy per accepted producer sequence.

## 11. Resource, corruption and scheduling qualification

P7 remains finite under adversarial pressure:

- recent committed retry window: newest five batches;
- reconstructable runtime producer cache: 4,096 resident entries;
- producer snapshot: hard 64 MiB file bound;
- controller producer sessions: hard logical/snapshot bounds;
- client producer partitions: configured finite bound with one-over rejection;
- pending idempotent proposal count/bytes: existing bounded P4 resources;
- client retry count: finite configured maximum.

Malformed producer snapshots and ControllerSnapshotV2 producer metadata fail closed even with
otherwise valid CRCs. Sequence and stream-offset arithmetic are non-wrapping.

A 257-contender runtime stress keeps proposal resources bounded while Raft control ticks continue and
leadership remains healthy.

## 12. P1-P6 compatibility audit

### P1 — Storage V1

P1 format freeze is `3515000`. `docs/FORMAT_V1.md` and Storage V1 index layout remain unchanged.
`format.rs` changes only validate the already-reserved producer fields when the frozen idempotent bit
is active. Storage is **44/44 PASS**, including exact non-idempotent/index/meta goldens and idempotent
producer-metadata/CRC checks.

### P2 — legacy wire

P2 closure is `a3cf08d`. The original `crates/astralog-protocol/tests/golden_vectors.rs` is
source-identical to that closure and remains **11/11 PASS**. P6/P7 new protocol goldens are separate
**6/6 + 4/4 PASS** suites. Produce V0 and all earlier V0 APIs remain byte-exact.

### P3 — consensus

`crates/astralog-raft` has zero source delta from P6 closure `a76b136`. Full Raft regression is
**122/122 PASS**. P7 consumes the existing current-term commit and durable barrier semantics rather
than altering them.

### P4 — replication / committed visibility

P7 adds a separate idempotent application/runtime path while retaining ordinary Produce V0. Full
replication is **82/82 PASS** and the frozen RF3 quorum/redirect/leader-term/committed-read integration
is **1/1 PASS**.

### P5 — controller / placement

P7 adds producer-session state to the controller but does not change placement, movement, broker
lifecycle or feature-level authority. P5 dynamic integration is **4/4 PASS** and complete controller
qualification is **106/106 PASS**.

### P6 — groups / offsets

`crates/astralog-groups` has zero source delta since P6 closure. Group deterministic/model
qualification is **55/55 PASS** and serialized RF3 public group/offset/fencing/churn integration is
**9/9 PASS** on the exact boundary-repair HEAD.

## 13. Later-phase scope audit

P7 deliberately does **not** implement later architecture phases:

- **P8 compression/performance:** no compression codec/runtime, batching-performance redesign or
  benchmark implementation is added. P7 continues to write the pre-existing Storage V1
  `Compression::None`.
- **P9 retention/compaction:** no retention timer, physical producer-history compactor, segment
  rewrite policy or cleanup lifecycle is introduced. The P7 committed-state recovery API is an
  explicit handoff surface for later P9 work.
- **P10 work queues/retries/DLQ:** producer retry is request idempotency, not queue redelivery. No
  visibility lease, delivery counter, retry queue or dead-letter queue semantics are introduced.
- **P11 production security/operations:** no TLS/mTLS authentication, ACL enforcement, drain
  workflow, continuous balancing or upgrade protocol is added.
- **P12 final chaos/benchmark closure:** P7 qualification is correctness/stress evidence, not a
  final production throughput/latency or chaos-completion claim.

There is no Cargo manifest/dependency delta from P6 closure. A source-delta debt search finds no new
`TODO`, `FIXME`, `XXX` or `HACK` marker attributable to P7 correctness work.

## 14. Final P7-K qualification

The exact P7-K closure-candidate tree is mechanically green before commit:

- focused P7-J replication model/resource/corruption tests: **9/9 PASS**;
- focused P7-J controller bound/corruption tests: **2/2 PASS**;
- focused P7-J client bound test: **1/1 PASS**;
- complete real P7 RF3/controller failover/stress suite: **12/12 PASS**;
- P7 runtime integration: **2/2 PASS**;
- serialized P6 RF3/group/offset/fencing/churn/public integration: **9/9 PASS**;
- Storage V1: **44/44 PASS**;
- original P2 / P6 / P7 protocol goldens: **11/11 + 6/6 + 4/4 PASS**;
- Raft: **122/122 PASS**;
- frozen P4 RF3 integration: **1/1 PASS**;
- P5 dynamic integration: **4/4 PASS**;
- controller: **106/106 PASS**;
- groups: **55/55 PASS**;
- replication: **82/82 PASS**;
- formatting: **PASS**;
- `git diff --check`: **PASS**;
- workspace Clippy `--all-targets --all-features -D warnings`: **PASS**;
- workspace doc tests: **PASS**;
- complete serialized all-target workspace: **548/548 PASS**.

The complete workspace is intentionally executed with one Cargo job and one test thread on this
Windows qualification host. Serialization changes neither coverage nor assertions; it avoids
cross-suite RF3 resource contention and local paging pressure that are irrelevant to the distributed
correctness being proved.

No P7 production source is changed by P7-K. The closure-candidate delta is limited to this report,
the fully checked P7 checklist and canonical status documentation.

## 15. Exact-HEAD closure procedure

1. finish P7-153 and record the final pre-commit qualification matrix;
2. check P7-154/P7-155 and commit this report, checklist and canonical status updates as one P7
   closure commit;
3. require `git status --short` to be empty and resolve the exact SHA;
4. rerun focused P7 qualification plus formatting, Clippy, doc tests and complete serialized
   all-target workspace on that exact SHA;
5. repeat the frozen P1/P2/P3/P4/P5/P6 compatibility/source audit;
6. attach a Git note to the closure commit recording exact-HEAD PASS evidence;
7. only then declare P7 closed and permit **P8 planning/contract freeze**.

P8 implementation remains disabled until its own contract/checklist is separately frozen.
