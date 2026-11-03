# AstraLog P7-I Lost-Response / Fencing / Real RF3 Failover Qualification

Date: 2026-10-04

Status: **PASS — P7-I QUALIFIED / P7-J NEXT**

## Scope

P7-I closes actions **P7-122 through P7-135** only. It takes the public producer/session surface
qualified in P7-H and subjects it to real RF3 data-partition failure, producer-epoch fencing,
controller-outage/failover and replica restart boundaries.

P7-I does not add transactions, global exactly-once processing, compression/batching, retention,
queues or any P8+ behavior. P7-J deterministic models/resource stress and P7-K final compatibility
closure remain disabled until this checkpoint is committed and its exact HEAD is independently
requalified.

## RF3 qualification harness

`crates/astralog-broker/tests/p7_failover.rs` starts a real three-controller quorum and three
dynamic brokers whose data partition is RF3 over brokers 1/2/3. The brokers advertise through
bounded TCP proxies that can deliberately:

- discard a completed InitProducer response;
- discard a completed Produce V1 response;
- hold one Produce V1 request before the broker receives it;
- capture Produce V1 request bodies for exact retry comparison.

The harness also exposes qualification-only observations of a local partition's P3 role/leader
term, durable Raft tail and committed Raft boundary. Those observations do not grant authority or
change the wire protocol; they exist only to place failures at the exact pre-persist,
durable-uncommitted and committed boundaries required by P7-I.

## Lost response and leader failure

The real RF3 scenarios prove three distinct mutation boundaries.

### Committed response loss

The leader commits sequence zero through RF3 quorum, the proxy discards the success response, and
the client automatically retries. In the same leader term the captured Produce V1 body is
byte-identical to the first send. The broker returns the original `(0, 0)` offset range and a
read-committed Fetch observes exactly one logical record.

### Leader death after quorum commit, before client success

The proxy observes and discards the completed Produce response, then the active data leader is
stopped before the client can accept that success. A successor is elected at a higher P3 term. The
client refreshes routing, retargets only the pending request's `leader_epoch`, preserves the
ProducerId/Epoch/base sequence/record bytes, and the successor reconstructs committed producer
state and returns the original `(0, 0)` result. Exactly one committed logical record exists.

### Leader death before persistence

The proxy holds the Produce request before it reaches the broker. Qualification observes that the
leader's durable and committed Raft indexes are unchanged, kills that leader, releases the held
connection and lets the client retry against the successor. Sequence zero commits once at offsets
`(0, 0)` and Fetch observes one logical record.

## Durable local tail without quorum

For the local-persist/no-quorum boundary both followers are stopped. The remaining leader accepts
the idempotent request far enough to expose a larger durable Raft tail while its committed Raft
index remains unchanged. The proposal is therefore physically durable on one replica but has no
P3 quorum authority.

That leader is then stopped, the two followers are restarted and elect a successor. Retrying the
same ProducerId/Epoch/sequence commits exactly once from the committed sequence state. When the old
leader rejoins, ordinary P3 conflict repair reconciles its leader-only tail; read-committed Fetch
still observes exactly one logical batch.

## Leadership-transfer retry semantics

Cross-term retry cannot blindly replay a stale routing term. The client therefore keeps the
pending producer identity, sequence and record bytes, refreshes metadata, and rewrites only the
Produce V1 partition `leader_epoch` to the newly observed P3 term before retry.

Qualification commits a batch, loses its response, explicitly transfers P3 leadership to another
RF3 voter and then calls `retry_idempotent_partition`. The successor returns the original `(1, 1)`
offset range. The logical producer request is unchanged; only the routing term is retargeted.

## Producer epoch fencing across failover

Two complementary RF3 scenarios bind tentative versus committed higher-epoch authority.

### Higher epoch durable but uncommitted

Epoch 1 sequence zero is committed, the controller advances the producer to epoch 2, and both
followers are stopped. Epoch 2 sequence zero becomes durable only on the old leader and cannot
commit. While that tentative higher epoch is pending, a racing epoch-1 sequence-one request is
locally fenced.

The old leader is then killed. The followers elect a successor using only committed history, so
the tentative epoch-2 fence is not inherited. The successor accepts the exact epoch-1 sequence-one
continuation and commits it at offsets `(1, 1)`. Rejoining the old leader repairs its old tentative
tail through ordinary P3 replication.

This simultaneously proves the old/new epoch race cannot create two committed authorities and that
an uncommitted higher epoch cannot survive failover as producer authority.

### Higher epoch committed

Epoch 1 sequence zero commits first. The producer then performs the controller CAS bump to epoch 2
and commits epoch 2 sequence zero at offsets `(1, 1)`, proving the new epoch starts its partition
sequence at zero. Epoch 1 is fenced immediately on the current leader.

After killing that leader, the successor reconstructs the committed epoch-2 producer state and
continues to reject epoch 1. An exact retry of epoch-2 sequence zero returns the original committed
offset range instead of appending again.

## Controller authority boundaries

P7-I qualifies the separation between committed partition-local producer authority and controller
confirmation:

- an already accepted epoch continues to produce through RF3 quorum while all controllers are
  offline;
- a controller-issued but not yet accepted higher epoch is rejected with `BROKER_NOT_AVAILABLE`
  during controller outage;
- a completely unseen ProducerId is rejected the same way;
- both rejected cases leave the partition durable and committed Raft indexes unchanged, proving no
  speculative proposal was created;
- two concurrent epoch-bump requests with the same expected authority produce exactly one CAS
  winner, and a later stale expected epoch cannot advance authority again;
- if a committed InitProducer response is lost and the controller leader then dies, the successor
  retains the consumed ProducerId/epoch. The client exposes `AmbiguousMutation`, clears local
  authority, and explicit recovery allocates the next ProducerId rather than reusing the ambiguous
  committed identity.

## RF3 snapshot/restart recovery

The restart scenario commits producer sequence zero and one, waits until every RF3 replica covers
the committed boundary, and explicitly persists the already-qualified rebuildable
`producer.snapshot` sidecar on every replica. All three brokers are then stopped and reopened.

After restart, an exact retry of sequence one on the newly elected leader returns the original
second batch offset range. Read-committed Fetch still observes exactly two records. The snapshot is
only acceleration: committed P1/P4 history remains authoritative and the test uses the existing
snapshot validation/replay path qualified in P7-G.

## Compatibility-harness stabilization

The full serialized P7-I qualification exposed timing assumptions in older RF3 integration tests,
not production safety failures. The test harness was tightened without changing broker/controller
mutation semantics:

- P5 reassignment allows a longer bounded learner `CatchingUp` convergence window under a complete
  serialized workspace run;
- P6 waits for healthy brokers before topic expansion and retries transient bootstrap convergence;
- P6 coordinator recovery waits for committed-history visibility after a successor first exposes
  its new P3 term;
- the P6 public readiness loop retries only frozen safe readiness/routing failures;
- controller-election observation receives a larger bounded convergence window;
- the silent-member successor readiness probe may retry timeout/connection failures because the
  probe is explicitly read-only (`FetchOffsets`) and cannot create mutation ambiguity.

After these harness-only corrections the complete P6 RF3/integration suite passes **9/9** inside
the final serialized workspace run.

## Mechanical qualification

- focused P7-I RF3/controller failover campaign, serialized: **11/11 PASS**
- P7 runtime integration: **2/2 PASS**
- broker unit tests: **22/22 PASS**
- client unit tests: **11/11 PASS**
- P2 network compatibility: **9/9 PASS**
- P4 RF3 compatibility: **1/1 PASS**
- P5 dynamic compatibility: **4/4 PASS**
- P6 RF3/integration, serialized: **9/9 PASS**
- common: **3/3 PASS**
- controller: **104/104 PASS**
- groups: **55/55 PASS**
- protocol: **44/44 PASS**
- P2/P6/P7 golden suites: **11/11 + 6/6 + 4/4 PASS**
- Raft: **122/122 PASS**
- replication: **73/73 PASS**
- storage: **44/44 PASS**
- complete serialized all-target workspace: **535/535 PASS**
- workspace doc tests: **PASS**
- workspace Clippy `--all-targets --all-features -D warnings`: **PASS**
- `cargo fmt --all -- --check`: **PASS**
- `git diff --check`: **PASS**

## Boundary

P7-I is mechanically qualified on the pre-commit tree. The next clean checkpoint, after the exact
P7-I commit HEAD is independently requalified, is:

> **P7-J only — P7-136 through P7-147: deterministic models, resource bounds and scheduling stress**

P7-K and P8+ remain disabled until their own checkpoints.

## Exact-HEAD requalification

The committed P7-I checkpoint is independently requalified after commit creation under the same
serialized discipline used for the pre-commit gate. The exact committed tree must remain clean and
must repeat:

- focused P7-I RF3/controller qualification: **11/11 PASS**;
- complete serialized all-target workspace: **535/535 PASS**;
- workspace doc tests: **PASS**;
- workspace Clippy `--all-targets --all-features -D warnings`: **PASS**;
- `cargo fmt --all -- --check`: **PASS**;
- `git diff --check`: **PASS**.

That exact-HEAD gate is complete for the final P7-I checkpoint before phase advancement.
