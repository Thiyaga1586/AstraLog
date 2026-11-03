# P7-F Runtime Admission / Pending Duplicate Suppression Qualification

Date: 2026-10-03

Status: **PASS — P7-F QUALIFIED / P7-G NEXT**

This report closes actions **P7-077 through P7-093** only. P7-F binds the
already-qualified controller producer authority, pure partition producer state
machine and committed Storage V1 recovery surface into the P4 partition runtime.
It does **not** implement `producer.snapshot`, public InitProducer/Produce V1
dispatch, client producer sessions, RF3 lost-response qualification or any
P7-G+ behavior.

## Runtime authority boundary

`ReplicatedPartition::propose_idempotent_records` is separate from legacy
`propose_records`. Admission requires:

- the local replica to be the current P3 leader;
- the exact current leader epoch;
- quorum acknowledgement;
- non-empty records with explicit timestamps >= 0;
- a checked ProducerAuthority and ProducerSequence range;
- the canonical P7 digest;
- bounded proposal count/bytes.

Legacy Produce V0 continues through the old path. P7-F adds no public Wire
dispatch or ApiVersions activation.

The replication crate remains controller-free. For unseen ProducerId or a
ProducerEpoch greater than committed partition authority, the actor returns
`ControllerValidationRequired` without proposing. `DynamicBrokerControl`, which
already owns both controller routing and the replication manager, performs a
fresh read of committed `producer_authority` and retries with one of three
explicit evidence states: exact confirmed authority, missing producer, or no
evidence. Equal already-accepted epochs use only committed partition state and
therefore do not add controller latency to the steady-state Produce path.

## Committed state and duplicate suppression

The partition runtime owns a bounded volatile producer cache. A cache miss does
not imply a new producer: it reconstructs through
`PartitionPersistence::recover_committed_producer_states()` before admission.
The cache contains only committed P7-D state and is capped at **4,096 entries**.
Deterministic eviction skips producer identities that currently have unresolved
idempotent authority; evicted state remains reconstructable from committed P1
history.

At most one unresolved idempotent range exists per producer-partition:

- an exact pending duplicate returns the same ProposalReceipt and broker waiters
  attach to the same proposal;
- pending bytes and proposal count are charged only once;
- the same range with changed digest is rejected;
- a different unresolved range receives bounded Busy;
- a lower epoch is fenced against the tentative higher pending authority.

If the range is already committed inside the retained five-batch window, P7-D
returns its original offsets/Raft index immediately. No new P3 proposal is
created and neither proposal numbering nor the committed stream high watermark
advances.

## Commit-only authority advancement

Tentative pending identity is stored separately from committed producer state.
The runtime calls P7-D `apply_committed` only after the P3 durable commit index
covers the idempotent proposal Raft index. Local admission, append construction,
pending coalescing and uncommitted persistence do not themselves advance
accepted ProducerEpoch or next sequence.

Conclusive proposal rejection or leadership loss clears pending idempotent
authority. Crash qualification additionally holds a higher-epoch proposal before
persistence, drops the runtime, restarts with an empty volatile cache and proves
that lazy recovery sees only the old committed epoch and accepts its exact next
sequence. P7-F deliberately does not claim that a locally durable RF1 tail must
disappear: such a tail can legitimately commit by prefix implication after the
same sole voter re-elects itself. The RF3 locally-durable/uncommitted truncation
boundary is reserved for P7-I.

## Real controller/broker qualification

One bounded real integration topology uses a three-controller quorum and one
dynamic RF1 broker at committed feature level 3. It mechanically proves:

1. an unknown ProducerId is rejected before partition proposal;
2. controller-issued epoch 1 sequence 0 commits;
3. exact committed retry returns the original offsets and Raft index;
4. changed-content retry is rejected;
5. exact next sequence commits once;
6. an unissued higher epoch is rejected;
7. a controller CAS bump to epoch 2 is accepted only at sequence zero;
8. committed epoch 2 locally fences epoch 1;
9. after all three controllers shut down, accepted epoch 2 sequence 1 still
   commits through the partition path.

This broker capability is exposed only through a deliberately internal
qualification handle. InitProducer key 20 and Produce V1 remain undispatched.

## Resource and scheduling qualification

- producer cache: **4,096 entries maximum**;
- one pending idempotent range per producer-partition;
- ordinary P4 pending proposal count/byte limits remain authoritative;
- exact coalescing does not double-charge pending bytes;
- saturated idempotent work does not prevent Raft control ticks from running;
- the complete P6 RF3 routing/session/coordinator suite remains **9/9 PASS**.

## Mechanical qualification

Finished-tree qualification is clean:

- `cargo fmt --all -- --check`: **PASS**
- `git diff --check`: **PASS**
- workspace Clippy `--all-targets --all-features -D warnings`: **PASS**
- focused P7-F replication runtime tests: **4/4 PASS**
- real controller/broker P7-F integration: **1/1 PASS**
- broker units: **19/19 PASS**
- P2 network: **9/9 PASS**
- P4 RF3: **1/1 PASS**
- P5 dynamic: **4/4 PASS**
- P6 RF3: **9/9 PASS**
- client: **10/10 PASS**
- common: **3/3 PASS**
- controller: **104/104 PASS**
- groups: **55/55 PASS**
- protocol: **44/44 PASS**
- original P2 goldens: **11/11 PASS**
- P6 goldens: **6/6 PASS**
- P7 goldens: **4/4 PASS**
- Raft: **122/122 PASS**
- replication: **66/66 PASS**
- storage: **44/44 PASS**
- complete serialized all-target workspace: **512/512 PASS**
- workspace doc tests: **PASS**

During qualification, an intentionally strengthened RF1 crash assertion was
found to be invalid: a locally durable higher-epoch tail on the sole voter can
legitimately become committed after that same voter restarts and elects itself.
The test was corrected to the actual P7-F boundary (crash before persistence),
with no production behavior weakened or changed by that correction. P7-I retains
the separate RF3 local-persist-before-quorum/truncation proof obligation.

## Boundary

P7-F is closed. The next clean checkpoint is:

> **P7-G only — P7-094 through P7-106: producer snapshot and crash recovery**

P7-G may add only the derived `ProducerSnapshotV1` acceleration/recovery path.
Public broker/client P7 activation, real RF3 lost-response qualification and all
later P7 work remain disabled until their own checkpoints.
