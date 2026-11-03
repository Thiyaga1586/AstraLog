# AstraLog P7-J Deterministic Models / Resource Bounds / Scheduling Stress Qualification

Date: 2026-10-04

Status: **PASS — P7-J QUALIFIED / P7-K NEXT**

## Scope

P7-J closes actions **P7-136 through P7-147** only. It adds deterministic qualification around
the idempotent-producer mechanisms already frozen and qualified in P7-D through P7-I.

P7-J introduces **no new production protocol, authority source, storage format, retry semantic or
P8+ feature**. All implementation changes in this checkpoint are tests. The production producer
state machine, runtime, controller snapshot format, client behavior, P1 Storage V1 bytes and P2
Wire V1 contract remain unchanged.

## Bounded producer state-space model

The P7-D admission oracle is extended across deeper committed histories and the five-batch retained
retry-window boundary. The P7-J bounded search varies accepted ProducerEpoch 1 through 3, one
through eight committed batches, presented ProducerEpoch 1 through 4, sequence positions from zero
through two beyond the current next sequence, and record counts one through three.

Every admission is checked against the frozen I-15/I-16/I-17 rules, and every admission attempt is
required to leave committed state unchanged. The campaign also verifies that retained retry state
never grows beyond five committed batches.

## Commit / response-loss / failover model

A compact permutation model separates whether the logical range committed before failover from
whether its client response was lost. If the range committed, successor admission must classify
the exact retry as Duplicate and return the original stream-offset/Raft identity. If the range did
not commit, the successor may admit the same sequence exactly once and then materialize one
committed range. Response loss never creates a second committed sequence range.

The final modeled state always has exactly one committed sequence-zero range and next sequence one.

## Higher-epoch / old-epoch race model

The deterministic epoch-race model covers both relevant committed orderings: epoch 2 sequence zero
committing before an old epoch-1 continuation, and epoch 1 exact continuation committing before
epoch 2 sequence zero. In both cases committed producer authority is monotonic. Once epoch 2 is
committed, subsequent epoch-1 admission is fenced and cannot append. Admission of a tentative
higher epoch remains side-effect free until committed apply, preserving the P7-I failover boundary.

## Recent-window eviction

The eviction campaign commits twelve consecutive one-record batches to one producer. The runtime
state retains exactly the newest five retry records. Sequences 7 through 11 deduplicate when
retried with their exact digest; sequences 0 through 6 are outside the retained window and fail
explicitly with OUT_OF_ORDER_SEQUENCE. No evicted retry is guessed or silently appended.

## Pending duplicate / coalescing campaign

A held producer range is subjected to **64 exact duplicate attempts**. Every identical retry
coalesces to the original ProposalId, pending proposal bytes do not grow, and the producer has
exactly one unresolved idempotent range. Changed content for the same sequence fails explicitly,
and a different unresolved sequence range receives bounded Busy. No retry allocates a second
proposal.

## Seeded multi-producer / multi-partition determinism

A fixed-seed campaign executes **192 deterministic actions** across four ProducerIds and three
logical partitions. Actions include exact-next commits, higher-epoch commits, retained duplicates,
old-epoch fences and sequence gaps.

The campaign produces both a canonical byte trace and a final producer-state summary. Running the
same seed twice must produce byte-identical trace/state, while a different seed must change the
trace. Every final producer retains at most five retry records.

## Arithmetic boundaries

Near-u32-max sequence ranges are exercised across record counts one through four. Exactly valid
ranges are accepted, while any range whose last sequence or next sequence would wrap is rejected.
The same campaign exercises stream-offset arithmetic at the exact u64 maximum boundary: a batch
ending exactly at the maximum is valid, while a one-over end offset is rejected.

## Producer-session and snapshot bounds

P7-J binds both client-local and controller-durable resource limits:

- a client producer session configured for two partition states admits exactly two distinct
  producer-partition entries and rejects the third atomically with no map mutation;
- ControllerSnapshotV2 is rejected when the byte limit is one byte below its canonical encoding;
- a valid-CRC snapshot declaring MAX_PRODUCER_SESSIONS + 1 is rejected before proportional
  session decoding/allocation;
- the same byte bound applies symmetrically to controller snapshot encode and decode.

## Partition cache / snapshot bounds

The already-qualified production cache test is part of the P7-J gate. Exactly 4,096 producer states
fit the resident cache; unresolved producer identity is protected from eviction; inserting
ProducerId 4,097 keeps the cache at 4,096 entries and deterministically evicts reconstructable
state. The independent snapshot-reconstruction proof reloads an evicted producer and preserves the
exact duplicate/original-offset decision.

## Malformed / corrupt producer recovery campaign

The corruption gate combines three independently bounded layers. Valid-CRC ProducerSnapshotV1
mutations cover unsupported version, non-zero reserved header/entry/retry fields, zero ProducerId,
zero ProducerEpoch, zero retained-batch count and zero retained Raft index. Existing P7-G recovery
also proves missing/corrupt/ahead derived snapshots are discarded and reconstructed from committed
P1 history.

Valid-CRC ControllerSnapshotV2 mutations cover zero ProducerId, zero ProducerEpoch, non-zero
reserved metadata and noncanonical producer-session ordering. Every mutation is rejected.

The frozen Storage V1 producer-metadata qualification remains green: CRC damage is detected, and
valid-CRC zero ProducerId, zero ProducerEpoch and sequence-wrap mutations are rejected rather than
repaired or interpreted.

## Retry-storm scheduling

A runtime with a one-proposal admission bound is placed under **257 consecutive idempotent
contender attempts** while one proposal occupies the bounded slot. Every contender fails with the
frozen pending-proposal limit, the producer cache remains bounded, and a Raft control tick is
executed after every rejected attempt. The partition remains leader and nonfatal throughout the
campaign. The full P6 RF3/integration suite remains 9/9 green in the serialized workspace gate.

## Repeated real RF3 failover stress

The P7-I real three-controller / three-broker RF3 harness is reused for a six-round seeded campaign.
One producer session is kept across the entire campaign. For each sequence 0 through 5, the current
leader commits exactly that sequence, is stopped, and a different higher-term successor is elected.
The exact committed sequence/payload is retried on the successor and must return the original
offset range rather than append. The seed may request one additional exact duplicate, which must
return the same offsets. The old leader is restarted and required to catch up before the next
round.

The leader changes every round, and the final read-committed Fetch contains exactly six logical
records. No supported retry creates a duplicate committed sequence range.

## Mechanical qualification

Focused and compatibility qualification on the finished pre-commit tree:

- P7-J replication model/stress tests: **9/9 PASS**
- P7-J controller bounds/corruption tests: **2/2 PASS**
- P7-J client bound test: **1/1 PASS**
- P7-J repeated real RF3 failover stress: **1/1 PASS**
- complete P7-I + P7-J failover suite, serialized: **12/12 PASS**
- broker unit tests: **22/22 PASS**
- P2 network compatibility: **9/9 PASS**
- P4 RF3 compatibility: **1/1 PASS**
- P5 dynamic compatibility: **4/4 PASS**
- P6 RF3/integration, serialized: **9/9 PASS**
- P7 runtime integration: **2/2 PASS**
- client complete suite: **12/12 PASS**
- common: **3/3 PASS**
- controller complete suite: **106/106 PASS**
- groups: **55/55 PASS**
- protocol: **44/44 PASS**
- P2/P6/P7 golden suites: **11/11 + 6/6 + 4/4 PASS**
- Raft: **122/122 PASS**
- replication complete suite: **82/82 PASS**
- storage: **44/44 PASS**
- complete serialized all-target workspace: **548/548 PASS**
- workspace doc tests: **PASS**
- workspace Clippy with all targets/features and warnings denied: **PASS**
- formatting check: **PASS**
- git diff hygiene: **PASS**

## Boundary

P7-J is mechanically qualified. The next clean checkpoint, after this commit is independently
requalified on its exact clean HEAD, is:

> **P7-K only — P7-148 through P7-155: compatibility audit and exact P7 closure**

P8 and later remain disabled until P7-K closes P7 itself.

## Exact-HEAD requalification

Before phase advancement, the final committed P7-J checkpoint is independently requalified under
the same serialized discipline. The exact clean HEAD must repeat the focused P7-J model/bound/stress
qualification, complete P7-I + P7-J failover suite, complete serialized all-target workspace
**548/548**, workspace doc tests, workspace Clippy with warnings denied, formatting and diff
hygiene.

The first exact-HEAD gate exposed a narrow current-term leader-readiness boundary and therefore did
**not** authorize P7-K directly from commit `f2afbd1`. The bounded correction and its requalification
evidence are recorded in `P7_J_BOUNDARY_REPAIR_REPORT.md`. That repair's exact clean HEAD is now the
authority that must pass before beginning P7-K.
