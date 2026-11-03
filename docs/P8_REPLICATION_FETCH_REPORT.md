# AstraLog P8-F Replication Pipelining and Fetch Tuning Qualification Report

Date: 2026-10-07

Status: **P8-F CLOSURE CANDIDATE — EXACT-HEAD PASS NOTE REQUIRED FOR AUTHORITY**

## Scope

P8-F closes **P8-091 through P8-107 only**. The execution checklist is canonical: **P8-108 is the first P8-G action** and remains disabled.

The authoritative predecessor is P8-E at `e9291916f37339d41c08cb8d0bc925da060a3861`, whose exact SHA carries the P8-E PASS note.

No P8-G quota/watermark behavior, P8-H benchmark harness, or later-phase behavior is activated here.

## Replication pipelining

Each leader partition now has finite per-follower scheduling state bounded independently by message count and logical bytes. Non-empty AppendEntries data is admitted only while both limits permit it; heartbeats and other Raft control traffic do not consume the data window.

The P8-F runtime does not create follower-specific unbounded retry history. Raft constructs only a bounded suffix for each replication effect, and the runtime further chunks that effect to the configured catch-up message ceiling. The ordinary P3 progress structure remains the sole replication-authority state: speculative P8-F windows never grant match/commit authority.

The standalone P3 default preserves historical behavior. P8-F opt-in occurs when `ReplicatedPartition` supplies its finite replication byte ceiling through `RaftLimits`.

## Reset and fencing boundaries

Speculative per-follower scheduling state is discarded on:

- leader-term change;
- newer inbound peer connection generation;
- newer broker incarnation;
- failed AppendEntries response requiring backtracking;
- snapshot / rebootstrap boundary.

Older broker incarnations and older inbound connection generations are rejected before their Raft response can affect the current window. The transport writer itself retains its already-bounded queue across reconnect; P8-F does not reset accounting behind a queued retry and therefore does not undercount outstanding data.

Only P3 AppendEntries responses advance authoritative peer progress. P8-F changes scheduling, not quorum mathematics, commit rules, or acknowledgement authority.

## Control progress and catch-up

The runtime reserves outbound queue capacity for control traffic independently from non-empty AppendEntries data. Focused saturation qualification proves a full data window cannot consume all heartbeat/control capacity.

A sustained RF3 campaign isolates a follower while quorum writes continue, checks the leader's per-follower count/byte bounds throughout the backlog, restarts/rejoins the follower, and proves convergence to the committed prefix without gaps or duplicate logical records.

## Fetch tuning

Fetch V0 schema and bytes remain frozen. P8-F changes only how committed data is scanned and assembled.

Storage exposes a bounded committed-record scan that walks frames incrementally and stops materializing record payloads once the caller's record/byte ceiling is reached. High-watermark calculation remains based on the committed Raft boundary and is not hidden merely because the selected response is full.

Broker response assembly consumes owned `DataRecord` values and moves key/value/header payloads into the wire record instead of cloning them. A reference-versus-optimized qualification produces byte-identical Fetch V0 bodies.

Long-poll notification uses a monotonic data epoch plus a coalesced wake flag. Bursty publishes can collapse redundant wake tasks while a waiter still observes every authoritative epoch advance. Existing `min_bytes`, `max_wait_ms`, leader fencing, and read-committed semantics are unchanged.

## Optional plaintext zero-copy decision

P8-106 is closed by an explicit **disabled / no demonstrated benefit** decision.

The safe P8-F path already removes record-level response clones. A deeper plaintext zero-copy implementation would require immutable segment-buffer lifetime ownership to extend through protocol body encoding and CRC-framed response ownership. That would add lifetime/ownership complexity at a frozen wire boundary, and this phase has no benchmark evidence showing the additional path is needed.

Therefore P8-F does **not** introduce unsafe code, borrowed response frames, a new protocol body, or a new Storage representation. This decision may be revisited only with measured evidence and the same frozen compatibility constraints.

## Qualification evidence

Focused P8-F proofs:

- Raft bounded replication-effect suffix: **1/1 PASS**;
- replication windows/reset/control/catch-up: **4/4 PASS**;
- broker fetch budget/wakeup/move assembly: **3/3 PASS**;
- focused total: **8/8 PASS**.

Complete package suites:

- broker: **26/26 PASS**;
- client: **22/22 PASS**;
- common: **3/3 PASS**;
- controller: **113/113 PASS**;
- groups: **55/55 PASS**;
- protocol: **45/45 PASS**;
- Raft: **128/128 PASS**;
- replication: **100/100 PASS**;
- storage: **53/53 PASS**.

Real/frozen integration and compatibility:

- P2 network: **9/9 PASS**;
- P4 RF3: **1/1 PASS**;
- P5/P8 dynamic: **5/5 PASS**;
- P6 RF3/group/offset/fencing/churn/public integration: **9/9 PASS**, serialized;
- P7 runtime: **2/2 PASS**;
- P7 failover: **12/12 PASS**, serialized;
- original P2/P6/P7 protocol goldens: **11/11 + 6/6 + 4/4 PASS**.

Complete serialized all-target workspace:

    604/604 PASS

Mechanical/frozen boundary:

- `cargo fmt --all -- --check`: **PASS**;
- workspace Clippy with `-D warnings`: **PASS**;
- workspace doc tests: **PASS**;
- `git diff --check`: **PASS**;
- Cargo manifests/lockfile unchanged from P8-E authority;
- original P2/P6/P7 golden-vector sources unchanged;
- Storage golden fixture surface unchanged.

One attempted broker-focused rebuild hit Windows linker `LNK1104` because an older `p6_routing` test executable still held the output file open. It was an environment/process-lock failure, not a test or source failure. After the stale process cleared, the unchanged tree passed the broker-focused gate and the complete serialized workspace.

## Closure boundary

P8-091 through P8-107 are mechanically satisfied on this pre-commit closure candidate.

P8-F becomes authoritative only after:

1. this exact bounded source/document set is committed as one P8-F checkpoint;
2. the committed clean HEAD repeats the focused, compatibility, complete **604/604**, and mechanical/frozen-byte gates; and
3. that exact SHA receives the P8-F PASS Git note.

Only then may **P8-G — beginning at P8-108** start. P8-H and later remain disabled.
