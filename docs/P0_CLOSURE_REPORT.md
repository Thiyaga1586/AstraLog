# P0 Closure Report

Phase: **P0 — Architecture & Correctness Constitution**

Result: **PASS — architecture frozen as Project Bible v1.0**

## Closure review performed

The P0 documents were cross-checked specifically for:

- control-plane/data-plane responsibility overlap
- unsafe leader-election assumptions
- ambiguous ack semantics
- page-cache versus durable-write confusion
- compaction rewriting live Raft history
- tombstone resurrection across replica failover
- consumer-group claims stronger than offset fencing can provide
- work-queue lease state surviving coordinator failover
- unbounded queue/gap state
- ambiguous DLQ transfer semantics
- stale broker/producer/member/lease authorities
- rolling-upgrade format hazards
- benchmark claims without provenance

## Issues discovered and resolved before freeze

### 1. Compaction versus Raft history

Resolved by splitting the conceptual storage lifecycle into:

- live Raft replication suffix above a snapshot floor
- retained stream history below that floor

Compaction cannot rewrite the live suffix.

### 2. Tombstone resurrection

Resolved with per-replica `clean_compacted_offset`, a Raft-committed monotonic `compaction_safe_offset`, and mandatory rebootstrap for a replica that falls behind the safe boundary.

### 3. Consumer-group processing fence

Clarified that group epochs fence coordinator/offset state, not arbitrary side effects already executed by a stale application.

### 4. Work-queue authority

Resolved by adding replicated `__astralog_queue_state` coordinator shards. Queue leases and acks are authoritative only after quorum commit.

### 5. Work-queue boundedness

Resolved by a bounded delivery window ahead of the durable cursor.

### 6. Visibility clocks

Clarified monotonic runtime timers plus persisted failover deadlines. Clock skew may cause early/late redelivery but not silent successful completion.

### 7. DLQ ambiguity

Removed an over-strong duplicate-suppression claim. AstraLog permits duplicate DLQ copies under ambiguous failover, carries a deterministic transfer ID, and prefers duplication over source-message loss.

### 8. Idempotent producer wording

Scoped the guarantee to “no second committed logical copy for a supported producer sequence range,” avoiding an invalid claim about transient uncommitted physical entries.

### 9. Multi-Raft transport explosion

Locked in broker-pair connection multiplexing. AstraLog does not create one TCP connection or OS thread per partition.

## P0 exit gate

- [x] all P0 documents cross-reviewed for contradictions
- [x] macro architecture frozen
- [x] no production implementation code created
- [x] ADR-001 through ADR-016 accepted
- [x] requested features mapped to phases
- [x] correctness invariants named
- [x] failure model documented
- [x] benchmark honesty policy documented
- [x] P1 can begin without macro redesign

## Frozen macro decisions

1. Rust
2. own storage engine
3. custom binary TCP protocol
4. dedicated controller Raft quorum
5. one Raft group per partition
6. direct client-to-leader routing
7. RF 1/3/5 with quorum commit semantics
8. cooperative stream consumer groups
9. producer ID/epoch/sequence idempotency
10. stream + SQS-like work-queue subscription models
11. coordinated compaction safety
12. learner-first replica movement
13. cluster feature-level rolling upgrades
14. bounded-memory/backpressure doctrine
15. formal/deterministic/chaos verification
16. generated-only benchmark claims

P0 is complete. Changes to these decisions now require the architecture-freeze ADR process.
