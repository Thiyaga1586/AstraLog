# AstraLog Architecture Decision Log

These are P0 decisions. Detailed ADR files may later expand them, but implementation must treat them as frozen unless the architecture-freeze process is followed.

| ADR | Decision | Status | Rationale |
|---|---|---|---|
| ADR-001 | Rust stable is the implementation language | Accepted | Systems performance, memory safety, explicit concurrency/resource ownership |
| ADR-002 | Separate controller Raft quorum from partition data Raft groups | Accepted | Keeps metadata strongly consistent while avoiding controller on every data request |
| ADR-003 | Multi-Raft: one Raft group per partition | Accepted | Precise quorum commit/election safety and independent partition failure domains |
| ADR-004 | No mandatory centralized API gateway | Accepted | Any broker bootstraps; clients route directly to partition leaders, avoiding a permanent bottleneck |
| ADR-005 | Own segmented append-only storage engine | Accepted | Storage/WAL/recovery is a core learning and correctness target |
| ADR-006 | Custom versioned TCP binary protocol | Accepted | Exposes networking/protocol design while keeping payload overhead controlled |
| ADR-007 | Cooperative incremental consumer groups | Accepted | Avoids full stop-the-world group reshuffles and minimizes movement |
| ADR-008 | Two subscription models: stream and work queue | Accepted | Captures Kafka-like streaming plus SQS-like visibility/retry semantics over one durable substrate |
| ADR-009 | CRC32C for record-batch corruption detection | Accepted | Strong, hardware-friendly checksum used by mature log/storage systems |
| ADR-010 | Quorum ack means Raft majority commit; leader ack is explicitly weaker | Accepted | Makes CAP/durability trade-off visible and testable |
| ADR-011 | Internal offsets/group state are normal replicated internal topics | Accepted | Reuses the platform's own durability machinery and avoids another database |
| ADR-012 | Idempotency uses producer ID + epoch + per-partition sequence | Accepted | Deterministic duplicate detection for retry ambiguity |
| ADR-013 | Rolled segments immutable; compaction copy-on-write | Accepted | Simplifies readers, recovery, and crash safety |
| ADR-014 | Learners before voter promotion during replica movement | Accepted | An empty new node must not immediately weaken quorum availability |
| ADR-015 | Cluster feature level gates irreversible new behavior | Accepted | Enables safe rolling upgrades and explicit compatibility |
| ADR-016 | Benchmark claims must be generated, never manually invented | Accepted | Preserves credibility and reproducibility |
| ADR-017 | Producer epochs are controller-issued and partition-enforced | Accepted | Keeps epoch issuance globally monotonic without putting the controller on every Produce; durable fencing becomes partition-local committed authority |
| ADR-018 | P8-E uses a bounded ordered multi-token persistence queue with a separate staged logical frontier and one monotonic durability frontier | Accepted | Enables true same-partition fsync amortization without speculative durability; only append-compatible work may queue and completion remains ordered/proven |
| ADR-019 | P9 compaction uses sparse Storage V2 only for immutable retained history below the committed snapshot floor; active append/live Raft suffix remain V1 and retained-history rebootstrap is versioned | Accepted | Batched V1 cannot remove an internal record without renumbering later offsets, while P4 rebootstrap V1 requires a prefix that physical retention intentionally removes |

## Change procedure

After a decision's freeze phase closes, modification requires a new superseding ADR that contains:

1. observed problem
2. evidence
3. alternatives
4. correctness impact
5. performance impact
6. wire/disk compatibility impact
7. migration strategy
8. rollback strategy
9. tests/formal proof updates

No decision changes simply because implementation became inconvenient.
