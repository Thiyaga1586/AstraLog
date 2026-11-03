# AstraLog Feature-to-Phase Matrix

This table proves that every requested hard part has an explicit implementation owner.

| Capability | Owning phase | Primary proof/exit evidence |
|---|---|---|
| append-only segment files | P1 | golden disk files + append/read/restart tests |
| write-ahead log durability | P1 | fsync/fdatasync crash tests |
| CRC corruption detection | P1 | bit-flip/truncation fuzz/property suite |
| partitioning by key | P2 | deterministic Murmur3 compatibility vectors |
| custom client/broker protocol | P2 | protocol golden bytes + fuzzing |
| batching | P8 | ordering + throughput regression suite |
| compression | P8 | CRC/decompression/size-bound tests |
| leader election | P3/P4 | deterministic Raft simulation + kill-leader integration |
| replica synchronization | P4/P5 | lag/rejoin/bootstrap/reassignment tests |
| quorum acknowledgement | P4 | acknowledged-message failure oracle |
| cluster metadata consensus | P5 | controller Raft failover tests |
| cluster membership | P5 | incarnation fencing + join/remove tests |
| rack/zone-aware placement | P5 | placement constraint tests |
| consumer offsets | P6 | replicated offset failover tests |
| consumer groups | P6 | churn/crash/coordinator-failover tests |
| cooperative rebalancing | P6 | ownership/revocation state-machine tests |
| idempotent producers | P7 | lost-response/retry/failover oracle |
| duplicate detection | P7 | sequence window and epoch tests |
| backpressure | P8 | overload memory-bound stress tests |
| retention | P9 | retention/lag/restart tests |
| log compaction | P9 | copy-on-write crash matrix |
| coordinated tombstone safety | P9 | offline-replica + leader-failover resurrection test |
| retry queues | P10 | persisted scheduler failover tests |
| visibility timeout | P10 | lease expiry/coordinator failover tests |
| dead-letter queue | P10 | poison-message + ambiguous transfer tests |
| partition rebalancing | P5/P11 | learner promotion + operational rebalance tests |
| graceful node draining | P11 | drain under sustained traffic |
| rolling upgrades | P11 | N/N+1 mixed-version suite |
| authentication/TLS/ACLs | P11 | authorization matrix + rotation tests |
| observability | P11 | metric/log/trace/cardinality audit |
| chaos engineering | P12 | full fault matrix |
| reproducible benchmark | P8 baseline, P12 release | raw data + provenance + generated report |
| zero fake benchmark numbers | all | invariant I-29 |

## Requested benchmark failure scenario

The exact “kill a broker while producing” demonstration belongs to P12, after P4/P7/P11 behavior is complete. Its report must measure rather than assume:

- broker/leader failure detection time
- new leader election time
- producer recovery time
- total successful acknowledgements
- missing quorum-acknowledged records
- duplicate committed logical records in idempotent mode
- steady-state throughput and p50/p95/p99/p999 latency
