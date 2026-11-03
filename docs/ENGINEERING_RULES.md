# AstraLog Engineering Rules

These rules apply from the first line of implementation.

## 1. Correctness gates

- New distributed behavior requires an invariant reference in code review.
- No correctness-critical TODO may be carried silently across phase closure.
- A bug caused by a particular fault schedule receives a deterministic regression seed/test.
- Timeouts improve liveness; they are not used as the proof of safety.

## 2. Dependency rules

- Do not add Kafka, Pulsar, BookKeeper, ZooKeeper, etcd, RabbitMQ, NATS, or a Raft crate.
- General-purpose runtime, crypto/TLS, compression, checksum, CLI, metrics, and testing libraries are allowed.
- Every new dependency must state why standard library/current dependencies are insufficient.
- Core crates cannot depend upward on broker/client binaries.

## 3. Rust rules

- stable toolchain pinned for each release
- `cargo fmt --check`
- `cargo clippy --all-targets --all-features -- -D warnings`
- unit/integration/doc tests green
- unsafe code denied by default; any necessary unsafe requires a focused safety comment and review
- avoid panics on network/disk/user input paths
- errors preserve actionable context without leaking secrets

## 4. Determinism

Core state machines accept injected:

- clock
- RNG
- network transport
- storage adapter/failpoints where practical

This allows deterministic simulation. Production implementations wrap real OS facilities.

## 5. Bounded concurrency

Every channel/queue has:

- capacity
- overflow behavior
- metric
- shutdown behavior

No “temporary” unbounded channel is accepted in the broker.

## 6. Disk rules

- authoritative log bytes are never repaired by guessing
- short writes and fsync errors are handled
- temp-file/rename protocols document crash points
- derived indexes can be deleted and rebuilt
- committed corruption is loud

## 7. Protocol rules

- input lengths validated before allocation
- unknown API version gets explicit response
- feature negotiation precedes use
- frame decoder fuzzed
- error retryability documented
- network inputs never trigger unbounded decompression/allocation

## 8. Observability rules

Every background state machine exposes:

- state
- progress
- error count
- last-success/failure timestamp where useful

Metrics avoid unbounded-cardinality labels.

## 9. Benchmark rules

- no benchmark runs in debug builds
- warmup separated from measurement
- raw data saved
- environment captured
- at least several repeated runs for published comparisons
- throughput never reported without latency and durability configuration

## 10. Phase closure

Each phase ends with:

- tests
- frozen contract/spec where applicable
- closure report
- known-limitations section
- clean working tree for phase-owned files
- baseline performance/regression evidence when relevant
