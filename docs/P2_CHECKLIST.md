# P2 — Binary Protocol, Single Broker, Client, Partitioning Checklist

Status: **CLOSED / FROZEN**

This checklist is the mechanical closure gate for P2.

## A. Frozen contract

- [x] `PROTOCOL_V1.md` written before networking implementation
- [x] Wire V1 connection preface fixed
- [x] Wire V1 frame/header/CRC grammar fixed
- [x] primitive/tagged-field encoding fixed
- [x] API key registry fixed
- [x] P2 API V0 schemas fixed
- [x] public error numbers and retry classes fixed
- [x] keyed Murmur3 partition function fixed
- [x] null-key sticky behavior fixed
- [x] protocol compatibility golden files committed

## B. Protocol crate

- [x] `astralog-protocol` crate created with no upward dependencies
- [x] Hello V1 encode/decode
- [x] frame encode/decode
- [x] CRC32C verification
- [x] primitive codecs
- [x] tagged-field codec
- [x] ApiVersions V0 codec
- [x] Metadata V0 codec
- [x] CreateTopic V0 codec
- [x] DeleteTopic V0 codec
- [x] DescribeTopic V0 codec
- [x] Produce V0 codec
- [x] Fetch V0 codec
- [x] ListOffsets V0 codec
- [x] public error enum/retry classification

## C. Protocol adversarial proof

- [x] fragmented frame input at every boundary
- [x] multiple frames in one input buffer
- [x] length below minimum rejected
- [x] length above hard/negotiated maximum rejected before allocation
- [x] impossible body/header/tag lengths rejected
- [x] duplicate/unsorted tags rejected
- [x] bad CRC rejected
- [x] unknown frame kind/flags rejected
- [x] duplicate active correlation ID rejected
- [x] arbitrary-byte decoder fuzz/property test does not panic
- [x] slow/incomplete frame bounded by assembly timeout

## D. Partitioning

- [x] Murmur3 implementation is protocol-owned, not runtime-hash-owned
- [x] all section 22.2 compatibility vectors pass
- [x] keyed partition compatibility golden vectors committed
- [x] sticky null-key partitioner implemented
- [x] sticky tests use injected deterministic RNG
- [x] metadata invalidation rotates unavailable sticky partition

## E. Single-broker network runtime

- [x] bounded TCP connection state machine
- [x] Hello -> ApiVersions -> Ready state enforcement
- [x] bounded receive buffer
- [x] bounded in-flight requests
- [x] bounded pending-response bytes
- [x] correlation multiplexing
- [x] out-of-order response support
- [x] request deadlines
- [x] no frame resynchronization guessing
- [x] graceful connection shutdown

## F. Single-broker metadata/admin

- [x] durable stable cluster ID
- [x] durable broker identity required by P2
- [x] monotonic metadata epoch
- [x] CreateTopic
- [x] DeleteTopic
- [x] DescribeTopic
- [x] Metadata
- [x] RF other than 1 rejected in P2
- [x] topic-name constraints enforced

## G. Storage integration

- [x] Produce appends through P1 storage API
- [x] `acks=leader` waits for qualified local durability
- [x] RF=1 `acks=quorum` maps to one-replica durable commit
- [x] `acks=0` produces no response and claims no delivery guarantee
- [x] Fetch defaults to committed visibility only
- [x] Fetch long polling implemented
- [x] ListOffsets earliest/latest/timestamp implemented
- [x] no P1 disk-format mutation

## H. Rust client

- [x] bootstrap connection + Hello
- [x] ApiVersions negotiation
- [x] Metadata cache
- [x] direct broker routing abstraction
- [x] bounded in-flight requests
- [x] request timeout
- [x] typed retry policy from public error taxonomy
- [x] Produce lost-response ambiguity documented
- [x] key partitioner
- [x] sticky null-key partitioner
- [x] metadata refresh on NOT_LEADER/STALE_EPOCH path

## I. Integration proof

- [x] protocol golden bytes green
- [x] fragmented TCP frame tests green
- [x] concurrent multiplexing test green
- [x] malformed/oversized frame tests green
- [x] slowloris test green
- [x] key partition compatibility vectors green
- [x] leader metadata refresh path green in single-node mode
- [x] single-broker produce/fetch durable restart green
- [x] committed-only fetch invariant green
- [x] bounded-memory assertions green

## J. Qualification and closure

- [x] `cargo fmt --all -- --check`
- [x] `cargo clippy --workspace --all-targets --all-features -- -D warnings`
- [x] `cargo test --workspace --all-features`
- [x] no P3+ implementation pulled forward
- [x] P2 closure report written
- [x] Wire V1 compatibility fixtures committed
- [x] clean working tree
- [x] P2 phase commit created
