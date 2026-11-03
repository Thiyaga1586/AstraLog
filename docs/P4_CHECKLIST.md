# P4 — Replicated Data Partitions and Quorum Acks Checklist

Status: **P4 CLOSED — REPLICATED PARTITIONS / QUORUM ACKS QUALIFIED**

## A. Contract freeze

- [x] P4 replication contract written before implementation
- [x] P1 byte-format immutability boundary explicit
- [x] P3 consensus ownership boundary explicit
- [x] deterministic application command encoding fixed
- [x] P3 log entry -> P1 frame mapping fixed
- [x] P4 hard-state/snapshot sidecar semantics fixed
- [x] persistence-batch execution ordering fixed
- [x] internal Raft transport framing/bounds fixed
- [x] leader epoch semantics fixed
- [x] `acks=0/leader/quorum` semantics fixed
- [x] committed high-watermark rule fixed
- [x] catch-up/rebootstrap safety boundary fixed
- [x] static P4 topology boundary vs P5 fixed

## B. Storage adapter

- [x] deterministic Application Command V1 codec
- [x] P3 application entry -> P1 Data frame
- [x] P3 control entry -> P1 Control frame
- [x] durable hard-state sidecar
- [x] durable snapshot sidecar
- [x] P1 suffix truncation API
- [x] rolled-segment suffix truncation
- [x] committed truncation rejected
- [x] indexes rebuilt after truncation
- [x] next Raft/stream positions restored after truncation
- [x] P3 persistence batch maps atomically enough for its durability contract
- [x] recovery reconstructs P3 `RecoveredState`
- [x] hardstate/P1 committed-index mismatch fails loudly

## C. Internal transport

- [x] internal Raft frame codec
- [x] all P3 RaftMessage variants encoded/decoded
- [x] CRC corruption detected
- [x] length/version bounds checked before allocation
- [x] broker/source identity checked
- [x] group identity checked
- [x] persistent broker-pair TCP connection
- [x] multiple partition groups multiplexed
- [x] bounded outbound queues
- [x] reconnect/backoff bounded
- [x] corrupt framing never resynchronized silently

## D. Replicated partition runtime

- [x] exactly one P3 core owner per local replica
- [x] bounded serialized input queue
- [x] persistence effects executed in order
- [x] persistence completion fed back to P3
- [x] outbound messages routed by replica assignment
- [x] Apply updates local committed visibility
- [x] election ticks injected without hidden P3 clock
- [x] RF=1
- [x] RF=3
- [x] RF=5
- [x] control traffic protected from producer starvation

## E. Producer semantics

- [x] deterministic base stream offset assigned before proposal
- [x] bounded unique proposal IDs
- [x] non-leader produce rejected/redirected
- [x] stale explicit leader epoch rejected
- [x] `acks=0` no-response behavior retained
- [x] `acks=leader` waits for leader local durable append only
- [x] `acks=quorum` waits for durable Raft commit
- [x] leadership loss before ack condition never returns success
- [x] pending proposal count/bytes bounded
- [x] lost quorum response ambiguity documented as non-idempotent until P7

## F. Fetch/ListOffsets visibility

- [x] P3 commit persists through P1 `mark_committed`
- [x] committed Raft index maps to stream high watermark
- [x] control entries do not advance stream high watermark
- [x] default Fetch returns committed data only
- [x] Fetch high watermark is committed stream watermark
- [x] ListOffsets latest uses committed visibility
- [x] follower/uncommitted local tails remain invisible

## G. Redirect and epoch fencing

- [x] leader epoch is Raft term
- [x] NotLeader includes best-known leader when available
- [x] StaleEpoch response includes current leader/term when available
- [x] old-term node cannot accept produce
- [x] metadata reports current static P4 replica set/leader term

## H. Catch-up / rebootstrap

- [x] ordinary AppendEntries catch-up
- [x] conflict repair after leader change
- [x] lagging follower rejoin
- [x] snapshot mode selected below retained floor
- [x] staged rebootstrap package validation
- [x] interrupted rebootstrap preserves old authoritative generation
- [x] post-snapshot ordinary suffix catch-up

## I. Failure qualification

- [x] leader dies before local persistence
- [x] leader dies after local persist before quorum
- [x] leader dies after quorum commit before response
- [x] duplicate retry after lost response
- [x] minority network partition
- [x] follower disk failure
- [x] leader disk failure
- [x] check-quorum loss with pending producers
- [x] stale internal term after reconnect
- [x] committed-only fetch during lag
- [x] RF 1/3/5 election/commit campaigns
- [x] conflicting uncommitted suffix truncation
- [x] snapshot/rebootstrap interruption

## J. Exit invariants

- [x] quorum-ack success survives tested minority failures
- [x] consumers never observe uncommitted data by default
- [x] leader-only acknowledged loss is demonstrated/documented
- [x] no split-brain committed histories
- [x] durable follower acknowledgement preserved through adapter/runtime
- [x] stale leaders fenced from client writes
- [x] recovery requires no guessed durable state

## K. Scope audit

- [x] no P1 format byte changes
- [x] no P3 consensus semantic changes
- [x] no P5 controller policy pulled forward
- [x] no P6/P7/P8/P9/P10 features pulled forward

## L. Qualification and closure

- [x] `cargo fmt --all -- --check`
- [x] workspace Clippy `-D warnings`
- [x] full workspace tests
- [x] P4 deterministic failure suite green
- [x] P1 golden compatibility green
- [x] P3 120-test safety regression green
- [x] P4 closure report written
- [x] clean working tree
- [x] P4 closure commit created
