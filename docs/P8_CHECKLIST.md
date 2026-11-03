# AstraLog P8 Execution Checklist

Status: **P8-I CLOSURE BOUNDARY — AUTHORITY REQUIRES THE EXACT-HEAD P8 PASS GIT NOTE**

This checklist is normative. An item is checked only when its stated proof exists mechanically.
P8 must preserve every frozen P0-P7 contract and P8_PERFORMANCE_V1.md.

## Phase exit invariants

- [x] no batching/compression/performance option changes quorum or leader ack semantics. **Closure proof:** P8-E ack-boundary tests plus frozen P4 RF3 and complete P7 failover/runtime suites are green in the final **638/638** workspace gate.
- [x] per-partition record order remains identical to the accepted logical order. **Closure proof:** client batching order, mixed-codec group-commit recovery, compressed rebootstrap and read-committed Fetch qualification are green in focused P8 **87/87** plus the full workspace gate.
- [x] P7 exact retry/fencing/sequence rules remain unchanged under batching and compression. **Closure proof:** buffered V1 retry preserves exact logical body/range, compressed producer-state reconstruction is identical to None, and P7 RF3 failover is **12/12 PASS** with runtime **2/2 PASS**.
- [x] every accumulator, queue, window, decompression allocation and benchmark buffer is bounded. **Closure proof:** P8-151 audits each P8 growth surface and focused one-over/resource qualification is green.
- [x] data-plane overload cannot starve Raft, heartbeat or health-critical control progress. **Closure proof:** P8-E/F/G control-progress stress plus P6 scheduling/control qualification remain green; focused P8 stress is **87/87 PASS**.
- [x] compressed recovery reconstructs the same logical records and producer state as uncompressed recovery. **Closure proof:** Storage mixed-codec restart/index tests and replication compressed-history producer-state/rebootstrap tests pass in both focused and complete gates.
- [x] every published P8 benchmark figure traces to raw data, config, environment and exact git SHA. **Closure proof:** generated baseline values exist only in the canonical raw/generated report pair bound to source `c7e7a1b7aee964e8ace2dd45be294d87c31f1265`; report regeneration is byte-identical.
- [x] P1-P7 compatibility remains green and no P9-P12 implementation is pulled forward. **Closure proof:** P8-144..155 audits plus the complete **638/638** regression, immutable golden-source checks and source/dependency scope scan are green.

## P8-A — Contract and checklist freeze

- [x] **P8-001** — Write P8_PERFORMANCE_V1.md. **Proof:** complete normative P8 performance/compression contract exists before implementation.
- [x] **P8-002** — Write complete P8_CHECKLIST.md. **Proof:** P8-A through P8-I are decomposed into bounded actions P8-001 through P8-160.
- [x] **P8-003** — Freeze P8 entering boundary. **Proof:** P7 semantic closure authority is d0d0e85 with its exact-HEAD note; qualified test-harness-only base 1384279 is the clean P8-A execution base and carries its own exact-HEAD note.
- [x] **P8-004** — Freeze correctness-over-performance rule. **Proof:** P8 performance is never authority for commit, fencing, ordering, dedupe or visibility.
- [x] **P8-005** — Freeze feature activation. **Proof:** feature key 14 version 1 plus committed cluster feature level 4 gate irreversible P8 writes/metadata.
- [x] **P8-006** — Freeze topic performance policy ownership. **Proof:** compression and batch admission policy are controller-owned, not broker-local failover-sensitive settings.
- [x] **P8-007** — Freeze controller version boundary. **Proof:** ControllerCommand/Snapshot V1/V2 stay immutable; P8 plans additive V3 state.
- [x] **P8-008** — Freeze public compatibility boundary. **Proof:** Produce V0/V1 and Fetch V0 bodies remain unchanged; CreateTopic/DescribeTopic use additive V1 only.
- [x] **P8-009** — Freeze P7 producer compatibility. **Proof:** P8 V1 retains exactly one unresolved idempotent range per producer-partition.
- [x] **P8-010** — Freeze Storage V1 compression semantics. **Proof:** reserved None/LZ4/Zstd byte is activated without changing the 96-byte frame envelope.
- [x] **P8-011** — Freeze decompression safety order. **Proof:** size validation and CRC precede bounded decompression and exact output-length validation.
- [x] **P8-012** — Freeze group-commit safety. **Proof:** P3 persistence ordering and no-success-on-ambiguous-durability remain authoritative.
- [x] **P8-013** — Freeze replication-window safety. **Proof:** pipelining changes scheduling only; P3 acknowledgements alone advance match/commit authority.
- [x] **P8-014** — Freeze overload-control boundary. **Proof:** client/broker/replication bounds, watermark hysteresis and reserved control capacity are explicit.
- [x] **P8-015** — Freeze quota/security boundary. **Proof:** P8 shaping is broker-local anonymous/topic performance control; authenticated policy remains P11.
- [x] **P8-016** — Freeze benchmark constitution. **Proof:** provenance, workload dimensions, metrics and release-build rules are explicit before benchmark code.
- [x] **P8-017** — Freeze later-phase exclusions. **Proof:** P9 retention, P10 queues, P11 security/ops and P12 release-chaos claims remain excluded.
- [x] **P8-018** — Freeze implementation checkpoints. **Proof:** P8-B through P8-I are ordered and no later checkpoint may start before independent qualification.

## P8-B — Controller topic-performance metadata and feature activation

- [x] **P8-019** — Add checked CompressionPolicy V1 enum. **Proof:** only None/LZ4/Zstd are representable and unknown values fail closed.
- [x] **P8-020** — Add TopicPerformancePolicyV1. **Proof:** topic ID, codec and record/batch limits are canonical checked values.
- [x] **P8-021** — Enforce global max-record relationship. **Proof:** topic max record cannot exceed the frozen 8 MiB public/global envelope.
- [x] **P8-022** — Enforce max-batch-record relationship. **Proof:** topic batch count cannot exceed 10,000 records.
- [x] **P8-023** — Enforce decompression-ceiling relationship. **Proof:** topic logical batch bytes cannot exceed active hard decompression bound.
- [x] **P8-024** — Add deterministic default policy for historical topics. **Proof:** V1/V2 controller state maps to None plus global limits without data rewrite.
- [x] **P8-025** — Add ControllerCommandV3 envelope. **Proof:** V1/V2 decoders reject V3 and historical command bytes remain exact.
- [x] **P8-026** — Add atomic CreateTopic-with-performance V3 command. **Proof:** topic and performance policy commit in one controller transition.
- [x] **P8-027** — Add ControllerSnapshotV3 envelope. **Proof:** exact V2 bytes plus sorted policy table and outer CRC round-trip canonically.
- [x] **P8-028** — Decode ControllerSnapshotV1 to default P8 policy state. **Proof:** P5 fields and allocator state remain unchanged.
- [x] **P8-029** — Decode ControllerSnapshotV2 to default P8 policy state. **Proof:** P7 producer sessions remain exact while P8 defaults are derived.
- [x] **P8-030** — Reject malformed/noncanonical V3 policy tables. **Proof:** duplicates, unsorted topics, invalid codecs and invalid bounds fail before state publication.
- [x] **P8-031** — Gate V3 controller mutation at feature level 4. **Proof:** lower levels reject atomically with no metadata epoch change.
- [x] **P8-032** — Extend controller/broker supported feature range to level 4. **Proof:** active broker compatibility checks prevent an unsupported feature bump.
- [x] **P8-033** — Activate feature key 14 version 1 metadata. **Proof:** supported/inactive before level 4, active after committed level 4 and metadata convergence.
- [x] **P8-034** — Add CreateTopic V1 body. **Proof:** V0 bytes stay exact and V1 carries validated P8 policy only.
- [x] **P8-035** — Add DescribeTopic V1 body. **Proof:** V0 bytes stay exact and V1 returns committed policy.
- [x] **P8-036** — Keep AlterTopicConfig unavailable in P8. **Proof:** key 6 dispatch/advertisement remains disabled.
- [x] **P8-037** — Qualify controller V3 restart/snapshot/failover. **Proof:** RF3 controller leadership changes preserve identical topic performance policies.
- [x] **P8-038** — Close P8-B compatibility gate. **Proof:** controller, protocol goldens, P5/P6/P7 integration and full workspace are green before P8-C.

## P8-C — Storage V1 LZ4/Zstd activation and recovery

- [x] **P8-039** — Add pinned LZ4 dependency/profile. **Proof:** exact `lz4_flex=0.14.0` raw-block safe/checked profile is manifest-pinned, lockfile-bound and documented.
- [x] **P8-040** — Add pinned Zstd dependency/profile. **Proof:** exact `zstd=0.14.0`, level-3, no-dictionary single-frame profile is manifest-pinned, lockfile-bound and documented.
- [x] **P8-041** — Implement canonical data-payload compression adapter. **Proof:** compression is applied only after the existing canonical data-record-set encoder produces the same raw payload used by None.
- [x] **P8-042** — Keep control frames uncompressed. **Proof:** the control encoder still emits None and the reader rejects forged compressed control frames.
- [x] **P8-043** — Implement no-gain fallback to None. **Proof:** both codec paths retain compressed bytes only when strictly smaller; deterministic no-gain input falls back to None.
- [x] **P8-044** — Encode stored/uncompressed lengths correctly. **Proof:** focused frame tests compare physical stored length, logical uncompressed length and total frame bytes for LZ4/Zstd.
- [x] **P8-045** — CRC compressed physical bytes before decode. **Proof:** compressed-payload bit corruption returns frame CRC mismatch before decompression.
- [x] **P8-046** — Bound declared uncompressed length before allocation. **Proof:** one-over configured logical ceiling returns `LimitExceeded` before CRC/decompression allocation.
- [x] **P8-047** — Require exact decompressed output length. **Proof:** short/long declarations, truncation and trailing codec data fail closed for both codecs.
- [x] **P8-048** — Reject unknown compression identifier. **Proof:** CRC-valid codec byte 3 is rejected explicitly; no fallback/guessing exists.
- [x] **P8-049** — Freeze immutable LZ4 golden Storage V1 bytes. **Proof:** `data_p8_lz4_v1.hex` binds exact header, raw-block compressed payload and frame CRC.
- [x] **P8-050** — Freeze immutable Zstd golden Storage V1 bytes. **Proof:** `data_p8_zstd_v1.hex` binds exact header, level-3 compressed payload and frame CRC.
- [x] **P8-051** — Preserve existing None golden bytes. **Proof:** every historical P1 noncompressed fixture is tree-identical to P8-B authority `339d87f` and its focused golden tests pass.
- [x] **P8-052** — Recover mixed None/LZ4/Zstd segment. **Proof:** one committed mixed-codec segment restarts with identical Raft/stream offsets and logical record values.
- [x] **P8-053** — Rebuild indexes from compressed frames. **Proof:** rebuilt sparse offset/time/Raft indexes derive the same logical keys from mixed-codec frames.
- [x] **P8-054** — Truncate uncommitted compressed suffix safely. **Proof:** committed truncation is rejected; uncommitted mixed-codec suffix truncates and replacement offsets resume exactly.
- [x] **P8-055** — Rebootstrap compressed partition safely. **Proof:** encoded/decoded rebootstrap preserves None control + LZ4/Zstd data frames, committed logical history, restart recovery and selected suffix codec.
- [x] **P8-056** — Reconstruct P7 producer state from compressed history. **Proof:** LZ4/Zstd history reconstructs byte/logically equal P7 authority, sequence, offsets and canonical retry digests to None history.

## P8-D — Client per-partition accumulator and batching

- [x] **P8-057** — Add bounded producer accumulator config. **Proof:** byte/record/partition/deadline limits validate before runtime start.
- [x] **P8-058** — Add per-topic-partition accumulator map. **Proof:** no batch ever crosses partition identity.
- [x] **P8-059** — Preserve keyed partitioner before accumulation. **Proof:** Murmur3 result is unchanged by batching.
- [x] **P8-060** — Preserve sticky null-key batch lifecycle. **Proof:** sticky partition rotates only after batch closure or invalidation.
- [x] **P8-061** — Close batch on max record count. **Proof:** exact boundary and one-over behavior are tested.
- [x] **P8-062** — Close batch on max logical bytes. **Proof:** encoded logical size is bounded before append to accumulator.
- [x] **P8-063** — Close batch on linger deadline. **Proof:** injected deterministic time proves no ordering change.
- [x] **P8-064** — Implement explicit producer flush. **Proof:** flush drains every currently closed/open valid batch within bounded deadline.
- [x] **P8-065** — Flush or reroute on metadata invalidation. **Proof:** stale leader route never silently changes record partition/order.
- [x] **P8-066** — Define client shutdown drain behavior. **Proof:** bounded close either returns terminal outcomes or explicit timeout.
- [x] **P8-067** — Bound total accumulator bytes. **Proof:** producer faster than network hits bounded wait/error without memory growth.
- [x] **P8-068** — Bound total buffered records. **Proof:** tiny-record pressure cannot bypass byte limits with unbounded element count.
- [x] **P8-069** — Bound active partition accumulators. **Proof:** high-cardinality partition access cannot create unbounded map state.
- [x] **P8-070** — Preserve one unresolved P7 range per partition. **Proof:** second idempotent batch waits until the first is terminal.
- [x] **P8-071** — Assign sequence only when batch closes for send. **Proof:** rejected/buffered records do not consume sequence authority prematurely.
- [x] **P8-072** — Preserve exact logical retry body/digest. **Proof:** retry after response loss uses identical records and sequence range.
- [x] **P8-073** — Close P8-D broker/client integration gate. **Proof:** batched V0/V1 produce and fetch preserve offsets/order and P7 failover semantics.

## P8-E — Persistence group commit

- [x] **P8-074** — Introduce bounded per-partition persistence staging queue. **Proof:** count/byte bounds reject before unbounded allocation.
- [x] **P8-075** — Add maximum group-delay bound. **Proof:** injected monotonic time forces flush at deadline.
- [x] **P8-076** — Preserve PersistenceBatch operation order. **Proof:** model compares grouped execution with sequential reference.
- [x] **P8-077** — Coalesce compatible append flushes only. **Proof:** unsafe operation kinds form hard group barriers.
- [x] **P8-078** — Fence AdvanceCommit behind required append durability. **Proof:** commit cannot outrun durable data.
- [x] **P8-079** — Fence snapshot/install boundaries. **Proof:** group commit never crosses install/rebootstrap transaction unsafely.
- [x] **P8-080** — Fence suffix truncation boundaries. **Proof:** grouped work cannot retain or commit a truncated suffix.
- [x] **P8-081** — Complete each original persistence token independently. **Proof:** one physical sync maps to exact eligible token set.
- [x] **P8-082** — Fail all unproven tokens after ambiguous sync error. **Proof:** no success is fabricated after injected fsync ambiguity.
- [x] **P8-083** — Preserve leader-ack durability semantics. **Proof:** local durable success boundary is unchanged by grouping.
- [x] **P8-084** — Preserve quorum-ack semantics. **Proof:** client success still waits for P3 majority commit.
- [x] **P8-085** — Prove group-delay does not starve Raft control. **Proof:** heartbeats/elections progress under sustained append pressure.
- [x] **P8-086** — Crash after staged append before grouped sync. **Proof:** restart exposes only durability justified by existing ack contract.
- [x] **P8-087** — Crash after grouped sync before completion delivery. **Proof:** replay/lost response follows existing P4/P7 ambiguity rules.
- [x] **P8-088** — Qualify mixed compressed group commit. **Proof:** None/LZ4/Zstd batches preserve logical order and durable offsets.
- [x] **P8-089** — Measure fsync-amortization counters without claiming benchmark results. **Proof:** deterministic counters distinguish logical tokens and physical syncs.
- [x] **P8-090** — Close P8-E full regression gate. **Proof:** storage/Raft/replication/P7 suites and workspace are green before P8-F.

## P8-F — Replication pipelining and fetch tuning

- [x] **P8-091** — Add per-follower in-flight message window. **Proof:** configured count bound is enforced.
- [x] **P8-092** — Add per-follower in-flight byte window. **Proof:** configured byte bound is enforced independently of count.
- [x] **P8-093** — Add bounded persisted-history catch-up window. **Proof:** slow follower catch-up does not retain unbounded leader memory.
- [x] **P8-094** — Reset windows on leader-term change. **Proof:** old-term queued state cannot grant new-term replication authority.
- [x] **P8-095** — Reset windows on connection replacement. **Proof:** stale transport acknowledgements cannot advance current window state.
- [x] **P8-096** — Fence broker-incarnation changes. **Proof:** old-incarnation replication traffic is rejected.
- [x] **P8-097** — Reset at snapshot/rebootstrap boundary. **Proof:** window state cannot straddle incompatible log generations.
- [x] **P8-098** — Reserve heartbeat/control transport capacity. **Proof:** saturated data windows still permit Raft control progress.
- [x] **P8-099** — Slow-follower sustained-write stress. **Proof:** leader memory remains within configured windows and quorum continues.
- [x] **P8-100** — Follower reconnect/catch-up stress. **Proof:** pipelined follower converges without gaps/duplicates.
- [x] **P8-101** — Preserve Fetch V0 bytes. **Proof:** original P2 Fetch golden remains exact.
- [x] **P8-102** — Keep committed-read visibility unchanged. **Proof:** fetch tuning never returns uncommitted tail.
- [x] **P8-103** — Bound fetch read-ahead by request/config bytes. **Proof:** large backlog cannot allocate beyond requested ceiling.
- [x] **P8-104** — Coalesce long-poll wakeups safely. **Proof:** min_bytes/max_wait semantics remain unchanged.
- [x] **P8-105** — Reduce response-assembly copies without schema change. **Proof:** optimized and reference Fetch V0 bodies are byte-identical.
- [x] **P8-106** — Evaluate optional copy-minimized plaintext path. **Proof:** either safe path qualifies or explicit no-benefit/disabled decision is recorded.
- [x] **P8-107** — Close P8-F RF3 regression gate. **Proof:** replication/fetch tuning passes leader failover, rejoin and committed-read suites.

## P8-G — Bounded backpressure, watermarks and quotas

- [x] **P8-108** — Add global broker data-admission count budget. **Proof:** data work rejects/waits before unbounded task growth.
- [x] **P8-109** — Add global broker data-admission byte budget. **Proof:** large requests cannot bypass count bound.
- [x] **P8-110** — Reserve independent control-plane capacity. **Proof:** data saturation does not consume Raft/heartbeat/admin reserve.
- [x] **P8-111** — Add broker memory low watermark. **Proof:** configuration validates finite positive threshold.
- [x] **P8-112** — Add broker memory high watermark. **Proof:** high is strictly greater than low and within configured budget.
- [x] **P8-113** — Enter pressure mode at high watermark. **Proof:** new data admission throttles/stops deterministically.
- [x] **P8-114** — Exit pressure mode only below low watermark. **Proof:** hysteresis prevents rapid oscillation.
- [x] **P8-115** — Preserve connection frame/in-flight/response bounds. **Proof:** existing P2 overload tests remain green.
- [x] **P8-116** — Add broker-local anonymous token bucket. **Proof:** monotonic refill and burst bounds are deterministic.
- [x] **P8-117** — Add broker-local topic token bucket. **Proof:** one topic can be throttled without unbounded per-topic state.
- [x] **P8-118** — Check quota before mutation. **Proof:** throttled Produce/Create mutation leaves authoritative state unchanged.
- [x] **P8-119** — Emit nonzero throttle_ms for response-bearing throttled work. **Proof:** client observes minimum retry delay.
- [x] **P8-120** — Keep request throttle_ms zero. **Proof:** protocol request validation remains unchanged.
- [x] **P8-121** — Define acks=0 overload behavior. **Proof:** no-response Produce remains bounded through admission/TCP flow control without false delivery claim.
- [x] **P8-122** — Make client honor throttle_ms. **Proof:** automatic retry/additional load waits at least the advertised delay.
- [x] **P8-123** — Stress producer faster than disk. **Proof:** accumulator/broker/group-commit bounds engage without OOM.
- [x] **P8-124** — Stress slow consumer with large backlog. **Proof:** fetch/response budgets remain finite.
- [x] **P8-125** — Stress simultaneous producer/follower/consumer pressure. **Proof:** all configured resource ceilings remain respected.
- [x] **P8-126** — Close P8-G control-starvation gate. **Proof:** Raft, heartbeats and health-critical work progress under saturated data pressure.

## P8-H — Benchmark harness, baseline and deterministic stress

- [x] **P8-127** — Add release-only benchmark runner. **Proof:** runner refuses debug profile.
- [x] **P8-128** — Add canonical benchmark manifest schema. **Proof:** all required workload/config fields validate before execution.
- [x] **P8-129** — Capture exact git/toolchain/build provenance. **Proof:** raw result cannot be written without provenance block.
- [x] **P8-130** — Capture host OS/CPU/RAM/disk/network provenance. **Proof:** required environment fields are present or result is invalid.
- [x] **P8-131** — Separate warmup from measurement. **Proof:** warmup samples cannot enter reported percentiles/throughput.
- [x] **P8-132** — Record raw latency samples/histograms. **Proof:** p50/p95/p99/p999/max are generated from raw data.
- [x] **P8-133** — Record throughput and system metrics. **Proof:** events/MiB, CPU, RSS, disk, fsync, network, lag and throttle/error metrics are captured.
- [x] **P8-134** — Build deterministic workload coverage matrix. **Proof:** every frozen factor level and critical pair is present.
- [x] **P8-135** — Add keyed/unkeyed workload generation. **Proof:** deterministic seeds reproduce record/partition choices.
- [x] **P8-136** — Add RF1/RF3 and leader/quorum workload coverage. **Proof:** unsupported combinations are rejected, not silently substituted.
- [x] **P8-137** — Add None/LZ4/Zstd workload coverage. **Proof:** committed topic policy matches each benchmark cell.
- [x] **P8-138** — Add single/multi-producer coverage. **Proof:** concurrency is explicit provenance.
- [x] **P8-139** — Add producer-only, backlog-consumer and simultaneous modes. **Proof:** each mode has deterministic start/stop boundaries.
- [x] **P8-140** — Generate raw machine-readable result bundle. **Proof:** report is reproducible from raw bundle alone.
- [x] **P8-141** — Generate deterministic human baseline report. **Proof:** identical raw bundle regenerates byte-identical report.
- [x] **P8-142** — Require repeated runs for comparisons. **Proof:** comparison generator refuses insufficient repeats.
- [x] **P8-143** — Add short benchmark-harness smoke test. **Proof:** normal workspace qualification tests tooling without running long benchmarks.

## P8-I — Compatibility audit and exact closure

- [x] **P8-144** — Requalify frozen P1 None-compression bytes/goldens. **Proof:** relative to P7 closure `d0d0e85`, the Storage golden directory changes only by adding `data_p8_lz4_v1.hex` and `data_p8_zstd_v1.hex`; every historical golden source remains identical, and final Storage qualification re-runs the exact None/index/meta fixtures.
- [x] **P8-145** — Requalify original P2 wire goldens. **Proof:** original P2/P6/P7 protocol golden source files are byte-identical to P7 closure; the final gate re-runs **11/11 + 6/6 + 4/4** while P8 topic V1 additions remain separate.
- [x] **P8-146** — Requalify P3 Raft semantics. **Proof:** P8's V2 persistence-frontier/window changes preserve the frozen election/log/commit/snapshot authority; the complete final Raft suite remains part of the closure gate.
- [x] **P8-147** — Requalify P4 quorum/committed-read semantics. **Proof:** the frozen real RF3 quorum/redirect/term/committed-read integration remains a required final closure test with P8 group commit, pipelining and Fetch tuning enabled.
- [x] **P8-148** — Requalify P5 controller/placement semantics. **Proof:** P8 V3 performance metadata remains feature-gated controller state only; placement/movement/lifecycle authority is unchanged, and controller + real P5/P8 dynamic integration remain required final tests.
- [x] **P8-149** — Requalify P6 group/offset semantics. **Proof:** P8 pressure controls reserve control capacity and do not replace group/offset authority; serialized P6 RF3 plus the deterministic groups suite remain required final tests.
- [x] **P8-150** — Requalify P7 idempotent semantics. **Proof:** batching/compression preserve ProducerId/Epoch/sequence and exact logical retry identity; P7 runtime + real RF3 failover remain required final tests and compressed-history producer reconstruction is included in the P8 stress gate.
- [x] **P8-151** — Audit I-26 bounded resources. **Proof:** client accumulators, V2 persistence queue, replication windows/effect slice, Fetch pages/responses, broker data admission/watermarks/quota buckets, decompression output and benchmark latency retention all have explicit finite count/byte/time bounds with one-over/fail-closed tests; focused P8 stress is **87/87 PASS**.
- [x] **P8-152** — Audit I-27 control progress. **Proof:** focused tests preserve Raft ticks through grouped staging, reserve replication control capacity, preserve Metadata under broker flood/slow consumer and preserve RF3 control/catch-up under combined producer/follower/consumer pressure; focused P8 stress is **87/87 PASS**.
- [x] **P8-153** — Audit I-28 feature compatibility. **Proof:** controller V3 policy, broker topic V1 activation and compression remain committed-feature-level-4 gated; legacy snapshots derive None and unsupported ranges fail closed. Focused controller/topic tests plus real dynamic P8 activation are green.
- [x] **P8-154** — Audit I-29 benchmark provenance. **Proof:** retained `docs/baselines/P8_ENGINEERING_BASELINE.raw` is canonical raw evidence bound to clean source `c7e7a1b7aee964e8ace2dd45be294d87c31f1265`, Rust 1.98.1 release and complete host/config provenance; the committed report command regenerates byte-identical Markdown with SHA-256 `DE6697F56442DC678173572A704F1829BB7F78756C56D868D175516EF7B4DCB6`. Handwritten prose publishes no throughput/latency value.
- [x] **P8-155** — Audit no P9-P12 scope pull-forward. **Proof:** added-source scan finds no retention/compaction/tombstone/queue-lease-DLQ/TLS-auth-ACL/drain-upgrade/release-chaos implementation, and dependency review adds only `astralog-benchmark`, frozen LZ4/Zstd and their build/transitive support.
- [x] **P8-156** — Run final deterministic P8 stress matrix. **Proof:** `cargo test --workspace p8 -- --test-threads=1` is **87/87 PASS**, covering compression, batching, group commit, replication windows/catch-up, Fetch tuning, overload/watermarks/quotas, feature gating, raw/report tooling and the P8-I topic-readiness repair.
- [x] **P8-157** — Run final full workspace regression. **Proof:** the closure tree passes benchmark **20/20**, broker **37/37**, P2 **9/9**, P4 **1/1**, P5/P8 dynamic **5/5**, P6 **9/9** serialized, P7 failover/runtime **12/12 + 2/2**, client **24/24**, common **3/3**, controller **113/113**, groups **55/55**, protocol **45/45**, P2/P6/P7 goldens **11/11 + 6/6 + 4/4**, Raft **128/128**, replication **101/101** and Storage **53/53** for complete serialized workspace **638/638 PASS**. Workspace Clippy `--all-targets --all-features -D warnings`, doc tests, release benchmark build, fmt and diff hygiene also pass.
- [x] **P8-158** — Generate final P8 engineering baseline. **Proof:** committed release tooling produced retained canonical raw evidence plus a provenance-bound deterministic report across the frozen 9-cell matrix with two complete repeats; reparsing/regeneration is byte-identical. The generated artifact alone contains throughput/latency values and is explicitly an engineering baseline, not a P12 release claim.
- [x] **P8-159** — Write P8_CLOSURE_REPORT.md and sole P8 closure commit. **Proof:** `docs/P8_CLOSURE_REPORT.md`, generated baseline/raw evidence, metrics-helper copy, this fully checked checklist and canonical status updates form the one bounded P8 closure set on top of the independently qualified P8-H authority and P8-I readiness repair.
- [x] **P8-160** — Requalify exact P8 closure HEAD. **Proof:** by the phase closure rule, the closure commit is not authoritative until its exact clean SHA repeats focused P8/compatibility/full-workspace **638/638**, release build, immutable/source audits, fmt, Clippy and doc tests, then receives the P8 PASS Git qualification note. That post-commit note is the mechanical evidence that makes this checked action authoritative without creating another moving closure commit.
