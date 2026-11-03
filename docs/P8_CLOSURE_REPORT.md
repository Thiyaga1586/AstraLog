# AstraLog P8 Closure Report

Date: 2026-10-07

Status: **P8-I PRE-COMMIT QUALIFIED — CLOSURE COMMIT AND EXACT-HEAD PASS NOTE REQUIRED FOR AUTHORITY**

## 1. Scope and authority lineage

P8 implements only **Batching, Compression, Flow Control and the Engineering Performance Baseline** defined by `docs/P8_PERFORMANCE_V1.md` and actions **P8-001 through P8-160**.

The last authoritative predecessor is P8-H at:

    58f529fe7ff266eb9b658c4f752af4677b6f310b

That SHA carries the exact-head P8-H Git qualification note and authoritatively closes P8-127..143.

P8-I begins with compatibility/audit work only. It does not authorize P9 retention/compaction, P10 work queues, P11 production security/operations/upgrades, or P12 release-grade chaos/benchmark publication.

## 2. P8-I benchmark-readiness repair

The first real P8 engineering-baseline attempt exposed one benchmark-harness readiness gap: CreateTopic V1 could return committed metadata before every newly created partition had converged to a concrete P3 leader in broker Metadata. The benchmark runner could therefore enter warmup against a topic that was committed but not yet routable.

The bounded repair is commit:

    c7e7a1b7aee964e8ace2dd45be294d87c31f1265

The repair changes only `crates/astralog-benchmark/src/runner.rs`:

- after CreateTopic/DescribeTopic policy identity is verified, the runner refreshes Metadata until the exact TopicId has the configured partition count and RF;
- every partition must report `PublicError::None`, a concrete non-sentinel leader broker, a concrete leader epoch and the exact replica count;
- the wait is bounded by the manifest request timeout and fails rather than silently measuring an unready topic;
- a pure readiness test covers missing leader/epoch and replica-count mismatch.

No Wire V1 body, Storage V1 byte, controller authority rule, P3/P4 semantics, P6/P7 authority or broker production path changes in this repair.

The repaired benchmark package is **20/20 PASS**, package Clippy `-D warnings`, formatting and diff hygiene are green, and the real engineering-baseline run completed successfully on that exact clean source SHA.

## 3. Compatibility audit — P1 through P7

### P1 Storage V1

Relative to authoritative P7 closure `d0d0e85`, the Storage golden directory has exactly two additions:

    data_p8_lz4_v1.hex
    data_p8_zstd_v1.hex

Every historical Storage golden is source-identical. P8 compression remains inside Storage V1's already-versioned data-frame representation, preserves None-compression historical bytes and leaves control frames None-only.

### P2 Wire V1

The original P2, P6 and P7 protocol golden-vector source files are unchanged from P7 closure. P8 adds topic V1 metadata/configuration without changing legacy Produce V0/V1 or Fetch V0 bodies.

### P3 / P4

P8-E intentionally extends only the previously re-frozen persistence scheduling frontier; P8-F adds bounded replication windows and Fetch implementation tuning. The final compatibility gate must keep the complete Raft suite and frozen RF3 quorum/redirect/term/committed-read integration green.

### P5

Topic performance metadata remains controller-committed and feature-level-4 gated. Placement, broker lifecycle, movement, preferred-leader and failure-domain authority remain the P5 state machine's responsibility. P8 does not create an alternate metadata authority.

### P6

Consumer-group and offset coordination retain the exact P6 group/owner fencing, committed internal-topic authority and coordinator routing. P8 pressure controls classify those control paths separately from saturated data admission.

### P7

Buffered/idempotent production preserves P7 ProducerId/Epoch/sequence and exact-retry semantics. Compression changes physical representation only; committed producer reconstruction and retry digests remain logical-record based. P8 does not weaken lost-response, fencing or failover rules.

## 4. I-26 bounded-resource audit

Every P8-owned growth surface has a finite count and/or byte bound with one-over tests or bounded failure behavior:

- client per-partition batch accumulators — record, byte, partition-cardinality, linger and global buffered-work limits;
- persistence-frontier V2 — queued token count, staged bytes and group-delay bounds;
- replication — per-follower message/byte windows plus bounded replication-effect history slicing;
- Fetch — request record/byte ceilings and bounded committed page scanning;
- broker admission — global data count/byte ceilings, strict low/high watermarks and finite quota bucket/cardinality bounds;
- compression/decompression — stored/logical length validation and decompression ceiling before proportional allocation;
- benchmark tooling — finite matrix, repeats, request bounds and hard per-stream retained latency-sample cap.

The focused P8-named stress campaign is **87/87 PASS** on the P8-I source candidate.

## 5. I-27 control-progress audit

P8 preserves control progress under admitted data pressure rather than treating performance as permission to starve consensus/control work. Existing deterministic/real tests cover:

- persistence group staging while Raft control ticks continue;
- replication-data saturation with reserved control capacity;
- slow follower sustained writes plus catch-up/restart;
- broker slow-consumer / producer-flood pressure while Metadata control still completes;
- combined producer/follower/consumer pressure while replication control progresses;
- P6 coordinator/session control remaining independently bounded.

These proofs are part of the **87/87** focused P8 campaign and the final complete-workspace gate.

## 6. I-28 feature-compatibility audit

Irreversible P8 topic-performance behavior remains gated by the existing committed active cluster feature level:

- controller topic-performance metadata requires feature level 4;
- dynamic brokers advertise/use topic V1 policy only after committed P8 readiness;
- compression policy is controller-derived and cannot be activated by client-local configuration alone;
- legacy topics/snapshots derive the frozen default None policy;
- unsupported/mismatched feature ranges fail before activation.

The P8 controller/topic focused tests and real dynamic-broker P8 integration remain part of the focused and complete-workspace qualification.

## 7. I-29 engineering-baseline provenance

P8-I generated and retains:

- `docs/baselines/P8_ENGINEERING_BASELINE.raw` — canonical `ASTRALOG_BENCH_RAW_V1` evidence;
- `docs/baselines/P8_ENGINEERING_BASELINE.md` — deterministic Markdown generated only from that raw bundle;
- `docs/baselines/P8_WINDOWS_PROCESS_METRICS.ps1` — byte-preserved copy of the Windows metrics helper used by the qualification driver.

The raw bundle records exact source SHA:

    c7e7a1b7aee964e8ace2dd45be294d87c31f1265

and records Rust `1.98.1`, release profile, OS/kernel, CPU/core count, RAM, disk/filesystem, loopback topology, workload configuration, warmup/measurement durations, repeats and the exact metrics command. It contains the deterministic **9-cell** matrix and **18 complete runs** required by the configured two repeats.

The committed `astralog-benchmark report` command reparsed the retained raw bundle and regenerated a byte-identical Markdown report. The generated report SHA-256 is:

    DE6697F56442DC678173572A704F1829BB7F78756C56D868D175516EF7B4DCB6

The raw bundle SHA-256 is:

    E09381482D902DA2121E8D731FF4BA724A106198BDAAB091FEDA7CAB5C317E11

No throughput or latency value is copied into handwritten status/report prose. Performance values live only in the generated raw/report artifacts.

### Baseline limitations

This is deliberately a **P8 engineering baseline**, not a P12 release benchmark:

- all controllers and brokers run on one Windows host over loopback;
- the manifest uses one partition per benchmark topic while still covering RF1/RF3 and the frozen workload-factor pairs;
- the Windows helper records process CPU/RSS and process I/O transfer counters; process-I/O bytes are a conservative proxy in the raw disk-byte slots;
- network byte counters come from the host network stack, not a broker-only NIC counter;
- AstraLog V1 exposes no public fsync-latency or replication-lag metrics endpoint, so those required raw fields are explicitly zero/unavailable rather than fabricated;
- short warmup/measurement windows are suitable for engineering regression evidence only.

Those limitations are why P12 still owns release-grade multi-host, chaos and publication claims.

## 8. P9-P12 scope audit

The P8 source/dependency delta from P7 closure contains no implementation of:

- time/size retention, physical segment deletion, tombstone grace or log compaction policy;
- queue receive/ack/nack, visibility leases, retry scheduling or DLQ transfer;
- TLS/mTLS, authentication, authenticated-principal mapping or ACL authorization;
- graceful drain, continuous leader balancing or rolling upgrade;
- release-grade chaos/leader-kill benchmark publication.

The dependency audit introduces the `astralog-benchmark` workspace crate and the frozen LZ4/Zstd codec stack only. No later-phase dependency family is present.

## 9. Final deterministic P8 stress gate

The command:

    cargo test --workspace p8 -- --test-threads=1

passes **87/87** matching tests across benchmark tooling, broker/load shaping/Fetch, real dynamic topic V1 activation, client batching/throttle, controller V3 topic policy, protocol V1 topic codec, Raft persistence-frontier/window logic, replication group-commit/pipelining/pressure/recovery and Storage compression/index/restart/truncation.

The real engineering baseline then runs the same committed release-only benchmark implementation against a real three-controller/three-broker dynamic cluster and completes the retained repeated raw bundle.

## 10. Pre-commit full-workspace closure gate

The documentation/generated-evidence closure candidate passes the complete serialized workspace:

- benchmark tooling: **20/20 PASS**;
- broker units: **37/37 PASS**;
- P2 network: **9/9 PASS**;
- P4 RF3: **1/1 PASS**;
- P5/P8 dynamic integration: **5/5 PASS**;
- P6 RF3 group/offset/fencing/churn/public integration: **9/9 PASS**, serialized;
- P7 real RF3 failover: **12/12 PASS**, serialized;
- P7 runtime: **2/2 PASS**;
- client: **24/24 PASS**;
- common: **3/3 PASS**;
- controller: **113/113 PASS**;
- groups: **55/55 PASS**;
- protocol: **45/45 PASS**;
- original P2/P6/P7 protocol goldens: **11/11 + 6/6 + 4/4 PASS**;
- Raft: **128/128 PASS**;
- replication: **101/101 PASS**;
- Storage: **53/53 PASS**.

Total:

    638/638 PASS

The same closure candidate also passes:

- workspace Clippy `--all-targets --all-features -D warnings`;
- workspace doc tests;
- optimized `astralog-benchmark` release build;
- `cargo fmt --all -- --check`;
- `git diff --check`;
- historical Storage-golden audit: only the two P8 compressed fixtures are additions relative to P7 closure;
- original P2/P6/P7 golden-vector sources unchanged from P7 closure;
- focused P8 stress **87/87 PASS**;
- benchmark package/readiness **20/20 PASS**;
- raw-bundle parse and byte-identical report regeneration.

No long benchmark is part of the ordinary 638-test workspace gate. The engineering baseline is retained separately as provenance-bound generated evidence described above.

## 11. Exact closure procedure

P8 becomes authoritative only when all of the following are true:

1. P8-001..160 are checked in the committed checklist with evidence;
2. the final pre-commit tree passes the complete compatibility/stress/workspace/mechanical gate;
3. one P8 closure commit records this report, checklist/status updates and retained baseline artifacts on top of the qualified P8-I readiness repair;
4. that exact clean closure SHA repeats the focused P8, full-workspace, Clippy/doc/fmt/diff and frozen compatibility gates; and
5. that exact SHA receives a Git qualification note declaring P8 authoritatively closed.

Only after that Git note exists may P9 planning/implementation begin. P10-P12 remain disabled behind their own future phase gates.
