# AstraLog P8-H Benchmark Harness and Deterministic Baseline Tooling Qualification Report

Date: 2026-10-07

Status: **P8-H CLOSURE CANDIDATE — EXACT-HEAD PASS NOTE REQUIRED FOR AUTHORITY**

## Scope

P8-H closes **P8-127 through P8-143 only**.

The authoritative predecessor is P8-G at:

    98bf895f35263a932c6a8a4145047ddd9134d5ed

That exact SHA carries the P8-G PASS Git note with the complete 618/618 exact-HEAD qualification.

P8-I final compatibility/closure work and every P9+ behavior remain disabled.

P8-H qualifies the repository-owned benchmark **machinery**. It deliberately does **not** publish a throughput, latency or comparative performance claim during normal qualification, and normal `cargo test` never executes the long benchmark. Any future number must come from the committed release-only runner and its retained raw result bundle.

## Benchmark crate and release boundary

P8-H introduces the workspace crate `astralog-benchmark` with a release-only long-run path. The benchmark entry point rejects debug builds before workload execution. The optimized benchmark binary itself is built during qualification, but the long benchmark workload is not run as part of the ordinary test gate.

The CLI supports:

- `run <manifest> <raw-out> <report-out>`;
- `report <raw> <report-out>`;
- `compare <baseline-raw> <contender-raw> <comparison-out>`.

The `run` path writes the validated canonical raw bundle before it generates the human report.

## Canonical manifest and provenance

`BenchmarkManifest` uses the canonical `ASTRALOG_BENCH_MANIFEST_V1` text envelope and fails closed on missing, duplicate, unknown or malformed fields.

The manifest binds workload and environment declarations including:

- bootstrap endpoint and topic prefix;
- partition count and multi-producer concurrency;
- warmup, measurement duration and repeat count;
- backlog size;
- client batching record/byte/linger limits;
- request timeout and Fetch record/byte limits;
- a hard cap on retained latency samples;
- deterministic seed;
- host kernel, CPU model, RAM, disk, filesystem and network topology;
- the external bounded system-metrics snapshot command.

Runtime provenance additionally records exact Git SHA, `rustc` version, release profile, Rust flags, target OS/architecture, host OS and available core count. The runner rejects a dirty Git worktree before capturing the SHA, so a published raw bundle cannot claim a commit that does not identify the source actually being benchmarked.

## Frozen workload coverage

The deterministic matrix generator covers every frozen level and every required critical pair for:

- record size: 100 B, 1 KiB, 10 KiB;
- keyed and unkeyed records;
- RF1 and RF3;
- leader and quorum acknowledgement;
- None, LZ4 and Zstd committed topic compression policy;
- single and configured multi-producer concurrency;
- producer-only, backlog-consumer and simultaneous produce/consume modes.

The matrix is generated deterministically rather than using the complete Cartesian product. A validator rejects unsupported levels, duplicate cells or any matrix that misses the frozen coverage obligations.

Workload data and partition choices come from a repository-owned deterministic seeded generator. Keyed cells always contain keys; unkeyed cells do not. Each V1 topic is created with the benchmark cell's RF/compression/batch policy and immediately re-described; a policy or TopicId mismatch aborts the run rather than silently substituting configuration.

## Warmup and measurement isolation

Warmup and measurement accounting are mechanically separate.

Producer-only and simultaneous modes use a separate warmup topic, delete it, then create the measured topic. Backlog-consumer mode pre-fills backlog before timing and uses a non-retaining warmup accumulator before the measured Fetch pass.

The warmup accumulator cannot add events, bytes, latency samples, errors or throttles to the reported measurement. Focused tests exercise this boundary directly.

## Raw metrics and bounded memory

Each measured run retains bounded raw producer and/or Fetch latency samples. The manifest hard-caps each latency stream, and attempting to exceed that cap fails the run instead of growing memory without bound.

The raw run also records:

- produced and fetched event counts;
- logical bytes;
- measurement elapsed time;
- start/end CPU time;
- RSS;
- disk read/write counters;
- network receive/transmit counters;
- fsync count and cumulative fsync time;
- replication lag;
- error and throttle counts.

The external metrics command is invoked directly rather than through a shell and its stdout is limited to 64 KiB before parsing. Every frozen metrics field is required and cumulative counters may not regress across one measured run.

`RawBundle` uses the canonical `ASTRALOG_BENCH_RAW_V1` envelope and requires a complete manifest, provenance block, exact workload matrix and every configured repeat before it can be serialized as valid benchmark evidence.

## Deterministic report and comparisons

The human Markdown report is generated only from a validated raw bundle. It derives events/sec, MiB/sec, producer and Fetch p50/p95/p99/p999/max, CPU/RSS, disk/network, fsync, lag and error/throttle fields mechanically from raw data. Percentiles use repository-owned nearest-rank calculation; no percentile is hand-entered.

Round-tripping a canonical raw bundle and regenerating its report produces byte-identical Markdown.

The comparison generator refuses bundles with fewer than two repeats per workload and refuses mismatched workload matrices. Accepted comparisons use complete repeated raw runs and deterministic integer arithmetic.

## Client compatibility helpers

The existing Rust client gains only two P8-H helper surfaces needed by the harness:

- CreateTopic V1 with explicit replication factor and `TopicPerformanceConfigV1`;
- DescribeTopic V1 so the committed performance policy can be verified before measurement.

No Wire V1 body, protocol registry, Storage V1 byte, Raft rule, producer/group authority or broker runtime semantic is changed by P8-H.

## Qualification evidence

Focused benchmark-tooling qualification on the final source candidate:

- benchmark crate tests: **19/19 PASS**;
- benchmark package Clippy with `-D warnings`: **PASS**;
- optimized `astralog-benchmark` binary build: **PASS**;
- `cargo fmt --all -- --check`: **PASS**;
- `git diff --check`: **PASS**.

The focused tests cover manifest canonicality/fail-closed validation, deterministic matrix coverage, raw bundle round trips, raw-derived percentiles, complete system metrics, release gating, clean-SHA provenance, deterministic keyed/unkeyed generation, warmup isolation, latency memory bounds, deterministic report regeneration, repeated-run comparison gating, bounded metrics-command parsing and the short offline harness smoke test.

Complete serialized all-target workspace qualification on the final source candidate:

- benchmark tooling: **19/19 PASS**;
- broker units: **37/37 PASS**;
- P2 network: **9/9 PASS**;
- P4 RF3: **1/1 PASS**;
- P5/P8 dynamic: **5/5 PASS**;
- P6 RF3/group/offset/fencing/churn/public integration: **9/9 PASS**, serialized;
- P7 runtime: **2/2 PASS**;
- P7 failover: **12/12 PASS**, serialized;
- client: **24/24 PASS**;
- common: **3/3 PASS**;
- controller: **113/113 PASS**;
- groups: **55/55 PASS**;
- protocol: **45/45 PASS**;
- original P2/P6/P7 protocol goldens: **11/11 + 6/6 + 4/4 PASS**;
- Raft: **128/128 PASS**;
- replication: **101/101 PASS**;
- storage: **53/53 PASS**.

Total:

    637/637 PASS

No long performance workload or benchmark number is part of that test suite.

Frozen compatibility checks already established on the final source candidate:

- `crates/astralog-protocol` unchanged from P8-G authority;
- `crates/astralog-storage` unchanged from P8-G authority;
- original P2/P6/P7 protocol golden-vector sources unchanged;
- the workspace manifest/lockfile delta is limited to registering the new benchmark crate and its existing workspace dependencies.

## Closure boundary

P8-127 through P8-143 are mechanically implemented on this pre-commit closure candidate.

P8-H becomes authoritative only after:

1. the final source/document set passes the complete serialized **637/637** workspace and mechanical gates;
2. this exact bounded P8-H set is committed as one checkpoint;
3. the committed clean HEAD repeats the focused, release-build, compatibility, complete-workspace and mechanical/frozen gates; and
4. that exact SHA receives the P8-H PASS Git note.

Only then may **P8-I — beginning at P8-144 compatibility audit and exact P8 closure** start. P9 and later remain disabled.
