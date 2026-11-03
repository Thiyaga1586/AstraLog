# AstraLog: runnable SDE interview demo

**Snapshot:** 2026-11-03 · **benchmark source:** `d7e32d3` · **scope:** portfolio demonstration, **not** a production release.

## What is actually running

AstraLog is a Rust distributed event log with segmented checksummed storage,
Raft-based partition replication and a Raft metadata-controller quorum.
The demo uses the real broker server and client, not mocked responses:

```text
                          bootstrap / metadata
  Rust client  <------------------------------------>  Controller Raft group (3 nodes)
      |
      | partition-leader TCP routing (produce / committed fetch)
      v
  Partition leader (broker A)
      |      \
      v       v
  voter B   voter C           (RF3 quorum replication)
      |
  each voter: durable segment log / index / manifest
```

The automated RF3 launcher starts **three controller nodes and three broker
runtimes within one integration-test process**, each communicating over actual
loopback TCP ports, with separate on-disk temporary directories. The RF1 case
starts one standalone broker. This is reproducible on one laptop but is **not**
a multi-machine deployment.

The RF3 setup explicitly commits controller feature level 4 before using the
P8 buffered producer; batching cannot be used against the standalone RF1
broker, which correctly does not advertise that feature.

## Run the complete demo with one command

PowerShell, from the project root; Rust 1.98.1 is pinned by
`rust-toolchain.toml`. Local Cargo dependencies must already be cached for
`--offline`.

```powershell
cd D:\Workspace\AstraLog
powershell -NoProfile -ExecutionPolicy Bypass -File scripts/run-interview-demo.ps1
```

This starts the release-mode RF1/RF3 benchmark, runs all three targeted
reliability tests, generates a summary, and saves a raw CSV, session log, and
environment metadata in a timestamped temporary output directory. For a
quicker functional check only, run the same script with `-Fast` (debug mode,
not valid for publishing performance results).

To control the benchmark settings manually instead:

```powershell
$env:CARGO_BUILD_JOBS = '2'
$env:CARGO_INCREMENTAL = '0'
$env:ASTRALOG_INTERVIEW_REPEATS = '2'
$env:ASTRALOG_INTERVIEW_DURATION_MS = '2500'
$env:ASTRALOG_INTERVIEW_CSV = Join-Path $env:TEMP 'astralog_interview_results.csv'
cargo test --release -p astralog-broker --test interview_benchmark --offline --locked -- --nocapture --test-threads=1
powershell -NoProfile -ExecutionPolicy Bypass -File scripts/summarize-interview-benchmark.ps1 -RawCsv $env:ASTRALOG_INTERVIEW_CSV -Output (Join-Path $env:TEMP 'astralog_interview_summary.md') -Mode release
```

For a **fast functional smoke check** instead of publishable performance
numbers, set `ASTRALOG_INTERVIEW_REPEATS='1'`,
`ASTRALOG_INTERVIEW_DURATION_MS='600'`, and remove `--release`. Do **not**
present debug-build figures as optimized throughput.

Each test actually creates its own topics, warms up, writes concurrently,
checks committed Fetch against acknowledged records, and shuts down all
started brokers. The **post-audit** harness additionally checks exact key,
payload identity, uniqueness and contiguous offsets; the original `d7e32d3`
baseline predates that stronger Fetch oracle. Non-idempotent produce errors
abort measurement; the harness does **not** retry ambiguous writes.

## Measured performance on 2026-11-03

The release execution used `d7e32d3`, with two repeats for each of six
configurations (**12 measured workloads**); both RF1 and RF3 integration
tests passed, every generated record was fetched, and producer errors were
zero.

<details>
<summary>Raw evidence and generated report</summary>

- [Raw 12-row CSV](benchmarks/interview_20261103/release_raw.csv)
- [Generated full percentile table](benchmarks/interview_20261103/SUMMARY.md)
- [Release test output](benchmarks/interview_20261103/release_test.log)
- [Deterministic summary generator](../scripts/summarize-interview-benchmark.ps1)

</details>

The key comparisons (medians of the two per-repeat measurements) are:

| Configuration | Producer throughput | Producer p95 | Producer p99 |
|---|---:|---:|---:|
| RF1 / leader ack / individual | 92.74 records/s | 52.36 ms | 61.11 ms |
| RF1 / quorum ack / individual | 81.99 records/s | 82.51 ms | 102.34 ms |
| RF3 / leader ack / individual | 14.48 records/s | 362.07 ms | 377.72 ms |
| RF3 / leader ack / buffered | 54.20 records/s | 132.48 ms | 152.54 ms |
| RF3 / quorum ack / individual | 14.34 records/s | 376.78 ms | 400.14 ms |
| RF3 / quorum ack / buffered | **47.09 records/s** | **103.84 ms** | 106.10 ms |

For the RF3 quorum workload, buffered producer calls achieved **3.28×**
the unbuffered producer throughput in these measurements. This is a narrow
local observation, not a general Kafka/Pulsar comparison, an absolute SLA, or
evidence that leader acknowledgement provides the same durability as quorum.

**Benchmark contract:** 1,024-byte record value; four concurrent producers;
eight excluded warmup writes per topic; 2.5-second target producer window per
repeat; distinct topic for each configuration; default **no compression**;
one-record committed Fetch requests measured **after** writes complete.
Throughput counts successful logical value bytes only (not protocol,
replication or storage overhead). The generated summary includes **p50, p95
and p99 for both Produce and Fetch**, and MiB/s.

**Host:** Windows 11 Home Single Language, build 26200; Intel Core
i7-13620H (10 cores/16 logical threads); 16.85 GB physical RAM;
Rust 1.98.1; release profile with thin LTO; C: NTFS temporary data,
loopback transport. CPU, fsync and lag measurements are **not** collected by
this interview runner. Two short repeats on a single laptop have substantial
uncertainty.

## Three focused reliability checks

```powershell
cargo test --offline --locked -p astralog-broker --test p2_network single_broker_round_trip_survives_restart -- --nocapture --test-threads=1
cargo test --offline --locked -p astralog-broker --test p7_failover p7_rf3_leader_death_after_quorum_before_response_deduplicates_on_successor -- --nocapture --test-threads=1
cargo test --offline --locked -p astralog-broker --test p7_failover p7_rf3_committed_retry_survives_explicit_leadership_transfer -- --nocapture --test-threads=1
```

All three **focused** checks passed on 2026-11-03 (1/1 each):

1. A single-broker committed record survives a disk-backed broker restart.
2. A lost idempotent Produce response followed by RF3 leader death is
   deduplicated on the successor.
3. An RF3 committed retry remains correct across explicit leader transfer.

Retained logs:
[restart and leader death](benchmarks/interview_20261103/reliability_restart_failover.log),
[leader transfer](benchmarks/interview_20261103/reliability_transfer.log).

**Important unresolved limitation:** an earlier, unchanged-source full
serialized workspace repeat at `563ede6` had P7 failover **8/12** because
four cases reported broker-health startup failures. These three new focused
passes do not erase that regression. No fresh complete workspace qualification
or P9-G exact-HEAD PASS note was claimed for the interview snapshot.

**Later audit:** The local post-snapshot remediation added a standalone static
RF3 test with three *separate broker operating-system processes*: it kills
the elected leader after a quorum commit, verifies the exact record on a
successor, and successfully appends a new quorum record. A newer P9-G full-run
failure also exposed a V1 obsolete-snapshot file-swap race; a minimal
committed-prefix guard and exact-byte regression test address that case.
These later fixes must not be attributed to the original `d7e32d3` benchmark
commit. See [the current audit matrix](AUDIT_CLOSURE_MATRIX.md) for test
evidence and unqualified Linux/power-loss/security scenarios.

## Explain this project in 60 seconds

> I built AstraLog to learn what a messaging platform does beneath its API.
> It has a Raft quorum for cluster metadata and a Raft group per data
> partition. Clients discover the partition leader, produce to an append-only
> checksummed log, and read committed data. I implemented recovery and
> idempotent producer state to handle failure and lost responses without
> duplicate commits. I tested real RF3 leader failure and transfer, then
> built a reproducible release-mode benchmark that measured batching against
> unbuffered writes. On my local four-producer workload, RF3 quorum throughput
> rose from about 14 to 47 1-KiB records per second with buffering. I can
> explain the latency and durability tradeoffs, the precise test conditions,
> and the unresolved full-suite flake.

## Resume bullets (use only the measured, qualified wording)

- Built **AstraLog**, a Rust distributed event log with Raft-partition
  replication, a three-node metadata controller quorum, checksummed segmented
  storage, leader elections and quorum acknowledgements.
- Implemented producer batching and idempotent retry/deduplication behavior;
  verified disk restart, committed-write leader failover and leadership
  transfer with targeted end-to-end tests.
- Authored a reproducible 1-KiB, four-producer benchmark measuring throughput
  and p50/p95/p99 latency; observed a **3.28× RF3 quorum throughput increase**
  from buffering (**14.34 → 47.09 records/s**) on a local Windows host.

The goal is an interview artifact that works, has real metrics, and is honest
about its limits. It is **not a claim of fully qualified P9, P10–P12, a
multi-machine deployment, or a production-ready broker**.
