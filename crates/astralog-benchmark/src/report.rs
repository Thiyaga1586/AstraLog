use std::collections::{BTreeMap, BTreeSet};

use crate::{BenchmarkError, RawBundle, Result, WorkloadCell, percentile_ns};

pub fn generate_report_markdown(bundle: &RawBundle) -> Result<String> {
    bundle.validate()?;
    let mut runs = bundle.runs.iter().collect::<Vec<_>>();
    runs.sort_by_key(|run| (run.cell.id(), run.repeat));

    let mut out = String::new();
    out.push_str("# AstraLog P8 Engineering Baseline\n\n");
    out.push_str("This report is generated deterministically from the retained raw P8 bundle.\n\n");
    out.push_str("## Provenance\n\n");
    out.push_str(&format!("- Git SHA: `{}`\n", bundle.provenance.git_sha));
    out.push_str(&format!("- Rust: `{}`\n", bundle.provenance.rustc_version));
    out.push_str(&format!(
        "- Build profile: `{}`\n",
        bundle.provenance.build_profile
    ));
    out.push_str(&format!(
        "- Target: `{}/{}`\n",
        bundle.provenance.target_os, bundle.provenance.target_arch
    ));
    out.push_str(&format!(
        "- Host OS/kernel: `{}` / `{}`\n",
        bundle.provenance.host_os, bundle.provenance.host_kernel
    ));
    out.push_str(&format!(
        "- CPU / cores: `{}` / {}\n",
        bundle.provenance.host_cpu_model, bundle.provenance.host_core_count
    ));
    out.push_str(&format!(
        "- RAM bytes: {}\n",
        bundle.provenance.host_ram_bytes
    ));
    out.push_str(&format!(
        "- Disk/filesystem: `{}` / `{}`\n",
        bundle.provenance.host_disk, bundle.provenance.host_filesystem
    ));
    out.push_str(&format!(
        "- Network topology: `{}`\n",
        bundle.provenance.network_topology
    ));
    out.push_str(&format!(
        "- Warmup / measurement: {} ms / {} ms\n",
        bundle.manifest.warmup_ms, bundle.manifest.measurement_ms
    ));
    out.push_str(&format!("- Repeats: {}\n", bundle.manifest.repeats));
    out.push_str(&format!(
        "- Batch max records/bytes/linger: {} / {} / {} ms\n",
        bundle.manifest.batch_max_records,
        bundle.manifest.batch_max_bytes,
        bundle.manifest.batch_linger_ms
    ));
    out.push_str(&format!(
        "- Max retained latency samples per stream: {}\n\n",
        bundle.manifest.max_latency_samples
    ));

    out.push_str("## Runs\n\n");
    out.push_str("| cell | repeat | events/s | MiB/s | prod p50 | p95 | p99 | p999 | max | fetch p50 | p95 | p99 | p999 | max | CPU ns | RSS B | disk R/W B | net R/W B | fsync count/avg ns | lag | errors | throttles |\n");
    out.push_str("| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |\n");
    for run in runs {
        let processed_events = run.produced_events.saturating_add(run.fetched_events);
        let events_per_sec = per_second_milli(processed_events, run.measurement_elapsed_ns);
        let mib_per_sec = mib_per_second_milli(run.logical_bytes, run.measurement_elapsed_ns);
        let cpu = run
            .end_metrics
            .cpu_nanos
            .saturating_sub(run.start_metrics.cpu_nanos);
        let disk_read = run
            .end_metrics
            .disk_read_bytes
            .saturating_sub(run.start_metrics.disk_read_bytes);
        let disk_write = run
            .end_metrics
            .disk_write_bytes
            .saturating_sub(run.start_metrics.disk_write_bytes);
        let network_rx = run
            .end_metrics
            .network_rx_bytes
            .saturating_sub(run.start_metrics.network_rx_bytes);
        let network_tx = run
            .end_metrics
            .network_tx_bytes
            .saturating_sub(run.start_metrics.network_tx_bytes);
        let fsync_count = run
            .end_metrics
            .fsync_count
            .saturating_sub(run.start_metrics.fsync_count);
        let fsync_ns = run
            .end_metrics
            .fsync_total_ns
            .saturating_sub(run.start_metrics.fsync_total_ns);
        let fsync_avg = fsync_ns.checked_div(fsync_count).unwrap_or(0);
        out.push_str(&format!(
            "| `{}` | {} | {} | {} | {} | {} | {} | {} | {} | {} | {} | {} | {} | {} | {} | {} | {}/{} | {}/{} | {}/{} | {} | {} | {} |\n",
            run.cell.id(),
            run.repeat,
            format_milli(events_per_sec),
            format_milli(mib_per_sec),
            latency(&run.producer_latency_ns, 50, 100),
            latency(&run.producer_latency_ns, 95, 100),
            latency(&run.producer_latency_ns, 99, 100),
            latency(&run.producer_latency_ns, 999, 1000),
            run.producer_latency_ns.iter().copied().max().unwrap_or(0),
            latency(&run.fetch_latency_ns, 50, 100),
            latency(&run.fetch_latency_ns, 95, 100),
            latency(&run.fetch_latency_ns, 99, 100),
            latency(&run.fetch_latency_ns, 999, 1000),
            run.fetch_latency_ns.iter().copied().max().unwrap_or(0),
            cpu,
            run.end_metrics.rss_bytes,
            disk_read,
            disk_write,
            network_rx,
            network_tx,
            fsync_count,
            fsync_avg,
            run.end_metrics.replication_lag,
            run.error_count,
            run.throttle_count,
        ));
    }
    Ok(out)
}

pub fn generate_comparison_markdown(baseline: &RawBundle, contender: &RawBundle) -> Result<String> {
    baseline.validate()?;
    contender.validate()?;
    if baseline.manifest.repeats < 2 || contender.manifest.repeats < 2 {
        return Err(BenchmarkError::Invalid(
            "published benchmark comparisons require at least two repeats per workload",
        ));
    }
    let baseline_ids = baseline
        .matrix
        .iter()
        .map(WorkloadCell::id)
        .collect::<BTreeSet<_>>();
    let contender_ids = contender
        .matrix
        .iter()
        .map(WorkloadCell::id)
        .collect::<BTreeSet<_>>();
    if baseline_ids != contender_ids {
        return Err(BenchmarkError::Invalid(
            "comparison bundles must contain the same workload matrix",
        ));
    }

    let left = aggregate_events_per_second(baseline);
    let right = aggregate_events_per_second(contender);
    let mut out = String::from("# AstraLog P8 Baseline Comparison\n\n");
    out.push_str("Comparison values are deterministic means of complete repeated raw runs.\n\n");
    out.push_str("| cell | baseline events/s | contender events/s | delta % |\n");
    out.push_str("| --- | ---: | ---: | ---: |\n");
    for id in baseline_ids {
        let baseline_value = left[&id];
        let contender_value = right[&id];
        let delta_basis_points = if baseline_value == 0 {
            0_i128
        } else {
            ((i128::from(contender_value) - i128::from(baseline_value)) * 10_000)
                / i128::from(baseline_value)
        };
        out.push_str(&format!(
            "| `{id}` | {} | {} | {} |\n",
            format_milli(baseline_value),
            format_milli(contender_value),
            format_basis_points(delta_basis_points),
        ));
    }
    Ok(out)
}

fn aggregate_events_per_second(bundle: &RawBundle) -> BTreeMap<String, u64> {
    let mut values = BTreeMap::<String, (u128, u64)>::new();
    for run in &bundle.runs {
        let processed_events = run.produced_events.saturating_add(run.fetched_events);
        let rate = per_second_milli(processed_events, run.measurement_elapsed_ns);
        let entry = values.entry(run.cell.id()).or_insert((0, 0));
        entry.0 = entry.0.saturating_add(u128::from(rate));
        entry.1 = entry.1.saturating_add(1);
    }
    values
        .into_iter()
        .map(|(id, (sum, count))| {
            let mean = if count == 0 {
                0
            } else {
                sum / u128::from(count)
            };
            (id, u64::try_from(mean).unwrap_or(u64::MAX))
        })
        .collect()
}

fn latency(samples: &[u64], numerator: u64, denominator: u64) -> u64 {
    percentile_ns(samples, numerator, denominator).unwrap_or(0)
}

fn per_second_milli(count: u64, elapsed_ns: u64) -> u64 {
    if elapsed_ns == 0 {
        return 0;
    }
    let value = u128::from(count).saturating_mul(1_000_000_000_000_u128) / u128::from(elapsed_ns);
    u64::try_from(value).unwrap_or(u64::MAX)
}

fn mib_per_second_milli(bytes: u64, elapsed_ns: u64) -> u64 {
    if elapsed_ns == 0 {
        return 0;
    }
    let numerator = u128::from(bytes).saturating_mul(1_000_000_000_000_u128);
    let denominator = u128::from(elapsed_ns).saturating_mul(1024 * 1024);
    u64::try_from(numerator / denominator).unwrap_or(u64::MAX)
}

fn format_milli(value: u64) -> String {
    format!("{}.{:03}", value / 1000, value % 1000)
}

fn format_basis_points(value: i128) -> String {
    let sign = if value < 0 { "-" } else { "" };
    let absolute = value.unsigned_abs();
    format!("{sign}{}.{:02}%", absolute / 100, absolute % 100)
}

#[cfg(test)]
mod tests {
    use std::path::PathBuf;

    use crate::{
        BenchmarkManifest, BenchmarkProvenance, BenchmarkRun, CompressionSpec, RawBundle,
        SystemMetricsSnapshot, WorkloadMode, coverage_matrix,
    };

    use super::*;

    fn bundle(repeats: u32, scale: u64) -> RawBundle {
        let manifest = BenchmarkManifest {
            benchmark_id: "report".into(),
            repository_root: PathBuf::from("."),
            bootstrap_endpoint: "127.0.0.1:9092".into(),
            topic_prefix: "p8report".into(),
            partition_count: 3,
            multi_producer_count: 2,
            warmup_ms: 1,
            measurement_ms: 10,
            repeats,
            backlog_records: 10,
            batch_max_records: 16,
            batch_max_bytes: 1024 * 1024,
            batch_linger_ms: 1,
            request_timeout_ms: 1000,
            fetch_max_bytes: 1024,
            fetch_max_records: 10,
            max_latency_samples: 1000,
            seed: 7,
            host_kernel: "kernel".into(),
            host_cpu_model: "cpu".into(),
            host_ram_bytes: 4096,
            host_disk: "disk".into(),
            host_filesystem: "fs".into(),
            network_topology: "loopback".into(),
            metrics_command: "metrics".into(),
        };
        let matrix = coverage_matrix(&manifest).unwrap();
        let mut runs = Vec::new();
        for cell in &matrix {
            for repeat in 1..=repeats {
                let producer_latency_ns = if cell.workload_mode == WorkloadMode::BacklogConsumer {
                    Vec::new()
                } else {
                    vec![100 * scale, 200 * scale]
                };
                let fetch_latency_ns = if cell.workload_mode == WorkloadMode::ProducerOnly {
                    Vec::new()
                } else {
                    vec![150 * scale, 250 * scale]
                };
                runs.push(BenchmarkRun {
                    cell: cell.clone(),
                    repeat,
                    topic_name: format!("topic-{repeat}"),
                    topic_id_hex: format!("{:032x}", repeat),
                    measurement_elapsed_ns: 10_000_000,
                    produced_events: if producer_latency_ns.is_empty() {
                        0
                    } else {
                        2 * scale
                    },
                    fetched_events: if fetch_latency_ns.is_empty() {
                        0
                    } else {
                        2 * scale
                    },
                    logical_bytes: (cell.record_size as u64).saturating_mul(4 * scale),
                    producer_latency_ns,
                    fetch_latency_ns,
                    start_metrics: SystemMetricsSnapshot::default(),
                    end_metrics: SystemMetricsSnapshot {
                        cpu_nanos: 100,
                        rss_bytes: 200,
                        disk_read_bytes: 10,
                        disk_write_bytes: 20,
                        network_rx_bytes: 30,
                        network_tx_bytes: 40,
                        fsync_count: 2,
                        fsync_total_ns: 50,
                        replication_lag: 0,
                    },
                    error_count: 0,
                    throttle_count: 0,
                });
            }
        }
        let _ = CompressionSpec::None;
        RawBundle {
            manifest,
            provenance: BenchmarkProvenance {
                git_sha: "a".repeat(40),
                rustc_version: "rustc test".into(),
                build_profile: "release".into(),
                rustflags: "-C target-cpu=native".into(),
                target_os: "test".into(),
                target_arch: "x86_64".into(),
                host_os: "test".into(),
                host_kernel: "kernel".into(),
                host_cpu_model: "cpu".into(),
                host_core_count: 4,
                host_ram_bytes: 4096,
                host_disk: "disk".into(),
                host_filesystem: "fs".into(),
                network_topology: "loopback".into(),
            },
            matrix,
            runs,
        }
    }

    #[test]
    fn p8h_report_generation_is_byte_deterministic_from_raw_bundle() {
        let raw = bundle(2, 1);
        let first = generate_report_markdown(&raw).unwrap();
        let second =
            generate_report_markdown(&RawBundle::parse(&raw.canonical_text().unwrap()).unwrap())
                .unwrap();
        assert_eq!(first, second);
        assert!(first.contains("producer latency") || first.contains("prod p50"));
    }

    #[test]
    fn p8h_comparison_requires_repeated_complete_runs() {
        assert!(generate_comparison_markdown(&bundle(1, 1), &bundle(1, 2)).is_err());
        let first = generate_comparison_markdown(&bundle(2, 1), &bundle(2, 2)).unwrap();
        let second = generate_comparison_markdown(&bundle(2, 1), &bundle(2, 2)).unwrap();
        assert_eq!(first, second);
    }
}
