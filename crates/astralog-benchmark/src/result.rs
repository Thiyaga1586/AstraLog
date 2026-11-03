use std::collections::{BTreeMap, BTreeSet};

use crate::{
    BenchmarkError, BenchmarkManifest, Result, WorkloadCell,
    manifest::{decode_value, encode_value},
    matrix::{
        parse_ack_mode, parse_compression, parse_key_mode, parse_workload_mode,
        validate_coverage_matrix,
    },
};

pub const RAW_HEADER: &str = "ASTRALOG_BENCH_RAW_V1";

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BenchmarkProvenance {
    pub git_sha: String,
    pub rustc_version: String,
    pub build_profile: String,
    pub rustflags: String,
    pub target_os: String,
    pub target_arch: String,
    pub host_os: String,
    pub host_kernel: String,
    pub host_cpu_model: String,
    pub host_core_count: usize,
    pub host_ram_bytes: u64,
    pub host_disk: String,
    pub host_filesystem: String,
    pub network_topology: String,
}

impl BenchmarkProvenance {
    pub fn validate(&self) -> Result<()> {
        for (name, value) in [
            ("git_sha", self.git_sha.as_str()),
            ("rustc_version", self.rustc_version.as_str()),
            ("build_profile", self.build_profile.as_str()),
            ("target_os", self.target_os.as_str()),
            ("target_arch", self.target_arch.as_str()),
            ("host_os", self.host_os.as_str()),
            ("host_kernel", self.host_kernel.as_str()),
            ("host_cpu_model", self.host_cpu_model.as_str()),
            ("host_disk", self.host_disk.as_str()),
            ("host_filesystem", self.host_filesystem.as_str()),
            ("network_topology", self.network_topology.as_str()),
        ] {
            if value.is_empty() {
                return Err(BenchmarkError::InvalidOwned(format!(
                    "benchmark provenance {name} is required"
                )));
            }
        }
        if self.git_sha.len() != 40 || !self.git_sha.bytes().all(|byte| byte.is_ascii_hexdigit()) {
            return Err(BenchmarkError::Invalid(
                "benchmark provenance requires an exact 40-hex git SHA",
            ));
        }
        if self.build_profile != "release" {
            return Err(BenchmarkError::Invalid(
                "benchmark provenance build_profile must be release",
            ));
        }
        if self.host_core_count == 0 || self.host_ram_bytes == 0 {
            return Err(BenchmarkError::Invalid(
                "benchmark host core/RAM provenance must be non-zero",
            ));
        }
        Ok(())
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct SystemMetricsSnapshot {
    pub cpu_nanos: u64,
    pub rss_bytes: u64,
    pub disk_read_bytes: u64,
    pub disk_write_bytes: u64,
    pub network_rx_bytes: u64,
    pub network_tx_bytes: u64,
    pub fsync_count: u64,
    pub fsync_total_ns: u64,
    pub replication_lag: u64,
}

impl SystemMetricsSnapshot {
    pub fn parse(text: &str) -> Result<Self> {
        let mut values = BTreeMap::<String, u64>::new();
        for line in text.lines().filter(|line| !line.trim().is_empty()) {
            let (key, value) = line
                .trim()
                .split_once('=')
                .ok_or(BenchmarkError::Invalid("malformed system metrics line"))?;
            if values.contains_key(key) {
                return Err(BenchmarkError::Invalid("duplicate system metrics key"));
            }
            values.insert(
                key.to_owned(),
                value
                    .parse::<u64>()
                    .map_err(|_| BenchmarkError::Invalid("invalid system metrics number"))?,
            );
        }
        const KEYS: [&str; 9] = [
            "cpu_nanos",
            "rss_bytes",
            "disk_read_bytes",
            "disk_write_bytes",
            "network_rx_bytes",
            "network_tx_bytes",
            "fsync_count",
            "fsync_total_ns",
            "replication_lag",
        ];
        for key in KEYS {
            if !values.contains_key(key) {
                return Err(BenchmarkError::InvalidOwned(format!(
                    "missing system metrics key {key}"
                )));
            }
        }
        if values.len() != KEYS.len() {
            return Err(BenchmarkError::Invalid(
                "unknown system metrics key is not allowed",
            ));
        }
        Ok(Self {
            cpu_nanos: values["cpu_nanos"],
            rss_bytes: values["rss_bytes"],
            disk_read_bytes: values["disk_read_bytes"],
            disk_write_bytes: values["disk_write_bytes"],
            network_rx_bytes: values["network_rx_bytes"],
            network_tx_bytes: values["network_tx_bytes"],
            fsync_count: values["fsync_count"],
            fsync_total_ns: values["fsync_total_ns"],
            replication_lag: values["replication_lag"],
        })
    }

    pub fn validate_delta_from(&self, start: Self) -> Result<()> {
        if self.cpu_nanos < start.cpu_nanos
            || self.disk_read_bytes < start.disk_read_bytes
            || self.disk_write_bytes < start.disk_write_bytes
            || self.network_rx_bytes < start.network_rx_bytes
            || self.network_tx_bytes < start.network_tx_bytes
            || self.fsync_count < start.fsync_count
            || self.fsync_total_ns < start.fsync_total_ns
        {
            return Err(BenchmarkError::Invalid(
                "cumulative system metrics regressed during benchmark run",
            ));
        }
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BenchmarkRun {
    pub cell: WorkloadCell,
    pub repeat: u32,
    pub topic_name: String,
    pub topic_id_hex: String,
    pub measurement_elapsed_ns: u64,
    pub produced_events: u64,
    pub fetched_events: u64,
    pub logical_bytes: u64,
    pub producer_latency_ns: Vec<u64>,
    pub fetch_latency_ns: Vec<u64>,
    pub start_metrics: SystemMetricsSnapshot,
    pub end_metrics: SystemMetricsSnapshot,
    pub error_count: u64,
    pub throttle_count: u64,
}

impl BenchmarkRun {
    pub fn validate(&self, manifest: &BenchmarkManifest) -> Result<()> {
        if self.repeat == 0 || self.repeat > manifest.repeats {
            return Err(BenchmarkError::Invalid(
                "benchmark run repeat is outside manifest bounds",
            ));
        }
        if self.topic_name.is_empty()
            || self.topic_name.contains(['\n', '\r', '\0'])
            || self.topic_id_hex.len() != 32
            || !self
                .topic_id_hex
                .bytes()
                .all(|byte| byte.is_ascii_hexdigit())
        {
            return Err(BenchmarkError::Invalid(
                "benchmark run topic identity is malformed",
            ));
        }
        if self.measurement_elapsed_ns == 0 {
            return Err(BenchmarkError::Invalid(
                "benchmark measurement elapsed time must be non-zero",
            ));
        }
        if self.producer_latency_ns.contains(&0) || self.fetch_latency_ns.contains(&0) {
            return Err(BenchmarkError::Invalid(
                "benchmark latency samples must be positive",
            ));
        }
        if self.producer_latency_ns.len() > manifest.max_latency_samples
            || self.fetch_latency_ns.len() > manifest.max_latency_samples
        {
            return Err(BenchmarkError::Invalid(
                "benchmark latency sample count exceeds manifest hard bound",
            ));
        }
        match self.cell.workload_mode {
            crate::WorkloadMode::ProducerOnly if self.producer_latency_ns.is_empty() => {
                return Err(BenchmarkError::Invalid(
                    "producer-only benchmark run has no producer samples",
                ));
            }
            crate::WorkloadMode::BacklogConsumer if self.fetch_latency_ns.is_empty() => {
                return Err(BenchmarkError::Invalid(
                    "backlog-consumer benchmark run has no fetch samples",
                ));
            }
            crate::WorkloadMode::Simultaneous
                if self.producer_latency_ns.is_empty() || self.fetch_latency_ns.is_empty() =>
            {
                return Err(BenchmarkError::Invalid(
                    "simultaneous benchmark run requires producer and fetch samples",
                ));
            }
            _ => {}
        }
        self.end_metrics.validate_delta_from(self.start_metrics)?;
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RawBundle {
    pub manifest: BenchmarkManifest,
    pub provenance: BenchmarkProvenance,
    pub matrix: Vec<WorkloadCell>,
    pub runs: Vec<BenchmarkRun>,
}

impl RawBundle {
    pub fn validate(&self) -> Result<()> {
        self.manifest.validate()?;
        self.provenance.validate()?;
        if self.provenance.host_kernel != self.manifest.host_kernel
            || self.provenance.host_cpu_model != self.manifest.host_cpu_model
            || self.provenance.host_ram_bytes != self.manifest.host_ram_bytes
            || self.provenance.host_disk != self.manifest.host_disk
            || self.provenance.host_filesystem != self.manifest.host_filesystem
            || self.provenance.network_topology != self.manifest.network_topology
        {
            return Err(BenchmarkError::Invalid(
                "raw bundle provenance does not match manifest host declaration",
            ));
        }
        validate_coverage_matrix(&self.manifest, &self.matrix)?;
        let expected_runs = self
            .matrix
            .len()
            .checked_mul(self.manifest.repeats as usize)
            .ok_or(BenchmarkError::Invalid("benchmark run count overflow"))?;
        if self.runs.len() != expected_runs {
            return Err(BenchmarkError::InvalidOwned(format!(
                "raw bundle run count {} does not match expected {expected_runs}",
                self.runs.len()
            )));
        }
        let matrix_ids = self
            .matrix
            .iter()
            .map(WorkloadCell::id)
            .collect::<BTreeSet<_>>();
        let mut observed = BTreeSet::new();
        for run in &self.runs {
            run.validate(&self.manifest)?;
            let cell_id = run.cell.id();
            if !matrix_ids.contains(&cell_id) {
                return Err(BenchmarkError::Invalid(
                    "raw bundle run references a workload outside the frozen matrix",
                ));
            }
            if !observed.insert((cell_id, run.repeat)) {
                return Err(BenchmarkError::Invalid(
                    "raw bundle contains duplicate workload repeat",
                ));
            }
        }
        Ok(())
    }

    pub fn canonical_text(&self) -> Result<String> {
        self.validate()?;
        let mut output = String::from(RAW_HEADER);
        output.push('\n');
        push(&mut output, "manifest", &self.manifest.canonical_text()?);
        push(&mut output, "provenance.git_sha", &self.provenance.git_sha);
        push(
            &mut output,
            "provenance.rustc_version",
            &self.provenance.rustc_version,
        );
        push(
            &mut output,
            "provenance.build_profile",
            &self.provenance.build_profile,
        );
        push(
            &mut output,
            "provenance.rustflags",
            &self.provenance.rustflags,
        );
        push(
            &mut output,
            "provenance.target_os",
            &self.provenance.target_os,
        );
        push(
            &mut output,
            "provenance.target_arch",
            &self.provenance.target_arch,
        );
        push(&mut output, "provenance.host_os", &self.provenance.host_os);
        push(
            &mut output,
            "provenance.host_kernel",
            &self.provenance.host_kernel,
        );
        push(
            &mut output,
            "provenance.host_cpu_model",
            &self.provenance.host_cpu_model,
        );
        push_num(
            &mut output,
            "provenance.host_core_count",
            self.provenance.host_core_count,
        );
        push_num(
            &mut output,
            "provenance.host_ram_bytes",
            self.provenance.host_ram_bytes,
        );
        push(
            &mut output,
            "provenance.host_disk",
            &self.provenance.host_disk,
        );
        push(
            &mut output,
            "provenance.host_filesystem",
            &self.provenance.host_filesystem,
        );
        push(
            &mut output,
            "provenance.network_topology",
            &self.provenance.network_topology,
        );
        push_num(&mut output, "matrix.count", self.matrix.len());
        for (index, cell) in self.matrix.iter().enumerate() {
            push(
                &mut output,
                &format!("matrix.{index:04}"),
                &encode_cell(cell),
            );
        }
        push_num(&mut output, "run.count", self.runs.len());
        for (index, run) in self.runs.iter().enumerate() {
            let prefix = format!("run.{index:06}");
            push(
                &mut output,
                &format!("{prefix}.cell"),
                &encode_cell(&run.cell),
            );
            push_num(&mut output, &format!("{prefix}.repeat"), run.repeat);
            push(
                &mut output,
                &format!("{prefix}.topic_name"),
                &run.topic_name,
            );
            push(
                &mut output,
                &format!("{prefix}.topic_id_hex"),
                &run.topic_id_hex,
            );
            push_num(
                &mut output,
                &format!("{prefix}.measurement_elapsed_ns"),
                run.measurement_elapsed_ns,
            );
            push_num(
                &mut output,
                &format!("{prefix}.produced_events"),
                run.produced_events,
            );
            push_num(
                &mut output,
                &format!("{prefix}.fetched_events"),
                run.fetched_events,
            );
            push_num(
                &mut output,
                &format!("{prefix}.logical_bytes"),
                run.logical_bytes,
            );
            push(
                &mut output,
                &format!("{prefix}.producer_latency_ns"),
                &join_u64(&run.producer_latency_ns),
            );
            push(
                &mut output,
                &format!("{prefix}.fetch_latency_ns"),
                &join_u64(&run.fetch_latency_ns),
            );
            push_metrics(
                &mut output,
                &format!("{prefix}.metrics.start"),
                run.start_metrics,
            );
            push_metrics(
                &mut output,
                &format!("{prefix}.metrics.end"),
                run.end_metrics,
            );
            push_num(
                &mut output,
                &format!("{prefix}.error_count"),
                run.error_count,
            );
            push_num(
                &mut output,
                &format!("{prefix}.throttle_count"),
                run.throttle_count,
            );
        }
        Ok(output)
    }

    pub fn parse(text: &str) -> Result<Self> {
        let mut lines = text.lines();
        if lines.next() != Some(RAW_HEADER) {
            return Err(BenchmarkError::Invalid(
                "raw benchmark header/version mismatch",
            ));
        }
        let mut values = BTreeMap::<String, String>::new();
        for line in lines {
            if line.is_empty() {
                continue;
            }
            let (key, raw) = line
                .split_once('=')
                .ok_or(BenchmarkError::Invalid("malformed raw benchmark line"))?;
            if key.is_empty() || values.contains_key(key) {
                return Err(BenchmarkError::Invalid("duplicate/empty raw benchmark key"));
            }
            values.insert(key.to_owned(), decode_value(raw)?);
        }
        let manifest = BenchmarkManifest::parse(&take(&mut values, "manifest")?)?;
        let provenance = BenchmarkProvenance {
            git_sha: take(&mut values, "provenance.git_sha")?,
            rustc_version: take(&mut values, "provenance.rustc_version")?,
            build_profile: take(&mut values, "provenance.build_profile")?,
            rustflags: take(&mut values, "provenance.rustflags")?,
            target_os: take(&mut values, "provenance.target_os")?,
            target_arch: take(&mut values, "provenance.target_arch")?,
            host_os: take(&mut values, "provenance.host_os")?,
            host_kernel: take(&mut values, "provenance.host_kernel")?,
            host_cpu_model: take(&mut values, "provenance.host_cpu_model")?,
            host_core_count: take_num(&mut values, "provenance.host_core_count")?,
            host_ram_bytes: take_num(&mut values, "provenance.host_ram_bytes")?,
            host_disk: take(&mut values, "provenance.host_disk")?,
            host_filesystem: take(&mut values, "provenance.host_filesystem")?,
            network_topology: take(&mut values, "provenance.network_topology")?,
        };
        let matrix_count: usize = take_num(&mut values, "matrix.count")?;
        let mut matrix = Vec::with_capacity(matrix_count);
        for index in 0..matrix_count {
            matrix.push(decode_cell(&take(
                &mut values,
                &format!("matrix.{index:04}"),
            )?)?);
        }
        let run_count: usize = take_num(&mut values, "run.count")?;
        let mut runs = Vec::with_capacity(run_count);
        for index in 0..run_count {
            let prefix = format!("run.{index:06}");
            let cell = decode_cell(&take(&mut values, &format!("{prefix}.cell"))?)?;
            let repeat = take_num(&mut values, &format!("{prefix}.repeat"))?;
            let topic_name = take(&mut values, &format!("{prefix}.topic_name"))?;
            let topic_id_hex = take(&mut values, &format!("{prefix}.topic_id_hex"))?;
            let measurement_elapsed_ns =
                take_num(&mut values, &format!("{prefix}.measurement_elapsed_ns"))?;
            let produced_events = take_num(&mut values, &format!("{prefix}.produced_events"))?;
            let fetched_events = take_num(&mut values, &format!("{prefix}.fetched_events"))?;
            let logical_bytes = take_num(&mut values, &format!("{prefix}.logical_bytes"))?;
            let producer_latency_ns = parse_u64_list(&take(
                &mut values,
                &format!("{prefix}.producer_latency_ns"),
            )?)?;
            let fetch_latency_ns =
                parse_u64_list(&take(&mut values, &format!("{prefix}.fetch_latency_ns"))?)?;
            let start_metrics = take_metrics(&mut values, &format!("{prefix}.metrics.start"))?;
            let end_metrics = take_metrics(&mut values, &format!("{prefix}.metrics.end"))?;
            let error_count = take_num(&mut values, &format!("{prefix}.error_count"))?;
            let throttle_count = take_num(&mut values, &format!("{prefix}.throttle_count"))?;
            runs.push(BenchmarkRun {
                cell,
                repeat,
                topic_name,
                topic_id_hex,
                measurement_elapsed_ns,
                produced_events,
                fetched_events,
                logical_bytes,
                producer_latency_ns,
                fetch_latency_ns,
                start_metrics,
                end_metrics,
                error_count,
                throttle_count,
            });
        }
        if !values.is_empty() {
            return Err(BenchmarkError::InvalidOwned(format!(
                "unknown raw benchmark keys remain: {}",
                values.keys().cloned().collect::<Vec<_>>().join(",")
            )));
        }
        let bundle = Self {
            manifest,
            provenance,
            matrix,
            runs,
        };
        bundle.validate()?;
        Ok(bundle)
    }
}

pub fn percentile_ns(samples: &[u64], numerator: u64, denominator: u64) -> Option<u64> {
    if samples.is_empty() || numerator == 0 || denominator == 0 || numerator > denominator {
        return None;
    }
    let mut sorted = samples.to_vec();
    sorted.sort_unstable();
    let count = u64::try_from(sorted.len()).ok()?;
    let rank = count.checked_mul(numerator)?.checked_add(denominator - 1)? / denominator;
    let index = usize::try_from(rank.saturating_sub(1)).ok()?;
    sorted.get(index).copied()
}

fn encode_cell(cell: &WorkloadCell) -> String {
    format!(
        "{}|{}|{}|{}|{}|{}|{}",
        cell.record_size,
        cell.key_mode.as_str(),
        cell.replication_factor,
        cell.ack_mode.as_str(),
        cell.compression.as_str(),
        cell.producer_count,
        cell.workload_mode.as_str(),
    )
}

fn decode_cell(value: &str) -> Result<WorkloadCell> {
    let parts = value.split('|').collect::<Vec<_>>();
    let [
        record_size,
        key_mode,
        replication_factor,
        ack_mode,
        compression,
        producer_count,
        mode,
    ] = parts.as_slice()
    else {
        return Err(BenchmarkError::Invalid("malformed benchmark workload cell"));
    };
    Ok(WorkloadCell {
        record_size: parse_num(record_size, "record_size")?,
        key_mode: parse_key_mode(key_mode)?,
        replication_factor: parse_num(replication_factor, "replication_factor")?,
        ack_mode: parse_ack_mode(ack_mode)?,
        compression: parse_compression(compression)?,
        producer_count: parse_num(producer_count, "producer_count")?,
        workload_mode: parse_workload_mode(mode)?,
    })
}

fn push(output: &mut String, key: &str, value: &str) {
    output.push_str(key);
    output.push('=');
    output.push_str(&encode_value(value));
    output.push('\n');
}

fn push_num<T: std::fmt::Display>(output: &mut String, key: &str, value: T) {
    push(output, key, &value.to_string());
}

fn push_metrics(output: &mut String, prefix: &str, metrics: SystemMetricsSnapshot) {
    for (suffix, value) in [
        ("cpu_nanos", metrics.cpu_nanos),
        ("rss_bytes", metrics.rss_bytes),
        ("disk_read_bytes", metrics.disk_read_bytes),
        ("disk_write_bytes", metrics.disk_write_bytes),
        ("network_rx_bytes", metrics.network_rx_bytes),
        ("network_tx_bytes", metrics.network_tx_bytes),
        ("fsync_count", metrics.fsync_count),
        ("fsync_total_ns", metrics.fsync_total_ns),
        ("replication_lag", metrics.replication_lag),
    ] {
        push_num(output, &format!("{prefix}.{suffix}"), value);
    }
}

fn take_metrics(
    values: &mut BTreeMap<String, String>,
    prefix: &str,
) -> Result<SystemMetricsSnapshot> {
    Ok(SystemMetricsSnapshot {
        cpu_nanos: take_num(values, &format!("{prefix}.cpu_nanos"))?,
        rss_bytes: take_num(values, &format!("{prefix}.rss_bytes"))?,
        disk_read_bytes: take_num(values, &format!("{prefix}.disk_read_bytes"))?,
        disk_write_bytes: take_num(values, &format!("{prefix}.disk_write_bytes"))?,
        network_rx_bytes: take_num(values, &format!("{prefix}.network_rx_bytes"))?,
        network_tx_bytes: take_num(values, &format!("{prefix}.network_tx_bytes"))?,
        fsync_count: take_num(values, &format!("{prefix}.fsync_count"))?,
        fsync_total_ns: take_num(values, &format!("{prefix}.fsync_total_ns"))?,
        replication_lag: take_num(values, &format!("{prefix}.replication_lag"))?,
    })
}

fn join_u64(values: &[u64]) -> String {
    values
        .iter()
        .map(u64::to_string)
        .collect::<Vec<_>>()
        .join(",")
}

fn parse_u64_list(value: &str) -> Result<Vec<u64>> {
    if value.is_empty() {
        return Ok(Vec::new());
    }
    value
        .split(',')
        .map(|item| parse_num(item, "latency sample"))
        .collect()
}

fn take(values: &mut BTreeMap<String, String>, key: &str) -> Result<String> {
    values
        .remove(key)
        .ok_or_else(|| BenchmarkError::InvalidOwned(format!("missing raw benchmark key {key}")))
}

fn take_num<T>(values: &mut BTreeMap<String, String>, key: &str) -> Result<T>
where
    T: std::str::FromStr,
{
    let value = take(values, key)?;
    parse_num(&value, key)
}

fn parse_num<T>(value: &str, name: &str) -> Result<T>
where
    T: std::str::FromStr,
{
    value.parse::<T>().map_err(|_| {
        BenchmarkError::InvalidOwned(format!("benchmark field {name} is not a valid number"))
    })
}

#[cfg(test)]
mod tests {
    use std::path::PathBuf;

    use crate::{AckModeSpec, CompressionSpec, KeyMode, WorkloadMode};

    use super::*;

    fn manifest() -> BenchmarkManifest {
        BenchmarkManifest {
            benchmark_id: "raw".into(),
            repository_root: PathBuf::from("."),
            bootstrap_endpoint: "127.0.0.1:9092".into(),
            topic_prefix: "p8raw".into(),
            partition_count: 1,
            multi_producer_count: 2,
            warmup_ms: 1,
            measurement_ms: 1,
            repeats: 2,
            backlog_records: 10,
            batch_max_records: 128,
            batch_max_bytes: 1024 * 1024,
            batch_linger_ms: 1,
            request_timeout_ms: 1_000,
            fetch_max_bytes: 1024,
            fetch_max_records: 10,
            max_latency_samples: 1_000,
            seed: 7,
            host_kernel: "kernel".into(),
            host_cpu_model: "cpu".into(),
            host_ram_bytes: 1024,
            host_disk: "disk".into(),
            host_filesystem: "fs".into(),
            network_topology: "loopback".into(),
            metrics_command: "metrics".into(),
        }
    }

    fn provenance() -> BenchmarkProvenance {
        BenchmarkProvenance {
            git_sha: "1".repeat(40),
            rustc_version: "rustc test".into(),
            build_profile: "release".into(),
            rustflags: String::new(),
            target_os: "test".into(),
            target_arch: "x86_64".into(),
            host_os: "test".into(),
            host_kernel: "kernel".into(),
            host_cpu_model: "cpu".into(),
            host_core_count: 4,
            host_ram_bytes: 1024,
            host_disk: "disk".into(),
            host_filesystem: "fs".into(),
            network_topology: "loopback".into(),
        }
    }

    fn run(cell: WorkloadCell, repeat: u32) -> BenchmarkRun {
        let producer_latency_ns = match cell.workload_mode {
            WorkloadMode::BacklogConsumer => Vec::new(),
            _ => vec![100, 200, 300],
        };
        let fetch_latency_ns = match cell.workload_mode {
            WorkloadMode::ProducerOnly => Vec::new(),
            _ => vec![150, 250],
        };
        BenchmarkRun {
            cell,
            repeat,
            topic_name: format!("topic-{repeat}"),
            topic_id_hex: "ab".repeat(16),
            measurement_elapsed_ns: 1_000_000,
            produced_events: 3,
            fetched_events: 2,
            logical_bytes: 300,
            producer_latency_ns,
            fetch_latency_ns,
            start_metrics: SystemMetricsSnapshot {
                cpu_nanos: 10,
                ..SystemMetricsSnapshot::default()
            },
            end_metrics: SystemMetricsSnapshot {
                cpu_nanos: 20,
                rss_bytes: 100,
                disk_read_bytes: 1,
                disk_write_bytes: 2,
                network_rx_bytes: 3,
                network_tx_bytes: 4,
                fsync_count: 1,
                fsync_total_ns: 5,
                replication_lag: 0,
            },
            error_count: 0,
            throttle_count: 0,
        }
    }

    #[test]
    fn p8h_raw_bundle_requires_complete_provenance_and_round_trips_byte_identically() {
        let manifest = manifest();
        let matrix = crate::coverage_matrix(&manifest).unwrap();
        let runs = matrix
            .iter()
            .flat_map(|cell| [run(cell.clone(), 1), run(cell.clone(), 2)])
            .collect();
        let bundle = RawBundle {
            manifest,
            provenance: provenance(),
            matrix,
            runs,
        };
        let encoded = bundle.canonical_text().unwrap();
        let decoded = RawBundle::parse(&encoded).unwrap();
        assert_eq!(decoded, bundle);
        assert_eq!(decoded.canonical_text().unwrap(), encoded);
    }

    #[test]
    fn p8h_percentiles_are_generated_from_raw_samples_by_nearest_rank() {
        let samples = [50, 10, 40, 20, 30];
        assert_eq!(percentile_ns(&samples, 50, 100), Some(30));
        assert_eq!(percentile_ns(&samples, 95, 100), Some(50));
        assert_eq!(percentile_ns(&samples, 999, 1000), Some(50));
        assert_eq!(percentile_ns(&[], 50, 100), None);
    }

    #[test]
    fn p8h_system_metric_snapshot_requires_every_frozen_metric() {
        let valid = "cpu_nanos=1\nrss_bytes=2\ndisk_read_bytes=3\ndisk_write_bytes=4\nnetwork_rx_bytes=5\nnetwork_tx_bytes=6\nfsync_count=7\nfsync_total_ns=8\nreplication_lag=9\n";
        assert_eq!(
            SystemMetricsSnapshot::parse(valid).unwrap().replication_lag,
            9
        );
        assert!(SystemMetricsSnapshot::parse("cpu_nanos=1\n").is_err());
    }

    #[test]
    fn p8h_run_cell_codec_preserves_all_frozen_dimensions() {
        let cell = WorkloadCell {
            record_size: 1024,
            key_mode: KeyMode::Keyed,
            replication_factor: 3,
            ack_mode: AckModeSpec::Quorum,
            compression: CompressionSpec::Zstd,
            producer_count: 4,
            workload_mode: WorkloadMode::Simultaneous,
        };
        assert_eq!(decode_cell(&encode_cell(&cell)).unwrap(), cell);
    }
}
