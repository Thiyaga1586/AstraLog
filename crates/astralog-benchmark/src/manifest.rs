use std::{collections::BTreeMap, path::PathBuf};

use astralog_protocol::api::{
    AckMode, MAX_RECORD_VALUE_LEN, MAX_RECORDS_PER_PARTITION,
    MAX_TOPIC_BATCH_UNCOMPRESSED_BYTES_V1, TopicCompressionV1,
};

use crate::{BenchmarkError, Result};

pub const MANIFEST_HEADER: &str = "ASTRALOG_BENCH_MANIFEST_V1";
pub const FROZEN_RECORD_SIZES: [usize; 3] = [100, 1024, 10 * 1024];

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub enum KeyMode {
    Keyed,
    Unkeyed,
}

impl KeyMode {
    pub const ALL: [Self; 2] = [Self::Keyed, Self::Unkeyed];

    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Keyed => "keyed",
            Self::Unkeyed => "unkeyed",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub enum CompressionSpec {
    None,
    Lz4,
    Zstd,
}

impl CompressionSpec {
    pub const ALL: [Self; 3] = [Self::None, Self::Lz4, Self::Zstd];

    pub const fn as_str(self) -> &'static str {
        match self {
            Self::None => "none",
            Self::Lz4 => "lz4",
            Self::Zstd => "zstd",
        }
    }

    pub const fn protocol(self) -> TopicCompressionV1 {
        match self {
            Self::None => TopicCompressionV1::None,
            Self::Lz4 => TopicCompressionV1::Lz4,
            Self::Zstd => TopicCompressionV1::Zstd,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub enum AckModeSpec {
    Leader,
    Quorum,
}

impl AckModeSpec {
    pub const ALL: [Self; 2] = [Self::Leader, Self::Quorum];

    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Leader => "leader",
            Self::Quorum => "quorum",
        }
    }

    pub const fn protocol(self) -> AckMode {
        match self {
            Self::Leader => AckMode::Leader,
            Self::Quorum => AckMode::Quorum,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub enum WorkloadMode {
    ProducerOnly,
    BacklogConsumer,
    Simultaneous,
}

impl WorkloadMode {
    pub const ALL: [Self; 3] = [
        Self::ProducerOnly,
        Self::BacklogConsumer,
        Self::Simultaneous,
    ];

    pub const fn as_str(self) -> &'static str {
        match self {
            Self::ProducerOnly => "producer_only",
            Self::BacklogConsumer => "backlog_consumer",
            Self::Simultaneous => "simultaneous",
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BenchmarkManifest {
    pub benchmark_id: String,
    pub repository_root: PathBuf,
    pub bootstrap_endpoint: String,
    pub topic_prefix: String,
    pub partition_count: u32,
    pub multi_producer_count: usize,
    pub warmup_ms: u64,
    pub measurement_ms: u64,
    pub repeats: u32,
    pub backlog_records: u64,
    pub batch_max_records: usize,
    pub batch_max_bytes: usize,
    pub batch_linger_ms: u64,
    pub request_timeout_ms: u64,
    pub fetch_max_bytes: u32,
    pub fetch_max_records: u32,
    pub max_latency_samples: usize,
    pub seed: u64,
    pub host_kernel: String,
    pub host_cpu_model: String,
    pub host_ram_bytes: u64,
    pub host_disk: String,
    pub host_filesystem: String,
    pub network_topology: String,
    pub metrics_command: String,
}

impl BenchmarkManifest {
    pub fn validate(&self) -> Result<()> {
        validate_token(&self.benchmark_id, "benchmark_id", 64)?;
        validate_token(&self.topic_prefix, "topic_prefix", 80)?;
        require_text(&self.bootstrap_endpoint, "bootstrap_endpoint")?;
        if self.repository_root.as_os_str().is_empty() {
            return Err(BenchmarkError::Invalid("repository_root is required"));
        }
        if self.partition_count == 0 || self.partition_count > 4096 {
            return Err(BenchmarkError::Invalid(
                "partition_count must be in 1..=4096",
            ));
        }
        if !(2..=64).contains(&self.multi_producer_count) {
            return Err(BenchmarkError::Invalid(
                "multi_producer_count must be in 2..=64",
            ));
        }
        if self.warmup_ms == 0 || self.measurement_ms == 0 {
            return Err(BenchmarkError::Invalid(
                "warmup_ms and measurement_ms must be non-zero",
            ));
        }
        if self.warmup_ms > 3_600_000 || self.measurement_ms > 3_600_000 {
            return Err(BenchmarkError::Invalid(
                "warmup/measurement duration exceeds one-hour hard bound",
            ));
        }
        if self.repeats == 0 || self.repeats > 20 {
            return Err(BenchmarkError::Invalid("repeats must be in 1..=20"));
        }
        if self.backlog_records == 0 || self.backlog_records > 10_000_000 {
            return Err(BenchmarkError::Invalid(
                "backlog_records must be in 1..=10000000",
            ));
        }
        if self.batch_max_records == 0 || self.batch_max_records > MAX_RECORDS_PER_PARTITION {
            return Err(BenchmarkError::Invalid(
                "batch_max_records exceeds frozen Wire V1 bound",
            ));
        }
        if self.batch_max_bytes < FROZEN_RECORD_SIZES[2]
            || self.batch_max_bytes > MAX_TOPIC_BATCH_UNCOMPRESSED_BYTES_V1 as usize
        {
            return Err(BenchmarkError::Invalid(
                "batch_max_bytes cannot fit every frozen record size or exceeds P8 bound",
            ));
        }
        if FROZEN_RECORD_SIZES[2] > MAX_RECORD_VALUE_LEN {
            return Err(BenchmarkError::Invalid(
                "frozen 10 KiB benchmark record exceeds Wire V1 value bound",
            ));
        }
        if self.batch_linger_ms > 60_000 {
            return Err(BenchmarkError::Invalid(
                "batch_linger_ms exceeds 60-second hard bound",
            ));
        }
        if self.request_timeout_ms == 0 || self.request_timeout_ms > 300_000 {
            return Err(BenchmarkError::Invalid(
                "request_timeout_ms must be in 1..=300000",
            ));
        }
        if self.fetch_max_bytes == 0 || self.fetch_max_records == 0 {
            return Err(BenchmarkError::Invalid(
                "fetch_max_bytes/fetch_max_records must be non-zero",
            ));
        }
        if self.max_latency_samples == 0 || self.max_latency_samples > 10_000_000 {
            return Err(BenchmarkError::Invalid(
                "max_latency_samples must be in 1..=10000000",
            ));
        }
        require_text(&self.host_kernel, "host_kernel")?;
        require_text(&self.host_cpu_model, "host_cpu_model")?;
        if self.host_ram_bytes == 0 {
            return Err(BenchmarkError::Invalid("host_ram_bytes must be non-zero"));
        }
        require_text(&self.host_disk, "host_disk")?;
        require_text(&self.host_filesystem, "host_filesystem")?;
        require_text(&self.network_topology, "network_topology")?;
        require_text(&self.metrics_command, "metrics_command")?;
        Ok(())
    }

    pub fn parse(text: &str) -> Result<Self> {
        let mut lines = text.lines();
        if lines.next() != Some(MANIFEST_HEADER) {
            return Err(BenchmarkError::Invalid(
                "benchmark manifest header/version mismatch",
            ));
        }
        let mut values = BTreeMap::<String, String>::new();
        for line in lines {
            if line.is_empty() {
                continue;
            }
            let (key, raw) = line
                .split_once('=')
                .ok_or(BenchmarkError::Invalid("malformed benchmark manifest line"))?;
            if key.is_empty() || values.contains_key(key) {
                return Err(BenchmarkError::Invalid(
                    "duplicate/empty benchmark manifest key",
                ));
            }
            values.insert(key.to_owned(), decode_value(raw)?);
        }
        const KEYS: [&str; 24] = [
            "benchmark_id",
            "repository_root",
            "bootstrap_endpoint",
            "topic_prefix",
            "partition_count",
            "multi_producer_count",
            "warmup_ms",
            "measurement_ms",
            "repeats",
            "backlog_records",
            "batch_max_records",
            "batch_max_bytes",
            "batch_linger_ms",
            "request_timeout_ms",
            "fetch_max_bytes",
            "fetch_max_records",
            "max_latency_samples",
            "seed",
            "host_kernel",
            "host_cpu_model",
            "host_ram_bytes",
            "host_disk",
            "host_filesystem",
            "network_topology",
        ];
        for key in KEYS.into_iter().chain(["metrics_command"]) {
            if !values.contains_key(key) {
                return Err(BenchmarkError::InvalidOwned(format!(
                    "missing benchmark manifest key {key}"
                )));
            }
        }
        if values.len() != 25 {
            return Err(BenchmarkError::Invalid(
                "unknown benchmark manifest key is not allowed",
            ));
        }
        let manifest = Self {
            benchmark_id: take(&mut values, "benchmark_id")?,
            repository_root: PathBuf::from(take(&mut values, "repository_root")?),
            bootstrap_endpoint: take(&mut values, "bootstrap_endpoint")?,
            topic_prefix: take(&mut values, "topic_prefix")?,
            partition_count: take_num(&mut values, "partition_count")?,
            multi_producer_count: take_num(&mut values, "multi_producer_count")?,
            warmup_ms: take_num(&mut values, "warmup_ms")?,
            measurement_ms: take_num(&mut values, "measurement_ms")?,
            repeats: take_num(&mut values, "repeats")?,
            backlog_records: take_num(&mut values, "backlog_records")?,
            batch_max_records: take_num(&mut values, "batch_max_records")?,
            batch_max_bytes: take_num(&mut values, "batch_max_bytes")?,
            batch_linger_ms: take_num(&mut values, "batch_linger_ms")?,
            request_timeout_ms: take_num(&mut values, "request_timeout_ms")?,
            fetch_max_bytes: take_num(&mut values, "fetch_max_bytes")?,
            fetch_max_records: take_num(&mut values, "fetch_max_records")?,
            max_latency_samples: take_num(&mut values, "max_latency_samples")?,
            seed: take_num(&mut values, "seed")?,
            host_kernel: take(&mut values, "host_kernel")?,
            host_cpu_model: take(&mut values, "host_cpu_model")?,
            host_ram_bytes: take_num(&mut values, "host_ram_bytes")?,
            host_disk: take(&mut values, "host_disk")?,
            host_filesystem: take(&mut values, "host_filesystem")?,
            network_topology: take(&mut values, "network_topology")?,
            metrics_command: take(&mut values, "metrics_command")?,
        };
        manifest.validate()?;
        Ok(manifest)
    }

    pub fn canonical_text(&self) -> Result<String> {
        self.validate()?;
        let pairs = [
            ("benchmark_id", self.benchmark_id.clone()),
            (
                "repository_root",
                self.repository_root.to_string_lossy().into_owned(),
            ),
            ("bootstrap_endpoint", self.bootstrap_endpoint.clone()),
            ("topic_prefix", self.topic_prefix.clone()),
            ("partition_count", self.partition_count.to_string()),
            (
                "multi_producer_count",
                self.multi_producer_count.to_string(),
            ),
            ("warmup_ms", self.warmup_ms.to_string()),
            ("measurement_ms", self.measurement_ms.to_string()),
            ("repeats", self.repeats.to_string()),
            ("backlog_records", self.backlog_records.to_string()),
            ("batch_max_records", self.batch_max_records.to_string()),
            ("batch_max_bytes", self.batch_max_bytes.to_string()),
            ("batch_linger_ms", self.batch_linger_ms.to_string()),
            ("request_timeout_ms", self.request_timeout_ms.to_string()),
            ("fetch_max_bytes", self.fetch_max_bytes.to_string()),
            ("fetch_max_records", self.fetch_max_records.to_string()),
            ("max_latency_samples", self.max_latency_samples.to_string()),
            ("seed", self.seed.to_string()),
            ("host_kernel", self.host_kernel.clone()),
            ("host_cpu_model", self.host_cpu_model.clone()),
            ("host_ram_bytes", self.host_ram_bytes.to_string()),
            ("host_disk", self.host_disk.clone()),
            ("host_filesystem", self.host_filesystem.clone()),
            ("network_topology", self.network_topology.clone()),
            ("metrics_command", self.metrics_command.clone()),
        ];
        let mut output = String::from(MANIFEST_HEADER);
        output.push('\n');
        for (key, value) in pairs {
            output.push_str(key);
            output.push('=');
            output.push_str(&encode_value(&value));
            output.push('\n');
        }
        Ok(output)
    }
}

fn validate_token(value: &str, name: &'static str, max: usize) -> Result<()> {
    if value.is_empty()
        || value.len() > max
        || !value
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'.' | b'_' | b'-'))
    {
        return Err(BenchmarkError::InvalidOwned(format!(
            "{name} must be 1..={max} ASCII [A-Za-z0-9._-]"
        )));
    }
    Ok(())
}

fn require_text(value: &str, name: &'static str) -> Result<()> {
    if value.is_empty() || value.contains(['\n', '\r', '\0']) {
        return Err(BenchmarkError::InvalidOwned(format!(
            "{name} must be non-empty single-line text"
        )));
    }
    Ok(())
}

fn take(values: &mut BTreeMap<String, String>, key: &'static str) -> Result<String> {
    values
        .remove(key)
        .ok_or_else(|| BenchmarkError::InvalidOwned(format!("missing {key}")))
}

fn take_num<T>(values: &mut BTreeMap<String, String>, key: &'static str) -> Result<T>
where
    T: std::str::FromStr,
{
    take(values, key)?.parse::<T>().map_err(|_| {
        BenchmarkError::InvalidOwned(format!("benchmark manifest {key} is not a valid number"))
    })
}

pub(crate) fn encode_value(value: &str) -> String {
    const HEX: &[u8; 16] = b"0123456789ABCDEF";
    let mut output = String::with_capacity(value.len());
    for byte in value.bytes() {
        match byte {
            b'%' => output.push_str("%25"),
            b'\n' => output.push_str("%0A"),
            b'\r' => output.push_str("%0D"),
            b'\t' => output.push_str("%09"),
            b'=' => output.push_str("%3D"),
            0x20..=0x7E => output.push(char::from(byte)),
            _ => {
                output.push('%');
                output.push(char::from(HEX[usize::from(byte >> 4)]));
                output.push(char::from(HEX[usize::from(byte & 0x0F)]));
            }
        }
    }
    output
}

pub(crate) fn decode_value(value: &str) -> Result<String> {
    let bytes = value.as_bytes();
    let mut output = Vec::with_capacity(bytes.len());
    let mut index = 0;
    while index < bytes.len() {
        if bytes[index] != b'%' {
            output.push(bytes[index]);
            index += 1;
            continue;
        }
        if index + 2 >= bytes.len() {
            return Err(BenchmarkError::Invalid("truncated percent escape"));
        }
        let high = hex(bytes[index + 1])?;
        let low = hex(bytes[index + 2])?;
        output.push((high << 4) | low);
        index += 3;
    }
    String::from_utf8(output).map_err(|_| BenchmarkError::Invalid("manifest value is not UTF-8"))
}

fn hex(value: u8) -> Result<u8> {
    match value {
        b'0'..=b'9' => Ok(value - b'0'),
        b'a'..=b'f' => Ok(value - b'a' + 10),
        b'A'..=b'F' => Ok(value - b'A' + 10),
        _ => Err(BenchmarkError::Invalid("invalid percent escape")),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn manifest() -> BenchmarkManifest {
        BenchmarkManifest {
            benchmark_id: "p8-baseline".into(),
            repository_root: PathBuf::from("C:/astralog"),
            bootstrap_endpoint: "127.0.0.1:9092".into(),
            topic_prefix: "p8bench".into(),
            partition_count: 6,
            multi_producer_count: 4,
            warmup_ms: 1_000,
            measurement_ms: 5_000,
            repeats: 3,
            backlog_records: 10_000,
            batch_max_records: 128,
            batch_max_bytes: 1024 * 1024,
            batch_linger_ms: 5,
            request_timeout_ms: 30_000,
            fetch_max_bytes: 1024 * 1024,
            fetch_max_records: 1_000,
            max_latency_samples: 100_000,
            seed: 42,
            host_kernel: "test-kernel".into(),
            host_cpu_model: "test-cpu".into(),
            host_ram_bytes: 16 * 1024 * 1024 * 1024,
            host_disk: "nvme-test".into(),
            host_filesystem: "testfs".into(),
            network_topology: "3 brokers / loopback".into(),
            metrics_command: "metrics --snapshot=now".into(),
        }
    }

    #[test]
    fn p8h_manifest_round_trip_is_canonical_and_complete() {
        let value = manifest();
        let encoded = value.canonical_text().unwrap();
        let decoded = BenchmarkManifest::parse(&encoded).unwrap();
        assert_eq!(decoded, value);
        assert_eq!(decoded.canonical_text().unwrap(), encoded);
    }

    #[test]
    fn p8h_manifest_rejects_missing_host_or_invalid_runtime_bounds() {
        let mut value = manifest();
        value.host_cpu_model.clear();
        assert!(value.validate().is_err());
        value.host_cpu_model = "cpu".into();
        value.batch_max_bytes = 1024;
        assert!(value.validate().is_err());
        value.batch_max_bytes = 1024 * 1024;
        value.repeats = 0;
        assert!(value.validate().is_err());
    }

    #[test]
    fn p8h_manifest_unknown_or_duplicate_fields_fail_closed() {
        let encoded = manifest().canonical_text().unwrap();
        assert!(BenchmarkManifest::parse(&(encoded.clone() + "unknown=1\n")).is_err());
        assert!(BenchmarkManifest::parse(&(encoded + "benchmark_id=duplicate\n")).is_err());
    }
}
