use std::collections::BTreeSet;

use crate::{
    AckModeSpec, BenchmarkError, BenchmarkManifest, CompressionSpec, KeyMode, Result, WorkloadMode,
    manifest::FROZEN_RECORD_SIZES,
};

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub struct WorkloadCell {
    pub record_size: usize,
    pub key_mode: KeyMode,
    pub replication_factor: u16,
    pub ack_mode: AckModeSpec,
    pub compression: CompressionSpec,
    pub producer_count: usize,
    pub workload_mode: WorkloadMode,
}

impl WorkloadCell {
    pub fn id(&self) -> String {
        format!(
            "s{}-{}-rf{}-{}-{}-p{}-{}",
            self.record_size,
            self.key_mode.as_str(),
            self.replication_factor,
            self.ack_mode.as_str(),
            self.compression.as_str(),
            self.producer_count,
            self.workload_mode.as_str(),
        )
    }

    pub(crate) fn coverage(&self) -> BTreeSet<String> {
        [
            format!("level:size:{}", self.record_size),
            format!("level:key:{}", self.key_mode.as_str()),
            format!("level:rf:{}", self.replication_factor),
            format!("level:ack:{}", self.ack_mode.as_str()),
            format!("level:compression:{}", self.compression.as_str()),
            format!("level:producers:{}", self.producer_count),
            format!("level:mode:{}", self.workload_mode.as_str()),
            format!(
                "pair:size-compression:{}:{}",
                self.record_size,
                self.compression.as_str()
            ),
            format!(
                "pair:rf-ack:{}:{}",
                self.replication_factor,
                self.ack_mode.as_str()
            ),
            format!(
                "pair:compression-mode:{}:{}",
                self.compression.as_str(),
                self.workload_mode.as_str()
            ),
            format!(
                "pair:producers-mode:{}:{}",
                self.producer_count,
                self.workload_mode.as_str()
            ),
        ]
        .into_iter()
        .collect()
    }
}

pub fn coverage_matrix(manifest: &BenchmarkManifest) -> Result<Vec<WorkloadCell>> {
    manifest.validate()?;
    let producer_counts = [1_usize, manifest.multi_producer_count];
    let mut candidates = Vec::new();
    for record_size in FROZEN_RECORD_SIZES {
        for key_mode in KeyMode::ALL {
            for replication_factor in [1_u16, 3_u16] {
                for ack_mode in AckModeSpec::ALL {
                    for compression in CompressionSpec::ALL {
                        for producer_count in producer_counts {
                            for workload_mode in WorkloadMode::ALL {
                                candidates.push(WorkloadCell {
                                    record_size,
                                    key_mode,
                                    replication_factor,
                                    ack_mode,
                                    compression,
                                    producer_count,
                                    workload_mode,
                                });
                            }
                        }
                    }
                }
            }
        }
    }
    candidates.sort();

    let mut uncovered = required_coverage(manifest);
    let mut selected: Vec<WorkloadCell> = Vec::new();
    while !uncovered.is_empty() {
        let best = candidates
            .iter()
            .filter(|candidate| !selected.contains(candidate))
            .map(|candidate| {
                let score = candidate.coverage().intersection(&uncovered).count();
                (score, candidate)
            })
            .max_by(|(left_score, left), (right_score, right)| {
                left_score
                    .cmp(right_score)
                    // Reverse the deterministic tie-break because max_by keeps
                    // the greater element and we want the lexicographically
                    // smallest candidate for equal coverage.
                    .then_with(|| right.cmp(left))
            })
            .filter(|(score, _)| *score > 0)
            .map(|(_, candidate)| (*candidate).clone())
            .ok_or(BenchmarkError::Invalid(
                "frozen workload dimensions cannot cover required matrix",
            ))?;
        for item in best.coverage() {
            uncovered.remove(&item);
        }
        selected.push(best);
    }
    validate_coverage_matrix(manifest, &selected)?;
    Ok(selected)
}

pub fn validate_coverage_matrix(
    manifest: &BenchmarkManifest,
    cells: &[WorkloadCell],
) -> Result<()> {
    manifest.validate()?;
    if cells.is_empty() {
        return Err(BenchmarkError::Invalid(
            "benchmark workload matrix is empty",
        ));
    }
    let mut covered = BTreeSet::new();
    let mut identities = BTreeSet::new();
    for cell in cells {
        if !FROZEN_RECORD_SIZES.contains(&cell.record_size)
            || ![1_u16, 3_u16].contains(&cell.replication_factor)
            || ![1_usize, manifest.multi_producer_count].contains(&cell.producer_count)
        {
            return Err(BenchmarkError::Invalid(
                "benchmark matrix contains an unsupported factor level",
            ));
        }
        if !identities.insert(cell.id()) {
            return Err(BenchmarkError::Invalid(
                "benchmark matrix contains duplicate workload cells",
            ));
        }
        covered.extend(cell.coverage());
    }
    let missing = required_coverage(manifest)
        .difference(&covered)
        .cloned()
        .collect::<Vec<_>>();
    if !missing.is_empty() {
        return Err(BenchmarkError::InvalidOwned(format!(
            "benchmark matrix misses frozen coverage items: {}",
            missing.join(",")
        )));
    }
    Ok(())
}

fn required_coverage(manifest: &BenchmarkManifest) -> BTreeSet<String> {
    let mut required = BTreeSet::new();
    for size in FROZEN_RECORD_SIZES {
        required.insert(format!("level:size:{size}"));
        for compression in CompressionSpec::ALL {
            required.insert(format!(
                "pair:size-compression:{size}:{}",
                compression.as_str()
            ));
        }
    }
    for key in KeyMode::ALL {
        required.insert(format!("level:key:{}", key.as_str()));
    }
    for rf in [1_u16, 3_u16] {
        required.insert(format!("level:rf:{rf}"));
        for ack in AckModeSpec::ALL {
            required.insert(format!("pair:rf-ack:{rf}:{}", ack.as_str()));
        }
    }
    for ack in AckModeSpec::ALL {
        required.insert(format!("level:ack:{}", ack.as_str()));
    }
    for compression in CompressionSpec::ALL {
        required.insert(format!("level:compression:{}", compression.as_str()));
        for mode in WorkloadMode::ALL {
            required.insert(format!(
                "pair:compression-mode:{}:{}",
                compression.as_str(),
                mode.as_str()
            ));
        }
    }
    for producers in [1_usize, manifest.multi_producer_count] {
        required.insert(format!("level:producers:{producers}"));
        for mode in WorkloadMode::ALL {
            required.insert(format!("pair:producers-mode:{producers}:{}", mode.as_str()));
        }
    }
    for mode in WorkloadMode::ALL {
        required.insert(format!("level:mode:{}", mode.as_str()));
    }
    required
}

pub(crate) fn parse_key_mode(value: &str) -> Result<KeyMode> {
    match value {
        "keyed" => Ok(KeyMode::Keyed),
        "unkeyed" => Ok(KeyMode::Unkeyed),
        _ => Err(BenchmarkError::Invalid("unknown benchmark key mode")),
    }
}

pub(crate) fn parse_compression(value: &str) -> Result<CompressionSpec> {
    match value {
        "none" => Ok(CompressionSpec::None),
        "lz4" => Ok(CompressionSpec::Lz4),
        "zstd" => Ok(CompressionSpec::Zstd),
        _ => Err(BenchmarkError::Invalid("unknown benchmark compression")),
    }
}

pub(crate) fn parse_ack_mode(value: &str) -> Result<AckModeSpec> {
    match value {
        "leader" => Ok(AckModeSpec::Leader),
        "quorum" => Ok(AckModeSpec::Quorum),
        _ => Err(BenchmarkError::Invalid("unknown benchmark ack mode")),
    }
}

pub(crate) fn parse_workload_mode(value: &str) -> Result<WorkloadMode> {
    match value {
        "producer_only" => Ok(WorkloadMode::ProducerOnly),
        "backlog_consumer" => Ok(WorkloadMode::BacklogConsumer),
        "simultaneous" => Ok(WorkloadMode::Simultaneous),
        _ => Err(BenchmarkError::Invalid("unknown benchmark workload mode")),
    }
}

#[cfg(test)]
mod tests {
    use std::path::PathBuf;

    use super::*;

    fn manifest() -> BenchmarkManifest {
        BenchmarkManifest {
            benchmark_id: "matrix".into(),
            repository_root: PathBuf::from("."),
            bootstrap_endpoint: "127.0.0.1:9092".into(),
            topic_prefix: "p8bench".into(),
            partition_count: 6,
            multi_producer_count: 4,
            warmup_ms: 10,
            measurement_ms: 20,
            repeats: 3,
            backlog_records: 100,
            batch_max_records: 128,
            batch_max_bytes: 1024 * 1024,
            batch_linger_ms: 5,
            request_timeout_ms: 30_000,
            fetch_max_bytes: 1024 * 1024,
            fetch_max_records: 1_000,
            max_latency_samples: 100_000,
            seed: 1,
            host_kernel: "kernel".into(),
            host_cpu_model: "cpu".into(),
            host_ram_bytes: 1024,
            host_disk: "disk".into(),
            host_filesystem: "fs".into(),
            network_topology: "network".into(),
            metrics_command: "metrics".into(),
        }
    }

    #[test]
    fn p8h_coverage_matrix_is_deterministic_and_covers_every_frozen_pair() {
        let manifest = manifest();
        let first = coverage_matrix(&manifest).unwrap();
        let second = coverage_matrix(&manifest).unwrap();
        assert_eq!(first, second);
        validate_coverage_matrix(&manifest, &first).unwrap();
        assert!(
            first.len() < 64,
            "matrix unexpectedly exploded: {}",
            first.len()
        );
    }

    #[test]
    fn p8h_matrix_rejects_unsupported_or_incomplete_cells() {
        let manifest = manifest();
        let mut matrix = coverage_matrix(&manifest).unwrap();
        matrix[0].replication_factor = 2;
        assert!(validate_coverage_matrix(&manifest, &matrix).is_err());
        let matrix = coverage_matrix(&manifest).unwrap();
        assert!(validate_coverage_matrix(&manifest, &matrix[..1]).is_err());
    }
}
