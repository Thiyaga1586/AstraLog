use std::{
    env,
    ffi::OsString,
    io::Read,
    process::{Command, Stdio},
    time::Duration,
};

use astralog_client::{AstraClient, ClientConfig, ClientError};
use astralog_common::TopicId;
use astralog_protocol::{
    PublicError, UNKNOWN_BROKER_ID, UNKNOWN_EPOCH,
    api::{
        FetchPartitionRequestV0, FetchRequestV0, IsolationLevel, MetadataResponseV0,
        TopicPerformanceConfigV1, WireRecordV0,
    },
};
use tokio::{task::JoinSet, time};

use crate::{
    BenchmarkError, BenchmarkManifest, BenchmarkProvenance, BenchmarkRun, KeyMode, RawBundle,
    Result, SystemMetricsSnapshot, WorkloadCell, WorkloadMode, coverage_matrix,
};

const METRICS_OUTPUT_LIMIT: u64 = 64 * 1024;

pub fn ensure_release_profile() -> Result<()> {
    ensure_profile(cfg!(debug_assertions))
}

fn ensure_profile(debug_assertions: bool) -> Result<()> {
    if debug_assertions {
        return Err(BenchmarkError::Invalid(
            "P8 benchmark runner refuses debug builds; rebuild and run with --release",
        ));
    }
    Ok(())
}

pub fn validate_runtime_provenance(
    manifest: &BenchmarkManifest,
    provenance: &BenchmarkProvenance,
) -> Result<()> {
    manifest.validate()?;
    provenance.validate()?;
    if provenance.host_kernel != manifest.host_kernel
        || provenance.host_cpu_model != manifest.host_cpu_model
        || provenance.host_ram_bytes != manifest.host_ram_bytes
        || provenance.host_disk != manifest.host_disk
        || provenance.host_filesystem != manifest.host_filesystem
        || provenance.network_topology != manifest.network_topology
    {
        return Err(BenchmarkError::Invalid(
            "runtime provenance does not match benchmark manifest host declaration",
        ));
    }
    Ok(())
}

pub async fn run_benchmark(manifest: BenchmarkManifest) -> Result<RawBundle> {
    ensure_release_profile()?;
    manifest.validate()?;
    let matrix = coverage_matrix(&manifest)?;
    let provenance = capture_provenance(&manifest)?;
    validate_runtime_provenance(&manifest, &provenance)?;

    let mut config = ClientConfig::new(manifest.bootstrap_endpoint.clone());
    config.client_id = format!("astralog-bench-{}", manifest.benchmark_id);
    config.request_timeout = Duration::from_millis(manifest.request_timeout_ms);
    config.batch_max_records = manifest.batch_max_records;
    config.batch_max_bytes = manifest.batch_max_bytes;
    config.batch_linger = Duration::from_millis(manifest.batch_linger_ms);
    config.producer_delivery_timeout = Duration::from_millis(manifest.request_timeout_ms);
    let client = AstraClient::connect(config).await?;

    let mut runs = Vec::with_capacity(
        matrix
            .len()
            .checked_mul(manifest.repeats as usize)
            .ok_or(BenchmarkError::Invalid("benchmark run count overflow"))?,
    );
    for cell in &matrix {
        for repeat in 1..=manifest.repeats {
            runs.push(run_cell(&client, &manifest, cell, repeat).await?);
        }
    }
    client.close().await;

    let bundle = RawBundle {
        manifest,
        provenance,
        matrix,
        runs,
    };
    bundle.validate()?;
    Ok(bundle)
}

async fn run_cell(
    client: &AstraClient,
    manifest: &BenchmarkManifest,
    cell: &WorkloadCell,
    repeat: u32,
) -> Result<BenchmarkRun> {
    let topic_stem = format!("{}-{}-r{repeat}", manifest.topic_prefix, cell.id());
    let performance = TopicPerformanceConfigV1 {
        compression: cell.compression.protocol(),
        max_record_bytes: u32::try_from(cell.record_size)
            .map_err(|_| BenchmarkError::Invalid("benchmark max_record_bytes overflow"))?,
        max_batch_records: u32::try_from(manifest.batch_max_records)
            .map_err(|_| BenchmarkError::Invalid("benchmark batch record count overflow"))?,
        max_batch_uncompressed_bytes: u32::try_from(manifest.batch_max_bytes)
            .map_err(|_| BenchmarkError::Invalid("benchmark batch byte count overflow"))?,
    };
    performance.validate()?;

    match cell.workload_mode {
        WorkloadMode::BacklogConsumer => {
            let topic_name = bounded_topic_name(&topic_stem)?;
            let topic_id =
                create_and_verify_topic(client, manifest, cell, &topic_name, performance).await?;
            prefill_backlog(client, manifest, cell, topic_id, repeat).await?;
            let context = WorkloadContext {
                client,
                manifest,
                cell,
                topic_id,
                repeat,
            };
            let mut warmup_offsets = vec![0_u64; manifest.partition_count as usize];
            let _ = run_phase(
                &context,
                Duration::from_millis(manifest.warmup_ms),
                false,
                &mut warmup_offsets,
            )
            .await?;
            let mut measurement_offsets = vec![0_u64; manifest.partition_count as usize];
            let start_metrics = snapshot_metrics(manifest.metrics_command.clone()).await?;
            let started = time::Instant::now();
            let phase = run_phase(
                &context,
                Duration::from_millis(manifest.measurement_ms),
                true,
                &mut measurement_offsets,
            )
            .await?;
            let elapsed = started.elapsed();
            let end_metrics = snapshot_metrics(manifest.metrics_command.clone()).await?;
            client.delete_topic(topic_name.clone()).await?;
            build_run(
                cell,
                repeat,
                RunMeasurement {
                    topic_name,
                    topic_id,
                    elapsed,
                    start_metrics,
                    end_metrics,
                    phase,
                },
            )
        }
        WorkloadMode::ProducerOnly | WorkloadMode::Simultaneous => {
            let warmup_name = bounded_topic_name(&format!("{topic_stem}-warmup"))?;
            let warmup_topic =
                create_and_verify_topic(client, manifest, cell, &warmup_name, performance).await?;
            let warmup_context = WorkloadContext {
                client,
                manifest,
                cell,
                topic_id: warmup_topic,
                repeat,
            };
            let mut warmup_offsets = vec![0_u64; manifest.partition_count as usize];
            let _ = run_phase(
                &warmup_context,
                Duration::from_millis(manifest.warmup_ms),
                false,
                &mut warmup_offsets,
            )
            .await?;
            client.delete_topic(warmup_name).await?;

            let topic_name = bounded_topic_name(&topic_stem)?;
            let topic_id =
                create_and_verify_topic(client, manifest, cell, &topic_name, performance).await?;
            let context = WorkloadContext {
                client,
                manifest,
                cell,
                topic_id,
                repeat,
            };
            let mut offsets = vec![0_u64; manifest.partition_count as usize];
            let start_metrics = snapshot_metrics(manifest.metrics_command.clone()).await?;
            let started = time::Instant::now();
            let phase = run_phase(
                &context,
                Duration::from_millis(manifest.measurement_ms),
                true,
                &mut offsets,
            )
            .await?;
            let elapsed = started.elapsed();
            let end_metrics = snapshot_metrics(manifest.metrics_command.clone()).await?;
            client.delete_topic(topic_name.clone()).await?;
            build_run(
                cell,
                repeat,
                RunMeasurement {
                    topic_name,
                    topic_id,
                    elapsed,
                    start_metrics,
                    end_metrics,
                    phase,
                },
            )
        }
    }
}

struct RunMeasurement {
    topic_name: String,
    topic_id: TopicId,
    elapsed: Duration,
    start_metrics: SystemMetricsSnapshot,
    end_metrics: SystemMetricsSnapshot,
    phase: PhaseAccumulator,
}

fn build_run(
    cell: &WorkloadCell,
    repeat: u32,
    measurement: RunMeasurement,
) -> Result<BenchmarkRun> {
    measurement
        .end_metrics
        .validate_delta_from(measurement.start_metrics)?;
    let run = BenchmarkRun {
        cell: cell.clone(),
        repeat,
        topic_name: measurement.topic_name,
        topic_id_hex: topic_id_hex(measurement.topic_id),
        measurement_elapsed_ns: u64::try_from(measurement.elapsed.as_nanos())
            .unwrap_or(u64::MAX)
            .max(1),
        produced_events: measurement.phase.produced_events,
        fetched_events: measurement.phase.fetched_events,
        logical_bytes: measurement.phase.logical_bytes,
        producer_latency_ns: measurement.phase.producer_latency_ns,
        fetch_latency_ns: measurement.phase.fetch_latency_ns,
        start_metrics: measurement.start_metrics,
        end_metrics: measurement.end_metrics,
        error_count: measurement.phase.error_count,
        throttle_count: measurement.phase.throttle_count,
    };
    Ok(run)
}

struct WorkloadContext<'a> {
    client: &'a AstraClient,
    manifest: &'a BenchmarkManifest,
    cell: &'a WorkloadCell,
    topic_id: TopicId,
    repeat: u32,
}

async fn create_and_verify_topic(
    client: &AstraClient,
    manifest: &BenchmarkManifest,
    cell: &WorkloadCell,
    topic_name: &str,
    performance: TopicPerformanceConfigV1,
) -> Result<TopicId> {
    let created = client
        .create_topic_with_performance(
            topic_name.to_owned(),
            manifest.partition_count,
            cell.replication_factor,
            performance,
        )
        .await?;
    if created.performance != performance {
        return Err(BenchmarkError::Invalid(
            "committed topic performance policy differs from requested benchmark cell",
        ));
    }
    let described = client
        .describe_topic_with_performance(topic_name.to_owned())
        .await?;
    if described.performance != performance || described.topic_id != created.topic_id {
        return Err(BenchmarkError::Invalid(
            "described topic policy/identity differs from committed benchmark topic",
        ));
    }
    wait_for_topic_readiness(client, manifest, cell, created.topic_id).await?;
    Ok(created.topic_id)
}

async fn wait_for_topic_readiness(
    client: &AstraClient,
    manifest: &BenchmarkManifest,
    cell: &WorkloadCell,
    topic_id: TopicId,
) -> Result<()> {
    let deadline = time::Instant::now() + Duration::from_millis(manifest.request_timeout_ms);
    loop {
        let refresh_error = match client.refresh_metadata().await {
            Ok(metadata) => {
                if topic_metadata_is_ready(
                    &metadata,
                    topic_id,
                    manifest.partition_count,
                    cell.replication_factor,
                ) {
                    return Ok(());
                }
                None
            }
            Err(error) => Some(error.to_string()),
        };
        if time::Instant::now() >= deadline {
            let suffix = refresh_error
                .as_deref()
                .map(|error| format!("; last metadata refresh error: {error}"))
                .unwrap_or_default();
            return Err(BenchmarkError::InvalidOwned(format!(
                "benchmark topic did not reach concrete P3 leader readiness before the request timeout{suffix}"
            )));
        }
        time::sleep(Duration::from_millis(20)).await;
    }
}

fn topic_metadata_is_ready(
    metadata: &MetadataResponseV0,
    topic_id: TopicId,
    expected_partitions: u32,
    expected_replication_factor: u16,
) -> bool {
    let Some(topic) = metadata
        .topics
        .iter()
        .find(|topic| topic.topic_id == topic_id)
    else {
        return false;
    };
    topic.topic_error == PublicError::None as u16
        && topic.partition_count == expected_partitions
        && topic.replication_factor == expected_replication_factor
        && topic.partitions.len() == expected_partitions as usize
        && topic.partitions.iter().all(|partition| {
            partition.partition_error == PublicError::None as u16
                && partition.leader_broker_id != UNKNOWN_BROKER_ID
                && partition.leader_epoch != UNKNOWN_EPOCH
                && partition.replica_broker_ids.len() == expected_replication_factor as usize
        })
}

async fn prefill_backlog(
    client: &AstraClient,
    manifest: &BenchmarkManifest,
    cell: &WorkloadCell,
    topic_id: TopicId,
    repeat: u32,
) -> Result<()> {
    let batch_records = records_per_batch(manifest, cell)?;
    let mut ordinal = 0_u64;
    while ordinal < manifest.backlog_records {
        let remaining = manifest.backlog_records - ordinal;
        let count = usize::try_from(remaining.min(batch_records as u64)).unwrap_or(batch_records);
        let (partition, records) = generated_batch(manifest, cell, repeat, 0, ordinal, count)?;
        match client
            .produce_partition(
                topic_id,
                partition,
                UNKNOWN_EPOCH,
                records,
                cell.ack_mode.protocol(),
            )
            .await
        {
            Ok(_) => ordinal = ordinal.saturating_add(count as u64),
            Err(ClientError::Broker(PublicError::BrokerBusy)) => continue,
            Err(error) => return Err(error.into()),
        }
    }
    Ok(())
}

async fn run_phase(
    context: &WorkloadContext<'_>,
    duration: Duration,
    retain_measurement: bool,
    fetch_offsets: &mut [u64],
) -> Result<PhaseAccumulator> {
    let deadline = time::Instant::now() + duration;
    let mut accumulator =
        PhaseAccumulator::new(retain_measurement, context.manifest.max_latency_samples);
    let mut operation = 0_u64;
    while time::Instant::now() < deadline {
        match context.cell.workload_mode {
            WorkloadMode::ProducerOnly => {
                run_producer_cycle(context, operation, &mut accumulator).await?;
            }
            WorkloadMode::BacklogConsumer => {
                run_fetch_cycle(context, operation, fetch_offsets, &mut accumulator).await?;
            }
            WorkloadMode::Simultaneous => {
                let mut producer_phase =
                    PhaseAccumulator::new(true, context.manifest.max_latency_samples);
                let mut fetch_phase =
                    PhaseAccumulator::new(true, context.manifest.max_latency_samples);
                let mut offsets = fetch_offsets.to_vec();
                let producer = run_producer_cycle(context, operation, &mut producer_phase);
                let fetch = run_fetch_cycle(context, operation, &mut offsets, &mut fetch_phase);
                let (producer_result, fetch_result) = tokio::join!(producer, fetch);
                producer_result?;
                fetch_result?;
                accumulator.merge(producer_phase)?;
                fetch_offsets.copy_from_slice(&offsets);
                accumulator.merge(fetch_phase)?;
            }
        }
        operation = operation.saturating_add(1);
    }
    Ok(accumulator)
}

async fn run_producer_cycle(
    context: &WorkloadContext<'_>,
    operation: u64,
    accumulator: &mut PhaseAccumulator,
) -> Result<()> {
    let count = records_per_batch(context.manifest, context.cell)?;
    let mut tasks = JoinSet::new();
    for producer in 0..context.cell.producer_count {
        let client = context.client.clone();
        let manifest = context.manifest.clone();
        let cell = context.cell.clone();
        let topic_id = context.topic_id;
        let repeat = context.repeat;
        tasks.spawn(async move {
            let base = operation
                .saturating_mul(cell.producer_count as u64)
                .saturating_add(producer as u64)
                .saturating_mul(count as u64);
            let (partition, records) =
                generated_batch(&manifest, &cell, repeat, producer as u64, base, count)?;
            let started = time::Instant::now();
            let result = client
                .produce_partition(
                    topic_id,
                    partition,
                    UNKNOWN_EPOCH,
                    records,
                    cell.ack_mode.protocol(),
                )
                .await;
            let latency = u64::try_from(started.elapsed().as_nanos())
                .unwrap_or(u64::MAX)
                .max(1);
            Ok::<_, BenchmarkError>((result, latency, count as u64))
        });
    }
    while let Some(joined) = tasks.join_next().await {
        let (result, latency, events) = joined.map_err(|error| {
            BenchmarkError::InvalidOwned(format!("benchmark producer task failed: {error}"))
        })??;
        match result {
            Ok(_) => accumulator.record_produce(
                events,
                events.saturating_mul(context.cell.record_size as u64),
                latency,
            )?,
            Err(ClientError::Broker(PublicError::BrokerBusy)) => accumulator.record_throttle(),
            Err(error) => return Err(error.into()),
        }
    }
    Ok(())
}

async fn run_fetch_cycle(
    context: &WorkloadContext<'_>,
    operation: u64,
    offsets: &mut [u64],
    accumulator: &mut PhaseAccumulator,
) -> Result<()> {
    let partition = u32::try_from(operation % u64::from(context.manifest.partition_count))
        .map_err(|_| BenchmarkError::Invalid("benchmark partition selection overflow"))?;
    let index = partition as usize;
    let request = FetchRequestV0 {
        isolation: IsolationLevel::ReadCommitted,
        min_bytes: 0,
        max_wait_ms: 0,
        partitions: vec![FetchPartitionRequestV0 {
            topic_id: context.topic_id,
            partition,
            leader_epoch: UNKNOWN_EPOCH,
            fetch_offset: offsets[index],
            max_bytes: context.manifest.fetch_max_bytes,
            max_records: context.manifest.fetch_max_records,
        }],
        tags: Vec::new(),
    };
    let started = time::Instant::now();
    match context.client.fetch(request).await {
        Ok(response) => {
            let latency = u64::try_from(started.elapsed().as_nanos())
                .unwrap_or(u64::MAX)
                .max(1);
            let partition = response.partitions.first().ok_or(BenchmarkError::Invalid(
                "benchmark Fetch returned no partition",
            ))?;
            let fetched = partition.records.len() as u64;
            offsets[index] = partition.next_offset;
            accumulator.record_fetch(
                fetched,
                fetched.saturating_mul(context.cell.record_size as u64),
                latency,
            )?;
        }
        Err(ClientError::Broker(PublicError::BrokerBusy)) => accumulator.record_throttle(),
        Err(error) => return Err(error.into()),
    }
    Ok(())
}

fn records_per_batch(manifest: &BenchmarkManifest, cell: &WorkloadCell) -> Result<usize> {
    let by_bytes = manifest.batch_max_bytes / cell.record_size;
    let count = manifest.batch_max_records.min(by_bytes);
    if count == 0 {
        return Err(BenchmarkError::Invalid(
            "benchmark cell cannot fit one record in configured batch",
        ));
    }
    Ok(count)
}

fn generated_batch(
    manifest: &BenchmarkManifest,
    cell: &WorkloadCell,
    repeat: u32,
    producer: u64,
    base_ordinal: u64,
    count: usize,
) -> Result<(u32, Vec<WireRecordV0>)> {
    let seed = workload_seed(manifest.seed, cell, repeat, producer, base_ordinal);
    let mut rng = DeterministicRng::new(seed);
    let partition = u32::try_from(rng.next_u64() % u64::from(manifest.partition_count))
        .map_err(|_| BenchmarkError::Invalid("benchmark partition conversion overflow"))?;
    let mut records = Vec::with_capacity(count);
    for ordinal in 0..count {
        let key = match cell.key_mode {
            KeyMode::Keyed => Some(rng.next_u64().to_le_bytes().to_vec()),
            KeyMode::Unkeyed => None,
        };
        let mut value = vec![0_u8; cell.record_size];
        for chunk in value.chunks_mut(8) {
            let bytes = rng.next_u64().to_le_bytes();
            let len = chunk.len();
            chunk.copy_from_slice(&bytes[..len]);
        }
        records.push(WireRecordV0 {
            timestamp_ms: i64::try_from(base_ordinal.saturating_add(ordinal as u64))
                .unwrap_or(i64::MAX),
            key,
            value: Some(value),
            headers: Vec::new(),
        });
    }
    Ok((partition, records))
}

fn workload_seed(root: u64, cell: &WorkloadCell, repeat: u32, producer: u64, ordinal: u64) -> u64 {
    let mut hash = 0xcbf29ce484222325_u64 ^ root;
    for byte in cell.id().bytes() {
        hash ^= u64::from(byte);
        hash = hash.wrapping_mul(0x100000001b3);
    }
    hash ^ u64::from(repeat).rotate_left(7) ^ producer.rotate_left(17) ^ ordinal.rotate_left(29)
}

#[derive(Debug, Clone, Copy)]
struct DeterministicRng(u64);

impl DeterministicRng {
    fn new(seed: u64) -> Self {
        Self(seed.max(1))
    }

    fn next_u64(&mut self) -> u64 {
        let mut value = self.0;
        value ^= value << 13;
        value ^= value >> 7;
        value ^= value << 17;
        self.0 = value;
        value
    }
}

#[derive(Debug)]
struct PhaseAccumulator {
    retain_measurement: bool,
    max_samples: usize,
    produced_events: u64,
    fetched_events: u64,
    logical_bytes: u64,
    producer_latency_ns: Vec<u64>,
    fetch_latency_ns: Vec<u64>,
    error_count: u64,
    throttle_count: u64,
}

impl PhaseAccumulator {
    fn new(retain_measurement: bool, max_samples: usize) -> Self {
        Self {
            retain_measurement,
            max_samples,
            produced_events: 0,
            fetched_events: 0,
            logical_bytes: 0,
            producer_latency_ns: Vec::new(),
            fetch_latency_ns: Vec::new(),
            error_count: 0,
            throttle_count: 0,
        }
    }

    fn record_produce(&mut self, events: u64, bytes: u64, latency_ns: u64) -> Result<()> {
        if !self.retain_measurement {
            return Ok(());
        }
        push_bounded(&mut self.producer_latency_ns, latency_ns, self.max_samples)?;
        self.produced_events = self.produced_events.saturating_add(events);
        self.logical_bytes = self.logical_bytes.saturating_add(bytes);
        Ok(())
    }

    fn record_fetch(&mut self, events: u64, bytes: u64, latency_ns: u64) -> Result<()> {
        if !self.retain_measurement {
            return Ok(());
        }
        push_bounded(&mut self.fetch_latency_ns, latency_ns, self.max_samples)?;
        self.fetched_events = self.fetched_events.saturating_add(events);
        self.logical_bytes = self.logical_bytes.saturating_add(bytes);
        Ok(())
    }

    fn record_throttle(&mut self) {
        if self.retain_measurement {
            self.error_count = self.error_count.saturating_add(1);
            self.throttle_count = self.throttle_count.saturating_add(1);
        }
    }

    fn merge(&mut self, other: Self) -> Result<()> {
        if !self.retain_measurement {
            return Ok(());
        }
        if self
            .producer_latency_ns
            .len()
            .saturating_add(other.producer_latency_ns.len())
            > self.max_samples
            || self
                .fetch_latency_ns
                .len()
                .saturating_add(other.fetch_latency_ns.len())
                > self.max_samples
        {
            return Err(BenchmarkError::Invalid(
                "benchmark latency sample hard bound exceeded",
            ));
        }
        self.produced_events = self.produced_events.saturating_add(other.produced_events);
        self.fetched_events = self.fetched_events.saturating_add(other.fetched_events);
        self.logical_bytes = self.logical_bytes.saturating_add(other.logical_bytes);
        self.producer_latency_ns.extend(other.producer_latency_ns);
        self.fetch_latency_ns.extend(other.fetch_latency_ns);
        self.error_count = self.error_count.saturating_add(other.error_count);
        self.throttle_count = self.throttle_count.saturating_add(other.throttle_count);
        Ok(())
    }
}

fn push_bounded(values: &mut Vec<u64>, value: u64, max: usize) -> Result<()> {
    if values.len() >= max {
        return Err(BenchmarkError::Invalid(
            "benchmark latency sample hard bound exceeded",
        ));
    }
    values.push(value.max(1));
    Ok(())
}

fn capture_provenance(manifest: &BenchmarkManifest) -> Result<BenchmarkProvenance> {
    let worktree_status = command_text_allow_empty(
        Command::new("git")
            .arg("-C")
            .arg(&manifest.repository_root)
            .args(["status", "--porcelain=v1", "--untracked-files=normal"]),
    )?;
    ensure_clean_worktree(&worktree_status)?;
    let git_sha = command_text(
        Command::new("git")
            .arg("-C")
            .arg(&manifest.repository_root)
            .args(["rev-parse", "HEAD"]),
    )?;
    let rustc_version = command_text(Command::new("rustc").arg("--version"))?;
    let rustflags = env::var("RUSTFLAGS")
        .or_else(|_| env::var("CARGO_ENCODED_RUSTFLAGS"))
        .unwrap_or_default();
    let provenance = BenchmarkProvenance {
        git_sha,
        rustc_version,
        build_profile: "release".into(),
        rustflags,
        target_os: env::consts::OS.into(),
        target_arch: env::consts::ARCH.into(),
        host_os: env::consts::OS.into(),
        host_kernel: manifest.host_kernel.clone(),
        host_cpu_model: manifest.host_cpu_model.clone(),
        host_core_count: std::thread::available_parallelism()
            .map(usize::from)
            .unwrap_or(1),
        host_ram_bytes: manifest.host_ram_bytes,
        host_disk: manifest.host_disk.clone(),
        host_filesystem: manifest.host_filesystem.clone(),
        network_topology: manifest.network_topology.clone(),
    };
    provenance.validate()?;
    Ok(provenance)
}

fn ensure_clean_worktree(status: &str) -> Result<()> {
    if !status.trim().is_empty() {
        return Err(BenchmarkError::Invalid(
            "benchmark provenance requires a clean git worktree so the exact SHA identifies the built source",
        ));
    }
    Ok(())
}

fn command_text(command: &mut Command) -> Result<String> {
    let output = command.output()?;
    if !output.status.success() {
        return Err(BenchmarkError::InvalidOwned(format!(
            "benchmark provenance command failed with {}",
            output.status
        )));
    }
    let value = String::from_utf8(output.stdout)
        .map_err(|_| BenchmarkError::Invalid("benchmark provenance output is not UTF-8"))?;
    let value = value.trim().to_owned();
    if value.is_empty() {
        return Err(BenchmarkError::Invalid(
            "benchmark provenance command returned empty output",
        ));
    }
    Ok(value)
}

fn command_text_allow_empty(command: &mut Command) -> Result<String> {
    let output = command.output()?;
    if !output.status.success() {
        return Err(BenchmarkError::InvalidOwned(format!(
            "benchmark provenance command failed with {}",
            output.status
        )));
    }
    String::from_utf8(output.stdout)
        .map(|value| value.trim().to_owned())
        .map_err(|_| BenchmarkError::Invalid("benchmark provenance output is not UTF-8"))
}

async fn snapshot_metrics(command: String) -> Result<SystemMetricsSnapshot> {
    tokio::task::spawn_blocking(move || snapshot_metrics_blocking(&command))
        .await
        .map_err(|error| {
            BenchmarkError::InvalidOwned(format!("metrics task join failed: {error}"))
        })?
}

fn snapshot_metrics_blocking(command: &str) -> Result<SystemMetricsSnapshot> {
    let parts = split_command(command)?;
    let (program, args) = parts
        .split_first()
        .ok_or(BenchmarkError::Invalid("metrics command is empty"))?;
    let mut child = Command::new(program)
        .args(args)
        .stdout(Stdio::piped())
        .stderr(Stdio::null())
        .spawn()?;
    let mut stdout = child.stdout.take().ok_or(BenchmarkError::Invalid(
        "metrics command stdout is unavailable",
    ))?;
    let mut bounded = stdout.by_ref().take(METRICS_OUTPUT_LIMIT + 1);
    let mut bytes = Vec::new();
    bounded.read_to_end(&mut bytes)?;
    let status = child.wait()?;
    if !status.success() {
        return Err(BenchmarkError::InvalidOwned(format!(
            "metrics command failed with {status}"
        )));
    }
    if bytes.len() as u64 > METRICS_OUTPUT_LIMIT {
        return Err(BenchmarkError::Invalid(
            "metrics command output exceeds 64 KiB hard bound",
        ));
    }
    let text = String::from_utf8(bytes)
        .map_err(|_| BenchmarkError::Invalid("metrics command output is not UTF-8"))?;
    SystemMetricsSnapshot::parse(&text)
}

fn split_command(command: &str) -> Result<Vec<OsString>> {
    let mut args = Vec::new();
    let mut current = String::new();
    let mut quoted = false;
    let mut escaped = false;
    for ch in command.chars() {
        if escaped {
            current.push(ch);
            escaped = false;
        } else if ch == '\\' {
            escaped = true;
        } else if ch == '"' {
            quoted = !quoted;
        } else if ch.is_whitespace() && !quoted {
            if !current.is_empty() {
                args.push(OsString::from(std::mem::take(&mut current)));
            }
        } else {
            current.push(ch);
        }
    }
    if escaped || quoted {
        return Err(BenchmarkError::Invalid(
            "metrics command has an unterminated escape or quote",
        ));
    }
    if !current.is_empty() {
        args.push(OsString::from(current));
    }
    if args.is_empty() {
        return Err(BenchmarkError::Invalid("metrics command is empty"));
    }
    Ok(args)
}

fn bounded_topic_name(value: &str) -> Result<String> {
    if value.is_empty() || value.len() > 249 {
        return Err(BenchmarkError::Invalid(
            "generated benchmark topic name exceeds Wire V1 bound",
        ));
    }
    Ok(value.to_owned())
}

fn topic_id_hex(topic_id: TopicId) -> String {
    topic_id
        .as_bytes()
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect()
}

#[cfg(test)]
mod tests {
    use std::path::PathBuf;

    use astralog_protocol::api::{BrokerMetadataV0, MetadataPartitionV0, MetadataTopicV0};

    use crate::{AckModeSpec, CompressionSpec};

    use super::*;

    fn manifest() -> BenchmarkManifest {
        BenchmarkManifest {
            benchmark_id: "runner".into(),
            repository_root: PathBuf::from("."),
            bootstrap_endpoint: "127.0.0.1:9092".into(),
            topic_prefix: "p8runner".into(),
            partition_count: 3,
            multi_producer_count: 2,
            warmup_ms: 1,
            measurement_ms: 1,
            repeats: 2,
            backlog_records: 10,
            batch_max_records: 4,
            batch_max_bytes: 64 * 1024,
            batch_linger_ms: 1,
            request_timeout_ms: 1000,
            fetch_max_bytes: 1024,
            fetch_max_records: 10,
            max_latency_samples: 32,
            seed: 99,
            host_kernel: "kernel".into(),
            host_cpu_model: "cpu".into(),
            host_ram_bytes: 1024,
            host_disk: "disk".into(),
            host_filesystem: "fs".into(),
            network_topology: "loopback".into(),
            metrics_command: "metrics".into(),
        }
    }

    fn cell(key_mode: KeyMode) -> WorkloadCell {
        WorkloadCell {
            record_size: 100,
            key_mode,
            replication_factor: 1,
            ack_mode: AckModeSpec::Leader,
            compression: CompressionSpec::None,
            producer_count: 1,
            workload_mode: WorkloadMode::ProducerOnly,
        }
    }

    #[test]
    fn p8h_release_runner_refuses_debug_profile() {
        assert!(ensure_profile(true).is_err());
        assert!(ensure_profile(false).is_ok());
        if cfg!(debug_assertions) {
            assert!(ensure_release_profile().is_err());
        }
    }

    #[test]
    fn p8h_workload_generation_is_seeded_and_reproducible_for_keyed_and_unkeyed() {
        let manifest = manifest();
        for key_mode in KeyMode::ALL {
            let cell = cell(key_mode);
            let first = generated_batch(&manifest, &cell, 1, 0, 0, 4).unwrap();
            let second = generated_batch(&manifest, &cell, 1, 0, 0, 4).unwrap();
            assert_eq!(first, second);
            match key_mode {
                KeyMode::Keyed => assert!(first.1.iter().all(|record| record.key.is_some())),
                KeyMode::Unkeyed => assert!(first.1.iter().all(|record| record.key.is_none())),
            }
        }
    }

    #[test]
    fn p8h_warmup_accumulator_cannot_enter_measurement_samples_or_throughput() {
        let mut warmup = PhaseAccumulator::new(false, 2);
        warmup.record_produce(10, 1000, 10).unwrap();
        warmup.record_fetch(10, 1000, 10).unwrap();
        warmup.record_throttle();
        assert_eq!(warmup.produced_events, 0);
        assert_eq!(warmup.fetched_events, 0);
        assert_eq!(warmup.logical_bytes, 0);
        assert!(warmup.producer_latency_ns.is_empty());
        assert!(warmup.fetch_latency_ns.is_empty());
        assert_eq!(warmup.error_count, 0);
        assert_eq!(warmup.throttle_count, 0);
    }

    #[test]
    fn p8h_latency_retention_has_a_hard_bound() {
        let mut phase = PhaseAccumulator::new(true, 1);
        phase.record_produce(1, 100, 10).unwrap();
        assert!(phase.record_produce(1, 100, 20).is_err());
    }

    #[test]
    fn p8h_runtime_provenance_requires_complete_matching_host_declaration() {
        let manifest = manifest();
        let mut provenance = BenchmarkProvenance {
            git_sha: "a".repeat(40),
            rustc_version: "rustc test".into(),
            build_profile: "release".into(),
            rustflags: String::new(),
            target_os: "test".into(),
            target_arch: "x86_64".into(),
            host_os: "test".into(),
            host_kernel: manifest.host_kernel.clone(),
            host_cpu_model: manifest.host_cpu_model.clone(),
            host_core_count: 4,
            host_ram_bytes: manifest.host_ram_bytes,
            host_disk: manifest.host_disk.clone(),
            host_filesystem: manifest.host_filesystem.clone(),
            network_topology: manifest.network_topology.clone(),
        };
        validate_runtime_provenance(&manifest, &provenance).unwrap();
        provenance.host_disk = "different".into();
        assert!(validate_runtime_provenance(&manifest, &provenance).is_err());
    }

    #[test]
    fn p8h_exact_sha_provenance_rejects_dirty_worktree_state() {
        assert!(ensure_clean_worktree("").is_ok());
        assert!(ensure_clean_worktree(" M crates/astralog-benchmark/src/runner.rs\n").is_err());
        assert!(ensure_clean_worktree("?? untracked.txt\n").is_err());
    }

    #[test]
    fn p8h_metrics_command_parser_is_bounded_and_quote_aware() {
        assert_eq!(
            split_command("metrics --device \"disk 0\"").unwrap(),
            vec![
                OsString::from("metrics"),
                OsString::from("--device"),
                OsString::from("disk 0")
            ]
        );
        assert!(split_command("metrics \"unterminated").is_err());
    }

    #[test]
    fn p8h_benchmark_harness_smoke_is_short_deterministic_and_offline() {
        let manifest = manifest();
        let matrix = coverage_matrix(&manifest).unwrap();
        assert!(!matrix.is_empty());
        assert!(matrix.len() < 64);
        let encoded = manifest.canonical_text().unwrap();
        assert_eq!(BenchmarkManifest::parse(&encoded).unwrap(), manifest);
        let first = generated_batch(&manifest, &matrix[0], 1, 0, 0, 2).unwrap();
        let second = generated_batch(&manifest, &matrix[0], 1, 0, 0, 2).unwrap();
        assert_eq!(first, second);
        assert!(ensure_profile(true).is_err());
    }

    #[test]
    fn p8i_benchmark_topic_readiness_requires_every_partition_replica_and_concrete_leader() {
        let topic_id = TopicId::new([9; 16]);
        let mut metadata = MetadataResponseV0 {
            cluster_id: [1; 16],
            metadata_epoch: 7,
            brokers: vec![BrokerMetadataV0 {
                broker_id: 1,
                node_uuid: [1; 16],
                incarnation_epoch: 1,
                host: "127.0.0.1".into(),
                port: 9092,
            }],
            topics: vec![MetadataTopicV0 {
                topic_error: PublicError::None as u16,
                topic_name: "ready".into(),
                topic_id,
                partition_count: 2,
                replication_factor: 1,
                partitions: vec![
                    MetadataPartitionV0 {
                        partition: 0,
                        partition_error: PublicError::None as u16,
                        leader_broker_id: 1,
                        leader_epoch: 3,
                        replica_broker_ids: vec![1],
                    },
                    MetadataPartitionV0 {
                        partition: 1,
                        partition_error: PublicError::None as u16,
                        leader_broker_id: UNKNOWN_BROKER_ID,
                        leader_epoch: UNKNOWN_EPOCH,
                        replica_broker_ids: vec![1],
                    },
                ],
            }],
            tags: Vec::new(),
        };
        assert!(!topic_metadata_is_ready(&metadata, topic_id, 2, 1));
        metadata.topics[0].partitions[1].leader_broker_id = 1;
        metadata.topics[0].partitions[1].leader_epoch = 4;
        assert!(topic_metadata_is_ready(&metadata, topic_id, 2, 1));
        metadata.topics[0].partitions[1].replica_broker_ids.clear();
        assert!(!topic_metadata_is_ready(&metadata, topic_id, 2, 1));
    }
}
