//! Interview-only, reproducible end-to-end benchmark. Not a release gate.
//! Run: cargo test --release -p astralog-broker --test interview_benchmark -- --nocapture --test-threads=1
use std::{
    collections::BTreeMap,
    fs::{self, OpenOptions},
    io::Write,
    net::{Ipv4Addr, SocketAddr},
    path::{Path, PathBuf},
    process::Command,
    sync::atomic::{AtomicU32, Ordering},
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};

use astralog_broker::{
    BrokerConfig, BrokerError, BrokerHandle, DynamicControllerConfig, DynamicReplicationConfig,
    start,
};
use astralog_client::{AstraClient, ClientConfig};
use astralog_controller::{
    BrokerAuthority, BrokerHeartbeatRequest, BrokerLifecycle, BrokerLivenessState,
    ControllerCommand, ControllerNodeConfig, ControllerNodeHandle, ControllerNodeId,
    ControllerPeerDescriptor, ControllerPeerTransportConfig, ControllerPersistenceConfig,
    ControllerRuntimeConfig, ControllerRuntimeLimits, ControllerState, ControllerTransportLimits,
    CreateTopicSpec, FailureDetectorConfig, FailureDomainPolicy, FeatureRange, HeartbeatLimits,
    start_controller_node,
};
use astralog_protocol::{
    UNKNOWN_BROKER_ID, UNKNOWN_EPOCH,
    api::{AckMode, FetchPartitionRequestV0, FetchRequestV0, IsolationLevel},
};
use astralog_raft::{NodeId, Role, StableConfiguration, TimingConfig};
use tokio::{task::JoinSet, time};

const VALUE_BYTES: usize = 1024;
const WORKERS: usize = 4;
const WARMUP_RECORDS: u64 = 8;

fn temp_dir(label: &str) -> PathBuf {
    let nonce = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    std::env::temp_dir().join(format!(
        "astralog-interview-{label}-{}-{nonce}",
        std::process::id()
    ))
}

fn reserve_addr() -> SocketAddr {
    static NEXT: AtomicU32 = AtomicU32::new(1);
    let slot = NEXT.fetch_add(1, Ordering::Relaxed);
    let pid = std::process::id();
    let listener = std::net::TcpListener::bind((
        Ipv4Addr::new(
            127,
            ((pid >> 8) & 255) as u8,
            (pid & 255) as u8,
            (slot % 250) as u8 + 1,
        ),
        0,
    ))
    .expect("reserve loopback port");
    listener.local_addr().unwrap()
}

fn cid(id: u64) -> ControllerNodeId {
    ControllerNodeId::new(id).unwrap()
}

fn controller_voters() -> StableConfiguration {
    StableConfiguration::new(
        [
            NodeId::new(1).unwrap(),
            NodeId::new(2).unwrap(),
            NodeId::new(3).unwrap(),
        ],
        [],
    )
    .unwrap()
}

fn controller_config(root: &Path, id: u64, addresses: &[SocketAddr; 3]) -> ControllerNodeConfig {
    let directory = root.join(format!("controller-{id}"));
    astralog_controller::initialize_controller_bootstrap(&directory, [5; 16], &controller_voters())
        .unwrap();
    let peers = (1..=3)
        .filter(|peer| *peer != id)
        .map(|peer| {
            (
                cid(peer),
                ControllerPeerDescriptor {
                    controller_id: cid(peer),
                    address: addresses[(peer - 1) as usize],
                },
            )
        })
        .collect();
    let election_timeout = 4 + id;
    ControllerNodeConfig {
        runtime: ControllerRuntimeConfig {
            local_node: cid(id),
            directory,
            static_configuration: controller_voters(),
            controller_feature_ranges: (1..=3)
                .map(|node| (cid(node), FeatureRange::new(1, 4).unwrap()))
                .collect(),
            initial_state: ControllerState::new([5; 16], 1, 1000).unwrap(),
            persistence: ControllerPersistenceConfig::default(),
            timing: TimingConfig::new(1, 5, 10).unwrap(),
            randomized_election_timeout: election_timeout,
            limits: ControllerRuntimeLimits::default(),
            heartbeat_limits: HeartbeatLimits::default(),
            failure_detector: FailureDetectorConfig::default(),
        },
        transport: ControllerPeerTransportConfig {
            local_controller_id: cid(id),
            cluster_id: [5; 16],
            bind_addr: addresses[(id - 1) as usize],
            peers,
            limits: ControllerTransportLimits::default(),
            max_inbound_frames: 1024,
            max_outbound_frames_per_peer: 1024,
            max_outbound_bytes_per_peer: 16 * 1024 * 1024,
            reconnect_min_delay: Duration::from_millis(5),
            reconnect_max_delay: Duration::from_millis(50),
        },
        tick_period: Duration::from_millis(25),
        election_timeout_sample: election_timeout,
        max_control_requests: 128,
    }
}

async fn controller_leader(
    nodes: &BTreeMap<ControllerNodeId, ControllerNodeHandle>,
) -> ControllerNodeId {
    let deadline = Instant::now() + Duration::from_secs(30);
    loop {
        for (id, node) in nodes {
            if node
                .status()
                .await
                .is_ok_and(|status| status.role == Role::Leader && !status.fatal)
            {
                return *id;
            }
        }
        assert!(Instant::now() < deadline, "RF3 controller election timeout");
        time::sleep(Duration::from_millis(25)).await;
    }
}

async fn registered(
    nodes: &BTreeMap<ControllerNodeId, ControllerNodeHandle>,
    cmd: ControllerCommand,
) -> astralog_controller::ApplyOutcome {
    let leader = controller_leader(nodes).await;
    // Dedicated local demo: controller not under failover, so any ambiguous
    // control mutation is a failure, not permission to submit it twice.
    nodes[&leader]
        .mutate(cmd)
        .await
        .expect("controller registration mutation")
}

async fn start_dynamic(
    config: BrokerConfig,
    nodes: &BTreeMap<ControllerNodeId, ControllerNodeHandle>,
) -> BrokerHandle {
    let deadline = Instant::now() + Duration::from_secs(30);
    loop {
        let _ = controller_leader(nodes).await;
        match start(config.clone()).await {
            Ok(broker) => return broker,
            Err(BrokerError::Controller(
                astralog_controller::ControllerError::NotLeader { .. }
                | astralog_controller::ControllerError::Busy(_),
            )) => {}
            Err(error) => panic!("RF3 broker startup: {error}"),
        }
        assert!(
            Instant::now() < deadline,
            "RF3 broker controller admission timeout"
        );
        time::sleep(Duration::from_millis(25)).await;
    }
}

async fn create_rf3_topic(nodes: &BTreeMap<ControllerNodeId, ControllerNodeHandle>, topic: &str) {
    let leader = controller_leader(nodes).await;
    nodes[&leader]
        .create_topic(CreateTopicSpec {
            topic_name: topic.to_string(),
            partition_count: 1,
            replication_factor: 3,
            failure_domain_policy: FailureDomainPolicy::Disabled,
        })
        .await
        .expect("RF3 controller topic creation");
}

async fn wait_for_topic_leader(client: &AstraClient, topic: &str) {
    let deadline = Instant::now() + Duration::from_secs(30);
    loop {
        if let Ok(metadata) = client.refresh_metadata().await
            && metadata.topics.iter().any(|entry| {
                entry.topic_name == topic
                    && entry.partitions.iter().any(|p| {
                        p.leader_broker_id != UNKNOWN_BROKER_ID && p.leader_epoch != UNKNOWN_EPOCH
                    })
            })
        {
            return;
        }
        assert!(
            Instant::now() < deadline,
            "RF3 leader not routable for {topic}"
        );
        time::sleep(Duration::from_millis(25)).await;
    }
}

fn duration_ms() -> u64 {
    std::env::var("ASTRALOG_INTERVIEW_DURATION_MS")
        .ok()
        .and_then(|value| value.parse().ok())
        .unwrap_or(2500)
        .clamp(500, 30_000)
}

fn repeat_count() -> usize {
    std::env::var("ASTRALOG_INTERVIEW_REPEATS")
        .ok()
        .and_then(|value| value.parse().ok())
        .unwrap_or(2_usize)
        .clamp(1, 5)
}

fn percentile(samples: &[u64], percent: usize) -> f64 {
    if samples.is_empty() {
        return 0.0;
    }
    let mut sorted = samples.to_vec();
    sorted.sort_unstable();
    let rank = (sorted.len() * percent).div_ceil(100).max(1);
    sorted[rank - 1] as f64 / 1_000_000.0
}

fn git_sha() -> String {
    Command::new("git")
        .args(["rev-parse", "HEAD"])
        .output()
        .ok()
        .filter(|out| out.status.success())
        .map(|out| String::from_utf8_lossy(&out.stdout).trim().to_string())
        .unwrap_or_else(|| "unknown".into())
}

fn git_dirty() -> String {
    Command::new("git")
        .args(["status", "--porcelain"])
        .output()
        .ok()
        .filter(|out| out.status.success())
        .map(|out| (!out.stdout.is_empty()).to_string())
        .unwrap_or_else(|| "unknown".into())
}

fn build_profile() -> &'static str {
    if cfg!(debug_assertions) {
        "debug"
    } else {
        "release"
    }
}

struct WorkloadResult {
    records: usize,
    elapsed: Duration,
    samples: Vec<u64>,
    errors: Vec<String>,
    // Identity is (worker key, monotonically increasing sequence). Preserve
    // the full payload so Fetch must prove contents, not merely record count.
    expected_records: BTreeMap<(u8, u64), Vec<u8>>,
}

impl WorkloadResult {
    fn throughput(&self) -> f64 {
        self.records as f64 / self.elapsed.as_secs_f64()
    }
    fn mib_per_sec(&self) -> f64 {
        self.throughput() * VALUE_BYTES as f64 / 1_048_576.0
    }
}

async fn produce_workload(
    client: &AstraClient,
    topic: &str,
    ack: AckMode,
    buffered: bool,
) -> WorkloadResult {
    let deadline = Instant::now() + Duration::from_millis(duration_ms());
    let started = Instant::now();
    let mut tasks = JoinSet::new();
    for worker in 0..WORKERS {
        let client = client.clone();
        let topic = topic.to_string();
        tasks.spawn(async move {
            let mut samples = Vec::new();
            let mut errors = Vec::new();
            let mut expected_records = BTreeMap::new();
            // Deterministic incompressible-looking bytes. Default topic
            // compression is None, but avoid a synthetic all-identical value.
            let mut state = (worker as u64 + 1) * 0x9e37_79b9;
            let mut payload = vec![0_u8; VALUE_BYTES];
            for byte in &mut payload {
                state ^= state << 13;
                state ^= state >> 7;
                state ^= state << 17;
                *byte = state as u8;
            }
            let key = vec![worker as u8; 4];
            let mut sequence = 0_u64;
            while Instant::now() < deadline {
                payload[..8].copy_from_slice(&sequence.to_le_bytes());
                sequence += 1;
                let one_started = Instant::now();
                let outcome = if buffered {
                    client
                        .produce_buffered(
                            &topic,
                            Some(key.clone()),
                            Some(payload.clone()),
                            Vec::new(),
                            ack,
                        )
                        .await
                } else {
                    client
                        .produce(
                            &topic,
                            Some(key.clone()),
                            Some(payload.clone()),
                            Vec::new(),
                            ack,
                        )
                        .await
                };
                match outcome {
                    Ok(_) => {
                        samples.push(u64::try_from(one_started.elapsed().as_nanos()).unwrap());
                        expected_records.insert((worker as u8, sequence - 1), payload.clone());
                    }
                    Err(error) => {
                        // Non-idempotent writes may have committed. Never blindly retry.
                        errors.push(format!("{error}"));
                        break;
                    }
                }
            }
            (samples, errors, expected_records)
        });
    }
    let mut result = WorkloadResult {
        records: 0,
        elapsed: Duration::ZERO,
        samples: Vec::new(),
        errors: Vec::new(),
        expected_records: BTreeMap::new(),
    };
    while let Some(joined) = tasks.join_next().await {
        let (samples, errors, expected_records) = joined.expect("producer task panicked");
        result.records += samples.len();
        result.samples.extend(samples);
        result.errors.extend(errors);
        for (identity, value) in expected_records {
            assert!(result.expected_records.insert(identity, value).is_none());
        }
    }
    result.elapsed = started.elapsed();
    result
}

async fn fetch_workload(
    client: &AstraClient,
    topic: &str,
    start_offset: u64,
    produced: &WorkloadResult,
) -> WorkloadResult {
    let metadata = client.refresh_metadata().await.expect("fetch metadata");
    let topic_id = metadata
        .topics
        .iter()
        .find(|entry| entry.topic_name == topic)
        .expect("benchmark topic metadata")
        .topic_id;
    let mut next_offset = start_offset;
    let expected = start_offset + produced.records as u64;
    let mut unseen = produced.expected_records.clone();
    assert_eq!(unseen.len(), produced.records);
    let started = Instant::now();
    let mut result = WorkloadResult {
        records: 0,
        elapsed: Duration::ZERO,
        samples: Vec::new(),
        errors: Vec::new(),
        expected_records: BTreeMap::new(),
    };
    while next_offset < expected {
        let started_one = Instant::now();
        let response = client
            .fetch(FetchRequestV0 {
                isolation: IsolationLevel::ReadCommitted,
                min_bytes: 0,
                max_wait_ms: 0,
                partitions: vec![FetchPartitionRequestV0 {
                    topic_id,
                    partition: 0,
                    leader_epoch: u64::MAX,
                    fetch_offset: next_offset,
                    // One record per Fetch makes the latency percentiles
                    // actual per-record request samples, not one bulk sample.
                    max_bytes: 32 * 1024,
                    max_records: 1,
                }],
                tags: Vec::new(),
            })
            .await
            .expect("fetch request");
        let part = response
            .partitions
            .first()
            .expect("fetch partition response");
        assert_eq!(part.error_code, 0, "fetch error {part:?}");
        if part.records.is_empty() {
            assert!(
                started.elapsed() < Duration::from_secs(20),
                "committed benchmark records not available"
            );
            time::sleep(Duration::from_millis(20)).await;
            continue;
        }
        assert_eq!(part.records.len(), 1, "Fetch must return one record");
        let fetched = &part.records[0];
        assert_eq!(
            fetched.stream_offset, next_offset,
            "gap or duplicate offset"
        );
        let key = fetched.record.key.as_ref().expect("fetched key missing");
        assert_eq!(key.len(), 4, "worker key length changed");
        assert!(key.iter().all(|byte| *byte == key[0]));
        let value = fetched
            .record
            .value
            .as_ref()
            .expect("fetched value missing");
        assert!(value.len() >= 8, "fetched sequence prefix missing");
        let sequence = u64::from_le_bytes(value[..8].try_into().unwrap());
        let original = unseen
            .remove(&(key[0], sequence))
            .expect("unrecognized or duplicate record identity");
        assert_eq!(value, &original, "committed fetched payload differs");
        assert!(
            fetched.record.headers.is_empty(),
            "unexpected record headers"
        );
        next_offset += 1;
        result.records += part.records.len();
        result
            .samples
            .push(u64::try_from(started_one.elapsed().as_nanos()).unwrap());
    }
    result.elapsed = started.elapsed();
    assert!(
        unseen.is_empty(),
        "some acknowledged records were not fetched"
    );
    result
}

fn record(
    profile: &str,
    ack: &str,
    buffered: bool,
    repeat: usize,
    produced: &WorkloadResult,
    fetched: &WorkloadResult,
) {
    let sha = git_sha();
    let row = format!(
        "{sha},{profile},{ack},{buffered},{repeat},{VALUE_BYTES},{WORKERS},{configured_ms},{produced_records},{records_per_sec:.3},{mib_per_sec:.3},{produce_p50_ms:.3},{produce_p95_ms:.3},{produce_p99_ms:.3},{produce_actual_ms},{fetched_records},{fetch_actual_ms},{fetch_p50_ms:.3},{fetch_p95_ms:.3},{fetch_p99_ms:.3},{producer_errors},{build_profile},{dirty},{produce_sample_count}",
        configured_ms = duration_ms(),
        produced_records = produced.records,
        records_per_sec = produced.throughput(),
        mib_per_sec = produced.mib_per_sec(),
        produce_p50_ms = percentile(&produced.samples, 50),
        produce_p95_ms = percentile(&produced.samples, 95),
        produce_p99_ms = percentile(&produced.samples, 99),
        produce_actual_ms = produced.elapsed.as_secs_f64() * 1_000.0,
        fetched_records = fetched.records,
        fetch_actual_ms = (fetched.elapsed.as_secs_f64() * 1000.0).round(),
        fetch_p50_ms = percentile(&fetched.samples, 50),
        fetch_p95_ms = percentile(&fetched.samples, 95),
        fetch_p99_ms = percentile(&fetched.samples, 99),
        producer_errors = produced.errors.len(),
        build_profile = build_profile(),
        dirty = git_dirty(),
        produce_sample_count = produced.samples.len(),
    );
    println!("ASTRALOG_INTERVIEW_CSV={row}");
    if let Ok(path) = std::env::var("ASTRALOG_INTERVIEW_CSV") {
        let path = PathBuf::from(path);
        if let Some(dir) = path.parent() {
            fs::create_dir_all(dir).expect("create result dir");
        }
        let fresh = !path.exists();
        let mut output = OpenOptions::new()
            .create(true)
            .append(true)
            .open(&path)
            .expect("open result csv");
        if fresh {
            writeln!(output, "sha,profile,acks,buffered,repeat,value_bytes,workers,configured_ms,produced_records,records_per_sec,mib_per_sec,produce_p50_ms,produce_p95_ms,produce_p99_ms,produce_actual_ms,fetched_records,fetch_actual_ms,fetch_p50_ms,fetch_p95_ms,fetch_p99_ms,producer_errors,build_profile,git_dirty,produce_sample_count").unwrap();
        }
        writeln!(output, "{row}").expect("write result csv");
    }
    // Separate raw request samples let a reviewer independently recompute
    // percentiles. Older published 2026-11-03 CSV/logs remain immutable.
    if let Ok(path) = std::env::var("ASTRALOG_INTERVIEW_SAMPLES_CSV") {
        let path = PathBuf::from(path);
        if let Some(dir) = path.parent() {
            fs::create_dir_all(dir).expect("create latency sample dir");
        }
        let fresh = !path.exists();
        let mut output = OpenOptions::new()
            .create(true)
            .append(true)
            .open(&path)
            .expect("open latency samples csv");
        if fresh {
            writeln!(
                output,
                "sha,profile,acks,buffered,repeat,metric,ordinal,elapsed_ns"
            )
            .expect("write latency header");
        }
        for (metric, samples) in [("produce", &produced.samples), ("fetch", &fetched.samples)] {
            for (ordinal, elapsed_ns) in samples.iter().enumerate() {
                writeln!(
                    output,
                    "{sha},{profile},{ack},{buffered},{repeat},{metric},{ordinal},{elapsed_ns}"
                )
                .expect("write latency sample");
            }
        }
    }
}

async fn measure_topic(
    client: &AstraClient,
    profile: &str,
    ack: AckMode,
    topic: &str,
    buffered: bool,
    repeat: usize,
) {
    // Warm-up is excluded from throughput and latency accounting.
    for index in 0..WARMUP_RECORDS {
        client
            .produce(
                topic,
                Some(vec![255]),
                Some(vec![index as u8; VALUE_BYTES]),
                Vec::new(),
                ack,
            )
            .await
            .expect("warmup produce");
    }
    let produced = produce_workload(client, topic, ack, buffered).await;
    assert!(
        produced.errors.is_empty() && produced.records > 0,
        "producer errors: {:?}",
        produced.errors
    );
    let fetched = fetch_workload(client, topic, WARMUP_RECORDS, &produced).await;
    assert_eq!(fetched.records, produced.records);
    record(
        profile,
        match ack {
            AckMode::None => "none",
            AckMode::Leader => "leader",
            AckMode::Quorum => "quorum",
        },
        buffered,
        repeat,
        &produced,
        &fetched,
    );
}

async fn run_rf1_profile(client: &AstraClient) {
    for repeat in 1..=repeat_count() {
        for (label, acks) in [("leader", AckMode::Leader), ("quorum", AckMode::Quorum)] {
            let topic = format!("interview-rf1-{label}-{repeat}");
            let created = client
                .create_topic(&topic, 1)
                .await
                .expect("RF1 topic create");
            assert_ne!(created.topic_id.as_bytes(), &[0; 16]);
            measure_topic(client, "rf1", acks, &topic, false, repeat).await;
        }
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn interview_rf1_roundtrip_and_benchmark() {
    let root = temp_dir("rf1");
    let config = BrokerConfig::new(root.clone(), "127.0.0.1:0".parse::<SocketAddr>().unwrap());
    let broker = start(config).await.expect("start RF1 broker");
    let mut cfg = ClientConfig::new(broker.local_addr().to_string());
    cfg.request_timeout = Duration::from_secs(10);
    cfg.producer_delivery_timeout = Duration::from_secs(30);
    let client = AstraClient::connect(cfg).await.expect("connect RF1 client");
    // The standalone RF1 P2 broker does not advertise P8 topic performance
    // metadata, so the client buffered API is correctly unavailable here.
    run_rf1_profile(&client).await;
    client.close().await;
    broker.shutdown().await.expect("shutdown RF1 broker");
    println!("ASTRALOG_INTERVIEW_DATA_DIR={}", root.display());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 12)]
async fn interview_rf3_roundtrip_and_benchmark() {
    let root = temp_dir("rf3");
    let controller_addrs = [reserve_addr(), reserve_addr(), reserve_addr()];
    let mut controllers = BTreeMap::new();
    for id in 1..=3 {
        controllers.insert(
            cid(id),
            start_controller_node(controller_config(&root, id, &controller_addrs))
                .await
                .expect("start RF3 controller"),
        );
    }
    let _ = controller_leader(&controllers).await;
    let client_addrs = [reserve_addr(), reserve_addr(), reserve_addr()];
    let raft_addrs = [reserve_addr(), reserve_addr(), reserve_addr()];
    let mut register_epochs = [0_u64; 3];
    for id in 1..=3_u32 {
        let idx = (id - 1) as usize;
        let registration = registered(
            &controllers,
            ControllerCommand::RegisterBroker {
                broker_id: id,
                node_uuid: [id as u8; 16],
                initial_incarnation: 1,
                client_host: client_addrs[idx].ip().to_string(),
                client_port: client_addrs[idx].port(),
                internal_host: raft_addrs[idx].ip().to_string(),
                internal_port: raft_addrs[idx].port(),
                rack: Some(format!("rack-{id}")),
                zone: Some(format!("zone-{id}")),
                min_feature: 1,
                max_feature: 4,
            },
        )
        .await;
        let activation = registered(
            &controllers,
            ControllerCommand::SetBrokerLifecycle {
                broker_id: id,
                expected_incarnation: 1,
                expected_lifecycle: BrokerLifecycle::Joining,
                new_lifecycle: BrokerLifecycle::Active,
            },
        )
        .await;
        register_epochs[idx] = registration.metadata_epoch.max(activation.metadata_epoch);
    }
    // P8 batching is intentionally feature-gated by an explicit committed
    // controller feature upgrade. Broker registration alone cannot enable it.
    let feature_leader = controller_leader(&controllers).await;
    controllers[&feature_leader]
        .failure_detector_tick(100)
        .await
        .unwrap();
    for id in 1..=3_u32 {
        controllers[&feature_leader]
            .heartbeat(BrokerHeartbeatRequest {
                authority: BrokerAuthority {
                    broker_id: id,
                    node_uuid: [id as u8; 16],
                    incarnation_epoch: 1,
                },
                applied_metadata_epoch: register_epochs[(id - 1) as usize],
                min_feature: 1,
                max_feature: 4,
                replica_progress: Vec::new(),
                movement_progress: Vec::new(),
            })
            .await
            .expect("feature-aware broker heartbeat");
    }
    controllers[&feature_leader]
        .bump_feature_level(4)
        .await
        .expect("commit P8 batching feature level");
    let clients = controllers
        .values()
        .map(ControllerNodeHandle::client)
        .collect::<Vec<_>>();
    let mut brokers = Vec::new();
    for id in 1..=3_u32 {
        let idx = (id - 1) as usize;
        let mut config = BrokerConfig::new(root.join(format!("broker-{id}")), client_addrs[idx]);
        config.broker_id = id;
        let mut replication = DynamicReplicationConfig::new(raft_addrs[idx]);
        replication.tick_interval = Duration::from_millis(20);
        let election_min = 25 + u64::from(id - 1) * 10;
        replication.election_timeout_min_ticks = election_min;
        replication.election_timeout_max_ticks = election_min + 1;
        let mut dynamic = DynamicControllerConfig::new(clients.clone(), replication);
        dynamic.metadata_refresh_interval = Duration::from_millis(100);
        config.dynamic_controller = Some(dynamic);
        brokers.push(start_dynamic(config, &controllers).await);
    }
    let healthy_deadline = Instant::now() + Duration::from_secs(30);
    loop {
        let leader = controller_leader(&controllers).await;
        // Logical failure detector tick only; no sleep-driven fabricated health.
        if let Err(error) = controllers[&leader].failure_detector_tick(1000).await {
            panic!("RF3 failure detector: {error}");
        }
        let mut healthy = true;
        for id in 1..=3_u32 {
            healthy &= controllers[&leader]
                .broker_liveness(id)
                .await
                .is_ok_and(|ob| ob.state == BrokerLivenessState::Healthy);
        }
        if healthy {
            break;
        }
        assert!(
            Instant::now() < healthy_deadline,
            "RF3 brokers did not become healthy"
        );
        time::sleep(Duration::from_millis(25)).await;
    }
    let mut cfg = ClientConfig::new(brokers[0].local_addr().to_string());
    cfg.request_timeout = Duration::from_secs(10);
    cfg.producer_delivery_timeout = Duration::from_secs(30);
    let client = AstraClient::connect(cfg).await.expect("connect RF3 client");
    for repeat in 1..=repeat_count() {
        for (label, acks) in [("leader", AckMode::Leader), ("quorum", AckMode::Quorum)] {
            for buffered in [false, true] {
                let mode = if buffered { "batch" } else { "plain" };
                let topic = format!("interview-rf3-{label}-{mode}-{repeat}");
                create_rf3_topic(&controllers, &topic).await;
                wait_for_topic_leader(&client, &topic).await;
                measure_topic(&client, "rf3", acks, &topic, buffered, repeat).await;
            }
        }
    }
    client.close().await;
    for broker in brokers {
        broker.shutdown().await.expect("shutdown RF3 broker");
    }
    for (_, node) in controllers {
        node.shutdown().await;
    }
    println!("ASTRALOG_INTERVIEW_DATA_DIR={}", root.display());
}
