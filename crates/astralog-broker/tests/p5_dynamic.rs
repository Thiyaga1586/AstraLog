use std::{
    collections::BTreeMap,
    fs,
    net::{Ipv4Addr, SocketAddr},
    path::{Path, PathBuf},
    sync::{
        OnceLock,
        atomic::{AtomicU32, Ordering},
    },
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use astralog_broker::{
    BrokerConfig, BrokerError, BrokerHandle, DynamicControllerConfig, DynamicReplicationConfig,
    PreferredLeaderDisposition, PreferredLeaderRequest, start as start_broker,
};
use astralog_client::{AstraClient, ClientConfig, ClientError, ProduceOutcome};
use astralog_common::ProducerSequence;
use astralog_controller::{
    BrokerAuthority, BrokerHeartbeatRequest, BrokerLifecycle, BrokerLivenessState,
    CleanupPolicyV1 as ControllerCleanupPolicyV1, ControllerCommand, ControllerNodeConfig,
    ControllerNodeHandle, ControllerNodeId, ControllerPeerDescriptor,
    ControllerPeerTransportConfig, ControllerPersistenceConfig, ControllerRuntimeConfig,
    ControllerRuntimeLimits, ControllerState, CreateTopicSpec, FailureDetectorConfig,
    FailureDomainPolicy, FeatureRange, HeartbeatLimits, MovementStage, ReassignmentSpec,
    SubscriptionProtectionV1 as ControllerSubscriptionProtectionV1, TopicPerformanceSpecV1,
    TopicRetentionSpecV1, start_controller_node,
};
use astralog_protocol::{
    ApiKey, ClientHello, FrameKind, MAX_WIRE_FRAME_LEN_V1, PublicError, ServerHello,
    UNKNOWN_BROKER_ID, UNKNOWN_EPOCH, WIRE_VERSION, WireFrame,
    api::{
        AckMode, AlterTopicConfigRequestV0, AlterTopicConfigResponseV0, ApiVersionsRequestV0,
        ApiVersionsResponseV0, BodyCodec, CreateTopicRequestV1, CreateTopicRequestV2,
        CreateTopicResponseV1, CreateTopicResponseV2, DescribeClusterRequestV0,
        DescribeClusterResponseV0, DescribeTopicRequestV1, DescribeTopicRequestV2,
        DescribeTopicResponseV1, DescribeTopicResponseV2, FetchPartitionRequestV0, FetchRequestV0,
        IsolationLevel, ListOffsetsQueryV0, ListOffsetsRequestV0, ReassignPartitionV0,
        ReassignPartitionsRequestV0, ReassignPartitionsResponseV0, TopicCleanupPolicyV1,
        TopicCompressionV1, TopicPerformanceConfigV1, TopicRetentionConfigV1,
        TopicRetentionPatchV1, WireRecordV0,
    },
    decode_frame, encode_frame,
};
use astralog_raft::{
    EffectiveConfiguration, NodeId, RaftError, Role, StableConfiguration, TimingConfig,
};
use astralog_storage::{
    CompactionLimits, Compression as StorageCompression, DataRecord, Frame as StorageFrame,
    PartitionLogConfig, StorageLimits, scan_segment,
};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::TcpStream,
    time,
};

fn temp_dir(label: &str) -> PathBuf {
    let nonce = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    let path = std::env::temp_dir().join(format!(
        "astralog-p5j-e2e-{label}-{}-{nonce}",
        std::process::id()
    ));
    fs::create_dir_all(&path).unwrap();
    path
}

fn reserve_addr() -> SocketAddr {
    static NEXT_LOOPBACK: AtomicU32 = AtomicU32::new(0);
    let slot = NEXT_LOOPBACK.fetch_add(1, Ordering::Relaxed);
    let process = std::process::id();
    let second = u8::try_from((process >> 8) & 0xff).unwrap();
    let third = u8::try_from(process & 0xff).unwrap();
    let fourth = u8::try_from(1 + slot % 250).unwrap();
    let listener =
        std::net::TcpListener::bind((Ipv4Addr::new(127, second, third, fourth), 0)).unwrap();
    let address = listener.local_addr().unwrap();
    drop(listener);
    address
}

fn p5_integration_lock() -> &'static tokio::sync::Mutex<()> {
    static LOCK: OnceLock<tokio::sync::Mutex<()>> = OnceLock::new();
    LOCK.get_or_init(|| tokio::sync::Mutex::new(()))
}

fn cid(value: u64) -> ControllerNodeId {
    ControllerNodeId::new(value).unwrap()
}

fn voters() -> StableConfiguration {
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
    astralog_controller::initialize_controller_bootstrap(&directory, [5; 16], &voters()).unwrap();
    let mut peers = BTreeMap::new();
    for peer in 1..=3_u64 {
        if peer != id {
            peers.insert(
                cid(peer),
                ControllerPeerDescriptor {
                    controller_id: cid(peer),
                    address: addresses[usize::try_from(peer - 1).unwrap()],
                },
            );
        }
    }
    let timeout = 4 + id;
    let controller_feature_ranges = (1..=3_u64)
        .map(|node| (cid(node), FeatureRange::new(1, 3).unwrap()))
        .collect();
    ControllerNodeConfig {
        runtime: ControllerRuntimeConfig {
            local_node: cid(id),
            directory,
            static_configuration: voters(),
            controller_feature_ranges,
            initial_state: ControllerState::new([5; 16], 1, 1_000).unwrap(),
            persistence: ControllerPersistenceConfig::default(),
            timing: TimingConfig::new(1, 5, 10).unwrap(),
            randomized_election_timeout: timeout,
            limits: ControllerRuntimeLimits::default(),
            heartbeat_limits: HeartbeatLimits::default(),
            failure_detector: FailureDetectorConfig::default(),
        },
        transport: ControllerPeerTransportConfig {
            local_controller_id: cid(id),
            cluster_id: [5; 16],
            bind_addr: addresses[usize::try_from(id - 1).unwrap()],
            peers,
            limits: astralog_controller::ControllerTransportLimits::default(),
            max_inbound_frames: 1024,
            max_outbound_frames_per_peer: 1024,
            max_outbound_bytes_per_peer: 16 * 1024 * 1024,
            reconnect_min_delay: Duration::from_millis(5),
            reconnect_max_delay: Duration::from_millis(50),
        },
        // Correctness qualification, not an election-latency benchmark. Keep
        // the frozen 5..=10 tick ratio but give the controller quorum enough
        // wall-clock room to avoid scheduler-induced perpetual candidacy under
        // repeated Windows/CI load.
        tick_period: Duration::from_millis(25),
        election_timeout_sample: timeout,
        max_control_requests: 128,
    }
}

fn p8_controller_config(root: &Path, id: u64, addresses: &[SocketAddr; 3]) -> ControllerNodeConfig {
    let mut config = controller_config(root, id, addresses);
    config.runtime.controller_feature_ranges = (1..=3_u64)
        .map(|node| (cid(node), FeatureRange::new(1, 4).unwrap()))
        .collect();
    config
}

fn p9_controller_config(root: &Path, id: u64, addresses: &[SocketAddr; 3]) -> ControllerNodeConfig {
    let mut config = controller_config(root, id, addresses);
    config.runtime.controller_feature_ranges = (1..=3_u64)
        .map(|node| (cid(node), FeatureRange::new(1, 5).unwrap()))
        .collect();
    config
}

async fn current_leader(
    nodes: &BTreeMap<ControllerNodeId, ControllerNodeHandle>,
) -> ControllerNodeId {
    let deadline = time::Instant::now() + Duration::from_secs(60);
    loop {
        for (id, node) in nodes {
            if let Ok(status) = node.status().await
                && status.role == Role::Leader
                && !status.fatal
            {
                return *id;
            }
        }
        assert!(
            time::Instant::now() < deadline,
            "controller leader election timed out"
        );
        time::sleep(Duration::from_millis(20)).await;
    }
}

async fn stable_controller_leader(
    nodes: &BTreeMap<ControllerNodeId, ControllerNodeHandle>,
) -> ControllerNodeId {
    let deadline = time::Instant::now() + Duration::from_secs(60);
    let mut last = None;
    let mut consecutive = 0_u8;
    loop {
        let leader = current_leader(nodes).await;
        let status = nodes[&leader].status().await.unwrap();
        let observation = (leader, status.term);
        if last == Some(observation) {
            consecutive = consecutive.saturating_add(1);
        } else {
            last = Some(observation);
            consecutive = 1;
        }
        if consecutive >= 5 {
            return leader;
        }
        assert!(
            time::Instant::now() < deadline,
            "controller leadership did not stabilize"
        );
        time::sleep(Duration::from_millis(20)).await;
    }
}

async fn start_dynamic_broker_when_controller_ready(
    config: BrokerConfig,
    controllers: &BTreeMap<ControllerNodeId, ControllerNodeHandle>,
) -> BrokerHandle {
    let deadline = time::Instant::now() + Duration::from_secs(60);
    loop {
        let _ = stable_controller_leader(controllers).await;
        match start_broker(config.clone()).await {
            Ok(handle) => return handle,
            Err(BrokerError::Controller(
                astralog_controller::ControllerError::NotLeader { .. }
                | astralog_controller::ControllerError::Busy(_),
            )) => {}
            Err(error) => panic!("dynamic broker startup failed: {error}"),
        }
        assert!(
            time::Instant::now() < deadline,
            "dynamic broker startup did not observe a stable controller leader"
        );
        time::sleep(Duration::from_millis(20)).await;
    }
}

async fn controller_snapshot(
    nodes: &BTreeMap<ControllerNodeId, ControllerNodeHandle>,
) -> astralog_controller::ControllerMetadataSnapshot {
    let deadline = time::Instant::now() + Duration::from_secs(30);
    loop {
        let leader = current_leader(nodes).await;
        match nodes[&leader].metadata_snapshot().await {
            Ok(snapshot) => return snapshot,
            Err(
                astralog_controller::ControllerError::NotLeader { .. }
                | astralog_controller::ControllerError::Busy(_),
            ) => {}
            Err(error) => panic!("controller metadata snapshot failed: {error}"),
        }
        assert!(
            time::Instant::now() < deadline,
            "controller metadata snapshot did not obtain a stable leader"
        );
        time::sleep(Duration::from_millis(20)).await;
    }
}

async fn create_topic_on_current_leader(
    nodes: &BTreeMap<ControllerNodeId, ControllerNodeHandle>,
    spec: CreateTopicSpec,
) -> astralog_controller::TopicCreateResult {
    let deadline = time::Instant::now() + Duration::from_secs(30);
    loop {
        let leader = current_leader(nodes).await;
        match nodes[&leader].create_topic(spec.clone()).await {
            Ok(created) => return created,
            Err(astralog_controller::ControllerError::NotLeader { .. }) => {}
            Err(error) => panic!("create-topic outcome is ambiguous or invalid: {error}"),
        }
        assert!(
            time::Instant::now() < deadline,
            "create-topic never reached a stable controller leader"
        );
        time::sleep(Duration::from_millis(20)).await;
    }
}

async fn create_retained_topic_on_current_leader(
    nodes: &BTreeMap<ControllerNodeId, ControllerNodeHandle>,
    spec: CreateTopicSpec,
    retention: TopicRetentionSpecV1,
) -> astralog_controller::TopicCreateResult {
    let deadline = time::Instant::now() + Duration::from_secs(30);
    let performance = TopicPerformanceSpecV1::default_v1();
    loop {
        let leader = current_leader(nodes).await;
        match nodes[&leader]
            .create_topic_with_retention(spec.clone(), performance, retention)
            .await
        {
            Ok(created) => return created,
            Err(
                astralog_controller::ControllerError::NotLeader { .. }
                | astralog_controller::ControllerError::Busy(_),
            ) => {}
            Err(error) => panic!("P9 retained create-topic failed: {error}"),
        }
        assert!(time::Instant::now() < deadline);
        time::sleep(Duration::from_millis(20)).await;
    }
}

async fn failure_detector_tick_on_current_leader(
    nodes: &BTreeMap<ControllerNodeId, ControllerNodeHandle>,
    tick: u64,
) -> ControllerNodeId {
    let deadline = time::Instant::now() + Duration::from_secs(30);
    loop {
        let leader = current_leader(nodes).await;
        match nodes[&leader].failure_detector_tick(tick).await {
            Ok(()) => return leader,
            Err(
                astralog_controller::ControllerError::NotLeader { .. }
                | astralog_controller::ControllerError::Busy(_),
            ) => {}
            Err(error) => panic!("failure-detector tick failed: {error}"),
        }
        assert!(
            time::Instant::now() < deadline,
            "failure-detector tick did not reach a current controller leader"
        );
        time::sleep(Duration::from_millis(20)).await;
    }
}

async fn quorum_produce_with_refresh(
    client: &AstraClient,
    topic: &str,
    key: Vec<u8>,
    value: Vec<u8>,
    timeout: Duration,
    context: &'static str,
) -> ProduceOutcome {
    let deadline = time::Instant::now() + timeout;
    loop {
        match client
            .produce(
                topic,
                Some(key.clone()),
                Some(value.clone()),
                Vec::new(),
                AckMode::Quorum,
            )
            .await
        {
            Ok(outcome) => return outcome,
            Err(
                error @ (ClientError::Metadata("partition leader is unknown")
                | ClientError::Metadata("partition leader authority is unknown")
                | ClientError::Metadata("topic not present in metadata")),
            ) => {
                // These route-resolution errors happen *before* Produce is
                // dispatched. Every other failure may hide an already
                // appended non-idempotent record: fail rather than replay.
                let refresh = client.refresh_metadata().await;
                if time::Instant::now() >= deadline {
                    panic!(
                        "quorum produce did not recover after bounded metadata refresh ({context}); produce_error={error:?}; refresh={refresh:?}"
                    );
                }
            }
            Err(error) => panic!(
                "P9/P5 integration refuses to blindly replay non-idempotent Produce after a potentially ambiguous response ({context}): {error:?}"
            ),
        }
        time::sleep(Duration::from_millis(20)).await;
    }
}

/// P9-G post-failover probe: the failure oracle is identical to the generic
/// non-idempotent Produce helper, but a RED captures the *live* P3 voter
/// roles and current disk-side safe/clean proofs. Never retry an ambiguous
/// write, infer P3 leadership from a controller snapshot, or trust a stale
/// local clean sidecar to declare the cluster healthy.
async fn p9g_failover_actor_disk_diagnostics(
    brokers: &[Option<BrokerHandle>],
    topic_id: astralog_common::TopicId,
    root: &Path,
    topic_hex: &str,
    durable_safe: fn(&Path) -> Option<u64>,
) -> Vec<(u32, String, Option<u64>, Option<u64>, Option<u64>)> {
    let mut diagnostics = Vec::new();
    for broker_id in 1_u32..=3 {
        let index = usize::try_from(broker_id - 1).unwrap();
        let actor = match brokers[index].as_ref() {
            Some(broker) => format!(
                "{:?}",
                time::timeout(
                    Duration::from_millis(350),
                    broker.observe_p9g_membership_internal(topic_id, 0),
                )
                .await,
            ),
            None => "stopped".into(),
        };
        let partition = root
            .join(format!("broker-{broker_id}"))
            .join("partitions")
            .join(topic_hex)
            .join("0000000000");
        let manifest_generation = astralog_storage::load_manifest(&partition)
            .ok()
            .flatten()
            .map(|manifest| manifest.generation);
        let clean = manifest_generation.map(|generation| {
            astralog_storage::LocalCleanProgressStore::new(
                &partition,
                topic_id,
                astralog_common::PartitionId::new(0),
            )
            .load_conservative(Some(generation))
            .get()
        });
        let safe = durable_safe(&partition);
        diagnostics.push((broker_id, actor, manifest_generation, clean, safe));
    }
    diagnostics
}

#[allow(clippy::too_many_arguments)]
async fn p9g_quorum_produce_with_failover_diagnostics(
    client: &AstraClient,
    topic: &str,
    key: Vec<u8>,
    value: Vec<u8>,
    timeout: Duration,
    brokers: &[Option<BrokerHandle>],
    topic_id: astralog_common::TopicId,
    root: &Path,
    topic_hex: &str,
    durable_safe: fn(&Path) -> Option<u64>,
) -> ProduceOutcome {
    let deadline = time::Instant::now() + timeout;
    loop {
        match client
            .produce(
                topic,
                Some(key.clone()),
                Some(value.clone()),
                Vec::new(),
                AckMode::Quorum,
            )
            .await
        {
            Ok(outcome) => return outcome,
            Err(
                error @ (ClientError::Metadata("partition leader is unknown")
                | ClientError::Metadata("partition leader authority is unknown")
                | ClientError::Metadata("topic not present in metadata")),
            ) => {
                let refresh = client.refresh_metadata().await;
                if time::Instant::now() >= deadline {
                    let diagnostics = p9g_failover_actor_disk_diagnostics(
                        brokers,
                        topic_id,
                        root,
                        topic_hex,
                        durable_safe,
                    )
                    .await;
                    panic!(
                        "P9-G quorum Produce could not recover from an explicitly pre-send routing failure; last={error:?}; refresh={refresh:?}; live_actor_manifest_clean_safe={diagnostics:?}"
                    );
                }
            }
            Err(error) => {
                let diagnostics = p9g_failover_actor_disk_diagnostics(
                    brokers,
                    topic_id,
                    root,
                    topic_hex,
                    durable_safe,
                )
                .await;
                panic!(
                    "P9-G quorum Produce must not replay an ambiguous post-failover mutation: {error:?}; live_actor_manifest_clean_safe={diagnostics:?}"
                );
            }
        }
        time::sleep(Duration::from_millis(20)).await;
    }
}

/// Preferred-leader requests must originate from the *current* P3 leader.
/// After controller-managed rejoin, an election may have happened before the
/// harness observes it. Discover the live leader on every attempt rather than
/// assuming the previous one still owns the partition. Never retry producer
/// writes or bypass the controller's P5 target eligibility check.
async fn p9g_transfer_to_current_leader(
    brokers: &[Option<BrokerHandle>],
    topic_id: astralog_common::TopicId,
    target_broker_id: u32,
    context: &str,
) -> u64 {
    let deadline = time::Instant::now() + Duration::from_secs(25);
    let mut last_failure = None;
    loop {
        for broker in brokers.iter().flatten() {
            let Ok(observation) = broker.observe_partition_internal(topic_id, 0).await else {
                continue;
            };
            if !observation.local_is_leader {
                continue;
            }
            match broker
                .request_preferred_leader(PreferredLeaderRequest {
                    topic_id,
                    partition_id: 0,
                    target_broker_id,
                })
                .await
            {
                Ok(outcome)
                    if matches!(
                        outcome.disposition,
                        PreferredLeaderDisposition::AlreadyPreferred
                            | PreferredLeaderDisposition::TransferObserved
                    ) =>
                {
                    let transfer_epoch = outcome.observed_leader_epoch;
                    let target_index = usize::try_from(target_broker_id - 1).unwrap();
                    let target = brokers
                        .get(target_index)
                        .and_then(Option::as_ref)
                        .expect("preferred-leader target must be live in the test harness");
                    // A TransferObserved result is a genuine P3 observation,
                    // but not a durable lease on future leadership. Under an
                    // election churn the target can lose that role before its
                    // own actor is observed. Re-discover the current live P3
                    // leader and re-request its authorized transfer instead of
                    // waiting the entire deadline for an obsolete handoff.
                    let recheck_deadline = time::Instant::now() + Duration::from_millis(500);
                    loop {
                        match target.observe_partition_internal(topic_id, 0).await {
                            Ok(target_observation)
                                if target_observation.local_is_leader
                                    && target_observation.leader_broker_id == target_broker_id
                                    && target_observation.leader_epoch >= transfer_epoch =>
                            {
                                return target_observation.leader_epoch;
                            }
                            Ok(target_observation) => {
                                last_failure = Some(format!(
                                    "target broker has not locally observed leadership yet: {target_observation:?}"
                                ));
                            }
                            Err(error) => {
                                last_failure = Some(format!(
                                    "target broker partition observation failed: {error:?}"
                                ));
                            }
                        }
                        if time::Instant::now() >= recheck_deadline {
                            break;
                        }
                        assert!(
                            time::Instant::now() < deadline,
                            "P9-G {context}: broker {target_broker_id} never directly observed local P3 leadership after transfer: last={last_failure:?}"
                        );
                        time::sleep(Duration::from_millis(20)).await;
                    }
                }
                Ok(outcome) => last_failure = Some(format!("unexpected {outcome:?}")),
                Err(error) => last_failure = Some(format!("{error:?}")),
            }
        }
        assert!(
            time::Instant::now() < deadline,
            "P9-G {context}: P3 leader never completed authorized transfer to broker {target_broker_id}: last={last_failure:?}"
        );
        time::sleep(Duration::from_millis(30)).await;
    }
}

async fn mutate(
    nodes: &BTreeMap<ControllerNodeId, ControllerNodeHandle>,
    command: ControllerCommand,
) -> astralog_controller::ApplyOutcome {
    loop {
        let leader = stable_controller_leader(nodes).await;
        match nodes[&leader].mutate(command.clone()).await {
            Ok(outcome) => return outcome,
            Err(error) => {
                if let Some(outcome) = reconcile_membership_command(nodes, &command).await {
                    return outcome;
                }
                if matches!(
                    error,
                    astralog_controller::ControllerError::NotLeader { .. }
                        | astralog_controller::ControllerError::Busy(_)
                ) {
                    time::sleep(Duration::from_millis(20)).await;
                    continue;
                }
                panic!("controller mutation failed: {error}");
            }
        }
    }
}

async fn reconcile_membership_command(
    nodes: &BTreeMap<ControllerNodeId, ControllerNodeHandle>,
    command: &ControllerCommand,
) -> Option<astralog_controller::ApplyOutcome> {
    let broker_id = match command {
        ControllerCommand::RegisterBroker { broker_id, .. }
        | ControllerCommand::RejoinBroker { broker_id, .. }
        | ControllerCommand::SetBrokerLifecycle { broker_id, .. } => *broker_id,
        _ => return None,
    };
    let snapshot = controller_snapshot(nodes).await;
    let broker = snapshot.state.broker(broker_id)?;
    let committed = match command {
        ControllerCommand::RegisterBroker {
            node_uuid,
            initial_incarnation,
            client_host,
            client_port,
            internal_host,
            internal_port,
            rack,
            zone,
            min_feature,
            max_feature,
            ..
        } => {
            broker.node_uuid == *node_uuid
                && broker.incarnation_epoch == *initial_incarnation
                && broker.lifecycle == BrokerLifecycle::Joining
                && broker.client_host == *client_host
                && broker.client_port == *client_port
                && broker.internal_host == *internal_host
                && broker.internal_port == *internal_port
                && broker.rack == *rack
                && broker.zone == *zone
                && broker.min_feature == *min_feature
                && broker.max_feature == *max_feature
        }
        ControllerCommand::RejoinBroker {
            node_uuid,
            new_incarnation,
            client_host,
            client_port,
            internal_host,
            internal_port,
            rack,
            zone,
            min_feature,
            max_feature,
            ..
        } => {
            broker.node_uuid == *node_uuid
                && broker.incarnation_epoch == *new_incarnation
                && broker.client_host == *client_host
                && broker.client_port == *client_port
                && broker.internal_host == *internal_host
                && broker.internal_port == *internal_port
                && broker.rack == *rack
                && broker.zone == *zone
                && broker.min_feature == *min_feature
                && broker.max_feature == *max_feature
        }
        ControllerCommand::SetBrokerLifecycle {
            expected_incarnation,
            new_lifecycle,
            ..
        } => {
            broker.incarnation_epoch == *expected_incarnation && broker.lifecycle == *new_lifecycle
        }
        _ => false,
    };
    committed.then_some(astralog_controller::ApplyOutcome {
        metadata_epoch: snapshot.epoch,
        producer_ids: None,
    })
}

#[tokio::test(flavor = "multi_thread", worker_threads = 6)]
async fn controller_topology_drives_broker_admin_metadata_and_survives_controller_outage() {
    let _test_guard = p5_integration_lock().lock().await;
    let root = temp_dir("controller-outage");
    let controller_addresses = [reserve_addr(), reserve_addr(), reserve_addr()];
    let mut controllers = BTreeMap::new();
    for id in 1..=3_u64 {
        controllers.insert(
            cid(id),
            start_controller_node(controller_config(&root, id, &controller_addresses))
                .await
                .unwrap(),
        );
    }
    let _ = current_leader(&controllers).await;

    let broker_client_addr = reserve_addr();
    let broker_raft_addr = reserve_addr();
    let broker2_raft_addr = reserve_addr();
    let register =
        |broker_id: u32, client: SocketAddr, raft: SocketAddr| ControllerCommand::RegisterBroker {
            broker_id,
            node_uuid: [broker_id as u8; 16],
            initial_incarnation: 1,
            client_host: client.ip().to_string(),
            client_port: client.port(),
            internal_host: raft.ip().to_string(),
            internal_port: raft.port(),
            rack: Some(format!("rack-{broker_id}")),
            zone: Some(format!("zone-{broker_id}")),
            min_feature: 1,
            max_feature: 1,
        };
    let broker2_client_addr = reserve_addr();
    mutate(
        &controllers,
        register(1, broker_client_addr, broker_raft_addr),
    )
    .await;
    mutate(
        &controllers,
        ControllerCommand::SetBrokerLifecycle {
            broker_id: 1,
            expected_incarnation: 1,
            expected_lifecycle: BrokerLifecycle::Joining,
            new_lifecycle: BrokerLifecycle::Active,
        },
    )
    .await;
    mutate(
        &controllers,
        register(2, broker2_client_addr, broker2_raft_addr),
    )
    .await;
    let active2 = mutate(
        &controllers,
        ControllerCommand::SetBrokerLifecycle {
            broker_id: 2,
            expected_incarnation: 1,
            expected_lifecycle: BrokerLifecycle::Joining,
            new_lifecycle: BrokerLifecycle::Active,
        },
    )
    .await;

    let leader = failure_detector_tick_on_current_leader(&controllers, 100).await;
    controllers[&leader]
        .heartbeat(BrokerHeartbeatRequest {
            authority: BrokerAuthority {
                broker_id: 1,
                node_uuid: [1; 16],
                incarnation_epoch: 1,
            },
            applied_metadata_epoch: active2.metadata_epoch,
            min_feature: 1,
            max_feature: 1,
            replica_progress: Vec::new(),
            movement_progress: Vec::new(),
        })
        .await
        .unwrap();
    let created = create_topic_on_current_leader(
        &controllers,
        CreateTopicSpec {
            topic_name: "orders".into(),
            partition_count: 1,
            replication_factor: 1,
            failure_domain_policy: FailureDomainPolicy::Disabled,
        },
    )
    .await;
    assert_eq!(created.assignments[0].voters, vec![1]);

    let clients = controllers
        .values()
        .map(ControllerNodeHandle::client)
        .collect();
    let mut broker_config = BrokerConfig::new(root.join("broker-1"), broker_client_addr);
    broker_config.advertised_host = broker_client_addr.ip().to_string();
    broker_config.advertised_port = broker_client_addr.port();
    broker_config.broker_id = 1;
    let mut dynamic_replication = DynamicReplicationConfig::new(broker_raft_addr);
    dynamic_replication.tick_interval = Duration::from_millis(20);
    let mut dynamic = DynamicControllerConfig::new(clients, dynamic_replication);
    dynamic.metadata_refresh_interval = Duration::from_millis(25);
    dynamic.max_pending_admin_operations = 8;
    broker_config.dynamic_controller = Some(dynamic);
    let broker = start_dynamic_broker_when_controller_ready(broker_config, &controllers).await;

    let mut client_config = ClientConfig::new(broker.local_addr().to_string());
    client_config.request_timeout = Duration::from_secs(3);
    client_config.batch_max_records = 2;
    client_config.batch_linger = Duration::from_secs(5);
    let client = AstraClient::connect(client_config).await.unwrap();
    let deadline = time::Instant::now() + Duration::from_secs(5);
    loop {
        let metadata = client.refresh_metadata().await.unwrap();
        let partition = &metadata.topics[0].partitions[0];
        if partition.leader_broker_id == 1 && partition.leader_epoch != UNKNOWN_EPOCH {
            assert_eq!(metadata.metadata_epoch, created.outcome.metadata_epoch);
            assert_eq!(partition.replica_broker_ids, vec![1]);
            break;
        }
        assert!(
            time::Instant::now() < deadline,
            "RF1 dynamic actor did not become leader"
        );
        time::sleep(Duration::from_millis(25)).await;
    }

    let mut raw = raw_hello(broker.local_addr()).await;
    send_request(
        &mut raw,
        ApiKey::ApiVersions,
        1,
        ApiVersionsRequestV0 {
            client_software_name: "p5j-test".into(),
            client_software_version: "0.1".into(),
            tags: Vec::new(),
        }
        .encode_body()
        .unwrap(),
    )
    .await;
    let describe = send_request(
        &mut raw,
        ApiKey::DescribeCluster,
        2,
        DescribeClusterRequestV0 { tags: Vec::new() }
            .encode_body()
            .unwrap(),
    )
    .await;
    let describe = DescribeClusterResponseV0::decode_body(&describe.body).unwrap();
    assert_eq!(describe.cluster_id, [5; 16]);
    assert_ne!(describe.controller_leader_id, 0);
    assert_eq!(describe.brokers.len(), 2);
    assert_eq!(describe.brokers[0].incarnation_epoch, 1);

    let reassign = send_request(
        &mut raw,
        ApiKey::ReassignPartitions,
        3,
        ReassignPartitionsRequestV0 {
            partitions: vec![ReassignPartitionV0 {
                topic_id: created.topic.topic_id,
                partition: 0,
                source_broker_id: 1,
                destination_broker_id: 2,
                preferred_leader_broker_id: UNKNOWN_BROKER_ID,
            }],
            tags: Vec::new(),
        }
        .encode_body()
        .unwrap(),
    )
    .await;
    let reassign = ReassignPartitionsResponseV0::decode_body(&reassign.body).unwrap();
    assert_eq!(reassign.results.len(), 1);
    let movement_id =
        astralog_controller::MovementId::new(reassign.results[0].movement_id).unwrap();
    let snapshot = controller_snapshot(&controllers).await;
    assert_ne!(
        snapshot.state.movements()[&movement_id].stage,
        astralog_controller::MovementStage::Complete,
        "P5-J admission must publish durable movement intent before P5-K completion"
    );
    assert_eq!(
        snapshot.state.assignments()[&(created.topic.topic_id, 0)].voters,
        vec![1]
    );

    for (_, controller) in controllers {
        controller.shutdown().await;
    }
    time::sleep(Duration::from_millis(100)).await;

    let cached_before = client.refresh_metadata().await.unwrap();
    assert!(
        client.create_topic("controller-offline", 1).await.is_err(),
        "controller-backed admin mutation unexpectedly succeeded without controller quorum"
    );
    let cached_after = client.refresh_metadata().await.unwrap();
    assert_eq!(cached_after.metadata_epoch, cached_before.metadata_epoch);
    assert_eq!(cached_after.cluster_id, cached_before.cluster_id);
    assert_eq!(cached_after.topics, cached_before.topics);

    let produced = client
        .produce(
            "orders",
            Some(b"k".to_vec()),
            Some(b"v".to_vec()),
            Vec::new(),
            AckMode::Quorum,
        )
        .await
        .unwrap();
    let fetched = client
        .fetch(FetchRequestV0 {
            isolation: IsolationLevel::ReadCommitted,
            min_bytes: 0,
            max_wait_ms: 0,
            partitions: vec![FetchPartitionRequestV0 {
                topic_id: created.topic.topic_id,
                partition: 0,
                leader_epoch: UNKNOWN_EPOCH,
                fetch_offset: produced.base_offset.unwrap(),
                max_bytes: 4096,
                max_records: 10,
            }],
            tags: Vec::new(),
        })
        .await
        .unwrap();
    assert_eq!(fetched.partitions[0].records.len(), 1);
    assert_eq!(
        fetched.partitions[0].records[0].record.value.as_deref(),
        Some(b"v".as_slice())
    );

    client.close().await;
    broker.shutdown().await.unwrap();
    fs::remove_dir_all(root).unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 6)]
async fn p8_topic_v1_is_feature_gated_and_round_trips_committed_policy() {
    let _test_guard = p5_integration_lock().lock().await;
    let root = temp_dir("p8-topic-policy");
    let controller_addresses = [reserve_addr(), reserve_addr(), reserve_addr()];
    let mut controllers = BTreeMap::new();
    for id in 1..=3_u64 {
        controllers.insert(
            cid(id),
            start_controller_node(p8_controller_config(&root, id, &controller_addresses))
                .await
                .unwrap(),
        );
    }
    let _ = stable_controller_leader(&controllers).await;

    let broker_client_addr = reserve_addr();
    let broker_raft_addr = reserve_addr();
    let registered = mutate(
        &controllers,
        ControllerCommand::RegisterBroker {
            broker_id: 1,
            node_uuid: [1; 16],
            initial_incarnation: 1,
            client_host: broker_client_addr.ip().to_string(),
            client_port: broker_client_addr.port(),
            internal_host: broker_raft_addr.ip().to_string(),
            internal_port: broker_raft_addr.port(),
            rack: Some("rack-1".into()),
            zone: Some("zone-1".into()),
            min_feature: 1,
            max_feature: 4,
        },
    )
    .await;
    let activated = mutate(
        &controllers,
        ControllerCommand::SetBrokerLifecycle {
            broker_id: 1,
            expected_incarnation: 1,
            expected_lifecycle: BrokerLifecycle::Joining,
            new_lifecycle: BrokerLifecycle::Active,
        },
    )
    .await;
    let leader = failure_detector_tick_on_current_leader(&controllers, 100).await;
    controllers[&leader]
        .heartbeat(BrokerHeartbeatRequest {
            authority: BrokerAuthority {
                broker_id: 1,
                node_uuid: [1; 16],
                incarnation_epoch: 1,
            },
            applied_metadata_epoch: registered.metadata_epoch.max(activated.metadata_epoch),
            min_feature: 1,
            max_feature: 4,
            replica_progress: Vec::new(),
            movement_progress: Vec::new(),
        })
        .await
        .unwrap();
    let leader = stable_controller_leader(&controllers).await;
    let bumped = controllers[&leader].bump_feature_level(4).await.unwrap();
    let bump_deadline = time::Instant::now() + Duration::from_secs(10);
    loop {
        let snapshot = controller_snapshot(&controllers).await;
        if snapshot.epoch >= bumped.metadata_epoch && snapshot.state.active_feature_level() == 4 {
            break;
        }
        assert!(time::Instant::now() < bump_deadline);
        time::sleep(Duration::from_millis(25)).await;
    }

    let clients = controllers
        .values()
        .map(ControllerNodeHandle::client)
        .collect();
    let mut broker_config = BrokerConfig::new(root.join("broker-1"), broker_client_addr);
    broker_config.advertised_host = broker_client_addr.ip().to_string();
    broker_config.advertised_port = broker_client_addr.port();
    broker_config.broker_id = 1;
    let mut dynamic_replication = DynamicReplicationConfig::new(broker_raft_addr);
    dynamic_replication.tick_interval = Duration::from_millis(20);
    let mut dynamic = DynamicControllerConfig::new(clients, dynamic_replication);
    dynamic.metadata_refresh_interval = Duration::from_millis(25);
    broker_config.dynamic_controller = Some(dynamic);
    let broker = start_dynamic_broker_when_controller_ready(broker_config, &controllers).await;

    let mut raw = raw_hello(broker.local_addr()).await;
    let api_versions = send_request_version(
        &mut raw,
        ApiKey::ApiVersions,
        0,
        1,
        ApiVersionsRequestV0 {
            client_software_name: "p8-test".into(),
            client_software_version: "0.1".into(),
            tags: Vec::new(),
        }
        .encode_body()
        .unwrap(),
    )
    .await;
    let api_versions = ApiVersionsResponseV0::decode_body(&api_versions.body).unwrap();
    let feature = api_versions
        .features
        .iter()
        .find(|feature| feature.feature_key == 14)
        .unwrap();
    assert_eq!(
        (
            feature.min_supported_version,
            feature.max_supported_version,
            feature.active_version,
        ),
        (1, 1, 1)
    );
    for api_key in [ApiKey::CreateTopic, ApiKey::DescribeTopic] {
        let range = api_versions
            .apis
            .iter()
            .find(|range| range.api_key == api_key as u16)
            .unwrap();
        assert_eq!((range.min_api_version, range.max_api_version), (0, 1));
    }
    assert!(
        api_versions
            .apis
            .iter()
            .all(|range| range.api_key != ApiKey::AlterTopicConfig as u16)
    );

    let performance = TopicPerformanceConfigV1 {
        compression: TopicCompressionV1::Zstd,
        max_record_bytes: 1024 * 1024,
        max_batch_records: 128,
        max_batch_uncompressed_bytes: 4 * 1024 * 1024,
    };
    let create = send_request_version(
        &mut raw,
        ApiKey::CreateTopic,
        1,
        2,
        CreateTopicRequestV1 {
            topic_name: "p8-wire-topic".into(),
            partition_count: 1,
            replication_factor: 1,
            performance,
            tags: Vec::new(),
        }
        .encode_body()
        .unwrap(),
    )
    .await;
    let create = CreateTopicResponseV1::decode_body(&create.body).unwrap();
    assert_eq!(create.performance, performance);

    let describe = send_request_version(
        &mut raw,
        ApiKey::DescribeTopic,
        1,
        3,
        DescribeTopicRequestV1 {
            topic_name: "p8-wire-topic".into(),
            tags: Vec::new(),
        }
        .encode_body()
        .unwrap(),
    )
    .await;
    let describe = DescribeTopicResponseV1::decode_body(&describe.body).unwrap();
    assert_eq!(describe.topic_id, create.topic_id);
    assert_eq!(describe.performance, performance);

    let mut client_config = ClientConfig::new(broker.local_addr().to_string());
    client_config.request_timeout = Duration::from_secs(3);
    let client = AstraClient::connect(client_config).await.unwrap();
    let ready_deadline = time::Instant::now() + Duration::from_secs(10);
    loop {
        let metadata = client.refresh_metadata().await.unwrap();
        let ready = metadata
            .topics
            .iter()
            .find(|topic| topic.topic_id == create.topic_id)
            .is_some_and(|topic| {
                topic.partitions.len() == 1
                    && topic.partitions[0].leader_broker_id == 1
                    && topic.partitions[0].leader_epoch != UNKNOWN_EPOCH
            });
        if ready {
            break;
        }
        assert!(
            time::Instant::now() < ready_deadline,
            "P8 compressed topic actor did not become ready"
        );
        time::sleep(Duration::from_millis(25)).await;
    }

    let values = (0_u8..6)
        .map(|ordinal| vec![b'A' + ordinal; 4096])
        .collect::<Vec<_>>();

    let delayed_v0b = async {
        time::sleep(Duration::from_millis(10)).await;
        client
            .produce_buffered(
                "p8-wire-topic",
                Some(b"p8-key".to_vec()),
                Some(values[1].clone()),
                Vec::new(),
                AckMode::Quorum,
            )
            .await
    };
    let (v0a, v0b) = tokio::join!(
        client.produce_buffered(
            "p8-wire-topic",
            Some(b"p8-key".to_vec()),
            Some(values[0].clone()),
            Vec::new(),
            AckMode::Quorum,
        ),
        delayed_v0b,
    );
    let v0a = v0a.unwrap();
    let v0b = v0b.unwrap();
    assert_eq!((v0a.base_offset, v0b.base_offset), (Some(0), Some(1)));

    client.init_producer().await.unwrap();
    let delayed_v1b = async {
        time::sleep(Duration::from_millis(10)).await;
        client
            .produce_idempotent_buffered(
                "p8-wire-topic",
                WireRecordV0 {
                    timestamp_ms: 101,
                    key: Some(b"p8-key".to_vec()),
                    value: Some(values[3].clone()),
                    headers: Vec::new(),
                },
            )
            .await
    };
    let (v1a, v1b) = tokio::join!(
        client.produce_idempotent_buffered(
            "p8-wire-topic",
            WireRecordV0 {
                timestamp_ms: 100,
                key: Some(b"p8-key".to_vec()),
                value: Some(values[2].clone()),
                headers: Vec::new(),
            },
        ),
        delayed_v1b,
    );
    let v1a = v1a.unwrap();
    let v1b = v1b.unwrap();
    assert_eq!((v1a.base_offset, v1b.base_offset), (2, 3));

    let explicit = client.produce_buffered(
        "p8-wire-topic",
        Some(b"p8-key".to_vec()),
        Some(values[4].clone()),
        Vec::new(),
        AckMode::Quorum,
    );
    let flush = async {
        time::sleep(Duration::from_millis(25)).await;
        client.flush_producer_batches().await
    };
    let (explicit, flush) = tokio::join!(explicit, flush);
    flush.unwrap();
    assert_eq!(explicit.unwrap().base_offset, Some(4));

    let shutdown_record = client.produce_buffered(
        "p8-wire-topic",
        Some(b"p8-key".to_vec()),
        Some(values[5].clone()),
        Vec::new(),
        AckMode::Quorum,
    );
    let shutdown = async {
        time::sleep(Duration::from_millis(25)).await;
        client.close_with_drain().await
    };
    let (shutdown_record, shutdown) = tokio::join!(shutdown_record, shutdown);
    shutdown.unwrap();
    assert_eq!(shutdown_record.unwrap().base_offset, Some(5));

    let verifier = AstraClient::connect(ClientConfig::new(broker.local_addr().to_string()))
        .await
        .unwrap();
    let fetched = verifier
        .fetch(FetchRequestV0 {
            isolation: IsolationLevel::ReadCommitted,
            min_bytes: 0,
            max_wait_ms: 0,
            partitions: vec![FetchPartitionRequestV0 {
                topic_id: create.topic_id,
                partition: 0,
                leader_epoch: UNKNOWN_EPOCH,
                fetch_offset: 0,
                max_bytes: 64 * 1024,
                max_records: 10,
            }],
            tags: Vec::new(),
        })
        .await
        .unwrap();
    assert_eq!(fetched.partitions[0].records.len(), values.len());
    for (record, expected) in fetched.partitions[0].records.iter().zip(&values) {
        assert_eq!(record.record.value.as_deref(), Some(expected.as_slice()));
    }
    verifier.close().await;

    let alter =
        send_request_raw_version(&mut raw, ApiKey::AlterTopicConfig, 0, 4, Vec::new()).await;
    assert_eq!(
        alter.error_code,
        astralog_protocol::PublicError::UnsupportedApiKey as u16
    );

    broker.shutdown().await.unwrap();

    let topic_hex = create
        .topic_id
        .as_bytes()
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect::<String>();
    let partition_dir = root
        .join("broker-1")
        .join("partitions")
        .join(topic_hex)
        .join("0000000000");
    let mut segment_paths = fs::read_dir(&partition_dir)
        .unwrap()
        .map(|entry| entry.unwrap().path())
        .filter(|path| path.extension().is_some_and(|extension| extension == "log"))
        .collect::<Vec<_>>();
    segment_paths.sort();
    assert!(!segment_paths.is_empty());
    let mut data_codecs = Vec::new();
    let mut stored_logical_records = 0_usize;
    for path in segment_paths {
        let scan = scan_segment(&path, StorageLimits::default()).unwrap();
        assert!(scan.failure.is_none());
        for frame in scan.frames {
            if let StorageFrame::Data(batch) = frame.frame {
                data_codecs.push(batch.compression);
                stored_logical_records += batch.records.len();
            }
        }
    }
    assert_eq!(stored_logical_records, values.len());
    assert!(!data_codecs.is_empty());
    assert!(
        data_codecs
            .iter()
            .all(|codec| *codec == StorageCompression::Zstd)
    );

    for (_, controller) in controllers {
        controller.shutdown().await;
    }
    fs::remove_dir_all(root).unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 6)]
async fn p9e_topic_v2_alter_and_compaction_policy_gate_are_level_five_committed() {
    let _test_guard = p5_integration_lock().lock().await;
    let root = temp_dir("p9-topic-policy");
    let controller_addresses = [reserve_addr(), reserve_addr(), reserve_addr()];
    let mut controllers = BTreeMap::new();
    for id in 1..=3_u64 {
        controllers.insert(
            cid(id),
            start_controller_node(p9_controller_config(&root, id, &controller_addresses))
                .await
                .unwrap(),
        );
    }
    let _ = stable_controller_leader(&controllers).await;

    let broker_client_addr = reserve_addr();
    let broker_raft_addr = reserve_addr();
    let registered = mutate(
        &controllers,
        ControllerCommand::RegisterBroker {
            broker_id: 1,
            node_uuid: [1; 16],
            initial_incarnation: 1,
            client_host: broker_client_addr.ip().to_string(),
            client_port: broker_client_addr.port(),
            internal_host: broker_raft_addr.ip().to_string(),
            internal_port: broker_raft_addr.port(),
            rack: Some("rack-1".into()),
            zone: Some("zone-1".into()),
            min_feature: 1,
            max_feature: 5,
        },
    )
    .await;
    let activated = mutate(
        &controllers,
        ControllerCommand::SetBrokerLifecycle {
            broker_id: 1,
            expected_incarnation: 1,
            expected_lifecycle: BrokerLifecycle::Joining,
            new_lifecycle: BrokerLifecycle::Active,
        },
    )
    .await;
    let leader = failure_detector_tick_on_current_leader(&controllers, 100).await;
    controllers[&leader]
        .heartbeat(BrokerHeartbeatRequest {
            authority: BrokerAuthority {
                broker_id: 1,
                node_uuid: [1; 16],
                incarnation_epoch: 1,
            },
            applied_metadata_epoch: registered.metadata_epoch.max(activated.metadata_epoch),
            min_feature: 1,
            max_feature: 5,
            replica_progress: Vec::new(),
            movement_progress: Vec::new(),
        })
        .await
        .unwrap();
    let leader = stable_controller_leader(&controllers).await;
    let bumped = controllers[&leader].bump_feature_level(5).await.unwrap();
    let bump_deadline = time::Instant::now() + Duration::from_secs(10);
    loop {
        let snapshot = controller_snapshot(&controllers).await;
        if snapshot.epoch >= bumped.metadata_epoch && snapshot.state.active_feature_level() == 5 {
            break;
        }
        assert!(time::Instant::now() < bump_deadline);
        time::sleep(Duration::from_millis(25)).await;
    }

    let clients = controllers
        .values()
        .map(ControllerNodeHandle::client)
        .collect();
    let mut broker_config = BrokerConfig::new(root.join("broker-1"), broker_client_addr);
    broker_config.advertised_host = broker_client_addr.ip().to_string();
    broker_config.advertised_port = broker_client_addr.port();
    broker_config.broker_id = 1;
    let mut dynamic_replication = DynamicReplicationConfig::new(broker_raft_addr);
    dynamic_replication.tick_interval = Duration::from_millis(20);
    let mut dynamic = DynamicControllerConfig::new(clients, dynamic_replication);
    dynamic.metadata_refresh_interval = Duration::from_millis(25);
    broker_config.dynamic_controller = Some(dynamic);
    let broker = start_dynamic_broker_when_controller_ready(broker_config, &controllers).await;

    let mut raw = raw_hello(broker.local_addr()).await;
    let versions = send_request_version(
        &mut raw,
        ApiKey::ApiVersions,
        0,
        1,
        ApiVersionsRequestV0 {
            client_software_name: "p9-test".into(),
            client_software_version: "0.1".into(),
            tags: Vec::new(),
        }
        .encode_body()
        .unwrap(),
    )
    .await;
    let versions = ApiVersionsResponseV0::decode_body(&versions.body).unwrap();
    let p9 = versions
        .features
        .iter()
        .find(|feature| feature.feature_key == 15)
        .unwrap();
    assert_eq!(
        (
            p9.min_supported_version,
            p9.max_supported_version,
            p9.active_version
        ),
        (1, 1, 1)
    );
    for api_key in [ApiKey::CreateTopic, ApiKey::DescribeTopic] {
        let range = versions
            .apis
            .iter()
            .find(|range| range.api_key == api_key as u16)
            .unwrap();
        assert_eq!((range.min_api_version, range.max_api_version), (0, 2));
    }
    assert!(
        versions
            .apis
            .iter()
            .any(|range| range.api_key == ApiKey::AlterTopicConfig as u16)
    );

    let performance = TopicPerformanceConfigV1 {
        compression: TopicCompressionV1::Lz4,
        max_record_bytes: 1024 * 1024,
        max_batch_records: 64,
        max_batch_uncompressed_bytes: 2 * 1024 * 1024,
    };
    let retention = TopicRetentionConfigV1 {
        cleanup_policy: TopicCleanupPolicyV1::CompactDelete,
        retention_ms: Some(60_000),
        retention_bytes: Some(1_000_000),
        tombstone_grace_ms: 30_000,
    };
    let create = send_request_version(
        &mut raw,
        ApiKey::CreateTopic,
        2,
        2,
        CreateTopicRequestV2 {
            topic_name: "p9-wire-topic".into(),
            partition_count: 1,
            replication_factor: 1,
            performance,
            retention,
            tags: Vec::new(),
        }
        .encode_body()
        .unwrap(),
    )
    .await;
    let create = CreateTopicResponseV2::decode_body(&create.body).unwrap();
    assert_eq!(create.performance, performance);
    assert_eq!(create.retention, retention);

    let describe = send_request_version(
        &mut raw,
        ApiKey::DescribeTopic,
        2,
        3,
        DescribeTopicRequestV2 {
            topic_name: "p9-wire-topic".into(),
            tags: Vec::new(),
        }
        .encode_body()
        .unwrap(),
    )
    .await;
    let describe = DescribeTopicResponseV2::decode_body(&describe.body).unwrap();
    assert_eq!(describe.topic_id, create.topic_id);
    assert_eq!(describe.performance, performance);
    assert_eq!(describe.retention, retention);

    let metadata_client = AstraClient::connect(ClientConfig::new(broker.local_addr().to_string()))
        .await
        .unwrap();
    let leader_deadline = time::Instant::now() + Duration::from_secs(15);
    let leader_epoch = loop {
        let metadata = metadata_client.refresh_metadata().await.unwrap();
        if let Some(partition) = metadata
            .topics
            .iter()
            .find(|topic| topic.topic_id == create.topic_id)
            .map(|topic| &topic.partitions[0])
            && partition.leader_broker_id == 1
            && partition.leader_epoch != UNKNOWN_EPOCH
        {
            break partition.leader_epoch;
        }
        assert!(time::Instant::now() < leader_deadline);
        time::sleep(Duration::from_millis(25)).await;
    };
    let invalid_limits = CompactionLimits {
        max_source_segments: 0,
        ..CompactionLimits::default()
    };
    let compact_gate_deadline = time::Instant::now() + Duration::from_secs(15);
    loop {
        match broker
            .run_keyed_compaction_once(create.topic_id, 0, leader_epoch, invalid_limits)
            .await
        {
            Err(PublicError::StorageError) => break,
            Err(PublicError::StaleEpoch | PublicError::BrokerNotAvailable) => {}
            other => panic!("CompactDelete must reach bounded cleaner validation, got {other:?}"),
        }
        assert!(time::Instant::now() < compact_gate_deadline);
        time::sleep(Duration::from_millis(25)).await;
    }

    let altered = TopicRetentionConfigV1 {
        cleanup_policy: TopicCleanupPolicyV1::Delete,
        retention_ms: None,
        retention_bytes: Some(8192),
        tombstone_grace_ms: 45_000,
    };
    let alter = send_request_version(
        &mut raw,
        ApiKey::AlterTopicConfig,
        0,
        4,
        AlterTopicConfigRequestV0 {
            topic_name: "p9-wire-topic".into(),
            retention: TopicRetentionPatchV1 {
                cleanup_policy: Some(TopicCleanupPolicyV1::Delete),
                retention_ms: Some(None),
                retention_bytes: Some(Some(8192)),
                tombstone_grace_ms: Some(45_000),
            },
            tags: Vec::new(),
        }
        .encode_body()
        .unwrap(),
    )
    .await;
    let alter = AlterTopicConfigResponseV0::decode_body(&alter.body).unwrap();
    assert_eq!(alter.topic_id, create.topic_id);
    assert!(alter.metadata_epoch > create.metadata_epoch);
    assert_eq!(alter.retention, altered);

    let describe = send_request_version(
        &mut raw,
        ApiKey::DescribeTopic,
        2,
        5,
        DescribeTopicRequestV2 {
            topic_name: "p9-wire-topic".into(),
            tags: Vec::new(),
        }
        .encode_body()
        .unwrap(),
    )
    .await;
    let describe = DescribeTopicResponseV2::decode_body(&describe.body).unwrap();
    assert_eq!(describe.retention, altered);
    assert_eq!(describe.metadata_epoch, alter.metadata_epoch);

    let delete_gate_deadline = time::Instant::now() + Duration::from_secs(15);
    loop {
        match broker
            .run_keyed_compaction_once(create.topic_id, 0, leader_epoch, invalid_limits)
            .await
        {
            Ok(None) => break,
            Err(PublicError::StorageError | PublicError::StaleEpoch) => {}
            other => panic!("Delete policy must disable keyed compaction, got {other:?}"),
        }
        assert!(time::Instant::now() < delete_gate_deadline);
        time::sleep(Duration::from_millis(25)).await;
    }
    metadata_client.close().await;

    let snapshot = controller_snapshot(&controllers).await;
    let committed = snapshot
        .state
        .topic_retention_policy(create.topic_id)
        .unwrap();
    assert_eq!(committed.retention_ms, None);
    assert_eq!(committed.retention_bytes, Some(8192));
    assert_eq!(committed.tombstone_grace_ms, 45_000);
    assert_eq!(
        committed.subscription_protection,
        astralog_controller::SubscriptionProtectionV1::Disabled
    );

    broker.shutdown().await.unwrap();
    for (_, controller) in controllers {
        controller.shutdown().await;
    }
    fs::remove_dir_all(root).unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 12)]
async fn preferred_leader_hook_validates_catchup_invokes_p3_and_only_reports_observed_result() {
    let _test_guard = p5_integration_lock().lock().await;
    let root = temp_dir("preferred-leader");
    let controller_addresses = [reserve_addr(), reserve_addr(), reserve_addr()];
    let mut controllers = BTreeMap::new();
    for id in 1..=3_u64 {
        controllers.insert(
            cid(id),
            start_controller_node(controller_config(&root, id, &controller_addresses))
                .await
                .unwrap(),
        );
    }
    let _ = stable_controller_leader(&controllers).await;

    let client_addresses = (0..4).map(|_| reserve_addr()).collect::<Vec<_>>();
    let raft_addresses = (0..4).map(|_| reserve_addr()).collect::<Vec<_>>();
    for broker_id in 1..=4_u32 {
        let index = usize::try_from(broker_id - 1).unwrap();
        mutate(
            &controllers,
            ControllerCommand::RegisterBroker {
                broker_id,
                node_uuid: [u8::try_from(broker_id).unwrap(); 16],
                initial_incarnation: 1,
                client_host: client_addresses[index].ip().to_string(),
                client_port: client_addresses[index].port(),
                internal_host: raft_addresses[index].ip().to_string(),
                internal_port: raft_addresses[index].port(),
                rack: Some(format!("rack-{broker_id}")),
                zone: Some(format!("zone-{broker_id}")),
                min_feature: 1,
                max_feature: 1,
            },
        )
        .await;
        mutate(
            &controllers,
            ControllerCommand::SetBrokerLifecycle {
                broker_id,
                expected_incarnation: 1,
                expected_lifecycle: BrokerLifecycle::Joining,
                new_lifecycle: BrokerLifecycle::Active,
            },
        )
        .await;
    }

    let controller_clients = controllers
        .values()
        .map(ControllerNodeHandle::client)
        .collect::<Vec<_>>();
    let mut brokers = Vec::new();
    for broker_id in 1..=4_u32 {
        let index = usize::try_from(broker_id - 1).unwrap();
        let mut config = BrokerConfig::new(
            root.join(format!("broker-{broker_id}")),
            client_addresses[index],
        );
        config.advertised_host = client_addresses[index].ip().to_string();
        config.advertised_port = client_addresses[index].port();
        config.broker_id = broker_id;
        let mut replication = DynamicReplicationConfig::new(raft_addresses[index]);
        replication.tick_interval = Duration::from_millis(20);
        // P5-M is qualifying an explicit leadership-transfer hook, not election
        // latency. Keep deterministic but wider election windows so ordinary
        // scheduler jitter cannot race the transfer on every broker probe.
        let election_min = 25 + u64::from(broker_id - 1) * 10;
        replication.election_timeout_min_ticks = election_min;
        replication.election_timeout_max_ticks = election_min + 1;
        let mut dynamic = DynamicControllerConfig::new(controller_clients.clone(), replication);
        // P5-P deliberately slows the control loop enough to sustain several
        // quorum writes while the movement is still active. This is test-only
        // pacing, not a production movement/election timing change.
        dynamic.metadata_refresh_interval = Duration::from_millis(100);
        dynamic.preferred_leader_timeout = Duration::from_secs(10);
        dynamic.max_pending_admin_operations = 16;
        config.dynamic_controller = Some(dynamic);
        brokers.push(start_dynamic_broker_when_controller_ready(config, &controllers).await);
    }

    let liveness_deadline = time::Instant::now() + Duration::from_secs(10);
    loop {
        let leader = failure_detector_tick_on_current_leader(&controllers, 1_000).await;
        let mut healthy = true;
        for broker_id in 1..=4_u32 {
            healthy &= controllers[&leader]
                .broker_liveness(broker_id)
                .await
                .is_ok_and(|observation| observation.state == BrokerLivenessState::Healthy);
        }
        if healthy {
            break;
        }
        assert!(
            time::Instant::now() < liveness_deadline,
            "P5-M brokers did not become healthy at the controller"
        );
        time::sleep(Duration::from_millis(25)).await;
    }

    let created = create_topic_on_current_leader(
        &controllers,
        CreateTopicSpec {
            topic_name: "leadership-hook".into(),
            partition_count: 1,
            replication_factor: 3,
            failure_domain_policy: FailureDomainPolicy::Disabled,
        },
    )
    .await;
    assert_eq!(created.assignments[0].voters, vec![1, 2, 3]);

    let mut client_config = ClientConfig::new(brokers[0].local_addr().to_string());
    client_config.request_timeout = Duration::from_secs(5);
    let client = AstraClient::connect(client_config).await.unwrap();
    let leader_deadline = time::Instant::now() + Duration::from_secs(10);
    let (initial_leader, initial_epoch) = loop {
        let metadata = client.refresh_metadata().await.unwrap();
        let Some(topic) = metadata
            .topics
            .iter()
            .find(|topic| topic.topic_id == created.topic.topic_id)
        else {
            assert!(
                time::Instant::now() < leader_deadline,
                "P5-M topic did not reach broker metadata before leader observation"
            );
            time::sleep(Duration::from_millis(25)).await;
            continue;
        };
        let partition = &topic.partitions[0];
        if partition.leader_broker_id != UNKNOWN_BROKER_ID
            && partition.leader_epoch != UNKNOWN_EPOCH
        {
            break (partition.leader_broker_id, partition.leader_epoch);
        }
        assert!(
            time::Instant::now() < leader_deadline,
            "P5-M RF3 partition did not elect a leader"
        );
        time::sleep(Duration::from_millis(25)).await;
    };

    quorum_produce_with_refresh(
        &client,
        "leadership-hook",
        b"before".to_vec(),
        b"before-transfer".to_vec(),
        Duration::from_secs(10),
        "before P5-M preferred-leader transfer",
    )
    .await;

    let catchup_deadline = time::Instant::now() + Duration::from_secs(10);
    loop {
        let controller = current_leader(&controllers).await;
        let mut committed = Vec::new();
        let mut complete = true;
        for broker_id in &created.assignments[0].voters {
            match controllers[&controller]
                .client()
                .replica_progress_observation(
                    *broker_id,
                    created.topic.topic_id,
                    0,
                    created.assignments[0].assignment_generation,
                )
                .await
            {
                Ok(Some(observation)) => committed.push(observation.report.committed_raft_index),
                Ok(None) | Err(_) => {
                    complete = false;
                    break;
                }
            }
        }
        if complete
            && committed.len() == created.assignments[0].voters.len()
            && committed[0] > 0
            && committed.windows(2).all(|window| window[0] == window[1])
        {
            break;
        }
        assert!(
            time::Instant::now() < catchup_deadline,
            "P5-M target voters did not converge through a common committed P3 boundary"
        );
        time::sleep(Duration::from_millis(25)).await;
    }

    let controller_before = controller_snapshot(&controllers).await;
    let metadata_epoch_before = controller_before.epoch;
    assert!(controller_before.state.movements().is_empty());

    let initial_leader_index = usize::try_from(initial_leader - 1).unwrap();
    assert!(
        brokers[initial_leader_index]
            .request_preferred_leader(PreferredLeaderRequest {
                topic_id: created.topic.topic_id,
                partition_id: 0,
                target_broker_id: 4,
            })
            .await
            .is_err(),
        "a non-voter must never be accepted as a preferred-leader target"
    );
    assert_eq!(
        controller_snapshot(&controllers).await.epoch,
        metadata_epoch_before
    );

    let transfer_deadline = time::Instant::now() + Duration::from_secs(10);
    let mut last_transfer_errors = BTreeMap::new();
    let mut requested_target = created.assignments[0]
        .voters
        .iter()
        .copied()
        .find(|broker_id| *broker_id != initial_leader)
        .unwrap();
    let (target, outcome) = 'transfer: loop {
        let request = PreferredLeaderRequest {
            topic_id: created.topic.topic_id,
            partition_id: 0,
            target_broker_id: requested_target,
        };
        let (one, two, three) = tokio::join!(
            brokers[0].request_preferred_leader(request),
            brokers[1].request_preferred_leader(request),
            brokers[2].request_preferred_leader(request),
        );
        for (source, result) in [(1_u32, one), (2_u32, two), (3_u32, three)] {
            match result {
                Ok(outcome)
                    if outcome.disposition == PreferredLeaderDisposition::TransferObserved =>
                {
                    break 'transfer (requested_target, outcome);
                }
                Ok(outcome)
                    if outcome.disposition == PreferredLeaderDisposition::AlreadyPreferred =>
                {
                    requested_target = created.assignments[0]
                        .voters
                        .iter()
                        .copied()
                        .find(|broker_id| *broker_id != outcome.target_broker_id)
                        .unwrap();
                }
                Ok(outcome) => panic!("unexpected P5-M disposition: {outcome:?}"),
                Err(
                    error @ BrokerError::InvalidConfig(
                        "preferred-leader hook must be invoked on the current P3 leader",
                    ),
                )
                | Err(
                    error @ BrokerError::Raft(RaftError::InvalidLeadershipTransfer(
                        "local node is not leader",
                    )),
                ) => {
                    last_transfer_errors.insert(source, error.to_string());
                }
                Err(error) => panic!("unexpected P5-M preferred-leader failure: {error}"),
            }
        }
        if time::Instant::now() >= transfer_deadline {
            let metadata = client.refresh_metadata().await;
            panic!(
                "P5-M could not reach any committed voter currently holding P3 leadership; last_errors={last_transfer_errors:?}; metadata={metadata:?}"
            );
        }
        time::sleep(Duration::from_millis(20)).await;
    };
    assert_eq!(outcome.target_broker_id, target);
    assert_eq!(
        outcome.disposition,
        PreferredLeaderDisposition::TransferObserved
    );
    assert!(outcome.observed_leader_epoch > initial_epoch);

    let observe_deadline = time::Instant::now() + Duration::from_secs(10);
    loop {
        let metadata = client.refresh_metadata().await.unwrap();
        let partition = &metadata
            .topics
            .iter()
            .find(|topic| topic.topic_id == created.topic.topic_id)
            .unwrap()
            .partitions[0];
        if partition.leader_broker_id == target
            && partition.leader_epoch >= outcome.observed_leader_epoch
        {
            break;
        }
        assert!(
            time::Instant::now() < observe_deadline,
            "public Metadata did not observe the P3 preferred-leader transfer"
        );
        time::sleep(Duration::from_millis(25)).await;
    }

    let already = brokers[usize::try_from(target - 1).unwrap()]
        .request_preferred_leader(PreferredLeaderRequest {
            topic_id: created.topic.topic_id,
            partition_id: 0,
            target_broker_id: target,
        })
        .await
        .unwrap();
    assert_eq!(
        already.disposition,
        PreferredLeaderDisposition::AlreadyPreferred
    );
    assert_eq!(already.target_broker_id, target);
    assert_eq!(
        controller_snapshot(&controllers).await.epoch,
        metadata_epoch_before
    );
    assert!(
        controller_snapshot(&controllers)
            .await
            .state
            .movements()
            .is_empty()
    );

    let after = quorum_produce_with_refresh(
        &client,
        "leadership-hook",
        b"after".to_vec(),
        b"after-transfer".to_vec(),
        Duration::from_secs(10),
        "after P5-M preferred-leader transfer",
    )
    .await;
    let fetched = client
        .fetch(FetchRequestV0 {
            isolation: IsolationLevel::ReadCommitted,
            min_bytes: 0,
            max_wait_ms: 0,
            partitions: vec![FetchPartitionRequestV0 {
                topic_id: created.topic.topic_id,
                partition: 0,
                leader_epoch: UNKNOWN_EPOCH,
                fetch_offset: after.base_offset.unwrap(),
                max_bytes: 4096,
                max_records: 1,
            }],
            tags: Vec::new(),
        })
        .await
        .unwrap();
    assert_eq!(fetched.partitions[0].records.len(), 1);
    assert_eq!(
        fetched.partitions[0].records[0].record.value.as_deref(),
        Some(b"after-transfer".as_slice())
    );

    client.close().await;
    for broker in brokers {
        broker.shutdown().await.unwrap();
    }
    for (_, controller) in controllers {
        controller.shutdown().await;
    }
    fs::remove_dir_all(root).unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 12)]
async fn rf3_reassignment_executes_learner_joint_consensus_and_preserves_source_bytes() {
    let _test_guard = p5_integration_lock().lock().await;
    let root = temp_dir("movement-rf3");
    let controller_addresses = [reserve_addr(), reserve_addr(), reserve_addr()];
    let mut controllers = BTreeMap::new();
    for id in 1..=3_u64 {
        controllers.insert(
            cid(id),
            start_controller_node(controller_config(&root, id, &controller_addresses))
                .await
                .unwrap(),
        );
    }
    let _ = stable_controller_leader(&controllers).await;

    let client_addresses = (0..4).map(|_| reserve_addr()).collect::<Vec<_>>();
    let raft_addresses = (0..4).map(|_| reserve_addr()).collect::<Vec<_>>();
    for broker_id in 1..=4_u32 {
        let index = usize::try_from(broker_id - 1).unwrap();
        mutate(
            &controllers,
            ControllerCommand::RegisterBroker {
                broker_id,
                node_uuid: [u8::try_from(broker_id).unwrap(); 16],
                initial_incarnation: 1,
                client_host: client_addresses[index].ip().to_string(),
                client_port: client_addresses[index].port(),
                internal_host: raft_addresses[index].ip().to_string(),
                internal_port: raft_addresses[index].port(),
                rack: Some(format!("rack-{broker_id}")),
                zone: Some(format!("zone-{broker_id}")),
                min_feature: 1,
                max_feature: 1,
            },
        )
        .await;
        mutate(
            &controllers,
            ControllerCommand::SetBrokerLifecycle {
                broker_id,
                expected_incarnation: 1,
                expected_lifecycle: BrokerLifecycle::Joining,
                new_lifecycle: BrokerLifecycle::Active,
            },
        )
        .await;
    }

    let controller_clients = controllers
        .values()
        .map(ControllerNodeHandle::client)
        .collect::<Vec<_>>();
    let mut brokers = Vec::new();
    for broker_id in 1..=4_u32 {
        let index = usize::try_from(broker_id - 1).unwrap();
        let mut config = BrokerConfig::new(
            root.join(format!("broker-{broker_id}")),
            client_addresses[index],
        );
        config.advertised_host = client_addresses[index].ip().to_string();
        config.advertised_port = client_addresses[index].port();
        config.broker_id = broker_id;
        let mut replication = DynamicReplicationConfig::new(raft_addresses[index]);
        // Match the frozen P4 correctness harness: this is not an election-
        // latency benchmark, so retain deterministic non-overlapping tick
        // windows with enough wall-clock headroom under repeated CI load.
        replication.tick_interval = Duration::from_millis(20);
        let election_min = 5 + u64::from(broker_id - 1) * 3;
        replication.election_timeout_min_ticks = election_min;
        replication.election_timeout_max_ticks = election_min + 1;
        let mut dynamic = DynamicControllerConfig::new(controller_clients.clone(), replication);
        // P5-P requires at least three quorum acknowledgements before the movement
        // reaches Complete. Slow only this qualification control loop so the test
        // proves sustained availability instead of racing a faster reconciler.
        dynamic.metadata_refresh_interval = Duration::from_millis(100);
        dynamic.max_pending_admin_operations = 16;
        config.dynamic_controller = Some(dynamic);
        brokers.push(start_dynamic_broker_when_controller_ready(config, &controllers).await);
    }

    let liveness_deadline = time::Instant::now() + Duration::from_secs(10);
    loop {
        let leader = failure_detector_tick_on_current_leader(&controllers, 1_000).await;
        let mut healthy = true;
        for broker_id in 1..=4_u32 {
            healthy &= controllers[&leader]
                .broker_liveness(broker_id)
                .await
                .is_ok_and(|observation| observation.state == BrokerLivenessState::Healthy);
        }
        if healthy {
            break;
        }
        assert!(
            time::Instant::now() < liveness_deadline,
            "dynamic brokers did not become healthy at the controller"
        );
        time::sleep(Duration::from_millis(25)).await;
    }

    let created = create_topic_on_current_leader(
        &controllers,
        CreateTopicSpec {
            topic_name: "movement-orders".into(),
            partition_count: 1,
            replication_factor: 3,
            failure_domain_policy: FailureDomainPolicy::Disabled,
        },
    )
    .await;
    assert_eq!(created.assignments[0].voters, vec![1, 2, 3]);

    let mut client_config = ClientConfig::new(brokers[0].local_addr().to_string());
    client_config.request_timeout = Duration::from_secs(5);
    let client = AstraClient::connect(client_config).await.unwrap();
    let leader_deadline = time::Instant::now() + Duration::from_secs(10);
    loop {
        let metadata = client.refresh_metadata().await.unwrap();
        let Some(topic) = metadata
            .topics
            .iter()
            .find(|topic| topic.topic_id == created.topic.topic_id)
        else {
            assert!(time::Instant::now() < leader_deadline);
            time::sleep(Duration::from_millis(25)).await;
            continue;
        };
        let partition = topic.partitions[0].clone();
        if partition.leader_broker_id != UNKNOWN_BROKER_ID
            && partition.leader_epoch != UNKNOWN_EPOCH
        {
            break;
        }
        assert!(
            time::Instant::now() < leader_deadline,
            "RF3 dynamic partition did not elect a leader"
        );
        time::sleep(Duration::from_millis(25)).await;
    }

    for value in [b"before-1".as_slice(), b"before-2".as_slice()] {
        quorum_produce_with_refresh(
            &client,
            "movement-orders",
            b"key".to_vec(),
            value.to_vec(),
            Duration::from_secs(10),
            "before movement",
        )
        .await;
    }

    let movement = loop {
        let leader = current_leader(&controllers).await;
        match controllers[&leader]
            .begin_reassignment(ReassignmentSpec {
                topic_id: created.topic.topic_id,
                partition_id: 0,
                source_broker_id: 1,
                destination_broker_id: 4,
                preferred_leader: Some(4),
            })
            .await
        {
            Ok(result) => break result,
            Err(astralog_controller::ControllerError::NotLeader { .. }) => continue,
            Err(error) => panic!("reassignment admission failed: {error}"),
        }
    };

    client.close().await;
    let source = brokers.remove(0);

    let mut writer_config = ClientConfig::new(brokers[0].local_addr().to_string());
    writer_config.request_timeout = Duration::from_secs(5);
    let movement_writer = AstraClient::connect(writer_config).await.unwrap();
    // P5-P qualifies sustained, paced quorum traffic while reconciliation is
    // active. The pacing gives the learner ordinary P4/P3 catch-up opportunity
    // while still proving that multiple acknowledged writes survive the move.
    let write_campaign = async {
        let mut movement_acks = Vec::new();
        let mut unacknowledged_unique_keys = Vec::new();
        let mut writes_while_active = 0_usize;
        let write_deadline = time::Instant::now() + Duration::from_secs(40);
        // Feature level 1 deliberately has no P7 idempotent producer. An
        // ambiguous P2 write must NEVER be retried with the same unique key
        // and payload. Keep a bounded supply of distinct new operations and
        // count *only* quorum-acknowledged writes toward the P5 proof.
        for sequence in 0..24_u32 {
            let key = format!("moving-{sequence}").into_bytes();
            let value = format!("movement-write-{sequence}").into_bytes();
            let result = movement_writer
                .produce(
                    "movement-orders",
                    Some(key.clone()),
                    Some(value.clone()),
                    Vec::new(),
                    AckMode::Quorum,
                )
                .await;
            match result {
                Ok(outcome) => {
                    movement_acks.push((
                        outcome
                            .base_offset
                            .expect("quorum Produce must return its original offset"),
                        value,
                    ));
                    let snapshot = controller_snapshot(&controllers).await;
                    if snapshot.state.movements()[&movement.movement.movement_id].stage
                        != MovementStage::Complete
                    {
                        writes_while_active += 1;
                    }
                }
                Err(
                    ClientError::Metadata("topic not present in metadata")
                    | ClientError::Metadata("partition leader is unknown")
                    | ClientError::Metadata("partition leader authority is unknown")
                    | ClientError::Broker(
                        PublicError::NotLeader
                        | PublicError::StaleEpoch
                        | PublicError::BrokerNotAvailable
                        | PublicError::BrokerBusy
                        | PublicError::RequestTimeout,
                    )
                    | ClientError::ConnectionClosed
                    | ClientError::Timeout
                    | ClientError::Io(_)
                    | ClientError::AmbiguousMutation { .. },
                ) => {
                    // Even NotLeader can originate from a proposal whose
                    // leadership was lost after local append. Its outcome is
                    // NOT proof of nonpublication; abandon the exact payload.
                    unacknowledged_unique_keys.push(key);
                    let _ = movement_writer.refresh_metadata().await;
                }
                Err(error) => {
                    panic!(
                        "unexpected P5 sustained-write response; cannot count or safely retry this operation: {error:?}"
                    );
                }
            }
            if movement_acks.len() >= 8 {
                break;
            }
            assert!(
                time::Instant::now() < write_deadline,
                "P5 could not collect eight distinct quorum-acknowledged writes within 40s; acked={}, unacknowledged={}",
                movement_acks.len(),
                unacknowledged_unique_keys.len()
            );
            time::sleep(Duration::from_millis(35)).await;
        }
        assert_eq!(
            movement_acks.len(),
            8,
            "P5 must retain eight truly acknowledged distinct writes even when some independent P2 attempts are ambiguous"
        );
        movement_writer.close().await;
        (
            movement_acks,
            writes_while_active,
            unacknowledged_unique_keys,
        )
    };

    let source_failure = async {
        // Full serialized qualification can leave the learner in CatchingUp
        // longer than the focused P5 run while prior RF3 disk/network work
        // drains. Keep this failure-injection wait bounded without treating
        // slow convergence as a movement-safety failure.
        let deadline = time::Instant::now() + Duration::from_secs(60);
        loop {
            let stage = controller_snapshot(&controllers).await.state.movements()
                [&movement.movement.movement_id]
                .stage;
            if matches!(
                stage,
                MovementStage::PromotionReady
                    | MovementStage::EnterJointRequested
                    | MovementStage::JointCommitted
                    | MovementStage::LeaveJointRequested
                    | MovementStage::NewStableCommitted
                    | MovementStage::LeadershipTransfer
            ) {
                source.shutdown().await.unwrap();
                break stage;
            }
            assert!(
                !matches!(
                    stage,
                    MovementStage::OldReplicaRemoveRequested
                        | MovementStage::OldReplicaRemoved
                        | MovementStage::Complete
                ),
                "movement passed the caught-up/source-failure injection point; stage={stage:?}"
            );
            assert!(
                time::Instant::now() < deadline,
                "destination did not become promotion-ready before source-failure injection; stage={stage:?}"
            );
            time::sleep(Duration::from_millis(5)).await;
        }
    };

    let ((movement_acks, writes_while_active, unacknowledged_unique_keys), source_failed_at) =
        tokio::join!(write_campaign, source_failure);
    assert!(
        writes_while_active >= 3,
        "sustained-write qualification must acknowledge several writes while movement is active; observed={writes_while_active}"
    );
    assert!(matches!(
        source_failed_at,
        MovementStage::PromotionReady
            | MovementStage::EnterJointRequested
            | MovementStage::JointCommitted
            | MovementStage::LeaveJointRequested
            | MovementStage::NewStableCommitted
            | MovementStage::LeadershipTransfer
    ));
    time::sleep(Duration::from_millis(50)).await;

    let movement_deadline = time::Instant::now() + Duration::from_secs(30);
    let final_snapshot = loop {
        // A replacement controller leader deliberately forgets volatile failure/progress
        // observations. Reinjection plus ordinary broker heartbeats repopulates them.
        failure_detector_tick_on_current_leader(&controllers, 1_000).await;
        let snapshot = controller_snapshot(&controllers).await;
        if snapshot.state.movements()[&movement.movement.movement_id].stage
            == MovementStage::Complete
        {
            break snapshot;
        }
        if time::Instant::now() >= movement_deadline {
            let stage = snapshot.state.movements()[&movement.movement.movement_id].stage;
            let controller = current_leader(&controllers).await;
            let progress = controllers[&controller]
                .client()
                .replica_progress_observation(
                    4,
                    created.topic.topic_id,
                    0,
                    astralog_controller::AssignmentGeneration::new(1).unwrap(),
                )
                .await;
            let public = {
                let mut probe_config = ClientConfig::new(brokers[0].local_addr().to_string());
                probe_config.request_timeout = Duration::from_secs(2);
                match AstraClient::connect(probe_config).await {
                    Ok(probe) => {
                        let metadata = probe.refresh_metadata().await;
                        probe.close().await;
                        metadata
                    }
                    Err(error) => Err(error),
                }
            };
            panic!(
                "RF3 reassignment did not reach COMPLETE; stage={stage:?}; destination_progress={progress:?}; public_metadata={public:?}"
            );
        }
        time::sleep(Duration::from_millis(40)).await;
    };

    let assignment = &final_snapshot.state.assignments()[&(created.topic.topic_id, 0)];
    assert_eq!(assignment.voters, vec![2, 3, 4]);
    assert!(assignment.learners.is_empty());
    assert_eq!(assignment.assignment_generation.get(), 2);

    let metadata_deadline = time::Instant::now() + Duration::from_secs(30);
    let post_bootstrap = 'metadata: loop {
        for broker in &brokers {
            let mut probe_config = ClientConfig::new(broker.local_addr().to_string());
            probe_config.request_timeout = Duration::from_secs(2);
            let Ok(probe) = AstraClient::connect(probe_config).await else {
                continue;
            };
            let metadata = probe.refresh_metadata().await;
            if let Ok(metadata) = metadata
                && let Some(topic) = metadata
                    .topics
                    .iter()
                    .find(|topic| topic.topic_id == created.topic.topic_id)
            {
                let partition = &topic.partitions[0];
                if [2, 3, 4].contains(&partition.leader_broker_id)
                    && partition.replica_broker_ids == vec![2, 3, 4]
                {
                    let bootstrap = broker.local_addr().to_string();
                    probe.close().await;
                    break 'metadata bootstrap;
                }
            }
            probe.close().await;
        }
        if time::Instant::now() >= metadata_deadline {
            panic!(
                "no surviving target replica exposed a target-set leader through public Metadata after movement completion"
            );
        }
        time::sleep(Duration::from_millis(25)).await;
    };

    let mut post_client_config = ClientConfig::new(post_bootstrap);
    post_client_config.request_timeout = Duration::from_secs(5);
    let post_client = AstraClient::connect(post_client_config).await.unwrap();

    for (offset, expected_value) in &movement_acks {
        let fetched = post_client
            .fetch(FetchRequestV0 {
                isolation: IsolationLevel::ReadCommitted,
                min_bytes: 0,
                max_wait_ms: 0,
                partitions: vec![FetchPartitionRequestV0 {
                    topic_id: created.topic.topic_id,
                    partition: 0,
                    leader_epoch: UNKNOWN_EPOCH,
                    fetch_offset: *offset,
                    max_bytes: 4096,
                    max_records: 1,
                }],
                tags: Vec::new(),
            })
            .await
            .unwrap();
        assert_eq!(fetched.partitions[0].records.len(), 1);
        assert_eq!(
            fetched.partitions[0].records[0].record.value.as_deref(),
            Some(expected_value.as_slice()),
            "a quorum-acknowledged write was lost across replica movement"
        );
    }

    // A non-acknowledged P2 request may still have committed. It is never
    // resent, counted as an ack or silently discarded as an impossible write.
    // Read the complete short retained suffix from the new P5 leader and
    // require each unique uncertain key to occur AT MOST once.
    let retained = post_client
        .fetch(FetchRequestV0 {
            isolation: IsolationLevel::ReadCommitted,
            min_bytes: 0,
            max_wait_ms: 0,
            partitions: vec![FetchPartitionRequestV0 {
                topic_id: created.topic.topic_id,
                partition: 0,
                leader_epoch: UNKNOWN_EPOCH,
                fetch_offset: 0,
                max_bytes: 128 * 1024,
                max_records: 64,
            }],
            tags: Vec::new(),
        })
        .await
        .unwrap();
    assert_eq!(retained.partitions[0].error_code, 0);
    for key in &unacknowledged_unique_keys {
        let count = retained.partitions[0]
            .records
            .iter()
            .filter(|record| record.record.key.as_deref() == Some(key.as_slice()))
            .count();
        assert!(
            count <= 1,
            "an uncertain P2 movement write must never be retried into duplicate committed history; key={key:?}, count={count}"
        );
    }

    let after = quorum_produce_with_refresh(
        &post_client,
        "movement-orders",
        b"key".to_vec(),
        b"after".to_vec(),
        Duration::from_secs(10),
        "after movement completion",
    )
    .await;
    let fetched = post_client
        .fetch(FetchRequestV0 {
            isolation: IsolationLevel::ReadCommitted,
            min_bytes: 0,
            max_wait_ms: 0,
            partitions: vec![FetchPartitionRequestV0 {
                topic_id: created.topic.topic_id,
                partition: 0,
                leader_epoch: UNKNOWN_EPOCH,
                fetch_offset: after.base_offset.unwrap(),
                max_bytes: 4096,
                max_records: 1,
            }],
            tags: Vec::new(),
        })
        .await
        .unwrap();
    assert_eq!(fetched.partitions[0].records.len(), 1);
    assert_eq!(
        fetched.partitions[0].records[0].record.value.as_deref(),
        Some(b"after".as_slice())
    );

    let mut topic_hex = String::with_capacity(32);
    for byte in created.topic.topic_id.as_bytes() {
        use std::fmt::Write as _;
        write!(&mut topic_hex, "{byte:02x}").unwrap();
    }
    let source_partition_dir = root
        .join("broker-1")
        .join("partitions")
        .join(topic_hex)
        .join("0000000000");
    assert!(
        source_partition_dir.is_dir(),
        "source replica bytes must remain orphaned/non-authoritative after removal"
    );

    post_client.close().await;
    for broker in brokers {
        broker.shutdown().await.unwrap();
    }
    for (_, controller) in controllers {
        controller.shutdown().await;
    }
    fs::remove_dir_all(root).unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 12)]
async fn movement_resumes_after_source_destination_and_controller_leader_failures() {
    let _test_guard = p5_integration_lock().lock().await;
    let root = temp_dir("movement-failures");
    let controller_addresses = [reserve_addr(), reserve_addr(), reserve_addr()];
    let mut controllers = BTreeMap::new();
    for id in 1..=3_u64 {
        controllers.insert(
            cid(id),
            start_controller_node(controller_config(&root, id, &controller_addresses))
                .await
                .unwrap(),
        );
    }
    let _ = stable_controller_leader(&controllers).await;

    let client_addresses = (0..4).map(|_| reserve_addr()).collect::<Vec<_>>();
    let raft_addresses = (0..4).map(|_| reserve_addr()).collect::<Vec<_>>();
    for broker_id in 1..=4_u32 {
        let index = usize::try_from(broker_id - 1).unwrap();
        mutate(
            &controllers,
            ControllerCommand::RegisterBroker {
                broker_id,
                node_uuid: [u8::try_from(broker_id).unwrap(); 16],
                initial_incarnation: 1,
                client_host: client_addresses[index].ip().to_string(),
                client_port: client_addresses[index].port(),
                internal_host: raft_addresses[index].ip().to_string(),
                internal_port: raft_addresses[index].port(),
                rack: Some(format!("rack-{broker_id}")),
                zone: Some(format!("zone-{broker_id}")),
                min_feature: 1,
                max_feature: 1,
            },
        )
        .await;
        mutate(
            &controllers,
            ControllerCommand::SetBrokerLifecycle {
                broker_id,
                expected_incarnation: 1,
                expected_lifecycle: BrokerLifecycle::Joining,
                new_lifecycle: BrokerLifecycle::Active,
            },
        )
        .await;
    }

    let controller_clients = controllers
        .values()
        .map(ControllerNodeHandle::client)
        .collect::<Vec<_>>();
    let make_broker_config = |broker_id: u32| {
        let index = usize::try_from(broker_id - 1).unwrap();
        let mut config = BrokerConfig::new(
            root.join(format!("broker-{broker_id}")),
            client_addresses[index],
        );
        config.advertised_host = client_addresses[index].ip().to_string();
        config.advertised_port = client_addresses[index].port();
        config.broker_id = broker_id;
        let mut replication = DynamicReplicationConfig::new(raft_addresses[index]);
        replication.tick_interval = Duration::from_millis(20);
        let election_min = 5 + u64::from(broker_id - 1) * 3;
        replication.election_timeout_min_ticks = election_min;
        replication.election_timeout_max_ticks = election_min + 1;
        let mut dynamic = DynamicControllerConfig::new(controller_clients.clone(), replication);
        dynamic.metadata_refresh_interval = Duration::from_millis(80);
        dynamic.max_pending_admin_operations = 16;
        config.dynamic_controller = Some(dynamic);
        config
    };
    let mut brokers = Vec::new();
    for broker_id in 1..=4_u32 {
        brokers.push(Some(
            start_dynamic_broker_when_controller_ready(make_broker_config(broker_id), &controllers)
                .await,
        ));
    }

    let liveness_deadline = time::Instant::now() + Duration::from_secs(10);
    loop {
        let leader = failure_detector_tick_on_current_leader(&controllers, 2_000).await;
        let mut all_healthy = true;
        for broker_id in 1..=4_u32 {
            all_healthy &= controllers[&leader]
                .broker_liveness(broker_id)
                .await
                .is_ok_and(|observation| observation.state == BrokerLivenessState::Healthy);
        }
        if all_healthy {
            break;
        }
        assert!(time::Instant::now() < liveness_deadline);
        time::sleep(Duration::from_millis(30)).await;
    }

    let created = create_topic_on_current_leader(
        &controllers,
        CreateTopicSpec {
            topic_name: "failure-orders".into(),
            partition_count: 1,
            replication_factor: 3,
            failure_domain_policy: FailureDomainPolicy::Disabled,
        },
    )
    .await;
    assert_eq!(created.assignments[0].voters, vec![1, 2, 3]);

    let mut producer_config =
        ClientConfig::new(brokers[0].as_ref().unwrap().local_addr().to_string());
    producer_config.request_timeout = Duration::from_secs(5);
    let producer = AstraClient::connect(producer_config).await.unwrap();
    let initial_deadline = time::Instant::now() + Duration::from_secs(10);
    loop {
        let metadata = producer.refresh_metadata().await.unwrap();
        if metadata
            .topics
            .iter()
            .find(|topic| topic.topic_id == created.topic.topic_id)
            .is_some_and(|topic| topic.partitions[0].leader_broker_id != UNKNOWN_BROKER_ID)
        {
            break;
        }
        assert!(time::Instant::now() < initial_deadline);
        time::sleep(Duration::from_millis(30)).await;
    }
    quorum_produce_with_refresh(
        &producer,
        "failure-orders",
        b"pre".to_vec(),
        b"durable-before-failures".to_vec(),
        Duration::from_secs(10),
        "failure scenario before faults",
    )
    .await;
    producer.close().await;

    let movement = loop {
        let leader = current_leader(&controllers).await;
        match controllers[&leader]
            .begin_reassignment(ReassignmentSpec {
                topic_id: created.topic.topic_id,
                partition_id: 0,
                source_broker_id: 1,
                destination_broker_id: 4,
                preferred_leader: None,
            })
            .await
        {
            Ok(result) => break result,
            Err(astralog_controller::ControllerError::NotLeader { .. }) => continue,
            Err(error) => panic!("failure-scenario reassignment admission failed: {error}"),
        }
    };

    let learner_requested_deadline = time::Instant::now() + Duration::from_secs(10);
    loop {
        let snapshot = controller_snapshot(&controllers).await;
        let stage = snapshot.state.movements()[&movement.movement.movement_id].stage;
        if stage == MovementStage::LearnerRequested {
            break;
        }
        assert!(
            time::Instant::now() < learner_requested_deadline,
            "movement skipped learner-requested failure injection point; stage={stage:?}"
        );
        time::sleep(Duration::from_millis(10)).await;
    }

    brokers[0].take().unwrap().shutdown().await.unwrap();
    brokers[3].take().unwrap().shutdown().await.unwrap();
    time::sleep(Duration::from_millis(50)).await;

    let lost_controller = current_leader(&controllers).await;
    controllers
        .remove(&lost_controller)
        .unwrap()
        .shutdown()
        .await;
    let _ = stable_controller_leader(&controllers).await;
    failure_detector_tick_on_current_leader(&controllers, 2_000).await;

    time::sleep(Duration::from_millis(300)).await;
    let stalled = controller_snapshot(&controllers).await;
    assert_ne!(
        stalled.state.movements()[&movement.movement.movement_id].stage,
        MovementStage::Complete,
        "movement must not complete while its destination is down"
    );

    mutate(
        &controllers,
        ControllerCommand::RejoinBroker {
            broker_id: 4,
            node_uuid: [4; 16],
            expected_incarnation: 1,
            new_incarnation: 2,
            client_host: client_addresses[3].ip().to_string(),
            client_port: client_addresses[3].port(),
            internal_host: raft_addresses[3].ip().to_string(),
            internal_port: raft_addresses[3].port(),
            rack: Some("rack-4".into()),
            zone: Some("zone-4".into()),
            min_feature: 1,
            max_feature: 1,
        },
    )
    .await;
    brokers[3] =
        Some(start_dynamic_broker_when_controller_ready(make_broker_config(4), &controllers).await);

    let completion_deadline = time::Instant::now() + Duration::from_secs(30);
    let final_snapshot = loop {
        failure_detector_tick_on_current_leader(&controllers, 2_000).await;
        let snapshot = controller_snapshot(&controllers).await;
        if snapshot.state.movements()[&movement.movement.movement_id].stage
            == MovementStage::Complete
        {
            break snapshot;
        }
        assert!(
            time::Instant::now() < completion_deadline,
            "failure-resumed movement did not complete; stage={:?}",
            snapshot.state.movements()[&movement.movement.movement_id].stage
        );
        time::sleep(Duration::from_millis(50)).await;
    };
    assert_eq!(
        final_snapshot.state.assignments()[&(created.topic.topic_id, 0)].voters,
        vec![2, 3, 4]
    );
    assert_eq!(
        final_snapshot.state.brokers()[&4].incarnation_epoch,
        2,
        "destination restart must use a strictly newer committed incarnation"
    );

    let mut post_config = ClientConfig::new(brokers[1].as_ref().unwrap().local_addr().to_string());
    post_config.request_timeout = Duration::from_secs(5);
    let post = AstraClient::connect(post_config).await.unwrap();
    let post_deadline = time::Instant::now() + Duration::from_secs(10);
    loop {
        let metadata = post.refresh_metadata().await.unwrap();
        if metadata
            .topics
            .iter()
            .find(|topic| topic.topic_id == created.topic.topic_id)
            .is_some_and(|topic| topic.partitions[0].leader_broker_id != UNKNOWN_BROKER_ID)
        {
            break;
        }
        assert!(time::Instant::now() < post_deadline);
        time::sleep(Duration::from_millis(30)).await;
    }
    quorum_produce_with_refresh(
        &post,
        "failure-orders",
        b"post".to_vec(),
        b"survived".to_vec(),
        Duration::from_secs(10),
        "failure scenario after recovery",
    )
    .await;
    post.close().await;

    for broker in brokers.into_iter().flatten() {
        broker.shutdown().await.unwrap();
    }
    for (_, controller) in controllers {
        controller.shutdown().await;
    }
    fs::remove_dir_all(root).unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 6)]
async fn p9f_rf3_checkpoint_survives_failover_and_preserves_exact_retry() {
    let _test_guard = p5_integration_lock().lock().await;
    let root = temp_dir("p9f-rf3-producer-checkpoint");
    let controller_addresses = [reserve_addr(), reserve_addr(), reserve_addr()];
    let mut controllers = BTreeMap::new();
    for id in 1..=3_u64 {
        controllers.insert(
            cid(id),
            start_controller_node(p9_controller_config(&root, id, &controller_addresses))
                .await
                .unwrap(),
        );
    }
    let _ = stable_controller_leader(&controllers).await;

    let client_addresses = (0..3).map(|_| reserve_addr()).collect::<Vec<_>>();
    let raft_addresses = (0..3).map(|_| reserve_addr()).collect::<Vec<_>>();
    for broker_id in 1..=3_u32 {
        let index = usize::try_from(broker_id - 1).unwrap();
        mutate(
            &controllers,
            ControllerCommand::RegisterBroker {
                broker_id,
                node_uuid: [u8::try_from(broker_id).unwrap(); 16],
                initial_incarnation: 1,
                client_host: client_addresses[index].ip().to_string(),
                client_port: client_addresses[index].port(),
                internal_host: raft_addresses[index].ip().to_string(),
                internal_port: raft_addresses[index].port(),
                rack: Some(format!("rack-{broker_id}")),
                zone: Some(format!("zone-{broker_id}")),
                min_feature: 1,
                max_feature: 5,
            },
        )
        .await;
        mutate(
            &controllers,
            ControllerCommand::SetBrokerLifecycle {
                broker_id,
                expected_incarnation: 1,
                expected_lifecycle: BrokerLifecycle::Joining,
                new_lifecycle: BrokerLifecycle::Active,
            },
        )
        .await;
    }

    let bump_deadline = time::Instant::now() + Duration::from_secs(30);
    loop {
        let leader = stable_controller_leader(&controllers).await;
        match controllers[&leader].bump_feature_level(5).await {
            Ok(_) => break,
            Err(
                astralog_controller::ControllerError::NotLeader { .. }
                | astralog_controller::ControllerError::Busy(_),
            ) => {}
            Err(error) => panic!("P9-F feature-level bump failed: {error}"),
        }
        assert!(time::Instant::now() < bump_deadline);
        time::sleep(Duration::from_millis(20)).await;
    }

    let controller_clients = controllers
        .values()
        .map(ControllerNodeHandle::client)
        .collect::<Vec<_>>();
    let make_broker_config = |broker_id: u32| {
        let index = usize::try_from(broker_id - 1).unwrap();
        let mut config = BrokerConfig::new(
            root.join(format!("broker-{broker_id}")),
            client_addresses[index],
        );
        config.advertised_host = client_addresses[index].ip().to_string();
        config.advertised_port = client_addresses[index].port();
        config.broker_id = broker_id;
        let mut replication = DynamicReplicationConfig::new(raft_addresses[index]);
        replication.tick_interval = Duration::from_millis(20);
        let election_min = 8 + u64::from(broker_id - 1) * 4;
        replication.election_timeout_min_ticks = election_min;
        replication.election_timeout_max_ticks = election_min + 1;
        replication.partition_log = PartitionLogConfig {
            max_segment_bytes: 768,
            max_segment_age_ms: None,
            index_interval_bytes: 1,
            limits: StorageLimits::default(),
        };
        let mut dynamic = DynamicControllerConfig::new(controller_clients.clone(), replication);
        dynamic.metadata_refresh_interval = Duration::from_millis(40);
        dynamic.preferred_leader_timeout = Duration::from_secs(10);
        config.dynamic_controller = Some(dynamic);
        config
    };
    let mut brokers = Vec::new();
    for broker_id in 1..=3_u32 {
        brokers.push(Some(
            start_dynamic_broker_when_controller_ready(make_broker_config(broker_id), &controllers)
                .await,
        ));
    }

    let healthy_deadline = time::Instant::now() + Duration::from_secs(15);
    loop {
        let leader = failure_detector_tick_on_current_leader(&controllers, 5_000).await;
        let mut healthy = true;
        for broker_id in 1..=3_u32 {
            healthy &= controllers[&leader]
                .broker_liveness(broker_id)
                .await
                .is_ok_and(|observation| observation.state == BrokerLivenessState::Healthy);
        }
        if healthy {
            break;
        }
        assert!(time::Instant::now() < healthy_deadline);
        time::sleep(Duration::from_millis(30)).await;
    }

    let created = create_retained_topic_on_current_leader(
        &controllers,
        CreateTopicSpec {
            topic_name: "p9f-checkpoint-rf3".into(),
            partition_count: 1,
            replication_factor: 3,
            failure_domain_policy: FailureDomainPolicy::Disabled,
        },
        TopicRetentionSpecV1 {
            cleanup_policy: ControllerCleanupPolicyV1::Compact,
            retention_ms: None,
            retention_bytes: None,
            tombstone_grace_ms: 86_400_000,
            subscription_protection: ControllerSubscriptionProtectionV1::Disabled,
        },
    )
    .await;
    assert_eq!(created.assignments[0].voters, vec![1, 2, 3]);

    let client = AstraClient::connect(ClientConfig::new(
        brokers[0].as_ref().unwrap().local_addr().to_string(),
    ))
    .await
    .unwrap();
    let leader_deadline = time::Instant::now() + Duration::from_secs(15);
    let (initial_leader, initial_epoch) = loop {
        let metadata = client.refresh_metadata().await.unwrap();
        if let Some(partition) = metadata
            .topics
            .iter()
            .find(|topic| topic.topic_id == created.topic.topic_id)
            .map(|topic| &topic.partitions[0])
            && partition.leader_broker_id != UNKNOWN_BROKER_ID
            && partition.leader_epoch != UNKNOWN_EPOCH
        {
            break (partition.leader_broker_id, partition.leader_epoch);
        }
        assert!(time::Instant::now() < leader_deadline);
        time::sleep(Duration::from_millis(25)).await;
    };
    let authority = client.init_producer().await.unwrap();
    let producer_record = DataRecord {
        timestamp_ms: 10,
        key: Some(b"checkpoint-key".to_vec()),
        value: Some(vec![b'P'; 2048]),
        headers: Vec::new(),
    };
    let initial_leader_index = usize::try_from(initial_leader - 1).unwrap();
    let first = brokers[initial_leader_index]
        .as_ref()
        .unwrap()
        .produce_idempotent_internal(
            created.topic.topic_id,
            0,
            initial_epoch,
            authority,
            ProducerSequence::ZERO,
            vec![producer_record.clone()],
        )
        .await
        .unwrap();
    assert_eq!((first.base_offset, first.last_offset), (0, 0));

    // Force a later keyed winner into another segment so the producer-bearing
    // source is a meaningful P9-F compaction candidate.
    quorum_produce_with_refresh(
        &client,
        "p9f-checkpoint-rf3",
        b"checkpoint-key".to_vec(),
        vec![b'W'; 2048],
        Duration::from_secs(10),
        "P9-F successor winner",
    )
    .await;

    let compact_deadline = time::Instant::now() + Duration::from_secs(20);
    let first_compaction = loop {
        match brokers[initial_leader_index]
            .as_ref()
            .unwrap()
            .run_keyed_compaction_once(
                created.topic.topic_id,
                0,
                initial_epoch,
                CompactionLimits::default(),
            )
            .await
        {
            Ok(Some(result)) if result.outcome.replaced_segments > 0 => break result,
            Ok(None) | Err(PublicError::BrokerNotAvailable | PublicError::StaleEpoch) => {}
            other => panic!("unexpected P9-F leader compaction result: {other:?}"),
        }
        assert!(
            time::Instant::now() < compact_deadline,
            "P9-F leader never published checkpoint-covered compaction"
        );
        time::sleep(Duration::from_millis(30)).await;
    };
    assert_eq!(first_compaction.leader_broker_id, initial_leader);

    // Remove the compacting leader. The checkpoint entry was quorum committed,
    // so a successor must recover producer authority from that retained
    // checkpoint and preserve exact retry semantics. Physical cleanup on the
    // successor while a voter is offline is deliberately not exercised here:
    // crossing that retained-history boundary can require Rebootstrap V2,
    // which is P9-G scope.
    brokers[initial_leader_index]
        .take()
        .unwrap()
        .shutdown()
        .await
        .unwrap();
    let survivor_addr = brokers
        .iter()
        .flatten()
        .next()
        .unwrap()
        .local_addr()
        .to_string();
    let post = AstraClient::connect(ClientConfig::new(survivor_addr))
        .await
        .unwrap();
    let failover_deadline = time::Instant::now() + Duration::from_secs(20);
    let successor = loop {
        let metadata = post.refresh_metadata().await.unwrap();
        if let Some(partition) = metadata
            .topics
            .iter()
            .find(|topic| topic.topic_id == created.topic.topic_id)
            .map(|topic| &topic.partitions[0])
            && partition.leader_broker_id != UNKNOWN_BROKER_ID
            && partition.leader_broker_id != initial_leader
            && partition.leader_epoch > initial_epoch
        {
            break partition.leader_broker_id;
        }
        assert!(time::Instant::now() < failover_deadline);
        time::sleep(Duration::from_millis(30)).await;
    };
    let successor_index = usize::try_from(successor - 1).unwrap();
    // Client metadata can observe the election slightly before the broker's
    // local actor/manager view. Reuse the P7 failover readiness condition
    // before probing idempotent admission.
    let retry_ready_deadline = time::Instant::now() + Duration::from_secs(10);
    let retry_epoch = loop {
        if let Ok(observation) = brokers[successor_index]
            .as_ref()
            .unwrap()
            .observe_partition_internal(created.topic.topic_id, 0)
            .await
            && observation.local_is_leader
            && observation.leader_broker_id == successor
            && observation.leader_epoch != UNKNOWN_EPOCH
        {
            break observation.leader_epoch;
        }
        assert!(
            time::Instant::now() < retry_ready_deadline,
            "P9-F successor actor did not become ready after leader failover"
        );
        time::sleep(Duration::from_millis(20)).await;
    };

    let retry = brokers[successor_index]
        .as_ref()
        .unwrap()
        .produce_idempotent_internal(
            created.topic.topic_id,
            0,
            retry_epoch,
            authority,
            ProducerSequence::ZERO,
            vec![producer_record],
        )
        .await
        .unwrap();
    assert_eq!(
        (retry.base_offset, retry.last_offset, retry.raft_index),
        (first.base_offset, first.last_offset, first.raft_index),
        "exact retry after RF3 failover must return the original committed range without append"
    );

    client.close().await;
    post.close().await;
    for broker in brokers.into_iter().flatten() {
        broker.shutdown().await.unwrap();
    }
    for (_, controller) in controllers {
        controller.shutdown().await;
    }
    fs::remove_dir_all(root).unwrap();
}

/// Live controller-managed RF3 baseline with *both* P9-G transports enabled.
/// The model-only three-replica tombstone proof cannot substitute for this:
/// P5 identities, real peer connection handshakes and P3 leadership changes
/// must coexist with the new sideband without weakening quorum replication.
#[tokio::test(flavor = "multi_thread", worker_threads = 6)]
async fn p9g_rf3_sideband_and_v2_opt_in_survive_broker_leader_failover() {
    run_p9g_rf3_sideband_v2_flow(false).await;
}

/// The tombstone test reuses the exact controller/P3/P5 sideband/rebootstrap
/// path, but additionally requires physical deletion to be fenced by the
/// actual all-voter committed safe bound, and an elected successor not to
/// resurrect any older value.
#[tokio::test(flavor = "multi_thread", worker_threads = 6)]
async fn p9g_rf3_tombstone_removal_after_safe_commit_cannot_resurrect_on_failover() {
    run_p9g_rf3_sideband_v2_flow(true).await;
}

async fn run_p9g_rf3_sideband_v2_flow(with_tombstone: bool) {
    if with_tombstone {
        eprintln!("P9-G live tombstone: starting controller-managed RF3 fixture");
    }
    fn durable_committed_compaction_safe_offset(partition: &Path) -> Option<u64> {
        let meta = astralog_storage::decode_partition_meta(
            &fs::read(partition.join("partition.meta")).ok()?,
        )
        .ok()?;
        let manifest = astralog_storage::load_manifest(partition).ok()??;
        let paths = astralog_storage::validate_partition_manifest_files(
            partition,
            &manifest,
            meta.topic_id,
            meta.partition_id,
        )
        .ok()?;
        let mut safe = 0_u64;
        for path in paths {
            match astralog_storage::read_segment_format(&path).ok()? {
                astralog_storage::StorageFormatVersion::V1 => {
                    let scan =
                        astralog_storage::scan_segment(&path, StorageLimits::default()).ok()?;
                    if scan.failure.is_some() {
                        return None;
                    }
                    for scanned in scan.frames {
                        if scanned.frame.raft_index().get() > meta.committed_raft_index {
                            break;
                        }
                        let StorageFrame::Control(control) = scanned.frame else {
                            continue;
                        };
                        if control.control_kind
                            != astralog_storage::P9_CONTROL_ADVANCE_COMPACTION_SAFE_OFFSET
                        {
                            continue;
                        }
                        if control.control_version
                            != astralog_storage::P9_MAINTENANCE_CONTROL_VERSION
                        {
                            return None;
                        }
                        let next =
                            astralog_storage::decode_compaction_safe_offset_body(&control.body)
                                .ok()?
                                .get();
                        if next <= safe {
                            return None;
                        }
                        safe = next;
                    }
                }
                astralog_storage::StorageFormatVersion::V2 => {
                    let scan =
                        astralog_storage::scan_retained_segment_v2(&path, StorageLimits::default())
                            .ok()?;
                    for scanned in scan.frames {
                        if scanned.frame.raft_index().get() > meta.committed_raft_index {
                            continue;
                        }
                        let astralog_storage::RetainedFrameV2::Control(control) = scanned.frame
                        else {
                            continue;
                        };
                        if control.kind
                            != astralog_storage::RetainedControlKindV2::AdvanceCompactionSafeOffset
                        {
                            continue;
                        }
                        let next =
                            astralog_storage::decode_compaction_safe_offset_body(&control.body)
                                .ok()?
                                .get();
                        if next <= safe {
                            return None;
                        }
                        safe = next;
                    }
                }
            }
        }
        Some(safe)
    }

    fn durable_key_versions(partition: &Path, key: &[u8]) -> Option<Vec<(u64, Option<Vec<u8>>)>> {
        let meta = astralog_storage::decode_partition_meta(
            &fs::read(partition.join("partition.meta")).ok()?,
        )
        .ok()?;
        let manifest = astralog_storage::load_manifest(partition).ok()??;
        let paths = astralog_storage::validate_partition_manifest_files(
            partition,
            &manifest,
            meta.topic_id,
            meta.partition_id,
        )
        .ok()?;
        let mut found = Vec::new();
        for path in paths {
            match astralog_storage::read_segment_format(&path).ok()? {
                astralog_storage::StorageFormatVersion::V1 => {
                    let scan =
                        astralog_storage::scan_segment(&path, StorageLimits::default()).ok()?;
                    if scan.failure.is_some() {
                        return None;
                    }
                    for scanned in scan.frames {
                        if scanned.frame.raft_index().get() > meta.committed_raft_index {
                            continue;
                        }
                        if let StorageFrame::Data(batch) = scanned.frame {
                            for (ordinal, record) in batch.records.into_iter().enumerate() {
                                if record.key.as_deref() == Some(key) {
                                    found.push((
                                        batch.base_stream_offset.get()
                                            + u64::try_from(ordinal).ok()?,
                                        record.value,
                                    ));
                                }
                            }
                        }
                    }
                }
                astralog_storage::StorageFormatVersion::V2 => {
                    let scan =
                        astralog_storage::scan_retained_segment_v2(&path, StorageLimits::default())
                            .ok()?;
                    for scanned in scan.frames {
                        if scanned.frame.raft_index().get() > meta.committed_raft_index {
                            continue;
                        }
                        if let astralog_storage::RetainedFrameV2::SparseData(batch) = scanned.frame
                        {
                            for retained in batch.records {
                                if retained.record.key.as_deref() == Some(key) {
                                    found.push((retained.offset.get(), retained.record.value));
                                }
                            }
                        }
                    }
                }
            }
        }
        found.sort_by_key(|(offset, _)| *offset);
        Some(found)
    }

    let _test_guard = p5_integration_lock().lock().await;
    let root = temp_dir("p9g-rf3-sideband-v2-failover");
    let controller_addresses = [reserve_addr(), reserve_addr(), reserve_addr()];
    let mut controllers = BTreeMap::new();
    for id in 1..=3_u64 {
        controllers.insert(
            cid(id),
            start_controller_node(p9_controller_config(&root, id, &controller_addresses))
                .await
                .unwrap(),
        );
    }
    let _ = stable_controller_leader(&controllers).await;
    if with_tombstone {
        eprintln!("P9-G live tombstone: three-controller quorum elected");
    }
    let client_addresses = (0..3).map(|_| reserve_addr()).collect::<Vec<_>>();
    let raft_addresses = (0..3).map(|_| reserve_addr()).collect::<Vec<_>>();
    for broker_id in 1..=3_u32 {
        let index = usize::try_from(broker_id - 1).unwrap();
        mutate(
            &controllers,
            ControllerCommand::RegisterBroker {
                broker_id,
                node_uuid: [u8::try_from(broker_id).unwrap(); 16],
                initial_incarnation: 1,
                client_host: client_addresses[index].ip().to_string(),
                client_port: client_addresses[index].port(),
                internal_host: raft_addresses[index].ip().to_string(),
                internal_port: raft_addresses[index].port(),
                rack: Some(format!("rack-{broker_id}")),
                zone: Some(format!("zone-{broker_id}")),
                min_feature: 1,
                max_feature: 5,
            },
        )
        .await;
        mutate(
            &controllers,
            ControllerCommand::SetBrokerLifecycle {
                broker_id,
                expected_incarnation: 1,
                expected_lifecycle: BrokerLifecycle::Joining,
                new_lifecycle: BrokerLifecycle::Active,
            },
        )
        .await;
    }
    let feature_deadline = time::Instant::now() + Duration::from_secs(30);
    loop {
        let leader = stable_controller_leader(&controllers).await;
        match controllers[&leader].bump_feature_level(5).await {
            Ok(_) => break,
            Err(
                astralog_controller::ControllerError::NotLeader { .. }
                | astralog_controller::ControllerError::Busy(_),
            ) => {}
            Err(error) => panic!("P9-G feature activation: {error}"),
        }
        assert!(time::Instant::now() < feature_deadline);
        time::sleep(Duration::from_millis(20)).await;
    }
    let controller_clients = controllers
        .values()
        .map(ControllerNodeHandle::client)
        .collect::<Vec<_>>();
    let make_broker_config = |broker_id: u32| {
        let index = usize::try_from(broker_id - 1).unwrap();
        let mut config = BrokerConfig::new(
            root.join(format!("broker-{broker_id}")),
            client_addresses[index],
        );
        config.advertised_host = client_addresses[index].ip().to_string();
        config.advertised_port = client_addresses[index].port();
        config.broker_id = broker_id;
        let mut replication = DynamicReplicationConfig::new(raft_addresses[index]);
        replication.tick_interval = Duration::from_millis(20);
        // This P9-G fixture compacts on disk and transfers a V2 snapshot.
        // Avoid election churn caused by a 160ms timeout under host load;
        // preserve real elections while giving disk work a bounded 800ms+
        // lease-free election window (not a production default change).
        let election_min = 40 + u64::from(broker_id - 1) * 5;
        replication.election_timeout_min_ticks = election_min;
        replication.election_timeout_max_ticks = election_min + 5;
        replication.partition_log = PartitionLogConfig {
            max_segment_bytes: 768,
            max_segment_age_ms: None,
            index_interval_bytes: 1,
            limits: StorageLimits::default(),
        };
        // These flags are off in the real default. This test explicitly
        // qualifies their coexistence only in a homogeneous P9-capable RF3.
        replication.enable_p9g_progress_reports = true;
        replication.enable_p9g_rebootstrap_v2 = true;
        let mut dynamic = DynamicControllerConfig::new(controller_clients.clone(), replication);
        dynamic.metadata_refresh_interval = Duration::from_millis(40);
        dynamic.preferred_leader_timeout = Duration::from_secs(10);
        config.dynamic_controller = Some(dynamic);
        config
    };
    let mut brokers = Vec::new();
    for broker_id in 1..=3_u32 {
        brokers.push(Some(
            start_dynamic_broker_when_controller_ready(make_broker_config(broker_id), &controllers)
                .await,
        ));
    }
    if with_tombstone {
        eprintln!("P9-G live tombstone: all three brokers started");
    }
    let healthy_deadline = time::Instant::now() + Duration::from_secs(15);
    loop {
        let leader = failure_detector_tick_on_current_leader(&controllers, 5_000).await;
        let mut healthy = true;
        for broker_id in 1..=3_u32 {
            healthy &= controllers[&leader]
                .broker_liveness(broker_id)
                .await
                .is_ok_and(|observation| observation.state == BrokerLivenessState::Healthy);
        }
        if healthy {
            break;
        }
        assert!(time::Instant::now() < healthy_deadline);
        time::sleep(Duration::from_millis(30)).await;
    }
    let created = create_retained_topic_on_current_leader(
        &controllers,
        CreateTopicSpec {
            topic_name: "p9g-rf3-opted-in".into(),
            partition_count: 1,
            replication_factor: 3,
            failure_domain_policy: FailureDomainPolicy::Disabled,
        },
        TopicRetentionSpecV1 {
            cleanup_policy: ControllerCleanupPolicyV1::CompactDelete,
            retention_ms: None,
            retention_bytes: None,
            // Exercise a real positive grace bound in the opt-in tombstone
            // case; the existing RF3 transport regression keeps the frozen
            // 24-hour default. No zero-grace exception is introduced.
            tombstone_grace_ms: if with_tombstone { 1 } else { 86_400_000 },
            subscription_protection: ControllerSubscriptionProtectionV1::Disabled,
        },
    )
    .await;
    assert_eq!(created.assignments[0].voters, vec![1, 2, 3]);
    let client = AstraClient::connect(ClientConfig::new(
        brokers[0].as_ref().unwrap().local_addr().to_string(),
    ))
    .await
    .unwrap();
    quorum_produce_with_refresh(
        &client,
        "p9g-rf3-opted-in",
        b"p9g-key".to_vec(),
        vec![b'1'; 128],
        Duration::from_secs(12),
        "P9-G initial value",
    )
    .await;
    quorum_produce_with_refresh(
        &client,
        "p9g-rf3-opted-in",
        b"p9g-surviving-key".to_vec(),
        vec![b's'; 128],
        Duration::from_secs(12),
        "P9-G retained independent key in same compacted source segment",
    )
    .await;
    let leader_deadline = time::Instant::now() + Duration::from_secs(15);
    let (initial_leader, initial_epoch) = loop {
        let metadata = client.refresh_metadata().await.unwrap();
        if let Some(partition) = metadata
            .topics
            .iter()
            .find(|topic| topic.topic_id == created.topic.topic_id)
            .map(|topic| &topic.partitions[0])
            && partition.leader_broker_id != UNKNOWN_BROKER_ID
            && partition.leader_epoch != UNKNOWN_EPOCH
        {
            break (partition.leader_broker_id, partition.leader_epoch);
        }
        assert!(time::Instant::now() < leader_deadline);
        time::sleep(Duration::from_millis(30)).await;
    };
    // Enough time for real peer kind-3 frames and actor ticks to circulate;
    // all three retain the source voter identities under committed P5.
    time::sleep(Duration::from_millis(300)).await;
    let offline_broker = (1..=3_u32)
        .find(|broker_id| *broker_id != initial_leader)
        .unwrap();
    let offline_index = usize::try_from(offline_broker - 1).unwrap();
    let index = usize::try_from(initial_leader - 1).unwrap();
    if with_tombstone {
        // The null value is a real quorum-committed keyed tombstone, not a
        // synthetic retained segment or a direct on-disk mutation. Do not
        // blindly retry an ambiguous producer write.
        let tombstone = client
            .produce(
                "p9g-rf3-opted-in",
                Some(b"p9g-key".to_vec()),
                None,
                Vec::new(),
                AckMode::Quorum,
            )
            .await
            .expect("live P9-G tombstone must be quorum acknowledged");
        assert_eq!(
            (tombstone.base_offset, tombstone.last_offset),
            (Some(2), Some(2)),
            "tombstone must supersede the previously quorum-committed value"
        );
        // The newest keyed marker must itself be in a closed segment for
        // the post-safe physical deletion gate to have a real candidate.
        quorum_produce_with_refresh(
            &client,
            "p9g-rf3-opted-in",
            b"p9g-independent-filler".to_vec(),
            vec![b'f'; 2048],
            Duration::from_secs(12),
            "P9-G close tombstone-bearing segment",
        )
        .await;
        eprintln!("P9-G live tombstone: source value, marker and closing data quorum-committed");
    } else {
        quorum_produce_with_refresh(
            &client,
            "p9g-rf3-opted-in",
            b"p9g-key".to_vec(),
            vec![b'2'; 512],
            Duration::from_secs(12),
            "P9-G second quorum produce before any eligible voter loses history",
        )
        .await;
    }
    // Ensure every P3 voter has acknowledged the second value before COW.
    // Otherwise a pre-COW generation can be queued as an earlier legitimate
    // InstallSnapshot while a voter is offline; P3 may install that version
    // first and refuse a later manifest at the *same* snapshot index.
    let all_live_deadline = time::Instant::now() + Duration::from_secs(10);
    loop {
        let expected = brokers[index]
            .as_ref()
            .unwrap()
            .observe_partition_internal(created.topic.topic_id, 0)
            .await
            .unwrap()
            .committed_raft_index;
        let mut caught_up = true;
        for broker in brokers.iter().flatten() {
            caught_up &= broker
                .observe_partition_internal(created.topic.topic_id, 0)
                .await
                .is_ok_and(|observation| observation.committed_raft_index >= expected);
        }
        if caught_up {
            break;
        }
        assert!(
            time::Instant::now() < all_live_deadline,
            "P9-G eligible voter lagged before COW generation selection"
        );
        time::sleep(Duration::from_millis(20)).await;
    }
    // Now compact a closed superseded keyed value while the voter set is
    // healthy, before inducing local disk loss on the selected follower.
    let compaction = brokers[index]
        .as_ref()
        .unwrap()
        .run_keyed_compaction_once(
            created.topic.topic_id,
            0,
            brokers[index]
                .as_ref()
                .unwrap()
                .observe_partition_internal(created.topic.topic_id, 0)
                .await
                .unwrap()
                .leader_epoch,
            CompactionLimits::default(),
        )
        .await
        .expect("P9-G V2 source must safely compact committed RF3 history")
        .expect("P9-G source has a superseded closed keyed segment");
    assert!(compaction.outcome.replaced_segments > 0);

    let mut topic_hex = String::new();
    for byte in created.topic.topic_id.as_bytes() {
        use std::fmt::Write as _;
        write!(&mut topic_hex, "{byte:02x}").unwrap();
    }
    let source_partition = root
        .join(format!("broker-{initial_leader}"))
        .join("partitions")
        .join(&topic_hex)
        .join("0000000000");
    if with_tombstone {
        assert_eq!(
            durable_committed_compaction_safe_offset(&source_partition),
            Some(0),
            "one independently cleaned voter cannot advance an all-voter RF3 safe offset"
        );
        assert_eq!(
            durable_key_versions(&source_partition, b"p9g-key"),
            Some(vec![(2, None)]),
            "pre-safe leader COW must remove older value but RETAIN the latest keyed tombstone"
        );
        eprintln!("P9-G live tombstone: pre-safe COW retains marker");
    }
    let source_snapshot = astralog_replication::SnapshotStore::new(&source_partition)
        .load()
        .unwrap()
        .expect("P9-G RF3 source COW requires durable P3 snapshot");
    let source_observation = brokers[index]
        .as_ref()
        .unwrap()
        .observe_partition_internal(created.topic.topic_id, 0)
        .await
        .unwrap();
    assert_eq!(
        source_snapshot.last_included_index.get(),
        source_observation.committed_raft_index,
        "V2 source export requires exact persisted P3 commit/snapshot boundary"
    );
    let source_manifest = astralog_storage::load_manifest(&source_partition)
        .unwrap()
        .expect("P9-G COW must publish authoritative manifest");
    assert!(
        source_manifest.generation >= 2,
        "P9-G RF3 proof requires a post-COW generation, not only V1 inventory adoption"
    );
    assert!(
        brokers[index]
            .as_ref()
            .unwrap()
            .observe_partition_internal(created.topic.topic_id, 0)
            .await
            .unwrap()
            .local_is_leader,
        "P9-G source must still be the P3 leader immediately after publishing sparse COW"
    );

    // Shut down the eligible follower only AFTER the post-COW generation is
    // fully published. Its next P3 snapshot cannot accidentally be satisfied
    // by a stale pre-COW manifest at the same Raft snapshot index.
    brokers[offline_index]
        .take()
        .unwrap()
        .shutdown()
        .await
        .unwrap();
    if with_tombstone {
        // Hold one committed P5 voter offline for longer than the sideband's
        // ten 20ms reporting ticks. A majority still exists, but an absent
        // eligible voter must not be silently removed from the all-voter
        // minimum and authorize physical deletion.
        time::sleep(Duration::from_millis(350)).await;
        assert_eq!(
            durable_committed_compaction_safe_offset(&source_partition),
            Some(0),
            "a live P3 majority cannot commit tombstone-safe progress while a current P5 voter has no verified clean history"
        );
        assert_eq!(
            durable_key_versions(&source_partition, b"p9g-key"),
            Some(vec![(2, None)]),
            "physically retained tombstone is mandatory while an eligible voter is offline"
        );
    }
    mutate(
        &controllers,
        ControllerCommand::RejoinBroker {
            broker_id: offline_broker,
            node_uuid: [u8::try_from(offline_broker).unwrap(); 16],
            expected_incarnation: 1,
            new_incarnation: 2,
            client_host: client_addresses[offline_index].ip().to_string(),
            client_port: client_addresses[offline_index].port(),
            internal_host: raft_addresses[offline_index].ip().to_string(),
            internal_port: raft_addresses[offline_index].port(),
            rack: Some(format!("rack-{offline_broker}")),
            zone: Some(format!("zone-{offline_broker}")),
            min_feature: 1,
            max_feature: 5,
        },
    )
    .await;
    let controller = stable_controller_leader(&controllers).await;
    assert_eq!(
        controllers[&controller]
            .broker_record(offline_broker)
            .await
            .unwrap()
            .unwrap()
            .incarnation_epoch,
        2,
        "new P5 incarnation must commit before the follower rejoins with V2"
    );
    // A current-P3 leader without an authoritative retained manifest may
    // legitimately rebuild the lost voter through ordinary contiguous V1
    // AppendEntries. That path is not a qualifying V2 network rebootstrap.
    // Re-establish leadership on the physically compacted source *after*
    // committing the new P5 incarnation but *before* the empty voter starts,
    // so the source can answer the follower's P3 InstallSnapshot with its
    // authoritative retained-history V2 inventory. This transfer comes from
    // the actual current leader and never falsifies a follower's commit.
    p9g_transfer_to_current_leader(
        &brokers,
        created.topic.topic_id,
        initial_leader,
        "V2 source must own current P3 leadership before erased voter rejoins",
    )
    .await;
    // Fault injection: after a clean broker shutdown, simulate loss of this
    // follower's local partition files. The controller still considers it an
    // eligible P5 voter, but P3 can no longer infer the earlier Raft prefix.
    // Retained-history V2 must establish an authoritative snapshot before
    // the follower is allowed to contribute newly committed suffix history.
    let target_partition = root
        .join(format!("broker-{offline_broker}"))
        .join("partitions")
        .join(&topic_hex)
        .join("0000000000");
    assert!(target_partition.starts_with(&root) && target_partition.is_dir());
    fs::remove_dir_all(&target_partition).unwrap();
    // Restart the voter with the P5 identity but no local Raft prefix.
    brokers[offline_index] = Some(
        start_dynamic_broker_when_controller_ready(
            make_broker_config(offline_broker),
            &controllers,
        )
        .await,
    );
    // Controller progress can be stale across a broker restart. Require
    // direct evidence from the current local partition actor AND the actual
    // staged V2 retained manifest; a cached controller index is not proof.
    let controller_deadline = time::Instant::now() + Duration::from_secs(30);
    loop {
        let live = brokers[offline_index]
            .as_ref()
            .unwrap()
            .observe_partition_internal(created.topic.topic_id, 0)
            .await;
        let source_now = astralog_storage::load_manifest(&source_partition)
            .ok()
            .flatten();
        let target_now = astralog_storage::load_manifest(&target_partition)
            .ok()
            .flatten();
        if source_now.as_ref().is_some_and(|source| {
            source.generation >= source_manifest.generation && target_now.as_ref() == Some(source)
        }) && live.as_ref().is_ok_and(|observation| {
            observation.committed_raft_index >= source_snapshot.last_included_index.get()
        }) {
            break;
        }
        if time::Instant::now() >= controller_deadline {
            let mut peer_status = Vec::new();
            for (peer_index, broker) in brokers.iter().enumerate() {
                if let Some(broker) = broker {
                    peer_status.push((
                        peer_index + 1,
                        broker
                            .observe_partition_internal(created.topic.topic_id, 0)
                            .await,
                    ));
                }
            }
            panic!(
                "P9-G exact retained V2 generation not installed: source={source_now:?} target={target_now:?} local={live:?} all_peers={peer_status:?}"
            );
        }
        time::sleep(Duration::from_millis(30)).await;
    }
    let installed_manifest = astralog_storage::load_manifest(&target_partition)
        .unwrap()
        .expect("V2 rebootstrap must publish a valid target manifest");
    let authoritative_manifest = astralog_storage::load_manifest(&source_partition)
        .unwrap()
        .expect("source must retain manifest after P5 incarnation refresh");
    assert_eq!(
        installed_manifest, authoritative_manifest,
        "P3-authorized V2 install must carry current post-COW inventory after P5 rotation"
    );
    let mut retained_v2_files = 0_usize;
    for entry in &authoritative_manifest.entries {
        let source_bytes = fs::read(source_partition.join(&entry.file_name)).unwrap();
        if source_bytes.len() >= 10
            && source_bytes.starts_with(b"ASTRALOG")
            && source_bytes[8..10] == 2_u16.to_le_bytes()
        {
            let target_bytes = fs::read(target_partition.join(&entry.file_name)).unwrap();
            assert_eq!(
                target_bytes, source_bytes,
                "actual retained V2 immutable file must transfer without missing or substituted bytes"
            );
            retained_v2_files += 1;
        }
    }
    assert!(
        retained_v2_files > 0,
        "post-COW V2 package must contain an immutable sparse retained segment"
    );
    if with_tombstone {
        eprintln!("P9-G live tombstone: P5 incarnation + V2 install confirmed");
    }
    // The remaining live follower has not inherited the source sidecar: make
    // it P3 leader and require its own bounded COW proof. Together with the
    // source and V2-rebootstrap voter, this gives positive current-generation
    // clean evidence for every current eligible voter rather than inferring a
    // cluster-safe value from one cleaner or from cached controller progress.
    // A controller/P3 election can race the V2 rejoin. The source inventory
    // was independently validated above; the following transfer must instead
    // originate from the *current* P3 leader, whichever broker it is.
    let independently_cleaned_broker = (1..=3_u32)
        .find(|broker_id| *broker_id != initial_leader && *broker_id != offline_broker)
        .unwrap();
    let independently_cleaned_index = usize::try_from(independently_cleaned_broker - 1).unwrap();
    p9g_transfer_to_current_leader(
        &brokers,
        created.topic.topic_id,
        independently_cleaned_broker,
        "independent final-voter COW after V2 rejoin",
    )
    .await;
    let clean_leader_deadline = time::Instant::now() + Duration::from_secs(15);
    let clean_leader_epoch = loop {
        let observation = brokers[independently_cleaned_index]
            .as_ref()
            .unwrap()
            .observe_partition_internal(created.topic.topic_id, 0)
            .await;
        if let Ok(observation) = observation
            && observation.local_is_leader
            && observation.leader_broker_id == independently_cleaned_broker
            && observation.leader_epoch > initial_epoch
        {
            break observation.leader_epoch;
        }
        assert!(
            time::Instant::now() < clean_leader_deadline,
            "P9-G could not transfer leadership to independently clean the final eligible voter"
        );
        time::sleep(Duration::from_millis(30)).await;
    };
    let follower_compaction = brokers[independently_cleaned_index]
        .as_ref()
        .unwrap()
        .run_keyed_compaction_once(
            created.topic.topic_id,
            0,
            clean_leader_epoch,
            CompactionLimits::default(),
        )
        .await
        .expect("P9-G final eligible voter must be able to compact as the real P3 leader")
        .expect("P9-G final eligible voter must still have a superseded closed keyed segment");
    assert!(follower_compaction.outcome.replaced_segments > 0);

    let independently_cleaned_partition = root
        .join(format!("broker-{independently_cleaned_broker}"))
        .join("partitions")
        .join(&topic_hex)
        .join("0000000000");
    let all_partition_paths = [
        source_partition.as_path(),
        target_partition.as_path(),
        independently_cleaned_partition.as_path(),
    ];
    let mut clean_offsets = Vec::new();
    for partition in all_partition_paths {
        let manifest = astralog_storage::load_manifest(partition)
            .unwrap()
            .expect("P9-G all-voter safe proof requires an authoritative manifest");
        let clean = astralog_storage::LocalCleanProgressStore::new(
            partition,
            created.topic.topic_id,
            astralog_common::PartitionId::new(0),
        )
        .load_conservative(Some(manifest.generation))
        .get();
        assert!(
            clean > 0,
            "P9-G eligible voter has no verified current-generation clean prefix: partition={partition:?} manifest_generation={} clean={clean}",
            manifest.generation
        );
        clean_offsets.push(clean);
    }
    let expected_safe = *clean_offsets
        .iter()
        .min()
        .expect("P9-G RF3 must have three eligible-voter clean observations");
    assert!(expected_safe > 0);

    // Sideband reports are only evidence. Require the all-voter minimum to
    // appear as the replicated maintenance control at or below each replica's
    // durable committed index. This observes disk state directly and does not
    // accept a volatile tracker candidate as cluster authority.
    let safe_deadline = time::Instant::now() + Duration::from_secs(15);
    loop {
        let observed = all_partition_paths
            .iter()
            .map(|partition| durable_committed_compaction_safe_offset(partition))
            .collect::<Vec<_>>();
        if observed.iter().all(|safe| *safe == Some(expected_safe)) {
            break;
        }
        assert!(
            observed.iter().flatten().all(|safe| *safe <= expected_safe),
            "P9-G committed safe offset exceeded the actual all-voter minimum: clean={clean_offsets:?} durable={observed:?}"
        );
        if time::Instant::now() >= safe_deadline {
            let diagnostics = p9g_failover_actor_disk_diagnostics(
                &brokers,
                created.topic.topic_id,
                &root,
                &topic_hex,
                durable_committed_compaction_safe_offset,
            )
            .await;
            panic!(
                "P9-G all-voter minimum never became a durable committed RF3 safe offset: clean={clean_offsets:?} durable={observed:?}; live_actor_manifest_clean_safe={diagnostics:?}"
            );
        }
        time::sleep(Duration::from_millis(30)).await;
    }
    if with_tombstone {
        eprintln!("P9-G live tombstone: all-voter safe committed on three disks");
    }

    if with_tombstone {
        let before = durable_key_versions(&independently_cleaned_partition, b"p9g-key")
            .expect("pre-reclaim manifest and segments must be readable");
        assert_eq!(
            before,
            vec![(2, None)],
            "the tombstone must still exist when all-voter safe is newly committed"
        );
        // The all-voter safe-offset commit can move P3 leadership or refresh
        // P5 metadata after the prior COW epoch was captured. An explicit
        // StaleEpoch is a pre-publication fence, never authority to continue
        // with a stale plan. Re-acquire real P3 leadership and verify the
        // marker still exists before a fresh bounded maintenance attempt.
        // This does not retry an ambiguous P2 Produce mutation.
        let reclaim_deadline = time::Instant::now() + Duration::from_secs(15);
        let reclaimed = loop {
            let reclaim_epoch = p9g_transfer_to_current_leader(
                &brokers,
                created.topic.topic_id,
                independently_cleaned_broker,
                "fresh post-safe CompactDelete authorization",
            )
            .await;
            match brokers[independently_cleaned_index]
                .as_ref()
                .unwrap()
                .run_keyed_compaction_once(
                    created.topic.topic_id,
                    0,
                    reclaim_epoch,
                    CompactionLimits::default(),
                )
                .await
            {
                Ok(Some(reclaimed)) => break reclaimed,
                Ok(None) => panic!(
                    "the committed-safe, closed tombstone source must be physically reclaimable"
                ),
                Err(PublicError::StaleEpoch) => {
                    assert_eq!(
                        durable_key_versions(&independently_cleaned_partition, b"p9g-key"),
                        Some(vec![(2, None)]),
                        "a stale maintenance authority must not physically omit the marker"
                    );
                    assert!(
                        time::Instant::now() < reclaim_deadline,
                        "P9-G post-grace COW never obtained current P3/P5 epoch"
                    );
                    time::sleep(Duration::from_millis(30)).await;
                }
                Err(error) => panic!(
                    "safe CompactDelete leader must accept the post-grace P9-G COW: {error:?}"
                ),
            }
        };
        assert!(
            reclaimed.outcome.replaced_segments > 0,
            "a successful no-op cannot count as tombstone deletion"
        );
        assert_eq!(
            durable_committed_compaction_safe_offset(&independently_cleaned_partition),
            Some(expected_safe),
            "physical tombstone deletion must preserve durable replicated safe authority"
        );
        assert_eq!(
            durable_key_versions(&independently_cleaned_partition, b"p9g-key"),
            Some(Vec::new()),
            "physical COW must eliminate the tombstone AND every older keyed value on this leader"
        );
        eprintln!("P9-G live tombstone: post-safe physical marker removal verified");
    }

    // Now fail the P3 leader that independently completed the final voter COW.
    // A successor must retain the same committed safe authority and continue
    // ordinary quorum writes without reintroducing the source version removed
    // by COW.
    brokers[independently_cleaned_index]
        .take()
        .unwrap()
        .shutdown()
        .await
        .unwrap();
    let survivor_addr = brokers
        .iter()
        .flatten()
        .next()
        .unwrap()
        .local_addr()
        .to_string();
    let successor_client = AstraClient::connect(ClientConfig::new(survivor_addr))
        .await
        .unwrap();
    let failover_deadline = time::Instant::now() + Duration::from_secs(20);
    loop {
        let metadata = successor_client.refresh_metadata().await.unwrap();
        if let Some(partition) = metadata
            .topics
            .iter()
            .find(|topic| topic.topic_id == created.topic.topic_id)
            .map(|topic| &topic.partitions[0])
            && partition.leader_broker_id != UNKNOWN_BROKER_ID
            && partition.leader_broker_id != independently_cleaned_broker
            && partition.leader_epoch > clean_leader_epoch
        {
            break;
        }
        assert!(
            time::Instant::now() < failover_deadline,
            "P9-G V2 installed but successor P3 election did not complete"
        );
        time::sleep(Duration::from_millis(30)).await;
    }
    let successor_metadata = successor_client.refresh_metadata().await.unwrap();
    let successor = successor_metadata
        .topics
        .iter()
        .find(|topic| topic.topic_id == created.topic.topic_id)
        .unwrap()
        .partitions[0]
        .leader_broker_id;
    let successor_partition = root
        .join(format!("broker-{successor}"))
        .join("partitions")
        .join(&topic_hex)
        .join("0000000000");
    assert_eq!(
        durable_committed_compaction_safe_offset(&successor_partition),
        Some(expected_safe),
        "P9-G successor must recover the quorum-committed all-voter safe offset from durable history"
    );
    // A controller metadata reply can briefly lead local P3 authority during
    // post-failover elections. Before sending a NON-IDEMPOTENT quorum Produce,
    // require repeatable target-local leadership at the advertised epoch.
    // This is only a pre-send readiness check; once Produce is sent, even a
    // NotLeader/timeout response remains potentially ambiguous and is NOT
    // retried by the helper below.
    let readiness_deadline = time::Instant::now() + Duration::from_secs(12);
    let mut last_confirmed = None;
    let mut consecutive_confirmations = 0_u8;
    loop {
        let advertised = successor_client.refresh_metadata().await.unwrap();
        let advertised = advertised
            .topics
            .iter()
            .find(|topic| topic.topic_id == created.topic.topic_id)
            .map(|topic| &topic.partitions[0]);
        let candidate = if let Some(partition) = advertised {
            let leader = partition.leader_broker_id;
            let epoch = partition.leader_epoch;
            if (1..=3).contains(&leader) && epoch != UNKNOWN_EPOCH {
                let target = brokers[usize::try_from(leader - 1).unwrap()].as_ref();
                if let Some(target) = target {
                    let local = time::timeout(
                        Duration::from_millis(350),
                        target.observe_partition_internal(created.topic.topic_id, 0),
                    )
                    .await;
                    if local.as_ref().is_ok_and(|result| {
                        result.as_ref().is_ok_and(|observed| {
                            observed.local_is_leader
                                && observed.leader_broker_id == leader
                                && observed.leader_epoch == epoch
                        })
                    }) {
                        Some((leader, epoch))
                    } else {
                        None
                    }
                } else {
                    None
                }
            } else {
                None
            }
        } else {
            None
        };
        if candidate.is_some() && candidate == last_confirmed {
            consecutive_confirmations += 1;
        } else {
            last_confirmed = candidate;
            consecutive_confirmations = u8::from(candidate.is_some());
        }
        if consecutive_confirmations >= 3 {
            break;
        }
        assert!(
            time::Instant::now() < readiness_deadline,
            "P9-G P3 successor had no stable target-local leader/epoch before quorum Produce; last={last_confirmed:?}"
        );
        time::sleep(Duration::from_millis(30)).await;
    }
    p9g_quorum_produce_with_failover_diagnostics(
        &successor_client,
        "p9g-rf3-opted-in",
        if with_tombstone {
            b"p9g-post-failover-independent-key".to_vec()
        } else {
            b"p9g-key".to_vec()
        },
        vec![b'3'; 1024],
        Duration::from_secs(12),
        &brokers,
        created.topic.topic_id,
        &root,
        &topic_hex,
        durable_committed_compaction_safe_offset,
    )
    .await;
    if with_tombstone {
        eprintln!("P9-G live tombstone: first leader failover + quorum suffix confirmed");
    }
    if with_tombstone {
        let successor_versions = durable_key_versions(&successor_partition, b"p9g-key")
            .expect("successor retained committed source must be independently readable");
        assert!(
            successor_versions.iter().all(|(_, value)| value.is_none()),
            "newly elected P3 leader must never resurrect an older keyed value: {successor_versions:?}"
        );
        let fetched = successor_client
            .fetch(FetchRequestV0 {
                isolation: IsolationLevel::ReadCommitted,
                min_bytes: 0,
                max_wait_ms: 0,
                partitions: vec![FetchPartitionRequestV0 {
                    topic_id: created.topic.topic_id,
                    partition: 0,
                    leader_epoch: UNKNOWN_EPOCH,
                    fetch_offset: 0,
                    max_bytes: 64 * 1024,
                    max_records: 32,
                }],
                tags: Vec::new(),
            })
            .await
            .expect("read-committed Fetch must recover after the tombstone-cleaner leader fails");
        assert_eq!(
            fetched.partitions[0].error_code, 0,
            "successor must serve a valid read-committed history"
        );
        assert!(
            fetched.partitions[0].records.iter().any(|record| {
                record.record.key.as_deref() == Some(b"p9g-post-failover-independent-key")
            }),
            "nonresurrection Fetch assertion must cover the newly committed successor suffix"
        );
        assert!(
            fetched.partitions[0].records.iter().all(|record| {
                record.record.key.as_deref() != Some(b"p9g-key") || record.record.value.is_none()
            }),
            "read-committed client must not observe any resurrected value after leader change"
        );
        assert_eq!(
            durable_key_versions(&independently_cleaned_partition, b"p9g-key"),
            Some(Vec::new()),
            "shutdown tombstone-cleaner leader must not regain discarded source bytes"
        );

        // A surviving leader that still retains the marker is not enough to
        // prove a tombstone-free replica can safely lead again. Rejoin the
        // physically cleaned broker with a fresh committed P5 incarnation,
        // catch it up through ordinary authenticated P3 replication, and
        // transfer leadership *back* to that exact manifest.
        mutate(
            &controllers,
            ControllerCommand::RejoinBroker {
                broker_id: independently_cleaned_broker,
                node_uuid: [u8::try_from(independently_cleaned_broker).unwrap(); 16],
                expected_incarnation: 1,
                new_incarnation: 2,
                client_host: client_addresses[independently_cleaned_index]
                    .ip()
                    .to_string(),
                client_port: client_addresses[independently_cleaned_index].port(),
                internal_host: raft_addresses[independently_cleaned_index].ip().to_string(),
                internal_port: raft_addresses[independently_cleaned_index].port(),
                rack: Some(format!("rack-{independently_cleaned_broker}")),
                zone: Some(format!("zone-{independently_cleaned_broker}")),
                min_feature: 1,
                max_feature: 5,
            },
        )
        .await;
        eprintln!("P9-G live tombstone: P5 rejoin incarnation committed");
        brokers[independently_cleaned_index] = Some(
            start_dynamic_broker_when_controller_ready(
                make_broker_config(independently_cleaned_broker),
                &controllers,
            )
            .await,
        );
        eprintln!("P9-G live tombstone: physically cleaned broker restarted");
        let rejoin_deadline = time::Instant::now() + Duration::from_secs(20);
        // Rejoining a broker commits new P5 incarnation metadata. A survivor
        // may briefly tear down/rebuild its local P3 actor to install that
        // authority; the cached read-only observation intentionally returns
        // Unavailable during the atomic routing swap, not stale authority.
        // Wait only for *actual* current P3 commit evidence rather than
        // treating one transitional unavailable result as a permanent failure.
        // The independently quorum-committed post-failover suffix must remain
        // ahead of the previous source snapshot, so a stale pre-failover
        // observation cannot accidentally satisfy this rejoin oracle.
        let successor_raft = loop {
            let observed = time::timeout(
                Duration::from_millis(500),
                brokers[usize::try_from(successor - 1).unwrap()]
                    .as_ref()
                    .unwrap()
                    .observe_partition_internal(created.topic.topic_id, 0),
            )
            .await;
            if let Ok(Ok(observation)) = &observed
                && observation.committed_raft_index > source_snapshot.last_included_index.get()
                && observation.leader_epoch != UNKNOWN_EPOCH
            {
                break observation.committed_raft_index;
            }
            assert!(
                time::Instant::now() < rejoin_deadline,
                "P9-G surviving P3 actor failed to recover after P5 incarnation rejoin while preserving the committed post-failover suffix: {observed:?}"
            );
            time::sleep(Duration::from_millis(30)).await;
        };
        loop {
            let rejoined = brokers[independently_cleaned_index]
                .as_ref()
                .unwrap()
                .observe_partition_internal(created.topic.topic_id, 0)
                .await;
            if rejoined
                .as_ref()
                .is_ok_and(|observation| observation.committed_raft_index >= successor_raft)
            {
                break;
            }
            assert!(
                time::Instant::now() < rejoin_deadline,
                "tombstone-free P5 incarnation could not catch up under P3: {rejoined:?}"
            );
            time::sleep(Duration::from_millis(30)).await;
        }
        assert_eq!(
            durable_key_versions(&independently_cleaned_partition, b"p9g-key"),
            Some(Vec::new()),
            "ordinary P3 catch-up must not resurrect deleted tombstone or obsolete source value"
        );
        eprintln!("P9-G live tombstone: restarted broker caught up without resurrection");
        p9g_transfer_to_current_leader(
            &brokers,
            created.topic.topic_id,
            independently_cleaned_broker,
            "return leadership to restarted tombstone-free broker",
        )
        .await;
        eprintln!("P9-G live tombstone: preferred leader transfer back observed");
        let return_deadline = time::Instant::now() + Duration::from_secs(20);
        loop {
            let observation = brokers[independently_cleaned_index]
                .as_ref()
                .unwrap()
                .observe_partition_internal(created.topic.topic_id, 0)
                .await;
            if observation.as_ref().is_ok_and(|state| {
                state.local_is_leader && state.leader_broker_id == independently_cleaned_broker
            }) {
                break;
            }
            assert!(
                time::Instant::now() < return_deadline,
                "P3 failed to elect the rejoined tombstone-free broker: {observation:?}"
            );
            time::sleep(Duration::from_millis(30)).await;
        }
        let rejoined_client = AstraClient::connect(ClientConfig::new(
            brokers[independently_cleaned_index]
                .as_ref()
                .unwrap()
                .local_addr()
                .to_string(),
        ))
        .await
        .unwrap();
        let rejoined_fetch = rejoined_client
            .fetch(FetchRequestV0 {
                isolation: IsolationLevel::ReadCommitted,
                min_bytes: 0,
                max_wait_ms: 0,
                partitions: vec![FetchPartitionRequestV0 {
                    topic_id: created.topic.topic_id,
                    partition: 0,
                    leader_epoch: UNKNOWN_EPOCH,
                    fetch_offset: 0,
                    max_bytes: 64 * 1024,
                    max_records: 32,
                }],
                tags: Vec::new(),
            })
            .await
            .expect("re-elected tombstone-free broker must serve read-committed fetch");
        assert_eq!(rejoined_fetch.partitions[0].error_code, 0);
        assert!(
            rejoined_fetch.partitions[0].records.iter().all(|record| {
                record.record.key.as_deref() != Some(b"p9g-key") || record.record.value.is_none()
            }),
            "no deleted value can reappear when the physically cleaned replica regains leadership"
        );
        assert_eq!(
            durable_key_versions(&independently_cleaned_partition, b"p9g-key"),
            Some(Vec::new()),
            "post-election durable inventory must remain tombstone-free and non-resurrecting"
        );
        eprintln!("P9-G live tombstone: re-elected tombstone-free broker Fetch checked");
        rejoined_client.close().await;
    }
    client.close().await;
    successor_client.close().await;
    for broker in brokers.into_iter().flatten() {
        broker.shutdown().await.unwrap();
    }
    for (_, controller) in controllers {
        controller.shutdown().await;
    }
    fs::remove_dir_all(root).unwrap();
}

/// Bounded live P9-G qualification across a real four-broker P3 joint
/// transition. The movement is old [1,2,3] -> new [2,3,4]. Keep the
/// destination-incarnation and old-only-voter outage experiments independent
/// while sharing their actual committed-Joint and positive safe/tombstone
/// publication oracles. A controller stage label cannot substitute for P3.
#[tokio::test(flavor = "multi_thread", worker_threads = 6)]
async fn p9g_joint_rf4_new_voter_incarnation_and_stable_safe_minimum() {
    run_p9g_joint_rf4_flow(false).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 6)]
async fn p9g_joint_rf4_old_only_voter_absent_until_committed_exclusion() {
    run_p9g_joint_rf4_flow(true).await;
}

async fn run_p9g_joint_rf4_flow(old_only_absent: bool) {
    eprintln!("P9G RF4 phase=start old_only_absent={old_only_absent}");
    fn durable_paths(
        partition: &Path,
        topic_id: astralog_common::TopicId,
        partition_id: astralog_common::PartitionId,
    ) -> Option<Vec<PathBuf>> {
        if let Some(manifest) = astralog_storage::load_manifest(partition).ok()? {
            return astralog_storage::validate_partition_manifest_files(
                partition,
                &manifest,
                topic_id,
                partition_id,
            )
            .ok();
        }
        let mut segments = Vec::new();
        for entry in fs::read_dir(partition).ok()? {
            let entry = entry.ok()?;
            if !entry.file_type().ok()?.is_file() {
                continue;
            }
            let name = entry.file_name();
            let Some(name) = name.to_str() else {
                continue;
            };
            if !name.ends_with(".log") {
                continue;
            }
            let id = astralog_storage::SegmentFileId::parse_log_file_name(name).ok()?;
            segments.push((id, entry.path()));
        }
        segments.sort_by_key(|(id, _)| {
            (
                id.base_raft_index.get(),
                id.base_stream_offset.get(),
                id.generation,
            )
        });
        (!segments.is_empty()).then(|| segments.into_iter().map(|(_, path)| path).collect())
    }

    fn durable_safe(partition: &Path) -> Option<u64> {
        let meta = astralog_storage::decode_partition_meta(
            &fs::read(partition.join("partition.meta")).ok()?,
        )
        .ok()?;
        let paths = durable_paths(partition, meta.topic_id, meta.partition_id)?;
        let mut safe = 0_u64;
        for path in paths {
            match astralog_storage::read_segment_format(&path).ok()? {
                astralog_storage::StorageFormatVersion::V1 => {
                    let scan =
                        astralog_storage::scan_segment(&path, StorageLimits::default()).ok()?;
                    if scan.failure.is_some() {
                        return None;
                    }
                    for scanned in scan.frames {
                        if scanned.frame.raft_index().get() > meta.committed_raft_index {
                            break;
                        }
                        let StorageFrame::Control(control) = scanned.frame else {
                            continue;
                        };
                        if control.control_kind
                            != astralog_storage::P9_CONTROL_ADVANCE_COMPACTION_SAFE_OFFSET
                        {
                            continue;
                        }
                        let next =
                            astralog_storage::decode_compaction_safe_offset_body(&control.body)
                                .ok()?
                                .get();
                        if next <= safe {
                            return None;
                        }
                        safe = next;
                    }
                }
                astralog_storage::StorageFormatVersion::V2 => {
                    let scan =
                        astralog_storage::scan_retained_segment_v2(&path, StorageLimits::default())
                            .ok()?;
                    for scanned in scan.frames {
                        if scanned.frame.raft_index().get() > meta.committed_raft_index {
                            continue;
                        }
                        let astralog_storage::RetainedFrameV2::Control(control) = scanned.frame
                        else {
                            continue;
                        };
                        if control.kind
                            != astralog_storage::RetainedControlKindV2::AdvanceCompactionSafeOffset
                        {
                            continue;
                        }
                        let next =
                            astralog_storage::decode_compaction_safe_offset_body(&control.body)
                                .ok()?
                                .get();
                        if next <= safe {
                            return None;
                        }
                        safe = next;
                    }
                }
            }
        }
        Some(safe)
    }

    fn durable_key_versions(partition: &Path, key: &[u8]) -> Option<Vec<(u64, Option<Vec<u8>>)>> {
        let meta = astralog_storage::decode_partition_meta(
            &fs::read(partition.join("partition.meta")).ok()?,
        )
        .ok()?;
        let paths = durable_paths(partition, meta.topic_id, meta.partition_id)?;
        let mut found = Vec::new();
        for path in paths {
            match astralog_storage::read_segment_format(&path).ok()? {
                astralog_storage::StorageFormatVersion::V1 => {
                    let scan =
                        astralog_storage::scan_segment(&path, StorageLimits::default()).ok()?;
                    if scan.failure.is_some() {
                        return None;
                    }
                    for scanned in scan.frames {
                        if scanned.frame.raft_index().get() > meta.committed_raft_index {
                            continue;
                        }
                        if let StorageFrame::Data(batch) = scanned.frame {
                            for (ordinal, record) in batch.records.into_iter().enumerate() {
                                if record.key.as_deref() == Some(key) {
                                    found.push((
                                        batch.base_stream_offset.get()
                                            + u64::try_from(ordinal).ok()?,
                                        record.value,
                                    ));
                                }
                            }
                        }
                    }
                }
                astralog_storage::StorageFormatVersion::V2 => {
                    let scan =
                        astralog_storage::scan_retained_segment_v2(&path, StorageLimits::default())
                            .ok()?;
                    for scanned in scan.frames {
                        if scanned.frame.raft_index().get() > meta.committed_raft_index {
                            continue;
                        }
                        if let astralog_storage::RetainedFrameV2::SparseData(batch) = scanned.frame
                        {
                            for retained in batch.records {
                                if retained.record.key.as_deref() == Some(key) {
                                    found.push((retained.offset.get(), retained.record.value));
                                }
                            }
                        }
                    }
                }
            }
        }
        found.sort_by_key(|(offset, _)| *offset);
        Some(found)
    }

    fn current_clean(partition: &Path, topic_id: astralog_common::TopicId) -> Option<(u64, u64)> {
        let manifest = astralog_storage::load_manifest(partition).ok()??;
        let clean = astralog_storage::LocalCleanProgressStore::new(
            partition,
            topic_id,
            astralog_common::PartitionId::new(0),
        )
        .load_conservative(Some(manifest.generation))
        .get();
        Some((manifest.generation, clean))
    }

    let _test_guard = p5_integration_lock().lock().await;
    let root = temp_dir("p9g-joint-rf4");
    let controller_addresses = [reserve_addr(), reserve_addr(), reserve_addr()];
    let mut controllers = BTreeMap::new();
    for id in 1..=3_u64 {
        controllers.insert(
            cid(id),
            start_controller_node(p9_controller_config(&root, id, &controller_addresses))
                .await
                .unwrap(),
        );
    }
    let _ = stable_controller_leader(&controllers).await;
    eprintln!("P9G RF4 phase=controller-quorum old_only_absent={old_only_absent}");

    let client_addresses = (0..4).map(|_| reserve_addr()).collect::<Vec<_>>();
    let raft_addresses = (0..4).map(|_| reserve_addr()).collect::<Vec<_>>();
    for broker_id in 1..=4_u32 {
        let index = usize::try_from(broker_id - 1).unwrap();
        mutate(
            &controllers,
            ControllerCommand::RegisterBroker {
                broker_id,
                node_uuid: [u8::try_from(broker_id).unwrap(); 16],
                initial_incarnation: 1,
                client_host: client_addresses[index].ip().to_string(),
                client_port: client_addresses[index].port(),
                internal_host: raft_addresses[index].ip().to_string(),
                internal_port: raft_addresses[index].port(),
                rack: Some(format!("rack-{broker_id}")),
                zone: Some(format!("zone-{broker_id}")),
                min_feature: 1,
                max_feature: 5,
            },
        )
        .await;
        mutate(
            &controllers,
            ControllerCommand::SetBrokerLifecycle {
                broker_id,
                expected_incarnation: 1,
                expected_lifecycle: BrokerLifecycle::Joining,
                new_lifecycle: BrokerLifecycle::Active,
            },
        )
        .await;
    }
    let feature_deadline = time::Instant::now() + Duration::from_secs(30);
    loop {
        let leader = stable_controller_leader(&controllers).await;
        match controllers[&leader].bump_feature_level(5).await {
            Ok(_) => break,
            Err(
                astralog_controller::ControllerError::NotLeader { .. }
                | astralog_controller::ControllerError::Busy(_),
            ) => {}
            Err(error) => panic!("P9-G joint feature activation failed: {error}"),
        }
        assert!(time::Instant::now() < feature_deadline);
        time::sleep(Duration::from_millis(20)).await;
    }

    let controller_clients = controllers
        .values()
        .map(ControllerNodeHandle::client)
        .collect::<Vec<_>>();
    let make_broker_config = |broker_id: u32| {
        let index = usize::try_from(broker_id - 1).unwrap();
        let mut config = BrokerConfig::new(
            root.join(format!("broker-{broker_id}")),
            client_addresses[index],
        );
        config.advertised_host = client_addresses[index].ip().to_string();
        config.advertised_port = client_addresses[index].port();
        config.broker_id = broker_id;
        let mut replication = DynamicReplicationConfig::new(raft_addresses[index]);
        replication.tick_interval = Duration::from_millis(20);
        let election_min = 8 + u64::from(broker_id - 1) * 4;
        replication.election_timeout_min_ticks = election_min;
        replication.election_timeout_max_ticks = election_min + 1;
        // Keep the active tail below the roll threshold after two deliberately
        // oversized closure writes. Leadership barriers should not manufacture
        // fresh clean evidence by forcing incidental manifest generations.
        replication.partition_log = PartitionLogConfig {
            max_segment_bytes: 4096,
            max_segment_age_ms: None,
            index_interval_bytes: 1,
            limits: StorageLimits::default(),
        };
        replication.enable_p9g_progress_reports = true;
        replication.enable_p9g_rebootstrap_v2 = true;
        let mut dynamic = DynamicControllerConfig::new(controller_clients.clone(), replication);
        dynamic.metadata_refresh_interval = match broker_id {
            1 | 4 => Duration::from_secs(2),
            _ => Duration::from_secs(10),
        };
        dynamic.preferred_leader_timeout = Duration::from_secs(10);
        dynamic.max_pending_admin_operations = 16;
        config.dynamic_controller = Some(dynamic);
        config
    };
    let mut brokers = Vec::new();
    for broker_id in 1..=4_u32 {
        brokers.push(Some(
            start_dynamic_broker_when_controller_ready(make_broker_config(broker_id), &controllers)
                .await,
        ));
    }
    let healthy_deadline = time::Instant::now() + Duration::from_secs(20);
    loop {
        let leader = failure_detector_tick_on_current_leader(&controllers, 5_000).await;
        let mut healthy = true;
        for broker_id in 1..=4_u32 {
            healthy &= controllers[&leader]
                .broker_liveness(broker_id)
                .await
                .is_ok_and(|observation| observation.state == BrokerLivenessState::Healthy);
        }
        if healthy {
            break;
        }
        assert!(time::Instant::now() < healthy_deadline);
        time::sleep(Duration::from_millis(30)).await;
    }

    let created = create_retained_topic_on_current_leader(
        &controllers,
        CreateTopicSpec {
            topic_name: "p9g-joint-rf4".into(),
            partition_count: 1,
            replication_factor: 3,
            failure_domain_policy: FailureDomainPolicy::Disabled,
        },
        TopicRetentionSpecV1 {
            cleanup_policy: ControllerCleanupPolicyV1::CompactDelete,
            retention_ms: None,
            retention_bytes: None,
            tombstone_grace_ms: 1,
            subscription_protection: ControllerSubscriptionProtectionV1::Disabled,
        },
    )
    .await;
    eprintln!("P9G RF4 phase=topic-created old_only_absent={old_only_absent}");
    assert_eq!(created.assignments[0].voters, vec![1, 2, 3]);
    let client = AstraClient::connect(ClientConfig::new(
        brokers[0].as_ref().unwrap().local_addr().to_string(),
    ))
    .await
    .unwrap();

    p9g_transfer_to_current_leader(
        &brokers,
        created.topic.topic_id,
        1,
        "establish source broker as pre-movement leader",
    )
    .await;
    let _initial_source_epoch = loop {
        let observation = brokers[0]
            .as_ref()
            .unwrap()
            .observe_partition_internal(created.topic.topic_id, 0)
            .await
            .unwrap();
        if observation.local_is_leader {
            break observation.leader_epoch;
        }
        time::sleep(Duration::from_millis(20)).await;
    };

    quorum_produce_with_refresh(
        &client,
        "p9g-joint-rf4",
        b"joint-key".to_vec(),
        b"old-value".to_vec(),
        Duration::from_secs(10),
        "P9-G joint initial value",
    )
    .await;
    quorum_produce_with_refresh(
        &client,
        "p9g-joint-rf4",
        b"joint-survivor".to_vec(),
        vec![b's'; 256],
        Duration::from_secs(10),
        "P9-G joint independent retained key",
    )
    .await;
    let tombstone = client
        .produce(
            "p9g-joint-rf4",
            Some(b"joint-key".to_vec()),
            None,
            Vec::new(),
            AckMode::Quorum,
        )
        .await
        .expect("P9-G joint tombstone must be quorum committed");
    assert_eq!(
        (tombstone.base_offset, tombstone.last_offset),
        (Some(2), Some(2))
    );
    quorum_produce_with_refresh(
        &client,
        "p9g-joint-rf4",
        b"joint-close-1".to_vec(),
        vec![b'f'; 5_000],
        Duration::from_secs(10),
        "P9-G joint close tombstone segment",
    )
    .await;
    // The oversized close can coincide with an ordinary P3 election or
    // controller-driven runtime reconciliation. Re-establish actual source
    // leadership before issuing the next non-idempotent P2 mutation, rather
    // than attempting to recover by replaying an ambiguous Produce response.
    p9g_transfer_to_current_leader(
        &brokers,
        created.topic.topic_id,
        1,
        "stable source before non-idempotent bounded active tail",
    )
    .await;
    let _ = client.refresh_metadata().await.unwrap();
    quorum_produce_with_refresh(
        &client,
        "p9g-joint-rf4",
        b"joint-active-tail".to_vec(),
        vec![b't'; 64],
        Duration::from_secs(10),
        "P9-G joint create bounded active tail",
    )
    .await;

    let old_catchup_deadline = time::Instant::now() + Duration::from_secs(12);
    loop {
        let expected = brokers[0]
            .as_ref()
            .unwrap()
            .observe_partition_internal(created.topic.topic_id, 0)
            .await
            .unwrap()
            .committed_raft_index;
        let mut caught_up = true;
        for broker in &brokers[..3] {
            caught_up &= broker
                .as_ref()
                .unwrap()
                .observe_partition_internal(created.topic.topic_id, 0)
                .await
                .is_ok_and(|observation| observation.committed_raft_index >= expected);
        }
        if caught_up {
            break;
        }
        assert!(time::Instant::now() < old_catchup_deadline);
        time::sleep(Duration::from_millis(30)).await;
    }

    // The epoch used by the COW publication fence must be re-read after
    // catch-up. A locally observed earlier leadership epoch is not authority
    // for a later storage rewrite if P3 has since elected a successor.
    let source_epoch = p9g_transfer_to_current_leader(
        &brokers,
        created.topic.topic_id,
        1,
        "fresh source P3 authority for joint-setup COW",
    )
    .await;

    let source_compaction = brokers[0]
        .as_ref()
        .unwrap()
        .run_keyed_compaction_once(
            created.topic.topic_id,
            0,
            source_epoch,
            CompactionLimits::default(),
        )
        .await
        .expect("P9-G joint source COW must be accepted")
        .expect("P9-G joint source must have a closed superseded keyed segment");
    assert!(source_compaction.outcome.replaced_segments > 0);

    let mut topic_hex = String::new();
    for byte in created.topic.topic_id.as_bytes() {
        use std::fmt::Write as _;
        write!(&mut topic_hex, "{byte:02x}").unwrap();
    }
    let partition_path = |broker_id: u32| {
        root.join(format!("broker-{broker_id}"))
            .join("partitions")
            .join(&topic_hex)
            .join("0000000000")
    };
    let source_partition = partition_path(1);
    assert_eq!(durable_safe(&source_partition), Some(0));
    assert_eq!(
        durable_key_versions(&source_partition, b"joint-key"),
        Some(vec![(2, None)]),
        "pre-safe COW may remove the obsolete value but must retain the latest tombstone"
    );
    assert!(
        current_clean(&source_partition, created.topic.topic_id)
            .is_some_and(|(_, clean)| clean > 0)
    );
    eprintln!("P9G RF4 phase=source-cow old_only_absent={old_only_absent}");

    let movement = loop {
        let leader = current_leader(&controllers).await;
        match controllers[&leader]
            .begin_reassignment(ReassignmentSpec {
                topic_id: created.topic.topic_id,
                partition_id: 0,
                source_broker_id: 1,
                destination_broker_id: 4,
                preferred_leader: None,
            })
            .await
        {
            Ok(result) => break result,
            Err(astralog_controller::ControllerError::NotLeader { .. }) => continue,
            Err(error) => panic!("P9-G joint reassignment admission failed: {error}"),
        }
    };
    let joint_deadline = time::Instant::now() + Duration::from_secs(35);
    let joint_snapshot = loop {
        let snapshot = controller_snapshot(&controllers).await;
        let stage = snapshot.state.movements()[&movement.movement.movement_id].stage;
        if stage == MovementStage::JointCommitted {
            break snapshot;
        }
        assert!(
            !matches!(
                stage,
                MovementStage::LeaveJointRequested
                    | MovementStage::NewStableCommitted
                    | MovementStage::LeadershipTransfer
                    | MovementStage::OldReplicaRemoveRequested
                    | MovementStage::OldReplicaRemoved
                    | MovementStage::Complete
            ),
            "P9-G joint acceptance missed the committed joint boundary; stage={stage:?}"
        );
        assert!(
            time::Instant::now() < joint_deadline,
            "P9-G live movement did not reach JointCommitted; stage={stage:?}"
        );
        time::sleep(Duration::from_millis(10)).await;
    };
    let joint_assignment = &joint_snapshot.state.assignments()[&(created.topic.topic_id, 0)];
    let joint_movement = &joint_snapshot.state.movements()[&movement.movement.movement_id];
    assert_eq!(joint_assignment.voters, vec![1, 2, 3]);
    assert_eq!(joint_movement.target_voters, vec![2, 3, 4]);
    assert_eq!(joint_movement.stage, MovementStage::JointCommitted);

    // An administrative JointCommitted stage alone is not proof that P3 has
    // durably adopted joint consensus. Inspect an actual running replica's
    // membership actor and require the exact committed four-voter union.
    let joint_observation = time::timeout(
        Duration::from_secs(3),
        brokers[0]
            .as_ref()
            .unwrap()
            .observe_p9g_membership_internal(created.topic.topic_id, 0),
    )
    .await;
    let committed_joint = match joint_observation {
        Ok(Ok(observed)) => observed,
        Ok(Err(error)) => panic!("P9-G P3 joint observation unavailable: {error:?}"),
        Err(_) => {
            let latest_controller = controller_snapshot(&controllers).await;
            let mut actors = Vec::new();
            for (index, broker) in brokers.iter().enumerate() {
                if let Some(broker) = broker {
                    actors.push((
                        index + 1,
                        format!(
                            "{:?}",
                            time::timeout(
                                Duration::from_millis(350),
                                broker.observe_partition_internal(created.topic.topic_id, 0),
                            )
                            .await
                        ),
                    ));
                }
            }
            panic!(
                "P9-G P3 joint observation timed out after 3s; controller_stage={:?} live_actors={actors:?}",
                latest_controller.state.movements()[&movement.movement.movement_id].stage,
            );
        }
    };
    // Frozen P4 transport identity mapping is NodeId(broker_id + 1), with
    // NodeId(1) reserved. Compare actual P3 node IDs, never public broker IDs.
    let node_for_broker = |broker_id: u32| NodeId::new(u64::from(broker_id) + 1).unwrap();
    let expected_old =
        StableConfiguration::new([1_u32, 2, 3].map(node_for_broker), [node_for_broker(4)]).unwrap();
    let expected_new = StableConfiguration::new([2_u32, 3, 4].map(node_for_broker), []).unwrap();
    match &committed_joint.effective_configuration {
        EffectiveConfiguration::Joint(joint) => {
            assert_eq!(joint.old(), &expected_old);
            assert_eq!(joint.new_config(), &expected_new);
            assert_eq!(
                committed_joint.effective_configuration.voters(),
                [1_u32, 2, 3, 4].map(node_for_broker).into_iter().collect(),
                "P9-G live committed joint must include all four P3 voters"
            );
        }
        other => panic!(
            "P9-G controller JointCommitted did not coincide with actual P3 joint membership: {other:?}"
        ),
    }
    assert!(
        committed_joint
            .pending_configuration_index
            .is_some_and(|index| index <= committed_joint.durable_commit_index),
        "P9-G P3 joint membership must be committed, not a proposed but uncommitted config"
    );

    let controller = current_leader(&controllers).await;
    let destination_progress = controllers[&controller]
        .client()
        .replica_progress_observation(
            4,
            created.topic.topic_id,
            0,
            created.assignments[0].assignment_generation,
        )
        .await
        .unwrap()
        .expect("JointCommitted requires live destination progress from broker 4");
    assert_eq!(destination_progress.authority.incarnation_epoch, 1);
    eprintln!("P9G RF4 phase=joint-committed old_only_absent={old_only_absent}");

    if old_only_absent {
        // Broker 1 is an *old-only* P3/P5 voter. Once joint is committed,
        // old quorum {1,2,3} still has {2,3}, and new quorum {2,3,4}
        // remains intact, so quorum progress is possible even without 1.
        // Its disappearance must nevertheless block *all-voter* P9-G safe
        // authority until P5 has committed a new assignment excluding 1.
        brokers[0].take().unwrap().shutdown().await.unwrap();
        time::sleep(Duration::from_millis(350)).await;
        let absent_snapshot = controller_snapshot(&controllers).await;
        assert_eq!(
            absent_snapshot.state.movements()[&movement.movement.movement_id].stage,
            MovementStage::JointCommitted,
            "old-only outage must be observed before the controller authorizes P5 exclusion"
        );
        let still_committed = &absent_snapshot.state.assignments()[&(created.topic.topic_id, 0)];
        assert_eq!(still_committed.voters, vec![1, 2, 3]);
        assert_eq!(still_committed.assignment_generation.get(), 1);
        let surviving_joint = time::timeout(
            Duration::from_secs(2),
            brokers[1]
                .as_ref()
                .unwrap()
                .observe_p9g_membership_internal(created.topic.topic_id, 0),
        )
        .await
        .expect("surviving P3 voter did not respond after old-only outage")
        .expect("surviving P3 voter had no committed joint membership");
        assert!(
            matches!(
                &surviving_joint.effective_configuration,
                EffectiveConfiguration::Joint(_)
            ),
            "P9-G old-only negative must use a real still-joint P3 voter, not an already-stable new membership"
        );
        assert_eq!(
            surviving_joint.effective_configuration.voters(),
            [1_u32, 2, 3, 4].map(node_for_broker).into_iter().collect(),
            "missing broker 1 remains an eligible effective P3 voter until committed exclusion"
        );
        for broker_id in [2_u32, 3, 4] {
            let surviving = partition_path(broker_id);
            assert_eq!(
                durable_safe(&surviving),
                Some(0),
                "a Raft joint majority without old-only broker 1 cannot establish P9-G all-voter safe authority; survivor={broker_id}"
            );
            assert_eq!(
                durable_key_versions(&surviving, b"joint-key")
                    .and_then(|versions| versions.last().cloned()),
                Some((2, None)),
                "no surviving voter may physically omit the latest marker while committed P5 voter 1 is missing; survivor={broker_id}"
            );
        }
    } else {
        // Missing new-only voter: both old and new joint quorums still have 2/3,
        // but all-effective-voter clean authority must not collapse to a quorum.
        brokers[3].take().unwrap().shutdown().await.unwrap();
        time::sleep(Duration::from_millis(150)).await;
        assert_eq!(
            controller_snapshot(&controllers).await.state.movements()
                [&movement.movement.movement_id]
                .stage,
            MovementStage::JointCommitted,
            "new-only voter absence must be observed while P3 is still in joint configuration"
        );
        assert_eq!(durable_safe(&source_partition), Some(0));
        assert_eq!(
            durable_key_versions(&source_partition, b"joint-key")
                .and_then(|versions| versions.last().cloned()),
            Some((2, None)),
            "an absent joint voter must not authorize physical tombstone removal"
        );

        // Rotate the absent destination's committed P5 identity. The controller's
        // prior incarnation-1 progress observation must immediately cease to be
        // usable before the incarnation-2 broker is restarted.
        mutate(
            &controllers,
            ControllerCommand::RejoinBroker {
                broker_id: 4,
                node_uuid: [4; 16],
                expected_incarnation: 1,
                new_incarnation: 2,
                client_host: client_addresses[3].ip().to_string(),
                client_port: client_addresses[3].port(),
                internal_host: raft_addresses[3].ip().to_string(),
                internal_port: raft_addresses[3].port(),
                rack: Some("rack-4".into()),
                zone: Some("zone-4".into()),
                min_feature: 1,
                max_feature: 5,
            },
        )
        .await;
        let controller = current_leader(&controllers).await;
        assert!(
            controllers[&controller]
                .client()
                .replica_progress_observation(
                    4,
                    created.topic.topic_id,
                    0,
                    created.assignments[0].assignment_generation,
                )
                .await
                .unwrap()
                .is_none(),
            "P5 incarnation rotation must invalidate cached destination progress from incarnation 1"
        );
        brokers[3] = Some(
            start_dynamic_broker_when_controller_ready(make_broker_config(4), &controllers).await,
        );
        assert_eq!(
            controller_snapshot(&controllers).await.state.movements()
                [&movement.movement.movement_id]
                .stage,
            MovementStage::JointCommitted,
            "P9-G stale-incarnation proof must remain at the live joint boundary"
        );

        let fresh_destination_deadline = time::Instant::now() + Duration::from_secs(5);
        loop {
            let controller = current_leader(&controllers).await;
            let fresh = controllers[&controller]
                .client()
                .replica_progress_observation(
                    4,
                    created.topic.topic_id,
                    0,
                    created.assignments[0].assignment_generation,
                )
                .await;
            if fresh.as_ref().is_ok_and(|observation| {
                observation
                    .as_ref()
                    .is_some_and(|observation| observation.authority.incarnation_epoch == 2)
            }) {
                break;
            }
            assert!(
                time::Instant::now() < fresh_destination_deadline,
                "incarnation-2 destination did not publish fresh controller-observed progress: {fresh:?}"
            );
            time::sleep(Duration::from_millis(30)).await;
        }
    }

    let completion_deadline = time::Instant::now() + Duration::from_secs(30);
    eprintln!("P9G RF4 phase=negative-fault-complete old_only_absent={old_only_absent}");
    let final_snapshot = loop {
        let snapshot = controller_snapshot(&controllers).await;
        if snapshot.state.movements()[&movement.movement.movement_id].stage
            == MovementStage::Complete
        {
            break snapshot;
        }
        if time::Instant::now() >= completion_deadline {
            // Failure-only diagnostics: preserve the original bounded
            // completion gate, but report whether the live P3 actors have a
            // leader and durable commit progress. Do not mutate P3/P5 state
            // or treat a controller movement stage as P3 membership proof.
            let mut actor_status = Vec::new();
            for broker_id in 1_u32..=4 {
                let index = usize::try_from(broker_id - 1).unwrap();
                let observed = match brokers[index].as_ref() {
                    Some(broker) => {
                        format!(
                            "{:?}",
                            time::timeout(
                                Duration::from_millis(300),
                                broker.observe_partition_internal(created.topic.topic_id, 0)
                            )
                            .await
                        )
                    }
                    None => "not running".into(),
                };
                actor_status.push((broker_id, observed));
            }
            panic!(
                "P9-G joint movement did not complete after safe negative phases; stage={:?} controller_epoch={} live_p3_actor_status={actor_status:?}",
                snapshot.state.movements()[&movement.movement.movement_id].stage,
                snapshot.epoch
            );
        }
        time::sleep(Duration::from_millis(40)).await;
    };
    let final_assignment = &final_snapshot.state.assignments()[&(created.topic.topic_id, 0)];
    eprintln!("P9G RF4 phase=movement-complete old_only_absent={old_only_absent}");
    assert_eq!(final_assignment.voters, vec![2, 3, 4]);
    assert_eq!(final_assignment.assignment_generation.get(), 2);
    if old_only_absent {
        assert!(brokers[0].is_none(), "old-only voter 1 must remain absent");
        assert_eq!(
            durable_safe(&source_partition),
            Some(0),
            "the removed old-only voter's orphan disk may not manufacture committed P9-G safe authority"
        );
    }

    // Controller completion is not enough for a broker-local maintenance
    // attempt: every target broker must apply the same *current* controller
    // metadata epoch, not merely expose the final voter IDs. The compaction
    // hook captures policy_epoch before planning and refreshes controller state
    // before COW publication; planning against an older broker-local epoch is
    // therefore correctly fenced as StaleEpoch. Require one bounded interval
    // where controller epoch is stable and all three target brokers report that
    // exact epoch plus the finalized [2,3,4] assignment.
    let metadata_sync_deadline = time::Instant::now() + Duration::from_secs(15);
    loop {
        let controller_before = controller_snapshot(&controllers).await;
        let expected_metadata_epoch = controller_before.epoch;
        assert!(
            expected_metadata_epoch >= final_snapshot.epoch,
            "controller metadata epoch regressed below the committed movement-complete snapshot"
        );
        let mut all_synced = true;
        let mut observed = Vec::new();
        for broker_id in [2_u32, 3, 4] {
            let index = usize::try_from(broker_id - 1).unwrap();
            let mut local_config =
                ClientConfig::new(brokers[index].as_ref().unwrap().local_addr().to_string());
            local_config.request_timeout = Duration::from_secs(2);
            let local = AstraClient::connect(local_config).await.unwrap();
            let metadata = local.refresh_metadata().await;
            all_synced &= metadata.as_ref().is_ok_and(|metadata| {
                metadata.metadata_epoch == expected_metadata_epoch
                    && metadata
                        .topics
                        .iter()
                        .find(|topic| topic.topic_id == created.topic.topic_id)
                        .is_some_and(|topic| {
                            topic.partitions[0].replica_broker_ids == vec![2, 3, 4]
                        })
            });
            observed.push((broker_id, metadata));
            local.close().await;
        }
        let controller_after = controller_snapshot(&controllers).await;
        if all_synced && controller_after.epoch == expected_metadata_epoch {
            break;
        }
        assert!(
            time::Instant::now() < metadata_sync_deadline,
            "P9-G target brokers did not synchronize to one stable current controller metadata epoch before maintenance: expected={expected_metadata_epoch} after={} observed={observed:?}",
            controller_after.epoch
        );
        time::sleep(Duration::from_millis(30)).await;
    }

    // Positive phase: every effective voter in the committed stable target
    // must derive positive clean progress through its own current manifest.
    // First give the actor-owned bounded current-generation revalidator time
    // to repair a harmless generation roll. Only genuinely unclean history is
    // then COW-compacted; no old-generation sidecar is accepted or copied.
    let mut stable_clean = BTreeMap::new();
    for broker_id in [2_u32, 3, 4] {
        p9g_transfer_to_current_leader(
            &brokers,
            created.topic.topic_id,
            broker_id,
            "stable target voter clean qualification",
        )
        .await;
        let index = usize::try_from(broker_id - 1).unwrap();
        let partition = partition_path(broker_id);
        let revalidation_deadline = time::Instant::now() + Duration::from_millis(1_500);
        let before = loop {
            let observed = current_clean(&partition, created.topic.topic_id).unwrap_or((0, 0));
            if observed.1 > 0 || time::Instant::now() >= revalidation_deadline {
                break observed;
            }
            time::sleep(Duration::from_millis(30)).await;
        };
        if before.1 == 0 {
            let leader_epoch = p9g_transfer_to_current_leader(
                &brokers,
                created.topic.topic_id,
                broker_id,
                "stable target voter COW authorization",
            )
            .await;
            let compacted = brokers[index]
                .as_ref()
                .unwrap()
                .run_keyed_compaction_once(
                    created.topic.topic_id,
                    0,
                    leader_epoch,
                    CompactionLimits::default(),
                )
                .await
                .expect("stable target voter COW must be accepted");
            assert!(
                compacted.is_some(),
                "P9-G voter {broker_id} remains zero after bounded actor revalidation and has no COW work; current manifest cannot establish fresh clean proof"
            );
        }
        let clean_deadline = time::Instant::now() + Duration::from_millis(1_500);
        let clean = loop {
            let observed = current_clean(&partition, created.topic.topic_id)
                .expect("stable target voter must expose an authoritative manifest");
            if observed.1 > 0 || time::Instant::now() >= clean_deadline {
                break observed;
            }
            time::sleep(Duration::from_millis(30)).await;
        };
        stable_clean.insert(broker_id, clean);
    }
    assert!(
        stable_clean.values().all(|(_, clean)| *clean > 0),
        "P9-G stable all-voter positive minimum is blocked even after bounded current-manifest revalidation/COW; current={stable_clean:?}"
    );
    eprintln!("P9G RF4 phase=all-clean old_only_absent={old_only_absent}");
    let expected_safe = stable_clean
        .values()
        .map(|(_, clean)| *clean)
        .min()
        .unwrap();
    assert!(
        expected_safe > 2,
        "safe lower bound must cover the tombstone offset"
    );

    let safe_deadline = time::Instant::now() + Duration::from_secs(15);
    loop {
        let durable = [2_u32, 3, 4].map(|broker_id| durable_safe(&partition_path(broker_id)));
        if durable.iter().all(|safe| *safe == Some(expected_safe)) {
            break;
        }
        assert!(
            durable.iter().flatten().all(|safe| *safe <= expected_safe),
            "committed P9-G safe state exceeded the actual stable all-voter minimum: clean={stable_clean:?} durable={durable:?}"
        );
        assert!(
            time::Instant::now() < safe_deadline,
            "stable all-voter minimum never became quorum-committed durable authority: clean={stable_clean:?} durable={durable:?}"
        );
        time::sleep(Duration::from_millis(30)).await;
    }

    let positive_leader = 4_u32;
    let positive_index = usize::try_from(positive_leader - 1).unwrap();
    let positive_epoch = p9g_transfer_to_current_leader(
        &brokers,
        created.topic.topic_id,
        positive_leader,
        "fresh post-safe tombstone reclaim leader",
    )
    .await;
    let removed = brokers[positive_index]
        .as_ref()
        .unwrap()
        .run_keyed_compaction_once(
            created.topic.topic_id,
            0,
            positive_epoch,
            CompactionLimits::default(),
        )
        .await
        .expect("post-safe tombstone COW must be accepted")
        .expect("post-safe tombstone source must remain physically reclaimable");
    assert!(removed.outcome.replaced_segments > 0);
    assert_eq!(
        durable_key_versions(&partition_path(positive_leader), b"joint-key"),
        Some(Vec::new()),
        "physical tombstone removal is permitted only after committed all-voter safe authority"
    );

    // Final failover: the replica that physically removed the marker can die;
    // a successor retaining its own marker must preserve the safe control and
    // never expose the obsolete pre-tombstone value.
    brokers[positive_index]
        .take()
        .unwrap()
        .shutdown()
        .await
        .unwrap();
    // A committed P5 stable configuration is authoritative, but metadata
    // reconciliation can briefly restart an unchanged P3 actor. Require
    // bounded eventual *actual* successor leadership rather than declaring a
    // five-second controller-scheduling delay a safety failure.
    let final_failover_deadline = time::Instant::now() + Duration::from_secs(20);
    let final_successor = loop {
        let mut found = None;
        let mut last_observed = Vec::new();
        for broker_id in [2_u32, 3] {
            let index = usize::try_from(broker_id - 1).unwrap();
            if let Some(broker) = brokers[index].as_ref() {
                let observed = time::timeout(
                    Duration::from_millis(500),
                    broker.observe_partition_internal(created.topic.topic_id, 0),
                )
                .await;
                if observed.as_ref().is_ok_and(|result| {
                    result.as_ref().is_ok_and(|state| {
                        state.local_is_leader && state.leader_epoch > positive_epoch
                    })
                }) {
                    found = Some(broker_id);
                    break;
                }
                last_observed.push((broker_id, format!("{observed:?}")));
            }
        }
        if let Some(found) = found {
            break found;
        }
        assert!(
            time::Instant::now() < final_failover_deadline,
            "P9-G RF4 stable committed voters did not elect a real P3 successor after cleaned leader shutdown: {last_observed:?}"
        );
        time::sleep(Duration::from_millis(20)).await;
    };
    let final_partition = partition_path(final_successor);
    assert_eq!(durable_safe(&final_partition), Some(expected_safe));
    assert!(
        durable_key_versions(&final_partition, b"joint-key")
            .is_some_and(|versions| versions.iter().all(|(_, value)| value.is_none())),
        "leader failover after safe commit must not resurrect the obsolete keyed value"
    );

    client.close().await;
    for broker in brokers.into_iter().flatten() {
        broker.shutdown().await.unwrap();
    }
    for (_, controller) in controllers {
        controller.shutdown().await;
    }
    fs::remove_dir_all(root).unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 6)]
async fn p9d_rf3_retention_floor_survives_lagging_rejoin_and_leader_failover() {
    let _test_guard = p5_integration_lock().lock().await;
    let root = temp_dir("p9d-rf3-retention");
    let controller_addresses = [reserve_addr(), reserve_addr(), reserve_addr()];
    let mut controllers = BTreeMap::new();
    for id in 1..=3_u64 {
        controllers.insert(
            cid(id),
            start_controller_node(p9_controller_config(&root, id, &controller_addresses))
                .await
                .unwrap(),
        );
    }
    let _ = stable_controller_leader(&controllers).await;

    let client_addresses = (0..3).map(|_| reserve_addr()).collect::<Vec<_>>();
    let raft_addresses = (0..3).map(|_| reserve_addr()).collect::<Vec<_>>();
    for broker_id in 1..=3_u32 {
        let index = usize::try_from(broker_id - 1).unwrap();
        mutate(
            &controllers,
            ControllerCommand::RegisterBroker {
                broker_id,
                node_uuid: [u8::try_from(broker_id).unwrap(); 16],
                initial_incarnation: 1,
                client_host: client_addresses[index].ip().to_string(),
                client_port: client_addresses[index].port(),
                internal_host: raft_addresses[index].ip().to_string(),
                internal_port: raft_addresses[index].port(),
                rack: Some(format!("rack-{broker_id}")),
                zone: Some(format!("zone-{broker_id}")),
                min_feature: 1,
                max_feature: 5,
            },
        )
        .await;
        mutate(
            &controllers,
            ControllerCommand::SetBrokerLifecycle {
                broker_id,
                expected_incarnation: 1,
                expected_lifecycle: BrokerLifecycle::Joining,
                new_lifecycle: BrokerLifecycle::Active,
            },
        )
        .await;
    }

    let bump_deadline = time::Instant::now() + Duration::from_secs(30);
    loop {
        let leader = stable_controller_leader(&controllers).await;
        match controllers[&leader].bump_feature_level(5).await {
            Ok(_) => break,
            Err(
                astralog_controller::ControllerError::NotLeader { .. }
                | astralog_controller::ControllerError::Busy(_),
            ) => {}
            Err(error) => panic!("P9-D feature-level bump failed: {error}"),
        }
        assert!(time::Instant::now() < bump_deadline);
        time::sleep(Duration::from_millis(20)).await;
    }

    let controller_clients = controllers
        .values()
        .map(ControllerNodeHandle::client)
        .collect::<Vec<_>>();
    let make_broker_config = |broker_id: u32| {
        let index = usize::try_from(broker_id - 1).unwrap();
        let mut config = BrokerConfig::new(
            root.join(format!("broker-{broker_id}")),
            client_addresses[index],
        );
        config.advertised_host = client_addresses[index].ip().to_string();
        config.advertised_port = client_addresses[index].port();
        config.broker_id = broker_id;
        let mut replication = DynamicReplicationConfig::new(raft_addresses[index]);
        replication.tick_interval = Duration::from_millis(20);
        let election_min = 8 + u64::from(broker_id - 1) * 4;
        replication.election_timeout_min_ticks = election_min;
        replication.election_timeout_max_ticks = election_min + 1;
        replication.partition_log = PartitionLogConfig {
            max_segment_bytes: 768,
            max_segment_age_ms: None,
            index_interval_bytes: 1,
            limits: StorageLimits::default(),
        };
        let mut dynamic = DynamicControllerConfig::new(controller_clients.clone(), replication);
        dynamic.metadata_refresh_interval = Duration::from_millis(40);
        dynamic.preferred_leader_timeout = Duration::from_secs(10);
        dynamic.max_pending_admin_operations = 16;
        config.dynamic_controller = Some(dynamic);
        config
    };
    let mut brokers = Vec::new();
    for broker_id in 1..=3_u32 {
        brokers.push(Some(
            start_dynamic_broker_when_controller_ready(make_broker_config(broker_id), &controllers)
                .await,
        ));
    }

    let healthy_deadline = time::Instant::now() + Duration::from_secs(15);
    loop {
        let leader = failure_detector_tick_on_current_leader(&controllers, 5_000).await;
        let mut healthy = true;
        for broker_id in 1..=3_u32 {
            healthy &= controllers[&leader]
                .broker_liveness(broker_id)
                .await
                .is_ok_and(|observation| observation.state == BrokerLivenessState::Healthy);
        }
        if healthy {
            break;
        }
        assert!(time::Instant::now() < healthy_deadline);
        time::sleep(Duration::from_millis(30)).await;
    }

    let retention = TopicRetentionSpecV1 {
        cleanup_policy: ControllerCleanupPolicyV1::Delete,
        retention_ms: Some(1),
        retention_bytes: Some(1),
        tombstone_grace_ms: 86_400_000,
        subscription_protection: ControllerSubscriptionProtectionV1::Disabled,
    };
    let created = create_retained_topic_on_current_leader(
        &controllers,
        CreateTopicSpec {
            topic_name: "p9d-retention-rf3".into(),
            partition_count: 1,
            replication_factor: 3,
            failure_domain_policy: FailureDomainPolicy::Disabled,
        },
        retention,
    )
    .await;
    assert_eq!(created.assignments[0].voters, vec![1, 2, 3]);

    let client = AstraClient::connect(ClientConfig::new(
        brokers[0].as_ref().unwrap().local_addr().to_string(),
    ))
    .await
    .unwrap();
    let leader_deadline = time::Instant::now() + Duration::from_secs(15);
    let (initial_leader, initial_epoch) = loop {
        let metadata = client.refresh_metadata().await.unwrap();
        if let Some(partition) = metadata
            .topics
            .iter()
            .find(|topic| topic.topic_id == created.topic.topic_id)
            .map(|topic| &topic.partitions[0])
            && partition.leader_broker_id != UNKNOWN_BROKER_ID
            && partition.leader_epoch != UNKNOWN_EPOCH
        {
            break (partition.leader_broker_id, partition.leader_epoch);
        }
        assert!(time::Instant::now() < leader_deadline);
        time::sleep(Duration::from_millis(25)).await;
    };

    for ordinal in 0_u8..4 {
        quorum_produce_with_refresh(
            &client,
            "p9d-retention-rf3",
            vec![ordinal],
            vec![b'A' + ordinal; 2048],
            Duration::from_secs(10),
            "P9-D pre-retention RF3 history",
        )
        .await;
    }

    let convergence_deadline = time::Instant::now() + Duration::from_secs(15);
    loop {
        let controller = current_leader(&controllers).await;
        let mut committed = Vec::new();
        let mut complete = true;
        for broker_id in 1..=3_u32 {
            match controllers[&controller]
                .client()
                .replica_progress_observation(
                    broker_id,
                    created.topic.topic_id,
                    0,
                    created.assignments[0].assignment_generation,
                )
                .await
            {
                Ok(Some(observation)) => committed.push(observation.report.committed_raft_index),
                _ => complete = false,
            }
        }
        if complete
            && committed.len() == 3
            && committed[0] > 0
            && committed.windows(2).all(|window| window[0] == window[1])
        {
            break;
        }
        assert!(time::Instant::now() < convergence_deadline);
        time::sleep(Duration::from_millis(30)).await;
    }

    let lagging_broker = (1..=3_u32)
        .find(|broker_id| *broker_id != initial_leader)
        .unwrap();
    let lagging_index = usize::try_from(lagging_broker - 1).unwrap();
    brokers[lagging_index]
        .take()
        .unwrap()
        .shutdown()
        .await
        .unwrap();

    let initial_leader_index = usize::try_from(initial_leader - 1).unwrap();
    let retention_outcome = brokers[initial_leader_index]
        .as_ref()
        .unwrap()
        .run_delete_retention_once(created.topic.topic_id, 0, initial_epoch, None, i64::MAX / 4)
        .await
        .unwrap()
        .expect("P9-D time/size policy should advance a closed-segment floor");
    assert!(retention_outcome.log_start_offset > 0);
    assert!(retention_outcome.log_start_offset < 4);
    let floor = retention_outcome.log_start_offset;

    let expired = client
        .fetch(FetchRequestV0 {
            isolation: IsolationLevel::ReadCommitted,
            min_bytes: 0,
            max_wait_ms: 0,
            partitions: vec![FetchPartitionRequestV0 {
                topic_id: created.topic.topic_id,
                partition: 0,
                leader_epoch: UNKNOWN_EPOCH,
                fetch_offset: 0,
                max_bytes: 64 * 1024,
                max_records: 10,
            }],
            tags: Vec::new(),
        })
        .await
        .unwrap();
    assert_eq!(
        expired.partitions[0].error_code,
        PublicError::OffsetOutOfRange as u16
    );
    assert_eq!(expired.partitions[0].log_start_offset, floor);
    let earliest = client
        .list_offsets(ListOffsetsRequestV0 {
            queries: vec![ListOffsetsQueryV0 {
                topic_id: created.topic.topic_id,
                partition: 0,
                leader_epoch: UNKNOWN_EPOCH,
                timestamp_selector: -2,
            }],
            tags: Vec::new(),
        })
        .await
        .unwrap();
    assert_eq!(earliest.results[0].offset, floor);

    brokers[lagging_index] = Some(
        start_dynamic_broker_when_controller_ready(
            make_broker_config(lagging_broker),
            &controllers,
        )
        .await,
    );
    let catchup_deadline = time::Instant::now() + Duration::from_secs(20);
    loop {
        let controller = current_leader(&controllers).await;
        let mut committed = Vec::new();
        let mut complete = true;
        for broker_id in 1..=3_u32 {
            match controllers[&controller]
                .client()
                .replica_progress_observation(
                    broker_id,
                    created.topic.topic_id,
                    0,
                    created.assignments[0].assignment_generation,
                )
                .await
            {
                Ok(Some(observation)) => committed.push(observation.report.committed_raft_index),
                _ => complete = false,
            }
        }
        if complete
            && committed.len() == 3
            && committed.windows(2).all(|window| window[0] == window[1])
        {
            break;
        }
        assert!(
            time::Instant::now() < catchup_deadline,
            "lagging P9-D voter did not catch up through the committed floor"
        );
        time::sleep(Duration::from_millis(30)).await;
    }

    brokers[initial_leader_index]
        .take()
        .unwrap()
        .shutdown()
        .await
        .unwrap();
    let survivor_addr = brokers
        .iter()
        .flatten()
        .next()
        .unwrap()
        .local_addr()
        .to_string();
    let post = AstraClient::connect(ClientConfig::new(survivor_addr))
        .await
        .unwrap();
    let failover_deadline = time::Instant::now() + Duration::from_secs(20);
    let successor = loop {
        let metadata = post.refresh_metadata().await.unwrap();
        if let Some(partition) = metadata
            .topics
            .iter()
            .find(|topic| topic.topic_id == created.topic.topic_id)
            .map(|topic| &topic.partitions[0])
            && partition.leader_broker_id != UNKNOWN_BROKER_ID
            && partition.leader_broker_id != initial_leader
            && partition.leader_epoch > initial_epoch
        {
            break partition.leader_broker_id;
        }
        assert!(time::Instant::now() < failover_deadline);
        time::sleep(Duration::from_millis(30)).await;
    };

    let successor_index = usize::try_from(successor - 1).unwrap();
    assert_eq!(
        brokers[successor_index]
            .as_ref()
            .unwrap()
            .run_delete_retention_once(
                created.topic.topic_id,
                0,
                initial_epoch,
                None,
                i64::MAX / 4,
            )
            .await,
        Err(PublicError::StaleEpoch),
        "a pre-failover leader epoch must never authorize a P9-D retention proposal"
    );

    let after_failover = post
        .fetch(FetchRequestV0 {
            isolation: IsolationLevel::ReadCommitted,
            min_bytes: 0,
            max_wait_ms: 0,
            partitions: vec![FetchPartitionRequestV0 {
                topic_id: created.topic.topic_id,
                partition: 0,
                leader_epoch: UNKNOWN_EPOCH,
                fetch_offset: 0,
                max_bytes: 64 * 1024,
                max_records: 10,
            }],
            tags: Vec::new(),
        })
        .await
        .unwrap();
    assert_eq!(
        after_failover.partitions[0].error_code,
        PublicError::OffsetOutOfRange as u16
    );
    assert_eq!(after_failover.partitions[0].log_start_offset, floor);
    let after_failover_earliest = post
        .list_offsets(ListOffsetsRequestV0 {
            queries: vec![ListOffsetsQueryV0 {
                topic_id: created.topic.topic_id,
                partition: 0,
                leader_epoch: UNKNOWN_EPOCH,
                timestamp_selector: -2,
            }],
            tags: Vec::new(),
        })
        .await
        .unwrap();
    assert_eq!(after_failover_earliest.results[0].offset, floor);

    if successor != lagging_broker {
        let transfer = brokers[successor_index]
            .as_ref()
            .unwrap()
            .request_preferred_leader(PreferredLeaderRequest {
                topic_id: created.topic.topic_id,
                partition_id: 0,
                target_broker_id: lagging_broker,
            })
            .await
            .unwrap();
        assert!(matches!(
            transfer.disposition,
            PreferredLeaderDisposition::AlreadyPreferred
                | PreferredLeaderDisposition::TransferObserved
        ));
        let target_deadline = time::Instant::now() + Duration::from_secs(15);
        loop {
            let metadata = post.refresh_metadata().await.unwrap();
            if metadata
                .topics
                .iter()
                .find(|topic| topic.topic_id == created.topic.topic_id)
                .is_some_and(|topic| topic.partitions[0].leader_broker_id == lagging_broker)
            {
                break;
            }
            assert!(time::Instant::now() < target_deadline);
            time::sleep(Duration::from_millis(30)).await;
        }
        let lagging_as_leader = post
            .list_offsets(ListOffsetsRequestV0 {
                queries: vec![ListOffsetsQueryV0 {
                    topic_id: created.topic.topic_id,
                    partition: 0,
                    leader_epoch: UNKNOWN_EPOCH,
                    timestamp_selector: -2,
                }],
                tags: Vec::new(),
            })
            .await
            .unwrap();
        assert_eq!(lagging_as_leader.results[0].offset, floor);
    }

    client.close().await;
    post.close().await;
    for broker in brokers.into_iter().flatten() {
        broker.shutdown().await.unwrap();
    }
    for (_, controller) in controllers {
        controller.shutdown().await;
    }
    fs::remove_dir_all(root).unwrap();
}

async fn raw_hello(addr: SocketAddr) -> TcpStream {
    let mut stream = TcpStream::connect(addr).await.unwrap();
    stream
        .write_all(&ClientHello::v1().encode().unwrap())
        .await
        .unwrap();
    let mut response = [0_u8; 16];
    stream.read_exact(&mut response).await.unwrap();
    let hello = ServerHello::decode(&response).unwrap();
    assert_eq!(hello.selected_wire_version, WIRE_VERSION);
    stream
}

async fn send_request(
    stream: &mut TcpStream,
    api: ApiKey,
    correlation_id: u64,
    body: Vec<u8>,
) -> WireFrame {
    let response = send_request_raw_version(stream, api, 0, correlation_id, body).await;
    assert_eq!(response.error_code, 0);
    response
}

async fn send_request_version(
    stream: &mut TcpStream,
    api: ApiKey,
    api_version: u16,
    correlation_id: u64,
    body: Vec<u8>,
) -> WireFrame {
    let response = send_request_raw_version(stream, api, api_version, correlation_id, body).await;
    assert_eq!(response.error_code, 0);
    response
}

async fn send_request_raw_version(
    stream: &mut TcpStream,
    api: ApiKey,
    api_version: u16,
    correlation_id: u64,
    body: Vec<u8>,
) -> WireFrame {
    let request = WireFrame {
        kind: FrameKind::Request,
        flags: 0,
        api_key: api as u16,
        api_version,
        error_code: 0,
        correlation_id,
        timeout_ms: 3_000,
        throttle_ms: 0,
        header_tags: Vec::new(),
        body,
    };
    stream
        .write_all(&encode_frame(&request, MAX_WIRE_FRAME_LEN_V1).unwrap())
        .await
        .unwrap();
    let mut prefix = [0_u8; 4];
    stream.read_exact(&mut prefix).await.unwrap();
    let len = usize::try_from(u32::from_be_bytes(prefix)).unwrap();
    let mut bytes = vec![0_u8; len + 4];
    bytes[..4].copy_from_slice(&prefix);
    stream.read_exact(&mut bytes[4..]).await.unwrap();
    let response = decode_frame(&bytes, MAX_WIRE_FRAME_LEN_V1).unwrap();
    assert_eq!(response.correlation_id, correlation_id);
    response
}

#[allow(dead_code)]
fn _compile_reassignment_shape(spec: ReassignmentSpec) -> ReassignmentSpec {
    spec
}
