use std::{
    collections::BTreeMap,
    fs, io,
    net::{Ipv4Addr, SocketAddr},
    path::{Path, PathBuf},
    sync::{
        Arc,
        atomic::{AtomicBool, AtomicU32, Ordering},
    },
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use astralog_broker::{
    BrokerConfig, BrokerError, DynamicControllerConfig, DynamicReplicationConfig,
    start as start_broker,
};
use astralog_client::{AstraClient, ClientConfig};
use astralog_common::{ProducerAuthority, ProducerEpoch, ProducerId, ProducerSequence};
use astralog_controller::{
    BrokerLifecycle, BrokerLivenessState, ControllerCommand, ControllerNodeConfig,
    ControllerNodeHandle, ControllerNodeId, ControllerPeerDescriptor,
    ControllerPeerTransportConfig, ControllerPersistenceConfig, ControllerRuntimeConfig,
    ControllerRuntimeLimits, ControllerState, CreateTopicSpec, FailureDetectorConfig,
    FailureDomainPolicy, FeatureRange, HeartbeatLimits, start_controller_node,
};
use astralog_protocol::{
    ApiKey, MAX_WIRE_FRAME_LEN_V1, PublicError, UNKNOWN_EPOCH,
    api::{FetchPartitionRequestV0, FetchRequestV0, IsolationLevel, WireRecordV0},
    decode_frame,
};
use astralog_raft::{NodeId, Role, StableConfiguration, TimingConfig};
use astralog_storage::DataRecord;
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{TcpListener, TcpStream},
    sync::Mutex,
    task::JoinHandle,
    time,
};

fn temp_dir(label: &str) -> PathBuf {
    let nonce = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    let path = std::env::temp_dir().join(format!(
        "astralog-p7f-e2e-{label}-{}-{nonce}",
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
    astralog_controller::initialize_controller_bootstrap(&directory, [7; 16], &voters()).unwrap();
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
    ControllerNodeConfig {
        runtime: ControllerRuntimeConfig {
            local_node: cid(id),
            directory,
            static_configuration: voters(),
            controller_feature_ranges: (1..=3_u64)
                .map(|node| (cid(node), FeatureRange::new(1, 3).unwrap()))
                .collect(),
            initial_state: ControllerState::new([7; 16], 1, 1_000).unwrap(),
            persistence: ControllerPersistenceConfig::default(),
            timing: TimingConfig::new(1, 5, 10).unwrap(),
            randomized_election_timeout: timeout,
            limits: ControllerRuntimeLimits::default(),
            heartbeat_limits: HeartbeatLimits::default(),
            failure_detector: FailureDetectorConfig::default(),
        },
        transport: ControllerPeerTransportConfig {
            local_controller_id: cid(id),
            cluster_id: [7; 16],
            bind_addr: addresses[usize::try_from(id - 1).unwrap()],
            peers,
            limits: astralog_controller::ControllerTransportLimits::default(),
            max_inbound_frames: 1024,
            max_outbound_frames_per_peer: 1024,
            max_outbound_bytes_per_peer: 16 * 1024 * 1024,
            reconnect_min_delay: Duration::from_millis(5),
            reconnect_max_delay: Duration::from_millis(50),
        },
        tick_period: Duration::from_millis(25),
        election_timeout_sample: timeout,
        max_control_requests: 128,
    }
}

async fn current_leader(
    nodes: &BTreeMap<ControllerNodeId, ControllerNodeHandle>,
) -> ControllerNodeId {
    let deadline = time::Instant::now() + Duration::from_secs(30);
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
        assert!(
            time::Instant::now() < deadline,
            "P7-F controller election timed out"
        );
        time::sleep(Duration::from_millis(20)).await;
    }
}

// Initialize the detector on the *current* leader. A leader change resets its
// volatile heartbeat observations; retry only leadership/queue races rather
// than ignoring an uninstalled tick and waiting for impossible liveness.
async fn detector_tick(
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
            Err(error) => panic!("P7-F/H failure-detector tick failed: {error}"),
        }
        assert!(
            time::Instant::now() < deadline,
            "P7-F/H failure-detector leader never accepted initial tick"
        );
        time::sleep(Duration::from_millis(20)).await;
    }
}

async fn mutate(
    nodes: &BTreeMap<ControllerNodeId, ControllerNodeHandle>,
    command: ControllerCommand,
) -> astralog_controller::ApplyOutcome {
    let deadline = time::Instant::now() + Duration::from_secs(30);
    loop {
        let leader = current_leader(nodes).await;
        match nodes[&leader].mutate(command.clone()).await {
            Ok(outcome) => return outcome,
            Err(astralog_controller::ControllerError::NotLeader { .. }) => {}
            Err(error) => panic!("P7-F controller mutation failed: {error}"),
        }
        assert!(time::Instant::now() < deadline);
        time::sleep(Duration::from_millis(20)).await;
    }
}

async fn create_topic(
    nodes: &BTreeMap<ControllerNodeId, ControllerNodeHandle>,
) -> astralog_controller::TopicCreateResult {
    let deadline = time::Instant::now() + Duration::from_secs(30);
    loop {
        let leader = current_leader(nodes).await;
        match nodes[&leader]
            .create_topic(CreateTopicSpec {
                topic_name: "p7f-runtime".into(),
                partition_count: 1,
                replication_factor: 1,
                failure_domain_policy: FailureDomainPolicy::Disabled,
            })
            .await
        {
            Ok(created) => return created,
            Err(astralog_controller::ControllerError::NotLeader { .. }) => {}
            Err(error) => panic!("P7-F create-topic failed: {error}"),
        }
        assert!(time::Instant::now() < deadline);
        time::sleep(Duration::from_millis(20)).await;
    }
}

async fn init_producer(
    nodes: &BTreeMap<ControllerNodeId, ControllerNodeHandle>,
    expected: Option<ProducerAuthority>,
) -> ProducerAuthority {
    let deadline = time::Instant::now() + Duration::from_secs(30);
    loop {
        let leader = current_leader(nodes).await;
        match nodes[&leader].init_producer(expected).await {
            Ok(authority) => return authority,
            Err(astralog_controller::ControllerError::NotLeader { .. }) => {}
            Err(error) => panic!("P7-F InitProducer failed: {error}"),
        }
        assert!(time::Instant::now() < deadline);
        time::sleep(Duration::from_millis(20)).await;
    }
}

fn record(value: &[u8]) -> DataRecord {
    DataRecord {
        timestamp_ms: 7,
        key: Some(b"k".to_vec()),
        value: Some(value.to_vec()),
        headers: Vec::new(),
    }
}

fn wire_record(value: &[u8]) -> WireRecordV0 {
    WireRecordV0 {
        timestamp_ms: 1_700_000_000_000,
        key: Some(b"k".to_vec()),
        value: Some(value.to_vec()),
        headers: Vec::new(),
    }
}

async fn read_wire_packet(stream: &mut TcpStream) -> io::Result<Vec<u8>> {
    let mut prefix = [0_u8; 4];
    stream.read_exact(&mut prefix).await?;
    let len = usize::try_from(u32::from_be_bytes(prefix))
        .map_err(|_| io::Error::new(io::ErrorKind::InvalidData, "wire length overflow"))?;
    if len > MAX_WIRE_FRAME_LEN_V1 {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "wire frame exceeds test proxy bound",
        ));
    }
    let mut bytes = vec![0_u8; len + 4];
    bytes[..4].copy_from_slice(&prefix);
    stream.read_exact(&mut bytes[4..]).await?;
    Ok(bytes)
}

async fn proxy_connection(
    mut downstream: TcpStream,
    upstream_addr: SocketAddr,
    drop_first_init_producer: Arc<AtomicBool>,
    drop_first_produce_v1: Arc<AtomicBool>,
    produce_bodies: Arc<Mutex<Vec<Vec<u8>>>>,
) -> io::Result<()> {
    let mut upstream = TcpStream::connect(upstream_addr).await?;
    let mut client_hello = [0_u8; 16];
    downstream.read_exact(&mut client_hello).await?;
    upstream.write_all(&client_hello).await?;
    let mut server_hello = [0_u8; 16];
    upstream.read_exact(&mut server_hello).await?;
    downstream.write_all(&server_hello).await?;

    loop {
        let request = match read_wire_packet(&mut downstream).await {
            Ok(request) => request,
            Err(error)
                if matches!(
                    error.kind(),
                    io::ErrorKind::UnexpectedEof
                        | io::ErrorKind::ConnectionReset
                        | io::ErrorKind::BrokenPipe
                ) =>
            {
                return Ok(());
            }
            Err(error) => return Err(error),
        };
        let request_frame = decode_frame(&request, MAX_WIRE_FRAME_LEN_V1)
            .map_err(|error| io::Error::new(io::ErrorKind::InvalidData, error.to_string()))?;
        upstream.write_all(&request).await?;
        let response = read_wire_packet(&mut upstream).await?;
        let is_init_producer =
            request_frame.api_key == ApiKey::InitProducer as u16 && request_frame.api_version == 0;
        if is_init_producer
            && drop_first_init_producer
                .compare_exchange(true, false, Ordering::SeqCst, Ordering::SeqCst)
                .is_ok()
        {
            continue;
        }
        let is_p7_produce =
            request_frame.api_key == ApiKey::Produce as u16 && request_frame.api_version == 1;
        if is_p7_produce {
            produce_bodies.lock().await.push(request_frame.body.clone());
            if drop_first_produce_v1
                .compare_exchange(true, false, Ordering::SeqCst, Ordering::SeqCst)
                .is_ok()
            {
                continue;
            }
        }
        downstream.write_all(&response).await?;
    }
}

async fn start_drop_first_produce_v1_proxy(
    proxy_addr: SocketAddr,
    upstream_addr: SocketAddr,
) -> (JoinHandle<()>, Arc<Mutex<Vec<Vec<u8>>>>) {
    let listener = TcpListener::bind(proxy_addr).await.unwrap();
    let drop_first_init = Arc::new(AtomicBool::new(true));
    let drop_first = Arc::new(AtomicBool::new(true));
    let bodies = Arc::new(Mutex::new(Vec::new()));
    let captured = bodies.clone();
    let task = tokio::spawn(async move {
        loop {
            let Ok((stream, _)) = listener.accept().await else {
                return;
            };
            let drop_first_init = drop_first_init.clone();
            let drop_first = drop_first.clone();
            let bodies = bodies.clone();
            tokio::spawn(async move {
                let _ =
                    proxy_connection(stream, upstream_addr, drop_first_init, drop_first, bodies)
                        .await;
            });
        }
    });
    (task, captured)
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn p7_controller_authority_binds_internal_idempotent_runtime() {
    let root = temp_dir("authority-runtime");
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
    mutate(
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
            max_feature: 3,
        },
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
        ControllerCommand::SetFeatureLevel {
            expected_feature_level: 1,
            new_feature_level: 3,
        },
    )
    .await;
    let clients = controllers
        .values()
        .map(ControllerNodeHandle::client)
        .collect::<Vec<_>>();
    let mut broker_config = BrokerConfig::new(root.join("broker-1"), broker_client_addr);
    broker_config.advertised_host = broker_client_addr.ip().to_string();
    broker_config.advertised_port = broker_client_addr.port();
    broker_config.broker_id = 1;
    let mut replication = DynamicReplicationConfig::new(broker_raft_addr);
    replication.tick_interval = Duration::from_millis(20);
    replication.election_timeout_min_ticks = 10;
    replication.election_timeout_max_ticks = 11;
    let mut dynamic = DynamicControllerConfig::new(clients, replication);
    dynamic.metadata_refresh_interval = Duration::from_millis(50);
    broker_config.dynamic_controller = Some(dynamic);

    let broker = {
        let deadline = time::Instant::now() + Duration::from_secs(30);
        loop {
            match start_broker(broker_config.clone()).await {
                Ok(broker) => break broker,
                Err(BrokerError::Controller(
                    astralog_controller::ControllerError::NotLeader { .. }
                    | astralog_controller::ControllerError::Busy(_),
                )) => {}
                Err(error) => panic!("P7-F broker startup failed: {error}"),
            }
            assert!(time::Instant::now() < deadline);
            time::sleep(Duration::from_millis(20)).await;
        }
    };

    let healthy_deadline = time::Instant::now() + Duration::from_secs(10);
    loop {
        // This integration only needs an initialized detector clock to prove
        // the broker actually heartbeats. Advancing by 1,000 logical ticks on
        // every 25-ms poll previously aged a healthy heartbeat beyond the
        // detector's 10-tick unavailable threshold immediately. Reinitialize
        // the fixed tick on a newly elected controller instead of simulating failure
        // detector time passage unrelated to real broker heartbeats.
        let leader = detector_tick(&controllers, 1_000).await;
        let observed = controllers[&leader].broker_liveness(1).await;
        if observed
            .as_ref()
            .is_ok_and(|observation| observation.state == BrokerLivenessState::Healthy)
        {
            break;
        }
        assert!(
            time::Instant::now() < healthy_deadline,
            "P7-F broker never became healthy at the controller: last observation={observed:?}"
        );
        time::sleep(Duration::from_millis(25)).await;
    }
    let created = create_topic(&controllers).await;
    assert_eq!(created.assignments[0].voters, vec![1]);

    let client = AstraClient::connect(ClientConfig::new(broker.local_addr().to_string()))
        .await
        .unwrap();
    let leader_epoch = {
        let deadline = time::Instant::now() + Duration::from_secs(10);
        loop {
            let metadata = client.refresh_metadata().await.unwrap();
            let topic = metadata
                .topics
                .iter()
                .find(|topic| topic.topic_id == created.topic.topic_id);
            if let Some(topic) = topic {
                let partition = &topic.partitions[0];
                if partition.leader_broker_id == 1 && partition.leader_epoch != UNKNOWN_EPOCH {
                    break partition.leader_epoch;
                }
            }
            assert!(
                time::Instant::now() < deadline,
                "P7-F RF1 leader did not activate"
            );
            time::sleep(Duration::from_millis(20)).await;
        }
    };

    let unknown = ProducerAuthority::new(
        ProducerId::new(99_999).unwrap(),
        ProducerEpoch::new(1).unwrap(),
    );
    let unknown_error = broker
        .produce_idempotent_internal(
            created.topic.topic_id,
            0,
            leader_epoch,
            unknown,
            ProducerSequence::ZERO,
            vec![record(b"unknown")],
        )
        .await
        .unwrap_err();
    assert_eq!(unknown_error.error, PublicError::UnknownProducer);

    let epoch_one = init_producer(&controllers, None).await;
    let first = broker
        .produce_idempotent_internal(
            created.topic.topic_id,
            0,
            leader_epoch,
            epoch_one,
            ProducerSequence::ZERO,
            vec![record(b"epoch1-seq0")],
        )
        .await
        .unwrap();
    assert_eq!(first.base_offset, 0);
    assert_eq!(first.last_offset, 0);

    let duplicate = broker
        .produce_idempotent_internal(
            created.topic.topic_id,
            0,
            leader_epoch,
            epoch_one,
            ProducerSequence::ZERO,
            vec![record(b"epoch1-seq0")],
        )
        .await
        .unwrap();
    assert_eq!(duplicate.base_offset, first.base_offset);
    assert_eq!(duplicate.last_offset, first.last_offset);
    assert_eq!(duplicate.raft_index, first.raft_index);

    let changed_retry = broker
        .produce_idempotent_internal(
            created.topic.topic_id,
            0,
            leader_epoch,
            epoch_one,
            ProducerSequence::ZERO,
            vec![record(b"changed")],
        )
        .await
        .unwrap_err();
    assert_eq!(changed_retry.error, PublicError::OutOfOrderSequence);

    let second = broker
        .produce_idempotent_internal(
            created.topic.topic_id,
            0,
            leader_epoch,
            epoch_one,
            ProducerSequence::new(1),
            vec![record(b"epoch1-seq1")],
        )
        .await
        .unwrap();
    assert_eq!(second.base_offset, 1);

    let unissued_higher = ProducerAuthority::new(
        epoch_one.producer_id,
        ProducerEpoch::new(epoch_one.producer_epoch.get() + 2).unwrap(),
    );
    let unissued_error = broker
        .produce_idempotent_internal(
            created.topic.topic_id,
            0,
            leader_epoch,
            unissued_higher,
            ProducerSequence::ZERO,
            vec![record(b"unissued")],
        )
        .await
        .unwrap_err();
    assert_eq!(unissued_error.error, PublicError::ProducerFenced);

    let epoch_two = init_producer(&controllers, Some(epoch_one)).await;
    let reset = broker
        .produce_idempotent_internal(
            created.topic.topic_id,
            0,
            leader_epoch,
            epoch_two,
            ProducerSequence::ZERO,
            vec![record(b"epoch2-seq0")],
        )
        .await
        .unwrap();
    assert_eq!(reset.base_offset, 2);

    let stale_error = broker
        .produce_idempotent_internal(
            created.topic.topic_id,
            0,
            leader_epoch,
            epoch_one,
            ProducerSequence::new(2),
            vec![record(b"stale")],
        )
        .await
        .unwrap_err();
    assert_eq!(stale_error.error, PublicError::ProducerFenced);

    for (_, controller) in controllers {
        controller.shutdown().await;
    }
    let outage_continuation = broker
        .produce_idempotent_internal(
            created.topic.topic_id,
            0,
            leader_epoch,
            epoch_two,
            ProducerSequence::new(1),
            vec![record(b"epoch2-seq1-controller-outage")],
        )
        .await
        .unwrap();
    assert_eq!(outage_continuation.base_offset, 3);

    broker.shutdown().await.unwrap();
    fs::remove_dir_all(root).unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn p7_public_rf1_init_produce_restart_and_lost_response_retry_commit_once() {
    let root = temp_dir("public-rf1-lost-response");
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

    let broker_bind_addr = reserve_addr();
    let proxy_addr = reserve_addr();
    let broker_raft_addr = reserve_addr();
    mutate(
        &controllers,
        ControllerCommand::RegisterBroker {
            broker_id: 1,
            node_uuid: [1; 16],
            initial_incarnation: 1,
            client_host: proxy_addr.ip().to_string(),
            client_port: proxy_addr.port(),
            internal_host: broker_raft_addr.ip().to_string(),
            internal_port: broker_raft_addr.port(),
            rack: Some("rack-1".into()),
            zone: Some("zone-1".into()),
            min_feature: 1,
            max_feature: 3,
        },
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
        ControllerCommand::SetFeatureLevel {
            expected_feature_level: 1,
            new_feature_level: 3,
        },
    )
    .await;

    let clients = controllers
        .values()
        .map(ControllerNodeHandle::client)
        .collect::<Vec<_>>();
    let mut broker_config = BrokerConfig::new(root.join("broker-public"), broker_bind_addr);
    broker_config.advertised_host = proxy_addr.ip().to_string();
    broker_config.advertised_port = proxy_addr.port();
    broker_config.broker_id = 1;
    let mut replication = DynamicReplicationConfig::new(broker_raft_addr);
    replication.tick_interval = Duration::from_millis(20);
    replication.election_timeout_min_ticks = 10;
    replication.election_timeout_max_ticks = 11;
    let mut dynamic = DynamicControllerConfig::new(clients, replication);
    dynamic.metadata_refresh_interval = Duration::from_millis(50);
    broker_config.dynamic_controller = Some(dynamic);

    let broker = {
        let deadline = time::Instant::now() + Duration::from_secs(30);
        loop {
            match start_broker(broker_config.clone()).await {
                Ok(broker) => break broker,
                Err(BrokerError::Controller(
                    astralog_controller::ControllerError::NotLeader { .. }
                    | astralog_controller::ControllerError::Busy(_),
                )) => {}
                Err(error) => panic!("P7-H broker startup failed: {error}"),
            }
            assert!(time::Instant::now() < deadline);
            time::sleep(Duration::from_millis(20)).await;
        }
    };

    let healthy_deadline = time::Instant::now() + Duration::from_secs(10);
    loop {
        // P7-H readiness must observe a real heartbeat at a fixed logical
        // time. Advancing 1,000 ticks per poll made every prior heartbeat
        // immediately unavailable, even if the broker was functioning.
        let leader = detector_tick(&controllers, 1_000).await;
        let observed = controllers[&leader].broker_liveness(1).await;
        if observed
            .as_ref()
            .is_ok_and(|observation| observation.state == BrokerLivenessState::Healthy)
        {
            break;
        }
        assert!(
            time::Instant::now() < healthy_deadline,
            "P7-H broker never became healthy at the controller: last observation={observed:?}"
        );
        time::sleep(Duration::from_millis(25)).await;
    }
    let created = create_topic(&controllers).await;
    assert_eq!(created.assignments[0].voters, vec![1]);

    let (proxy_task, captured_bodies) =
        start_drop_first_produce_v1_proxy(proxy_addr, broker.local_addr()).await;
    let mut client_config = ClientConfig::new(proxy_addr.to_string());
    client_config.request_timeout = Duration::from_millis(750);
    client_config.max_producer_retries = 2;
    let client = AstraClient::connect(client_config).await.unwrap();

    let ambiguous = client.init_producer().await.unwrap_err();
    assert!(matches!(
        ambiguous,
        astralog_client::ClientError::AmbiguousMutation {
            api: ApiKey::InitProducer
        }
    ));
    assert_eq!(client.producer_authority().await, None);
    let authority_one = client.recover_producer_session().await.unwrap();
    assert_eq!(authority_one.producer_id.get(), 1_001);
    assert_eq!(authority_one.producer_epoch.get(), 1);
    let metadata_deadline = time::Instant::now() + Duration::from_secs(10);
    loop {
        let metadata = client.refresh_metadata().await.unwrap();
        let ready = metadata
            .topics
            .iter()
            .find(|topic| topic.topic_id == created.topic.topic_id)
            .and_then(|topic| topic.partitions.first())
            .is_some_and(|partition| {
                partition.leader_broker_id == 1 && partition.leader_epoch != UNKNOWN_EPOCH
            });
        if ready {
            break;
        }
        assert!(time::Instant::now() < metadata_deadline);
        time::sleep(Duration::from_millis(20)).await;
    }

    let first = client
        .produce_idempotent_partition(
            created.topic.topic_id,
            0,
            vec![wire_record(b"public-lost-response")],
        )
        .await
        .unwrap();
    assert_eq!((first.base_offset, first.last_offset), (0, 0));
    let bodies = captured_bodies.lock().await.clone();
    assert!(bodies.len() >= 2, "client did not issue the exact retry");
    assert_eq!(
        bodies[0], bodies[1],
        "automatic retry regenerated Produce V1 request bytes"
    );

    let fetched = client
        .fetch(FetchRequestV0 {
            isolation: IsolationLevel::ReadCommitted,
            min_bytes: 0,
            max_wait_ms: 0,
            partitions: vec![FetchPartitionRequestV0 {
                topic_id: created.topic.topic_id,
                partition: 0,
                leader_epoch: UNKNOWN_EPOCH,
                fetch_offset: 0,
                max_bytes: 1024 * 1024,
                max_records: 10,
            }],
            tags: Vec::new(),
        })
        .await
        .unwrap();
    assert_eq!(fetched.partitions[0].records.len(), 1);
    assert_eq!(fetched.partitions[0].high_watermark, 1);

    let authority_two = client.restart_producer().await.unwrap();
    assert_eq!(authority_two.producer_id, authority_one.producer_id);
    assert_eq!(authority_two.producer_epoch.get(), 2);
    let second = client
        .produce_idempotent_partition(
            created.topic.topic_id,
            0,
            vec![wire_record(b"public-epoch-two-seq-zero")],
        )
        .await
        .unwrap();
    assert_eq!((second.base_offset, second.last_offset), (1, 1));

    client.close().await;
    proxy_task.abort();
    broker.shutdown().await.unwrap();
    for (_, controller) in controllers {
        controller.shutdown().await;
    }
    fs::remove_dir_all(root).unwrap();
}
