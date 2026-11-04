use std::{
    collections::BTreeMap,
    fs, io,
    net::{Ipv4Addr, SocketAddr},
    path::{Path, PathBuf},
    sync::{
        Arc, OnceLock,
        atomic::{AtomicBool, AtomicU32, Ordering},
    },
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use astralog_broker::{
    BrokerConfig, BrokerError, BrokerHandle, DynamicControllerConfig, DynamicReplicationConfig,
    P7InternalProduceOutcome, PreferredLeaderRequest, start as start_broker,
};
use astralog_client::{AstraClient, ClientConfig, ClientError};
use astralog_common::{ProducerAuthority, ProducerId, ProducerSequence};
use astralog_controller::{
    BrokerLifecycle, BrokerLivenessState, ControllerCommand, ControllerNodeConfig,
    ControllerNodeHandle, ControllerNodeId, ControllerPeerDescriptor,
    ControllerPeerTransportConfig, ControllerPersistenceConfig, ControllerRuntimeConfig,
    ControllerRuntimeLimits, ControllerState, CreateTopicSpec, FailureDetectorConfig,
    FailureDomainPolicy, FeatureRange, HeartbeatLimits, start_controller_node,
};
use astralog_protocol::{
    ApiKey, MAX_WIRE_FRAME_LEN_V1, PublicError, UNKNOWN_BROKER_ID, UNKNOWN_EPOCH,
    api::WireRecordV0,
    api::{FetchPartitionRequestV0, FetchRequestV0, IsolationLevel},
    decode_frame,
};
use astralog_raft::{NodeId, Role, StableConfiguration, TimingConfig};
use astralog_storage::DataRecord;
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{TcpListener, TcpStream},
    sync::{Mutex, Notify},
    task::JoinHandle,
    time,
};

fn p7i_integration_lock() -> &'static Mutex<()> {
    static LOCK: OnceLock<Mutex<()>> = OnceLock::new();
    LOCK.get_or_init(|| Mutex::new(()))
}

fn temp_dir(label: &str) -> PathBuf {
    let nonce = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    let path = std::env::temp_dir().join(format!(
        "astralog-p7i-e2e-{label}-{}-{nonce}",
        std::process::id()
    ));
    fs::create_dir_all(&path).unwrap();
    path
}

fn count_producer_snapshots(root: &Path) -> usize {
    let mut count = 0_usize;
    for entry in fs::read_dir(root).unwrap() {
        let entry = entry.unwrap();
        let path = entry.path();
        let file_type = entry.file_type().unwrap();
        if file_type.is_dir() {
            count = count.saturating_add(count_producer_snapshots(&path));
        } else if path
            .file_name()
            .is_some_and(|name| name == std::ffi::OsStr::new("producer.snapshot"))
        {
            count = count.saturating_add(1);
        }
    }
    count
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

async fn current_controller_leader(
    nodes: &BTreeMap<ControllerNodeId, ControllerNodeHandle>,
) -> ControllerNodeId {
    let deadline = time::Instant::now() + Duration::from_secs(60);
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
            "P7-I controller leader election timed out"
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
        let leader = current_controller_leader(nodes).await;
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
            "P7-I controller leadership did not stabilize"
        );
        time::sleep(Duration::from_millis(20)).await;
    }
}

async fn mutate(
    nodes: &BTreeMap<ControllerNodeId, ControllerNodeHandle>,
    command: ControllerCommand,
) -> astralog_controller::ApplyOutcome {
    let deadline = time::Instant::now() + Duration::from_secs(60);
    loop {
        let leader = stable_controller_leader(nodes).await;
        match nodes[&leader].mutate(command.clone()).await {
            Ok(outcome) => return outcome,
            Err(
                astralog_controller::ControllerError::NotLeader { .. }
                | astralog_controller::ControllerError::Busy(_),
            ) => {}
            Err(error) => panic!("P7-I controller mutation failed: {error}"),
        }
        assert!(time::Instant::now() < deadline);
        time::sleep(Duration::from_millis(20)).await;
    }
}

async fn create_rf3_topic(
    nodes: &BTreeMap<ControllerNodeId, ControllerNodeHandle>,
) -> astralog_controller::TopicCreateResult {
    let deadline = time::Instant::now() + Duration::from_secs(60);
    let spec = CreateTopicSpec {
        topic_name: "p7i-rf3".into(),
        partition_count: 1,
        replication_factor: 3,
        failure_domain_policy: FailureDomainPolicy::Disabled,
    };
    loop {
        let leader = stable_controller_leader(nodes).await;
        match nodes[&leader].create_topic(spec.clone()).await {
            Ok(created) => return created,
            Err(
                astralog_controller::ControllerError::NotLeader { .. }
                | astralog_controller::ControllerError::Busy(_),
            ) => {}
            Err(error) => panic!("P7-I create-topic failed: {error}"),
        }
        assert!(time::Instant::now() < deadline);
        time::sleep(Duration::from_millis(20)).await;
    }
}

async fn init_producer_controller(
    nodes: &BTreeMap<ControllerNodeId, ControllerNodeHandle>,
    expected: Option<ProducerAuthority>,
) -> ProducerAuthority {
    let deadline = time::Instant::now() + Duration::from_secs(60);
    loop {
        let leader = stable_controller_leader(nodes).await;
        match nodes[&leader].init_producer(expected).await {
            Ok(authority) => return authority,
            Err(
                astralog_controller::ControllerError::NotLeader { .. }
                | astralog_controller::ControllerError::Busy(_),
            ) => {}
            Err(error) => panic!("P7-I InitProducer controller mutation failed: {error}"),
        }
        assert!(time::Instant::now() < deadline);
        time::sleep(Duration::from_millis(20)).await;
    }
}

async fn start_broker_when_controller_ready(
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
            Err(error) => panic!("P7-I broker startup failed: {error}"),
        }
        assert!(time::Instant::now() < deadline);
        time::sleep(Duration::from_millis(20)).await;
    }
}

async fn failure_detector_tick(
    nodes: &BTreeMap<ControllerNodeId, ControllerNodeHandle>,
    tick: u64,
) -> ControllerNodeId {
    let deadline = time::Instant::now() + Duration::from_secs(60);
    loop {
        let leader = current_controller_leader(nodes).await;
        match nodes[&leader].failure_detector_tick(tick).await {
            Ok(()) => return leader,
            Err(
                astralog_controller::ControllerError::NotLeader { .. }
                | astralog_controller::ControllerError::Busy(_),
            ) => {}
            Err(error) => panic!("P7-I failure-detector tick failed: {error}"),
        }
        assert!(time::Instant::now() < deadline);
        time::sleep(Duration::from_millis(20)).await;
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
            "wire frame exceeds P7-I proxy bound",
        ));
    }
    let mut bytes = vec![0_u8; len + 4];
    bytes[..4].copy_from_slice(&prefix);
    stream.read_exact(&mut bytes[4..]).await?;
    Ok(bytes)
}

#[derive(Default)]
struct ProxyControl {
    drop_next_init_producer_response: AtomicBool,
    init_producer_response_seen: Notify,
    drop_next_produce_response: AtomicBool,
    hold_next_produce_request: AtomicBool,
    produce_request_held: Notify,
    release_held_produce_request: Notify,
    produce_response_seen: Notify,
    produce_bodies: Mutex<Vec<Vec<u8>>>,
}

async fn proxy_connection(
    mut downstream: TcpStream,
    upstream_addr: SocketAddr,
    control: Arc<ProxyControl>,
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
        let is_p7_produce =
            request_frame.api_key == ApiKey::Produce as u16 && request_frame.api_version == 1;
        if is_p7_produce
            && control
                .hold_next_produce_request
                .compare_exchange(true, false, Ordering::SeqCst, Ordering::SeqCst)
                .is_ok()
        {
            control.produce_request_held.notify_waiters();
            control.release_held_produce_request.notified().await;
        }
        upstream.write_all(&request).await?;
        let response = read_wire_packet(&mut upstream).await?;
        let is_init_producer =
            request_frame.api_key == ApiKey::InitProducer as u16 && request_frame.api_version == 0;
        if is_init_producer {
            control.init_producer_response_seen.notify_waiters();
            if control
                .drop_next_init_producer_response
                .compare_exchange(true, false, Ordering::SeqCst, Ordering::SeqCst)
                .is_ok()
            {
                continue;
            }
        }
        if is_p7_produce {
            control
                .produce_bodies
                .lock()
                .await
                .push(request_frame.body.clone());
            control.produce_response_seen.notify_waiters();
            if control
                .drop_next_produce_response
                .compare_exchange(true, false, Ordering::SeqCst, Ordering::SeqCst)
                .is_ok()
            {
                continue;
            }
        }
        downstream.write_all(&response).await?;
    }
}

async fn start_proxy(
    proxy_addr: SocketAddr,
    upstream_addr: SocketAddr,
    control: Arc<ProxyControl>,
) -> JoinHandle<()> {
    let listener = TcpListener::bind(proxy_addr).await.unwrap();
    tokio::spawn(async move {
        loop {
            let Ok((stream, _)) = listener.accept().await else {
                return;
            };
            let control = control.clone();
            tokio::spawn(async move {
                let _ = proxy_connection(stream, upstream_addr, control).await;
            });
        }
    })
}

struct Rf3Harness {
    root: PathBuf,
    controllers: BTreeMap<ControllerNodeId, ControllerNodeHandle>,
    brokers: Vec<Option<BrokerHandle>>,
    broker_configs: Vec<BrokerConfig>,
    proxy_tasks: Vec<JoinHandle<()>>,
    proxy_controls: Vec<Arc<ProxyControl>>,
    proxy_addresses: Vec<SocketAddr>,
    topic_id: astralog_common::TopicId,
}

impl Rf3Harness {
    async fn start(label: &str) -> Self {
        let root = temp_dir(label);
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

        let broker_bind_addresses = (0..3).map(|_| reserve_addr()).collect::<Vec<_>>();
        let proxy_addresses = (0..3).map(|_| reserve_addr()).collect::<Vec<_>>();
        let raft_addresses = (0..3).map(|_| reserve_addr()).collect::<Vec<_>>();
        for broker_id in 1..=3_u32 {
            let index = usize::try_from(broker_id - 1).unwrap();
            mutate(
                &controllers,
                ControllerCommand::RegisterBroker {
                    broker_id,
                    node_uuid: [u8::try_from(broker_id).unwrap(); 16],
                    initial_incarnation: 1,
                    client_host: proxy_addresses[index].ip().to_string(),
                    client_port: proxy_addresses[index].port(),
                    internal_host: raft_addresses[index].ip().to_string(),
                    internal_port: raft_addresses[index].port(),
                    rack: Some(format!("rack-{broker_id}")),
                    zone: Some(format!("zone-{broker_id}")),
                    min_feature: 1,
                    max_feature: 3,
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
        mutate(
            &controllers,
            ControllerCommand::SetFeatureLevel {
                expected_feature_level: 1,
                new_feature_level: 3,
            },
        )
        .await;

        let controller_clients = controllers
            .values()
            .map(ControllerNodeHandle::client)
            .collect::<Vec<_>>();
        let mut brokers = Vec::new();
        let mut broker_configs = Vec::new();
        let mut proxy_tasks = Vec::new();
        let mut proxy_controls = Vec::new();
        for broker_id in 1..=3_u32 {
            let index = usize::try_from(broker_id - 1).unwrap();
            let mut config = BrokerConfig::new(
                root.join(format!("broker-{broker_id}")),
                broker_bind_addresses[index],
            );
            config.advertised_host = proxy_addresses[index].ip().to_string();
            config.advertised_port = proxy_addresses[index].port();
            config.broker_id = broker_id;
            let mut replication = DynamicReplicationConfig::new(raft_addresses[index]);
            replication.tick_interval = Duration::from_millis(20);
            let election_min = 20 + u64::from(broker_id - 1) * 5;
            replication.election_timeout_min_ticks = election_min;
            replication.election_timeout_max_ticks = election_min + 1;
            let mut dynamic = DynamicControllerConfig::new(controller_clients.clone(), replication);
            dynamic.metadata_refresh_interval = Duration::from_millis(50);
            dynamic.preferred_leader_timeout = Duration::from_secs(10);
            dynamic.max_pending_admin_operations = 32;
            config.dynamic_controller = Some(dynamic);
            let broker = start_broker_when_controller_ready(config.clone(), &controllers).await;
            let control = Arc::new(ProxyControl::default());
            let proxy_task =
                start_proxy(proxy_addresses[index], broker.local_addr(), control.clone()).await;
            brokers.push(Some(broker));
            broker_configs.push(config);
            proxy_tasks.push(proxy_task);
            proxy_controls.push(control);
        }

        let healthy_deadline = time::Instant::now() + Duration::from_secs(60);
        loop {
            // Readiness is not a failure-detector expiry test. Keep its logical
            // clock fixed while real broker heartbeats arrive. Advancing it by
            // 1,000 per poll made healthy brokers instantly unavailable (the
            // configured suspect/unavailable thresholds are only 3/10 ticks).
            let leader = failure_detector_tick(&controllers, 1_000).await;
            let mut all_healthy = true;
            let mut observations = Vec::new();
            for broker_id in 1..=3_u32 {
                let observation = controllers[&leader].broker_liveness(broker_id).await;
                all_healthy &= observation
                    .as_ref()
                    .is_ok_and(|state| state.state == BrokerLivenessState::Healthy);
                observations.push((broker_id, observation));
            }
            if all_healthy {
                break;
            }
            assert!(
                time::Instant::now() < healthy_deadline,
                "P7-I brokers never became healthy; controller={leader:?}; liveness={observations:?}"
            );
            time::sleep(Duration::from_millis(30)).await;
        }

        let created = create_rf3_topic(&controllers).await;
        assert_eq!(created.assignments[0].voters, vec![1, 2, 3]);
        Self {
            root,
            controllers,
            brokers,
            broker_configs,
            proxy_tasks,
            proxy_controls,
            proxy_addresses,
            topic_id: created.topic.topic_id,
        }
    }

    async fn client_on(&self, broker_id: u32, max_producer_retries: usize) -> AstraClient {
        let index = usize::try_from(broker_id - 1).unwrap();
        let mut config = ClientConfig::new(self.proxy_addresses[index].to_string());
        config.request_timeout = Duration::from_millis(750);
        config.max_producer_retries = max_producer_retries;
        let client = AstraClient::connect(config).await.unwrap();
        let deadline = time::Instant::now() + Duration::from_secs(20);
        loop {
            let metadata = client.refresh_metadata().await.unwrap();
            let ready = metadata
                .topics
                .iter()
                .find(|topic| topic.topic_id == self.topic_id)
                .and_then(|topic| topic.partitions.first())
                .is_some_and(|partition| {
                    partition.leader_broker_id != UNKNOWN_BROKER_ID
                        && partition.leader_epoch != UNKNOWN_EPOCH
                });
            if ready {
                return client;
            }
            assert!(
                time::Instant::now() < deadline,
                "P7-I RF3 partition leader did not become visible"
            );
            time::sleep(Duration::from_millis(20)).await;
        }
    }

    async fn client(&self, max_producer_retries: usize) -> AstraClient {
        self.client_on(1, max_producer_retries).await
    }

    async fn route(&self, client: &AstraClient) -> (u32, u64) {
        let deadline = time::Instant::now() + Duration::from_secs(20);
        loop {
            let metadata = client.refresh_metadata().await.unwrap();
            if let Some(partition) = metadata
                .topics
                .iter()
                .find(|topic| topic.topic_id == self.topic_id)
                .and_then(|topic| topic.partitions.first())
                && partition.leader_broker_id != UNKNOWN_BROKER_ID
                && partition.leader_epoch != UNKNOWN_EPOCH
            {
                return (partition.leader_broker_id, partition.leader_epoch);
            }
            assert!(time::Instant::now() < deadline);
            time::sleep(Duration::from_millis(20)).await;
        }
    }

    async fn stop_broker(&mut self, broker_id: u32) {
        let index = usize::try_from(broker_id - 1).unwrap();
        if let Some(broker) = self.brokers[index].take() {
            broker.shutdown().await.unwrap();
        }
    }

    async fn restart_broker(&mut self, broker_id: u32) {
        let index = usize::try_from(broker_id - 1).unwrap();
        assert!(
            self.brokers[index].is_none(),
            "P7-I broker is already running"
        );
        let broker = start_broker_when_controller_ready(
            self.broker_configs[index].clone(),
            &self.controllers,
        )
        .await;
        self.brokers[index] = Some(broker);
    }

    async fn wait_partition_leader_excluding(&self, excluded: Option<u32>) -> (u32, u64) {
        let deadline = time::Instant::now() + Duration::from_secs(30);
        loop {
            for broker_id in 1..=3_u32 {
                if excluded == Some(broker_id) {
                    continue;
                }
                let index = usize::try_from(broker_id - 1).unwrap();
                let Some(broker) = self.brokers[index].as_ref() else {
                    continue;
                };
                if let Ok(observation) = broker.observe_partition_internal(self.topic_id, 0).await
                    && observation.local_is_leader
                    && observation.leader_broker_id == broker_id
                    && observation.leader_epoch != UNKNOWN_EPOCH
                {
                    return (broker_id, observation.leader_epoch);
                }
            }
            assert!(
                time::Instant::now() < deadline,
                "P7-I RF3 successor leader did not emerge"
            );
            time::sleep(Duration::from_millis(20)).await;
        }
    }

    async fn produce_idempotent_internal_when_ready(
        &self,
        authority: ProducerAuthority,
        sequence: ProducerSequence,
        payload: &[u8],
    ) -> (u32, u64, P7InternalProduceOutcome) {
        let deadline = time::Instant::now() + Duration::from_secs(30);
        loop {
            let (leader_id, leader_epoch) = self.wait_partition_leader_excluding(None).await;
            let leader_index = usize::try_from(leader_id - 1).unwrap();
            let result = self.brokers[leader_index]
                .as_ref()
                .unwrap()
                .produce_idempotent_internal(
                    self.topic_id,
                    0,
                    leader_epoch,
                    authority,
                    sequence,
                    vec![data_record(payload)],
                )
                .await;
            match result {
                Ok(outcome) => return (leader_id, leader_epoch, outcome),
                Err(error)
                    if matches!(
                        error.error,
                        PublicError::BrokerBusy | PublicError::NotLeader | PublicError::StaleEpoch
                    ) => {}
                Err(error) => panic!("P7-J idempotent retry failed terminally: {error:?}"),
            }
            assert!(
                time::Instant::now() < deadline,
                "P7-J successor never became safe for idempotent admission"
            );
            time::sleep(Duration::from_millis(20)).await;
        }
    }

    async fn terminal_idempotent_error_when_ready(
        &self,
        authority: ProducerAuthority,
        sequence: ProducerSequence,
        payload: &[u8],
    ) -> (u32, u64, PublicError) {
        let deadline = time::Instant::now() + Duration::from_secs(30);
        loop {
            let (leader_id, leader_epoch) = self.wait_partition_leader_excluding(None).await;
            let leader_index = usize::try_from(leader_id - 1).unwrap();
            let result = self.brokers[leader_index]
                .as_ref()
                .unwrap()
                .produce_idempotent_internal(
                    self.topic_id,
                    0,
                    leader_epoch,
                    authority,
                    sequence,
                    vec![data_record(payload)],
                )
                .await;
            match result {
                Err(error)
                    if matches!(
                        error.error,
                        PublicError::BrokerBusy | PublicError::NotLeader | PublicError::StaleEpoch
                    ) => {}
                Err(error) => return (leader_id, leader_epoch, error.error),
                Ok(outcome) => panic!(
                    "P7-J expected terminal producer error, got success at offsets ({}, {})",
                    outcome.base_offset, outcome.last_offset
                ),
            }
            assert!(
                time::Instant::now() < deadline,
                "P7-J successor never became ready to return terminal producer error"
            );
            time::sleep(Duration::from_millis(20)).await;
        }
    }

    async fn wait_replica_committed_at_least(&self, broker_id: u32, raft_index: u64) {
        let deadline = time::Instant::now() + Duration::from_secs(30);
        loop {
            let index = usize::try_from(broker_id - 1).unwrap();
            if let Some(broker) = self.brokers[index].as_ref()
                && let Ok(observation) = broker.observe_partition_internal(self.topic_id, 0).await
                && observation.committed_raft_index >= raft_index
            {
                return;
            }
            assert!(
                time::Instant::now() < deadline,
                "P7-I restarted replica did not catch up to committed retry"
            );
            time::sleep(Duration::from_millis(20)).await;
        }
    }

    async fn shutdown_controllers(&mut self) {
        let controllers = std::mem::take(&mut self.controllers);
        for (_, controller) in controllers {
            controller.shutdown().await;
        }
    }

    async fn shutdown(mut self) {
        for broker in self.brokers.drain(..).flatten() {
            broker.shutdown().await.unwrap();
        }
        for task in self.proxy_tasks {
            task.abort();
        }
        for (_, controller) in self.controllers {
            controller.shutdown().await;
        }
        fs::remove_dir_all(self.root).unwrap();
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

fn data_record(value: &[u8]) -> DataRecord {
    DataRecord {
        timestamp_ms: 1_700_000_000_000,
        key: Some(b"k".to_vec()),
        value: Some(value.to_vec()),
        headers: Vec::new(),
    }
}

#[derive(Debug, Clone, Copy)]
struct P7jSeededRng(u64);

impl P7jSeededRng {
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

async fn assert_committed_record_count(
    client: &AstraClient,
    topic_id: astralog_common::TopicId,
    expected: usize,
) {
    let deadline = time::Instant::now() + Duration::from_secs(20);
    loop {
        match client
            .fetch(FetchRequestV0 {
                isolation: IsolationLevel::ReadCommitted,
                min_bytes: 0,
                max_wait_ms: 0,
                partitions: vec![FetchPartitionRequestV0 {
                    topic_id,
                    partition: 0,
                    leader_epoch: UNKNOWN_EPOCH,
                    fetch_offset: 0,
                    max_bytes: 1024 * 1024,
                    max_records: 100,
                }],
                tags: Vec::new(),
            })
            .await
        {
            Ok(response)
                if response.partitions.first().is_some_and(|partition| {
                    partition.error_code == 0
                        && partition.records.len() == expected
                        && partition
                            .records
                            .iter()
                            .enumerate()
                            .all(|(index, record)| record.stream_offset == index as u64)
                }) =>
            {
                return;
            }
            Ok(_) | Err(_) => {
                let _ = client.refresh_metadata().await;
            }
        }
        assert!(
            time::Instant::now() < deadline,
            "P7-I committed record count did not converge to {expected}"
        );
        time::sleep(Duration::from_millis(20)).await;
    }
}

/// Content oracle for the main lost-response / failover scenarios. Count and
/// offsets alone would miss a substituted payload or an extra duplicate.
async fn assert_committed_values(
    client: &AstraClient,
    topic_id: astralog_common::TopicId,
    expected: &[&[u8]],
) {
    assert_committed_record_count(client, topic_id, expected.len()).await;
    let response = client
        .fetch(FetchRequestV0 {
            isolation: IsolationLevel::ReadCommitted,
            min_bytes: 0,
            max_wait_ms: 0,
            partitions: vec![FetchPartitionRequestV0 {
                topic_id,
                partition: 0,
                leader_epoch: UNKNOWN_EPOCH,
                fetch_offset: 0,
                max_bytes: 1024 * 1024,
                max_records: 100,
            }],
            tags: Vec::new(),
        })
        .await
        .expect("content oracle fetch");
    let partition = response
        .partitions
        .first()
        .expect("content oracle partition");
    assert_eq!(partition.error_code, 0);
    assert_eq!(partition.records.len(), expected.len());
    for (index, (record, value)) in partition.records.iter().zip(expected).enumerate() {
        assert_eq!(record.stream_offset, index as u64);
        assert_eq!(
            record.record.value.as_deref(),
            Some(*value),
            "wrong committed payload at offset {index}"
        );
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 10)]
async fn p7_rf3_lost_response_after_quorum_retries_one_committed_copy() {
    let _guard = p7i_integration_lock().lock().await;
    let harness = Rf3Harness::start("lost-response-rf3").await;
    let client = harness.client(2).await;
    client.init_producer().await.unwrap();
    let (leader_id, _) = harness.route(&client).await;
    let leader_index = usize::try_from(leader_id - 1).unwrap();
    harness.proxy_controls[leader_index]
        .drop_next_produce_response
        .store(true, Ordering::SeqCst);

    let outcome = client
        .produce_idempotent_partition(harness.topic_id, 0, vec![wire_record(b"rf3-lost-success")])
        .await
        .unwrap();
    assert_eq!((outcome.base_offset, outcome.last_offset), (0, 0));
    let bodies = harness.proxy_controls[leader_index]
        .produce_bodies
        .lock()
        .await
        .clone();
    assert!(bodies.len() >= 2, "P7-I client did not retry lost success");
    assert_eq!(
        bodies[0], bodies[1],
        "same-term lost-success retry must preserve exact Produce V1 body"
    );
    assert_committed_values(&client, harness.topic_id, &[b"rf3-lost-success"]).await;

    client.close().await;
    harness.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 10)]
async fn p7_rf3_leader_death_after_quorum_before_response_deduplicates_on_successor() {
    let _guard = p7i_integration_lock().lock().await;
    let mut harness = Rf3Harness::start("leader-death-after-quorum").await;
    let observer = harness.client(4).await;
    let (leader_id, _) = harness.route(&observer).await;
    observer.close().await;
    let bootstrap_id = [1_u32, 2, 3]
        .into_iter()
        .find(|candidate| *candidate != leader_id)
        .unwrap();
    let client = harness.client_on(bootstrap_id, 6).await;
    client.init_producer().await.unwrap();
    let (confirmed_leader, _) = harness.route(&client).await;
    assert_eq!(confirmed_leader, leader_id);
    let leader_index = usize::try_from(leader_id - 1).unwrap();
    let control = harness.proxy_controls[leader_index].clone();
    control
        .drop_next_produce_response
        .store(true, Ordering::SeqCst);
    let response_seen = control.produce_response_seen.notified();
    let producer = client.clone();
    let topic_id = harness.topic_id;
    let produce_task = tokio::spawn(async move {
        producer
            .produce_idempotent_partition(
                topic_id,
                0,
                vec![wire_record(b"commit-then-leader-death")],
            )
            .await
    });
    time::timeout(Duration::from_secs(20), response_seen)
        .await
        .expect("P7-I proxy never observed committed Produce response");

    harness.brokers[leader_index]
        .take()
        .unwrap()
        .shutdown()
        .await
        .unwrap();
    let outcome = time::timeout(Duration::from_secs(30), produce_task)
        .await
        .expect("P7-I client did not recover after committed leader death")
        .unwrap()
        .unwrap();
    assert_eq!((outcome.base_offset, outcome.last_offset), (0, 0));
    assert_committed_values(&client, harness.topic_id, &[b"commit-then-leader-death"]).await;

    client.close().await;
    harness.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 10)]
async fn p7_rf3_local_persist_without_quorum_is_repaired_and_retry_commits_once() {
    let _guard = p7i_integration_lock().lock().await;
    let mut harness = Rf3Harness::start("local-persist-before-quorum").await;
    let bootstrap = harness.client(0).await;
    let authority = bootstrap.init_producer().await.unwrap();
    let (leader_id, leader_epoch) = harness.route(&bootstrap).await;
    bootstrap.close().await;

    let followers = [1_u32, 2, 3]
        .into_iter()
        .filter(|broker_id| *broker_id != leader_id)
        .collect::<Vec<_>>();
    for broker_id in &followers {
        harness.stop_broker(*broker_id).await;
    }

    let leader_index = usize::try_from(leader_id - 1).unwrap();
    let uncommitted_raft_index = {
        let leader = harness.brokers[leader_index].as_ref().unwrap();
        let baseline = leader
            .observe_partition_internal(harness.topic_id, 0)
            .await
            .unwrap();
        assert!(baseline.local_is_leader);
        assert_eq!(baseline.leader_epoch, leader_epoch);

        let produce = leader.produce_idempotent_internal(
            harness.topic_id,
            0,
            leader_epoch,
            authority,
            ProducerSequence::ZERO,
            vec![data_record(b"durable-without-quorum")],
        );
        tokio::pin!(produce);
        let deadline = time::Instant::now() + Duration::from_secs(20);
        loop {
            tokio::select! {
                result = &mut produce => {
                    panic!("P7-I no-quorum proposal completed unexpectedly: {result:?}");
                }
                _ = time::sleep(Duration::from_millis(20)) => {}
            }
            let observation = leader
                .observe_partition_internal(harness.topic_id, 0)
                .await
                .unwrap();
            if observation.durable_raft_index > baseline.durable_raft_index
                && observation.committed_raft_index == baseline.committed_raft_index
            {
                break observation.durable_raft_index;
            }
            assert!(
                time::Instant::now() < deadline,
                "P7-I leader never exposed a durable uncommitted idempotent tail"
            );
        }
    };

    harness.stop_broker(leader_id).await;
    for broker_id in &followers {
        harness.restart_broker(*broker_id).await;
    }
    let (successor_id, successor_epoch, retried) = harness
        .produce_idempotent_internal_when_ready(
            authority,
            ProducerSequence::ZERO,
            b"durable-without-quorum",
        )
        .await;
    assert_ne!(successor_id, leader_id);
    assert!(successor_epoch > leader_epoch);
    assert_eq!((retried.base_offset, retried.last_offset), (0, 0));

    // Rejoining the failed leader forces its leader-only durable suffix through
    // normal P3 conflict repair. The committed retry must remain the sole
    // logical sequence-zero batch after that repair.
    harness.restart_broker(leader_id).await;
    harness
        .wait_replica_committed_at_least(leader_id, retried.raft_index)
        .await;
    let repaired = harness.brokers[leader_index]
        .as_ref()
        .unwrap()
        .observe_partition_internal(harness.topic_id, 0)
        .await
        .unwrap();
    assert!(repaired.committed_raft_index >= retried.raft_index);
    assert!(repaired.durable_raft_index >= uncommitted_raft_index);

    let reader = harness.client_on(successor_id, 0).await;
    assert_committed_values(&reader, harness.topic_id, &[b"durable-without-quorum"]).await;
    reader.close().await;
    harness.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 10)]
async fn p7_rf3_leader_death_before_request_reaches_persistence_retries_sequence_once() {
    let _guard = p7i_integration_lock().lock().await;
    let mut harness = Rf3Harness::start("leader-death-before-persist").await;
    let observer = harness.client(0).await;
    let (leader_id, leader_epoch) = harness.route(&observer).await;
    observer.close().await;
    let bootstrap_id = [1_u32, 2, 3]
        .into_iter()
        .find(|candidate| *candidate != leader_id)
        .unwrap();
    let client = harness.client_on(bootstrap_id, 6).await;
    client.init_producer().await.unwrap();
    let (confirmed_leader, confirmed_epoch) = harness.route(&client).await;
    assert_eq!(
        (confirmed_leader, confirmed_epoch),
        (leader_id, leader_epoch)
    );

    let leader_index = usize::try_from(leader_id - 1).unwrap();
    let baseline = harness.brokers[leader_index]
        .as_ref()
        .unwrap()
        .observe_partition_internal(harness.topic_id, 0)
        .await
        .unwrap();
    let control = harness.proxy_controls[leader_index].clone();
    let held = control.produce_request_held.notified();
    control
        .hold_next_produce_request
        .store(true, Ordering::SeqCst);
    let producer = client.clone();
    let topic_id = harness.topic_id;
    let produce_task = tokio::spawn(async move {
        producer
            .produce_idempotent_partition(
                topic_id,
                0,
                vec![wire_record(b"leader-dies-before-persist")],
            )
            .await
    });
    time::timeout(Duration::from_secs(20), held)
        .await
        .expect("P7-I proxy never held the pre-persistence Produce request");

    let still_unmodified = harness.brokers[leader_index]
        .as_ref()
        .unwrap()
        .observe_partition_internal(harness.topic_id, 0)
        .await
        .unwrap();
    assert_eq!(
        (
            still_unmodified.durable_raft_index,
            still_unmodified.committed_raft_index
        ),
        (baseline.durable_raft_index, baseline.committed_raft_index),
        "held request reached partition persistence before the injected leader death"
    );

    harness.stop_broker(leader_id).await;
    control.release_held_produce_request.notify_waiters();
    let first_outcome = time::timeout(Duration::from_secs(30), produce_task)
        .await
        .expect("P7-I client did not recover from pre-persistence leader death")
        .unwrap();
    let outcome = match first_outcome {
        Ok(outcome) => outcome,
        Err(ClientError::Timeout | ClientError::ConnectionClosed | ClientError::Io(_)) => {
            // The killed leader may close or reset its TCP connection instead
            // of exhausting the bounded automatic retry budget as a timeout.
            // All three outcomes are ambiguous, not terminal for an idempotent
            // request: the client must retain the exact unresolved sequence.
            // An explicit retry after successor election must commit it once,
            // without allocating a new sequence or duplicating the batch.
            let (successor_id, successor_epoch) = harness
                .wait_partition_leader_excluding(Some(leader_id))
                .await;
            assert_ne!(successor_id, leader_id);
            assert!(successor_epoch > leader_epoch);
            client
                .retry_idempotent_partition(harness.topic_id, 0)
                .await
                .unwrap()
        }
        Err(error) => panic!(
            "P7-I pre-persistence failover returned unexpected terminal client error: {error}"
        ),
    };
    assert_eq!((outcome.base_offset, outcome.last_offset), (0, 0));
    assert_committed_values(&client, harness.topic_id, &[b"leader-dies-before-persist"]).await;

    client.close().await;
    harness.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 10)]
async fn p7_rf3_uncommitted_higher_epoch_fences_locally_but_not_after_failover() {
    let _guard = p7i_integration_lock().lock().await;
    let mut harness = Rf3Harness::start("uncommitted-higher-epoch").await;
    let client = harness.client(0).await;
    let epoch_one = client.init_producer().await.unwrap();
    let first = client
        .produce_idempotent_partition(harness.topic_id, 0, vec![wire_record(b"epoch1-seq0")])
        .await
        .unwrap();
    assert_eq!((first.base_offset, first.last_offset), (0, 0));
    let epoch_two = client.restart_producer().await.unwrap();
    assert_eq!(epoch_two.producer_id, epoch_one.producer_id);
    assert_eq!(
        epoch_two.producer_epoch.get(),
        epoch_one.producer_epoch.get() + 1
    );
    let (leader_id, leader_epoch) = harness.route(&client).await;
    client.close().await;

    let followers = [1_u32, 2, 3]
        .into_iter()
        .filter(|broker_id| *broker_id != leader_id)
        .collect::<Vec<_>>();
    for broker_id in &followers {
        harness.stop_broker(*broker_id).await;
    }
    let leader_index = usize::try_from(leader_id - 1).unwrap();
    let tentative_raft_index = {
        let leader = harness.brokers[leader_index].as_ref().unwrap();
        let baseline = leader
            .observe_partition_internal(harness.topic_id, 0)
            .await
            .unwrap();
        let higher = leader.produce_idempotent_internal(
            harness.topic_id,
            0,
            leader_epoch,
            epoch_two,
            ProducerSequence::ZERO,
            vec![data_record(b"epoch2-uncommitted")],
        );
        tokio::pin!(higher);
        let deadline = time::Instant::now() + Duration::from_secs(20);
        let tentative_raft_index = loop {
            tokio::select! {
                result = &mut higher => {
                    panic!("P7-I higher epoch committed without RF3 quorum: {result:?}");
                }
                _ = time::sleep(Duration::from_millis(20)) => {}
            }
            let observation = leader
                .observe_partition_internal(harness.topic_id, 0)
                .await
                .unwrap();
            if observation.durable_raft_index > baseline.durable_raft_index
                && observation.committed_raft_index == baseline.committed_raft_index
            {
                break observation.durable_raft_index;
            }
            assert!(
                time::Instant::now() < deadline,
                "P7-I higher epoch never reached durable-uncommitted state"
            );
        };

        let old_race = leader
            .produce_idempotent_internal(
                harness.topic_id,
                0,
                leader_epoch,
                epoch_one,
                ProducerSequence::new(1),
                vec![data_record(b"epoch1-races-pending-epoch2")],
            )
            .await
            .unwrap_err();
        assert_eq!(old_race.error, PublicError::ProducerFenced);
        tentative_raft_index
    };

    harness.stop_broker(leader_id).await;
    for broker_id in &followers {
        harness.restart_broker(*broker_id).await;
    }
    let (successor_id, successor_epoch, old_epoch_continues) = harness
        .produce_idempotent_internal_when_ready(
            epoch_one,
            ProducerSequence::new(1),
            b"epoch1-after-tentative-epoch2-failover",
        )
        .await;
    assert_ne!(successor_id, leader_id);
    assert!(successor_epoch > leader_epoch);
    assert_eq!(
        (
            old_epoch_continues.base_offset,
            old_epoch_continues.last_offset
        ),
        (1, 1)
    );

    harness.restart_broker(leader_id).await;
    harness
        .wait_replica_committed_at_least(leader_id, old_epoch_continues.raft_index)
        .await;
    let repaired = harness.brokers[leader_index]
        .as_ref()
        .unwrap()
        .observe_partition_internal(harness.topic_id, 0)
        .await
        .unwrap();
    assert!(repaired.durable_raft_index >= tentative_raft_index);
    assert!(repaired.committed_raft_index >= old_epoch_continues.raft_index);

    let reader = harness.client_on(successor_id, 0).await;
    assert_committed_values(
        &reader,
        harness.topic_id,
        &[b"epoch1-seq0", b"epoch1-after-tentative-epoch2-failover"],
    )
    .await;
    reader.close().await;
    harness.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 10)]
async fn p7_rf3_committed_higher_epoch_resets_sequence_and_fences_old_after_failover() {
    let _guard = p7i_integration_lock().lock().await;
    let mut harness = Rf3Harness::start("committed-higher-epoch").await;
    let client = harness.client(2).await;
    let epoch_one = client.init_producer().await.unwrap();
    let first = client
        .produce_idempotent_partition(harness.topic_id, 0, vec![wire_record(b"epoch1-seq0")])
        .await
        .unwrap();
    assert_eq!((first.base_offset, first.last_offset), (0, 0));

    let epoch_two = client.restart_producer().await.unwrap();
    assert_eq!(epoch_two.producer_id, epoch_one.producer_id);
    assert_eq!(
        epoch_two.producer_epoch.get(),
        epoch_one.producer_epoch.get() + 1
    );
    let higher = client
        .produce_idempotent_partition(harness.topic_id, 0, vec![wire_record(b"epoch2-seq0")])
        .await
        .unwrap();
    assert_eq!((higher.base_offset, higher.last_offset), (1, 1));
    let (leader_id, leader_epoch) = harness.route(&client).await;
    let leader_index = usize::try_from(leader_id - 1).unwrap();

    let stale_local = harness.brokers[leader_index]
        .as_ref()
        .unwrap()
        .produce_idempotent_internal(
            harness.topic_id,
            0,
            leader_epoch,
            epoch_one,
            ProducerSequence::new(1),
            vec![data_record(b"stale-after-epoch2")],
        )
        .await
        .unwrap_err();
    assert_eq!(stale_local.error, PublicError::ProducerFenced);
    client.close().await;

    harness.stop_broker(leader_id).await;
    let (successor_id, successor_epoch, stale_after_failover) = harness
        .terminal_idempotent_error_when_ready(
            epoch_one,
            ProducerSequence::new(1),
            b"stale-after-epoch2-failover",
        )
        .await;
    assert_ne!(successor_id, leader_id);
    assert!(successor_epoch > leader_epoch);
    let successor_index = usize::try_from(successor_id - 1).unwrap();
    assert_eq!(stale_after_failover, PublicError::ProducerFenced);

    let duplicate_higher = harness.brokers[successor_index]
        .as_ref()
        .unwrap()
        .produce_idempotent_internal(
            harness.topic_id,
            0,
            successor_epoch,
            epoch_two,
            ProducerSequence::ZERO,
            vec![data_record(b"epoch2-seq0")],
        )
        .await
        .unwrap();
    assert_eq!(
        (duplicate_higher.base_offset, duplicate_higher.last_offset),
        (higher.base_offset, higher.last_offset)
    );

    let reader = harness.client_on(successor_id, 0).await;
    assert_committed_values(&reader, harness.topic_id, &[b"epoch1-seq0", b"epoch2-seq0"]).await;
    reader.close().await;
    harness.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 10)]
async fn p7_rf3_controller_outage_preserves_accepted_epoch_and_rejects_unseen_or_higher() {
    let _guard = p7i_integration_lock().lock().await;
    let mut harness = Rf3Harness::start("controller-outage-authority").await;
    let client = harness.client(2).await;
    let epoch_one = client.init_producer().await.unwrap();
    let first = client
        .produce_idempotent_partition(
            harness.topic_id,
            0,
            vec![wire_record(b"epoch1-before-outage")],
        )
        .await
        .unwrap();
    assert_eq!((first.base_offset, first.last_offset), (0, 0));

    // Advance controller authority without presenting the new epoch to this
    // partition. Partition-local epoch one therefore remains the accepted data
    // authority until epoch two is actually validated and committed here.
    let epoch_two = init_producer_controller(&harness.controllers, Some(epoch_one)).await;
    let unseen = init_producer_controller(&harness.controllers, None).await;
    let (leader_id, leader_epoch) = harness.route(&client).await;
    let leader_index = usize::try_from(leader_id - 1).unwrap();

    harness.shutdown_controllers().await;

    let continuation = client
        .produce_idempotent_partition(
            harness.topic_id,
            0,
            vec![wire_record(b"epoch1-during-controller-outage")],
        )
        .await
        .unwrap();
    assert_eq!((continuation.base_offset, continuation.last_offset), (1, 1));

    let baseline = harness.brokers[leader_index]
        .as_ref()
        .unwrap()
        .observe_partition_internal(harness.topic_id, 0)
        .await
        .unwrap();
    let higher_error = harness.brokers[leader_index]
        .as_ref()
        .unwrap()
        .produce_idempotent_internal(
            harness.topic_id,
            0,
            leader_epoch,
            epoch_two,
            ProducerSequence::ZERO,
            vec![data_record(b"higher-without-controller")],
        )
        .await
        .unwrap_err();
    assert_eq!(higher_error.error, PublicError::BrokerNotAvailable);
    let unseen_error = harness.brokers[leader_index]
        .as_ref()
        .unwrap()
        .produce_idempotent_internal(
            harness.topic_id,
            0,
            leader_epoch,
            unseen,
            ProducerSequence::ZERO,
            vec![data_record(b"unseen-without-controller")],
        )
        .await
        .unwrap_err();
    assert_eq!(unseen_error.error, PublicError::BrokerNotAvailable);
    let after_rejections = harness.brokers[leader_index]
        .as_ref()
        .unwrap()
        .observe_partition_internal(harness.topic_id, 0)
        .await
        .unwrap();
    assert_eq!(
        (
            after_rejections.durable_raft_index,
            after_rejections.committed_raft_index
        ),
        (baseline.durable_raft_index, baseline.committed_raft_index),
        "unseen/higher authority was proposed while controller confirmation was unavailable"
    );
    assert_committed_values(
        &client,
        harness.topic_id,
        &[b"epoch1-before-outage", b"epoch1-during-controller-outage"],
    )
    .await;

    client.close().await;
    harness.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 6)]
async fn p7_controller_concurrent_epoch_bump_cas_allows_exactly_one_winner() {
    let _guard = p7i_integration_lock().lock().await;
    let root = temp_dir("controller-epoch-cas-race");
    let addresses = [reserve_addr(), reserve_addr(), reserve_addr()];
    let mut controllers = BTreeMap::new();
    for id in 1..=3_u64 {
        controllers.insert(
            cid(id),
            start_controller_node(controller_config(&root, id, &addresses))
                .await
                .unwrap(),
        );
    }
    mutate(
        &controllers,
        ControllerCommand::SetFeatureLevel {
            expected_feature_level: 1,
            new_feature_level: 3,
        },
    )
    .await;
    let first = init_producer_controller(&controllers, None).await;
    let leader = stable_controller_leader(&controllers).await;
    let left = controllers[&leader].init_producer(Some(first));
    let right = controllers[&leader].init_producer(Some(first));
    let (left, right) = tokio::join!(left, right);
    let winners = [left.as_ref().ok().copied(), right.as_ref().ok().copied()]
        .into_iter()
        .flatten()
        .collect::<Vec<_>>();
    assert_eq!(
        winners.len(),
        1,
        "same expected epoch produced multiple CAS winners"
    );
    assert!(left.is_err() || right.is_err());
    let winner = winners[0];
    assert_eq!(winner.producer_id, first.producer_id);
    assert_eq!(winner.producer_epoch.get(), first.producer_epoch.get() + 1);
    let current_leader = stable_controller_leader(&controllers).await;
    assert_eq!(
        controllers[&current_leader]
            .producer_authority(first.producer_id)
            .await
            .unwrap(),
        Some(winner)
    );
    assert!(
        controllers[&current_leader]
            .init_producer(Some(first))
            .await
            .is_err(),
        "stale expected epoch unexpectedly advanced authority after CAS winner"
    );

    for (_, controller) in controllers {
        controller.shutdown().await;
    }
    fs::remove_dir_all(root).unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 10)]
async fn p7_public_ambiguous_init_survives_controller_leader_failover_without_id_reuse() {
    let _guard = p7i_integration_lock().lock().await;
    let mut harness = Rf3Harness::start("init-controller-failover").await;
    let client = harness.client_on(1, 2).await;
    let control = harness.proxy_controls[0].clone();
    let response_seen = control.init_producer_response_seen.notified();
    control
        .drop_next_init_producer_response
        .store(true, Ordering::SeqCst);
    let producer = client.clone();
    let init_task = tokio::spawn(async move { producer.init_producer().await });
    time::timeout(Duration::from_secs(20), response_seen)
        .await
        .expect("P7-I proxy never observed committed InitProducer response");

    let failed_controller = stable_controller_leader(&harness.controllers).await;
    harness
        .controllers
        .remove(&failed_controller)
        .unwrap()
        .shutdown()
        .await;
    let ambiguous = time::timeout(Duration::from_secs(10), init_task)
        .await
        .expect("P7-I lost InitProducer response did not resolve")
        .unwrap()
        .unwrap_err();
    assert!(matches!(
        ambiguous,
        ClientError::AmbiguousMutation {
            api: ApiKey::InitProducer
        }
    ));
    assert_eq!(client.producer_authority().await, None);

    let successor = stable_controller_leader(&harness.controllers).await;
    let ambiguous_id = ProducerId::new(1_000).unwrap();
    let committed_ambiguous = harness.controllers[&successor]
        .producer_authority(ambiguous_id)
        .await
        .unwrap()
        .expect("committed ambiguous ProducerId disappeared after controller failover");
    assert_eq!(committed_ambiguous.producer_epoch.get(), 1);

    let recovered = client.recover_producer_session().await.unwrap();
    assert_eq!(recovered.producer_id.get(), 1_001);
    assert_eq!(recovered.producer_epoch.get(), 1);
    assert_ne!(recovered.producer_id, committed_ambiguous.producer_id);

    client.close().await;
    harness.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 10)]
async fn p7_rf3_restart_from_producer_snapshots_deduplicates_exact_retry() {
    let _guard = p7i_integration_lock().lock().await;
    let mut harness = Rf3Harness::start("snapshot-restart-exact-retry").await;
    let client = harness.client(2).await;
    let authority = client.init_producer().await.unwrap();
    let first = client
        .produce_idempotent_partition(harness.topic_id, 0, vec![wire_record(b"snapshot-seq0")])
        .await
        .unwrap();
    let second = client
        .produce_idempotent_partition(harness.topic_id, 0, vec![wire_record(b"snapshot-seq1")])
        .await
        .unwrap();
    assert_eq!((first.base_offset, first.last_offset), (0, 0));
    assert_eq!((second.base_offset, second.last_offset), (1, 1));

    let (leader_id, _) = harness.route(&client).await;
    let leader_index = usize::try_from(leader_id - 1).unwrap();
    let snapshot_boundary = harness.brokers[leader_index]
        .as_ref()
        .unwrap()
        .observe_partition_internal(harness.topic_id, 0)
        .await
        .unwrap()
        .committed_raft_index;
    for broker_id in 1..=3_u32 {
        harness
            .wait_replica_committed_at_least(broker_id, snapshot_boundary)
            .await;
        let index = usize::try_from(broker_id - 1).unwrap();
        harness.brokers[index]
            .as_ref()
            .unwrap()
            .persist_producer_snapshot_internal(harness.topic_id, 0)
            .await
            .unwrap();
    }
    assert!(
        count_producer_snapshots(&harness.root) >= 3,
        "P7-I did not materialize a producer snapshot on every RF3 replica"
    );
    client.close().await;

    for broker_id in 1..=3_u32 {
        harness.stop_broker(broker_id).await;
    }
    for broker_id in 1..=3_u32 {
        harness.restart_broker(broker_id).await;
    }
    assert!(
        count_producer_snapshots(&harness.root) >= 3,
        "P7-I producer snapshots disappeared across broker restart"
    );

    let (restarted_leader_id, _restarted_epoch, duplicate) = harness
        .produce_idempotent_internal_when_ready(
            authority,
            ProducerSequence::new(1),
            b"snapshot-seq1",
        )
        .await;
    assert_eq!(
        (duplicate.base_offset, duplicate.last_offset),
        (second.base_offset, second.last_offset)
    );

    let reader = harness.client_on(restarted_leader_id, 0).await;
    assert_committed_values(
        &reader,
        harness.topic_id,
        &[b"snapshot-seq0", b"snapshot-seq1"],
    )
    .await;
    reader.close().await;
    harness.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 10)]
async fn p7_rf3_committed_retry_survives_explicit_leadership_transfer() {
    let _guard = p7i_integration_lock().lock().await;
    let harness = Rf3Harness::start("transfer-retry").await;
    let client = harness.client(0).await;
    client.init_producer().await.unwrap();
    let first = client
        .produce_idempotent_partition(harness.topic_id, 0, vec![wire_record(b"before-transfer")])
        .await
        .unwrap();
    assert_eq!((first.base_offset, first.last_offset), (0, 0));

    let (leader_id, leader_epoch) = harness.route(&client).await;
    let leader_index = usize::try_from(leader_id - 1).unwrap();
    harness.proxy_controls[leader_index]
        .drop_next_produce_response
        .store(true, Ordering::SeqCst);
    let ambiguous = client
        .produce_idempotent_partition(
            harness.topic_id,
            0,
            vec![wire_record(b"committed-before-transfer")],
        )
        .await
        .unwrap_err();
    assert!(matches!(
        ambiguous,
        ClientError::Timeout | ClientError::ConnectionClosed | ClientError::Io(_)
    ));

    let target_id = [1_u32, 2, 3]
        .into_iter()
        .find(|candidate| *candidate != leader_id)
        .unwrap();
    harness.brokers[leader_index]
        .as_ref()
        .unwrap()
        .request_preferred_leader(PreferredLeaderRequest {
            topic_id: harness.topic_id,
            partition_id: 0,
            target_broker_id: target_id,
        })
        .await
        .unwrap();

    let transfer_deadline = time::Instant::now() + Duration::from_secs(20);
    loop {
        let (observed_leader, observed_epoch) = harness.route(&client).await;
        if observed_leader == target_id && observed_epoch > leader_epoch {
            break;
        }
        assert!(
            time::Instant::now() < transfer_deadline,
            "P7-I leadership transfer did not become visible"
        );
        time::sleep(Duration::from_millis(20)).await;
    }

    let retried = client
        .retry_idempotent_partition(harness.topic_id, 0)
        .await
        .unwrap();
    assert_eq!((retried.base_offset, retried.last_offset), (1, 1));

    client.close().await;
    harness.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 10)]
async fn p7j_seeded_repeated_rf3_failover_preserves_unique_committed_sequences() {
    let _guard = p7i_integration_lock().lock().await;
    let mut harness = Rf3Harness::start("p7j-repeated-failover").await;
    let bootstrap = harness.client(0).await;
    let authority = bootstrap.init_producer().await.unwrap();
    bootstrap.close().await;

    let mut rng = P7jSeededRng::new(0x5eed_fa17_0a57_0007);
    let mut leader_trace = Vec::new();
    let mut expected_payloads = Vec::new();
    const ROUNDS: u32 = 6;

    for sequence in 0..ROUNDS {
        let nonce = rng.next_u64();
        let payload = format!("p7j-failover-{sequence}-{nonce:016x}").into_bytes();
        expected_payloads.push(payload.clone());
        let (leader_id, _leader_epoch, committed) = harness
            .produce_idempotent_internal_when_ready(
                authority,
                ProducerSequence::new(sequence),
                &payload,
            )
            .await;
        leader_trace.push(leader_id);
        assert_eq!(
            (committed.base_offset, committed.last_offset),
            (u64::from(sequence), u64::from(sequence))
        );

        harness.stop_broker(leader_id).await;
        let (successor_id, successor_epoch, duplicate) = harness
            .produce_idempotent_internal_when_ready(
                authority,
                ProducerSequence::new(sequence),
                &payload,
            )
            .await;
        assert_ne!(successor_id, leader_id);
        let successor_index = usize::try_from(successor_id - 1).unwrap();
        assert_eq!(
            (duplicate.base_offset, duplicate.last_offset),
            (committed.base_offset, committed.last_offset),
            "P7-J failover retry appended a duplicate logical sequence"
        );

        if rng.next_u64() & 1 == 1 {
            let duplicate_again = harness.brokers[successor_index]
                .as_ref()
                .unwrap()
                .produce_idempotent_internal(
                    harness.topic_id,
                    0,
                    successor_epoch,
                    authority,
                    ProducerSequence::new(sequence),
                    vec![data_record(&payload)],
                )
                .await
                .unwrap();
            assert_eq!(
                (duplicate_again.base_offset, duplicate_again.last_offset),
                (committed.base_offset, committed.last_offset)
            );
        }

        harness.restart_broker(leader_id).await;
        harness
            .wait_replica_committed_at_least(leader_id, duplicate.raft_index)
            .await;
    }

    assert!(
        leader_trace
            .windows(2)
            .all(|leaders| leaders[0] != leaders[1]),
        "P7-J repeated failover did not move leadership each round"
    );
    let (reader_broker, _) = harness.wait_partition_leader_excluding(None).await;
    let reader = harness.client_on(reader_broker, 0).await;
    let expected = expected_payloads
        .iter()
        .map(Vec::as_slice)
        .collect::<Vec<_>>();
    assert_committed_values(&reader, harness.topic_id, &expected).await;
    reader.close().await;
    harness.shutdown().await;
}
