use std::{
    collections::BTreeMap,
    fs,
    net::{Ipv4Addr, SocketAddr},
    path::{Path, PathBuf},
    sync::atomic::{AtomicU32, Ordering},
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use astralog_broker::{
    BrokerConfig, BrokerError, BrokerHandle, DynamicControllerConfig, DynamicReplicationConfig,
    GroupCommitOutcome, GroupCoordinatorError, GroupCoordinatorObservation, OffsetCoordinatorError,
    OffsetFenceCommitOutcome, P6InternalTopics, PreferredLeaderRequest, start as start_broker,
};
use astralog_client::{AstraClient, ClientConfig, ClientError, GroupCoordinatorRoute};
use astralog_common::TopicId;
use astralog_controller::{
    BrokerLifecycle, BrokerLivenessState, ControllerCommand, ControllerNodeConfig,
    ControllerNodeHandle, ControllerNodeId, ControllerPeerDescriptor,
    ControllerPeerTransportConfig, ControllerPersistenceConfig, ControllerRuntimeConfig,
    ControllerRuntimeLimits, ControllerState, CreateTopicSpec, FailureDetectorConfig,
    FailureDomainPolicy, FeatureRange, HeartbeatLimits, TopicExpansionSpec, start_controller_node,
};
use astralog_groups::{
    CooperativeStateMachine, GROUPS_INTERNAL_TOPIC_NAME, GroupCommand, GroupMutationPlan,
    GroupPhase, GroupRecordV1, GroupStateMachine, OFFSETS_INTERNAL_TOPIC_NAME, OffsetGroupFenceV1,
    OffsetOwnerFenceV1, P6_INTERNAL_TOPIC_PARTITIONS, P6_INTERNAL_TOPIC_REPLICATION_FACTOR,
};
use astralog_protocol::{
    ApiKey, ClientHello, FrameKind, MAX_WIRE_FRAME_LEN_V1, PublicError, ServerHello, WireFrame,
    api::{
        ApiVersionsRequestV0, ApiVersionsResponseV0, BodyCodec, CommitOffsetV0, GroupEpoch,
        GroupId, JoinSubscribeRequestV0, JoinSubscribeResponseV0, MemberAuthorityV0, MemberEpoch,
        MemberId, TopicPartitionV0,
    },
    decode_frame, encode_frame,
};
use astralog_raft::{NodeId, Role, StableConfiguration, TimingConfig};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::TcpStream,
    time,
};

static P6_E2E_TEST_LOCK: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct OffsetLeaderObservation {
    shard: u32,
    broker_id: u32,
    epoch: u64,
}

fn temp_dir(label: &str) -> PathBuf {
    let nonce = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    let path = std::env::temp_dir().join(format!(
        "astralog-p6c-e2e-{label}-{}-{nonce}",
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
    astralog_controller::initialize_controller_bootstrap(&directory, [6; 16], &voters()).unwrap();
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
            initial_state: ControllerState::new([6; 16], 1, 1_000).unwrap(),
            persistence: ControllerPersistenceConfig::default(),
            timing: TimingConfig::new(1, 5, 10).unwrap(),
            randomized_election_timeout: timeout,
            limits: ControllerRuntimeLimits::default(),
            heartbeat_limits: HeartbeatLimits::default(),
            failure_detector: FailureDetectorConfig::default(),
        },
        transport: ControllerPeerTransportConfig {
            local_controller_id: cid(id),
            cluster_id: [6; 16],
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

async fn controller_leader(
    controllers: &BTreeMap<ControllerNodeId, ControllerNodeHandle>,
) -> ControllerNodeId {
    // Full serialized P6/P7 qualification can leave controller sockets and
    // storage work draining from the previous RF3 scenario. Election is a
    // read-only observation boundary here, so allow bounded convergence
    // without weakening any controller mutation or authority assertion.
    let deadline = time::Instant::now() + Duration::from_secs(120);
    loop {
        for (id, node) in controllers {
            if let Ok(status) = node.status().await
                && status.role == Role::Leader
                && !status.fatal
            {
                return *id;
            }
        }
        assert!(
            time::Instant::now() < deadline,
            "controller election timed out"
        );
        time::sleep(Duration::from_millis(20)).await;
    }
}

async fn mutate(
    controllers: &BTreeMap<ControllerNodeId, ControllerNodeHandle>,
    command: ControllerCommand,
) {
    let deadline = time::Instant::now() + Duration::from_secs(30);
    loop {
        let leader = controller_leader(controllers).await;
        match controllers[&leader].mutate(command.clone()).await {
            Ok(_) => return,
            Err(
                astralog_controller::ControllerError::NotLeader { .. }
                | astralog_controller::ControllerError::Busy(_)
                | astralog_controller::ControllerError::Invalid("unknown broker"),
            ) => {}
            Err(error) => {
                // These P6 tests use only broker registration/lifecycle/rejoin as
                // setup mutations. Their response may be lost after the command
                // has already committed; blindly retrying then legitimately
                // returns an "already registered" / stale incarnation/lifecycle
                // error. Treat that ambiguity as success only when a fresh
                // committed controller snapshot proves the exact requested setup
                // effect is already present. This is test-harness idempotence,
                // not a production mutation semantic change.
                if setup_command_is_committed(&controllers[&leader], &command).await {
                    return;
                }
                // A committed setup command can be followed immediately by a
                // controller leadership change. The new leader may reject the
                // duplicate command from its already-advanced state before its
                // materialized metadata snapshot exposes that same committed
                // effect to this observation path. Keep these exact duplicate /
                // stale-CAS outcomes bounded and poll until the snapshot proves
                // the requested setup effect. If a different incarnation or
                // lifecycle actually won, the exact proof never becomes true
                // and the existing deadline still fails the test.
                if matches!(
                    error,
                    astralog_controller::ControllerError::Invalid(
                        "broker id already registered"
                            | "node UUID already registered"
                            | "stale broker incarnation"
                            | "stale broker lifecycle"
                    )
                ) {
                    assert!(
                        time::Instant::now() < deadline,
                        "controller setup mutation ambiguity did not converge: {error}"
                    );
                    time::sleep(Duration::from_millis(20)).await;
                    continue;
                }
                panic!("controller setup mutation failed: {error}");
            }
        }
        assert!(
            time::Instant::now() < deadline,
            "controller mutation timed out"
        );
        time::sleep(Duration::from_millis(20)).await;
    }
}

async fn setup_command_is_committed(
    controller: &ControllerNodeHandle,
    command: &ControllerCommand,
) -> bool {
    let Ok(snapshot) = controller.metadata_snapshot().await else {
        return false;
    };
    match command {
        ControllerCommand::RegisterBroker {
            broker_id,
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
        } => snapshot.state.broker(*broker_id).is_some_and(|broker| {
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
        }),
        ControllerCommand::RejoinBroker {
            broker_id,
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
        } => snapshot.state.broker(*broker_id).is_some_and(|broker| {
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
        }),
        ControllerCommand::SetBrokerLifecycle {
            broker_id,
            expected_incarnation,
            new_lifecycle,
            ..
        } => snapshot.state.broker(*broker_id).is_some_and(|broker| {
            broker.incarnation_epoch == *expected_incarnation && broker.lifecycle == *new_lifecycle
        }),
        ControllerCommand::SetFeatureLevel {
            new_feature_level, ..
        } => snapshot.state.active_feature_level() == *new_feature_level,
        _ => false,
    }
}

async fn failure_tick(controllers: &BTreeMap<ControllerNodeId, ControllerNodeHandle>, tick: u64) {
    let deadline = time::Instant::now() + Duration::from_secs(30);
    loop {
        let leader = controller_leader(controllers).await;
        match controllers[&leader].failure_detector_tick(tick).await {
            Ok(()) => return,
            Err(
                astralog_controller::ControllerError::NotLeader { .. }
                | astralog_controller::ControllerError::Busy(_),
            ) => {}
            Err(error) => panic!("failure-detector tick failed: {error}"),
        }
        assert!(time::Instant::now() < deadline);
        time::sleep(Duration::from_millis(20)).await;
    }
}

async fn start_broker_when_ready(
    config: BrokerConfig,
    controllers: &BTreeMap<ControllerNodeId, ControllerNodeHandle>,
) -> BrokerHandle {
    let deadline = time::Instant::now() + Duration::from_secs(60);
    loop {
        let _ = controller_leader(controllers).await;
        match start_broker(config.clone()).await {
            Ok(handle) => return handle,
            Err(BrokerError::Controller(
                astralog_controller::ControllerError::NotLeader { .. }
                | astralog_controller::ControllerError::Busy(_),
            )) => {}
            Err(error) => panic!("dynamic broker startup failed: {error}"),
        }
        assert!(time::Instant::now() < deadline);
        time::sleep(Duration::from_millis(20)).await;
    }
}

async fn wait_healthy(
    controllers: &BTreeMap<ControllerNodeId, ControllerNodeHandle>,
    broker_ids: &[u32],
    tick: u64,
) {
    let deadline = time::Instant::now() + Duration::from_secs(30);
    loop {
        failure_tick(controllers, tick).await;
        let leader = controller_leader(controllers).await;
        let mut healthy = true;
        for broker_id in broker_ids {
            healthy &= controllers[&leader]
                .broker_liveness(*broker_id)
                .await
                .is_ok_and(|observation| observation.state == BrokerLivenessState::Healthy);
        }
        if healthy {
            return;
        }
        assert!(
            time::Instant::now() < deadline,
            "brokers did not become healthy"
        );
        time::sleep(Duration::from_millis(25)).await;
    }
}

async fn resolve(
    client: &AstraClient,
    brokers: &[Option<BrokerHandle>],
    group_id: &GroupId,
    context: &'static str,
) -> GroupCoordinatorRoute {
    let deadline = time::Instant::now() + Duration::from_secs(30);
    loop {
        if let Ok(route) = client.resolve_group_coordinator(group_id).await {
            let index = usize::try_from(route.coordinator_broker_id - 1).ok();
            if let Some(broker) = index
                .and_then(|index| brokers.get(index))
                .and_then(Option::as_ref)
                && broker
                    .validate_group_coordinator_authority(group_id, route.coordinator_epoch)
                    .await
                    .is_ok()
            {
                return route;
            }
        }
        assert!(
            time::Instant::now() < deadline,
            "group coordinator resolution did not converge to live authority ({context})"
        );
        time::sleep(Duration::from_millis(40)).await;
    }
}

async fn resolve_after_failover(
    client: &AstraClient,
    brokers: &[Option<BrokerHandle>],
    group_id: &GroupId,
    previous_broker_id: u32,
    previous_epoch: u64,
    context: &'static str,
) -> GroupCoordinatorRoute {
    let deadline = time::Instant::now() + Duration::from_secs(30);
    loop {
        if let Ok(route) = client.resolve_group_coordinator(group_id).await
            && route.coordinator_broker_id != previous_broker_id
            && route.coordinator_epoch > previous_epoch
        {
            let index = usize::try_from(route.coordinator_broker_id - 1).ok();
            if let Some(broker) = index
                .and_then(|index| brokers.get(index))
                .and_then(Option::as_ref)
                && broker
                    .validate_group_coordinator_authority(group_id, route.coordinator_epoch)
                    .await
                    .is_ok()
            {
                return route;
            }
        }
        assert!(
            time::Instant::now() < deadline,
            "group coordinator failover did not converge ({context})"
        );
        time::sleep(Duration::from_millis(40)).await;
    }
}

async fn commit_group_on_current_coordinator(
    brokers: &[Option<BrokerHandle>],
    group_id: &GroupId,
    record: GroupRecordV1,
    context: &'static str,
) -> (GroupCoordinatorObservation, GroupCommitOutcome) {
    let deadline = time::Instant::now() + Duration::from_secs(30);
    loop {
        let route = observe_current_coordinator(brokers, group_id, None, context).await;
        let index = usize::try_from(route.hint.coordinator_broker_id - 1).unwrap();
        if let Some(broker) = brokers.get(index).and_then(Option::as_ref) {
            match broker
                .commit_group_record(route.hint.coordinator_epoch, record.clone())
                .await
            {
                Ok(outcome) => return (route, outcome),
                Err(
                    GroupCoordinatorError::NotCoordinator { .. }
                    | GroupCoordinatorError::Busy
                    | GroupCoordinatorError::NotReady(_)
                    | GroupCoordinatorError::MutationAmbiguous { .. },
                ) => {}
                Err(error) => panic!("group mutation failed ({context}): {error}"),
            }
        }
        assert!(
            time::Instant::now() < deadline,
            "group mutation authority did not converge ({context})"
        );
        time::sleep(Duration::from_millis(40)).await;
    }
}

async fn observe_current_coordinator(
    brokers: &[Option<BrokerHandle>],
    group_id: &GroupId,
    previous: Option<(u32, u64)>,
    context: &'static str,
) -> GroupCoordinatorObservation {
    let deadline = time::Instant::now() + Duration::from_secs(60);
    loop {
        let mut by_term = BTreeMap::<u64, GroupCoordinatorObservation>::new();
        for broker in brokers.iter().flatten() {
            if let Ok(observation) = broker.observe_group_coordinator(group_id).await {
                let broker_id = observation.hint.coordinator_broker_id;
                let epoch = observation.hint.coordinator_epoch;
                if broker_id == u32::MAX || epoch == 0 || epoch == u64::MAX {
                    continue;
                }
                let Ok(index) = usize::try_from(broker_id - 1) else {
                    continue;
                };
                if brokers.get(index).is_none_or(Option::is_none) {
                    continue;
                }
                by_term.entry(epoch).or_insert(observation);
            }
        }
        for (&epoch, candidate) in by_term.iter().rev() {
            let leader = candidate.hint.coordinator_broker_id;
            if previous
                .is_some_and(|(old_broker, old_epoch)| leader == old_broker || epoch <= old_epoch)
            {
                continue;
            }
            let index = usize::try_from(leader - 1).unwrap();
            if let Some(authority) = brokers.get(index).and_then(Option::as_ref)
                && authority
                    .validate_group_coordinator_authority(group_id, epoch)
                    .await
                    .is_ok()
            {
                return *candidate;
            }
        }
        assert!(
            time::Instant::now() < deadline,
            "in-process coordinator observation did not converge ({context})"
        );
        time::sleep(Duration::from_millis(40)).await;
    }
}

async fn observe_offsets_leader(
    _client: &AstraClient,
    brokers: &[Option<BrokerHandle>],
    _topics: P6InternalTopics,
    group_id: &GroupId,
    previous: Option<(u32, u64)>,
    context: &'static str,
) -> OffsetLeaderObservation {
    let deadline = time::Instant::now() + Duration::from_secs(60);
    loop {
        let mut by_term = BTreeMap::new();
        for broker in brokers.iter().flatten() {
            if let Ok(observation) = broker.observe_offset_shard_internal(group_id.clone()).await {
                if observation.leader_broker_id == u32::MAX
                    || observation.leader_epoch == 0
                    || observation.leader_epoch == u64::MAX
                {
                    continue;
                }
                by_term
                    .entry(observation.leader_epoch)
                    .or_insert(observation);
            }
        }
        for (&epoch, observation) in by_term.iter().rev() {
            let broker_id = observation.leader_broker_id;
            if previous.is_some_and(|(old_broker, old_epoch)| {
                broker_id == old_broker || epoch <= old_epoch
            }) {
                continue;
            }
            let Ok(index) = usize::try_from(broker_id - 1) else {
                continue;
            };
            if let Some(broker) = brokers.get(index).and_then(Option::as_ref)
                && broker
                    .recover_offset_state_internal(group_id.clone(), epoch)
                    .await
                    .is_ok()
            {
                return OffsetLeaderObservation {
                    shard: observation.shard,
                    broker_id,
                    epoch,
                };
            }
        }
        assert!(
            time::Instant::now() < deadline,
            "offsets-shard leader did not converge ({context})"
        );
        time::sleep(Duration::from_millis(40)).await;
    }
}

async fn commit_group_fence_on_current_offsets(
    client: &AstraClient,
    brokers: &[Option<BrokerHandle>],
    topics: P6InternalTopics,
    fence: OffsetGroupFenceV1,
    context: &'static str,
) -> OffsetFenceCommitOutcome {
    let deadline = time::Instant::now() + Duration::from_secs(60);
    loop {
        let route =
            observe_offsets_leader(client, brokers, topics, &fence.group_id, None, context).await;
        let index = usize::try_from(route.broker_id - 1).unwrap();
        match brokers[index]
            .as_ref()
            .unwrap()
            .commit_offset_group_fence_internal(route.epoch, fence.clone())
            .await
        {
            Ok(outcome) => return outcome,
            Err(
                OffsetCoordinatorError::MutationAmbiguous { .. }
                | OffsetCoordinatorError::NotLeader { .. }
                | OffsetCoordinatorError::Busy
                | OffsetCoordinatorError::Replication(
                    PublicError::NotLeader
                    | PublicError::BrokerBusy
                    | PublicError::BrokerNotAvailable,
                ),
            ) => {
                assert!(
                    time::Instant::now() < deadline,
                    "idempotent group-fence commit did not converge ({context})"
                );
                time::sleep(Duration::from_millis(40)).await;
            }
            Err(error) => panic!("group-fence commit failed ({context}): {error}"),
        }
    }
}

async fn commit_owner_fence_on_current_offsets(
    client: &AstraClient,
    brokers: &[Option<BrokerHandle>],
    topics: P6InternalTopics,
    fence: OffsetOwnerFenceV1,
    context: &'static str,
) -> OffsetFenceCommitOutcome {
    let deadline = time::Instant::now() + Duration::from_secs(60);
    loop {
        let route =
            observe_offsets_leader(client, brokers, topics, &fence.group_id, None, context).await;
        let index = usize::try_from(route.broker_id - 1).unwrap();
        match brokers[index]
            .as_ref()
            .unwrap()
            .commit_offset_owner_fence_internal(route.epoch, fence.clone())
            .await
        {
            Ok(outcome) => return outcome,
            Err(
                OffsetCoordinatorError::MutationAmbiguous { .. }
                | OffsetCoordinatorError::NotLeader { .. }
                | OffsetCoordinatorError::Busy
                | OffsetCoordinatorError::Replication(
                    PublicError::NotLeader
                    | PublicError::BrokerBusy
                    | PublicError::BrokerNotAvailable,
                ),
            ) => {
                assert!(
                    time::Instant::now() < deadline,
                    "idempotent owner-fence commit did not converge ({context})"
                );
                time::sleep(Duration::from_millis(40)).await;
            }
            Err(error) => panic!("owner-fence commit failed ({context}): {error}"),
        }
    }
}

async fn wait_group_replica_convergence(
    brokers: &[Option<BrokerHandle>],
    group_id: &GroupId,
    context: &'static str,
) {
    let deadline = time::Instant::now() + Duration::from_secs(60);
    loop {
        let mut counts = BTreeMap::<(u32, u64), usize>::new();
        let mut diagnostics = Vec::new();
        for (index, broker) in brokers.iter().enumerate() {
            let broker_id = u32::try_from(index + 1).unwrap();
            let Some(broker) = broker.as_ref() else {
                diagnostics.push(format!("broker {broker_id}: down"));
                continue;
            };
            match broker.observe_group_coordinator(group_id).await {
                Ok(observation) => {
                    let leader = observation.hint.coordinator_broker_id;
                    let epoch = observation.hint.coordinator_epoch;
                    diagnostics.push(format!("broker {broker_id}: leader={leader} term={epoch}"));
                    if leader == u32::MAX || epoch == 0 || epoch == u64::MAX {
                        continue;
                    }
                    *counts.entry((leader, epoch)).or_default() += 1;
                }
                Err(error) => diagnostics.push(format!("broker {broker_id}: {error}")),
            }
        }
        if counts
            .values()
            .any(|count| *count >= usize::from(P6_INTERNAL_TOPIC_REPLICATION_FACTOR))
        {
            return;
        }
        assert!(
            time::Instant::now() < deadline,
            "group RF3 replicas did not converge after rejoin ({context}); counts={counts:?}; observations={diagnostics:?}"
        );
        time::sleep(Duration::from_millis(40)).await;
    }
}

async fn refresh_p6_topology_on_live_brokers(
    brokers: &[Option<BrokerHandle>],
    context: &'static str,
) {
    for broker in brokers.iter().flatten() {
        let deadline = time::Instant::now() + Duration::from_secs(30);
        loop {
            match broker.ensure_p6_internal_topics().await {
                Ok(_) => break,
                Err(error) => {
                    assert!(
                        time::Instant::now() < deadline,
                        "broker metadata/topology did not refresh after rejoin ({context}): {error}"
                    );
                }
            }
            time::sleep(Duration::from_millis(40)).await;
        }
    }
}

async fn start_p6_cluster(
    label: &str,
) -> (
    PathBuf,
    BTreeMap<ControllerNodeId, ControllerNodeHandle>,
    Vec<SocketAddr>,
    Vec<SocketAddr>,
    Vec<BrokerConfig>,
    Vec<Option<BrokerHandle>>,
    P6InternalTopics,
    AstraClient,
) {
    // The serialized P6 qualification binary repeatedly tears down and
    // recreates three controller transports plus four broker/client/Raft
    // transports. On Windows, the preceding Tokio runtime can finish its
    // socket/file-handle teardown just after the test future returns. Give that
    // teardown a small bounded drain interval before allocating the next
    // process-salted loopback topology. This is test-harness pacing only; it
    // does not alter controller election or production retry timing.
    time::sleep(Duration::from_millis(250)).await;
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
    let _ = controller_leader(&controllers).await;

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

    let controller_clients = controllers
        .values()
        .map(ControllerNodeHandle::client)
        .collect::<Vec<_>>();
    let configs = (1..=4_u32)
        .map(|broker_id| {
            let index = usize::try_from(broker_id - 1).unwrap();
            broker_config(
                &root,
                broker_id,
                client_addresses[index],
                raft_addresses[index],
                &controller_clients,
            )
        })
        .collect::<Vec<_>>();
    let mut brokers = Vec::new();
    for config in &configs {
        brokers.push(Some(
            start_broker_when_ready(config.clone(), &controllers).await,
        ));
    }
    wait_healthy(&controllers, &[1, 2, 3, 4], 1_000).await;
    let topics_deadline = time::Instant::now() + Duration::from_secs(30);
    let topics = loop {
        match brokers[0]
            .as_ref()
            .unwrap()
            .ensure_p6_internal_topics()
            .await
        {
            Ok(topics) => break topics,
            Err(error) => {
                assert!(
                    time::Instant::now() < topics_deadline,
                    "P6 internal-topic bootstrap did not converge: {error}"
                );
                wait_healthy(&controllers, &[1, 2, 3, 4], 1_100).await;
                time::sleep(Duration::from_millis(40)).await;
            }
        }
    };
    let mut client_config =
        ClientConfig::new(brokers[0].as_ref().unwrap().local_addr().to_string());
    client_config.request_timeout = Duration::from_secs(5);
    let client_deadline = time::Instant::now() + Duration::from_secs(30);
    let client = loop {
        match AstraClient::connect(client_config.clone()).await {
            Ok(client) => break client,
            Err(error) => {
                assert!(
                    time::Instant::now() < client_deadline,
                    "P6 bootstrap client did not converge: {error}"
                );
                time::sleep(Duration::from_millis(40)).await;
            }
        }
    };
    (
        root,
        controllers,
        client_addresses,
        raft_addresses,
        configs,
        brokers,
        topics,
        client,
    )
}

fn member(value: u8) -> MemberId {
    MemberId::new([value; 16]).unwrap()
}

fn test_partition(partition: u32) -> TopicPartitionV0 {
    TopicPartitionV0 {
        topic_id: TopicId::new([0x44; 16]),
        partition,
    }
}

fn broker_config(
    root: &Path,
    broker_id: u32,
    client_addr: SocketAddr,
    raft_addr: SocketAddr,
    controllers: &[astralog_controller::ControllerClient],
) -> BrokerConfig {
    let mut config = BrokerConfig::new(root.join(format!("broker-{broker_id}")), client_addr);
    config.advertised_host = client_addr.ip().to_string();
    config.advertised_port = client_addr.port();
    config.broker_id = broker_id;
    let mut replication = DynamicReplicationConfig::new(raft_addr);
    // P6-C creates 128 real RF3 internal partitions. Use the production-default
    // Raft tick instead of the accelerated small-P5 harness tick so qualification
    // does not manufacture a synchronized multi-group election/heartbeat storm.
    replication.tick_interval = Duration::from_millis(100);
    let election_min = 8 + u64::from(broker_id - 1) * 4;
    replication.election_timeout_min_ticks = election_min;
    replication.election_timeout_max_ticks = election_min + 1;
    let mut dynamic = DynamicControllerConfig::new(controllers.to_vec(), replication);
    dynamic.metadata_refresh_interval = Duration::from_millis(100);
    dynamic.preferred_leader_timeout = Duration::from_secs(10);
    dynamic.max_pending_admin_operations = 16;
    config.dynamic_controller = Some(dynamic);
    config
}

async fn read_raw_frame(stream: &mut TcpStream) -> WireFrame {
    let mut prefix = [0_u8; 4];
    stream.read_exact(&mut prefix).await.unwrap();
    let declared = usize::try_from(u32::from_be_bytes(prefix)).unwrap();
    assert!(declared <= MAX_WIRE_FRAME_LEN_V1);
    let mut encoded = vec![0_u8; declared + 4];
    encoded[..4].copy_from_slice(&prefix);
    stream.read_exact(&mut encoded[4..]).await.unwrap();
    decode_frame(&encoded, MAX_WIRE_FRAME_LEN_V1).unwrap()
}

async fn raw_public_request(endpoint: SocketAddr, api: ApiKey, body: Vec<u8>) -> WireFrame {
    let mut stream = TcpStream::connect(endpoint).await.unwrap();
    stream
        .write_all(&ClientHello::v1().encode().unwrap())
        .await
        .unwrap();
    let mut hello = [0_u8; 16];
    stream.read_exact(&mut hello).await.unwrap();
    assert_eq!(
        ServerHello::decode(&hello).unwrap().status,
        astralog_protocol::HelloStatus::Ok
    );

    let versions = WireFrame {
        kind: FrameKind::Request,
        flags: 0,
        api_key: ApiKey::ApiVersions as u16,
        api_version: 0,
        error_code: 0,
        correlation_id: 1,
        timeout_ms: 5_000,
        throttle_ms: 0,
        header_tags: Vec::new(),
        body: ApiVersionsRequestV0 {
            client_software_name: "p6j-test".into(),
            client_software_version: "1".into(),
            tags: Vec::new(),
        }
        .encode_body()
        .unwrap(),
    };
    stream
        .write_all(&encode_frame(&versions, MAX_WIRE_FRAME_LEN_V1).unwrap())
        .await
        .unwrap();
    let versions_response = read_raw_frame(&mut stream).await;
    assert_eq!(versions_response.error_code, PublicError::None as u16);

    let request = WireFrame {
        kind: FrameKind::Request,
        flags: 0,
        api_key: api as u16,
        api_version: 0,
        error_code: 0,
        correlation_id: 2,
        timeout_ms: 5_000,
        throttle_ms: 0,
        header_tags: Vec::new(),
        body,
    };
    stream
        .write_all(&encode_frame(&request, MAX_WIRE_FRAME_LEN_V1).unwrap())
        .await
        .unwrap();
    read_raw_frame(&mut stream).await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 16)]
async fn p6_public_broker_client_integration_routes_groups_and_offsets_end_to_end() {
    let _test_guard = P6_E2E_TEST_LOCK.lock().await;
    let (root, controllers, _client_addresses, _raft_addresses, _configs, brokers, topics, client) =
        start_p6_cluster("public-integration").await;

    let leader = controller_leader(&controllers).await;
    let topic_id = controllers[&leader]
        .create_topic(CreateTopicSpec {
            topic_name: "p6j-orders".into(),
            partition_count: 2,
            replication_factor: 3,
            failure_domain_policy: FailureDomainPolicy::Preferred,
        })
        .await
        .unwrap()
        .topic
        .topic_id;
    let topic_deadline = time::Instant::now() + Duration::from_secs(30);
    loop {
        if let Ok(topic) = client.describe_topic("p6j-orders").await
            && topic.topic_id == topic_id
            && topic.partition_count == 2
        {
            break;
        }
        assert!(
            time::Instant::now() < topic_deadline,
            "P6-J setup topic did not converge"
        );
        time::sleep(Duration::from_millis(50)).await;
    }
    let group_id = GroupId::new("p6j-public").unwrap();
    let first_join = JoinSubscribeRequestV0 {
        group_id: group_id.clone(),
        member_id: [0; 16],
        instance_id: None,
        expected_group_epoch: 0,
        expected_member_epoch: 0,
        session_timeout_ms: 30_000,
        subscriptions: vec![topic_id],
        tags: Vec::new(),
    };

    // P6 public keys are allocated but must remain unsupported until the
    // feature gate is committed and both internal topics are serving exactly.
    let pre_activation_route = resolve(&client, &brokers, &group_id, "pre-activation route").await;
    let raw_pre_activation = raw_public_request(
        brokers[0].as_ref().unwrap().local_addr(),
        ApiKey::JoinSubscribe,
        first_join.encode_body().unwrap(),
    )
    .await;
    assert_eq!(
        raw_pre_activation.error_code,
        PublicError::UnsupportedApiKey as u16
    );
    assert!(raw_pre_activation.body.is_empty());
    let pre_activation_join = client.join_subscribe(first_join.clone()).await;
    assert!(
        matches!(
            pre_activation_join,
            Err(ClientError::UnsupportedApi(ApiKey::JoinSubscribe))
                | Err(ClientError::Broker(PublicError::UnsupportedApiKey))
        ),
        "pre-activation JoinSubscribe did not preserve unsupported behavior: {pre_activation_join:?}"
    );
    let pre_activation_index =
        usize::try_from(pre_activation_route.coordinator_broker_id - 1).unwrap();
    assert!(
        brokers[pre_activation_index]
            .as_ref()
            .unwrap()
            .recover_group_state(group_id.clone(), pre_activation_route.coordinator_epoch)
            .await
            .unwrap()
            .is_none(),
        "unsupported pre-activation JoinSubscribe must not mutate group history"
    );

    mutate(
        &controllers,
        ControllerCommand::SetFeatureLevel {
            expected_feature_level: 1,
            new_feature_level: 2,
        },
    )
    .await;

    // Existing clients/connections intentionally predate feature activation.
    // Wait for readiness using only read-only coordinator authority + ApiVersions
    // observations, then issue the first mutation exactly once. This preserves
    // the frozen P6 rule that an ambiguous mutation must not be replayed blindly.
    let readiness_deadline = time::Instant::now() + Duration::from_secs(60);
    loop {
        let route = resolve(
            &client,
            &brokers,
            &group_id,
            "post-activation public readiness",
        )
        .await;
        let index = usize::try_from(route.coordinator_broker_id - 1).unwrap();
        let versions_frame = raw_public_request(
            brokers[index].as_ref().unwrap().local_addr(),
            ApiKey::ApiVersions,
            ApiVersionsRequestV0 {
                client_software_name: "p6j-readiness".into(),
                client_software_version: "1".into(),
                tags: Vec::new(),
            }
            .encode_body()
            .unwrap(),
        )
        .await;
        if versions_frame.error_code == PublicError::None as u16 {
            let versions = ApiVersionsResponseV0::decode_body(&versions_frame.body).unwrap();
            if versions
                .apis
                .iter()
                .any(|api| api.api_key == ApiKey::JoinSubscribe as u16 && api.min_api_version == 0)
            {
                break;
            }
        }
        assert!(
            time::Instant::now() < readiness_deadline,
            "P6 public readiness did not activate after feature level 2"
        );
        time::sleep(Duration::from_millis(50)).await;
    }
    let joined = client
        .join_subscribe(first_join.clone())
        .await
        .expect("first public JoinSubscribe failed after read-only readiness convergence");
    assert_eq!(joined.hint.current_group_epoch, 1);
    assert_eq!(joined.hint.current_member_epoch, 1);
    assert_eq!(joined.group_phase, GroupPhase::Stable as u8);
    assert_eq!(joined.active_assignments.len(), 2);
    assert!(joined.revocations.is_empty());
    let member1 = MemberId::new(joined.member_id).unwrap();
    let member1_state = client.group_member_state(&group_id).await.unwrap();
    assert_eq!(member1_state.authority.member_id, member1);
    assert_eq!(member1_state.authority.group_epoch.get(), 1);

    let group_route = resolve(&client, &brokers, &group_id, "public group route").await;
    let coordinator_index = usize::try_from(group_route.coordinator_broker_id - 1).unwrap();
    let before_wrong_broker = brokers[coordinator_index]
        .as_ref()
        .unwrap()
        .recover_group_state(group_id.clone(), group_route.coordinator_epoch)
        .await
        .unwrap()
        .unwrap();
    let non_coordinator = brokers
        .iter()
        .flatten()
        .find(|broker| {
            broker.local_addr() != brokers[coordinator_index].as_ref().unwrap().local_addr()
        })
        .unwrap();
    let wrong_broker = raw_public_request(
        non_coordinator.local_addr(),
        ApiKey::JoinSubscribe,
        JoinSubscribeRequestV0 {
            expected_group_epoch: 1,
            ..first_join.clone()
        }
        .encode_body()
        .unwrap(),
    )
    .await;
    assert_eq!(wrong_broker.error_code, PublicError::NotLeader as u16);
    let wrong_broker_body = JoinSubscribeResponseV0::decode_body(&wrong_broker.body).unwrap();
    assert_eq!(
        wrong_broker_body.hint.coordinator_broker_id,
        group_route.coordinator_broker_id
    );
    assert_eq!(
        wrong_broker_body.hint.coordinator_epoch,
        group_route.coordinator_epoch
    );
    let after_wrong_broker = brokers[coordinator_index]
        .as_ref()
        .unwrap()
        .recover_group_state(group_id.clone(), group_route.coordinator_epoch)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(before_wrong_broker, after_wrong_broker);

    let heartbeat = client.heartbeat_group(&group_id).await.unwrap();
    assert_eq!(heartbeat.hint.current_group_epoch, 1);
    assert_eq!(heartbeat.active_assignments, joined.active_assignments);

    // Force the offsets shard to a broker different from the group
    // coordinator. Public CommitOffsets must still be sent only to the group
    // coordinator and reach this remote offsets leader through the P6-J
    // broker-internal hop.
    let initial_offsets = observe_offsets_leader(
        &client,
        &brokers,
        topics,
        &group_id,
        None,
        "public offsets route",
    )
    .await;
    let placement = controllers[&controller_leader(&controllers).await]
        .metadata_snapshot()
        .await
        .unwrap();
    let offsets_voters =
        &placement.state.assignments()[&(topics.offsets_topic_id, initial_offsets.shard)].voters;
    let desired_offsets_leader = *offsets_voters
        .iter()
        .find(|broker_id| **broker_id != group_route.coordinator_broker_id)
        .unwrap();
    let offsets_route = if initial_offsets.broker_id == desired_offsets_leader {
        initial_offsets
    } else {
        let initial_index = usize::try_from(initial_offsets.broker_id - 1).unwrap();
        let transfer_deadline = time::Instant::now() + Duration::from_secs(30);
        loop {
            match brokers[initial_index]
                .as_ref()
                .unwrap()
                .request_preferred_leader(PreferredLeaderRequest {
                    topic_id: topics.offsets_topic_id,
                    partition_id: initial_offsets.shard,
                    target_broker_id: desired_offsets_leader,
                })
                .await
            {
                Ok(_) => break,
                Err(BrokerError::InvalidConfig(
                    "preferred-leader transfer result was not observed before bounded timeout",
                )) => break,
                Err(error) => {
                    assert!(
                        time::Instant::now() < transfer_deadline,
                        "offsets preferred-leader transfer failed: {error}"
                    );
                    time::sleep(Duration::from_millis(50)).await;
                }
            }
        }
        observe_offsets_leader(
            &client,
            &brokers,
            topics,
            &group_id,
            Some((initial_offsets.broker_id, initial_offsets.epoch)),
            "public remote offsets leader",
        )
        .await
    };
    assert_eq!(offsets_route.broker_id, desired_offsets_leader);
    assert_ne!(offsets_route.broker_id, group_route.coordinator_broker_id);

    let first_partition = joined.active_assignments[0];
    let committed = client
        .commit_group_offsets(
            &group_id,
            vec![CommitOffsetV0 {
                topic_id: first_partition.topic_id,
                partition: first_partition.partition,
                next_offset: 42,
                metadata: "p6j-public".into(),
            }],
        )
        .await
        .unwrap();
    assert!(committed.offsets_commit_index > 0);
    let fetched = client
        .fetch_group_offsets(&group_id, vec![first_partition])
        .await
        .unwrap();
    let fetched_value = fetched[0].value.as_ref().unwrap();
    assert_eq!(fetched_value.next_offset, 42);
    assert_eq!(fetched_value.metadata, "p6j-public");
    assert_eq!(fetched_value.group_epoch.get(), 1);
    assert_eq!(fetched_value.member_id, member1);
    assert_eq!(fetched_value.member_epoch.get(), 1);

    // A second independent client joins the same group. The first client must
    // refresh its stale GroupEpoch through the committed error hint, continue
    // processing the unaffected partition, optionally commit the revoked one,
    // then acknowledge the exact cooperative revocation set.
    let mut client2_config =
        ClientConfig::new(brokers[0].as_ref().unwrap().local_addr().to_string());
    // This is the second client in a long RF3 integration scenario. Keep the
    // transport/API-version setup deadline above the generic five-second
    // default so host load cannot fail the fixture before the second group
    // mutation is even issued. JoinSubscribe itself retains the same frozen
    // ambiguity/retry semantics.
    client2_config.request_timeout = Duration::from_secs(20);
    let client2 = AstraClient::connect(client2_config).await.unwrap();
    let joined2 = client2
        .join_subscribe(JoinSubscribeRequestV0 {
            group_id: group_id.clone(),
            member_id: [0; 16],
            instance_id: None,
            expected_group_epoch: 1,
            expected_member_epoch: 0,
            session_timeout_ms: 30_000,
            subscriptions: vec![topic_id],
            tags: Vec::new(),
        })
        .await
        .unwrap();
    assert_eq!(joined2.hint.current_group_epoch, 2);
    assert_eq!(joined2.group_phase, GroupPhase::Revoking as u8);

    let first_after_join = client.heartbeat_group(&group_id).await.unwrap();
    assert_eq!(first_after_join.hint.current_group_epoch, 2);
    assert_eq!(first_after_join.revocations.len(), 1);
    let plan = client.cooperative_revoke_plan(&group_id).await.unwrap();
    assert_eq!(plan.revoke, first_after_join.revocations);
    assert_eq!(plan.keep_processing.len(), 1);
    assert!(
        plan.keep_processing
            .iter()
            .all(|partition| !plan.revoke.contains(partition))
    );
    let revoked_partition = plan.revoke[0];
    let acked = client
        .complete_cooperative_revoke(
            &group_id,
            plan.revoke.clone(),
            Some(vec![CommitOffsetV0 {
                topic_id: revoked_partition.topic_id,
                partition: revoked_partition.partition,
                next_offset: 77,
                metadata: "before-revoke".into(),
            }]),
        )
        .await
        .unwrap();
    assert_eq!(acked.group_phase, GroupPhase::Stable as u8);
    assert!(acked.revocations.is_empty());
    assert_eq!(acked.active_assignments, plan.keep_processing);

    let second_after_handoff = client2.heartbeat_group(&group_id).await.unwrap();
    assert_eq!(second_after_handoff.group_phase, GroupPhase::Stable as u8);
    assert_eq!(second_after_handoff.active_assignments.len(), 1);
    assert!(
        second_after_handoff
            .active_assignments
            .contains(&revoked_partition)
    );
    let handoff_value = client2
        .fetch_group_offsets(&group_id, vec![revoked_partition])
        .await
        .unwrap();
    assert_eq!(handoff_value[0].value.as_ref().unwrap().next_offset, 77);

    // Leave uses the same exact coordinator authority path. The departing
    // member is removed durably, the remaining member regains the released
    // partition only through the normal group/owner-fence workflow, and the
    // client drops the departed local authority after success.
    let left = client2.leave_group(&group_id).await.unwrap();
    assert_eq!(left.hint.current_group_epoch, 3);
    assert_eq!(left.hint.current_member_epoch, 0);
    assert!(client2.group_member_state(&group_id).await.is_none());
    let first_after_leave = client.heartbeat_group(&group_id).await.unwrap();
    assert_eq!(first_after_leave.hint.current_group_epoch, 3);
    assert_eq!(first_after_leave.group_phase, GroupPhase::Stable as u8);
    assert_eq!(first_after_leave.active_assignments.len(), 2);
    assert!(first_after_leave.revocations.is_empty());

    client2.close().await;
    client.close().await;
    for broker in brokers.into_iter().flatten() {
        broker.shutdown().await.unwrap();
    }
    for (_, controller) in controllers {
        controller.shutdown().await;
    }
    fs::remove_dir_all(root).unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 16)]
async fn p6_churn_timeout_static_and_partition_expansion_are_authoritative() {
    let _test_guard = P6_E2E_TEST_LOCK.lock().await;
    let (
        root,
        controllers,
        _client_addresses,
        _raft_addresses,
        _configs,
        brokers,
        topics,
        bootstrap_client,
    ) = start_p6_cluster("churn-timeout-expansion").await;
    bootstrap_client.close().await;
    let mut churn_client_config =
        ClientConfig::new(brokers[0].as_ref().unwrap().local_addr().to_string());
    // This scenario deliberately layers sustained group/offset churn, member
    // timeout work, static replacement and partition expansion into one long
    // RF3 run. Give ordinary request/response delivery enough headroom under
    // whole-workspace serialization so a committed first dynamic join is not
    // misclassified as an application-level ambiguous mutation solely because
    // the host is busy. The mutation remains single-shot and the client still
    // refuses blind retries if the longer transport deadline is genuinely lost.
    churn_client_config.request_timeout = Duration::from_secs(20);
    let client = AstraClient::connect(churn_client_config).await.unwrap();

    let leader = controller_leader(&controllers).await;
    let topic_id = controllers[&leader]
        .create_topic(CreateTopicSpec {
            topic_name: "p6k-orders".into(),
            partition_count: 2,
            replication_factor: 3,
            failure_domain_policy: FailureDomainPolicy::Preferred,
        })
        .await
        .unwrap()
        .topic
        .topic_id;
    mutate(
        &controllers,
        ControllerCommand::SetFeatureLevel {
            expected_feature_level: 1,
            new_feature_level: 2,
        },
    )
    .await;

    let group_id = GroupId::new("p6k-load").unwrap();
    resolve(
        &client,
        &brokers,
        &group_id,
        "P6-K first join group-coordinator readiness",
    )
    .await;
    observe_offsets_leader(
        &client,
        &brokers,
        topics,
        &group_id,
        None,
        "P6-K first join offsets-shard readiness",
    )
    .await;
    let first_request = JoinSubscribeRequestV0 {
        group_id: group_id.clone(),
        member_id: [0; 16],
        instance_id: None,
        expected_group_epoch: 0,
        expected_member_epoch: 0,
        session_timeout_ms: 30_000,
        subscriptions: vec![topic_id],
        tags: Vec::new(),
    };
    let activation_deadline = time::Instant::now() + Duration::from_secs(60);
    let first = loop {
        match client.join_subscribe(first_request.clone()).await {
            Ok(response) => break response,
            Err(ClientError::UnsupportedApi(ApiKey::JoinSubscribe))
            | Err(ClientError::Broker(PublicError::UnsupportedApiKey)) => {
                assert!(time::Instant::now() < activation_deadline);
                time::sleep(Duration::from_millis(50)).await;
            }
            Err(ClientError::Metadata(_)) => {
                assert!(time::Instant::now() < activation_deadline);
                time::sleep(Duration::from_millis(50)).await;
            }
            Err(error) => panic!("P6-K first join failed: {error}"),
        }
    };
    assert_eq!(first.group_phase, GroupPhase::Stable as u8);
    assert_eq!(first.active_assignments.len(), 2);

    let mut second_config =
        ClientConfig::new(brokers[1].as_ref().unwrap().local_addr().to_string());
    second_config.request_timeout = Duration::from_secs(20);
    let second = AstraClient::connect(second_config).await.unwrap();

    // P6-139: keep committed Fetch/CommitOffsets traffic flowing while a
    // second member joins. During REVOKING the old owner may continue only on
    // assignments that remain active; the new owner must not become active
    // until release + owner fence complete.
    let load_client = client.clone();
    let load_group = group_id.clone();
    let load_task = tokio::spawn(async move {
        let mut successful_rounds = 0_u32;
        for sequence in 0..20_u64 {
            if let Ok(heartbeat) = load_client.heartbeat_group(&load_group).await {
                let revoking = heartbeat
                    .revocations
                    .iter()
                    .copied()
                    .collect::<std::collections::BTreeSet<_>>();
                if let Some(partition) = heartbeat
                    .active_assignments
                    .iter()
                    .copied()
                    .find(|partition| !revoking.contains(partition))
                {
                    match load_client
                        .commit_group_offsets(
                            &load_group,
                            vec![CommitOffsetV0 {
                                topic_id: partition.topic_id,
                                partition: partition.partition,
                                next_offset: 1_000 + sequence,
                                metadata: "join-load".into(),
                            }],
                        )
                        .await
                    {
                        Ok(_) => {
                            load_client
                                .fetch_group_offsets(&load_group, vec![partition])
                                .await
                                .unwrap();
                            successful_rounds += 1;
                        }
                        Err(ClientError::Broker(
                            PublicError::GroupEpochStale
                            | PublicError::RebalanceInProgress
                            | PublicError::InvalidOffsetCommit,
                        )) => {}
                        Err(error) => panic!("P6-K join-load offset path failed: {error}"),
                    }
                }
            }
            time::sleep(Duration::from_millis(25)).await;
        }
        successful_rounds
    });
    time::sleep(Duration::from_millis(75)).await;
    let second_join = second
        .join_subscribe(JoinSubscribeRequestV0 {
            group_id: group_id.clone(),
            member_id: [0; 16],
            instance_id: None,
            expected_group_epoch: 1,
            expected_member_epoch: 0,
            session_timeout_ms: 30_000,
            subscriptions: vec![topic_id],
            tags: Vec::new(),
        })
        .await
        .unwrap();
    assert_eq!(second_join.hint.current_group_epoch, 2);
    assert_eq!(second_join.group_phase, GroupPhase::Revoking as u8);
    let first_during_join = client.heartbeat_group(&group_id).await.unwrap();
    assert_eq!(first_during_join.hint.current_group_epoch, 2);
    assert!(
        first_during_join
            .active_assignments
            .iter()
            .all(|partition| !second_join.active_assignments.contains(partition)),
        "new member became active before old ownership was released"
    );
    let load_rounds = load_task.await.unwrap();
    assert!(
        load_rounds > 0,
        "sustained join load made no forward progress"
    );

    let revoke_plan = client.cooperative_revoke_plan(&group_id).await.unwrap();
    assert_eq!(revoke_plan.revoke.len(), 1);
    assert_eq!(revoke_plan.keep_processing.len(), 1);
    client
        .complete_cooperative_revoke(&group_id, revoke_plan.revoke.clone(), None)
        .await
        .unwrap();
    let first_stable = client.heartbeat_group(&group_id).await.unwrap();
    let second_stable = second.heartbeat_group(&group_id).await.unwrap();
    assert_eq!(first_stable.group_phase, GroupPhase::Stable as u8);
    assert_eq!(second_stable.group_phase, GroupPhase::Stable as u8);
    assert_eq!(first_stable.active_assignments.len(), 1);
    assert_eq!(second_stable.active_assignments.len(), 1);
    assert_ne!(
        first_stable.active_assignments,
        second_stable.active_assignments
    );

    // P6-140: Leave while the surviving member is still committing/fetching.
    let leave_load_client = client.clone();
    let leave_load_group = group_id.clone();
    let leave_load = tokio::spawn(async move {
        let partition = leave_load_client
            .heartbeat_group(&leave_load_group)
            .await
            .unwrap()
            .active_assignments[0];
        let mut successes = 0_u32;
        for sequence in 0..12_u64 {
            match leave_load_client
                .commit_group_offsets(
                    &leave_load_group,
                    vec![CommitOffsetV0 {
                        topic_id: partition.topic_id,
                        partition: partition.partition,
                        next_offset: 2_000 + sequence,
                        metadata: "leave-load".into(),
                    }],
                )
                .await
            {
                Ok(_) => {
                    let _ = leave_load_client
                        .fetch_group_offsets(&leave_load_group, vec![partition])
                        .await
                        .unwrap();
                    successes += 1;
                }
                Err(ClientError::Broker(
                    PublicError::GroupEpochStale
                    | PublicError::RebalanceInProgress
                    | PublicError::InvalidOffsetCommit,
                )) => {
                    let _ = leave_load_client.heartbeat_group(&leave_load_group).await;
                }
                Err(error) => panic!("P6-K leave-load offset path failed: {error}"),
            }
            time::sleep(Duration::from_millis(20)).await;
        }
        successes
    });
    time::sleep(Duration::from_millis(40)).await;
    let leave = second.leave_group(&group_id).await.unwrap();
    assert_eq!(leave.hint.current_group_epoch, 3);
    assert!(leave_load.await.unwrap() > 0);
    let after_leave = client.heartbeat_group(&group_id).await.unwrap();
    assert_eq!(after_leave.group_phase, GroupPhase::Stable as u8);
    assert_eq!(after_leave.active_assignments.len(), 2);

    // P6-141/P6-142: a 6s member that heartbeats just before its deadline
    // remains authoritative. Once that member stops heartbeating without Leave,
    // the maintenance loop durably removes it and fences the stale session.
    // This is an independent consumer session, not a broker-failure probe.
    // Bootstrap it through the already-proven first broker and keep connection
    // setup outside the 6s consumer-session timing assertion below.
    let mut crash_config = ClientConfig::new(brokers[0].as_ref().unwrap().local_addr().to_string());
    crash_config.request_timeout = Duration::from_secs(20);
    let crashed = AstraClient::connect(crash_config).await.unwrap();
    let crash_join = crashed
        .join_subscribe(JoinSubscribeRequestV0 {
            group_id: group_id.clone(),
            member_id: [0; 16],
            instance_id: None,
            expected_group_epoch: 3,
            expected_member_epoch: 0,
            session_timeout_ms: 6_000,
            subscriptions: vec![topic_id],
            tags: Vec::new(),
        })
        .await
        .unwrap();
    assert_eq!(crash_join.hint.current_group_epoch, 4);
    let first_revoke = client.heartbeat_group(&group_id).await.unwrap();
    let crash_revoke = client.cooperative_revoke_plan(&group_id).await.unwrap();
    assert_eq!(first_revoke.revocations, crash_revoke.revoke);
    assert!(!crash_revoke.revoke.is_empty());
    match client
        .complete_cooperative_revoke(&group_id, crash_revoke.revoke.clone(), None)
        .await
    {
        Ok(acked) => {
            assert_eq!(acked.hint.current_group_epoch, 4);
            assert_eq!(acked.group_phase, GroupPhase::Stable as u8);
        }
        Err(ClientError::AmbiguousMutation {
            api: ApiKey::RevokeAck,
        }) => {
            // A lost RevokeAck response does not license a second mutation.
            // The only safe recovery oracle is the exact committed group
            // transition: epoch 4 is stable, the original revocations are
            // gone, and the intended new member owns precisely those
            // partitions. If this cannot be proven, keep the test RED.
            let deadline = time::Instant::now() + Duration::from_secs(30);
            loop {
                let route = observe_current_coordinator(
                    &brokers,
                    &group_id,
                    None,
                    "P6-K ambiguous crash-member RevokeAck authority",
                )
                .await;
                let index = usize::try_from(route.hint.coordinator_broker_id - 1).unwrap();
                if let Ok(Some(group)) = brokers[index]
                    .as_ref()
                    .unwrap()
                    .recover_group_state(group_id.clone(), route.hint.coordinator_epoch)
                    .await
                    && group.active_group_epoch == 4
                    && group.phase == GroupPhase::Stable
                    && group.pending_group_epoch.is_none()
                    && group.pending_revocations.is_empty()
                    && group.members.len() == 2
                    && group.active_assignments.len() == 2
                    && group.active_assignments.iter().all(|(partition, owner)| {
                        if crash_revoke.revoke.contains(partition) {
                            owner.as_bytes() == crash_join.member_id
                        } else {
                            owner.as_bytes() == first.member_id
                        }
                    })
                    && crash_revoke.revoke.iter().all(|partition| {
                        group
                            .active_assignments
                            .get(partition)
                            .is_some_and(|owner| owner.as_bytes() == crash_join.member_id)
                    })
                {
                    break;
                }
                assert!(
                    time::Instant::now() < deadline,
                    "ambiguous RevokeAck was not proven committed at exact stable group epoch 4; refusing any mutation retry"
                );
                time::sleep(Duration::from_millis(40)).await;
            }
        }
        Err(error) => panic!("P6-K cooperative RevokeAck failed: {error}"),
    }
    let crash_stable = crashed.heartbeat_group(&group_id).await.unwrap();
    assert_eq!(crash_stable.group_phase, GroupPhase::Stable as u8);
    assert_eq!(crash_stable.hint.current_group_epoch, 4);

    time::sleep(Duration::from_millis(5_300)).await;
    let slow_heartbeat = crashed.heartbeat_group(&group_id).await.unwrap();
    assert_eq!(slow_heartbeat.hint.current_group_epoch, 4);
    assert_eq!(slow_heartbeat.group_phase, GroupPhase::Stable as u8);

    let timeout_deadline = time::Instant::now() + Duration::from_secs(12);
    let after_timeout = loop {
        match client.heartbeat_group(&group_id).await {
            Ok(response)
                if response.hint.current_group_epoch >= 5
                    && response.group_phase == GroupPhase::Stable as u8
                    && response.active_assignments.len() == 2 =>
            {
                break response;
            }
            Ok(_) | Err(ClientError::Broker(PublicError::GroupEpochStale)) => {}
            Err(error) => panic!("surviving member failed during crash timeout: {error}"),
        }
        assert!(
            time::Instant::now() < timeout_deadline,
            "crashed member was not durably removed after session timeout"
        );
        time::sleep(Duration::from_millis(100)).await;
    };
    assert_eq!(after_timeout.hint.current_group_epoch, 5);
    let stale_after_timeout = crashed.heartbeat_group(&group_id).await.unwrap_err();
    assert!(matches!(
        stale_after_timeout,
        ClientError::Broker(PublicError::GroupEpochStale | PublicError::UnknownMember)
    ));

    // P6-151: P5 expands the subscribed topic. With no membership change, P6
    // maintenance must prepare a strictly higher GroupEpoch, preserve all valid
    // existing ownership, fence the two new partitions, and converge to STABLE.
    wait_healthy(&controllers, &[1, 2, 3, 4], 2_000).await;
    let expand_leader = controller_leader(&controllers).await;
    controllers[&expand_leader]
        .expand_topic(TopicExpansionSpec {
            topic_id,
            expected_partition_count: 2,
            new_partition_count: 4,
        })
        .await
        .unwrap();
    let metadata_deadline = time::Instant::now() + Duration::from_secs(30);
    loop {
        if let Ok(topic) = client.describe_topic("p6k-orders").await
            && topic.partition_count == 4
        {
            break;
        }
        assert!(
            time::Instant::now() < metadata_deadline,
            "expanded P5 topic metadata did not reach the client"
        );
        time::sleep(Duration::from_millis(100)).await;
    }
    let pre_expand = after_timeout.active_assignments.clone();
    let expansion_deadline = time::Instant::now() + Duration::from_secs(30);
    let expanded = loop {
        // The group is explicitly active during this qualification. Keep the
        // surviving member heartbeating while P5 metadata converges so a
        // coordinator leadership change gets the frozen P6-G full-grace
        // observation before its local maintenance loop resumes the group.
        // A transient stale epoch is expected while the higher epoch commits.
        match client.heartbeat_group(&group_id).await {
            Ok(_)
            | Err(ClientError::Broker(
                PublicError::GroupEpochStale
                | PublicError::NotLeader
                | PublicError::BrokerNotAvailable,
            ))
            | Err(ClientError::Metadata(_)) => {}
            Err(error) => panic!("active member heartbeat failed during expansion: {error}"),
        }
        let route = observe_current_coordinator(
            &brokers,
            &group_id,
            None,
            "partition expansion convergence",
        )
        .await;
        let index = usize::try_from(route.hint.coordinator_broker_id - 1).unwrap();
        if let Ok(Some(group)) = brokers[index]
            .as_ref()
            .unwrap()
            .recover_group_state(group_id.clone(), route.hint.coordinator_epoch)
            .await
            && group.active_group_epoch >= 6
            && group.phase == GroupPhase::Stable
            && group.active_assignments.len() == 4
        {
            break group;
        }
        assert!(
            time::Instant::now() < expansion_deadline,
            "P6 did not reconcile committed topic expansion"
        );
        time::sleep(Duration::from_millis(100)).await;
    };
    assert_eq!(expanded.active_group_epoch, 6);
    assert!(
        pre_expand
            .iter()
            .all(|partition| expanded.active_assignments.contains_key(partition)),
        "expansion moved still-valid single-member ownership"
    );

    // P6-150: two concurrent static replacements race using the same old
    // MemberEpoch. Exactly one replacement may commit; the final durable member
    // epoch advances once and the original session is fenced.
    let static_group = GroupId::new("p6k-static-race").unwrap();
    let mut static_bootstrap_config =
        ClientConfig::new(brokers[0].as_ref().unwrap().local_addr().to_string());
    static_bootstrap_config.request_timeout = Duration::from_secs(20);
    let static_old_deadline = time::Instant::now() + Duration::from_secs(30);
    let static_old = loop {
        match AstraClient::connect(static_bootstrap_config.clone()).await {
            Ok(client) => break client,
            Err(error) => {
                assert!(
                    time::Instant::now() < static_old_deadline,
                    "static-race bootstrap client did not converge: {error}"
                );
                time::sleep(Duration::from_millis(40)).await;
            }
        }
    };
    let static_join_request = JoinSubscribeRequestV0 {
        group_id: static_group.clone(),
        member_id: [0; 16],
        instance_id: Some("static-worker".into()),
        expected_group_epoch: 0,
        expected_member_epoch: 0,
        session_timeout_ms: 30_000,
        subscriptions: vec![topic_id],
        tags: Vec::new(),
    };
    let static_member_id = match static_old.join_subscribe(static_join_request).await {
        Ok(response) => {
            assert_eq!(response.hint.current_group_epoch, 1);
            assert_eq!(response.hint.current_member_epoch, 1);
            response.member_id
        }
        Err(ClientError::AmbiguousMutation {
            api: ApiKey::JoinSubscribe,
        }) => {
            // JoinSubscribe is not blindly retryable after a lost response.
            // Resolve the ambiguity only from committed group history: this
            // static instance id is unique by contract, so exactly one stable
            // epoch-1 member proves that the original mutation committed.
            let deadline = time::Instant::now() + Duration::from_secs(60);
            loop {
                let route = observe_current_coordinator(
                    &brokers,
                    &static_group,
                    None,
                    "static-race ambiguous bootstrap join",
                )
                .await;
                let index = usize::try_from(route.hint.coordinator_broker_id - 1).unwrap();
                if let Ok(Some(group)) = brokers[index]
                    .as_ref()
                    .unwrap()
                    .recover_group_state(static_group.clone(), route.hint.coordinator_epoch)
                    .await
                {
                    let matching = group
                        .members
                        .values()
                        .filter(|member| member.instance_id.as_deref() == Some("static-worker"))
                        .collect::<Vec<_>>();
                    if group.active_group_epoch == 1
                        && group.phase == GroupPhase::Stable
                        && matching.len() == 1
                        && matching[0].member_epoch.get() == 1
                    {
                        break matching[0].member_id.as_bytes();
                    }
                }
                assert!(
                    time::Instant::now() < deadline,
                    "ambiguous static bootstrap JoinSubscribe did not resolve from committed history"
                );
                time::sleep(Duration::from_millis(40)).await;
            }
        }
        Err(error) => panic!("static bootstrap JoinSubscribe failed terminally: {error}"),
    };
    let replacement_request = JoinSubscribeRequestV0 {
        group_id: static_group.clone(),
        member_id: static_member_id,
        instance_id: Some("static-worker".into()),
        expected_group_epoch: 1,
        expected_member_epoch: 1,
        session_timeout_ms: 30_000,
        subscriptions: vec![topic_id],
        tags: Vec::new(),
    };
    let mut race_a_config =
        ClientConfig::new(brokers[1].as_ref().unwrap().local_addr().to_string());
    race_a_config.request_timeout = Duration::from_secs(20);
    let race_a_deadline = time::Instant::now() + Duration::from_secs(30);
    let race_a = loop {
        match AstraClient::connect(race_a_config.clone()).await {
            Ok(client) => break client,
            Err(error) => {
                assert!(
                    time::Instant::now() < race_a_deadline,
                    "static-race client A did not converge: {error}"
                );
                time::sleep(Duration::from_millis(40)).await;
            }
        }
    };
    let mut race_b_config =
        ClientConfig::new(brokers[2].as_ref().unwrap().local_addr().to_string());
    race_b_config.request_timeout = Duration::from_secs(20);
    let race_b_deadline = time::Instant::now() + Duration::from_secs(30);
    let race_b = loop {
        match AstraClient::connect(race_b_config.clone()).await {
            Ok(client) => break client,
            Err(error) => {
                assert!(
                    time::Instant::now() < race_b_deadline,
                    "static-race client B did not converge: {error}"
                );
                time::sleep(Duration::from_millis(40)).await;
            }
        }
    };
    let (replace_a, replace_b) = tokio::join!(
        race_a.join_subscribe(replacement_request.clone()),
        race_b.join_subscribe(replacement_request)
    );
    let successful_replacements = [replace_a.as_ref().ok(), replace_b.as_ref().ok()]
        .into_iter()
        .flatten()
        .collect::<Vec<_>>();
    assert!(!successful_replacements.is_empty());
    for response in &successful_replacements {
        assert_eq!(response.member_id, static_member_id);
        assert_eq!(response.hint.current_group_epoch, 2);
        assert_eq!(response.hint.current_member_epoch, 2);
    }
    if successful_replacements.len() == 2 {
        assert_eq!(successful_replacements[0], successful_replacements[1]);
    }

    let static_route = resolve(&client, &brokers, &static_group, "static race final state").await;
    let static_index = usize::try_from(static_route.coordinator_broker_id - 1).unwrap();
    let static_final = brokers[static_index]
        .as_ref()
        .unwrap()
        .recover_group_state(static_group.clone(), static_route.coordinator_epoch)
        .await
        .unwrap()
        .unwrap();
    let static_member = MemberId::new(static_member_id).unwrap();
    assert_eq!(static_final.active_group_epoch, 2);
    assert_eq!(static_final.members[&static_member].member_epoch.get(), 2);
    assert_eq!(static_final.members.len(), 1);
    let old_static_heartbeat = static_old.heartbeat_group(&static_group).await.unwrap_err();
    assert!(matches!(
        old_static_heartbeat,
        ClientError::Broker(PublicError::GroupEpochStale | PublicError::UnknownMember)
    ));

    race_b.close().await;
    race_a.close().await;
    static_old.close().await;
    crashed.close().await;
    second.close().await;
    client.close().await;
    for broker in brokers.into_iter().flatten() {
        broker.shutdown().await.unwrap();
    }
    for (_, controller) in controllers {
        controller.shutdown().await;
    }
    fs::remove_dir_all(root).unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 16)]
async fn p6_silent_member_gets_fresh_grace_then_times_out_after_coordinator_failover() {
    let _test_guard = P6_E2E_TEST_LOCK.lock().await;
    let (
        root,
        controllers,
        _client_addresses,
        _raft_addresses,
        _configs,
        mut brokers,
        topics,
        bootstrap_client,
    ) = start_p6_cluster("silent-session-failover").await;
    bootstrap_client.close().await;
    let mut silent_client_config =
        ClientConfig::new(brokers[0].as_ref().unwrap().local_addr().to_string());
    // This long RF3 session-timeout/failover scenario can run while Windows is
    // still draining socket/file work from preceding qualification cases. Give
    // the initial dynamic join enough response-delivery headroom that a correctly
    // committed mutation is not classified as ambiguous solely because the host
    // misses the generic five-second client deadline. The mutation remains
    // single-shot; a genuine response loss still returns AmbiguousMutation and is
    // never retried blindly.
    silent_client_config.request_timeout = Duration::from_secs(20);
    let client = AstraClient::connect(silent_client_config).await.unwrap();

    let leader = controller_leader(&controllers).await;
    let topic_id = controllers[&leader]
        .create_topic(CreateTopicSpec {
            topic_name: "p6k-silent-orders".into(),
            partition_count: 1,
            replication_factor: 3,
            failure_domain_policy: FailureDomainPolicy::Preferred,
        })
        .await
        .unwrap()
        .topic
        .topic_id;
    mutate(
        &controllers,
        ControllerCommand::SetFeatureLevel {
            expected_feature_level: 1,
            new_feature_level: 2,
        },
    )
    .await;

    let group_id = GroupId::new("p6k-silent-failover").unwrap();
    resolve(
        &client,
        &brokers,
        &group_id,
        "silent-member initial group-coordinator readiness",
    )
    .await;
    observe_offsets_leader(
        &client,
        &brokers,
        topics,
        &group_id,
        None,
        "silent-member initial offsets-shard readiness",
    )
    .await;
    observe_current_coordinator(
        &brokers,
        &group_id,
        None,
        "silent-member initial group-shard readiness",
    )
    .await;
    let join_request = JoinSubscribeRequestV0 {
        group_id: group_id.clone(),
        member_id: [0; 16],
        instance_id: None,
        expected_group_epoch: 0,
        expected_member_epoch: 0,
        session_timeout_ms: 6_000,
        subscriptions: vec![topic_id],
        tags: Vec::new(),
    };
    let activation_deadline = time::Instant::now() + Duration::from_secs(60);
    loop {
        match client.join_subscribe(join_request.clone()).await {
            Ok(response) => {
                assert_eq!(response.hint.current_group_epoch, 1);
                assert_eq!(response.group_phase, GroupPhase::Stable as u8);
                break;
            }
            Err(ClientError::AmbiguousMutation {
                api: ApiKey::JoinSubscribe,
            }) => {
                // A response timeout cannot distinguish a lost acknowledgment
                // from an uncommitted JoinSubscribe. This test sent exactly
                // one anonymous join to a brand-new group, so a *committed*
                // stable epoch-1 record with exactly that member contract is
                // sufficient evidence of success. Never resend the mutation.
                let recovery_deadline = time::Instant::now() + Duration::from_secs(30);
                loop {
                    let route = observe_current_coordinator(
                        &brokers,
                        &group_id,
                        None,
                        "silent-member ambiguous JoinSubscribe proof",
                    )
                    .await;
                    let index = usize::try_from(route.hint.coordinator_broker_id - 1).unwrap();
                    if let Ok(Some(group)) = brokers[index]
                        .as_ref()
                        .unwrap()
                        .recover_group_state(group_id.clone(), route.hint.coordinator_epoch)
                        .await
                        && group.active_group_epoch == 1
                        && group.phase == GroupPhase::Stable
                        && group.members.len() == 1
                        && group.members.values().all(|member| {
                            member.instance_id.is_none()
                                && member.member_epoch.get() == 1
                                && member.session_timeout_ms == 6_000
                                && member.subscriptions.as_slice() == [topic_id]
                        })
                    {
                        break;
                    }
                    assert!(
                        time::Instant::now() < recovery_deadline,
                        "ambiguous silent-member JoinSubscribe was not proven committed; refusing blind retry"
                    );
                    time::sleep(Duration::from_millis(50)).await;
                }
                break;
            }
            Err(ClientError::UnsupportedApi(ApiKey::JoinSubscribe))
            | Err(ClientError::Broker(PublicError::UnsupportedApiKey))
            | Err(ClientError::Metadata(_)) => {
                assert!(time::Instant::now() < activation_deadline);
                time::sleep(Duration::from_millis(50)).await;
            }
            Err(error) => panic!("silent-member join failed: {error}"),
        }
    }

    let before = observe_current_coordinator(
        &brokers,
        &group_id,
        None,
        "silent-member coordinator before failover",
    )
    .await;
    let failed_id = before.hint.coordinator_broker_id;
    let failed_index = usize::try_from(failed_id - 1).unwrap();
    brokers[failed_index]
        .take()
        .unwrap()
        .shutdown()
        .await
        .unwrap();

    let successor = observe_current_coordinator(
        &brokers,
        &group_id,
        Some((failed_id, before.hint.coordinator_epoch)),
        "silent-member coordinator successor",
    )
    .await;
    time::sleep(Duration::from_secs(2)).await;
    let successor_index = usize::try_from(successor.hint.coordinator_broker_id - 1).unwrap();
    let during_grace = brokers[successor_index]
        .as_ref()
        .unwrap()
        .recover_group_state(group_id.clone(), successor.hint.coordinator_epoch)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(during_grace.active_group_epoch, 1);
    assert_eq!(during_grace.members.len(), 1);

    // The two-second committed-history observation above proves that failover
    // does not reuse the predecessor's expired/near-expired volatile deadline.
    // From here, observe only the authoritative group history. Do not use
    // FetchOffsets as a group-coordinator readiness proxy: that API is served
    // by the separate offset coordinator and can legitimately lag independently.
    let overall_deadline = time::Instant::now() + Duration::from_secs(120);
    let mut current_term = successor.hint.coordinator_epoch;
    // The semantic timeout remains the committed 6s member timeout. This
    // larger test-only observation budget allows bounded coordinator recovery/
    // maintenance retries on a loaded Windows qualification host; success still
    // requires a durable member removal under one unchanged coordinator term.
    let mut stable_term_deadline = time::Instant::now() + Duration::from_secs(30);
    loop {
        let route = observe_current_coordinator(
            &brokers,
            &group_id,
            None,
            "silent-member timeout after coordinator failover",
        )
        .await;
        if route.hint.coordinator_epoch != current_term {
            assert!(route.hint.coordinator_epoch > current_term);
            current_term = route.hint.coordinator_epoch;
            stable_term_deadline = time::Instant::now() + Duration::from_secs(30);
        }
        let index = usize::try_from(route.hint.coordinator_broker_id - 1).unwrap();
        if let Ok(Some(group)) = brokers[index]
            .as_ref()
            .unwrap()
            .recover_group_state(group_id.clone(), route.hint.coordinator_epoch)
            .await
            && group.members.is_empty()
            && group.phase == GroupPhase::Stable
            && group.active_group_epoch >= 2
        {
            break;
        }
        assert!(
            time::Instant::now() < stable_term_deadline,
            "silent member did not time out after a stable coordinator term received full grace"
        );
        assert!(
            time::Instant::now() < overall_deadline,
            "coordinator never remained stable long enough to qualify silent-member timeout"
        );
        time::sleep(Duration::from_millis(100)).await;
    }

    client.close().await;
    for broker in brokers.into_iter().flatten() {
        broker.shutdown().await.unwrap();
    }
    for (_, controller) in controllers {
        controller.shutdown().await;
    }
    fs::remove_dir_all(root).unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 16)]
async fn p6_internal_topics_bootstrap_route_failover_and_restart_are_authoritative() {
    let _test_guard = P6_E2E_TEST_LOCK.lock().await;
    let root = temp_dir("routing");
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
    let _ = controller_leader(&controllers).await;

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

    let controller_clients = controllers
        .values()
        .map(ControllerNodeHandle::client)
        .collect::<Vec<_>>();
    let configs = (1..=4_u32)
        .map(|broker_id| {
            let index = usize::try_from(broker_id - 1).unwrap();
            broker_config(
                &root,
                broker_id,
                client_addresses[index],
                raft_addresses[index],
                &controller_clients,
            )
        })
        .collect::<Vec<_>>();
    let mut brokers = Vec::new();
    for config in &configs {
        brokers.push(Some(
            start_broker_when_ready(config.clone(), &controllers).await,
        ));
    }
    wait_healthy(&controllers, &[1, 2, 3, 4], 1_000).await;

    let topics_deadline = time::Instant::now() + Duration::from_secs(30);
    let topics = loop {
        match brokers[0]
            .as_ref()
            .unwrap()
            .ensure_p6_internal_topics()
            .await
        {
            Ok(topics) => break topics,
            Err(error) => {
                assert!(
                    time::Instant::now() < topics_deadline,
                    "routing P6 bootstrap did not converge: {error}"
                );
                wait_healthy(&controllers, &[1, 2, 3, 4], 1_050).await;
                time::sleep(Duration::from_millis(40)).await;
            }
        }
    };
    let second_deadline = time::Instant::now() + Duration::from_secs(30);
    let second = loop {
        match brokers[1]
            .as_ref()
            .unwrap()
            .ensure_p6_internal_topics()
            .await
        {
            Ok(topics) => break topics,
            Err(error) => {
                assert!(
                    time::Instant::now() < second_deadline,
                    "second P6 bootstrap observation did not converge: {error}"
                );
                time::sleep(Duration::from_millis(40)).await;
            }
        }
    };
    assert_eq!(
        second, topics,
        "create-once bootstrap must preserve TopicIds"
    );

    let leader = controller_leader(&controllers).await;
    let snapshot = controllers[&leader].metadata_snapshot().await.unwrap();
    let groups = snapshot
        .state
        .topic_by_name(GROUPS_INTERNAL_TOPIC_NAME)
        .unwrap();
    let offsets = snapshot
        .state
        .topic_by_name(OFFSETS_INTERNAL_TOPIC_NAME)
        .unwrap();
    assert_eq!(groups.topic_id, topics.groups_topic_id);
    assert_eq!(offsets.topic_id, topics.offsets_topic_id);
    for topic in [groups, offsets] {
        assert_eq!(topic.partition_count, P6_INTERNAL_TOPIC_PARTITIONS);
        assert_eq!(
            topic.replication_factor,
            P6_INTERNAL_TOPIC_REPLICATION_FACTOR
        );
        assert_eq!(topic.lifecycle, astralog_controller::TopicLifecycle::Active);
        assert_eq!(
            topic.failure_domain_policy,
            astralog_controller::FailureDomainPolicy::Preferred
        );
    }

    let mut client_config =
        ClientConfig::new(brokers[0].as_ref().unwrap().local_addr().to_string());
    client_config.request_timeout = Duration::from_secs(5);
    let client_deadline = time::Instant::now() + Duration::from_secs(30);
    let client = loop {
        match AstraClient::connect(client_config.clone()).await {
            Ok(client) => break client,
            Err(error) => {
                assert!(
                    time::Instant::now() < client_deadline,
                    "routing bootstrap client did not converge: {error}"
                );
                time::sleep(Duration::from_millis(40)).await;
            }
        }
    };
    assert!(
        client
            .create_topic(GROUPS_INTERNAL_TOPIC_NAME, 1)
            .await
            .is_err()
    );
    assert!(
        client
            .delete_topic(OFFSETS_INTERNAL_TOPIC_NAME)
            .await
            .is_err()
    );
    let after_public_rejection = controllers[&controller_leader(&controllers).await]
        .metadata_snapshot()
        .await
        .unwrap();
    assert_eq!(
        after_public_rejection
            .state
            .topic_by_name(GROUPS_INTERNAL_TOPIC_NAME)
            .unwrap()
            .topic_id,
        topics.groups_topic_id
    );
    assert_eq!(
        after_public_rejection
            .state
            .topic_by_name(OFFSETS_INTERNAL_TOPIC_NAME)
            .unwrap()
            .topic_id,
        topics.offsets_topic_id
    );

    let group_id = GroupId::new("orders").unwrap();
    let initial = resolve(&client, &brokers, &group_id, "initial leader").await;
    let leader_index = usize::try_from(initial.coordinator_broker_id - 1).unwrap();
    brokers[leader_index]
        .as_ref()
        .unwrap()
        .validate_group_coordinator_authority(&group_id, initial.coordinator_epoch)
        .await
        .unwrap();

    // `resolve` above has already exercised ordinary client Metadata and P3
    // leader/term discovery. Selecting another legal RF3 voter for an explicit
    // leadership-transfer test is placement-only information, so obtain it from
    // the committed controller assignment instead of issuing a redundant client
    // metadata refresh that can inherit an unrelated ambiguous admin timeout.
    let placement = controllers[&controller_leader(&controllers).await]
        .metadata_snapshot()
        .await
        .unwrap();
    let voters = &placement.state.assignments()[&(topics.groups_topic_id, initial.shard)].voters;
    let target = *voters
        .iter()
        .find(|broker_id| **broker_id != initial.coordinator_broker_id)
        .unwrap();
    match brokers[leader_index]
        .as_ref()
        .unwrap()
        .request_preferred_leader(PreferredLeaderRequest {
            topic_id: topics.groups_topic_id,
            partition_id: initial.shard,
            target_broker_id: target,
        })
        .await
    {
        Ok(_) => {}
        Err(BrokerError::InvalidConfig(
            "preferred-leader transfer result was not observed before bounded timeout",
        )) => {
            // The transfer request may already have crossed the P3 boundary even if
            // the old leader does not observe the result before its bounded wait
            // expires. The fresh-client resolution below is the authoritative proof:
            // it must observe this exact target at a strictly higher term.
        }
        Err(error) => panic!("preferred-leader transfer failed before observation: {error}"),
    }

    // Re-bootstrap the observation client after the explicit transfer. The
    // long-lived bootstrap client intentionally predates the term change and
    // is not itself under test for P6-J coordinator-cache refresh semantics.
    // A fresh client still proves ordinary Metadata -> P3 leader/term routing
    // without requiring future P6-J retry/cache behavior from this P6-C test.
    let target_index = usize::try_from(target - 1).unwrap();
    let mut transferred_config = ClientConfig::new(
        brokers[target_index]
            .as_ref()
            .unwrap()
            .local_addr()
            .to_string(),
    );
    transferred_config.request_timeout = Duration::from_secs(5);
    let transferred_client = AstraClient::connect(transferred_config).await.unwrap();
    let transferred = resolve(
        &transferred_client,
        &brokers,
        &group_id,
        "post-transfer leader",
    )
    .await;
    assert_eq!(transferred.coordinator_broker_id, target);
    assert!(transferred.coordinator_epoch > initial.coordinator_epoch);
    assert_eq!(transferred.hint().coordinator_broker_id, target);
    assert!(
        brokers[leader_index]
            .as_ref()
            .unwrap()
            .validate_group_coordinator_authority(&group_id, initial.coordinator_epoch)
            .await
            .is_err(),
        "old P3 leader/term must be fenced after step-down"
    );

    brokers[target_index]
        .as_ref()
        .unwrap()
        .validate_group_coordinator_authority(&group_id, transferred.coordinator_epoch)
        .await
        .unwrap();
    transferred_client.close().await;

    let bootstrap_survivor = (1..=4_u32).find(|broker_id| *broker_id != target).unwrap();
    let survivor_index = usize::try_from(bootstrap_survivor - 1).unwrap();
    let mut survivor_config = ClientConfig::new(
        brokers[survivor_index]
            .as_ref()
            .unwrap()
            .local_addr()
            .to_string(),
    );
    survivor_config.request_timeout = Duration::from_secs(5);
    let survivor_deadline = time::Instant::now() + Duration::from_secs(30);
    let survivor_client = loop {
        match AstraClient::connect(survivor_config.clone()).await {
            Ok(client) => break client,
            Err(error) => {
                assert!(
                    time::Instant::now() < survivor_deadline,
                    "routing survivor client did not converge: {error}"
                );
                time::sleep(Duration::from_millis(40)).await;
            }
        }
    };

    let failed = brokers[target_index].take().unwrap();
    failed.shutdown().await.unwrap();
    resolve_after_failover(
        &survivor_client,
        &brokers,
        &group_id,
        target,
        transferred.coordinator_epoch,
        "leader failure",
    )
    .await;

    mutate(
        &controllers,
        ControllerCommand::RejoinBroker {
            broker_id: target,
            node_uuid: [u8::try_from(target).unwrap(); 16],
            expected_incarnation: 1,
            new_incarnation: 2,
            client_host: client_addresses[target_index].ip().to_string(),
            client_port: client_addresses[target_index].port(),
            internal_host: raft_addresses[target_index].ip().to_string(),
            internal_port: raft_addresses[target_index].port(),
            rack: Some(format!("rack-{target}")),
            zone: Some(format!("zone-{target}")),
            min_feature: 1,
            max_feature: 3,
        },
    )
    .await;
    brokers[target_index] =
        Some(start_broker_when_ready(configs[target_index].clone(), &controllers).await);
    wait_healthy(&controllers, &[target], 2_000).await;
    let after_restart = brokers[target_index]
        .as_ref()
        .unwrap()
        .ensure_p6_internal_topics()
        .await
        .unwrap();
    assert_eq!(
        after_restart, topics,
        "restart must not recreate/remap internal topics"
    );

    let leader = controller_leader(&controllers).await;
    let final_snapshot = controllers[&leader].metadata_snapshot().await.unwrap();
    assert_eq!(
        final_snapshot
            .state
            .topic_by_name(GROUPS_INTERNAL_TOPIC_NAME)
            .unwrap()
            .topic_id,
        topics.groups_topic_id
    );
    assert_eq!(
        final_snapshot
            .state
            .topic_by_name(OFFSETS_INTERNAL_TOPIC_NAME)
            .unwrap()
            .topic_id,
        topics.offsets_topic_id
    );

    client.close().await;
    survivor_client.close().await;
    for broker in brokers.into_iter().flatten() {
        broker.shutdown().await.unwrap();
    }
    for (_, controller) in controllers {
        controller.shutdown().await;
    }
    fs::remove_dir_all(root).unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 16)]
async fn p6_group_history_commits_recovers_and_survives_coordinator_failures() {
    let _test_guard = P6_E2E_TEST_LOCK.lock().await;
    let (
        root,
        mut controllers,
        client_addresses,
        raft_addresses,
        configs,
        mut brokers,
        _topics,
        bootstrap_client,
    ) = start_p6_cluster("group-history").await;
    bootstrap_client.close().await;
    let group_id = GroupId::new("p6h-orders").unwrap();
    let m1 = member(1);
    let m2 = member(2);
    let subscribed_topic = test_partition(0).topic_id;
    let mut membership = GroupStateMachine::new(group_id.clone());
    let prepared_v1 = membership
        .apply(GroupCommand::AdmitMember {
            expected_group_epoch: 0,
            allocated_member_id: m1,
            instance_id: None,
            session_timeout_ms: 30_000,
            subscriptions: vec![subscribed_topic],
            plan: GroupMutationPlan {
                target_assignments: BTreeMap::from([(test_partition(0), m1)]),
            },
        })
        .unwrap();

    let (first_route, committed) = commit_group_on_current_coordinator(
        &brokers,
        &group_id,
        prepared_v1.clone(),
        "initial join commit",
    )
    .await;
    let first_index = usize::try_from(first_route.hint.coordinator_broker_id - 1).unwrap();
    assert_eq!(committed.record, prepared_v1);

    // Model a lost success response: the caller knows only the candidate it sent.
    // Retrying exact state-version bytes must discover the committed record and avoid
    // appending a duplicate transition.
    let (_, retry) = commit_group_on_current_coordinator(
        &brokers,
        &group_id,
        prepared_v1.clone(),
        "lost join response retry",
    )
    .await;
    assert!(
        !retry.appended,
        "identical lost-response retry must be idempotent"
    );

    // P6-144: coordinator failure while the latest committed record is still
    // PREPARING_EPOCH_FENCE must reconstruct that exact workflow on the higher-
    // term successor before any activation is allowed.
    let failed_first = brokers[first_index].take().unwrap();
    failed_first.shutdown().await.unwrap();
    // A successor may expose its new P3 leader term before its current-term
    // barrier has made a prior-term durable group record visible through the
    // committed-read path. Coordinator authority and committed-history
    // visibility therefore converge at distinct moments. The P6 contract is
    // eventual reconstruction from committed history, so qualify that exact
    // boundary instead of assuming role observation implies instant replay.
    let recovery_deadline = time::Instant::now() + Duration::from_secs(60);
    let recovered_join = loop {
        let route = observe_current_coordinator(
            &brokers,
            &group_id,
            Some((
                first_route.hint.coordinator_broker_id,
                first_route.hint.coordinator_epoch,
            )),
            "join leader failure",
        )
        .await;
        let index = usize::try_from(route.hint.coordinator_broker_id - 1).unwrap();
        match brokers[index]
            .as_ref()
            .unwrap()
            .recover_group_state(group_id.clone(), route.hint.coordinator_epoch)
            .await
        {
            Ok(Some(record)) => break record,
            Ok(None)
            | Err(
                GroupCoordinatorError::NotCoordinator { .. }
                | GroupCoordinatorError::Busy
                | GroupCoordinatorError::NotReady(_),
            ) => {}
            Err(error) => panic!("group recovery failed after coordinator failover: {error}"),
        }
        assert!(
            time::Instant::now() < recovery_deadline,
            "committed group history did not become visible on the successor"
        );
        time::sleep(Duration::from_millis(40)).await;
    };
    assert_eq!(recovered_join, prepared_v1);
    assert_eq!(recovered_join.phase, GroupPhase::PreparingEpochFence);

    // Rejoin the failed broker so the RF3 shard regains all three voters before the
    // next deliberate coordinator failure.
    let failed_broker_id = first_route.hint.coordinator_broker_id;
    let failed_index = usize::try_from(failed_broker_id - 1).unwrap();
    mutate(
        &controllers,
        ControllerCommand::RejoinBroker {
            broker_id: failed_broker_id,
            node_uuid: [u8::try_from(failed_broker_id).unwrap(); 16],
            expected_incarnation: 1,
            new_incarnation: 2,
            client_host: client_addresses[failed_index].ip().to_string(),
            client_port: client_addresses[failed_index].port(),
            internal_host: raft_addresses[failed_index].ip().to_string(),
            internal_port: raft_addresses[failed_index].port(),
            rack: Some(format!("rack-{failed_broker_id}")),
            zone: Some(format!("zone-{failed_broker_id}")),
            min_feature: 1,
            max_feature: 3,
        },
    )
    .await;
    brokers[failed_index] =
        Some(start_broker_when_ready(configs[failed_index].clone(), &controllers).await);
    wait_healthy(&controllers, &[failed_broker_id], 2_000).await;
    refresh_p6_topology_on_live_brokers(&brokers, "first coordinator rejoin").await;
    wait_group_replica_convergence(&brokers, &group_id, "first coordinator rejoin").await;

    // Build the already-qualified P6-F transition sequence in memory; P6-H's job is
    // only to bind these canonical group records to committed __astralog_groups history.
    let mut cooperative = CooperativeStateMachine::from_group_record(prepared_v1.clone()).unwrap();
    let group_fence = cooperative.required_group_fence().unwrap();
    cooperative
        .apply_committed_group_fence(group_fence)
        .unwrap();
    let revoking_v2 = cooperative.activate_prepared_epoch().unwrap();
    let owner_fence = cooperative.required_owner_fences().unwrap().pop().unwrap();
    cooperative
        .apply_committed_owner_fence(owner_fence)
        .unwrap();
    let stable_v3 = cooperative.activate_fenced_targets().unwrap();
    assert_eq!(stable_v3.phase, GroupPhase::Stable);

    for record in [revoking_v2.clone(), stable_v3.clone()] {
        commit_group_on_current_coordinator(
            &brokers,
            &group_id,
            record,
            "post-join recovery progression",
        )
        .await;
    }

    // P6-143: fail the current coordinator while the committed group is fully
    // STABLE. The successor must reconstruct the exact record before any new
    // mutation is attempted. Restore RF3 afterwards so later phase-specific
    // failures still exercise a real majority rather than a degraded shard.
    let stable_route =
        observe_current_coordinator(&brokers, &group_id, None, "stable coordinator failure").await;
    let stable_failed_id = stable_route.hint.coordinator_broker_id;
    let stable_failed_index = usize::try_from(stable_failed_id - 1).unwrap();
    let controller = controller_leader(&controllers).await;
    let stable_incarnation = controllers[&controller]
        .metadata_snapshot()
        .await
        .unwrap()
        .state
        .broker(stable_failed_id)
        .unwrap()
        .incarnation_epoch;
    brokers[stable_failed_index]
        .take()
        .unwrap()
        .shutdown()
        .await
        .unwrap();
    let stable_successor = observe_current_coordinator(
        &brokers,
        &group_id,
        Some((stable_failed_id, stable_route.hint.coordinator_epoch)),
        "stable coordinator successor",
    )
    .await;
    let stable_successor_index =
        usize::try_from(stable_successor.hint.coordinator_broker_id - 1).unwrap();
    let recovered_stable = brokers[stable_successor_index]
        .as_ref()
        .unwrap()
        .recover_group_state(group_id.clone(), stable_successor.hint.coordinator_epoch)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(recovered_stable, stable_v3);
    assert_eq!(recovered_stable.phase, GroupPhase::Stable);

    mutate(
        &controllers,
        ControllerCommand::RejoinBroker {
            broker_id: stable_failed_id,
            node_uuid: [u8::try_from(stable_failed_id).unwrap(); 16],
            expected_incarnation: stable_incarnation,
            new_incarnation: stable_incarnation.checked_add(1).unwrap(),
            client_host: client_addresses[stable_failed_index].ip().to_string(),
            client_port: client_addresses[stable_failed_index].port(),
            internal_host: raft_addresses[stable_failed_index].ip().to_string(),
            internal_port: raft_addresses[stable_failed_index].port(),
            rack: Some(format!("rack-{stable_failed_id}")),
            zone: Some(format!("zone-{stable_failed_id}")),
            min_feature: 1,
            max_feature: 3,
        },
    )
    .await;
    brokers[stable_failed_index] =
        Some(start_broker_when_ready(configs[stable_failed_index].clone(), &controllers).await);
    wait_healthy(&controllers, &[stable_failed_id], 2_500).await;
    refresh_p6_topology_on_live_brokers(&brokers, "stable coordinator rejoin").await;
    wait_group_replica_convergence(&brokers, &group_id, "stable coordinator rejoin").await;

    let mut membership = GroupStateMachine::from_snapshot(stable_v3.clone()).unwrap();
    let prepared_v4 = membership
        .apply(GroupCommand::AdmitMember {
            expected_group_epoch: 1,
            allocated_member_id: m2,
            instance_id: None,
            session_timeout_ms: 30_000,
            subscriptions: vec![subscribed_topic],
            plan: GroupMutationPlan {
                target_assignments: BTreeMap::from([
                    (test_partition(0), m2),
                    (test_partition(1), m1),
                ]),
            },
        })
        .unwrap();
    let (_, _) = commit_group_on_current_coordinator(
        &brokers,
        &group_id,
        prepared_v4.clone(),
        "second member admission",
    )
    .await;

    let mut cooperative = CooperativeStateMachine::from_group_record(prepared_v4.clone()).unwrap();
    cooperative
        .apply_committed_group_fence(cooperative.required_group_fence().unwrap())
        .unwrap();
    let revoking_v5 = cooperative.activate_prepared_epoch().unwrap();
    commit_group_on_current_coordinator(
        &brokers,
        &group_id,
        revoking_v5.clone(),
        "second epoch activation",
    )
    .await;
    let authority_m1 = astralog_protocol::api::MemberAuthorityV0 {
        group_id: group_id.clone(),
        member_id: m1,
        group_epoch: GroupEpoch::new(2).unwrap(),
        member_epoch: MemberEpoch::new(1).unwrap(),
    };
    cooperative
        .revoke_ack(&authority_m1, &[test_partition(0)])
        .unwrap();
    let released_v6 = cooperative.current().clone();
    let (released_route, _) = commit_group_on_current_coordinator(
        &brokers,
        &group_id,
        released_v6.clone(),
        "released partition persistence",
    )
    .await;
    assert!(
        !released_v6
            .active_assignments
            .contains_key(&test_partition(0))
    );
    assert_eq!(released_v6.phase, GroupPhase::Revoking);

    // P6-145: fail the coordinator in REVOKING after durable release but before
    // owner-fence/target activation. The successor must preserve the released
    // no-owner state and resume the same pending handoff.
    let failed_second_id = released_route.hint.coordinator_broker_id;
    let failed_second_index = usize::try_from(failed_second_id - 1).unwrap();
    let failed_second = brokers[failed_second_index].take().unwrap();
    failed_second.shutdown().await.unwrap();
    let third_route = observe_current_coordinator(
        &brokers,
        &group_id,
        Some((failed_second_id, released_route.hint.coordinator_epoch)),
        "released handoff leader failure",
    )
    .await;
    let third_index = usize::try_from(third_route.hint.coordinator_broker_id - 1).unwrap();
    let recovered_release = brokers[third_index]
        .as_ref()
        .unwrap()
        .recover_group_state(group_id.clone(), third_route.hint.coordinator_epoch)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(recovered_release, released_v6);
    assert!(
        !recovered_release
            .active_assignments
            .contains_key(&test_partition(0))
    );

    // Restore the second failed broker before exercising leave/timeout durability.
    mutate(
        &controllers,
        ControllerCommand::RejoinBroker {
            broker_id: failed_second_id,
            node_uuid: [u8::try_from(failed_second_id).unwrap(); 16],
            expected_incarnation: 1,
            new_incarnation: 2,
            client_host: client_addresses[failed_second_index].ip().to_string(),
            client_port: client_addresses[failed_second_index].port(),
            internal_host: raft_addresses[failed_second_index].ip().to_string(),
            internal_port: raft_addresses[failed_second_index].port(),
            rack: Some(format!("rack-{failed_second_id}")),
            zone: Some(format!("zone-{failed_second_id}")),
            min_feature: 1,
            max_feature: 3,
        },
    )
    .await;
    brokers[failed_second_index] =
        Some(start_broker_when_ready(configs[failed_second_index].clone(), &controllers).await);
    wait_healthy(&controllers, &[failed_second_id], 3_000).await;
    refresh_p6_topology_on_live_brokers(&brokers, "second coordinator rejoin").await;
    wait_group_replica_convergence(&brokers, &group_id, "second coordinator rejoin").await;

    for fence in cooperative.required_owner_fences().unwrap() {
        cooperative.apply_committed_owner_fence(fence).unwrap();
    }
    let stable_v7 = cooperative.activate_fenced_targets().unwrap();
    let (_, _) = commit_group_on_current_coordinator(
        &brokers,
        &group_id,
        stable_v7.clone(),
        "released handoff activation",
    )
    .await;

    let mut membership = GroupStateMachine::from_snapshot(stable_v7.clone()).unwrap();
    let leave_v8 = membership
        .apply(GroupCommand::Leave {
            expected_group_epoch: GroupEpoch::new(2).unwrap(),
            member_id: m2,
            expected_member_epoch: MemberEpoch::new(1).unwrap(),
            plan: GroupMutationPlan {
                target_assignments: BTreeMap::from([
                    (test_partition(0), m1),
                    (test_partition(1), m1),
                ]),
            },
        })
        .unwrap();
    let (leave_route, _) = commit_group_on_current_coordinator(
        &brokers,
        &group_id,
        leave_v8.clone(),
        "leave-timeout persistence",
    )
    .await;
    assert!(!leave_v8.members.contains_key(&m2));

    let failed_third_id = leave_route.hint.coordinator_broker_id;
    let failed_third_index = usize::try_from(failed_third_id - 1).unwrap();
    let failed_third = brokers[failed_third_index].take().unwrap();
    failed_third.shutdown().await.unwrap();
    let fourth_route = observe_current_coordinator(
        &brokers,
        &group_id,
        Some((failed_third_id, leave_route.hint.coordinator_epoch)),
        "leave-timeout leader failure",
    )
    .await;
    let fourth_index = usize::try_from(fourth_route.hint.coordinator_broker_id - 1).unwrap();
    let recovered_leave = brokers[fourth_index]
        .as_ref()
        .unwrap()
        .recover_group_state(group_id.clone(), fourth_route.hint.coordinator_epoch)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(recovered_leave, leave_v8);
    assert!(!recovered_leave.members.contains_key(&m2));

    // P5 controller loss is observation-only for the already-existing internal
    // partitions. A canonical group transition must still commit on their P3 quorum.
    for (_, controller) in std::mem::take(&mut controllers) {
        controller.shutdown().await;
    }
    let mut cooperative = CooperativeStateMachine::from_group_record(leave_v8.clone()).unwrap();
    cooperative
        .apply_committed_group_fence(cooperative.required_group_fence().unwrap())
        .unwrap();
    let outage_v9 = cooperative.activate_prepared_epoch().unwrap();
    commit_group_on_current_coordinator(
        &brokers,
        &group_id,
        outage_v9.clone(),
        "controller outage group mutation",
    )
    .await;
    let recovered_during_outage = brokers[fourth_index]
        .as_ref()
        .unwrap()
        .recover_group_state(group_id.clone(), fourth_route.hint.coordinator_epoch)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(recovered_during_outage, outage_v9);

    for broker in brokers.into_iter().flatten() {
        broker.shutdown().await.unwrap();
    }
    fs::remove_dir_all(root).unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 16)]
async fn p6_offsets_fences_commit_fetch_and_failover_are_authoritative() {
    let _test_guard = P6_E2E_TEST_LOCK.lock().await;
    let (root, controllers, client_addresses, raft_addresses, configs, mut brokers, topics, client) =
        start_p6_cluster("offset-history").await;

    let group_id = GroupId::new("p6i-orders").unwrap();
    let m1 = member(1);
    let m2 = member(2);
    let p0 = test_partition(0);
    let p1 = test_partition(1);
    let subscribed_topic = p0.topic_id;

    // Bootstrap epoch 1. New ownership cannot become active until both the
    // epoch fence and per-partition owner fences have quorum-committed in the
    // matching __astralog_offsets shard.
    let mut membership = GroupStateMachine::new(group_id.clone());
    let prepared_v1 = membership
        .apply(GroupCommand::AdmitMember {
            expected_group_epoch: 0,
            allocated_member_id: m1,
            instance_id: None,
            session_timeout_ms: 30_000,
            subscriptions: vec![subscribed_topic],
            plan: GroupMutationPlan {
                target_assignments: BTreeMap::from([(p0, m1), (p1, m1)]),
            },
        })
        .unwrap();
    commit_group_on_current_coordinator(
        &brokers,
        &group_id,
        prepared_v1.clone(),
        "offset bootstrap prepare",
    )
    .await;

    let mut cooperative = CooperativeStateMachine::from_group_record(prepared_v1.clone()).unwrap();
    assert!(cooperative.activate_prepared_epoch().is_err());
    let bootstrap_group_fence = cooperative.required_group_fence().unwrap();
    commit_group_fence_on_current_offsets(
        &client,
        &brokers,
        topics,
        bootstrap_group_fence.clone(),
        "bootstrap fence",
    )
    .await;
    cooperative
        .apply_committed_group_fence(bootstrap_group_fence)
        .unwrap();
    let revoking_v2 = cooperative.activate_prepared_epoch().unwrap();
    commit_group_on_current_coordinator(
        &brokers,
        &group_id,
        revoking_v2.clone(),
        "offset bootstrap epoch activation",
    )
    .await;

    assert!(cooperative.activate_fenced_targets().is_err());
    let bootstrap_owner_fences = cooperative.required_owner_fences().unwrap();
    assert_eq!(bootstrap_owner_fences.len(), 2);
    for fence in bootstrap_owner_fences {
        commit_owner_fence_on_current_offsets(
            &client,
            &brokers,
            topics,
            fence.clone(),
            "bootstrap owner fence",
        )
        .await;
        cooperative.apply_committed_owner_fence(fence).unwrap();
    }
    let stable_v3 = cooperative.activate_fenced_targets().unwrap();
    assert_eq!(stable_v3.phase, GroupPhase::Stable);
    commit_group_on_current_coordinator(
        &brokers,
        &group_id,
        stable_v3.clone(),
        "offset bootstrap stable",
    )
    .await;

    let authority_m1_epoch1 = MemberAuthorityV0 {
        group_id: group_id.clone(),
        member_id: m1,
        group_epoch: GroupEpoch::new(1).unwrap(),
        member_epoch: MemberEpoch::new(1).unwrap(),
    };
    let route = observe_offsets_leader(
        &client,
        &brokers,
        topics,
        &group_id,
        None,
        "initial offset commit",
    )
    .await;
    let route_index = usize::try_from(route.broker_id - 1).unwrap();
    let initial = brokers[route_index]
        .as_ref()
        .unwrap()
        .commit_offsets_internal(
            route.epoch,
            stable_v3.clone(),
            authority_m1_epoch1.clone(),
            vec![CommitOffsetV0 {
                topic_id: p0.topic_id,
                partition: p0.partition,
                next_offset: 100,
                metadata: "initial".into(),
            }],
        )
        .await
        .unwrap();
    assert!(initial.offsets_commit_index > 0);
    assert_eq!(initial.values[0].next_offset, 100);
    assert_eq!(initial.values[0].metadata, "initial");
    assert!(initial.values[0].commit_timestamp_ms > 0);
    let initial_timestamp = initial.values[0].commit_timestamp_ms;

    // P6-148: prepare epoch 2 and durably advance the offsets fence before activating
    // the group epoch. The old epoch still exists in committed group state at
    // this instant, so this is the cross-topic stale-commit race boundary.
    let mut membership = GroupStateMachine::from_snapshot(stable_v3.clone()).unwrap();
    let prepared_v4 = membership
        .apply(GroupCommand::AdmitMember {
            expected_group_epoch: 1,
            allocated_member_id: m2,
            instance_id: None,
            session_timeout_ms: 30_000,
            subscriptions: vec![subscribed_topic],
            plan: GroupMutationPlan {
                target_assignments: BTreeMap::from([(p0, m2), (p1, m1)]),
            },
        })
        .unwrap();
    commit_group_on_current_coordinator(
        &brokers,
        &group_id,
        prepared_v4.clone(),
        "epoch two prepare",
    )
    .await;
    let mut cooperative = CooperativeStateMachine::from_group_record(prepared_v4.clone()).unwrap();
    assert!(cooperative.activate_prepared_epoch().is_err());
    let epoch_two_fence = cooperative.required_group_fence().unwrap();
    let route = observe_offsets_leader(
        &client,
        &brokers,
        topics,
        &group_id,
        None,
        "epoch two group fence",
    )
    .await;
    let route_index = usize::try_from(route.broker_id - 1).unwrap();
    brokers[route_index]
        .as_ref()
        .unwrap()
        .commit_offset_group_fence_internal(route.epoch, epoch_two_fence.clone())
        .await
        .unwrap();

    let stale_epoch = brokers[route_index]
        .as_ref()
        .unwrap()
        .commit_offsets_internal(
            route.epoch,
            stable_v3.clone(),
            authority_m1_epoch1.clone(),
            vec![CommitOffsetV0 {
                topic_id: p0.topic_id,
                partition: p0.partition,
                next_offset: 101,
                metadata: "must-not-commit".into(),
            }],
        )
        .await
        .unwrap_err();
    assert_eq!(
        stale_epoch,
        OffsetCoordinatorError::Validation(PublicError::GroupEpochStale)
    );
    cooperative
        .apply_committed_group_fence(epoch_two_fence)
        .unwrap();
    let revoking_v5 = cooperative.activate_prepared_epoch().unwrap();
    commit_group_on_current_coordinator(
        &brokers,
        &group_id,
        revoking_v5.clone(),
        "epoch two activation",
    )
    .await;

    let authority_m1_epoch2 = MemberAuthorityV0 {
        group_id: group_id.clone(),
        member_id: m1,
        group_epoch: GroupEpoch::new(2).unwrap(),
        member_epoch: MemberEpoch::new(1).unwrap(),
    };
    let route = observe_offsets_leader(
        &client,
        &brokers,
        topics,
        &group_id,
        None,
        "current old owner commit",
    )
    .await;
    let route_index = usize::try_from(route.broker_id - 1).unwrap();
    brokers[route_index]
        .as_ref()
        .unwrap()
        .commit_offsets_internal(
            route.epoch,
            revoking_v5.clone(),
            authority_m1_epoch2.clone(),
            vec![CommitOffsetV0 {
                topic_id: p0.topic_id,
                partition: p0.partition,
                next_offset: 120,
                metadata: "before-release".into(),
            }],
        )
        .await
        .unwrap();

    cooperative.revoke_ack(&authority_m1_epoch2, &[p0]).unwrap();
    let released_v6 = cooperative.current().clone();
    commit_group_on_current_coordinator(
        &brokers,
        &group_id,
        released_v6.clone(),
        "epoch two release",
    )
    .await;
    assert!(cooperative.activate_fenced_targets().is_err());
    let owner_fence = cooperative
        .required_owner_fences()
        .unwrap()
        .into_iter()
        .find(|fence| fence.topic_partition == p0)
        .unwrap();
    let route = observe_offsets_leader(
        &client,
        &brokers,
        topics,
        &group_id,
        None,
        "epoch two owner fence",
    )
    .await;
    let route_index = usize::try_from(route.broker_id - 1).unwrap();
    brokers[route_index]
        .as_ref()
        .unwrap()
        .commit_offset_owner_fence_internal(route.epoch, owner_fence.clone())
        .await
        .unwrap();

    // P6-149: even a caller presenting the pre-release group image cannot smuggle a
    // late old-owner write after the durable owner fence has changed authority.
    let late_old_owner = brokers[route_index]
        .as_ref()
        .unwrap()
        .commit_offsets_internal(
            route.epoch,
            revoking_v5.clone(),
            authority_m1_epoch2.clone(),
            vec![CommitOffsetV0 {
                topic_id: p0.topic_id,
                partition: p0.partition,
                next_offset: 121,
                metadata: "late-old-owner".into(),
            }],
        )
        .await
        .unwrap_err();
    assert_eq!(
        late_old_owner,
        OffsetCoordinatorError::Validation(PublicError::InvalidOffsetCommit)
    );

    cooperative
        .apply_committed_owner_fence(owner_fence.clone())
        .unwrap();
    let stable_v7 = cooperative.activate_fenced_targets().unwrap();
    commit_group_on_current_coordinator(
        &brokers,
        &group_id,
        stable_v7.clone(),
        "epoch two target activation",
    )
    .await;
    assert_eq!(stable_v7.active_assignments[&p0], m2);
    assert_eq!(stable_v7.active_assignments[&p1], m1);

    let authority_m2 = MemberAuthorityV0 {
        group_id: group_id.clone(),
        member_id: m2,
        group_epoch: GroupEpoch::new(2).unwrap(),
        member_epoch: MemberEpoch::new(1).unwrap(),
    };
    let route = observe_offsets_leader(
        &client,
        &brokers,
        topics,
        &group_id,
        None,
        "authorized rewind",
    )
    .await;
    let route_index = usize::try_from(route.broker_id - 1).unwrap();
    let rewind = brokers[route_index]
        .as_ref()
        .unwrap()
        .commit_offsets_internal(
            route.epoch,
            stable_v7.clone(),
            authority_m2.clone(),
            vec![CommitOffsetV0 {
                topic_id: p0.topic_id,
                partition: p0.partition,
                next_offset: 5,
                metadata: "rewind".into(),
            }],
        )
        .await
        .unwrap();
    assert!(rewind.offsets_commit_index > initial.offsets_commit_index);
    assert_eq!(rewind.values[0].next_offset, 5);
    assert_eq!(rewind.values[0].metadata, "rewind");
    assert!(rewind.values[0].commit_timestamp_ms >= initial_timestamp);
    let rewind_timestamp = rewind.values[0].commit_timestamp_ms;

    // A whole multi-offset request is rejected when one partition belongs to
    // another active member. The already-committed p0 value must not change.
    let atomic_reject = brokers[route_index]
        .as_ref()
        .unwrap()
        .commit_offsets_internal(
            route.epoch,
            stable_v7.clone(),
            authority_m2.clone(),
            vec![
                CommitOffsetV0 {
                    topic_id: p0.topic_id,
                    partition: p0.partition,
                    next_offset: 6,
                    metadata: "atomic-first".into(),
                },
                CommitOffsetV0 {
                    topic_id: p1.topic_id,
                    partition: p1.partition,
                    next_offset: 6,
                    metadata: "atomic-invalid".into(),
                },
            ],
        )
        .await
        .unwrap_err();
    assert_eq!(
        atomic_reject,
        OffsetCoordinatorError::Validation(PublicError::InvalidOffsetCommit)
    );

    let mut stale_member = authority_m2.clone();
    stale_member.member_epoch = MemberEpoch::new(2).unwrap();
    let stale_member_error = brokers[route_index]
        .as_ref()
        .unwrap()
        .commit_offsets_internal(
            route.epoch,
            stable_v7.clone(),
            stale_member,
            vec![CommitOffsetV0 {
                topic_id: p0.topic_id,
                partition: p0.partition,
                next_offset: 6,
                metadata: "stale-member".into(),
            }],
        )
        .await
        .unwrap_err();
    assert_eq!(
        stale_member_error,
        OffsetCoordinatorError::Validation(PublicError::UnknownMember)
    );

    let fetched = brokers[route_index]
        .as_ref()
        .unwrap()
        .fetch_offsets_internal(group_id.clone(), route.epoch, vec![p0, p1])
        .await
        .unwrap();
    assert_eq!(fetched.values.len(), 2);
    let p0_value = fetched.values[0].1.as_ref().unwrap();
    assert_eq!(p0_value.next_offset, 5);
    assert_eq!(p0_value.metadata, "rewind");
    assert_eq!(p0_value.commit_timestamp_ms, rewind_timestamp);
    assert!(fetched.values[1].1.is_none());

    // Model a lost successful CommitOffsets response by sending the equivalent
    // request again. P7 idempotency is intentionally absent: a later write may
    // be appended, but authority cannot regress and the effective application
    // offset/metadata remain the requested values.
    let retry = brokers[route_index]
        .as_ref()
        .unwrap()
        .commit_offsets_internal(
            route.epoch,
            stable_v7.clone(),
            authority_m2.clone(),
            vec![CommitOffsetV0 {
                topic_id: p0.topic_id,
                partition: p0.partition,
                next_offset: 5,
                metadata: "rewind".into(),
            }],
        )
        .await
        .unwrap();
    assert!(retry.offsets_commit_index > rewind.offsets_commit_index);
    assert_eq!(retry.values[0].next_offset, 5);
    assert_eq!(retry.values[0].metadata, "rewind");
    assert_eq!(retry.values[0].group_epoch, authority_m2.group_epoch);
    assert_eq!(retry.values[0].member_id, authority_m2.member_id);
    assert_eq!(retry.values[0].member_epoch, authority_m2.member_epoch);
    let retry_timestamp = retry.values[0].commit_timestamp_ms;

    // Kill the real offsets-shard leader. A higher-term successor must recover
    // the committed fences and latest logical value from RF3 history and keep
    // accepting offset writes.
    let failed_route = observe_offsets_leader(
        &client,
        &brokers,
        topics,
        &group_id,
        None,
        "pre-offset-failover leader",
    )
    .await;
    let failed_index = usize::try_from(failed_route.broker_id - 1).unwrap();
    let failed = brokers[failed_index].take().unwrap();
    failed.shutdown().await.unwrap();

    let successor = observe_offsets_leader(
        &client,
        &brokers,
        topics,
        &group_id,
        Some((failed_route.broker_id, failed_route.epoch)),
        "offset leader failure",
    )
    .await;
    let successor_index = usize::try_from(successor.broker_id - 1).unwrap();
    let recovered = brokers[successor_index]
        .as_ref()
        .unwrap()
        .recover_offset_state_internal(group_id.clone(), successor.epoch)
        .await
        .unwrap();
    assert_eq!(recovered.group_fence().unwrap().group_epoch.get(), 2);
    assert_eq!(recovered.owner_fence(p0), Some(&owner_fence));
    let recovered_value = recovered.value(p0).unwrap();
    assert_eq!(recovered_value.next_offset, 5);
    assert_eq!(recovered_value.metadata, "rewind");
    assert_eq!(recovered_value.commit_timestamp_ms, retry_timestamp);

    let post_failover = brokers[successor_index]
        .as_ref()
        .unwrap()
        .commit_offsets_internal(
            successor.epoch,
            stable_v7.clone(),
            authority_m2.clone(),
            vec![CommitOffsetV0 {
                topic_id: p0.topic_id,
                partition: p0.partition,
                next_offset: 7,
                metadata: "post-failover".into(),
            }],
        )
        .await
        .unwrap();
    assert!(post_failover.offsets_commit_index > retry.offsets_commit_index);
    let post_failover_timestamp = post_failover.values[0].commit_timestamp_ms;

    // Restart the failed replica, let P5 restore its incarnation/topology, then
    // transfer the offsets shard back to it. Fetching there proves restart
    // recovery preserves metadata + the server commit timestamp as well.
    mutate(
        &controllers,
        ControllerCommand::RejoinBroker {
            broker_id: failed_route.broker_id,
            node_uuid: [u8::try_from(failed_route.broker_id).unwrap(); 16],
            expected_incarnation: 1,
            new_incarnation: 2,
            client_host: client_addresses[failed_index].ip().to_string(),
            client_port: client_addresses[failed_index].port(),
            internal_host: raft_addresses[failed_index].ip().to_string(),
            internal_port: raft_addresses[failed_index].port(),
            rack: Some(format!("rack-{}", failed_route.broker_id)),
            zone: Some(format!("zone-{}", failed_route.broker_id)),
            min_feature: 1,
            max_feature: 3,
        },
    )
    .await;
    brokers[failed_index] =
        Some(start_broker_when_ready(configs[failed_index].clone(), &controllers).await);
    wait_healthy(&controllers, &[failed_route.broker_id], 4_000).await;
    refresh_p6_topology_on_live_brokers(&brokers, "offset leader restart").await;

    let transfer_deadline = time::Instant::now() + Duration::from_secs(30);
    loop {
        let current = observe_offsets_leader(
            &client,
            &brokers,
            topics,
            &group_id,
            None,
            "restart transfer source",
        )
        .await;
        let current_index = usize::try_from(current.broker_id - 1).unwrap();
        if brokers[current_index]
            .as_ref()
            .unwrap()
            .request_preferred_leader(PreferredLeaderRequest {
                topic_id: topics.offsets_topic_id,
                partition_id: current.shard,
                target_broker_id: failed_route.broker_id,
            })
            .await
            .is_ok()
        {
            break;
        }
        assert!(
            time::Instant::now() < transfer_deadline,
            "restarted offsets replica did not become eligible for leadership"
        );
        time::sleep(Duration::from_millis(80)).await;
    }

    let restarted = observe_offsets_leader(
        &client,
        &brokers,
        topics,
        &group_id,
        Some((successor.broker_id, successor.epoch)),
        "restarted offset leader",
    )
    .await;
    assert_eq!(restarted.broker_id, failed_route.broker_id);
    let restarted_index = usize::try_from(restarted.broker_id - 1).unwrap();
    let after_restart = brokers[restarted_index]
        .as_ref()
        .unwrap()
        .fetch_offsets_internal(group_id.clone(), restarted.epoch, vec![p0])
        .await
        .unwrap();
    let after_restart_value = after_restart.values[0].1.as_ref().unwrap();
    assert_eq!(after_restart_value.next_offset, 7);
    assert_eq!(after_restart_value.metadata, "post-failover");
    assert_eq!(
        after_restart_value.commit_timestamp_ms,
        post_failover_timestamp
    );
    assert_eq!(after_restart_value.group_epoch, authority_m2.group_epoch);
    assert_eq!(after_restart_value.member_id, authority_m2.member_id);
    assert_eq!(after_restart_value.member_epoch, authority_m2.member_epoch);

    client.close().await;
    for broker in brokers.into_iter().flatten() {
        broker.shutdown().await.unwrap();
    }
    for (_, controller) in controllers {
        controller.shutdown().await;
    }
    fs::remove_dir_all(root).unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 16)]
async fn p6_group_fence_leader_failure_never_activates_epoch_early() {
    let _test_guard = P6_E2E_TEST_LOCK.lock().await;
    let (
        root,
        controllers,
        _client_addresses,
        _raft_addresses,
        _configs,
        mut brokers,
        topics,
        client,
    ) = start_p6_cluster("group-fence-leader-failure").await;
    let group_id = GroupId::new("p6k-group-fence-failure").unwrap();
    let owner = member(31);
    let p0 = test_partition(0);

    let mut membership = GroupStateMachine::new(group_id.clone());
    let prepared = membership
        .apply(GroupCommand::AdmitMember {
            expected_group_epoch: 0,
            allocated_member_id: owner,
            instance_id: None,
            session_timeout_ms: 30_000,
            subscriptions: vec![p0.topic_id],
            plan: GroupMutationPlan {
                target_assignments: BTreeMap::from([(p0, owner)]),
            },
        })
        .unwrap();
    commit_group_on_current_coordinator(
        &brokers,
        &group_id,
        prepared.clone(),
        "P6-K group-fence prepare",
    )
    .await;
    let mut cooperative = CooperativeStateMachine::from_group_record(prepared.clone()).unwrap();
    let fence = cooperative.required_group_fence().unwrap();
    assert!(cooperative.activate_prepared_epoch().is_err());

    let failed_route = observe_offsets_leader(
        &client,
        &brokers,
        topics,
        &group_id,
        None,
        "P6-K pre-group-fence failure",
    )
    .await;
    let failed_index = usize::try_from(failed_route.broker_id - 1).unwrap();
    brokers[failed_index]
        .take()
        .unwrap()
        .shutdown()
        .await
        .unwrap();

    let group_route = observe_current_coordinator(
        &brokers,
        &group_id,
        None,
        "P6-K group state after offsets leader death",
    )
    .await;
    let group_index = usize::try_from(group_route.hint.coordinator_broker_id - 1).unwrap();
    let durable_before_fence = brokers[group_index]
        .as_ref()
        .unwrap()
        .recover_group_state(group_id.clone(), group_route.hint.coordinator_epoch)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(durable_before_fence, prepared);
    assert_eq!(durable_before_fence.phase, GroupPhase::PreparingEpochFence);
    assert_eq!(durable_before_fence.active_group_epoch, 0);

    let successor = observe_offsets_leader(
        &client,
        &brokers,
        topics,
        &group_id,
        Some((failed_route.broker_id, failed_route.epoch)),
        "P6-K group-fence successor",
    )
    .await;
    let successor_index = usize::try_from(successor.broker_id - 1).unwrap();
    let offsets_before_fence = brokers[successor_index]
        .as_ref()
        .unwrap()
        .recover_offset_state_internal(group_id.clone(), successor.epoch)
        .await
        .unwrap();
    assert!(offsets_before_fence.group_fence().is_none());

    brokers[successor_index]
        .as_ref()
        .unwrap()
        .commit_offset_group_fence_internal(successor.epoch, fence.clone())
        .await
        .unwrap();
    cooperative.apply_committed_group_fence(fence).unwrap();
    let activated = cooperative.activate_prepared_epoch().unwrap();
    commit_group_on_current_coordinator(
        &brokers,
        &group_id,
        activated.clone(),
        "P6-K post-group-fence activation",
    )
    .await;
    assert_eq!(activated.active_group_epoch, 1);

    client.close().await;
    for broker in brokers.into_iter().flatten() {
        broker.shutdown().await.unwrap();
    }
    for (_, controller) in controllers {
        controller.shutdown().await;
    }
    fs::remove_dir_all(root).unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 16)]
async fn p6_owner_fence_leader_failure_never_activates_target_early() {
    let _test_guard = P6_E2E_TEST_LOCK.lock().await;
    let (
        root,
        controllers,
        _client_addresses,
        _raft_addresses,
        _configs,
        mut brokers,
        topics,
        client,
    ) = start_p6_cluster("owner-fence-leader-failure").await;
    let group_id = GroupId::new("p6k-owner-fence-failure").unwrap();
    let old_owner = member(41);
    let new_owner = member(42);
    let p0 = test_partition(0);

    // Bootstrap one active owner through the full epoch-1 fence sequence.
    let mut membership = GroupStateMachine::new(group_id.clone());
    let prepared_v1 = membership
        .apply(GroupCommand::AdmitMember {
            expected_group_epoch: 0,
            allocated_member_id: old_owner,
            instance_id: None,
            session_timeout_ms: 30_000,
            subscriptions: vec![p0.topic_id],
            plan: GroupMutationPlan {
                target_assignments: BTreeMap::from([(p0, old_owner)]),
            },
        })
        .unwrap();
    commit_group_on_current_coordinator(
        &brokers,
        &group_id,
        prepared_v1.clone(),
        "P6-K owner-fence bootstrap prepare",
    )
    .await;
    let mut cooperative = CooperativeStateMachine::from_group_record(prepared_v1).unwrap();
    let bootstrap_group_fence = cooperative.required_group_fence().unwrap();
    commit_group_fence_on_current_offsets(
        &client,
        &brokers,
        topics,
        bootstrap_group_fence.clone(),
        "P6-K owner-fence bootstrap group fence",
    )
    .await;
    cooperative
        .apply_committed_group_fence(bootstrap_group_fence)
        .unwrap();
    let bootstrap_revoking = cooperative.activate_prepared_epoch().unwrap();
    commit_group_on_current_coordinator(
        &brokers,
        &group_id,
        bootstrap_revoking,
        "P6-K owner-fence bootstrap activation",
    )
    .await;
    for fence in cooperative.required_owner_fences().unwrap() {
        commit_owner_fence_on_current_offsets(
            &client,
            &brokers,
            topics,
            fence.clone(),
            "P6-K owner-fence bootstrap owner fence",
        )
        .await;
        cooperative.apply_committed_owner_fence(fence).unwrap();
    }
    let stable_v1 = cooperative.activate_fenced_targets().unwrap();
    commit_group_on_current_coordinator(
        &brokers,
        &group_id,
        stable_v1.clone(),
        "P6-K owner-fence bootstrap stable",
    )
    .await;

    // Prepare epoch 2 with p0 moving to the new member and advance the group
    // fence normally so the exact owner-fence boundary is exposed.
    let mut membership = GroupStateMachine::from_snapshot(stable_v1.clone()).unwrap();
    let prepared_v2 = membership
        .apply(GroupCommand::AdmitMember {
            expected_group_epoch: 1,
            allocated_member_id: new_owner,
            instance_id: None,
            session_timeout_ms: 30_000,
            subscriptions: vec![p0.topic_id],
            plan: GroupMutationPlan {
                target_assignments: BTreeMap::from([(p0, new_owner)]),
            },
        })
        .unwrap();
    commit_group_on_current_coordinator(
        &brokers,
        &group_id,
        prepared_v2.clone(),
        "P6-K owner-fence epoch-two prepare",
    )
    .await;
    let mut cooperative = CooperativeStateMachine::from_group_record(prepared_v2).unwrap();
    let group_fence = cooperative.required_group_fence().unwrap();
    commit_group_fence_on_current_offsets(
        &client,
        &brokers,
        topics,
        group_fence.clone(),
        "P6-K owner-fence epoch-two group fence",
    )
    .await;
    cooperative
        .apply_committed_group_fence(group_fence)
        .unwrap();
    let revoking = cooperative.activate_prepared_epoch().unwrap();
    commit_group_on_current_coordinator(
        &brokers,
        &group_id,
        revoking.clone(),
        "P6-K owner-fence epoch-two activation",
    )
    .await;
    let old_authority = MemberAuthorityV0 {
        group_id: group_id.clone(),
        member_id: old_owner,
        group_epoch: GroupEpoch::new(2).unwrap(),
        member_epoch: MemberEpoch::new(1).unwrap(),
    };
    cooperative.revoke_ack(&old_authority, &[p0]).unwrap();
    let released = cooperative.current().clone();
    commit_group_on_current_coordinator(
        &brokers,
        &group_id,
        released.clone(),
        "P6-K owner-fence durable release",
    )
    .await;
    let owner_fence = cooperative.required_owner_fences().unwrap().remove(0);
    assert!(!released.active_assignments.contains_key(&p0));
    assert_eq!(released.target_assignments.get(&p0), Some(&new_owner));
    assert!(cooperative.activate_fenced_targets().is_err());

    let failed_route = observe_offsets_leader(
        &client,
        &brokers,
        topics,
        &group_id,
        None,
        "P6-K pre-owner-fence failure",
    )
    .await;
    let failed_index = usize::try_from(failed_route.broker_id - 1).unwrap();
    brokers[failed_index]
        .take()
        .unwrap()
        .shutdown()
        .await
        .unwrap();

    let group_route = observe_current_coordinator(
        &brokers,
        &group_id,
        None,
        "P6-K released state after offsets leader death",
    )
    .await;
    let group_index = usize::try_from(group_route.hint.coordinator_broker_id - 1).unwrap();
    let durable_release = brokers[group_index]
        .as_ref()
        .unwrap()
        .recover_group_state(group_id.clone(), group_route.hint.coordinator_epoch)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(durable_release, released);
    assert!(!durable_release.active_assignments.contains_key(&p0));
    assert_eq!(
        durable_release.target_assignments.get(&p0),
        Some(&new_owner)
    );

    let successor = observe_offsets_leader(
        &client,
        &brokers,
        topics,
        &group_id,
        Some((failed_route.broker_id, failed_route.epoch)),
        "P6-K owner-fence successor",
    )
    .await;
    let successor_index = usize::try_from(successor.broker_id - 1).unwrap();
    let before_owner_fence = brokers[successor_index]
        .as_ref()
        .unwrap()
        .recover_offset_state_internal(group_id.clone(), successor.epoch)
        .await
        .unwrap();
    assert!(before_owner_fence.owner_fence(p0).is_none());
    commit_owner_fence_on_current_offsets(
        &client,
        &brokers,
        topics,
        owner_fence.clone(),
        "P6-K owner-fence successor commit",
    )
    .await;
    cooperative
        .apply_committed_owner_fence(owner_fence)
        .unwrap();
    let stable_v2 = cooperative.activate_fenced_targets().unwrap();
    commit_group_on_current_coordinator(
        &brokers,
        &group_id,
        stable_v2.clone(),
        "P6-K post-owner-fence activation",
    )
    .await;
    assert_eq!(stable_v2.active_assignments.get(&p0), Some(&new_owner));

    client.close().await;
    for broker in brokers.into_iter().flatten() {
        broker.shutdown().await.unwrap();
    }
    for (_, controller) in controllers {
        controller.shutdown().await;
    }
    fs::remove_dir_all(root).unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 16)]
async fn p6_group_join_leader_failure_before_commit_creates_no_phantom_history() {
    let _test_guard = P6_E2E_TEST_LOCK.lock().await;
    let (
        root,
        controllers,
        _client_addresses,
        _raft_addresses,
        _configs,
        mut brokers,
        _topics,
        bootstrap_client,
    ) = start_p6_cluster("group-precommit-failure").await;
    bootstrap_client.close().await;
    let group_id = GroupId::new("p6h-precommit").unwrap();

    let before = observe_current_coordinator(&brokers, &group_id, None, "pre-commit leader").await;
    let failed_index = usize::try_from(before.hint.coordinator_broker_id - 1).unwrap();
    let failed = brokers[failed_index].take().unwrap();
    failed.shutdown().await.unwrap();

    let after = observe_current_coordinator(
        &brokers,
        &group_id,
        Some((
            before.hint.coordinator_broker_id,
            before.hint.coordinator_epoch,
        )),
        "pre-commit failover",
    )
    .await;
    let after_index = usize::try_from(after.hint.coordinator_broker_id - 1).unwrap();
    let recovered = brokers[after_index]
        .as_ref()
        .unwrap()
        .recover_group_state(group_id, after.hint.coordinator_epoch)
        .await
        .unwrap();
    assert!(
        recovered.is_none(),
        "leader failure before a group mutation commit must not create authority"
    );

    for broker in brokers.into_iter().flatten() {
        broker.shutdown().await.unwrap();
    }
    for (_, controller) in controllers {
        controller.shutdown().await;
    }
    fs::remove_dir_all(root).unwrap();
}
