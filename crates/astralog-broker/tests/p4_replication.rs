use std::{
    collections::{BTreeMap, BTreeSet},
    fs::{self, OpenOptions},
    io::Write,
    net::{Ipv4Addr, SocketAddr, TcpListener as StdTcpListener},
    path::{Path, PathBuf},
    process::{Child, Command, Stdio},
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use astralog_broker::{
    BrokerConfig, BrokerHandle, StaticBrokerConfig, StaticReplicationConfig, StaticTopicConfig,
    start,
};
use astralog_client::{AstraClient, ClientConfig};
use astralog_common::TopicId;
use astralog_protocol::{
    ApiKey, ClientHello, FrameKind, NO_RESPONSE_FLAG, PublicError, ServerHello, UNKNOWN_EPOCH,
    WIRE_VERSION, WireFrame,
    api::{
        AckMode, ApiVersionsRequestV0, BodyCodec, FetchPartitionRequestV0, FetchRequestV0,
        IsolationLevel, ListOffsetsQueryV0, ListOffsetsRequestV0, MetadataResponseV0,
        ProducePartitionRequestV0, ProduceRequestV0, ProduceResponseV0, WireRecordV0,
    },
    decode_frame, encode_frame,
};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::TcpStream,
    time,
};

const MAX_FRAME: usize = 1024 * 1024;
// Correctness qualification, not a latency benchmark. Full-workspace parallel
// test load can delay TCP startup/election noticeably on constrained CI hosts,
// so keep the observation window generous without changing any broker/Raft
// runtime timing or election semantics.
const ELECTION_WAIT: Duration = Duration::from_secs(30);
const PROCESS_KILL_TEST: &str = "rf3_separate_process_leader_kill_preserves_committed_quorum";
const PROCESS_CHILD_ID: &str = "ASTRALOG_P4_RF3_PROCESS_CHILD_ID";
const PROCESS_ROOT: &str = "ASTRALOG_P4_RF3_PROCESS_ROOT";
const PROCESS_CLIENT_ADDRS: &str = "ASTRALOG_P4_RF3_PROCESS_CLIENT_ADDRS";
const PROCESS_RAFT_ADDRS: &str = "ASTRALOG_P4_RF3_PROCESS_RAFT_ADDRS";
const PROCESS_READY_MARKER: &str = "ASTRALOG_P4_RF3_PROCESS_READY_MARKER";
const PROCESS_READY_WAIT: Duration = Duration::from_secs(30);
const PROCESS_CHILD_WATCHDOG: Duration = Duration::from_secs(90);

struct BrokerChildren {
    children: BTreeMap<u32, Child>,
}

impl BrokerChildren {
    fn spawn(
        root: &Path,
        client_addrs: &BTreeMap<u32, SocketAddr>,
        raft_addrs: &BTreeMap<u32, SocketAddr>,
    ) -> Self {
        let executable = std::env::current_exe().expect("P4 process test executable must exist");
        let encoded_client_addrs = encode_process_addresses(client_addrs);
        let encoded_raft_addrs = encode_process_addresses(raft_addrs);
        let mut spawned = Self {
            children: BTreeMap::new(),
        };

        for broker_id in 1..=3_u32 {
            let ready = process_ready_marker(root, broker_id);
            let child = Command::new(&executable)
                .arg("--exact")
                .arg(PROCESS_KILL_TEST)
                .arg("--test-threads=1")
                .env(PROCESS_CHILD_ID, broker_id.to_string())
                .env(PROCESS_ROOT, root)
                .env(PROCESS_CLIENT_ADDRS, &encoded_client_addrs)
                .env(PROCESS_RAFT_ADDRS, &encoded_raft_addrs)
                .env(PROCESS_READY_MARKER, &ready)
                .stdout(Stdio::inherit())
                .stderr(Stdio::inherit())
                .spawn()
                .unwrap_or_else(|error| {
                    panic!("failed to spawn P4 RF3 broker child {broker_id}: {error}")
                });
            spawned.children.insert(broker_id, child);
        }
        spawned
    }

    async fn wait_ready(&mut self, root: &Path, broker_id: u32) {
        let marker = process_ready_marker(root, broker_id);
        let expected = format!("broker-{broker_id}").into_bytes();
        let deadline = time::Instant::now() + PROCESS_READY_WAIT;
        loop {
            if fs::read(&marker).is_ok_and(|bytes| bytes == expected) {
                return;
            }
            if let Some(status) = self
                .children
                .get_mut(&broker_id)
                .expect("P4 RF3 child process must exist while awaiting readiness")
                .try_wait()
                .expect("P4 RF3 child status must be readable")
            {
                panic!("P4 RF3 broker child {broker_id} exited before readiness: {status}");
            }
            assert!(
                time::Instant::now() < deadline,
                "P4 RF3 broker child {broker_id} did not become ready within {PROCESS_READY_WAIT:?}"
            );
            time::sleep(Duration::from_millis(20)).await;
        }
    }

    fn kill_and_wait(&mut self, broker_id: u32) {
        let mut child = self
            .children
            .remove(&broker_id)
            .expect("elected P4 RF3 leader must have a child process");
        child
            .kill()
            .expect("elected P4 RF3 leader process must still be alive at crash point");
        let status = child
            .wait()
            .expect("killed P4 RF3 leader process must be reapable");
        assert!(
            !status.success(),
            "Child::kill must terminate the elected P4 RF3 leader abnormally"
        );
    }
}

impl Drop for BrokerChildren {
    fn drop(&mut self) {
        for child in self.children.values_mut() {
            let _ = child.kill();
            let _ = child.wait();
        }
    }
}

fn temp_dir(name: &str) -> PathBuf {
    let nonce = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    std::env::temp_dir().join(format!(
        "astralog-p4-network-{name}-{}-{nonce}",
        std::process::id()
    ))
}

fn reserve_addr() -> SocketAddr {
    let listener = StdTcpListener::bind((Ipv4Addr::LOCALHOST, 0)).unwrap();
    let address = listener.local_addr().unwrap();
    drop(listener);
    address
}

fn encode_process_addresses(addresses: &BTreeMap<u32, SocketAddr>) -> String {
    (1..=3_u32)
        .map(|broker_id| addresses[&broker_id].to_string())
        .collect::<Vec<_>>()
        .join(",")
}

fn parse_process_addresses(name: &str) -> BTreeMap<u32, SocketAddr> {
    let raw = std::env::var(name).unwrap_or_else(|_| panic!("missing {name}"));
    let parsed = raw
        .split(',')
        .enumerate()
        .map(|(index, address)| {
            let broker_id = u32::try_from(index + 1).expect("broker id index fits u32");
            let address = address
                .parse::<SocketAddr>()
                .unwrap_or_else(|error| panic!("invalid {name} address {address}: {error}"));
            (broker_id, address)
        })
        .collect::<BTreeMap<_, _>>();
    assert_eq!(
        parsed.len(),
        3,
        "{name} must contain exactly three addresses"
    );
    parsed
}

fn process_ready_marker(root: &Path, broker_id: u32) -> PathBuf {
    root.join(format!("broker-{broker_id}.ready"))
}

fn client_config(addr: SocketAddr) -> ClientConfig {
    let mut config = ClientConfig::new(addr.to_string());
    config.max_frame_len = MAX_FRAME;
    config.request_timeout = Duration::from_secs(3);
    config.connect_timeout = Duration::from_secs(2);
    config
}

fn record(value: &[u8]) -> WireRecordV0 {
    WireRecordV0 {
        timestamp_ms: -1,
        key: None,
        value: Some(value.to_vec()),
        headers: Vec::new(),
    }
}

fn build_cluster(
    root: &std::path::Path,
) -> (
    TopicId,
    BTreeMap<u32, BrokerConfig>,
    BTreeMap<u32, SocketAddr>,
) {
    let mut client_addrs = BTreeMap::new();
    let mut raft_addrs = BTreeMap::new();
    for broker_id in 1..=3_u32 {
        client_addrs.insert(broker_id, reserve_addr());
        raft_addrs.insert(broker_id, reserve_addr());
    }
    let (topic_id, configs) = build_cluster_with_addresses(root, &client_addrs, &raft_addrs);
    (topic_id, configs, client_addrs)
}

fn build_cluster_with_addresses(
    root: &Path,
    client_addrs: &BTreeMap<u32, SocketAddr>,
    raft_addrs: &BTreeMap<u32, SocketAddr>,
) -> (TopicId, BTreeMap<u32, BrokerConfig>) {
    assert_eq!(
        client_addrs.len(),
        3,
        "P4 RF3 requires three client addresses"
    );
    assert_eq!(raft_addrs.len(), 3, "P4 RF3 requires three Raft addresses");
    let topic_id = TopicId::new([0x72; 16]);
    let cluster_id = [0x61; 16];
    let brokers = (1..=3_u32)
        .map(|broker_id| StaticBrokerConfig {
            broker_id,
            node_uuid: [u8::try_from(broker_id).unwrap(); 16],
            client_host: "127.0.0.1".into(),
            client_port: client_addrs[&broker_id].port(),
            raft_address: raft_addrs[&broker_id],
            transport_incarnation: u64::from(broker_id) * 10,
        })
        .collect::<Vec<_>>();
    let topics = vec![StaticTopicConfig {
        name: "orders".into(),
        topic_id,
        partition_replicas: vec![vec![1, 2, 3]],
    }];

    let mut configs = BTreeMap::new();
    for broker_id in 1..=3_u32 {
        let mut config = BrokerConfig::new(
            root.join(format!("broker-{broker_id}")),
            client_addrs[&broker_id],
        );
        config.broker_id = broker_id;
        config.advertised_host = "127.0.0.1".into();
        config.advertised_port = client_addrs[&broker_id].port();
        config.max_frame_len = MAX_FRAME;
        config.max_in_flight_per_connection = 16;
        config.max_pending_response_bytes = 20 * MAX_FRAME;
        config.frame_assembly_timeout = Duration::from_secs(1);
        let mut replication = StaticReplicationConfig::new(
            cluster_id,
            raft_addrs[&broker_id],
            brokers.clone(),
            topics.clone(),
        );
        replication.tick_interval = Duration::from_millis(20);
        // This is a correctness E2E, not an election-randomness test. Give each
        // broker a legal but non-overlapping election window so repeated
        // qualification cannot livelock on unlucky split-vote samples. The
        // production default remains randomized and P3 owns its safety tests.
        let election_min = 5 + (u64::from(broker_id) - 1) * 3;
        replication.election_timeout_min_ticks = election_min;
        replication.election_timeout_max_ticks = election_min + 1;
        replication.reconnect_min_delay = Duration::from_millis(5);
        replication.reconnect_max_delay = Duration::from_millis(50);
        config.static_replication = Some(replication);
        configs.insert(broker_id, config);
    }
    (topic_id, configs)
}

async fn run_process_broker_child() {
    let broker_id = std::env::var(PROCESS_CHILD_ID)
        .expect("P4 RF3 child broker id must be set")
        .parse::<u32>()
        .expect("P4 RF3 child broker id must be numeric");
    assert!((1..=3).contains(&broker_id));
    let root =
        PathBuf::from(std::env::var_os(PROCESS_ROOT).expect("P4 RF3 child root must be set"));
    let client_addrs = parse_process_addresses(PROCESS_CLIENT_ADDRS);
    let raft_addrs = parse_process_addresses(PROCESS_RAFT_ADDRS);
    let (_, configs) = build_cluster_with_addresses(&root, &client_addrs, &raft_addrs);
    let _broker = start(configs[&broker_id].clone())
        .await
        .expect("P4 RF3 child broker must start");

    let marker = PathBuf::from(
        std::env::var_os(PROCESS_READY_MARKER).expect("P4 RF3 child ready marker must be set"),
    );
    let mut ready = OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(&marker)
        .expect("P4 RF3 child must create its isolated ready marker");
    ready
        .write_all(format!("broker-{broker_id}").as_bytes())
        .expect("P4 RF3 child must write its ready marker");
    ready
        .sync_all()
        .expect("P4 RF3 child must fsync its ready marker");

    time::sleep(PROCESS_CHILD_WATCHDOG).await;
    panic!("P4 RF3 parent never terminated broker child {broker_id}");
}

async fn start_cluster(configs: &BTreeMap<u32, BrokerConfig>) -> BTreeMap<u32, BrokerHandle> {
    let mut handles = BTreeMap::new();
    for broker_id in 1..=3_u32 {
        handles.insert(broker_id, start(configs[&broker_id].clone()).await.unwrap());
    }
    handles
}

async fn wait_for_leader(client: &AstraClient, topic_id: TopicId) -> (u32, u64) {
    let deadline = time::Instant::now() + ELECTION_WAIT;
    loop {
        if let Ok(metadata) = client.refresh_metadata().await
            && let Some(partition) = metadata
                .topics
                .iter()
                .find(|topic| topic.topic_id == topic_id)
                .and_then(|topic| {
                    topic
                        .partitions
                        .iter()
                        .find(|partition| partition.partition == 0)
                })
            && partition.leader_broker_id != u32::MAX
            && partition.leader_epoch != UNKNOWN_EPOCH
            && partition.leader_epoch > 0
        {
            return (partition.leader_broker_id, partition.leader_epoch);
        }
        assert!(
            time::Instant::now() < deadline,
            "P4 leader election timed out"
        );
        time::sleep(Duration::from_millis(25)).await;
    }
}

async fn wait_for_new_leader(
    client: &AstraClient,
    topic_id: TopicId,
    previous_leader: u32,
    previous_epoch: u64,
) -> (u32, u64) {
    let deadline = time::Instant::now() + ELECTION_WAIT;
    loop {
        let (leader, epoch) = wait_for_leader(client, topic_id).await;
        if leader != previous_leader && epoch > previous_epoch {
            return (leader, epoch);
        }
        assert!(
            time::Instant::now() < deadline,
            "P4 failover leader election timed out"
        );
        time::sleep(Duration::from_millis(25)).await;
    }
}

async fn raw_ready(addr: SocketAddr) -> TcpStream {
    let mut stream = TcpStream::connect(addr).await.unwrap();
    stream
        .write_all(&ClientHello::v1().encode().unwrap())
        .await
        .unwrap();
    let mut hello = [0_u8; 16];
    stream.read_exact(&mut hello).await.unwrap();
    let hello = ServerHello::decode(&hello).unwrap();
    assert_eq!(hello.selected_wire_version, WIRE_VERSION);

    let request = ApiVersionsRequestV0 {
        client_software_name: "p4-network-test".into(),
        client_software_version: "0.1".into(),
        tags: Vec::new(),
    };
    let frame = request_frame(ApiKey::ApiVersions, 1, request.encode_body().unwrap());
    stream
        .write_all(&encode_frame(&frame, MAX_FRAME).unwrap())
        .await
        .unwrap();
    let response = read_raw_frame(&mut stream).await;
    assert_eq!(response.error_code, PublicError::None as u16);
    stream
}

fn request_frame(api: ApiKey, correlation: u64, body: Vec<u8>) -> WireFrame {
    WireFrame {
        kind: FrameKind::Request,
        flags: 0,
        api_key: api as u16,
        api_version: 0,
        error_code: 0,
        correlation_id: correlation,
        timeout_ms: 3_000,
        throttle_ms: 0,
        header_tags: Vec::new(),
        body,
    }
}

async fn read_raw_frame(stream: &mut TcpStream) -> WireFrame {
    let mut prefix = [0_u8; 4];
    time::timeout(Duration::from_secs(3), stream.read_exact(&mut prefix))
        .await
        .unwrap()
        .unwrap();
    let len = usize::try_from(u32::from_be_bytes(prefix)).unwrap();
    let mut bytes = vec![0_u8; len + 4];
    bytes[..4].copy_from_slice(&prefix);
    stream.read_exact(&mut bytes[4..]).await.unwrap();
    decode_frame(&bytes, MAX_FRAME).unwrap()
}

async fn direct_produce(
    addr: SocketAddr,
    topic_id: TopicId,
    epoch: u64,
    value: &[u8],
) -> ProduceResponseV0 {
    let mut stream = raw_ready(addr).await;
    let request = ProduceRequestV0 {
        acks: AckMode::Leader,
        partitions: vec![ProducePartitionRequestV0 {
            topic_id,
            partition: 0,
            leader_epoch: epoch,
            records: vec![record(value)],
        }],
        tags: Vec::new(),
    };
    let frame = request_frame(ApiKey::Produce, 2, request.encode_body().unwrap());
    stream
        .write_all(&encode_frame(&frame, MAX_FRAME).unwrap())
        .await
        .unwrap();
    let response = read_raw_frame(&mut stream).await;
    assert_eq!(response.error_code, PublicError::None as u16);
    ProduceResponseV0::decode_body(&response.body).unwrap()
}

async fn direct_no_response_produce(addr: SocketAddr, topic_id: TopicId, epoch: u64, value: &[u8]) {
    let mut stream = raw_ready(addr).await;
    let request = ProduceRequestV0 {
        acks: AckMode::None,
        partitions: vec![ProducePartitionRequestV0 {
            topic_id,
            partition: 0,
            leader_epoch: epoch,
            records: vec![record(value)],
        }],
        tags: Vec::new(),
    };
    let mut frame = request_frame(ApiKey::Produce, 0, request.encode_body().unwrap());
    frame.flags = NO_RESPONSE_FLAG;
    frame.timeout_ms = 0;
    stream
        .write_all(&encode_frame(&frame, MAX_FRAME).unwrap())
        .await
        .unwrap();
    assert!(
        time::timeout(Duration::from_millis(150), read_raw_frame(&mut stream))
            .await
            .is_err(),
        "acks=0 must never produce a response frame"
    );
}

fn partition_metadata(metadata: &MetadataResponseV0, topic_id: TopicId) -> (u32, u64, Vec<u32>) {
    let partition = metadata
        .topics
        .iter()
        .find(|topic| topic.topic_id == topic_id)
        .unwrap()
        .partitions
        .iter()
        .find(|partition| partition.partition == 0)
        .unwrap();
    (
        partition.leader_broker_id,
        partition.leader_epoch,
        partition.replica_broker_ids.clone(),
    )
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn rf3_separate_process_leader_kill_preserves_committed_quorum() {
    if std::env::var_os(PROCESS_CHILD_ID).is_some() {
        run_process_broker_child().await;
        return;
    }

    let root = temp_dir("rf3-process-kill");
    fs::create_dir_all(&root).unwrap();
    let client_addrs = (1..=3_u32)
        .map(|broker_id| (broker_id, reserve_addr()))
        .collect::<BTreeMap<_, _>>();
    let raft_addrs = (1..=3_u32)
        .map(|broker_id| (broker_id, reserve_addr()))
        .collect::<BTreeMap<_, _>>();
    let (topic_id, _) = build_cluster_with_addresses(&root, &client_addrs, &raft_addrs);
    let mut children = BrokerChildren::spawn(&root, &client_addrs, &raft_addrs);

    let child_pids = children
        .children
        .values()
        .map(|child| child.id())
        .collect::<BTreeSet<_>>();
    assert_eq!(
        child_pids.len(),
        3,
        "P4 crash qualification requires three distinct broker OS processes"
    );
    for broker_id in 1..=3_u32 {
        children.wait_ready(&root, broker_id).await;
    }

    let client = AstraClient::connect(client_config(client_addrs[&1]))
        .await
        .unwrap();
    let (leader, epoch) = wait_for_leader(&client, topic_id).await;
    let committed = client
        .produce_partition(
            topic_id,
            0,
            epoch,
            vec![record(b"committed-before-process-kill")],
            AckMode::Quorum,
        )
        .await
        .expect("pre-crash quorum Produce transport must complete")
        .expect("quorum Produce must return a response");
    assert_eq!(committed.partitions[0].error_code, PublicError::None as u16);
    assert_eq!(committed.partitions[0].base_offset, 0);

    // The committed response above is the crash boundary. Terminate the elected
    // leader process directly; no broker shutdown path or in-process drop runs.
    children.kill_and_wait(leader);
    client.close().await;

    let survivor = (1..=3_u32).find(|broker_id| *broker_id != leader).unwrap();
    let failover_client = AstraClient::connect(client_config(client_addrs[&survivor]))
        .await
        .unwrap();
    let (successor, successor_epoch) =
        wait_for_new_leader(&failover_client, topic_id, leader, epoch).await;
    assert_ne!(successor, leader);
    assert!(successor_epoch > epoch);

    let recovered = failover_client
        .fetch(FetchRequestV0 {
            isolation: IsolationLevel::ReadCommitted,
            min_bytes: 0,
            max_wait_ms: 0,
            partitions: vec![FetchPartitionRequestV0 {
                topic_id,
                partition: 0,
                leader_epoch: successor_epoch,
                fetch_offset: 0,
                max_bytes: 4096,
                max_records: 10,
            }],
            tags: Vec::new(),
        })
        .await
        .unwrap();
    assert_eq!(recovered.partitions[0].error_code, PublicError::None as u16);
    assert_eq!(recovered.partitions[0].high_watermark, 1);
    assert_eq!(recovered.partitions[0].records.len(), 1);
    assert_eq!(
        recovered.partitions[0].records[0].record.value.as_deref(),
        Some(b"committed-before-process-kill".as_slice())
    );

    // P2 Produce is non-idempotent, so this mutation is intentionally issued
    // exactly once. Any transport timeout or ambiguous response fails the test;
    // there is no blind retry after bytes may have reached the successor.
    let post_crash = failover_client
        .produce_partition(
            topic_id,
            0,
            successor_epoch,
            vec![record(b"successor-after-process-kill")],
            AckMode::Quorum,
        )
        .await
        .expect("successor quorum Produce must complete without retry")
        .expect("successor quorum Produce must return a response");
    assert_eq!(
        post_crash.partitions[0].error_code,
        PublicError::None as u16
    );
    assert_eq!(post_crash.partitions[0].base_offset, 1);

    let final_read = failover_client
        .fetch(FetchRequestV0 {
            isolation: IsolationLevel::ReadCommitted,
            min_bytes: 0,
            max_wait_ms: 0,
            partitions: vec![FetchPartitionRequestV0 {
                topic_id,
                partition: 0,
                leader_epoch: successor_epoch,
                fetch_offset: 0,
                max_bytes: 4096,
                max_records: 10,
            }],
            tags: Vec::new(),
        })
        .await
        .unwrap();
    assert_eq!(
        final_read.partitions[0].error_code,
        PublicError::None as u16
    );
    assert_eq!(final_read.partitions[0].high_watermark, 2);
    assert_eq!(final_read.partitions[0].records.len(), 2);
    assert_eq!(
        final_read.partitions[0].records[1].record.value.as_deref(),
        Some(b"successor-after-process-kill".as_slice())
    );

    failover_client.close().await;
    drop(children);
    fs::remove_dir_all(root).unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn rf3_tcp_quorum_redirect_epoch_and_failover_are_end_to_end() {
    let root = temp_dir("rf3-e2e");
    let (topic_id, configs, client_addrs) = build_cluster(&root);
    let mut handles = start_cluster(&configs).await;

    let client = AstraClient::connect(client_config(client_addrs[&1]))
        .await
        .unwrap();
    let (leader, epoch) = wait_for_leader(&client, topic_id).await;
    let metadata = client.refresh_metadata().await.unwrap();
    let (metadata_leader, metadata_epoch, replicas) = partition_metadata(&metadata, topic_id);
    assert_eq!(metadata_leader, leader);
    assert_eq!(metadata_epoch, epoch);
    assert_eq!(replicas, vec![1, 2, 3]);
    assert_eq!(metadata.brokers.len(), 3);

    let follower = (1..=3_u32).find(|broker| *broker != leader).unwrap();
    let direct = direct_produce(
        client_addrs[&follower],
        topic_id,
        UNKNOWN_EPOCH,
        b"wrong-node",
    )
    .await;
    let direct_result = &direct.partitions[0];
    assert_eq!(
        PublicError::try_from(direct_result.error_code).unwrap(),
        PublicError::NotLeader
    );
    assert_eq!(direct_result.leader_broker_id, leader);
    assert_eq!(direct_result.leader_epoch, epoch);

    // An acks=0 write sent to the wrong node is rejected internally but the
    // protocol deliberately returns no response, so the producer cannot know
    // whether the request was accepted. This is the frozen P4 fire-and-forget
    // contract rather than an accidental success acknowledgement.
    direct_no_response_produce(
        client_addrs[&follower],
        topic_id,
        UNKNOWN_EPOCH,
        b"acks-zero-wrong-node",
    )
    .await;

    let stale = client
        .produce_partition(
            topic_id,
            0,
            epoch.saturating_sub(1),
            vec![record(b"stale")],
            AckMode::Leader,
        )
        .await
        .unwrap()
        .unwrap();
    assert_eq!(
        PublicError::try_from(stale.partitions[0].error_code).unwrap(),
        PublicError::StaleEpoch
    );
    assert_eq!(stale.partitions[0].leader_broker_id, leader);
    assert_eq!(stale.partitions[0].leader_epoch, epoch);

    let future = client
        .produce_partition(
            topic_id,
            0,
            epoch.saturating_add(100),
            vec![record(b"future-epoch")],
            AckMode::Leader,
        )
        .await
        .unwrap()
        .unwrap();
    assert_eq!(
        PublicError::try_from(future.partitions[0].error_code).unwrap(),
        PublicError::StaleEpoch
    );
    assert_eq!(future.partitions[0].leader_broker_id, leader);
    assert_eq!(future.partitions[0].leader_epoch, epoch);

    let quorum = client
        .produce_partition(topic_id, 0, epoch, vec![record(b"first")], AckMode::Quorum)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(quorum.partitions[0].error_code, PublicError::None as u16);
    assert_eq!(quorum.partitions[0].base_offset, 0);

    let fetch = client
        .fetch(FetchRequestV0 {
            isolation: IsolationLevel::ReadCommitted,
            min_bytes: 0,
            max_wait_ms: 0,
            partitions: vec![FetchPartitionRequestV0 {
                topic_id,
                partition: 0,
                leader_epoch: epoch,
                fetch_offset: 0,
                max_bytes: 4096,
                max_records: 10,
            }],
            tags: Vec::new(),
        })
        .await
        .unwrap();
    assert_eq!(fetch.partitions[0].records.len(), 1);
    assert_eq!(fetch.partitions[0].high_watermark, 1);
    let latest = client
        .list_offsets(ListOffsetsRequestV0 {
            queries: vec![ListOffsetsQueryV0 {
                topic_id,
                partition: 0,
                leader_epoch: epoch,
                timestamp_selector: -1,
            }],
            tags: Vec::new(),
        })
        .await
        .unwrap();
    assert_eq!(latest.results[0].error_code, PublicError::None as u16);
    assert_eq!(latest.results[0].offset, 1);

    let stopped_follower = follower;
    handles
        .remove(&stopped_follower)
        .unwrap()
        .shutdown()
        .await
        .unwrap();
    let (leader_after_follower_stop, epoch_after_follower_stop) =
        wait_for_leader(&client, topic_id).await;
    assert_ne!(leader_after_follower_stop, stopped_follower);
    let second = client
        .produce_partition(
            topic_id,
            0,
            epoch_after_follower_stop,
            vec![record(b"second")],
            AckMode::Quorum,
        )
        .await
        .unwrap()
        .unwrap();
    assert_eq!(second.partitions[0].error_code, PublicError::None as u16);
    assert_eq!(second.partitions[0].base_offset, 1);

    handles.insert(
        stopped_follower,
        start(configs[&stopped_follower].clone()).await.unwrap(),
    );
    time::sleep(Duration::from_millis(250)).await;

    client.close().await;
    handles
        .remove(&leader_after_follower_stop)
        .unwrap()
        .shutdown()
        .await
        .unwrap();
    let survivor = (1..=3_u32)
        .find(|broker| *broker != leader_after_follower_stop && handles.contains_key(broker))
        .unwrap();
    let failover_client = AstraClient::connect(client_config(client_addrs[&survivor]))
        .await
        .unwrap();
    let (new_leader, new_epoch) = wait_for_new_leader(
        &failover_client,
        topic_id,
        leader_after_follower_stop,
        epoch_after_follower_stop,
    )
    .await;
    assert_ne!(new_leader, leader_after_follower_stop);
    assert!(new_epoch > epoch_after_follower_stop);

    let after_failover = failover_client
        .fetch(FetchRequestV0 {
            isolation: IsolationLevel::ReadCommitted,
            min_bytes: 0,
            max_wait_ms: 0,
            partitions: vec![FetchPartitionRequestV0 {
                topic_id,
                partition: 0,
                leader_epoch: new_epoch,
                fetch_offset: 0,
                max_bytes: 4096,
                max_records: 10,
            }],
            tags: Vec::new(),
        })
        .await
        .unwrap();
    assert_eq!(after_failover.partitions[0].records.len(), 2);
    assert_eq!(after_failover.partitions[0].high_watermark, 2);
    assert_eq!(
        after_failover.partitions[0].records[0]
            .record
            .value
            .as_deref(),
        Some(b"first".as_slice())
    );
    assert_eq!(
        after_failover.partitions[0].records[1]
            .record
            .value
            .as_deref(),
        Some(b"second".as_slice())
    );

    // Reopen the former leader from its P1 bytes + P4 sidecars, then remove the
    // other follower. The next quorum write can succeed only if that restarted
    // broker reconstructed P3 state and caught up correctly.
    handles.insert(
        leader_after_follower_stop,
        start(configs[&leader_after_follower_stop].clone())
            .await
            .unwrap(),
    );
    time::sleep(Duration::from_millis(350)).await;
    let other_follower = handles
        .keys()
        .copied()
        .find(|broker| *broker != new_leader && *broker != leader_after_follower_stop)
        .unwrap();
    handles
        .remove(&other_follower)
        .unwrap()
        .shutdown()
        .await
        .unwrap();

    let after_restart = failover_client
        .produce_partition(
            topic_id,
            0,
            new_epoch,
            vec![record(b"restart-quorum")],
            AckMode::Quorum,
        )
        .await
        .unwrap()
        .unwrap();
    assert_eq!(
        after_restart.partitions[0].error_code,
        PublicError::None as u16
    );
    assert_eq!(after_restart.partitions[0].base_offset, 2);

    let latest_after_restart = failover_client
        .list_offsets(ListOffsetsRequestV0 {
            queries: vec![ListOffsetsQueryV0 {
                topic_id,
                partition: 0,
                leader_epoch: new_epoch,
                timestamp_selector: -1,
            }],
            tags: Vec::new(),
        })
        .await
        .unwrap();
    assert_eq!(latest_after_restart.results[0].offset, 3);

    let final_fetch = failover_client
        .fetch(FetchRequestV0 {
            isolation: IsolationLevel::ReadCommitted,
            min_bytes: 0,
            max_wait_ms: 0,
            partitions: vec![FetchPartitionRequestV0 {
                topic_id,
                partition: 0,
                leader_epoch: new_epoch,
                fetch_offset: 0,
                max_bytes: 4096,
                max_records: 10,
            }],
            tags: Vec::new(),
        })
        .await
        .unwrap();
    assert_eq!(final_fetch.partitions[0].records.len(), 3);
    assert_eq!(final_fetch.partitions[0].high_watermark, 3);
    assert_eq!(
        final_fetch.partitions[0].records[2].record.value.as_deref(),
        Some(b"restart-quorum".as_slice())
    );

    // Leader-side acks=0 goes through the real NO_RESPONSE client path. The
    // API returns None because no acknowledgement is promised or emitted.
    let no_response = failover_client
        .produce_partition(
            topic_id,
            0,
            new_epoch,
            vec![record(b"fire-and-forget")],
            AckMode::None,
        )
        .await
        .unwrap();
    assert!(no_response.is_none());

    failover_client.close().await;
    for (_, handle) in handles {
        handle.shutdown().await.unwrap();
    }
    let _ = fs::remove_dir_all(root);
}
