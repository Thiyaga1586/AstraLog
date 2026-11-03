use std::{
    fs,
    net::SocketAddr,
    path::PathBuf,
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use astralog_broker::{BrokerConfig, start};
use astralog_client::{AstraClient, ClientConfig};
use astralog_common::TopicId;
use astralog_protocol::{
    ApiKey, ClientHello, FrameKind, MAX_WIRE_FRAME_LEN_V1, PublicError, ServerHello, WIRE_VERSION,
    WireFrame,
    api::{
        AckMode, ApiVersionsRequestV0, BodyCodec, FetchPartitionRequestV0, FetchRequestV0,
        IsolationLevel, ListOffsetsQueryV0, ListOffsetsRequestV0, MetadataRequestV0,
        ProducePartitionRequestV0, ProduceRequestV0, WireRecordV0,
    },
    decode_frame, encode_frame,
};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::TcpStream,
    time,
};

fn temp_dir(name: &str) -> PathBuf {
    let nonce = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    std::env::temp_dir().join(format!(
        "astralog-p2-network-{name}-{}-{nonce}",
        std::process::id()
    ))
}

fn broker_config(dir: PathBuf) -> BrokerConfig {
    let mut config = BrokerConfig::new(dir, "127.0.0.1:0".parse::<SocketAddr>().unwrap());
    config.advertised_port = 0;
    config.max_frame_len = 1024 * 1024;
    config.max_in_flight_per_connection = 8;
    config.max_pending_response_bytes = 9 * 1024 * 1024;
    config.frame_assembly_timeout = Duration::from_millis(500);
    config
}

fn client_config(addr: SocketAddr) -> ClientConfig {
    let mut config = ClientConfig::new(addr.to_string());
    config.max_frame_len = 1024 * 1024;
    config.request_timeout = Duration::from_secs(3);
    config
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn single_broker_round_trip_survives_restart() {
    let dir = temp_dir("restart");
    let broker = start(broker_config(dir.clone())).await.unwrap();
    let client = AstraClient::connect(client_config(broker.local_addr()))
        .await
        .unwrap();
    let first_metadata = client.metadata().await.unwrap();
    let first_cluster = first_metadata.cluster_id;
    let first_node = first_metadata.brokers[0].node_uuid;
    let first_incarnation = first_metadata.brokers[0].incarnation_epoch;

    let created = client.create_topic("orders", 3).await.unwrap();
    let outcome = client
        .produce(
            "orders",
            Some(b"order-7".to_vec()),
            Some(b"total=42".to_vec()),
            Vec::new(),
            AckMode::Quorum,
        )
        .await
        .unwrap();
    assert_eq!(outcome.base_offset, Some(0));
    client
        .produce_partition(
            created.topic_id,
            outcome.partition,
            u64::MAX,
            vec![WireRecordV0 {
                timestamp_ms: 9_000_000_000_000,
                key: Some(b"order-8".to_vec()),
                value: Some(b"leader-durable".to_vec()),
                headers: Vec::new(),
            }],
            AckMode::Leader,
        )
        .await
        .unwrap();

    let fetch = client
        .fetch(FetchRequestV0 {
            isolation: IsolationLevel::ReadCommitted,
            min_bytes: 0,
            max_wait_ms: 0,
            partitions: vec![FetchPartitionRequestV0 {
                topic_id: created.topic_id,
                partition: outcome.partition,
                leader_epoch: u64::MAX,
                fetch_offset: 0,
                max_bytes: 4096,
                max_records: 10,
            }],
            tags: Vec::new(),
        })
        .await
        .unwrap();
    assert_eq!(fetch.partitions[0].records.len(), 2);
    assert_eq!(fetch.partitions[0].high_watermark, 2);

    let offsets = client
        .list_offsets(ListOffsetsRequestV0 {
            queries: vec![
                ListOffsetsQueryV0 {
                    topic_id: created.topic_id,
                    partition: outcome.partition,
                    leader_epoch: u64::MAX,
                    timestamp_selector: -2,
                },
                ListOffsetsQueryV0 {
                    topic_id: created.topic_id,
                    partition: outcome.partition,
                    leader_epoch: u64::MAX,
                    timestamp_selector: -1,
                },
                ListOffsetsQueryV0 {
                    topic_id: created.topic_id,
                    partition: outcome.partition,
                    leader_epoch: u64::MAX,
                    timestamp_selector: 9_000_000_000_000,
                },
            ],
            tags: Vec::new(),
        })
        .await
        .unwrap();
    assert_eq!(offsets.results[0].offset, 0);
    assert_eq!(offsets.results[1].offset, 2);
    assert_eq!(offsets.results[2].offset, 1);
    assert_eq!(offsets.results[2].timestamp_ms, 9_000_000_000_000);

    client.close().await;
    broker.shutdown().await.unwrap();

    let broker = start(broker_config(dir.clone())).await.unwrap();
    let client = AstraClient::connect(client_config(broker.local_addr()))
        .await
        .unwrap();
    let second_metadata = client.metadata().await.unwrap();
    assert_eq!(second_metadata.cluster_id, first_cluster);
    assert_eq!(second_metadata.brokers[0].node_uuid, first_node);
    assert!(second_metadata.brokers[0].incarnation_epoch > first_incarnation);
    assert!(second_metadata.metadata_epoch > first_metadata.metadata_epoch);

    let fetch = client
        .fetch(FetchRequestV0 {
            isolation: IsolationLevel::ReadCommitted,
            min_bytes: 0,
            max_wait_ms: 0,
            partitions: vec![FetchPartitionRequestV0 {
                topic_id: created.topic_id,
                partition: outcome.partition,
                leader_epoch: u64::MAX,
                fetch_offset: 0,
                max_bytes: 4096,
                max_records: 10,
            }],
            tags: Vec::new(),
        })
        .await
        .unwrap();
    assert_eq!(fetch.partitions[0].records.len(), 2);
    assert_eq!(
        fetch.partitions[0].records[0].record.value.as_deref(),
        Some(&b"total=42"[..])
    );
    assert_eq!(
        fetch.partitions[0].records[1].record.value.as_deref(),
        Some(&b"leader-durable"[..])
    );

    client.close().await;
    broker.shutdown().await.unwrap();
    let _ = fs::remove_dir_all(dir);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn one_connection_multiplexes_long_fetch_and_fast_list_offsets() {
    let dir = temp_dir("multiplex");
    let broker = start(broker_config(dir.clone())).await.unwrap();
    let client = AstraClient::connect(client_config(broker.local_addr()))
        .await
        .unwrap();
    let created = client.create_topic("events", 1).await.unwrap();

    let long_poll_client = client.clone();
    let long_poll = tokio::spawn(async move {
        long_poll_client
            .fetch(FetchRequestV0 {
                isolation: IsolationLevel::ReadCommitted,
                min_bytes: 1,
                max_wait_ms: 1_500,
                partitions: vec![FetchPartitionRequestV0 {
                    topic_id: created.topic_id,
                    partition: 0,
                    leader_epoch: u64::MAX,
                    fetch_offset: 0,
                    max_bytes: 4096,
                    max_records: 10,
                }],
                tags: Vec::new(),
            })
            .await
    });

    time::sleep(Duration::from_millis(75)).await;
    let started = time::Instant::now();
    let offsets = client
        .list_offsets(ListOffsetsRequestV0 {
            queries: vec![ListOffsetsQueryV0 {
                topic_id: created.topic_id,
                partition: 0,
                leader_epoch: u64::MAX,
                timestamp_selector: -1,
            }],
            tags: Vec::new(),
        })
        .await
        .unwrap();
    assert_eq!(offsets.results[0].offset, 0);
    assert!(
        started.elapsed() < Duration::from_millis(700),
        "fast request was head-of-line blocked by long poll"
    );

    client
        .produce(
            "events",
            Some(b"k".to_vec()),
            Some(b"v".to_vec()),
            Vec::new(),
            AckMode::Quorum,
        )
        .await
        .unwrap();
    let fetch = long_poll.await.unwrap().unwrap();
    assert_eq!(fetch.partitions[0].records.len(), 1);

    client.close().await;
    broker.shutdown().await.unwrap();
    let _ = fs::remove_dir_all(dir);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn fragmented_api_versions_frame_is_accepted_byte_by_byte() {
    let dir = temp_dir("fragmented");
    let broker = start(broker_config(dir.clone())).await.unwrap();
    let mut stream = raw_hello(broker.local_addr()).await;
    let frame = api_versions_frame(7);
    let bytes = encode_frame(&frame, MAX_WIRE_FRAME_LEN_V1).unwrap();
    for byte in bytes {
        stream.write_all(&[byte]).await.unwrap();
    }
    let response = read_raw_frame(&mut stream, 1024 * 1024).await;
    assert_eq!(response.correlation_id, 7);
    assert_eq!(response.error_code, 0);

    broker.shutdown().await.unwrap();
    let _ = fs::remove_dir_all(dir);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn oversized_prefix_and_slowloris_partial_frame_are_closed() {
    let dir = temp_dir("bounds");
    let mut config = broker_config(dir.clone());
    config.max_frame_len = 1024;
    config.max_in_flight_per_connection = 2;
    config.max_pending_response_bytes = 4096;
    config.frame_assembly_timeout = Duration::from_millis(120);
    let broker = start(config).await.unwrap();

    let mut oversized = raw_hello(broker.local_addr()).await;
    oversized.write_all(&1025_u32.to_be_bytes()).await.unwrap();
    let mut one = [0_u8; 1];
    let closed = time::timeout(Duration::from_secs(1), oversized.read(&mut one))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(closed, 0);

    let mut slow = raw_hello(broker.local_addr()).await;
    slow.write_all(&[0]).await.unwrap();
    time::sleep(Duration::from_millis(250)).await;
    let closed = time::timeout(Duration::from_secs(1), slow.read(&mut one))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(closed, 0);

    broker.shutdown().await.unwrap();
    let _ = fs::remove_dir_all(dir);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn duplicate_active_correlation_id_closes_connection() {
    let dir = temp_dir("duplicate-correlation");
    let broker = start(broker_config(dir.clone())).await.unwrap();
    let client = AstraClient::connect(client_config(broker.local_addr()))
        .await
        .unwrap();
    let created = client.create_topic("empty", 1).await.unwrap();

    let mut stream = raw_hello(broker.local_addr()).await;
    let api = api_versions_frame(1);
    stream
        .write_all(&encode_frame(&api, MAX_WIRE_FRAME_LEN_V1).unwrap())
        .await
        .unwrap();
    let _ = read_raw_frame(&mut stream, 1024 * 1024).await;

    let fetch = FetchRequestV0 {
        isolation: IsolationLevel::ReadCommitted,
        min_bytes: 1,
        max_wait_ms: 1_000,
        partitions: vec![FetchPartitionRequestV0 {
            topic_id: created.topic_id,
            partition: 0,
            leader_epoch: u64::MAX,
            fetch_offset: 0,
            max_bytes: 4096,
            max_records: 10,
        }],
        tags: Vec::new(),
    };
    let long = request_frame(ApiKey::Fetch, 99, 2_000, fetch.encode_body().unwrap());
    let metadata = MetadataRequestV0 {
        all_topics: true,
        topic_names: Vec::new(),
        tags: Vec::new(),
    };
    let duplicate = request_frame(ApiKey::Metadata, 99, 2_000, metadata.encode_body().unwrap());
    let mut combined = encode_frame(&long, MAX_WIRE_FRAME_LEN_V1).unwrap();
    combined.extend_from_slice(&encode_frame(&duplicate, MAX_WIRE_FRAME_LEN_V1).unwrap());
    stream.write_all(&combined).await.unwrap();

    let mut byte = [0_u8; 1];
    let closed = time::timeout(Duration::from_secs(1), stream.read(&mut byte))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(closed, 0);

    client.close().await;
    broker.shutdown().await.unwrap();
    let _ = fs::remove_dir_all(dir);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn stale_epoch_response_refreshes_client_metadata_cache() {
    let dir = temp_dir("metadata-refresh");
    let broker = start(broker_config(dir.clone())).await.unwrap();
    let first = AstraClient::connect(client_config(broker.local_addr()))
        .await
        .unwrap();
    let second = AstraClient::connect(client_config(broker.local_addr()))
        .await
        .unwrap();

    let base = first.create_topic("base", 1).await.unwrap();
    let before = first.metadata().await.unwrap();
    assert!(
        !before
            .topics
            .iter()
            .any(|topic| topic.topic_name == "later")
    );

    second.create_topic("later", 1).await.unwrap();
    let response = first
        .fetch(FetchRequestV0 {
            isolation: IsolationLevel::ReadCommitted,
            min_bytes: 0,
            max_wait_ms: 0,
            partitions: vec![FetchPartitionRequestV0 {
                topic_id: base.topic_id,
                partition: 0,
                leader_epoch: 999,
                fetch_offset: 0,
                max_bytes: 4096,
                max_records: 10,
            }],
            tags: Vec::new(),
        })
        .await
        .unwrap();
    assert_eq!(
        PublicError::try_from(response.partitions[0].error_code).unwrap(),
        PublicError::StaleEpoch
    );

    let refreshed = first.metadata().await.unwrap();
    assert!(refreshed.metadata_epoch > before.metadata_epoch);
    assert!(
        refreshed
            .topics
            .iter()
            .any(|topic| topic.topic_name == "later")
    );

    first.close().await;
    second.close().await;
    broker.shutdown().await.unwrap();
    let _ = fs::remove_dir_all(dir);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn allocated_future_api_is_unsupported_key_and_wrong_p2_version_is_unsupported_version() {
    let dir = temp_dir("api-errors");
    let broker = start(broker_config(dir.clone())).await.unwrap();
    let mut stream = raw_hello(broker.local_addr()).await;

    let api = api_versions_frame(1);
    stream
        .write_all(&encode_frame(&api, MAX_WIRE_FRAME_LEN_V1).unwrap())
        .await
        .unwrap();
    let _ = read_raw_frame(&mut stream, 1024 * 1024).await;

    let future = request_frame(ApiKey::DescribeCluster, 2, 2_000, Vec::new());
    stream
        .write_all(&encode_frame(&future, MAX_WIRE_FRAME_LEN_V1).unwrap())
        .await
        .unwrap();
    let response = read_raw_frame(&mut stream, 1024 * 1024).await;
    assert_eq!(
        PublicError::try_from(response.error_code).unwrap(),
        PublicError::UnsupportedApiKey
    );

    let mut wrong_version = request_frame(
        ApiKey::Metadata,
        3,
        2_000,
        MetadataRequestV0 {
            all_topics: true,
            topic_names: Vec::new(),
            tags: Vec::new(),
        }
        .encode_body()
        .unwrap(),
    );
    wrong_version.api_version = 1;
    stream
        .write_all(&encode_frame(&wrong_version, MAX_WIRE_FRAME_LEN_V1).unwrap())
        .await
        .unwrap();
    let response = read_raw_frame(&mut stream, 1024 * 1024).await;
    assert_eq!(
        PublicError::try_from(response.error_code).unwrap(),
        PublicError::UnsupportedApiVersion
    );

    broker.shutdown().await.unwrap();
    let _ = fs::remove_dir_all(dir);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn multi_partition_fetch_is_truncated_to_negotiated_frame_budget() {
    let dir = temp_dir("fetch-budget");
    let mut config = broker_config(dir.clone());
    config.max_frame_len = 2048;
    let broker = start(config).await.unwrap();
    let mut client_config = client_config(broker.local_addr());
    client_config.max_frame_len = 2048;
    let client = AstraClient::connect(client_config).await.unwrap();
    let created = client.create_topic("bounded", 2).await.unwrap();

    for partition in 0..2 {
        client
            .produce_partition(
                created.topic_id,
                partition,
                u64::MAX,
                vec![WireRecordV0 {
                    timestamp_ms: -1,
                    key: Some(vec![partition as u8]),
                    value: Some(vec![b'x'; 1_000]),
                    headers: Vec::new(),
                }],
                AckMode::Quorum,
            )
            .await
            .unwrap();
    }

    let response = client
        .fetch(FetchRequestV0 {
            isolation: IsolationLevel::ReadCommitted,
            min_bytes: 0,
            max_wait_ms: 0,
            partitions: (0..2)
                .map(|partition| FetchPartitionRequestV0 {
                    topic_id: created.topic_id,
                    partition,
                    leader_epoch: u64::MAX,
                    fetch_offset: 0,
                    max_bytes: 2_000,
                    max_records: 10,
                })
                .collect(),
            tags: Vec::new(),
        })
        .await
        .unwrap();
    let returned_records: usize = response
        .partitions
        .iter()
        .map(|partition| partition.records.len())
        .sum();
    assert_eq!(returned_records, 1);
    assert!(
        response
            .partitions
            .iter()
            .all(|partition| partition.high_watermark == 1)
    );

    client.close().await;
    broker.shutdown().await.unwrap();
    let _ = fs::remove_dir_all(dir);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn topic_admin_lifecycle_is_authoritative_across_restart() {
    let dir = temp_dir("admin-lifecycle");
    let broker = start(broker_config(dir.clone())).await.unwrap();
    let client = AstraClient::connect(client_config(broker.local_addr()))
        .await
        .unwrap();

    let created = client.create_topic("temporary", 2).await.unwrap();
    let described = client.describe_topic("temporary").await.unwrap();
    assert_eq!(described.topic_id, created.topic_id);
    assert_eq!(described.partition_count, 2);

    let deleted = client.delete_topic("temporary").await.unwrap();
    assert_eq!(deleted.deleted_topic_id, created.topic_id);
    assert!(client.describe_topic("temporary").await.is_err());

    client.close().await;
    broker.shutdown().await.unwrap();

    let broker = start(broker_config(dir.clone())).await.unwrap();
    let client = AstraClient::connect(client_config(broker.local_addr()))
        .await
        .unwrap();
    assert!(
        client
            .metadata()
            .await
            .unwrap()
            .topics
            .iter()
            .all(|topic| topic.topic_name != "temporary")
    );
    let recreated = client.create_topic("temporary", 1).await.unwrap();
    assert_ne!(recreated.topic_id, created.topic_id);

    client.close().await;
    broker.shutdown().await.unwrap();
    let _ = fs::remove_dir_all(dir);
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

fn api_versions_frame(correlation_id: u64) -> WireFrame {
    let request = ApiVersionsRequestV0 {
        client_software_name: "network-test".into(),
        client_software_version: "0.1".into(),
        tags: Vec::new(),
    };
    request_frame(
        ApiKey::ApiVersions,
        correlation_id,
        2_000,
        request.encode_body().unwrap(),
    )
}

fn request_frame(api: ApiKey, correlation_id: u64, timeout_ms: u32, body: Vec<u8>) -> WireFrame {
    WireFrame {
        kind: FrameKind::Request,
        flags: 0,
        api_key: api as u16,
        api_version: 0,
        error_code: 0,
        correlation_id,
        timeout_ms,
        throttle_ms: 0,
        header_tags: Vec::new(),
        body,
    }
}

async fn read_raw_frame(stream: &mut TcpStream, max_frame_len: usize) -> WireFrame {
    let mut prefix = [0_u8; 4];
    stream.read_exact(&mut prefix).await.unwrap();
    let len = usize::try_from(u32::from_be_bytes(prefix)).unwrap();
    assert!(len <= max_frame_len);
    let mut bytes = vec![0_u8; len + 4];
    bytes[..4].copy_from_slice(&prefix);
    stream.read_exact(&mut bytes[4..]).await.unwrap();
    decode_frame(&bytes, max_frame_len).unwrap()
}

#[allow(dead_code)]
fn _compile_produce_shape(topic_id: TopicId) -> ProduceRequestV0 {
    ProduceRequestV0 {
        acks: AckMode::Quorum,
        partitions: vec![ProducePartitionRequestV0 {
            topic_id,
            partition: 0,
            leader_epoch: u64::MAX,
            records: vec![WireRecordV0 {
                timestamp_ms: -1,
                key: None,
                value: None,
                headers: Vec::new(),
            }],
        }],
        tags: Vec::new(),
    }
}
