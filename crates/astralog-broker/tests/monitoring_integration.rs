use std::{
    fs,
    net::{SocketAddr, TcpListener as StdTcpListener},
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use astralog_broker::{BrokerConfig, start};
use astralog_client::{AstraClient, ClientConfig};
use astralog_protocol::api::{AckMode, FetchPartitionRequestV0, FetchRequestV0, IsolationLevel};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::TcpStream,
    time,
};

fn available_loopback_addr() -> SocketAddr {
    let listener = StdTcpListener::bind("127.0.0.1:0").unwrap();
    listener.local_addr().unwrap()
}

async fn read_metrics(addr: SocketAddr) -> String {
    time::timeout(Duration::from_secs(3), async {
        let mut stream = TcpStream::connect(addr).await.unwrap();
        stream
            .write_all(b"GET /metrics HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n\r\n")
            .await
            .unwrap();
        let mut bytes = Vec::new();
        stream.read_to_end(&mut bytes).await.unwrap();
        String::from_utf8(bytes).unwrap()
    })
    .await
    .expect("metrics HTTP response must be bounded")
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn real_produce_fetch_updates_opt_in_prometheus_metrics() {
    let nonce = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    let data = std::env::temp_dir().join(format!(
        "astralog-metrics-integration-{}-{nonce}",
        std::process::id()
    ));
    let http_addr = available_loopback_addr();
    let mut config = BrokerConfig::new(&data, "127.0.0.1:0".parse().unwrap());
    config.metrics_bind_addr = Some(http_addr);
    let broker = start(config).await.unwrap();
    let mut client_config = ClientConfig::new(broker.local_addr().to_string());
    client_config.request_timeout = Duration::from_secs(4);
    let client = AstraClient::connect(client_config).await.unwrap();
    let topic = client.create_topic("metrics-test", 1).await.unwrap();

    let produced = client
        .produce(
            "metrics-test",
            Some(b"trace".to_vec()),
            Some(b"real-payload".to_vec()),
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
                topic_id: topic.topic_id,
                partition: produced.partition,
                leader_epoch: u64::MAX,
                fetch_offset: produced.base_offset.expect("successful offset"),
                max_bytes: 4096,
                max_records: 2,
            }],
            tags: Vec::new(),
        })
        .await
        .unwrap();
    assert_eq!(fetched.partitions[0].records.len(), 1);

    let body = read_metrics(http_addr).await;
    assert!(body.contains("HTTP/1.1 200 OK"), "{body}");
    assert!(body.contains("astralog_produce_records_total 1"), "{body}");
    assert!(body.contains("astralog_produce_bytes_total 12"), "{body}");
    assert!(body.contains("astralog_fetch_records_total 1"), "{body}");
    assert!(body.contains("astralog_fetch_bytes_total 12"), "{body}");
    assert!(
        body.contains("astralog_broker_requests_total{api=\"produce\",result=\"ok\"} 1"),
        "{body}"
    );
    assert!(
        body.contains("astralog_broker_requests_total{api=\"fetch\",result=\"ok\"} 1"),
        "{body}"
    );
    assert!(
        body.contains(
            "astralog_broker_request_duration_seconds_bucket{api=\"produce\",le=\"+Inf\"} 1"
        ),
        "{body}"
    );

    client.close().await;
    broker.shutdown().await.unwrap();
    let _ = fs::remove_dir_all(data);
}
