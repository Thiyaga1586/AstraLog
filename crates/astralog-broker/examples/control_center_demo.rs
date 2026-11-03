//! Local RF3 topology for the read-only AstraLog Control Center.
//!
//! This is a disposable, static P4 cluster. It is not a production deployment.
//! Ctrl+C stops the example process. It performs no networking beyond loopback.

use std::{
    net::{Ipv4Addr, SocketAddr},
    path::PathBuf,
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use astralog_broker::{
    BrokerConfig, StaticBrokerConfig, StaticReplicationConfig, StaticTopicConfig, start,
};
use astralog_client::{AstraClient, ClientConfig};
use astralog_common::TopicId;
use astralog_protocol::api::{AckMode, FetchPartitionRequestV0, FetchRequestV0, IsolationLevel};

fn local_addr(port: u16) -> SocketAddr {
    SocketAddr::from((Ipv4Addr::LOCALHOST, port))
}

struct DemoArgs {
    port: u16,
    traffic: bool,
}

fn demo_args() -> Result<DemoArgs, String> {
    let mut args = std::env::args().skip(1);
    let mut options = DemoArgs {
        port: 7001,
        traffic: false,
    };
    while let Some(arg) = args.next() {
        match arg.as_str() {
            "--help" | "-h" => {
                println!("AstraLog local RF3 Control Center demo");
                println!(
                    "Usage: cargo run -p astralog-broker --example control_center_demo -- [--base-port PORT] [--traffic]"
                );
                println!(
                    "Starts three static brokers and optional real synthetic Produce/Fetch traffic."
                );
                println!("Broker ports: PORT..PORT+2, Raft ports: PORT+100..PORT+102.");
                println!("Prometheus endpoints: PORT+2200..PORT+2202, loopback only.");
                println!("All files use disposable temporary storage; no authentication.");
                std::process::exit(0);
            }
            "--traffic" => options.traffic = true,
            "--base-port" => {
                options.port = args
                    .next()
                    .ok_or("--base-port requires a number")?
                    .parse::<u16>()
                    .map_err(|_| "base port must be a valid u16")?;
            }
            _ => return Err(format!("unknown argument: {arg}")),
        }
    }
    if !(1024..=63000).contains(&options.port) {
        return Err("base port must be between 1024 and 63000".into());
    }
    Ok(options)
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let options = demo_args()?;
    let port = options.port;
    let nanos = SystemTime::now().duration_since(UNIX_EPOCH)?.as_nanos();
    let root: PathBuf = std::env::temp_dir().join(format!(
        "astralog-control-center-rf3-{}-{nanos}",
        std::process::id()
    ));

    let cluster_id = [0x61; 16];
    let brokers = (1..=3_u32)
        .map(|id| {
            let n = u16::try_from(id - 1).expect("demo broker id within range");
            StaticBrokerConfig {
                broker_id: id,
                node_uuid: [u8::try_from(id).expect("demo id fits into u8"); 16],
                client_host: "127.0.0.1".into(),
                client_port: port + n,
                raft_address: local_addr(port + 100 + n),
                transport_incarnation: u64::from(id) * 10,
            }
        })
        .collect::<Vec<_>>();

    let topics = [
        ("orders", [0x31; 16], 4_usize),
        ("payments", [0x32; 16], 3),
        ("notifications", [0x33; 16], 2),
    ]
    .into_iter()
    .map(|(name, id, partitions)| StaticTopicConfig {
        name: name.into(),
        topic_id: TopicId::new(id),
        partition_replicas: vec![vec![1, 2, 3]; partitions],
    })
    .collect::<Vec<_>>();

    let mut handles = Vec::new();
    for id in 1..=3_u32 {
        let n = u16::try_from(id - 1).expect("demo broker id within range");
        let mut config = BrokerConfig::new(root.join(format!("broker-{id}")), local_addr(port + n));
        config.broker_id = id;
        config.advertised_host = "127.0.0.1".into();
        config.advertised_port = port + n;
        config.metrics_bind_addr = Some(local_addr(port + 2200 + n));
        config.max_frame_len = 1024 * 1024;
        config.max_in_flight_per_connection = 16;
        config.max_pending_response_bytes = 20 * 1024 * 1024;
        let mut static_config = StaticReplicationConfig::new(
            cluster_id,
            local_addr(port + 100 + n),
            brokers.clone(),
            topics.clone(),
        );
        static_config.tick_interval = Duration::from_millis(20);
        let first_tick = 5 + (u64::from(id) - 1) * 3;
        static_config.election_timeout_min_ticks = first_tick;
        static_config.election_timeout_max_ticks = first_tick + 1;
        static_config.reconnect_min_delay = Duration::from_millis(5);
        static_config.reconnect_max_delay = Duration::from_millis(50);
        config.static_replication = Some(static_config);
        handles.push(start(config).await?);
    }

    println!("AstraLog Control Center: real RF3 static demo started");
    println!("Broker Metadata V0: 127.0.0.1:{port}");
    println!("Brokers: {}, {}, {}", port, port + 1, port + 2);
    println!("Topics: orders (4), payments (3), notifications (2)");
    println!(
        "Prometheus scrapes: {}, {}, {}",
        port + 2200,
        port + 2201,
        port + 2202
    );
    println!("The broker election may need a few seconds before leaders appear.");
    println!("Start the read-only adapter with --bootstrap 127.0.0.1:{port}");
    println!("This disposable local topology is distinct from performance benchmarks.");
    if options.traffic {
        println!(
            "Synthetic workload enabled: real acknowledged Produce and committed Fetch requests."
        );
        println!("This workload is test traffic, not an independent performance benchmark.");
        tokio::spawn(async move {
            let mut attempts = 0_u64;
            loop {
                let mut config = ClientConfig::new(local_addr(port).to_string());
                config.request_timeout = Duration::from_secs(5);
                match AstraClient::connect(config).await {
                    Ok(client) => {
                        let mut ticker = tokio::time::interval(Duration::from_millis(125));
                        ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
                        loop {
                            ticker.tick().await;
                            let payload = format!("control-center-synthetic-{attempts:08}");
                            attempts = attempts.wrapping_add(1);
                            if let Ok(ack) = client
                                .produce(
                                    "orders",
                                    None,
                                    Some(payload.into_bytes()),
                                    Vec::new(),
                                    AckMode::Quorum,
                                )
                                .await
                                && let Some(offset) = ack.base_offset
                            {
                                let _ = client
                                    .fetch(FetchRequestV0 {
                                        isolation: IsolationLevel::ReadCommitted,
                                        min_bytes: 0,
                                        max_wait_ms: 0,
                                        partitions: vec![FetchPartitionRequestV0 {
                                            topic_id: TopicId::new([0x31; 16]),
                                            partition: ack.partition,
                                            leader_epoch: u64::MAX,
                                            fetch_offset: offset,
                                            max_bytes: 4096,
                                            max_records: 1,
                                        }],
                                        tags: Vec::new(),
                                    })
                                    .await;
                            }
                        }
                    }
                    Err(error) => {
                        eprintln!(
                            "Synthetic traffic client retrying after connection failure: {error}"
                        );
                        tokio::time::sleep(Duration::from_secs(2)).await;
                    }
                }
            }
        });
    }
    println!("Press Ctrl+C to terminate this demonstration.");

    // Keep all three broker actors and their independent partition Raft groups alive.
    // The operating system handles Ctrl+C. Temp files are disposable by design.
    let _brokers = handles;
    std::future::pending::<()>().await;
    Ok(())
}
