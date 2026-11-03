#![forbid(unsafe_code)]

use std::{
    net::SocketAddr,
    str::FromStr,
    sync::Arc,
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use astralog_client::{AstraClient, ClientConfig, ClientError};
use astralog_protocol::{UNKNOWN_BROKER_ID, UNKNOWN_EPOCH, api::MetadataResponseV0};
use axum::{
    Json, Router,
    extract::State,
    http::{HeaderValue, Method, header},
    routing::get,
};
use serde::Serialize;
use tokio::{net::TcpListener, sync::RwLock, time};
use tower_http::cors::CorsLayer;

pub const DEFAULT_BIND: &str = "127.0.0.1:8787";
pub const DEFAULT_BOOTSTRAP: &str = "127.0.0.1:9092";
pub const DEFAULT_POLL_INTERVAL_MS: u64 = 2_000;
pub const DEFAULT_POLL_TIMEOUT_MS: u64 = 1_500;
pub const DEFAULT_VITE_ORIGINS: [&str; 2] = ["http://127.0.0.1:5173", "http://localhost:5173"];

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DashboardConfig {
    pub bind_addr: SocketAddr,
    pub bootstrap_endpoint: String,
    pub poll_interval: Duration,
    pub poll_timeout: Duration,
    pub vite_origins: Vec<String>,
}

impl Default for DashboardConfig {
    fn default() -> Self {
        Self {
            bind_addr: DEFAULT_BIND.parse().expect("valid dashboard default bind"),
            bootstrap_endpoint: DEFAULT_BOOTSTRAP.to_owned(),
            poll_interval: Duration::from_millis(DEFAULT_POLL_INTERVAL_MS),
            poll_timeout: Duration::from_millis(DEFAULT_POLL_TIMEOUT_MS),
            vite_origins: DEFAULT_VITE_ORIGINS
                .iter()
                .map(|value| (*value).to_owned())
                .collect(),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CliAction {
    Run(DashboardConfig),
    Help,
}

impl DashboardConfig {
    pub fn parse_from<I, S>(args: I) -> Result<CliAction, String>
    where
        I: IntoIterator<Item = S>,
        S: Into<String>,
    {
        let mut config = Self::default();
        let mut custom_origins = Vec::new();
        let mut args = args.into_iter().map(Into::into);
        while let Some(arg) = args.next() {
            match arg.as_str() {
                "--help" | "-h" => return Ok(CliAction::Help),
                "--bootstrap" => {
                    config.bootstrap_endpoint = next_value(&mut args, "--bootstrap")?;
                    if config.bootstrap_endpoint.trim().is_empty() {
                        return Err("--bootstrap must not be empty".to_owned());
                    }
                }
                "--bind" => {
                    let value = next_value(&mut args, "--bind")?;
                    config.bind_addr = SocketAddr::from_str(&value).map_err(|_| {
                        "--bind must be an IP socket address such as 127.0.0.1:8787".to_owned()
                    })?;
                    validate_loopback_bind(config.bind_addr)?;
                }
                "--poll-interval-ms" => {
                    let value = parse_millis(
                        &next_value(&mut args, "--poll-interval-ms")?,
                        "--poll-interval-ms",
                    )?;
                    if value < 100 {
                        return Err("--poll-interval-ms must be >= 100".to_owned());
                    }
                    config.poll_interval = Duration::from_millis(value);
                }
                "--poll-timeout-ms" => {
                    let value = parse_millis(
                        &next_value(&mut args, "--poll-timeout-ms")?,
                        "--poll-timeout-ms",
                    )?;
                    if value < 5 {
                        return Err("--poll-timeout-ms must be >= 5".to_owned());
                    }
                    if value > 300_000 {
                        return Err("--poll-timeout-ms must be <= 300000".to_owned());
                    }
                    config.poll_timeout = Duration::from_millis(value);
                }
                "--vite-origin" => {
                    let value = next_value(&mut args, "--vite-origin")?;
                    validate_vite_origin(&value)?;
                    if !custom_origins.contains(&value) {
                        custom_origins.push(value);
                    }
                }
                _ => return Err(format!("unknown argument: {arg}")),
            }
        }
        validate_loopback_bind(config.bind_addr)?;
        if !custom_origins.is_empty() {
            config.vite_origins = custom_origins;
        }
        Ok(CliAction::Run(config))
    }
}

fn next_value<I>(args: &mut I, flag: &str) -> Result<String, String>
where
    I: Iterator<Item = String>,
{
    args.next()
        .ok_or_else(|| format!("{flag} requires a value"))
}

fn parse_millis(value: &str, flag: &str) -> Result<u64, String> {
    let parsed = value
        .parse::<u64>()
        .map_err(|_| format!("{flag} must be a positive integer"))?;
    if parsed == 0 {
        return Err(format!("{flag} must be positive"));
    }
    Ok(parsed)
}

fn validate_loopback_bind(bind_addr: SocketAddr) -> Result<(), String> {
    if !bind_addr.ip().is_loopback() {
        return Err("dashboard HTTP listener must bind to a loopback address".to_owned());
    }
    Ok(())
}

fn validate_vite_origin(origin: &str) -> Result<(), String> {
    let suffix = origin
        .strip_prefix("http://localhost:")
        .or_else(|| origin.strip_prefix("http://127.0.0.1:"))
        .or_else(|| origin.strip_prefix("http://[::1]:"))
        .ok_or_else(|| "--vite-origin must use http and a loopback host".to_owned())?;
    let port = suffix.parse::<u16>().ok();
    if suffix.is_empty() || suffix.contains('/') || port.is_none_or(|value| value == 0) {
        return Err("--vite-origin must include only a valid loopback host and port".to_owned());
    }
    Ok(())
}

pub fn help_text() -> &'static str {
    "AstraLog Control Center local read-only adapter\n\
Usage:\n\
  cargo run -p astralog-dashboard -- [options]\n\n\
Options:\n\
  --bootstrap <host:port>       AstraLog bootstrap broker (default 127.0.0.1:9092)\n\
  --bind <ip:port>              Local HTTP bind (default 127.0.0.1:8787, loopback only)\n\
  --poll-interval-ms <ms>       Metadata poll cadence, min 100 (default 2000)\n\
  --poll-timeout-ms <ms>        Hard poll bound, 5..=300000 (default 1500)\n\
  --vite-origin <origin>        Allowed local Vite origin, repeatable\n\
  -h, --help                    Print this help\n\n\
The adapter only reads AstraLog metadata. It does not expose mutating broker APIs.\n"
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum ConnectionState {
    Connected,
    Disconnected,
    Unavailable,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct AdapterOverview {
    pub state: ConnectionState,
    pub bootstrap: String,
    pub observed_at_ms: u64,
    pub last_success_at_ms: Option<u64>,
    pub error: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct ClusterSummary {
    pub broker_count: usize,
    pub topic_count: usize,
    pub partition_count: usize,
    pub partitions_without_leader: usize,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct BrokerOverview {
    pub broker_id: u32,
    pub node_uuid: String,
    pub incarnation_epoch: u64,
    pub host: String,
    pub port: u16,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct PartitionOverview {
    pub partition: u32,
    pub error_code: u16,
    pub leader_broker_id: Option<u32>,
    pub leader_epoch: Option<u64>,
    pub replica_broker_ids: Vec<u32>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct TopicOverview {
    pub name: String,
    pub topic_id: String,
    pub error_code: u16,
    pub partition_count: u32,
    pub replication_factor: u16,
    pub partitions: Vec<PartitionOverview>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct ClusterOverview {
    pub cluster_id: String,
    pub metadata_epoch: u64,
    pub summary: ClusterSummary,
    pub brokers: Vec<BrokerOverview>,
    pub topics: Vec<TopicOverview>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct OverviewResponse {
    pub schema_version: u16,
    pub adapter: AdapterOverview,
    pub cluster: Option<ClusterOverview>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct HealthResponse {
    pub schema_version: u16,
    pub service: &'static str,
    pub service_ok: bool,
    pub metadata_state: ConnectionState,
    pub last_success_at_ms: Option<u64>,
}

#[derive(Debug, Clone)]
struct DashboardState {
    bootstrap: String,
    state: ConnectionState,
    observed_at_ms: u64,
    last_success_at_ms: Option<u64>,
    error: Option<String>,
    cluster: Option<ClusterOverview>,
}

impl DashboardState {
    fn new(bootstrap: String) -> Self {
        Self {
            bootstrap,
            state: ConnectionState::Disconnected,
            observed_at_ms: unix_time_ms(),
            last_success_at_ms: None,
            error: Some("metadata has not been observed yet".to_owned()),
            cluster: None,
        }
    }

    fn record_success(&mut self, metadata: MetadataResponseV0) {
        let now = unix_time_ms();
        self.state = ConnectionState::Connected;
        self.observed_at_ms = now;
        self.last_success_at_ms = Some(now);
        self.error = None;
        self.cluster = Some(cluster_overview(metadata));
    }

    fn record_failure(&mut self, message: String) {
        self.observed_at_ms = unix_time_ms();
        self.state = if self.last_success_at_ms.is_some() {
            ConnectionState::Unavailable
        } else {
            ConnectionState::Disconnected
        };
        self.error = Some(message);
    }

    fn overview(&self) -> OverviewResponse {
        OverviewResponse {
            schema_version: 1,
            adapter: AdapterOverview {
                state: self.state,
                bootstrap: self.bootstrap.clone(),
                observed_at_ms: self.observed_at_ms,
                last_success_at_ms: self.last_success_at_ms,
                error: self.error.clone(),
            },
            cluster: self.cluster.clone(),
        }
    }

    fn health(&self) -> HealthResponse {
        HealthResponse {
            schema_version: 1,
            service: "astralog-dashboard",
            service_ok: true,
            metadata_state: self.state,
            last_success_at_ms: self.last_success_at_ms,
        }
    }
}

type SharedState = Arc<RwLock<DashboardState>>;

pub async fn run(config: DashboardConfig) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    validate_loopback_bind(config.bind_addr).map_err(std::io::Error::other)?;
    for origin in &config.vite_origins {
        validate_vite_origin(origin).map_err(std::io::Error::other)?;
    }

    let listener = TcpListener::bind(config.bind_addr).await?;
    let local_addr = listener.local_addr()?;
    let state = Arc::new(RwLock::new(DashboardState::new(
        config.bootstrap_endpoint.clone(),
    )));
    let poller_state = state.clone();
    let poller_config = config.clone();
    tokio::spawn(async move {
        poll_metadata(poller_state, poller_config).await;
    });

    let app = router(state, &config)?;
    eprintln!(
        "AstraLog Control Center adapter listening on http://{local_addr}; bootstrap={}",
        config.bootstrap_endpoint
    );
    axum::serve(listener, app).await?;
    Ok(())
}

fn router(
    state: SharedState,
    config: &DashboardConfig,
) -> Result<Router, Box<dyn std::error::Error + Send + Sync>> {
    let origins = config
        .vite_origins
        .iter()
        .map(|value| HeaderValue::from_str(value))
        .collect::<Result<Vec<_>, _>>()?;
    let cors = CorsLayer::new()
        .allow_origin(origins)
        .allow_methods([Method::GET])
        .allow_headers([header::CONTENT_TYPE]);
    Ok(Router::new()
        .route("/api/overview", get(api_overview))
        .route("/api/health", get(api_health))
        .with_state(state)
        .layer(cors))
}

async fn api_overview(State(state): State<SharedState>) -> Json<OverviewResponse> {
    Json(state.read().await.overview())
}

async fn api_health(State(state): State<SharedState>) -> Json<HealthResponse> {
    Json(state.read().await.health())
}

async fn poll_metadata(state: SharedState, config: DashboardConfig) {
    let mut client: Option<AstraClient> = None;
    let mut ticker = time::interval(config.poll_interval);
    ticker.set_missed_tick_behavior(time::MissedTickBehavior::Skip);
    loop {
        ticker.tick().await;
        let result = time::timeout(config.poll_timeout, poll_once(&mut client, &config)).await;
        match result {
            Ok(Ok(metadata)) => state.write().await.record_success(metadata),
            Ok(Err(message)) => state.write().await.record_failure(message),
            Err(_) => {
                if let Some(existing) = client.take() {
                    let _ = time::timeout(Duration::from_millis(250), existing.close()).await;
                }
                state.write().await.record_failure(format!(
                    "metadata poll exceeded {} ms",
                    config.poll_timeout.as_millis()
                ));
            }
        }
    }
}

async fn poll_once(
    client: &mut Option<AstraClient>,
    config: &DashboardConfig,
) -> Result<MetadataResponseV0, String> {
    if let Some(existing) = client.as_ref() {
        match existing.refresh_metadata().await {
            Ok(metadata) => return Ok(metadata),
            Err(error) => {
                let message = read_only_client_error("metadata refresh", error);
                let failed = client.take().expect("client exists");
                failed.close().await;
                return Err(message);
            }
        }
    }

    let mut client_config = ClientConfig::new(config.bootstrap_endpoint.clone());
    client_config.client_id = "astralog-dashboard".to_owned();
    client_config.connect_timeout = config.poll_timeout;
    client_config.request_timeout = config.poll_timeout;
    client_config.producer_delivery_timeout = config.poll_timeout;
    let connected = AstraClient::connect(client_config)
        .await
        .map_err(|error| read_only_client_error("bootstrap connection", error))?;
    let metadata = connected
        .metadata()
        .await
        .ok_or_else(|| "bootstrap connection returned no metadata".to_owned())?;
    *client = Some(connected);
    Ok(metadata)
}

fn read_only_client_error(context: &str, error: ClientError) -> String {
    match error {
        ClientError::Timeout => format!("{context} timed out"),
        ClientError::ConnectionClosed => format!("{context} connection closed"),
        other => format!("{context} failed: {other}"),
    }
}

fn cluster_overview(mut metadata: MetadataResponseV0) -> ClusterOverview {
    metadata.brokers.sort_by_key(|broker| broker.broker_id);
    metadata
        .topics
        .sort_by(|left, right| left.topic_name.cmp(&right.topic_name));
    let brokers = metadata
        .brokers
        .into_iter()
        .map(|broker| BrokerOverview {
            broker_id: broker.broker_id,
            node_uuid: uuid_string(&broker.node_uuid),
            incarnation_epoch: broker.incarnation_epoch,
            host: broker.host,
            port: broker.port,
        })
        .collect::<Vec<_>>();
    let topics = metadata
        .topics
        .into_iter()
        .map(|mut topic| {
            topic
                .partitions
                .sort_by_key(|partition| partition.partition);
            TopicOverview {
                name: topic.topic_name,
                topic_id: format!("{:?}", topic.topic_id),
                error_code: topic.topic_error,
                partition_count: topic.partition_count,
                replication_factor: topic.replication_factor,
                partitions: topic
                    .partitions
                    .into_iter()
                    .map(|partition| PartitionOverview {
                        partition: partition.partition,
                        error_code: partition.partition_error,
                        leader_broker_id: (partition.leader_broker_id != UNKNOWN_BROKER_ID)
                            .then_some(partition.leader_broker_id),
                        leader_epoch: (partition.leader_epoch != UNKNOWN_EPOCH)
                            .then_some(partition.leader_epoch),
                        replica_broker_ids: partition.replica_broker_ids,
                    })
                    .collect(),
            }
        })
        .collect::<Vec<_>>();
    let partition_count = topics.iter().map(|topic| topic.partitions.len()).sum();
    let partitions_without_leader = topics
        .iter()
        .flat_map(|topic| &topic.partitions)
        .filter(|partition| partition.leader_broker_id.is_none())
        .count();
    ClusterOverview {
        cluster_id: uuid_string(&metadata.cluster_id),
        metadata_epoch: metadata.metadata_epoch,
        summary: ClusterSummary {
            broker_count: brokers.len(),
            topic_count: topics.len(),
            partition_count,
            partitions_without_leader,
        },
        brokers,
        topics,
    }
}

fn uuid_string(bytes: &[u8; 16]) -> String {
    format!(
        "{:02x}{:02x}{:02x}{:02x}-{:02x}{:02x}-{:02x}{:02x}-{:02x}{:02x}-{:02x}{:02x}{:02x}{:02x}{:02x}{:02x}",
        bytes[0],
        bytes[1],
        bytes[2],
        bytes[3],
        bytes[4],
        bytes[5],
        bytes[6],
        bytes[7],
        bytes[8],
        bytes[9],
        bytes[10],
        bytes[11],
        bytes[12],
        bytes[13],
        bytes[14],
        bytes[15]
    )
}

fn unix_time_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis()
        .try_into()
        .unwrap_or(u64::MAX)
}

#[cfg(test)]
mod tests {
    use astralog_common::TopicId;
    use astralog_protocol::api::{BrokerMetadataV0, MetadataPartitionV0, MetadataTopicV0};

    use super::*;

    fn metadata() -> MetadataResponseV0 {
        MetadataResponseV0 {
            cluster_id: [0x11; 16],
            metadata_epoch: 42,
            brokers: vec![
                BrokerMetadataV0 {
                    broker_id: 2,
                    node_uuid: [0x22; 16],
                    incarnation_epoch: 7,
                    host: "127.0.0.1".to_owned(),
                    port: 9093,
                },
                BrokerMetadataV0 {
                    broker_id: 1,
                    node_uuid: [0x21; 16],
                    incarnation_epoch: 4,
                    host: "127.0.0.1".to_owned(),
                    port: 9092,
                },
            ],
            topics: vec![MetadataTopicV0 {
                topic_error: 0,
                topic_name: "orders".to_owned(),
                topic_id: TopicId::new([0x33; 16]),
                partition_count: 2,
                replication_factor: 2,
                partitions: vec![
                    MetadataPartitionV0 {
                        partition: 1,
                        partition_error: 0,
                        leader_broker_id: UNKNOWN_BROKER_ID,
                        leader_epoch: UNKNOWN_EPOCH,
                        replica_broker_ids: vec![1, 2],
                    },
                    MetadataPartitionV0 {
                        partition: 0,
                        partition_error: 0,
                        leader_broker_id: 2,
                        leader_epoch: 8,
                        replica_broker_ids: vec![1, 2],
                    },
                ],
            }],
            tags: Vec::new(),
        }
    }

    #[test]
    fn cli_defaults_are_local_and_bounded() {
        let CliAction::Run(config) = DashboardConfig::parse_from(Vec::<String>::new()).unwrap()
        else {
            panic!("expected run config");
        };
        assert!(config.bind_addr.ip().is_loopback());
        assert_eq!(config.bootstrap_endpoint, DEFAULT_BOOTSTRAP);
        assert_eq!(config.poll_interval, Duration::from_millis(2_000));
        assert_eq!(config.poll_timeout, Duration::from_millis(1_500));
        assert_eq!(
            config.vite_origins,
            DEFAULT_VITE_ORIGINS
                .iter()
                .map(|value| (*value).to_owned())
                .collect::<Vec<_>>()
        );
    }

    #[test]
    fn cli_rejects_remote_listener_and_remote_cors_origin() {
        assert!(DashboardConfig::parse_from(["--bind", "0.0.0.0:8787"]).is_err());
        assert!(
            DashboardConfig::parse_from(["--vite-origin", "https://example.com:5173"]).is_err()
        );
        assert!(DashboardConfig::parse_from(["--vite-origin", "http://localhost:0"]).is_err());
        assert!(DashboardConfig::parse_from(["--poll-interval-ms", "99"]).is_err());
        assert!(DashboardConfig::parse_from(["--poll-timeout-ms", "4"]).is_err());
    }

    #[test]
    fn overview_maps_real_metadata_and_unknown_leader_explicitly() {
        let cluster = cluster_overview(metadata());
        assert_eq!(cluster.metadata_epoch, 42);
        assert_eq!(cluster.summary.broker_count, 2);
        assert_eq!(cluster.summary.topic_count, 1);
        assert_eq!(cluster.summary.partition_count, 2);
        assert_eq!(cluster.summary.partitions_without_leader, 1);
        assert_eq!(cluster.brokers[0].broker_id, 1);
        assert_eq!(cluster.topics[0].partitions[0].leader_broker_id, Some(2));
        assert_eq!(cluster.topics[0].partitions[1].leader_broker_id, None);
        assert_eq!(cluster.topics[0].partitions[1].leader_epoch, None);
    }

    #[test]
    fn state_distinguishes_disconnected_from_stale_unavailable() {
        let mut state = DashboardState::new("127.0.0.1:9092".to_owned());
        state.record_failure("connection refused".to_owned());
        assert_eq!(state.state, ConnectionState::Disconnected);
        assert!(state.cluster.is_none());

        state.record_success(metadata());
        let last_success = state.last_success_at_ms;
        assert_eq!(state.state, ConnectionState::Connected);
        assert!(state.cluster.is_some());

        state.record_failure("metadata timeout".to_owned());
        assert_eq!(state.state, ConnectionState::Unavailable);
        assert_eq!(state.last_success_at_ms, last_success);
        assert!(state.cluster.is_some());
    }

    #[test]
    fn overview_json_has_typed_schema_and_no_latency_claims() {
        let mut state = DashboardState::new("127.0.0.1:9092".to_owned());
        state.record_success(metadata());
        let json = serde_json::to_value(state.overview()).unwrap();
        assert_eq!(json["schema_version"], 1);
        assert_eq!(json["adapter"]["state"], "connected");
        assert_eq!(json["cluster"]["summary"]["partition_count"], 2);
        assert!(json.get("latency").is_none());
        assert!(json.get("throughput").is_none());
    }

    #[test]
    fn read_only_timeout_text_does_not_claim_ambiguous_mutation() {
        let message = read_only_client_error("metadata refresh", ClientError::Timeout);
        assert_eq!(message, "metadata refresh timed out");
        assert!(!message.contains("mutation"));
    }
}
