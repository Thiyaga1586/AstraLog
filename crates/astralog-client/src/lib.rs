#![forbid(unsafe_code)]

mod batching;

use std::{
    collections::{BTreeMap, BTreeSet, HashMap},
    fmt, io,
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
    },
    time::Duration,
};

use astralog_common::{ProducerAuthority, ProducerSequence, TopicId};
use astralog_groups::{
    GROUPS_INTERNAL_TOPIC_NAME, P6_INTERNAL_TOPIC_PARTITIONS, P6_INTERNAL_TOPIC_REPLICATION_FACTOR,
    group_shard,
};
use astralog_protocol::{
    ApiKey, ClientHello, FrameDecoder, FrameKind, HelloStatus, MAX_WIRE_FRAME_LEN_V1,
    NO_RESPONSE_FLAG, PartitionRng, ProtocolError, PublicError, RetryClass, ServerHello,
    StickyPartitioner, TaggedField, UNKNOWN_BROKER_ID, UNKNOWN_EPOCH, WIRE_VERSION, WireFrame,
    api::{
        AckMode, ApiVersionsRequestV0, ApiVersionsResponseV0, BodyCodec, CommitOffsetV0,
        CommitOffsetsRequestV0, CommitOffsetsResponseV0, CoordinatorHintV0, CreateTopicRequestV0,
        CreateTopicRequestV1, CreateTopicResponseV0, CreateTopicResponseV1, DeleteTopicRequestV0,
        DeleteTopicResponseV0, DescribeTopicRequestV0, DescribeTopicRequestV1,
        DescribeTopicResponseV0, DescribeTopicResponseV1, FetchOffsetResultV0,
        FetchOffsetsRequestV0, FetchOffsetsResponseV0, FetchRequestV0, FetchResponseV0, GroupEpoch,
        GroupId, HeartbeatRequestV0, HeartbeatResponseV0, InitProducerRequestV0,
        InitProducerResponseV0, JoinSubscribeRequestV0, JoinSubscribeResponseV0, LeaveRequestV0,
        LeaveResponseV0, ListOffsetsRequestV0, ListOffsetsResponseV0, MAX_RECORDS_PER_PARTITION,
        MAX_TOPIC_BATCH_UNCOMPRESSED_BYTES_V1, MemberAuthorityV0, MemberEpoch, MemberId,
        MetadataPartitionV0, MetadataRequestV0, MetadataResponseV0, MetadataTopicV0,
        ProducePartitionRequestV0, ProducePartitionRequestV1, ProduceRequestV0, ProduceRequestV1,
        ProduceResponseV0, ProduceResponseV1, RevokeAckRequestV0, TopicPartitionV0,
        TopicPerformanceConfigV1, WireHeaderV0, WireRecordV0,
    },
    encode_frame, partition_for_key,
};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{
        TcpStream,
        tcp::{OwnedReadHalf, OwnedWriteHalf},
    },
    sync::{Mutex, Notify, RwLock, Semaphore, oneshot},
    time,
};

const MIN_WIRE_FRAME_LEN: usize = 44;
const DEFAULT_MAX_FRAME_LEN: usize = 16 * 1024 * 1024;
const DEFAULT_MAX_IN_FLIGHT: usize = 64;
const DEFAULT_MAX_GROUP_CACHE: usize = 1_024;
const DEFAULT_MAX_GROUP_IN_FLIGHT: usize = 64;
const DEFAULT_MAX_GROUP_RETRIES: usize = 2;
const DEFAULT_MAX_PRODUCER_PARTITIONS: usize = 4_096;
const DEFAULT_MAX_PRODUCER_RETRIES: usize = 2;
const DEFAULT_BATCH_MAX_RECORDS: usize = 128;
const DEFAULT_BATCH_MAX_BYTES: usize = 1024 * 1024;
const DEFAULT_BATCH_LINGER_MS: u64 = 5;
const DEFAULT_ACCUMULATOR_MAX_BYTES: usize = 64 * 1024 * 1024;
const DEFAULT_ACCUMULATOR_MAX_RECORDS: usize = 65_536;
const DEFAULT_ACCUMULATOR_MAX_PARTITIONS: usize = 4_096;
const DEFAULT_DELIVERY_TIMEOUT_MS: u64 = 30_000;

use batching::{
    AccumulatorLimits, AdmissionAttempt, AdmissionSpec, BatchMode, BufferedOffsets, PartitionKey,
    ProducerAccumulator, TopicBatchLimits,
};

pub type Result<T> = std::result::Result<T, ClientError>;
type PendingSender = oneshot::Sender<Result<WireFrame>>;
type PendingMap = Arc<Mutex<HashMap<u64, PendingSender>>>;

#[derive(Debug)]
pub enum ClientError {
    Io(io::Error),
    Protocol(ProtocolError),
    Broker(PublicError),
    Timeout,
    ConnectionClosed,
    InvalidResponse(&'static str),
    InvalidResponseOwned(String),
    UnsupportedApi(ApiKey),
    Metadata(&'static str),
    Entropy(getrandom::Error),
    AmbiguousMutation { api: ApiKey },
    ProducerSessionRequired,
    ProducerSessionAlreadyInitialized,
    ProducerRequestInFlight { topic_id: TopicId, partition: u32 },
    ProducerStateFull,
    AccumulatorFull,
    AccumulatorClosed,
    AccumulatorRecordTooLarge,
    BatchDelivery(String),
}

impl fmt::Display for ClientError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Io(err) => write!(f, "I/O error: {err}"),
            Self::Protocol(err) => write!(f, "protocol error: {err}"),
            Self::Broker(err) => write!(f, "broker returned {err:?}"),
            Self::Timeout => f.write_str("request timed out; mutation outcome may be ambiguous"),
            Self::ConnectionClosed => f.write_str("broker connection closed"),
            Self::InvalidResponse(message) => write!(f, "invalid broker response: {message}"),
            Self::InvalidResponseOwned(message) => write!(f, "invalid broker response: {message}"),
            Self::UnsupportedApi(api) => write!(f, "broker does not advertise API {api:?} V0"),
            Self::Metadata(message) => write!(f, "metadata error: {message}"),
            Self::Entropy(err) => write!(f, "OS entropy error: {err}"),
            Self::AmbiguousMutation { api } => write!(
                f,
                "{api:?} mutation outcome is ambiguous; client did not retry blindly"
            ),
            Self::ProducerSessionRequired => {
                f.write_str("idempotent producer session is not initialized")
            }
            Self::ProducerSessionAlreadyInitialized => {
                f.write_str("producer session already exists; use restart_producer")
            }
            Self::ProducerRequestInFlight {
                topic_id,
                partition,
            } => write!(
                f,
                "producer request is already unresolved for {topic_id:?}/{partition}"
            ),
            Self::ProducerStateFull => f.write_str("bounded producer partition state is full"),
            Self::AccumulatorFull => f.write_str("bounded producer accumulator is full"),
            Self::AccumulatorClosed => f.write_str("producer accumulator is closed"),
            Self::AccumulatorRecordTooLarge => {
                f.write_str("record exceeds producer accumulator/topic batch bounds")
            }
            Self::BatchDelivery(message) => write!(f, "batched produce delivery failed: {message}"),
        }
    }
}

impl std::error::Error for ClientError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Io(err) => Some(err),
            Self::Protocol(err) => Some(err),
            _ => None,
        }
    }
}

impl From<io::Error> for ClientError {
    fn from(value: io::Error) -> Self {
        Self::Io(value)
    }
}

impl From<ProtocolError> for ClientError {
    fn from(value: ProtocolError) -> Self {
        Self::Protocol(value)
    }
}

impl From<getrandom::Error> for ClientError {
    fn from(value: getrandom::Error) -> Self {
        Self::Entropy(value)
    }
}

#[derive(Debug, Clone)]
pub struct ClientConfig {
    pub bootstrap_endpoint: String,
    pub client_id: String,
    pub request_timeout: Duration,
    pub connect_timeout: Duration,
    pub max_frame_len: usize,
    pub max_in_flight: usize,
    pub max_group_cache: usize,
    pub max_group_in_flight: usize,
    pub max_group_retries: usize,
    pub max_producer_partitions: usize,
    pub max_producer_retries: usize,
    pub batch_max_records: usize,
    pub batch_max_bytes: usize,
    pub batch_linger: Duration,
    pub accumulator_max_bytes: usize,
    pub accumulator_max_records: usize,
    pub accumulator_max_partitions: usize,
    pub producer_delivery_timeout: Duration,
}

impl ClientConfig {
    pub fn new(bootstrap_endpoint: impl Into<String>) -> Self {
        Self {
            bootstrap_endpoint: bootstrap_endpoint.into(),
            client_id: "astralog-rust-client".into(),
            request_timeout: Duration::from_secs(5),
            connect_timeout: Duration::from_secs(5),
            max_frame_len: DEFAULT_MAX_FRAME_LEN,
            max_in_flight: DEFAULT_MAX_IN_FLIGHT,
            max_group_cache: DEFAULT_MAX_GROUP_CACHE,
            max_group_in_flight: DEFAULT_MAX_GROUP_IN_FLIGHT,
            max_group_retries: DEFAULT_MAX_GROUP_RETRIES,
            max_producer_partitions: DEFAULT_MAX_PRODUCER_PARTITIONS,
            max_producer_retries: DEFAULT_MAX_PRODUCER_RETRIES,
            batch_max_records: DEFAULT_BATCH_MAX_RECORDS,
            batch_max_bytes: DEFAULT_BATCH_MAX_BYTES,
            batch_linger: Duration::from_millis(DEFAULT_BATCH_LINGER_MS),
            accumulator_max_bytes: DEFAULT_ACCUMULATOR_MAX_BYTES,
            accumulator_max_records: DEFAULT_ACCUMULATOR_MAX_RECORDS,
            accumulator_max_partitions: DEFAULT_ACCUMULATOR_MAX_PARTITIONS,
            producer_delivery_timeout: Duration::from_millis(DEFAULT_DELIVERY_TIMEOUT_MS),
        }
    }

    fn validate(&self) -> Result<()> {
        if self.bootstrap_endpoint.is_empty() {
            return Err(ClientError::Metadata("bootstrap endpoint is empty"));
        }
        if self.client_id.len() > 128 {
            return Err(ClientError::Metadata("client id exceeds Wire V1 maximum"));
        }
        if self.request_timeout < Duration::from_millis(1)
            || self.request_timeout > Duration::from_millis(300_000)
        {
            return Err(ClientError::Metadata(
                "request timeout must be in 1..=300000ms",
            ));
        }
        if self.connect_timeout.is_zero() {
            return Err(ClientError::Metadata("connect timeout must be positive"));
        }
        if !(MIN_WIRE_FRAME_LEN..=MAX_WIRE_FRAME_LEN_V1).contains(&self.max_frame_len) {
            return Err(ClientError::Metadata("invalid configured frame limit"));
        }
        if self.max_in_flight == 0 {
            return Err(ClientError::Metadata("max_in_flight must be positive"));
        }
        if self.max_group_cache == 0 || self.max_group_cache > 65_536 {
            return Err(ClientError::Metadata(
                "max_group_cache must be in 1..=65536",
            ));
        }
        if self.max_group_in_flight == 0 || self.max_group_in_flight > self.max_in_flight {
            return Err(ClientError::Metadata(
                "max_group_in_flight must be positive and no larger than max_in_flight",
            ));
        }
        if self.max_group_retries > 8 {
            return Err(ClientError::Metadata("max_group_retries must be <= 8"));
        }
        if self.max_producer_partitions == 0 || self.max_producer_partitions > 65_536 {
            return Err(ClientError::Metadata(
                "max_producer_partitions must be in 1..=65536",
            ));
        }
        if self.max_producer_retries > 8 {
            return Err(ClientError::Metadata("max_producer_retries must be <= 8"));
        }
        if self.batch_max_records == 0 || self.batch_max_records > MAX_RECORDS_PER_PARTITION {
            return Err(ClientError::Metadata(
                "batch_max_records must be in 1..=MAX_RECORDS_PER_PARTITION",
            ));
        }
        if self.batch_max_bytes == 0
            || self.batch_max_bytes > MAX_TOPIC_BATCH_UNCOMPRESSED_BYTES_V1 as usize
        {
            return Err(ClientError::Metadata(
                "batch_max_bytes exceeds P8 logical batch ceiling",
            ));
        }
        if self.batch_linger.is_zero() || self.batch_linger > self.producer_delivery_timeout {
            return Err(ClientError::Metadata(
                "batch_linger must be positive and <= producer_delivery_timeout",
            ));
        }
        if self.accumulator_max_bytes == 0
            || self.accumulator_max_records == 0
            || self.accumulator_max_partitions == 0
            || self.accumulator_max_partitions > 65_536
        {
            return Err(ClientError::Metadata(
                "producer accumulator bounds must be finite and positive",
            ));
        }
        if self.producer_delivery_timeout < self.request_timeout
            || self.producer_delivery_timeout > Duration::from_millis(300_000)
        {
            return Err(ClientError::Metadata(
                "producer_delivery_timeout must cover request_timeout and be <= 300000ms",
            ));
        }
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ProduceOutcome {
    pub topic_id: TopicId,
    pub partition: u32,
    pub base_offset: Option<u64>,
    pub last_offset: Option<u64>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct IdempotentProduceOutcome {
    pub topic_id: TopicId,
    pub partition: u32,
    pub base_offset: u64,
    pub last_offset: u64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct PendingProducerRequest {
    base_sequence: ProducerSequence,
    record_count: u32,
    body: Vec<u8>,
    active: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct ProducerPartitionState {
    next_sequence: ProducerSequence,
    pending: Option<PendingProducerRequest>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct ProducerSessionState {
    authority: ProducerAuthority,
    partitions: BTreeMap<(TopicId, u32), ProducerPartitionState>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct GroupCoordinatorRoute {
    pub shard: u32,
    pub groups_topic_id: TopicId,
    pub coordinator_broker_id: u32,
    pub coordinator_epoch: u64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GroupMemberState {
    pub authority: MemberAuthorityV0,
    pub group_phase: u8,
    pub heartbeat_interval_ms: u32,
    pub active_assignments: Vec<TopicPartitionV0>,
    pub revocations: Vec<TopicPartitionV0>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CooperativeRevokePlan {
    pub authority: MemberAuthorityV0,
    pub keep_processing: Vec<TopicPartitionV0>,
    pub revoke: Vec<TopicPartitionV0>,
}

impl GroupCoordinatorRoute {
    pub const fn hint(self) -> CoordinatorHintV0 {
        CoordinatorHintV0 {
            coordinator_broker_id: self.coordinator_broker_id,
            coordinator_epoch: self.coordinator_epoch,
            current_group_epoch: 0,
            current_member_epoch: 0,
        }
    }
}

#[derive(Clone)]
pub struct AstraClient {
    inner: Arc<ClientInner>,
}

struct ClientInner {
    config: ClientConfig,
    bootstrap: Arc<BrokerConnection>,
    metadata: RwLock<Option<MetadataResponseV0>>,
    connections: RwLock<HashMap<u32, Arc<BrokerConnection>>>,
    sticky: Mutex<HashMap<TopicId, StickyPartitioner<ClientRng>>>,
    group_routes: RwLock<BTreeMap<GroupId, GroupCoordinatorRoute>>,
    group_members: RwLock<BTreeMap<GroupId, GroupMemberState>>,
    group_permits: Arc<Semaphore>,
    producer_control: Mutex<()>,
    producer: Mutex<Option<ProducerSessionState>>,
    topic_performance: RwLock<HashMap<TopicId, TopicPerformanceConfigV1>>,
    accumulator: Mutex<ProducerAccumulator>,
    accumulator_notify: Notify,
    batch_clock_start: time::Instant,
}

impl AstraClient {
    pub async fn connect(config: ClientConfig) -> Result<Self> {
        config.validate()?;
        let max_group_in_flight = config.max_group_in_flight;
        let bootstrap =
            Arc::new(BrokerConnection::connect(&config.bootstrap_endpoint, &config).await?);
        let client = Self {
            inner: Arc::new(ClientInner {
                config,
                bootstrap,
                metadata: RwLock::new(None),
                connections: RwLock::new(HashMap::new()),
                sticky: Mutex::new(HashMap::new()),
                group_routes: RwLock::new(BTreeMap::new()),
                group_members: RwLock::new(BTreeMap::new()),
                group_permits: Arc::new(Semaphore::new(max_group_in_flight)),
                producer_control: Mutex::new(()),
                producer: Mutex::new(None),
                topic_performance: RwLock::new(HashMap::new()),
                accumulator: Mutex::new(ProducerAccumulator::default()),
                accumulator_notify: Notify::new(),
                batch_clock_start: time::Instant::now(),
            }),
        };
        client.refresh_metadata().await?;
        Ok(client)
    }

    pub async fn refresh_metadata(&self) -> Result<MetadataResponseV0> {
        let request = MetadataRequestV0 {
            all_topics: true,
            topic_names: Vec::new(),
            tags: Vec::new(),
        };
        let response: MetadataResponseV0 = self
            .inner
            .bootstrap
            .request_body(
                ApiKey::Metadata,
                0,
                request.encode_body()?,
                self.inner.config.request_timeout,
            )
            .await?;
        let previous = self.inner.metadata.read().await.clone();
        *self.inner.metadata.write().await = Some(response.clone());
        if let Some(previous) = previous {
            let invalidated = invalidated_partition_routes(&previous, &response);
            if !invalidated.is_empty() {
                {
                    let mut sticky = self.inner.sticky.lock().await;
                    for (topic_id, partition) in &invalidated {
                        if let Some(partitioner) = sticky.get_mut(topic_id) {
                            partitioner.invalidate(*partition);
                        }
                    }
                }
                let closed_any = {
                    let mut accumulator = self.inner.accumulator.lock().await;
                    let mut closed_any = false;
                    for key in invalidated {
                        closed_any |= accumulator.request_close(key);
                    }
                    closed_any
                };
                if closed_any {
                    self.inner.accumulator_notify.notify_waiters();
                }
            }
        }
        Ok(response)
    }

    pub async fn metadata(&self) -> Option<MetadataResponseV0> {
        self.inner.metadata.read().await.clone()
    }

    pub async fn resolve_group_coordinator(
        &self,
        group_id: &GroupId,
    ) -> Result<GroupCoordinatorRoute> {
        let metadata_request = MetadataRequestV0 {
            all_topics: false,
            topic_names: vec![GROUPS_INTERNAL_TOPIC_NAME.to_owned()],
            tags: Vec::new(),
        };
        let cluster_metadata: MetadataResponseV0 = self
            .inner
            .bootstrap
            .request_body(
                ApiKey::Metadata,
                0,
                metadata_request.encode_body()?,
                self.inner.config.request_timeout,
            )
            .await?;
        let groups_topic = cluster_metadata
            .topics
            .iter()
            .find(|topic| topic.topic_name == GROUPS_INTERNAL_TOPIC_NAME)
            .cloned()
            .ok_or(ClientError::Metadata(
                "P6 groups internal topic is absent from metadata",
            ))?;
        validate_groups_topic_metadata(&groups_topic)?;
        let shard = group_shard(group_id)
            .map_err(|_| ClientError::Metadata("group shard mapping failed"))?;
        let authoritative_partition = groups_topic
            .partitions
            .iter()
            .find(|partition| partition.partition == shard)
            .ok_or(ClientError::Metadata(
                "group coordinator shard is absent from metadata",
            ))?;
        validate_replica_set(authoritative_partition)?;

        let body = metadata_request.encode_body()?;
        let mut observations = Vec::new();
        for broker_id in &authoritative_partition.replica_broker_ids {
            let Ok(connection) = self
                .connection_for_broker_from_metadata(*broker_id, &cluster_metadata)
                .await
            else {
                continue;
            };
            let response: Result<MetadataResponseV0> = connection
                .request_body(
                    ApiKey::Metadata,
                    0,
                    body.clone(),
                    self.inner.config.request_timeout,
                )
                .await;
            let Ok(response) = response else {
                continue;
            };
            if response.cluster_id != cluster_metadata.cluster_id {
                continue;
            }
            let Some(topic) = response
                .topics
                .iter()
                .find(|topic| topic.topic_name == GROUPS_INTERNAL_TOPIC_NAME)
            else {
                continue;
            };
            if topic.topic_id != groups_topic.topic_id
                || validate_groups_topic_metadata(topic).is_err()
            {
                continue;
            }
            let Some(partition) = topic
                .partitions
                .iter()
                .find(|partition| partition.partition == shard)
            else {
                continue;
            };
            if partition.replica_broker_ids != authoritative_partition.replica_broker_ids {
                continue;
            }
            observations.push(partition.clone());
        }

        let route = select_group_coordinator(
            shard,
            groups_topic.topic_id,
            &authoritative_partition.replica_broker_ids,
            &observations,
        )?;
        self.cache_group_route(group_id.clone(), route).await;
        Ok(route)
    }

    pub async fn group_member_state(&self, group_id: &GroupId) -> Option<GroupMemberState> {
        self.inner.group_members.read().await.get(group_id).cloned()
    }

    pub async fn cooperative_revoke_plan(
        &self,
        group_id: &GroupId,
    ) -> Result<CooperativeRevokePlan> {
        let state = self
            .group_member_state(group_id)
            .await
            .ok_or(ClientError::Metadata("group member state is unavailable"))?;
        let revoking = state.revocations.iter().copied().collect::<BTreeSet<_>>();
        let keep_processing = state
            .active_assignments
            .iter()
            .copied()
            .filter(|partition| !revoking.contains(partition))
            .collect();
        Ok(CooperativeRevokePlan {
            authority: state.authority,
            keep_processing,
            revoke: state.revocations,
        })
    }

    pub async fn join_subscribe(
        &self,
        request: JoinSubscribeRequestV0,
    ) -> Result<JoinSubscribeResponseV0> {
        let group_id = request.group_id.clone();
        let frame = self
            .group_request_frame(
                &group_id,
                ApiKey::JoinSubscribe,
                request.encode_body()?,
                true,
            )
            .await?;
        let response = JoinSubscribeResponseV0::decode_body(&frame.body)?;
        let member_id = MemberId::new(response.member_id)?;
        let authority = member_authority_from_hint(&group_id, member_id, response.hint)?;
        self.update_member_state(
            group_id,
            GroupMemberState {
                authority,
                group_phase: response.group_phase,
                heartbeat_interval_ms: response.heartbeat_interval_ms,
                active_assignments: response.active_assignments.clone(),
                revocations: response.revocations.clone(),
            },
        )
        .await?;
        Ok(response)
    }

    pub async fn heartbeat_group(&self, group_id: &GroupId) -> Result<HeartbeatResponseV0> {
        for attempt in 0..=1 {
            let state = self
                .group_member_state(group_id)
                .await
                .ok_or(ClientError::Metadata("group member state is unavailable"))?;
            let request = HeartbeatRequestV0 {
                authority: state.authority.clone(),
                tags: Vec::new(),
            };
            match self
                .group_request_frame(group_id, ApiKey::Heartbeat, request.encode_body()?, true)
                .await
            {
                Ok(frame) => {
                    let response = HeartbeatResponseV0::decode_body(&frame.body)?;
                    let authority = member_authority_from_hint(
                        group_id,
                        state.authority.member_id,
                        response.hint,
                    )?;
                    self.update_member_state(
                        group_id.clone(),
                        GroupMemberState {
                            authority,
                            group_phase: response.group_phase,
                            heartbeat_interval_ms: response.heartbeat_interval_ms,
                            active_assignments: response.active_assignments.clone(),
                            revocations: response.revocations.clone(),
                        },
                    )
                    .await?;
                    return Ok(response);
                }
                Err(ClientError::Broker(PublicError::GroupEpochStale)) if attempt == 0 => {
                    continue;
                }
                Err(error) => return Err(error),
            }
        }
        Err(ClientError::InvalidResponse(
            "heartbeat epoch refresh exhausted unexpectedly",
        ))
    }

    pub async fn revoke_ack(
        &self,
        group_id: &GroupId,
        revoked: Vec<TopicPartitionV0>,
    ) -> Result<HeartbeatResponseV0> {
        let state = self
            .group_member_state(group_id)
            .await
            .ok_or(ClientError::Metadata("group member state is unavailable"))?;
        let request = RevokeAckRequestV0 {
            authority: state.authority.clone(),
            revoked,
            tags: Vec::new(),
        };
        let frame = self
            .group_request_frame(group_id, ApiKey::RevokeAck, request.encode_body()?, true)
            .await?;
        let response = HeartbeatResponseV0::decode_body(&frame.body)?;
        let authority =
            member_authority_from_hint(group_id, state.authority.member_id, response.hint)?;
        self.update_member_state(
            group_id.clone(),
            GroupMemberState {
                authority,
                group_phase: response.group_phase,
                heartbeat_interval_ms: response.heartbeat_interval_ms,
                active_assignments: response.active_assignments.clone(),
                revocations: response.revocations.clone(),
            },
        )
        .await?;
        Ok(response)
    }

    pub async fn complete_cooperative_revoke(
        &self,
        group_id: &GroupId,
        revoked: Vec<TopicPartitionV0>,
        commit_before_ack: Option<Vec<CommitOffsetV0>>,
    ) -> Result<HeartbeatResponseV0> {
        let plan = self.cooperative_revoke_plan(group_id).await?;
        if revoked != plan.revoke {
            return Err(ClientError::Metadata(
                "revoke acknowledgement must match the current canonical revocation set",
            ));
        }
        if let Some(offsets) = commit_before_ack
            && !offsets.is_empty()
        {
            let revoked_set = revoked.iter().copied().collect::<BTreeSet<_>>();
            if offsets.iter().any(|offset| {
                !revoked_set.contains(&TopicPartitionV0 {
                    topic_id: offset.topic_id,
                    partition: offset.partition,
                })
            }) {
                return Err(ClientError::Metadata(
                    "pre-revoke offset commit contains a non-revoked partition",
                ));
            }
            self.commit_group_offsets(group_id, offsets).await?;
        }
        self.revoke_ack(group_id, revoked).await
    }

    pub async fn leave_group(&self, group_id: &GroupId) -> Result<LeaveResponseV0> {
        let state = self
            .group_member_state(group_id)
            .await
            .ok_or(ClientError::Metadata("group member state is unavailable"))?;
        let request = LeaveRequestV0 {
            authority: state.authority,
            tags: Vec::new(),
        };
        let frame = self
            .group_request_frame(group_id, ApiKey::Leave, request.encode_body()?, true)
            .await?;
        let response = LeaveResponseV0::decode_body(&frame.body)?;
        self.inner.group_members.write().await.remove(group_id);
        Ok(response)
    }

    pub async fn commit_group_offsets(
        &self,
        group_id: &GroupId,
        offsets: Vec<CommitOffsetV0>,
    ) -> Result<CommitOffsetsResponseV0> {
        let state = self
            .group_member_state(group_id)
            .await
            .ok_or(ClientError::Metadata("group member state is unavailable"))?;
        let request = CommitOffsetsRequestV0 {
            authority: state.authority,
            offsets,
            tags: Vec::new(),
        };
        let frame = self
            .group_request_frame(
                group_id,
                ApiKey::CommitOffsets,
                request.encode_body()?,
                true,
            )
            .await?;
        CommitOffsetsResponseV0::decode_body(&frame.body).map_err(Into::into)
    }

    pub async fn fetch_group_offsets(
        &self,
        group_id: &GroupId,
        partitions: Vec<TopicPartitionV0>,
    ) -> Result<Vec<FetchOffsetResultV0>> {
        let request = FetchOffsetsRequestV0 {
            group_id: group_id.clone(),
            partitions,
            tags: Vec::new(),
        };
        let frame = self
            .group_request_frame(
                group_id,
                ApiKey::FetchOffsets,
                request.encode_body()?,
                false,
            )
            .await?;
        Ok(FetchOffsetsResponseV0::decode_body(&frame.body)?.results)
    }

    pub async fn create_topic(
        &self,
        topic_name: impl Into<String>,
        partition_count: u32,
    ) -> Result<CreateTopicResponseV0> {
        let request = CreateTopicRequestV0 {
            topic_name: topic_name.into(),
            partition_count,
            replication_factor: 1,
            tags: Vec::new(),
        };
        let response = self
            .inner
            .bootstrap
            .request_body(
                ApiKey::CreateTopic,
                0,
                request.encode_body()?,
                self.inner.config.request_timeout,
            )
            .await?;
        self.refresh_metadata().await?;
        Ok(response)
    }

    pub async fn create_topic_with_performance(
        &self,
        topic_name: impl Into<String>,
        partition_count: u32,
        replication_factor: u16,
        performance: TopicPerformanceConfigV1,
    ) -> Result<CreateTopicResponseV1> {
        let request = CreateTopicRequestV1 {
            topic_name: topic_name.into(),
            partition_count,
            replication_factor,
            performance,
            tags: Vec::new(),
        };
        let response = self
            .inner
            .bootstrap
            .request_body(
                ApiKey::CreateTopic,
                1,
                request.encode_body()?,
                self.inner.config.request_timeout,
            )
            .await?;
        self.refresh_metadata().await?;
        Ok(response)
    }

    pub async fn delete_topic(
        &self,
        topic_name: impl Into<String>,
    ) -> Result<DeleteTopicResponseV0> {
        let request = DeleteTopicRequestV0 {
            topic_name: topic_name.into(),
            tags: Vec::new(),
        };
        let response = self
            .inner
            .bootstrap
            .request_body(
                ApiKey::DeleteTopic,
                0,
                request.encode_body()?,
                self.inner.config.request_timeout,
            )
            .await?;
        self.refresh_metadata().await?;
        Ok(response)
    }

    pub async fn describe_topic(
        &self,
        topic_name: impl Into<String>,
    ) -> Result<DescribeTopicResponseV0> {
        let request = DescribeTopicRequestV0 {
            topic_name: topic_name.into(),
            tags: Vec::new(),
        };
        self.inner
            .bootstrap
            .request_body(
                ApiKey::DescribeTopic,
                0,
                request.encode_body()?,
                self.inner.config.request_timeout,
            )
            .await
    }

    pub async fn describe_topic_with_performance(
        &self,
        topic_name: impl Into<String>,
    ) -> Result<DescribeTopicResponseV1> {
        let request = DescribeTopicRequestV1 {
            topic_name: topic_name.into(),
            tags: Vec::new(),
        };
        self.inner
            .bootstrap
            .request_body(
                ApiKey::DescribeTopic,
                1,
                request.encode_body()?,
                self.inner.config.request_timeout,
            )
            .await
    }

    /// Produces one record using the Wire V1 keyed/sticky partitioning rules.
    ///
    /// P2 is intentionally non-idempotent. A timeout, broken connection, or
    /// lost response after the broker may have appended is returned to the
    /// caller and is **not** automatically retried, because retrying could
    /// create a duplicate. P7 adds producer IDs/epochs/sequences.
    pub async fn produce(
        &self,
        topic_name: &str,
        key: Option<Vec<u8>>,
        value: Option<Vec<u8>>,
        headers: Vec<WireHeaderV0>,
        acks: AckMode,
    ) -> Result<ProduceOutcome> {
        let topic = self.topic_metadata(topic_name).await?;
        let partition = match key.as_deref() {
            Some(key) => partition_for_key(key, topic.partition_count)?,
            None => {
                self.choose_sticky_partition(topic.topic_id, &topic.partitions)
                    .await?
            }
        };

        let result = self
            .produce_once(
                topic.topic_id,
                partition,
                topic
                    .partitions
                    .iter()
                    .find(|p| p.partition == partition)
                    .map(|p| p.leader_epoch)
                    .unwrap_or(u64::MAX),
                WireRecordV0 {
                    timestamp_ms: -1,
                    key,
                    value,
                    headers,
                },
                acks,
            )
            .await;

        if result.is_ok() {
            self.close_sticky_batch(topic.topic_id).await;
        }
        result
    }

    pub async fn produce_buffered(
        &self,
        topic_name: &str,
        key: Option<Vec<u8>>,
        value: Option<Vec<u8>>,
        headers: Vec<WireHeaderV0>,
        acks: AckMode,
    ) -> Result<ProduceOutcome> {
        let topic = self.topic_metadata(topic_name).await?;
        let sticky = key.is_none();
        let partition = match key.as_deref() {
            Some(key) => partition_for_key(key, topic.partition_count)?,
            None => {
                self.choose_sticky_partition(topic.topic_id, &topic.partitions)
                    .await?
            }
        };
        let offsets = self
            .buffer_record_and_wait(
                &topic,
                partition,
                WireRecordV0 {
                    timestamp_ms: -1,
                    key,
                    value,
                    headers,
                },
                BatchMode::V0(acks),
                sticky,
            )
            .await?;
        Ok(ProduceOutcome {
            topic_id: topic.topic_id,
            partition,
            base_offset: offsets.base_offset,
            last_offset: offsets.last_offset,
        })
    }

    pub async fn produce_idempotent_buffered(
        &self,
        topic_name: &str,
        record: WireRecordV0,
    ) -> Result<IdempotentProduceOutcome> {
        if record.timestamp_ms < 0 {
            return Err(ClientError::Metadata(
                "idempotent buffered Produce requires explicit non-negative timestamp",
            ));
        }
        let topic = self.topic_metadata(topic_name).await?;
        let sticky = record.key.is_none();
        let partition = match record.key.as_deref() {
            Some(key) => partition_for_key(key, topic.partition_count)?,
            None => {
                self.choose_sticky_partition(topic.topic_id, &topic.partitions)
                    .await?
            }
        };
        let offsets = self
            .buffer_record_and_wait(&topic, partition, record, BatchMode::V1, sticky)
            .await?;
        let base_offset = offsets.base_offset.ok_or(ClientError::InvalidResponse(
            "idempotent buffered Produce omitted base offset",
        ))?;
        let last_offset = offsets.last_offset.ok_or(ClientError::InvalidResponse(
            "idempotent buffered Produce omitted last offset",
        ))?;
        Ok(IdempotentProduceOutcome {
            topic_id: topic.topic_id,
            partition,
            base_offset,
            last_offset,
        })
    }

    pub async fn produce_partition(
        &self,
        topic_id: TopicId,
        partition: u32,
        leader_epoch: u64,
        records: Vec<WireRecordV0>,
        acks: AckMode,
    ) -> Result<Option<ProduceResponseV0>> {
        let leader_id = self.leader_for(topic_id, partition).await?;
        let connection = self.connection_for_broker(leader_id).await?;
        let request = ProduceRequestV0 {
            acks,
            partitions: vec![ProducePartitionRequestV0 {
                topic_id,
                partition,
                leader_epoch,
                records,
            }],
            tags: Vec::new(),
        };

        if acks == AckMode::None {
            connection
                .send_no_response(
                    ApiKey::Produce,
                    0,
                    request.encode_body()?,
                    self.inner.config.request_timeout,
                )
                .await?;
            return Ok(None);
        }

        let response: ProduceResponseV0 = connection
            .request_body(
                ApiKey::Produce,
                0,
                request.encode_body()?,
                self.inner.config.request_timeout,
            )
            .await?;
        if response.partitions.iter().any(|partition| {
            matches!(
                PublicError::try_from(partition.error_code),
                Ok(PublicError::NotLeader | PublicError::StaleEpoch)
            )
        }) {
            self.refresh_metadata().await?;
        }
        Ok(Some(response))
    }

    pub async fn producer_authority(&self) -> Option<ProducerAuthority> {
        self.inner
            .producer
            .lock()
            .await
            .as_ref()
            .map(|session| session.authority)
    }

    pub async fn init_producer(&self) -> Result<ProducerAuthority> {
        let _control = self.inner.producer_control.lock().await;
        if self.inner.producer.lock().await.is_some() {
            return Err(ClientError::ProducerSessionAlreadyInitialized);
        }
        let result = self.init_producer_exchange(None).await;
        self.finish_init_producer(None, result).await
    }

    pub async fn restart_producer(&self) -> Result<ProducerAuthority> {
        let _control = self.inner.producer_control.lock().await;
        let expected = {
            let producer = self.inner.producer.lock().await;
            let session = producer
                .as_ref()
                .ok_or(ClientError::ProducerSessionRequired)?;
            if let Some(((topic_id, partition), _)) = session
                .partitions
                .iter()
                .find(|(_, state)| state.pending.is_some())
            {
                return Err(ClientError::ProducerRequestInFlight {
                    topic_id: *topic_id,
                    partition: *partition,
                });
            }
            session.authority
        };
        let result = self.init_producer_exchange(Some(expected)).await;
        self.finish_init_producer(Some(expected), result).await
    }

    pub async fn recover_producer_session(&self) -> Result<ProducerAuthority> {
        let _control = self.inner.producer_control.lock().await;
        {
            let producer = self.inner.producer.lock().await;
            if let Some(session) = producer.as_ref()
                && let Some(((topic_id, partition), _)) = session
                    .partitions
                    .iter()
                    .find(|(_, state)| state.pending.is_some())
            {
                return Err(ClientError::ProducerRequestInFlight {
                    topic_id: *topic_id,
                    partition: *partition,
                });
            }
        }
        *self.inner.producer.lock().await = None;
        let result = self.init_producer_exchange(None).await;
        self.finish_init_producer(None, result).await
    }

    pub async fn produce_idempotent_partition(
        &self,
        topic_id: TopicId,
        partition: u32,
        records: Vec<WireRecordV0>,
    ) -> Result<IdempotentProduceOutcome> {
        let (leader_broker_id, leader_epoch) = self.leader_route(topic_id, partition).await?;
        let _control = self.inner.producer_control.lock().await;
        {
            let mut producer = self.inner.producer.lock().await;
            let session = producer
                .as_mut()
                .ok_or(ClientError::ProducerSessionRequired)?;
            reserve_producer_request(
                session,
                self.inner.config.max_producer_partitions,
                topic_id,
                partition,
                leader_epoch,
                records,
            )?;
        }
        drop(_control);
        self.drive_pending_produce(topic_id, partition, leader_broker_id)
            .await
    }

    pub async fn retry_idempotent_partition(
        &self,
        topic_id: TopicId,
        partition: u32,
    ) -> Result<IdempotentProduceOutcome> {
        let _control = self.inner.producer_control.lock().await;
        {
            let producer = self.inner.producer.lock().await;
            let session = producer
                .as_ref()
                .ok_or(ClientError::ProducerSessionRequired)?;
            let pending = session
                .partitions
                .get(&(topic_id, partition))
                .and_then(|state| state.pending.as_ref())
                .ok_or(ClientError::Metadata(
                    "producer partition has no unresolved request to retry",
                ))?;
            if pending.active {
                return Err(ClientError::ProducerRequestInFlight {
                    topic_id,
                    partition,
                });
            }
        }
        let leader_broker_id = self.refresh_producer_route(topic_id, partition).await?;
        {
            let mut producer = self.inner.producer.lock().await;
            let session = producer
                .as_mut()
                .ok_or(ClientError::ProducerSessionRequired)?;
            let pending = session
                .partitions
                .get_mut(&(topic_id, partition))
                .and_then(|state| state.pending.as_mut())
                .ok_or(ClientError::Metadata(
                    "producer partition has no unresolved request to retry",
                ))?;
            pending.active = true;
        }
        drop(_control);
        self.drive_pending_produce(topic_id, partition, leader_broker_id)
            .await
    }

    async fn init_producer_exchange(
        &self,
        expected_authority: Option<ProducerAuthority>,
    ) -> Result<ProducerAuthority> {
        let body = InitProducerRequestV0 {
            expected_authority,
            tags: Vec::new(),
        }
        .encode_body()?;
        let response = self
            .inner
            .bootstrap
            .request_raw(
                ApiKey::InitProducer,
                0,
                body,
                self.inner.config.request_timeout,
            )
            .await;
        let frame = match response {
            Err(error) if transport_outcome_is_ambiguous(&error) => {
                return Err(ClientError::AmbiguousMutation {
                    api: ApiKey::InitProducer,
                });
            }
            Err(error) => return Err(error),
            Ok(frame) => frame,
        };
        let error = PublicError::try_from(frame.error_code)?;
        if error == PublicError::RequestTimeout {
            return Err(ClientError::AmbiguousMutation {
                api: ApiKey::InitProducer,
            });
        }
        if error != PublicError::None {
            return Err(ClientError::Broker(error));
        }
        Ok(InitProducerResponseV0::decode_body(&frame.body)?.authority)
    }

    async fn finish_init_producer(
        &self,
        expected_authority: Option<ProducerAuthority>,
        result: Result<ProducerAuthority>,
    ) -> Result<ProducerAuthority> {
        match result {
            Ok(authority) => {
                if let Some(expected) = expected_authority
                    && (authority.producer_id != expected.producer_id
                        || expected.producer_epoch.checked_next() != Some(authority.producer_epoch))
                {
                    *self.inner.producer.lock().await = None;
                    return Err(ClientError::InvalidResponse(
                        "InitProducer epoch bump did not return the exact next authority",
                    ));
                }
                *self.inner.producer.lock().await = Some(ProducerSessionState {
                    authority,
                    partitions: BTreeMap::new(),
                });
                Ok(authority)
            }
            Err(
                error @ ClientError::AmbiguousMutation {
                    api: ApiKey::InitProducer,
                },
            )
            | Err(error @ ClientError::Broker(PublicError::ProducerFenced))
            | Err(error @ ClientError::Broker(PublicError::UnknownProducer)) => {
                *self.inner.producer.lock().await = None;
                Err(error)
            }
            Err(error) => Err(error),
        }
    }

    async fn drive_pending_produce(
        &self,
        topic_id: TopicId,
        partition: u32,
        mut leader_broker_id: u32,
    ) -> Result<IdempotentProduceOutcome> {
        let mut attempts = 0_usize;
        loop {
            let body = self.pending_producer_body(topic_id, partition).await?;
            let connection = match self.connection_for_broker(leader_broker_id).await {
                Ok(connection) => connection,
                Err(error)
                    if attempts < self.inner.config.max_producer_retries
                        && transport_retryable(&error) =>
                {
                    attempts += 1;
                    self.inner
                        .connections
                        .write()
                        .await
                        .remove(&leader_broker_id);
                    leader_broker_id = match self.refresh_producer_route(topic_id, partition).await
                    {
                        Ok(leader) => leader,
                        Err(error) => {
                            self.release_pending_producer_drive(topic_id, partition)
                                .await;
                            return Err(error);
                        }
                    };
                    continue;
                }
                Err(error) => {
                    self.release_pending_producer_drive(topic_id, partition)
                        .await;
                    return Err(error);
                }
            };
            let frame = connection
                .request_raw(ApiKey::Produce, 1, body, self.inner.config.request_timeout)
                .await;
            let frame = match frame {
                Ok(frame) => frame,
                Err(error)
                    if attempts < self.inner.config.max_producer_retries
                        && transport_retryable(&error) =>
                {
                    attempts += 1;
                    if matches!(error, ClientError::Io(_) | ClientError::ConnectionClosed) {
                        self.inner
                            .connections
                            .write()
                            .await
                            .remove(&leader_broker_id);
                    }
                    continue;
                }
                Err(error) => {
                    self.release_pending_producer_drive(topic_id, partition)
                        .await;
                    return Err(error);
                }
            };
            let top_error = match PublicError::try_from(frame.error_code) {
                Ok(error) => error,
                Err(error) => {
                    self.release_pending_producer_drive(topic_id, partition)
                        .await;
                    return Err(error.into());
                }
            };
            if top_error != PublicError::None {
                if top_error.retry_class() == Some(RetryClass::Retryable)
                    && attempts < self.inner.config.max_producer_retries
                {
                    attempts += 1;
                    if matches!(top_error, PublicError::NotLeader | PublicError::StaleEpoch) {
                        leader_broker_id =
                            match self.refresh_producer_route(topic_id, partition).await {
                                Ok(leader) => leader,
                                Err(error) => {
                                    self.release_pending_producer_drive(topic_id, partition)
                                        .await;
                                    return Err(error);
                                }
                            };
                    }
                    continue;
                }
                if top_error.retry_class() == Some(RetryClass::Terminal) {
                    self.clear_pending_producer(topic_id, partition).await;
                } else {
                    self.release_pending_producer_drive(topic_id, partition)
                        .await;
                }
                return Err(ClientError::Broker(top_error));
            }
            let response = match ProduceResponseV1::decode_body(&frame.body) {
                Ok(response) => response,
                Err(error) => {
                    self.release_pending_producer_drive(topic_id, partition)
                        .await;
                    return Err(error.into());
                }
            };
            let result = match response.partitions.first() {
                Some(result) => result,
                None => {
                    self.release_pending_producer_drive(topic_id, partition)
                        .await;
                    return Err(ClientError::InvalidResponse(
                        "Produce V1 response has no partition result",
                    ));
                }
            };
            if response.partitions.len() != 1
                || result.topic_id != topic_id
                || result.partition != partition
            {
                self.release_pending_producer_drive(topic_id, partition)
                    .await;
                return Err(ClientError::InvalidResponse(
                    "Produce V1 response does not match requested partition",
                ));
            }
            let error = match PublicError::try_from(result.error_code) {
                Ok(error) => error,
                Err(error) => {
                    self.release_pending_producer_drive(topic_id, partition)
                        .await;
                    return Err(error.into());
                }
            };
            if error == PublicError::None {
                if result.base_offset == u64::MAX || result.last_offset == u64::MAX {
                    self.release_pending_producer_drive(topic_id, partition)
                        .await;
                    return Err(ClientError::InvalidResponse(
                        "successful Produce V1 response omitted committed offsets",
                    ));
                }
                return self
                    .complete_pending_producer(
                        topic_id,
                        partition,
                        result.base_offset,
                        result.last_offset,
                    )
                    .await;
            }
            if error.retry_class() == Some(RetryClass::Retryable)
                && attempts < self.inner.config.max_producer_retries
            {
                attempts += 1;
                if matches!(error, PublicError::NotLeader | PublicError::StaleEpoch) {
                    leader_broker_id = match self.refresh_producer_route(topic_id, partition).await
                    {
                        Ok(leader) => leader,
                        Err(error) => {
                            self.release_pending_producer_drive(topic_id, partition)
                                .await;
                            return Err(error);
                        }
                    };
                }
                continue;
            }
            if error.retry_class() == Some(RetryClass::Terminal) {
                self.clear_pending_producer(topic_id, partition).await;
            } else {
                self.release_pending_producer_drive(topic_id, partition)
                    .await;
            }
            return Err(ClientError::Broker(error));
        }
    }

    async fn pending_producer_body(&self, topic_id: TopicId, partition: u32) -> Result<Vec<u8>> {
        let producer = self.inner.producer.lock().await;
        producer
            .as_ref()
            .ok_or(ClientError::ProducerSessionRequired)?
            .partitions
            .get(&(topic_id, partition))
            .and_then(|state| state.pending.as_ref())
            .map(|pending| pending.body.clone())
            .ok_or(ClientError::Metadata(
                "producer pending request disappeared",
            ))
    }

    async fn refresh_producer_route(&self, topic_id: TopicId, partition: u32) -> Result<u32> {
        self.refresh_metadata().await?;
        let (leader_broker_id, leader_epoch) = self.leader_route(topic_id, partition).await?;
        let mut producer = self.inner.producer.lock().await;
        let session = producer
            .as_mut()
            .ok_or(ClientError::ProducerSessionRequired)?;
        retarget_pending_producer_request(session, topic_id, partition, leader_epoch)?;
        Ok(leader_broker_id)
    }

    async fn complete_pending_producer(
        &self,
        topic_id: TopicId,
        partition: u32,
        base_offset: u64,
        last_offset: u64,
    ) -> Result<IdempotentProduceOutcome> {
        let mut producer = self.inner.producer.lock().await;
        let session = producer
            .as_mut()
            .ok_or(ClientError::ProducerSessionRequired)?;
        complete_producer_request(session, topic_id, partition, base_offset, last_offset)
    }

    async fn clear_pending_producer(&self, topic_id: TopicId, partition: u32) {
        if let Some(session) = self.inner.producer.lock().await.as_mut()
            && let Some(state) = session.partitions.get_mut(&(topic_id, partition))
        {
            state.pending = None;
        }
    }

    async fn release_pending_producer_drive(&self, topic_id: TopicId, partition: u32) {
        if let Some(session) = self.inner.producer.lock().await.as_mut()
            && let Some(pending) = session
                .partitions
                .get_mut(&(topic_id, partition))
                .and_then(|state| state.pending.as_mut())
        {
            pending.active = false;
        }
    }

    pub async fn fetch(&self, request: FetchRequestV0) -> Result<FetchResponseV0> {
        if request.max_wait_ms > duration_ms_u32(self.inner.config.request_timeout) {
            return Err(ClientError::Metadata(
                "Fetch max_wait_ms exceeds request timeout",
            ));
        }
        let (topic_id, partition) = request
            .partitions
            .first()
            .map(|p| (p.topic_id, p.partition))
            .ok_or(ClientError::Metadata(
                "Fetch requires at least one partition",
            ))?;
        let leader_id = self.leader_for(topic_id, partition).await?;
        let connection = self.connection_for_broker(leader_id).await?;
        let response: FetchResponseV0 = connection
            .request_body(
                ApiKey::Fetch,
                0,
                request.encode_body()?,
                self.inner.config.request_timeout,
            )
            .await?;
        if response.partitions.iter().any(|partition| {
            matches!(
                PublicError::try_from(partition.error_code),
                Ok(PublicError::NotLeader | PublicError::StaleEpoch)
            )
        }) {
            self.refresh_metadata().await?;
        }
        Ok(response)
    }

    pub async fn list_offsets(
        &self,
        request: ListOffsetsRequestV0,
    ) -> Result<ListOffsetsResponseV0> {
        let leader_id = if let Some(query) = request.queries.first() {
            self.leader_for(query.topic_id, query.partition).await?
        } else {
            return Err(ClientError::Metadata(
                "ListOffsets requires at least one query",
            ));
        };
        let connection = self.connection_for_broker(leader_id).await?;
        let response: ListOffsetsResponseV0 = connection
            .request_body(
                ApiKey::ListOffsets,
                0,
                request.encode_body()?,
                self.inner.config.request_timeout,
            )
            .await?;
        if response.results.iter().any(|result| {
            matches!(
                PublicError::try_from(result.error_code),
                Ok(PublicError::NotLeader | PublicError::StaleEpoch)
            )
        }) {
            self.refresh_metadata().await?;
        }
        Ok(response)
    }

    pub async fn close(&self) {
        let _ = self.close_with_drain().await;
    }

    pub async fn flush_producer_batches(&self) -> Result<()> {
        let deadline = time::Instant::now() + self.inner.config.producer_delivery_timeout;
        loop {
            let keys = { self.inner.accumulator.lock().await.request_close_all() };
            for key in keys {
                self.drive_accumulator_partition(key).await?;
            }
            if self.inner.accumulator.lock().await.counts().2 == 0 {
                return Ok(());
            }
            if time::Instant::now() >= deadline {
                return Err(ClientError::Timeout);
            }
            time::sleep(Duration::from_millis(1)).await;
        }
    }

    pub async fn close_with_drain(&self) -> Result<()> {
        self.inner.accumulator.lock().await.close_admission();
        let drain = self.flush_producer_batches().await;
        self.close_connections().await;
        drain
    }

    async fn close_connections(&self) {
        self.inner.bootstrap.close().await;
        let connections: Vec<_> = self
            .inner
            .connections
            .read()
            .await
            .values()
            .cloned()
            .collect();
        for connection in connections {
            connection.close().await;
        }
    }

    async fn buffer_record_and_wait(
        &self,
        topic: &MetadataTopicV0,
        partition: u32,
        record: WireRecordV0,
        mode: BatchMode,
        sticky: bool,
    ) -> Result<BufferedOffsets> {
        validate_buffered_delivery_budget(&self.inner.config)?;
        let performance = self.topic_performance_v1(topic).await?;
        let logical_bytes = record.encoded_logical_len()?;
        let topic_limits = TopicBatchLimits {
            max_record_bytes: usize::try_from(performance.max_record_bytes)
                .map_err(|_| ClientError::Metadata("topic record-byte limit overflow"))?,
            max_batch_records: self.inner.config.batch_max_records.min(
                usize::try_from(performance.max_batch_records)
                    .map_err(|_| ClientError::Metadata("topic batch-record limit overflow"))?,
            ),
            max_batch_bytes: self.inner.config.batch_max_bytes.min(
                usize::try_from(performance.max_batch_uncompressed_bytes)
                    .map_err(|_| ClientError::Metadata("topic batch-byte limit overflow"))?,
            ),
        };
        let accumulator_limits = AccumulatorLimits {
            max_total_bytes: self.inner.config.accumulator_max_bytes,
            max_total_records: self.inner.config.accumulator_max_records,
            max_partitions: self.inner.config.accumulator_max_partitions,
        };
        let key = (topic.topic_id, partition);
        let linger_ms = u64::try_from(self.inner.config.batch_linger.as_millis())
            .map_err(|_| ClientError::Metadata("batch linger conversion overflow"))?;

        let mut admission = loop {
            let now_ms = self.batch_now_ms();
            let attempt = self.inner.accumulator.lock().await.admit(
                &record,
                AdmissionSpec {
                    limits: accumulator_limits,
                    topic_limits,
                    key,
                    mode,
                    logical_bytes,
                    now_ms,
                    linger_ms,
                    sticky,
                },
            )?;
            match attempt {
                AdmissionAttempt::Accepted(admission) => break admission,
                AdmissionAttempt::DrainFirst => {
                    self.drive_accumulator_partition(key).await?;
                }
            }
        };

        let delivery_deadline = time::Instant::now() + self.inner.config.producer_delivery_timeout;
        if admission.drive_now {
            self.drive_accumulator_partition(key).await?;
        } else {
            loop {
                let remaining_ms = admission.deadline_ms.saturating_sub(self.batch_now_ms());
                tokio::select! {
                    result = &mut admission.receiver => return decode_buffered_delivery(result),
                    _ = time::sleep(Duration::from_millis(remaining_ms)) => {
                        let expired = self
                            .inner
                            .accumulator
                            .lock()
                            .await
                            .close_expired(self.batch_now_ms());
                        for expired_key in expired {
                            self.drive_accumulator_partition(expired_key).await?;
                        }
                        break;
                    }
                    _ = self.inner.accumulator_notify.notified() => {
                        self.drive_accumulator_partition(key).await?;
                    }
                }
            }
        }

        match time::timeout_at(delivery_deadline, admission.receiver).await {
            Ok(result) => decode_buffered_delivery(result),
            Err(_) => Err(ClientError::Timeout),
        }
    }

    async fn topic_performance_v1(
        &self,
        topic: &MetadataTopicV0,
    ) -> Result<TopicPerformanceConfigV1> {
        if let Some(policy) = self
            .inner
            .topic_performance
            .read()
            .await
            .get(&topic.topic_id)
            .copied()
        {
            return Ok(policy);
        }
        if !self.inner.bootstrap.supports_api(ApiKey::DescribeTopic, 1) {
            return Err(ClientError::UnsupportedApi(ApiKey::DescribeTopic));
        }
        let response: DescribeTopicResponseV1 = self
            .inner
            .bootstrap
            .request_body(
                ApiKey::DescribeTopic,
                1,
                DescribeTopicRequestV1 {
                    topic_name: topic.topic_name.clone(),
                    tags: Vec::new(),
                }
                .encode_body()?,
                self.inner.config.request_timeout,
            )
            .await?;
        if response.topic_id != topic.topic_id || response.partition_count != topic.partition_count
        {
            return Err(ClientError::InvalidResponse(
                "DescribeTopic V1 does not match metadata topic identity",
            ));
        }
        response.performance.validate()?;
        self.inner
            .topic_performance
            .write()
            .await
            .insert(topic.topic_id, response.performance);
        Ok(response.performance)
    }

    fn batch_now_ms(&self) -> u64 {
        self.inner
            .batch_clock_start
            .elapsed()
            .as_millis()
            .min(u128::from(u64::MAX)) as u64
    }

    async fn producer_partition_pending(&self, key: PartitionKey) -> bool {
        self.inner
            .producer
            .lock()
            .await
            .as_ref()
            .and_then(|session| session.partitions.get(&key))
            .is_some_and(|state| state.pending.is_some())
    }

    async fn drive_accumulator_partition(&self, key: PartitionKey) -> Result<()> {
        if !self.inner.accumulator.lock().await.begin_drive(key) {
            return Ok(());
        }
        loop {
            let closed_mode = { self.inner.accumulator.lock().await.closed_mode(key) };
            if !batch_drive_allowed(
                closed_mode,
                closed_mode == Some(BatchMode::V1) && self.producer_partition_pending(key).await,
            ) {
                self.inner.accumulator.lock().await.end_drive(key);
                return Ok(());
            }
            let Some(batch) = ({ self.inner.accumulator.lock().await.take_closed(key) }) else {
                self.inner.accumulator.lock().await.end_drive(key);
                return Ok(());
            };
            if batch.has_sticky_record {
                self.close_sticky_batch(key.0).await;
            }
            let delivery = match batch.mode {
                BatchMode::V0(acks) => self.send_buffered_v0_batch(key, &batch.records, acks).await,
                BatchMode::V1 => self.send_buffered_v1_batch(key, &batch.records).await,
            };
            match delivery {
                Ok(outcomes) => batch.complete_success(outcomes)?,
                Err(error) => batch.complete_error(error.to_string()),
            }
            self.inner.accumulator.lock().await.complete_in_flight(key);
        }
    }

    async fn send_buffered_v0_batch(
        &self,
        key: PartitionKey,
        records: &[WireRecordV0],
        acks: AckMode,
    ) -> Result<Vec<BufferedOffsets>> {
        let (_, leader_epoch) = self.leader_route(key.0, key.1).await?;
        let response = self
            .produce_partition(key.0, key.1, leader_epoch, records.to_vec(), acks)
            .await?;
        let Some(response) = response else {
            return Ok(vec![
                BufferedOffsets {
                    base_offset: None,
                    last_offset: None,
                };
                records.len()
            ]);
        };
        let result = response
            .partitions
            .first()
            .ok_or(ClientError::InvalidResponse(
                "batched Produce V0 response has no partition",
            ))?;
        if response.partitions.len() != 1 || result.topic_id != key.0 || result.partition != key.1 {
            return Err(ClientError::InvalidResponse(
                "batched Produce V0 response identity mismatch",
            ));
        }
        let error = PublicError::try_from(result.error_code)?;
        if error != PublicError::None {
            return Err(ClientError::Broker(error));
        }
        distribute_record_offsets(result.base_offset, result.last_offset, records.len(), false)
    }

    async fn send_buffered_v1_batch(
        &self,
        key: PartitionKey,
        records: &[WireRecordV0],
    ) -> Result<Vec<BufferedOffsets>> {
        let deadline = time::Instant::now() + self.inner.config.producer_delivery_timeout;
        let mut first = true;
        loop {
            let result = if first {
                first = false;
                self.produce_idempotent_partition(key.0, key.1, records.to_vec())
                    .await
            } else {
                self.retry_idempotent_partition(key.0, key.1).await
            };
            match result {
                Ok(outcome) => {
                    return distribute_record_offsets(
                        outcome.base_offset,
                        outcome.last_offset,
                        records.len(),
                        true,
                    );
                }
                Err(error) => {
                    if !self.producer_partition_pending(key).await {
                        return Err(error);
                    }
                    if time::Instant::now() >= deadline {
                        return Err(ClientError::Timeout);
                    }
                    time::sleep(Duration::from_millis(1)).await;
                }
            }
        }
    }

    async fn produce_once(
        &self,
        topic_id: TopicId,
        partition: u32,
        leader_epoch: u64,
        record: WireRecordV0,
        acks: AckMode,
    ) -> Result<ProduceOutcome> {
        let response = self
            .produce_partition(topic_id, partition, leader_epoch, vec![record], acks)
            .await?;
        let Some(response) = response else {
            return Ok(ProduceOutcome {
                topic_id,
                partition,
                base_offset: None,
                last_offset: None,
            });
        };
        let result = response
            .partitions
            .first()
            .ok_or(ClientError::InvalidResponse(
                "Produce response has no partition result",
            ))?;
        let error = PublicError::try_from(result.error_code)?;
        if error != PublicError::None {
            if matches!(error, PublicError::NotLeader | PublicError::StaleEpoch) {
                self.refresh_metadata().await?;
            }
            return Err(ClientError::Broker(error));
        }
        Ok(ProduceOutcome {
            topic_id,
            partition,
            base_offset: (result.base_offset != u64::MAX).then_some(result.base_offset),
            last_offset: (result.last_offset != u64::MAX).then_some(result.last_offset),
        })
    }

    async fn topic_metadata(
        &self,
        topic_name: &str,
    ) -> Result<astralog_protocol::api::MetadataTopicV0> {
        if let Some(topic) = self
            .inner
            .metadata
            .read()
            .await
            .as_ref()
            .and_then(|metadata| {
                metadata
                    .topics
                    .iter()
                    .find(|topic| topic.topic_name == topic_name)
            })
            .cloned()
        {
            return Ok(topic);
        }
        let metadata = self.refresh_metadata().await?;
        metadata
            .topics
            .into_iter()
            .find(|topic| topic.topic_name == topic_name)
            .ok_or(ClientError::Metadata("topic not present in metadata"))
    }

    async fn choose_sticky_partition(
        &self,
        topic_id: TopicId,
        partitions: &[astralog_protocol::api::MetadataPartitionV0],
    ) -> Result<u32> {
        let writable: Vec<u32> = partitions
            .iter()
            .filter(|p| p.partition_error == 0 && p.leader_broker_id != u32::MAX)
            .map(|p| p.partition)
            .collect();
        let mut sticky = self.inner.sticky.lock().await;
        if let std::collections::hash_map::Entry::Vacant(entry) = sticky.entry(topic_id) {
            entry.insert(StickyPartitioner::new(ClientRng::new()?));
        }
        sticky
            .get_mut(&topic_id)
            .ok_or(ClientError::Metadata("sticky partitioner missing"))?
            .choose(&writable)
            .map_err(Into::into)
    }

    async fn close_sticky_batch(&self, topic_id: TopicId) {
        if let Some(sticky) = self.inner.sticky.lock().await.get_mut(&topic_id) {
            sticky.on_batch_closed();
        }
    }

    async fn leader_for(&self, topic_id: TopicId, partition: u32) -> Result<u32> {
        if let Some((leader, _)) = self.find_leader_route(topic_id, partition).await {
            return Ok(leader);
        }
        self.refresh_metadata().await?;
        self.find_leader_route(topic_id, partition)
            .await
            .map(|(leader, _)| leader)
            .ok_or(ClientError::Metadata("partition leader is unknown"))
    }

    async fn leader_route(&self, topic_id: TopicId, partition: u32) -> Result<(u32, u64)> {
        if let Some(route) = self.find_leader_route(topic_id, partition).await
            && route.1 != UNKNOWN_EPOCH
        {
            return Ok(route);
        }
        self.refresh_metadata().await?;
        self.find_leader_route(topic_id, partition)
            .await
            .filter(|(_, epoch)| *epoch != UNKNOWN_EPOCH)
            .ok_or(ClientError::Metadata(
                "partition leader authority is unknown",
            ))
    }

    async fn find_leader_route(&self, topic_id: TopicId, partition: u32) -> Option<(u32, u64)> {
        self.inner
            .metadata
            .read()
            .await
            .as_ref()
            .and_then(|metadata| {
                metadata
                    .topics
                    .iter()
                    .find(|topic| topic.topic_id == topic_id)
                    .and_then(|topic| topic.partitions.iter().find(|p| p.partition == partition))
                    .map(|p| (p.leader_broker_id, p.leader_epoch))
            })
            .filter(|(broker_id, _)| *broker_id != UNKNOWN_BROKER_ID)
    }

    async fn connection_for_broker(&self, broker_id: u32) -> Result<Arc<BrokerConnection>> {
        let metadata = self
            .inner
            .metadata
            .read()
            .await
            .clone()
            .ok_or(ClientError::Metadata("broker metadata is unavailable"))?;
        self.connection_for_broker_from_metadata(broker_id, &metadata)
            .await
    }

    async fn connection_for_broker_from_metadata(
        &self,
        broker_id: u32,
        metadata: &MetadataResponseV0,
    ) -> Result<Arc<BrokerConnection>> {
        if let Some(connection) = self.inner.connections.read().await.get(&broker_id).cloned() {
            return Ok(connection);
        }
        let endpoint = metadata
            .brokers
            .iter()
            .find(|broker| broker.broker_id == broker_id)
            .map(|broker| format!("{}:{}", broker.host, broker.port))
            .ok_or(ClientError::Metadata("leader broker address is unknown"))?;
        let connection = Arc::new(BrokerConnection::connect(&endpoint, &self.inner.config).await?);
        let mut connections = self.inner.connections.write().await;
        Ok(connections
            .entry(broker_id)
            .or_insert_with(|| connection.clone())
            .clone())
    }

    async fn group_request_frame(
        &self,
        group_id: &GroupId,
        api: ApiKey,
        body: Vec<u8>,
        mutation: bool,
    ) -> Result<WireFrame> {
        let deadline = time::Instant::now() + self.inner.config.request_timeout;
        let _permit = time::timeout_at(deadline, self.inner.group_permits.clone().acquire_owned())
            .await
            .map_err(|_| ClientError::Timeout)?
            .map_err(|_| ClientError::ConnectionClosed)?;
        let mut route =
            if let Some(route) = self.inner.group_routes.read().await.get(group_id).copied() {
                route
            } else {
                self.resolve_group_coordinator(group_id).await?
            };
        let mut attempts = 0_usize;
        let mut reconnected_for_api = false;
        loop {
            let connection = self
                .connection_for_broker(route.coordinator_broker_id)
                .await?;
            let response = connection
                .request_raw(api, 0, body.clone(), self.inner.config.request_timeout)
                .await;
            let frame = match response {
                Ok(frame) => frame,
                Err(ClientError::UnsupportedApi(_)) if !reconnected_for_api => {
                    self.inner
                        .connections
                        .write()
                        .await
                        .remove(&route.coordinator_broker_id);
                    reconnected_for_api = true;
                    continue;
                }
                Err(error) if mutation && transport_outcome_is_ambiguous(&error) => {
                    return Err(ClientError::AmbiguousMutation { api });
                }
                Err(error)
                    if !mutation
                        && attempts < self.inner.config.max_group_retries
                        && transport_retryable(&error) =>
                {
                    attempts += 1;
                    route = self.resolve_group_coordinator(group_id).await?;
                    continue;
                }
                Err(error) => return Err(error),
            };
            let error = PublicError::try_from(frame.error_code)?;
            let hint = decode_p6_hint(api, &frame.body)?;
            if error == PublicError::GroupEpochStale {
                self.advance_group_epoch_from_hint(group_id, hint).await?;
            }
            if let Some(hint_route) = route_from_hint(route, hint) {
                self.cache_group_route(group_id.clone(), hint_route).await;
                if hint_route.coordinator_epoch >= route.coordinator_epoch {
                    route = hint_route;
                }
            }
            if error == PublicError::None {
                return Ok(frame);
            }
            if mutation && error == PublicError::RequestTimeout {
                return Err(ClientError::AmbiguousMutation { api });
            }
            if matches!(
                error,
                PublicError::NotLeader | PublicError::BrokerBusy | PublicError::BrokerNotAvailable
            ) && attempts < self.inner.config.max_group_retries
            {
                attempts += 1;
                route = if hint.coordinator_broker_id != UNKNOWN_BROKER_ID
                    && hint.coordinator_epoch != UNKNOWN_EPOCH
                    && hint.coordinator_epoch >= route.coordinator_epoch
                {
                    GroupCoordinatorRoute {
                        shard: route.shard,
                        groups_topic_id: route.groups_topic_id,
                        coordinator_broker_id: hint.coordinator_broker_id,
                        coordinator_epoch: hint.coordinator_epoch,
                    }
                } else {
                    self.resolve_group_coordinator(group_id).await?
                };
                continue;
            }
            return Err(ClientError::Broker(error));
        }
    }

    async fn cache_group_route(&self, group_id: GroupId, route: GroupCoordinatorRoute) {
        let mut routes = self.inner.group_routes.write().await;
        insert_bounded_group_route(
            &mut routes,
            self.inner.config.max_group_cache,
            group_id,
            route,
        );
    }

    async fn update_member_state(
        &self,
        group_id: GroupId,
        incoming: GroupMemberState,
    ) -> Result<()> {
        let mut members = self.inner.group_members.write().await;
        insert_bounded_member_state(
            &mut members,
            self.inner.config.max_group_cache,
            group_id,
            incoming,
        )
    }

    async fn advance_group_epoch_from_hint(
        &self,
        group_id: &GroupId,
        hint: CoordinatorHintV0,
    ) -> Result<()> {
        if hint.current_group_epoch == 0 || hint.current_member_epoch == 0 {
            return Ok(());
        }
        let mut members = self.inner.group_members.write().await;
        let Some(state) = members.get_mut(group_id) else {
            return Ok(());
        };
        if state.authority.member_epoch.get() != hint.current_member_epoch
            || hint.current_group_epoch <= state.authority.group_epoch.get()
        {
            return Ok(());
        }
        state.authority.group_epoch = GroupEpoch::new(hint.current_group_epoch)
            .map_err(|_| ClientError::InvalidResponse("group epoch hint is invalid"))?;
        Ok(())
    }
}

fn insert_bounded_group_route(
    routes: &mut BTreeMap<GroupId, GroupCoordinatorRoute>,
    max: usize,
    group_id: GroupId,
    route: GroupCoordinatorRoute,
) {
    if let Some(existing) = routes.get(&group_id)
        && existing.coordinator_epoch > route.coordinator_epoch
    {
        return;
    }
    if !routes.contains_key(&group_id)
        && routes.len() >= max
        && let Some(first) = routes.keys().next().cloned()
    {
        routes.remove(&first);
    }
    routes.insert(group_id, route);
}

fn insert_bounded_member_state(
    members: &mut BTreeMap<GroupId, GroupMemberState>,
    max: usize,
    group_id: GroupId,
    incoming: GroupMemberState,
) -> Result<()> {
    if let Some(existing) = members.get(&group_id) {
        let old = (
            existing.authority.group_epoch.get(),
            existing.authority.member_epoch.get(),
        );
        let new = (
            incoming.authority.group_epoch.get(),
            incoming.authority.member_epoch.get(),
        );
        if new < old {
            return Ok(());
        }
        if new == old && existing.authority.member_id != incoming.authority.member_id {
            return Err(ClientError::InvalidResponse(
                "same group/member epoch tuple changed MemberId",
            ));
        }
    }
    if !members.contains_key(&group_id)
        && members.len() >= max
        && let Some(first) = members.keys().next().cloned()
    {
        members.remove(&first);
    }
    members.insert(group_id, incoming);
    Ok(())
}

fn member_authority_from_hint(
    group_id: &GroupId,
    member_id: MemberId,
    hint: CoordinatorHintV0,
) -> Result<MemberAuthorityV0> {
    let group_epoch = GroupEpoch::new(hint.current_group_epoch).map_err(|_| {
        ClientError::InvalidResponse("successful group response has zero GroupEpoch")
    })?;
    let member_epoch = MemberEpoch::new(hint.current_member_epoch).map_err(|_| {
        ClientError::InvalidResponse("successful group response has zero MemberEpoch")
    })?;
    Ok(MemberAuthorityV0 {
        group_id: group_id.clone(),
        member_id,
        group_epoch,
        member_epoch,
    })
}

fn decode_p6_hint(api: ApiKey, body: &[u8]) -> Result<CoordinatorHintV0> {
    match api {
        ApiKey::JoinSubscribe => Ok(JoinSubscribeResponseV0::decode_body(body)?.hint),
        ApiKey::Heartbeat | ApiKey::RevokeAck => Ok(HeartbeatResponseV0::decode_body(body)?.hint),
        ApiKey::Leave => Ok(LeaveResponseV0::decode_body(body)?.hint),
        ApiKey::CommitOffsets => Ok(CommitOffsetsResponseV0::decode_body(body)?.hint),
        ApiKey::FetchOffsets => Ok(FetchOffsetsResponseV0::decode_body(body)?.hint),
        _ => Err(ClientError::InvalidResponse(
            "non-P6 API reached group response decoder",
        )),
    }
}

fn route_from_hint(
    current: GroupCoordinatorRoute,
    hint: CoordinatorHintV0,
) -> Option<GroupCoordinatorRoute> {
    if hint.coordinator_broker_id == UNKNOWN_BROKER_ID
        || hint.coordinator_epoch == UNKNOWN_EPOCH
        || hint.coordinator_epoch < current.coordinator_epoch
    {
        return None;
    }
    Some(GroupCoordinatorRoute {
        shard: current.shard,
        groups_topic_id: current.groups_topic_id,
        coordinator_broker_id: hint.coordinator_broker_id,
        coordinator_epoch: hint.coordinator_epoch,
    })
}

fn transport_outcome_is_ambiguous(error: &ClientError) -> bool {
    matches!(
        error,
        ClientError::Io(_) | ClientError::Timeout | ClientError::ConnectionClosed
    )
}

fn transport_retryable(error: &ClientError) -> bool {
    matches!(
        error,
        ClientError::Io(_)
            | ClientError::Timeout
            | ClientError::ConnectionClosed
            | ClientError::Broker(
                PublicError::NotLeader | PublicError::BrokerBusy | PublicError::BrokerNotAvailable
            )
    )
}

fn validate_buffered_delivery_budget(config: &ClientConfig) -> Result<()> {
    let producer_attempts = u32::try_from(config.max_producer_retries.saturating_add(1))
        .map_err(|_| ClientError::Metadata("producer retry count conversion overflow"))?;
    let minimum_delivery = config
        .request_timeout
        .checked_mul(producer_attempts)
        .ok_or(ClientError::Metadata("producer delivery deadline overflow"))?;
    if config.producer_delivery_timeout < minimum_delivery {
        return Err(ClientError::Metadata(
            "buffered producer delivery timeout must cover the configured P7 retry budget",
        ));
    }
    Ok(())
}

fn invalidated_partition_routes(
    previous: &MetadataResponseV0,
    current: &MetadataResponseV0,
) -> Vec<PartitionKey> {
    let mut invalidated = Vec::new();
    for old_topic in &previous.topics {
        let new_topic = current
            .topics
            .iter()
            .find(|topic| topic.topic_id == old_topic.topic_id);
        for old_partition in &old_topic.partitions {
            let changed = new_topic
                .and_then(|topic| {
                    topic
                        .partitions
                        .iter()
                        .find(|partition| partition.partition == old_partition.partition)
                })
                .is_none_or(|partition| {
                    partition.leader_broker_id != old_partition.leader_broker_id
                        || partition.leader_epoch != old_partition.leader_epoch
                        || partition.partition_error != old_partition.partition_error
                });
            if changed {
                invalidated.push((old_topic.topic_id, old_partition.partition));
            }
        }
    }
    invalidated
}

fn batch_drive_allowed(mode: Option<BatchMode>, producer_pending: bool) -> bool {
    !matches!(mode, Some(BatchMode::V1)) || !producer_pending
}

fn decode_buffered_delivery(
    result: std::result::Result<batching::DeliveryResult, oneshot::error::RecvError>,
) -> Result<BufferedOffsets> {
    match result {
        Ok(Ok(offsets)) => Ok(offsets),
        Ok(Err(error)) => Err(ClientError::BatchDelivery(error)),
        Err(_) => Err(ClientError::ConnectionClosed),
    }
}

fn distribute_record_offsets(
    base_offset: u64,
    last_offset: u64,
    record_count: usize,
    require_offsets: bool,
) -> Result<Vec<BufferedOffsets>> {
    if record_count == 0 {
        return Err(ClientError::InvalidResponse(
            "batched Produce contained no records",
        ));
    }
    if base_offset == u64::MAX || last_offset == u64::MAX {
        if require_offsets {
            return Err(ClientError::InvalidResponse(
                "idempotent batched Produce omitted committed offsets",
            ));
        }
        return Ok(vec![
            BufferedOffsets {
                base_offset: None,
                last_offset: None,
            };
            record_count
        ]);
    }
    let expected_last = base_offset
        .checked_add(
            u64::try_from(record_count - 1)
                .map_err(|_| ClientError::InvalidResponse("batched record count overflow"))?,
        )
        .ok_or(ClientError::InvalidResponse(
            "batched Produce offset range overflow",
        ))?;
    if last_offset != expected_last {
        return Err(ClientError::InvalidResponse(
            "batched Produce returned non-contiguous offset range",
        ));
    }
    (0..record_count)
        .map(|ordinal| {
            let offset =
                base_offset
                    .checked_add(u64::try_from(ordinal).map_err(|_| {
                        ClientError::InvalidResponse("batched offset ordinal overflow")
                    })?)
                    .ok_or(ClientError::InvalidResponse(
                        "batched Produce offset overflow",
                    ))?;
            Ok(BufferedOffsets {
                base_offset: Some(offset),
                last_offset: Some(offset),
            })
        })
        .collect()
}

fn reserve_producer_request(
    session: &mut ProducerSessionState,
    max_partitions: usize,
    topic_id: TopicId,
    partition: u32,
    leader_epoch: u64,
    records: Vec<WireRecordV0>,
) -> Result<()> {
    let key = (topic_id, partition);
    if !session.partitions.contains_key(&key) && session.partitions.len() >= max_partitions {
        return Err(ClientError::ProducerStateFull);
    }
    let authority = session.authority;
    let state = session
        .partitions
        .entry(key)
        .or_insert_with(|| ProducerPartitionState {
            next_sequence: ProducerSequence::ZERO,
            pending: None,
        });
    if state.pending.is_some() {
        return Err(ClientError::ProducerRequestInFlight {
            topic_id,
            partition,
        });
    }
    let record_count = u32::try_from(records.len())
        .map_err(|_| ClientError::Metadata("idempotent record count overflows u32"))?;
    let base_sequence = state.next_sequence;
    let body = ProduceRequestV1 {
        authority,
        partitions: vec![ProducePartitionRequestV1 {
            topic_id,
            partition,
            leader_epoch,
            base_sequence,
            records,
        }],
        tags: Vec::new(),
    }
    .encode_body()?;
    state.pending = Some(PendingProducerRequest {
        base_sequence,
        record_count,
        body,
        active: true,
    });
    Ok(())
}

fn retarget_pending_producer_request(
    session: &mut ProducerSessionState,
    topic_id: TopicId,
    partition: u32,
    leader_epoch: u64,
) -> Result<()> {
    let state = session
        .partitions
        .get_mut(&(topic_id, partition))
        .ok_or(ClientError::Metadata(
            "producer partition state disappeared during route refresh",
        ))?;
    let pending = state.pending.as_mut().ok_or(ClientError::Metadata(
        "producer pending request disappeared during route refresh",
    ))?;
    let mut request = ProduceRequestV1::decode_body(&pending.body)?;
    if request.authority != session.authority || request.partitions.len() != 1 {
        return Err(ClientError::InvalidResponse(
            "pending Produce V1 request identity changed before route refresh",
        ));
    }
    let request_partition = request
        .partitions
        .first_mut()
        .ok_or(ClientError::InvalidResponse(
            "pending Produce V1 request lost its partition",
        ))?;
    let record_count = u32::try_from(request_partition.records.len())
        .map_err(|_| ClientError::Metadata("pending idempotent record count overflows u32"))?;
    if request_partition.topic_id != topic_id
        || request_partition.partition != partition
        || request_partition.base_sequence != pending.base_sequence
        || record_count != pending.record_count
    {
        return Err(ClientError::InvalidResponse(
            "pending Produce V1 sequence or record identity changed before route refresh",
        ));
    }
    if request_partition.leader_epoch != leader_epoch {
        request_partition.leader_epoch = leader_epoch;
        pending.body = request.encode_body()?;
    }
    Ok(())
}

fn complete_producer_request(
    session: &mut ProducerSessionState,
    topic_id: TopicId,
    partition: u32,
    base_offset: u64,
    last_offset: u64,
) -> Result<IdempotentProduceOutcome> {
    let state = session
        .partitions
        .get_mut(&(topic_id, partition))
        .ok_or(ClientError::Metadata(
            "producer partition state disappeared",
        ))?;
    let pending = state.pending.as_ref().ok_or(ClientError::Metadata(
        "producer pending request disappeared",
    ))?;
    let next_sequence = pending
        .base_sequence
        .checked_next_after(pending.record_count)
        .ok_or(ClientError::Metadata("producer sequence overflow"))?;
    state.pending = None;
    state.next_sequence = next_sequence;
    Ok(IdempotentProduceOutcome {
        topic_id,
        partition,
        base_offset,
        last_offset,
    })
}

fn validate_groups_topic_metadata(topic: &MetadataTopicV0) -> Result<()> {
    if topic.topic_name != GROUPS_INTERNAL_TOPIC_NAME
        || topic.topic_error != PublicError::None as u16
        || topic.partition_count != P6_INTERNAL_TOPIC_PARTITIONS
        || topic.replication_factor != P6_INTERNAL_TOPIC_REPLICATION_FACTOR
        || topic.partitions.len()
            != usize::try_from(P6_INTERNAL_TOPIC_PARTITIONS)
                .map_err(|_| ClientError::Metadata("P6 partition-count conversion failed"))?
    {
        return Err(ClientError::Metadata(
            "P6 groups internal topic does not match frozen topology",
        ));
    }
    for (expected, partition) in topic.partitions.iter().enumerate() {
        if partition.partition
            != u32::try_from(expected).map_err(|_| {
                ClientError::Metadata("P6 partition index does not fit protocol partition id")
            })?
        {
            return Err(ClientError::Metadata(
                "P6 groups topic partitions are not canonical/contiguous",
            ));
        }
        validate_replica_set(partition)?;
    }
    Ok(())
}

fn validate_replica_set(partition: &MetadataPartitionV0) -> Result<()> {
    if partition.replica_broker_ids.len() != usize::from(P6_INTERNAL_TOPIC_REPLICATION_FACTOR) {
        return Err(ClientError::Metadata(
            "P6 internal shard does not have frozen RF3",
        ));
    }
    let replicas = partition
        .replica_broker_ids
        .iter()
        .copied()
        .collect::<BTreeSet<_>>();
    if replicas.len() != partition.replica_broker_ids.len()
        || replicas.contains(&astralog_protocol::UNKNOWN_BROKER_ID)
    {
        return Err(ClientError::Metadata(
            "P6 internal shard replica set is non-canonical",
        ));
    }
    Ok(())
}

fn select_group_coordinator(
    shard: u32,
    groups_topic_id: TopicId,
    replicas: &[u32],
    observations: &[MetadataPartitionV0],
) -> Result<GroupCoordinatorRoute> {
    let mut max_epoch = 0_u64;
    for observation in observations {
        if observation.leader_epoch != 0
            && observation.leader_epoch != astralog_protocol::UNKNOWN_EPOCH
        {
            max_epoch = max_epoch.max(observation.leader_epoch);
        }
    }
    if max_epoch == 0 {
        return Err(ClientError::Metadata(
            "no concrete P3 coordinator term was observed from RF3 replicas",
        ));
    }
    let leaders = observations
        .iter()
        .filter(|observation| observation.leader_epoch == max_epoch)
        .filter_map(|observation| {
            (observation.leader_broker_id != astralog_protocol::UNKNOWN_BROKER_ID)
                .then_some(observation.leader_broker_id)
        })
        .collect::<BTreeSet<_>>();
    if leaders.len() != 1 {
        return Err(ClientError::Metadata(
            "highest observed P3 coordinator term has no unique leader",
        ));
    }
    let coordinator_broker_id = *leaders
        .iter()
        .next()
        .ok_or(ClientError::Metadata("coordinator leader set is empty"))?;
    if !replicas.contains(&coordinator_broker_id) {
        return Err(ClientError::Metadata(
            "observed coordinator leader is not in committed RF3 assignment",
        ));
    }
    Ok(GroupCoordinatorRoute {
        shard,
        groups_topic_id,
        coordinator_broker_id,
        coordinator_epoch: max_epoch,
    })
}

#[derive(Debug)]
struct BrokerConnection {
    writer: Mutex<OwnedWriteHalf>,
    pending: PendingMap,
    permits: Arc<Semaphore>,
    next_correlation: AtomicU64,
    max_frame_len: usize,
    supported_apis: HashMap<u16, (u16, u16)>,
    client_id: String,
    throttle: ClientThrottle,
}

#[derive(Debug, Default)]
struct ClientThrottle {
    until: Mutex<Option<time::Instant>>,
}

impl ClientThrottle {
    async fn wait(&self, deadline: time::Instant) -> Result<()> {
        loop {
            let until = {
                let mut until = self.until.lock().await;
                if until.is_some_and(|value| value <= time::Instant::now()) {
                    *until = None;
                }
                *until
            };
            let Some(until) = until else {
                return Ok(());
            };
            time::timeout_at(deadline, time::sleep_until(until))
                .await
                .map_err(|_| ClientError::Timeout)?;
        }
    }

    async fn observe(&self, throttle_ms: u32) {
        if throttle_ms == 0 {
            return;
        }
        let candidate = time::Instant::now() + Duration::from_millis(u64::from(throttle_ms));
        let mut until = self.until.lock().await;
        if until.is_none_or(|current| candidate > current) {
            *until = Some(candidate);
        }
    }
}

impl BrokerConnection {
    async fn connect(endpoint: &str, config: &ClientConfig) -> Result<Self> {
        let mut stream = time::timeout(config.connect_timeout, TcpStream::connect(endpoint))
            .await
            .map_err(|_| ClientError::Timeout)??;
        stream.set_nodelay(true)?;

        let hello = ClientHello::v1().encode()?;
        time::timeout(config.connect_timeout, stream.write_all(&hello))
            .await
            .map_err(|_| ClientError::Timeout)??;
        let mut server_hello_bytes = [0_u8; 16];
        time::timeout(
            config.connect_timeout,
            stream.read_exact(&mut server_hello_bytes),
        )
        .await
        .map_err(|_| ClientError::Timeout)??;
        let server_hello = ServerHello::decode(&server_hello_bytes)?;
        if server_hello.status != HelloStatus::Ok
            || server_hello.selected_wire_version != WIRE_VERSION
        {
            return Err(ClientError::InvalidResponse(
                "broker did not negotiate Wire V1",
            ));
        }

        let api_request = ApiVersionsRequestV0 {
            client_software_name: "astralog-client".into(),
            client_software_version: env!("CARGO_PKG_VERSION").into(),
            tags: Vec::new(),
        };
        let api_frame = WireFrame {
            kind: FrameKind::Request,
            flags: 0,
            api_key: ApiKey::ApiVersions as u16,
            api_version: 0,
            error_code: 0,
            correlation_id: 1,
            timeout_ms: duration_ms_u32(config.request_timeout),
            throttle_ms: 0,
            header_tags: header_tags(&config.client_id),
            body: api_request.encode_body()?,
        };
        let encoded = encode_frame(&api_frame, config.max_frame_len)?;
        time::timeout(config.request_timeout, stream.write_all(&encoded))
            .await
            .map_err(|_| ClientError::Timeout)??;
        let api_response_frame =
            read_one_frame(&mut stream, config.max_frame_len, config.request_timeout).await?;
        validate_response_envelope(&api_response_frame, ApiKey::ApiVersions, 0, 1)?;
        let top_error = PublicError::try_from(api_response_frame.error_code)?;
        if top_error != PublicError::None {
            return Err(ClientError::Broker(top_error));
        }
        let api_response = ApiVersionsResponseV0::decode_body(&api_response_frame.body)?;
        if api_response.selected_wire_version != WIRE_VERSION {
            return Err(ClientError::InvalidResponse(
                "ApiVersions selected wire version disagrees with Hello",
            ));
        }
        let broker_max = usize::try_from(api_response.max_frame_len)
            .map_err(|_| ClientError::InvalidResponse("broker max frame length overflow"))?;
        if !(MIN_WIRE_FRAME_LEN..=MAX_WIRE_FRAME_LEN_V1).contains(&broker_max) {
            return Err(ClientError::InvalidResponse(
                "broker advertised invalid maximum frame length",
            ));
        }
        let max_frame_len = config.max_frame_len.min(broker_max);
        let supported_apis: HashMap<u16, (u16, u16)> = api_response
            .apis
            .iter()
            .map(|api| (api.api_key, (api.min_api_version, api.max_api_version)))
            .collect();

        let (reader, writer) = stream.into_split();
        let pending = Arc::new(Mutex::new(HashMap::new()));
        spawn_reader(reader, pending.clone(), max_frame_len);

        Ok(Self {
            writer: Mutex::new(writer),
            pending,
            permits: Arc::new(Semaphore::new(config.max_in_flight)),
            next_correlation: AtomicU64::new(2),
            max_frame_len,
            supported_apis,
            client_id: config.client_id.clone(),
            throttle: ClientThrottle::default(),
        })
    }

    async fn request_body<T: BodyCodec>(
        &self,
        api: ApiKey,
        api_version: u16,
        body: Vec<u8>,
        timeout: Duration,
    ) -> Result<T> {
        let frame = self.request(api, api_version, body, timeout).await?;
        T::decode_body(&frame.body).map_err(Into::into)
    }

    async fn request(
        &self,
        api: ApiKey,
        api_version: u16,
        body: Vec<u8>,
        timeout: Duration,
    ) -> Result<WireFrame> {
        let response = self.request_raw(api, api_version, body, timeout).await?;
        let error = PublicError::try_from(response.error_code)?;
        if error != PublicError::None {
            return Err(ClientError::Broker(error));
        }
        Ok(response)
    }

    async fn request_raw(
        &self,
        api: ApiKey,
        api_version: u16,
        body: Vec<u8>,
        timeout: Duration,
    ) -> Result<WireFrame> {
        self.ensure_api(api, api_version)?;
        let deadline = time::Instant::now() + timeout;
        self.throttle.wait(deadline).await?;
        let permit = time::timeout_at(deadline, self.permits.clone().acquire_owned())
            .await
            .map_err(|_| ClientError::Timeout)?
            .map_err(|_| ClientError::ConnectionClosed)?;
        let correlation_id = self.next_correlation_id();
        let frame = WireFrame {
            kind: FrameKind::Request,
            flags: 0,
            api_key: api as u16,
            api_version,
            error_code: 0,
            correlation_id,
            timeout_ms: duration_ms_u32(timeout),
            throttle_ms: 0,
            header_tags: header_tags(&self.client_id),
            body,
        };
        let encoded = encode_frame(&frame, self.max_frame_len)?;
        let (tx, rx) = oneshot::channel();
        {
            use std::collections::hash_map::Entry;
            let mut pending = self.pending.lock().await;
            if let Entry::Vacant(entry) = pending.entry(correlation_id) {
                entry.insert(tx);
            } else {
                return Err(ClientError::InvalidResponse(
                    "correlation id collided with an active request",
                ));
            }
        }

        let write_result = time::timeout_at(deadline, async {
            let mut writer = self.writer.lock().await;
            writer.write_all(&encoded).await
        })
        .await;
        match write_result {
            Ok(Ok(())) => {}
            Ok(Err(err)) => {
                self.pending.lock().await.remove(&correlation_id);
                return Err(ClientError::Io(err));
            }
            Err(_) => {
                self.pending.lock().await.remove(&correlation_id);
                return Err(ClientError::Timeout);
            }
        }

        let response = match time::timeout_at(deadline, rx).await {
            Ok(Ok(result)) => result?,
            Ok(Err(_)) => return Err(ClientError::ConnectionClosed),
            Err(_) => {
                self.pending.lock().await.remove(&correlation_id);
                return Err(ClientError::Timeout);
            }
        };
        drop(permit);
        validate_response_envelope(&response, api, api_version, correlation_id)?;
        self.throttle.observe(response.throttle_ms).await;
        Ok(response)
    }

    async fn send_no_response(
        &self,
        api: ApiKey,
        api_version: u16,
        body: Vec<u8>,
        timeout: Duration,
    ) -> Result<()> {
        self.ensure_api(api, api_version)?;
        let deadline = time::Instant::now() + timeout;
        self.throttle.wait(deadline).await?;
        let _permit = time::timeout_at(deadline, self.permits.clone().acquire_owned())
            .await
            .map_err(|_| ClientError::Timeout)?
            .map_err(|_| ClientError::ConnectionClosed)?;
        let frame = WireFrame {
            kind: FrameKind::Request,
            flags: NO_RESPONSE_FLAG,
            api_key: api as u16,
            api_version,
            error_code: 0,
            correlation_id: 0,
            timeout_ms: 0,
            throttle_ms: 0,
            header_tags: header_tags(&self.client_id),
            body,
        };
        let encoded = encode_frame(&frame, self.max_frame_len)?;
        time::timeout_at(deadline, async {
            let mut writer = self.writer.lock().await;
            writer.write_all(&encoded).await
        })
        .await
        .map_err(|_| ClientError::Timeout)??;
        Ok(())
    }

    async fn close(&self) {
        let _ = self.writer.lock().await.shutdown().await;
    }

    fn ensure_api(&self, api: ApiKey, api_version: u16) -> Result<()> {
        let Some((min, max)) = self.supported_apis.get(&(api as u16)).copied() else {
            return Err(ClientError::UnsupportedApi(api));
        };
        if !(min..=max).contains(&api_version) {
            return Err(ClientError::UnsupportedApi(api));
        }
        Ok(())
    }

    fn supports_api(&self, api: ApiKey, api_version: u16) -> bool {
        self.supported_apis
            .get(&(api as u16))
            .is_some_and(|(min, max)| (*min..=*max).contains(&api_version))
    }

    fn next_correlation_id(&self) -> u64 {
        loop {
            let id = self.next_correlation.fetch_add(1, Ordering::Relaxed);
            if id != 0 {
                return id;
            }
        }
    }
}

fn spawn_reader(mut reader: OwnedReadHalf, pending: PendingMap, max_frame_len: usize) {
    tokio::spawn(async move {
        let mut decoder = match FrameDecoder::new(max_frame_len, max_frame_len + 4) {
            Ok(decoder) => decoder,
            Err(_) => {
                fail_all_pending(&pending).await;
                return;
            }
        };
        let mut scratch = vec![0_u8; 64 * 1024];
        loop {
            let read = match reader.read(&mut scratch).await {
                Ok(0) | Err(_) => break,
                Ok(read) => read,
            };
            let frames = match decoder.push(&scratch[..read]) {
                Ok(frames) => frames,
                Err(_) => break,
            };
            for frame in frames {
                if frame.kind != FrameKind::Response {
                    fail_all_pending(&pending).await;
                    return;
                }
                let sender = { pending.lock().await.remove(&frame.correlation_id) };
                let Some(sender) = sender else {
                    fail_all_pending(&pending).await;
                    return;
                };
                let _ = sender.send(Ok(frame));
            }
        }
        fail_all_pending(&pending).await;
    });
}

async fn fail_all_pending(pending: &PendingMap) {
    let senders: Vec<_> = pending
        .lock()
        .await
        .drain()
        .map(|(_, sender)| sender)
        .collect();
    for sender in senders {
        let _ = sender.send(Err(ClientError::ConnectionClosed));
    }
}

async fn read_one_frame(
    stream: &mut TcpStream,
    max_frame_len: usize,
    timeout: Duration,
) -> Result<WireFrame> {
    let mut prefix = [0_u8; 4];
    time::timeout(timeout, stream.read_exact(&mut prefix))
        .await
        .map_err(|_| ClientError::Timeout)??;
    let declared = usize::try_from(u32::from_be_bytes(prefix))
        .map_err(|_| ClientError::InvalidResponse("frame length overflow"))?;
    if !(MIN_WIRE_FRAME_LEN..=max_frame_len.min(MAX_WIRE_FRAME_LEN_V1)).contains(&declared) {
        return Err(ClientError::InvalidResponse(
            "broker frame length outside negotiated bounds",
        ));
    }
    let mut bytes = Vec::with_capacity(declared + 4);
    bytes.extend_from_slice(&prefix);
    bytes.resize(declared + 4, 0);
    time::timeout(timeout, stream.read_exact(&mut bytes[4..]))
        .await
        .map_err(|_| ClientError::Timeout)??;
    astralog_protocol::decode_frame(&bytes, max_frame_len).map_err(Into::into)
}

fn validate_response_envelope(
    frame: &WireFrame,
    api: ApiKey,
    api_version: u16,
    correlation_id: u64,
) -> Result<()> {
    if frame.kind != FrameKind::Response
        || frame.api_key != api as u16
        || frame.api_version != api_version
        || frame.correlation_id != correlation_id
    {
        return Err(ClientError::InvalidResponse(
            "response header does not match request",
        ));
    }
    Ok(())
}

fn header_tags(client_id: &str) -> Vec<TaggedField> {
    if client_id.is_empty() {
        Vec::new()
    } else {
        vec![TaggedField {
            id: 1,
            value: client_id.as_bytes().to_vec(),
        }]
    }
}

fn duration_ms_u32(duration: Duration) -> u32 {
    duration.as_millis().min(u128::from(u32::MAX)) as u32
}

#[derive(Debug)]
struct ClientRng {
    state: u64,
}

impl ClientRng {
    fn new() -> Result<Self> {
        let mut bytes = [0_u8; 8];
        getrandom::fill(&mut bytes)?;
        let mut state = u64::from_le_bytes(bytes);
        if state == 0 {
            state = 0x9E37_79B9_7F4A_7C15;
        }
        Ok(Self { state })
    }
}

impl PartitionRng for ClientRng {
    fn next_u32(&mut self) -> u32 {
        self.state ^= self.state >> 12;
        self.state ^= self.state << 25;
        self.state ^= self.state >> 27;
        (self.state.wrapping_mul(0x2545_F491_4F6C_DD1D) >> 32) as u32
    }
}

pub fn retry_class(error: PublicError) -> Option<RetryClass> {
    error.retry_class()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[derive(Debug, Clone)]
    struct TestRng {
        values: Vec<u32>,
        index: usize,
    }

    impl PartitionRng for TestRng {
        fn next_u32(&mut self) -> u32 {
            let value = self.values[self.index % self.values.len()];
            self.index += 1;
            value
        }
    }

    fn observation(epoch: u64, leader: u32) -> MetadataPartitionV0 {
        MetadataPartitionV0 {
            partition: 9,
            partition_error: 0,
            leader_broker_id: leader,
            leader_epoch: epoch,
            replica_broker_ids: vec![1, 2, 3],
        }
    }

    #[test]
    fn client_config_enforces_wire_bounds() {
        let mut config = ClientConfig::new("127.0.0.1:9092");
        assert!(config.validate().is_ok());
        config.request_timeout = Duration::from_nanos(1);
        assert!(config.validate().is_err());
        config.request_timeout = Duration::from_millis(1);
        assert!(config.validate().is_ok());
        config.max_frame_len = 1;
        assert!(config.validate().is_err());
        config.max_frame_len = DEFAULT_MAX_FRAME_LEN;
        config.max_group_cache = 0;
        assert!(config.validate().is_err());
        config.max_group_cache = 16;
        config.max_group_in_flight = config.max_in_flight + 1;
        assert!(config.validate().is_err());
        config.max_group_in_flight = 8;
        config.max_group_retries = 9;
        assert!(config.validate().is_err());
        config.max_group_retries = DEFAULT_MAX_GROUP_RETRIES;
        config.max_producer_partitions = 0;
        assert!(config.validate().is_err());
        config.max_producer_partitions = 65_537;
        assert!(config.validate().is_err());
        config.max_producer_partitions = DEFAULT_MAX_PRODUCER_PARTITIONS;
        config.max_producer_retries = 9;
        assert!(config.validate().is_err());
    }

    #[test]
    fn p8_batch_config_is_finite_and_buffer_runtime_checks_retry_budget() {
        let mut config = ClientConfig::new("127.0.0.1:9092");
        assert!(config.validate().is_ok());

        config.batch_max_records = 0;
        assert!(config.validate().is_err());
        config.batch_max_records = DEFAULT_BATCH_MAX_RECORDS;
        config.batch_max_bytes = MAX_TOPIC_BATCH_UNCOMPRESSED_BYTES_V1 as usize + 1;
        assert!(config.validate().is_err());
        config.batch_max_bytes = DEFAULT_BATCH_MAX_BYTES;
        config.batch_linger = Duration::ZERO;
        assert!(config.validate().is_err());
        config.batch_linger = Duration::from_millis(DEFAULT_BATCH_LINGER_MS);
        config.accumulator_max_records = 0;
        assert!(config.validate().is_err());
        config.accumulator_max_records = DEFAULT_ACCUMULATOR_MAX_RECORDS;
        config.accumulator_max_bytes = 0;
        assert!(config.validate().is_err());
        config.accumulator_max_bytes = DEFAULT_ACCUMULATOR_MAX_BYTES;
        config.accumulator_max_partitions = 65_537;
        assert!(config.validate().is_err());
        config.accumulator_max_partitions = DEFAULT_ACCUMULATOR_MAX_PARTITIONS;
        config.request_timeout = Duration::from_secs(5);
        config.max_producer_retries = 2;
        config.producer_delivery_timeout = Duration::from_secs(5);
        assert!(config.validate().is_ok());
        assert!(validate_buffered_delivery_budget(&config).is_err());
        config.producer_delivery_timeout = Duration::from_secs(15);
        assert!(validate_buffered_delivery_budget(&config).is_ok());
    }

    #[test]
    fn p8_batching_preserves_keyed_mapping_and_sticky_rotates_only_after_close() {
        assert_eq!(partition_for_key(b"orders-123", 16).unwrap(), 7);

        let mut sticky = StickyPartitioner::new(TestRng {
            values: vec![1, 0, 2],
            index: 0,
        });
        let partitions = [0, 1, 2];
        let first = sticky.choose(&partitions).unwrap();
        assert_eq!(first, sticky.choose(&partitions).unwrap());

        let mut accumulator = ProducerAccumulator::default();
        let topic = TopicId::new([0x91; 16]);
        let record = WireRecordV0 {
            timestamp_ms: -1,
            key: None,
            value: Some(vec![b'x'; 16]),
            headers: Vec::new(),
        };
        let len = record.encoded_logical_len().unwrap();
        let AdmissionAttempt::Accepted(_) = accumulator
            .admit(
                &record,
                AdmissionSpec {
                    limits: AccumulatorLimits {
                        max_total_bytes: 1024,
                        max_total_records: 8,
                        max_partitions: 2,
                    },
                    topic_limits: TopicBatchLimits {
                        max_record_bytes: 1024,
                        max_batch_records: 8,
                        max_batch_bytes: 1024,
                    },
                    key: (topic, first),
                    mode: BatchMode::V0(AckMode::Quorum),
                    logical_bytes: len,
                    now_ms: 0,
                    linger_ms: 10,
                    sticky: true,
                },
            )
            .unwrap()
        else {
            panic!()
        };
        assert_eq!(first, sticky.choose(&partitions).unwrap());
        accumulator.request_close((topic, first));
        assert!(accumulator.begin_drive((topic, first)));
        let closed = accumulator.take_closed((topic, first)).unwrap();
        assert!(closed.has_sticky_record);
        sticky.on_batch_closed();
        assert_ne!(first, sticky.choose(&partitions).unwrap());
    }

    #[test]
    fn p8_route_invalidation_detects_only_changed_partition_authority() {
        let topic_id = TopicId::new([0x92; 16]);
        let metadata = |metadata_epoch: u64, leader: u32, leader_epoch: u64| MetadataResponseV0 {
            cluster_id: [9; 16],
            metadata_epoch,
            brokers: Vec::new(),
            topics: vec![MetadataTopicV0 {
                topic_error: 0,
                topic_name: "p8-route".into(),
                topic_id,
                partition_count: 2,
                replication_factor: 1,
                partitions: vec![
                    MetadataPartitionV0 {
                        partition: 0,
                        partition_error: 0,
                        leader_broker_id: leader,
                        leader_epoch,
                        replica_broker_ids: vec![leader],
                    },
                    MetadataPartitionV0 {
                        partition: 1,
                        partition_error: 0,
                        leader_broker_id: 3,
                        leader_epoch: 7,
                        replica_broker_ids: vec![3],
                    },
                ],
            }],
            tags: Vec::new(),
        };
        let old = metadata(10, 1, 4);
        let same = metadata(11, 1, 4);
        assert!(invalidated_partition_routes(&old, &same).is_empty());
        let changed = metadata(12, 2, 5);
        assert_eq!(
            invalidated_partition_routes(&old, &changed),
            vec![(topic_id, 0)]
        );
    }

    #[test]
    fn p8_buffering_consumes_no_sequence_until_closed_batch_reserves_send_range() {
        let authority = ProducerAuthority::new(
            astralog_common::ProducerId::new(88).unwrap(),
            astralog_common::ProducerEpoch::new(1).unwrap(),
        );
        let topic = TopicId::new([0x93; 16]);
        let record = WireRecordV0 {
            timestamp_ms: 100,
            key: Some(b"k".to_vec()),
            value: Some(b"v".to_vec()),
            headers: Vec::new(),
        };
        let len = record.encoded_logical_len().unwrap();
        let mut accumulator = ProducerAccumulator::default();
        let mut session = ProducerSessionState {
            authority,
            partitions: BTreeMap::new(),
        };
        accumulator
            .admit(
                &record,
                AdmissionSpec {
                    limits: AccumulatorLimits {
                        max_total_bytes: 1024,
                        max_total_records: 8,
                        max_partitions: 2,
                    },
                    topic_limits: TopicBatchLimits {
                        max_record_bytes: 1024,
                        max_batch_records: 8,
                        max_batch_bytes: 1024,
                    },
                    key: (topic, 0),
                    mode: BatchMode::V1,
                    logical_bytes: len,
                    now_ms: 0,
                    linger_ms: 10,
                    sticky: false,
                },
            )
            .unwrap();
        assert!(
            session.partitions.is_empty(),
            "buffer admission must not reserve P7 sequence"
        );

        accumulator.request_close((topic, 0));
        assert!(accumulator.begin_drive((topic, 0)));
        let closed = accumulator.take_closed((topic, 0)).unwrap();
        assert_eq!(closed.records, vec![record.clone()]);
        reserve_producer_request(&mut session, 2, topic, 0, 9, closed.records).unwrap();
        let pending = session.partitions[&(topic, 0)].pending.as_ref().unwrap();
        assert_eq!(pending.base_sequence, ProducerSequence::ZERO);
        assert_eq!(
            session.partitions[&(topic, 0)].next_sequence,
            ProducerSequence::ZERO
        );
    }

    #[test]
    fn p8_closed_multi_record_v1_retry_preserves_exact_logical_body_and_sequence_range() {
        let authority = ProducerAuthority::new(
            astralog_common::ProducerId::new(89).unwrap(),
            astralog_common::ProducerEpoch::new(2).unwrap(),
        );
        let topic = TopicId::new([0x94; 16]);
        let records = vec![
            WireRecordV0 {
                timestamp_ms: 10,
                key: Some(b"a".to_vec()),
                value: Some(b"one".to_vec()),
                headers: Vec::new(),
            },
            WireRecordV0 {
                timestamp_ms: 11,
                key: Some(b"b".to_vec()),
                value: Some(b"two".to_vec()),
                headers: Vec::new(),
            },
        ];
        let mut session = ProducerSessionState {
            authority,
            partitions: BTreeMap::new(),
        };
        reserve_producer_request(&mut session, 4, topic, 0, 7, records.clone()).unwrap();
        let original = session.partitions[&(topic, 0)]
            .pending
            .as_ref()
            .unwrap()
            .body
            .clone();
        let original_decoded = ProduceRequestV1::decode_body(&original).unwrap();
        assert_eq!(original_decoded.partitions[0].records, records);
        assert_eq!(
            original_decoded.partitions[0].base_sequence,
            ProducerSequence::ZERO
        );
        retarget_pending_producer_request(&mut session, topic, 0, 8).unwrap();
        let retried = ProduceRequestV1::decode_body(
            &session.partitions[&(topic, 0)]
                .pending
                .as_ref()
                .unwrap()
                .body,
        )
        .unwrap();
        assert_eq!(
            retried.partitions[0].records,
            original_decoded.partitions[0].records
        );
        assert_eq!(retried.partitions[0].base_sequence, ProducerSequence::ZERO);
        assert_eq!(retried.partitions[0].leader_epoch, 8);
    }

    #[test]
    fn p8_v1_batch_drive_waits_behind_one_unresolved_p7_range() {
        assert!(!batch_drive_allowed(Some(BatchMode::V1), true));
        assert!(batch_drive_allowed(Some(BatchMode::V1), false));
        assert!(batch_drive_allowed(
            Some(BatchMode::V0(AckMode::Quorum)),
            true
        ));
        assert!(batch_drive_allowed(None, true));
    }

    #[test]
    fn client_rng_is_non_constant() {
        let mut rng = ClientRng {
            state: 0x1234_5678_9ABC_DEF0,
        };
        assert_ne!(rng.next_u32(), rng.next_u32());
    }

    #[test]
    fn public_retry_class_is_exposed_without_reinterpretation() {
        assert_eq!(
            retry_class(PublicError::NotLeader),
            Some(RetryClass::Retryable)
        );
        assert_eq!(
            retry_class(PublicError::ProducerFenced),
            Some(RetryClass::Terminal)
        );
        assert_eq!(
            retry_class(PublicError::UnknownProducer),
            Some(RetryClass::Terminal)
        );
        assert_eq!(
            retry_class(PublicError::OutOfOrderSequence),
            Some(RetryClass::Terminal)
        );
        assert_eq!(
            retry_class(PublicError::RequestTimeout),
            Some(RetryClass::Retryable)
        );
        assert_eq!(
            retry_class(PublicError::BrokerBusy),
            Some(RetryClass::Retryable)
        );
    }

    #[tokio::test]
    async fn p8g_client_throttle_gate_delays_the_next_send_by_at_least_the_hint() {
        let throttle = ClientThrottle::default();
        let started = time::Instant::now();
        throttle.observe(20).await;
        throttle
            .wait(time::Instant::now() + Duration::from_secs(1))
            .await
            .unwrap();
        assert!(started.elapsed() >= Duration::from_millis(20));

        let immediate = time::Instant::now();
        throttle
            .wait(time::Instant::now() + Duration::from_secs(1))
            .await
            .unwrap();
        assert!(immediate.elapsed() < Duration::from_millis(20));
    }

    #[tokio::test]
    async fn p8g_client_throttle_extension_cannot_be_cleared_by_an_older_waiter() {
        let throttle = Arc::new(ClientThrottle::default());
        throttle.observe(20).await;
        let started = time::Instant::now();
        let extender = throttle.clone();
        let extension = tokio::spawn(async move {
            time::sleep(Duration::from_millis(5)).await;
            extender.observe(100).await;
        });
        throttle
            .wait(time::Instant::now() + Duration::from_secs(1))
            .await
            .unwrap();
        extension.await.unwrap();
        assert!(started.elapsed() >= Duration::from_millis(80));
    }

    #[test]
    fn p7_client_producer_state_is_bounded_pending_exact_and_success_ordered() {
        let authority = ProducerAuthority::new(
            astralog_common::ProducerId::new(7).unwrap(),
            astralog_common::ProducerEpoch::new(1).unwrap(),
        );
        let topic = TopicId::new([0x77; 16]);
        let other_topic = TopicId::new([0x78; 16]);
        let record = WireRecordV0 {
            timestamp_ms: 123,
            key: Some(b"k".to_vec()),
            value: Some(b"v".to_vec()),
            headers: Vec::new(),
        };
        let mut session = ProducerSessionState {
            authority,
            partitions: BTreeMap::new(),
        };

        reserve_producer_request(&mut session, 1, topic, 0, 9, vec![record.clone()]).unwrap();
        let state = &session.partitions[&(topic, 0)];
        assert_eq!(state.next_sequence, ProducerSequence::ZERO);
        let first_bytes = state.pending.as_ref().unwrap().body.clone();
        assert_eq!(
            state.pending.as_ref().unwrap().base_sequence,
            ProducerSequence::ZERO
        );
        assert!(state.pending.as_ref().unwrap().active);
        let decoded = ProduceRequestV1::decode_body(&first_bytes).unwrap();
        assert_eq!(decoded.authority, authority);
        assert_eq!(decoded.partitions[0].leader_epoch, 9);
        assert_eq!(decoded.partitions[0].base_sequence, ProducerSequence::ZERO);
        assert_eq!(decoded.partitions[0].records, vec![record.clone()]);
        assert_eq!(
            session.partitions[&(topic, 0)]
                .pending
                .as_ref()
                .unwrap()
                .body,
            first_bytes,
            "automatic retries must reuse the exact encoded Produce V1 body"
        );
        retarget_pending_producer_request(&mut session, topic, 0, 10).unwrap();
        let retargeted = ProduceRequestV1::decode_body(
            &session.partitions[&(topic, 0)]
                .pending
                .as_ref()
                .unwrap()
                .body,
        )
        .unwrap();
        assert_eq!(retargeted.authority, authority);
        assert_eq!(retargeted.partitions[0].leader_epoch, 10);
        assert_eq!(
            retargeted.partitions[0].base_sequence,
            ProducerSequence::ZERO
        );
        assert_eq!(retargeted.partitions[0].records, vec![record.clone()]);
        assert!(matches!(
            reserve_producer_request(&mut session, 1, topic, 0, 9, vec![record.clone()]),
            Err(ClientError::ProducerRequestInFlight { .. })
        ));
        assert!(matches!(
            reserve_producer_request(&mut session, 1, other_topic, 0, 9, vec![record.clone()]),
            Err(ClientError::ProducerStateFull)
        ));

        let outcome = complete_producer_request(&mut session, topic, 0, 11, 11).unwrap();
        assert_eq!(outcome.base_offset, 11);
        assert_eq!(outcome.last_offset, 11);
        assert_eq!(
            session.partitions[&(topic, 0)].next_sequence,
            ProducerSequence::new(1)
        );
        assert!(session.partitions[&(topic, 0)].pending.is_none());

        reserve_producer_request(&mut session, 1, topic, 0, 9, vec![record]).unwrap();
        let second = ProduceRequestV1::decode_body(
            &session.partitions[&(topic, 0)]
                .pending
                .as_ref()
                .unwrap()
                .body,
        )
        .unwrap();
        assert_eq!(second.partitions[0].base_sequence, ProducerSequence::new(1));
    }

    #[test]
    fn p7j_client_producer_partition_one_over_rejects_without_mutation() {
        let authority = ProducerAuthority::new(
            astralog_common::ProducerId::new(71).unwrap(),
            astralog_common::ProducerEpoch::new(1).unwrap(),
        );
        let record = WireRecordV0 {
            timestamp_ms: 123,
            key: Some(b"k".to_vec()),
            value: Some(b"v".to_vec()),
            headers: Vec::new(),
        };
        let mut session = ProducerSessionState {
            authority,
            partitions: BTreeMap::new(),
        };
        let topics = [
            TopicId::new([0x71; 16]),
            TopicId::new([0x72; 16]),
            TopicId::new([0x73; 16]),
        ];
        reserve_producer_request(&mut session, 2, topics[0], 0, 1, vec![record.clone()]).unwrap();
        reserve_producer_request(&mut session, 2, topics[1], 0, 1, vec![record.clone()]).unwrap();
        let before = session.partitions.clone();
        assert!(matches!(
            reserve_producer_request(&mut session, 2, topics[2], 0, 1, vec![record]),
            Err(ClientError::ProducerStateFull)
        ));
        assert_eq!(session.partitions, before);
        assert_eq!(session.partitions.len(), 2);
    }

    #[test]
    fn group_coordinator_selection_uses_highest_observed_p3_term() {
        let route = select_group_coordinator(
            9,
            TopicId::new([9; 16]),
            &[1, 2, 3],
            &[observation(7, 1), observation(8, 2), observation(8, 2)],
        )
        .unwrap();
        assert_eq!(route.shard, 9);
        assert_eq!(route.coordinator_broker_id, 2);
        assert_eq!(route.coordinator_epoch, 8);
        assert_eq!(route.hint().current_group_epoch, 0);
        assert_eq!(route.hint().current_member_epoch, 0);
    }

    #[test]
    fn group_coordinator_selection_rejects_conflicting_highest_term_leaders() {
        assert!(
            select_group_coordinator(
                9,
                TopicId::new([9; 16]),
                &[1, 2, 3],
                &[observation(8, 2), observation(8, 3)],
            )
            .is_err()
        );
    }

    #[test]
    fn group_coordinator_selection_does_not_fall_back_below_unknown_newer_term() {
        assert!(
            select_group_coordinator(
                9,
                TopicId::new([9; 16]),
                &[1, 2, 3],
                &[
                    observation(7, 1),
                    observation(8, astralog_protocol::UNKNOWN_BROKER_ID),
                ],
            )
            .is_err()
        );
    }

    #[test]
    fn group_route_cache_is_bounded_and_never_regresses_term() {
        let mut routes = BTreeMap::new();
        let g1 = GroupId::new("a").unwrap();
        let g2 = GroupId::new("b").unwrap();
        let base = GroupCoordinatorRoute {
            shard: 1,
            groups_topic_id: TopicId::new([1; 16]),
            coordinator_broker_id: 1,
            coordinator_epoch: 7,
        };
        insert_bounded_group_route(&mut routes, 1, g1.clone(), base);
        let stale = GroupCoordinatorRoute {
            coordinator_broker_id: 2,
            coordinator_epoch: 6,
            ..base
        };
        insert_bounded_group_route(&mut routes, 1, g1.clone(), stale);
        assert_eq!(routes[&g1], base);

        let newer = GroupCoordinatorRoute {
            coordinator_broker_id: 3,
            coordinator_epoch: 8,
            ..base
        };
        insert_bounded_group_route(&mut routes, 1, g2.clone(), newer);
        assert_eq!(routes.len(), 1);
        assert_eq!(routes[&g2], newer);
        assert!(!routes.contains_key(&g1));
    }

    #[test]
    fn member_authority_cache_rejects_regression_and_conflicting_same_epoch_identity() {
        let group_id = GroupId::new("member-cache").unwrap();
        let member1 = MemberId::new([1; 16]).unwrap();
        let member2 = MemberId::new([2; 16]).unwrap();
        let mut members = BTreeMap::new();
        let make = |member_id: MemberId, group_epoch: u64, member_epoch: u64| GroupMemberState {
            authority: MemberAuthorityV0 {
                group_id: group_id.clone(),
                member_id,
                group_epoch: GroupEpoch::new(group_epoch).unwrap(),
                member_epoch: MemberEpoch::new(member_epoch).unwrap(),
            },
            group_phase: 1,
            heartbeat_interval_ms: 1_000,
            active_assignments: Vec::new(),
            revocations: Vec::new(),
        };
        insert_bounded_member_state(&mut members, 2, group_id.clone(), make(member1, 4, 2))
            .unwrap();
        insert_bounded_member_state(&mut members, 2, group_id.clone(), make(member1, 3, 9))
            .unwrap();
        assert_eq!(members[&group_id].authority.group_epoch.get(), 4);
        assert_eq!(members[&group_id].authority.member_epoch.get(), 2);
        assert!(
            insert_bounded_member_state(&mut members, 2, group_id.clone(), make(member2, 4, 2))
                .is_err()
        );
    }

    #[test]
    fn group_retry_matrix_separates_routing_from_ambiguous_mutation_failures() {
        assert!(transport_retryable(&ClientError::Timeout));
        assert!(transport_retryable(&ClientError::ConnectionClosed));
        assert!(transport_retryable(&ClientError::Broker(
            PublicError::NotLeader
        )));
        assert!(transport_outcome_is_ambiguous(&ClientError::Timeout));
        assert!(transport_outcome_is_ambiguous(
            &ClientError::ConnectionClosed
        ));
        assert!(!transport_outcome_is_ambiguous(&ClientError::Broker(
            PublicError::NotLeader
        )));
        assert_eq!(
            PublicError::GroupEpochStale.retry_class(),
            Some(RetryClass::Retryable)
        );
        assert!(!matches!(
            PublicError::GroupEpochStale,
            PublicError::NotLeader | PublicError::BrokerBusy | PublicError::BrokerNotAvailable
        ));

        let retryable = [
            PublicError::NotLeader,
            PublicError::BrokerBusy,
            PublicError::BrokerNotAvailable,
            PublicError::RequestTimeout,
            PublicError::GroupEpochStale,
            PublicError::UnknownMember,
            PublicError::RebalanceInProgress,
        ];
        for error in retryable {
            assert_eq!(
                error.retry_class(),
                Some(RetryClass::Retryable),
                "P6 retryable error classification changed for {error:?}"
            );
        }
        let terminal = [
            PublicError::InvalidOffsetCommit,
            PublicError::UnsupportedFeature,
            PublicError::ClusterFeatureTooOld,
            PublicError::UnsupportedApiKey,
        ];
        for error in terminal {
            assert_eq!(
                error.retry_class(),
                Some(RetryClass::Terminal),
                "P6 terminal error classification changed for {error:?}"
            );
        }
    }

    #[test]
    fn coordinator_hint_cannot_regress_cached_route() {
        let route = GroupCoordinatorRoute {
            shard: 3,
            groups_topic_id: TopicId::new([3; 16]),
            coordinator_broker_id: 2,
            coordinator_epoch: 9,
        };
        let stale = CoordinatorHintV0 {
            coordinator_broker_id: 1,
            coordinator_epoch: 8,
            current_group_epoch: 10,
            current_member_epoch: 4,
        };
        assert!(route_from_hint(route, stale).is_none());
        let newer = CoordinatorHintV0 {
            coordinator_broker_id: 3,
            coordinator_epoch: 10,
            current_group_epoch: 10,
            current_member_epoch: 4,
        };
        assert_eq!(
            route_from_hint(route, newer).unwrap().coordinator_broker_id,
            3
        );
    }
}
