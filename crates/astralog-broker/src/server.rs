use std::{
    collections::{BTreeSet, HashSet},
    net::SocketAddr,
    path::PathBuf,
    sync::Arc,
    time::{Duration, Instant as StdInstant},
};

use astralog_common::{ProducerAuthority, ProducerSequence, StreamOffset, TopicId};
use astralog_groups::{GroupRecordV1, OffsetGroupFenceV1, OffsetOwnerFenceV1, OffsetStateMachine};
use astralog_protocol::{
    ApiKey, ClientHello, FrameDecoder, FrameKind, HelloStatus, MAX_WIRE_FRAME_LEN_V1,
    MIN_WIRE_FRAME_LEN, NO_RESPONSE_FLAG, PublicError, ServerHello, WIRE_VERSION, WireFrame,
    api::{
        AckMode, AlterTopicConfigRequestV0, ApiVersionsRequestV0, BodyCodec, CommitOffsetV0,
        CommitOffsetsRequestV0, CommitOffsetsResponseV0, CreateTopicRequestV0,
        CreateTopicRequestV1, CreateTopicRequestV2, DeleteTopicRequestV0, DescribeClusterRequestV0,
        DescribeTopicRequestV0, DescribeTopicRequestV1, DescribeTopicRequestV2,
        FetchOffsetsRequestV0, FetchOffsetsResponseV0, FetchRequestV0, GroupId, HeartbeatRequestV0,
        HeartbeatResponseV0, InitProducerRequestV0, InitProducerResponseV0, JoinSubscribeRequestV0,
        JoinSubscribeResponseV0, LeaveRequestV0, LeaveResponseV0, ListOffsetsRequestV0,
        MAX_PARTITIONS_PER_REQUEST, MAX_TOPICS_PER_REQUEST, MemberAuthorityV0, MetadataRequestV0,
        ProduceRequestV0, ProduceRequestV1, ReassignPartitionsRequestV0, RevokeAckRequestV0,
        TopicPartitionV0,
    },
    encode_frame, select_wire_version,
};
use astralog_storage::{CompactionLimits, DataRecord};
use axum::{Router, http::header, routing::get};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{
        TcpListener, TcpStream,
        tcp::{OwnedReadHalf, OwnedWriteHalf},
    },
    sync::{Mutex, Semaphore, oneshot},
    task::JoinSet,
    time::{self, Instant},
};

use crate::{
    BrokerError, DynamicControllerConfig, GroupCoordinatorObservation, P6InternalTopics,
    P7InternalProduceError, P7InternalProduceOutcome, PreferredLeaderOutcome,
    PreferredLeaderRequest, Result,
    controller::{DynamicBrokerControl, DynamicControlRuntime},
    group_coordinator::{
        GroupCommitOutcome, GroupCoordinatorError, GroupCoordinatorRuntime,
        GroupCoordinatorRuntimeConfig,
    },
    load_shaping::{
        DataAdmissionGate, DataAdmissionLimits, DataAdmissionPermit, QuotaGate, QuotaLimits,
    },
    monitoring::BrokerMetrics,
    offset_coordinator::{
        OffsetCommitOutcome, OffsetCoordinatorError, OffsetCoordinatorRuntime,
        OffsetCoordinatorRuntimeConfig, OffsetFenceCommitOutcome, OffsetFetchOutcome,
        OffsetShardObservation,
    },
    p6_service::{P6Failure, P6Service, PRIVATE_P6_OFFSET_API},
    replication::{ReplicationRuntimeHandle, StaticReplicationConfig, start_static_replication},
    state::{BrokerState, RuntimeConfig},
};

const MIN_OPERATIONAL_FRAME_LEN: usize = 1024;

#[derive(Clone)]
pub struct BrokerConfig {
    pub data_dir: PathBuf,
    pub bind_addr: SocketAddr,
    /// An optional, independent local HTTP endpoint exposing read-only metrics.
    /// None by default, so legacy test and production configurations do not
    /// open a new listener without an explicit operator opt-in.
    pub metrics_bind_addr: Option<SocketAddr>,
    pub advertised_host: String,
    pub advertised_port: u16,
    pub broker_id: u32,
    pub max_frame_len: usize,
    pub max_connections: usize,
    pub max_in_flight_per_connection: usize,
    pub max_pending_response_bytes: usize,
    pub max_data_in_flight: usize,
    pub max_data_in_flight_bytes: usize,
    pub data_memory_low_watermark_bytes: usize,
    pub data_memory_high_watermark_bytes: usize,
    pub anonymous_quota_bytes_per_second: u64,
    pub anonymous_quota_burst_bytes: usize,
    pub topic_quota_bytes_per_second: u64,
    pub topic_quota_burst_bytes: usize,
    pub overload_throttle_ms: u32,
    pub max_topics: usize,
    pub max_partitions_per_topic: u32,
    pub max_total_partitions: usize,
    pub frame_assembly_timeout: Duration,
    pub p6_maintenance_interval: Duration,
    pub group_coordinator: GroupCoordinatorRuntimeConfig,
    pub offset_coordinator: OffsetCoordinatorRuntimeConfig,
    pub static_replication: Option<StaticReplicationConfig>,
    pub dynamic_controller: Option<DynamicControllerConfig>,
}

impl BrokerConfig {
    pub fn new(data_dir: impl Into<PathBuf>, bind_addr: SocketAddr) -> Self {
        Self {
            data_dir: data_dir.into(),
            bind_addr,
            metrics_bind_addr: None,
            advertised_host: bind_addr.ip().to_string(),
            advertised_port: bind_addr.port(),
            broker_id: 1,
            max_frame_len: 16 * 1024 * 1024,
            max_connections: 1024,
            max_in_flight_per_connection: 32,
            max_pending_response_bytes: 544 * 1024 * 1024,
            max_data_in_flight: 4096,
            max_data_in_flight_bytes: 256 * 1024 * 1024,
            data_memory_low_watermark_bytes: 128 * 1024 * 1024,
            data_memory_high_watermark_bytes: 192 * 1024 * 1024,
            anonymous_quota_bytes_per_second: 128 * 1024 * 1024,
            anonymous_quota_burst_bytes: 256 * 1024 * 1024,
            topic_quota_bytes_per_second: 64 * 1024 * 1024,
            topic_quota_burst_bytes: 128 * 1024 * 1024,
            overload_throttle_ms: 10,
            max_topics: MAX_TOPICS_PER_REQUEST,
            max_partitions_per_topic: MAX_PARTITIONS_PER_REQUEST as u32,
            max_total_partitions: 16_384,
            frame_assembly_timeout: Duration::from_secs(10),
            p6_maintenance_interval: Duration::from_millis(100),
            group_coordinator: GroupCoordinatorRuntimeConfig::default(),
            offset_coordinator: OffsetCoordinatorRuntimeConfig::default(),
            static_replication: None,
            dynamic_controller: None,
        }
    }

    fn validate(&self) -> Result<()> {
        // The portfolio broker has no transport authentication or TLS. Fail
        // closed instead of accidentally serving unauthenticated clients on
        // an internet/LAN interface. Loopback still permits the RF3 demo.
        if !self.bind_addr.ip().is_loopback() {
            return Err(BrokerError::InvalidConfig(
                "unauthenticated AstraLog demo must bind only to loopback",
            ));
        }
        if let Some(addr) = self.metrics_bind_addr
            && (!addr.ip().is_loopback() || addr.port() == 0 || addr == self.bind_addr)
        {
            return Err(BrokerError::InvalidConfig(
                "metrics endpoint requires a distinct, nonzero loopback address",
            ));
        }
        if !(MIN_OPERATIONAL_FRAME_LEN..=MAX_WIRE_FRAME_LEN_V1).contains(&self.max_frame_len) {
            return Err(BrokerError::InvalidConfig(
                "max_frame_len must be at least 1024 bytes and within Wire V1 bounds",
            ));
        }
        if self.max_connections == 0 {
            return Err(BrokerError::InvalidConfig(
                "max_connections must be positive",
            ));
        }
        if self.max_in_flight_per_connection == 0 {
            return Err(BrokerError::InvalidConfig(
                "max_in_flight_per_connection must be positive",
            ));
        }
        DataAdmissionLimits {
            max_in_flight: self.max_data_in_flight,
            max_bytes: self.max_data_in_flight_bytes,
            low_watermark_bytes: self.data_memory_low_watermark_bytes,
            high_watermark_bytes: self.data_memory_high_watermark_bytes,
        }
        .validate()
        .map_err(BrokerError::InvalidConfig)?;
        if self.max_data_in_flight_bytes < self.max_frame_len {
            return Err(BrokerError::InvalidConfig(
                "global data byte budget must admit one maximum Wire V1 frame",
            ));
        }
        let anonymous_burst = u64::try_from(self.anonymous_quota_burst_bytes)
            .map_err(|_| BrokerError::InvalidConfig("anonymous quota burst exceeds u64"))?;
        let topic_burst = u64::try_from(self.topic_quota_burst_bytes)
            .map_err(|_| BrokerError::InvalidConfig("topic quota burst exceeds u64"))?;
        QuotaLimits {
            anonymous_rate_bytes_per_second: self.anonymous_quota_bytes_per_second,
            anonymous_burst_bytes: anonymous_burst,
            topic_rate_bytes_per_second: self.topic_quota_bytes_per_second,
            topic_burst_bytes: topic_burst,
            max_topic_buckets: self.max_topics,
            minimum_throttle_ms: self.overload_throttle_ms,
        }
        .validate()
        .map_err(BrokerError::InvalidConfig)?;
        if self.anonymous_quota_burst_bytes < self.max_frame_len
            || self.topic_quota_burst_bytes < self.max_frame_len
        {
            return Err(BrokerError::InvalidConfig(
                "quota burst capacities must admit one maximum Wire V1 frame",
            ));
        }
        if self.max_topics == 0 || self.max_topics > MAX_TOPICS_PER_REQUEST {
            return Err(BrokerError::InvalidConfig(
                "max_topics must be in 1..=Wire V1 topic bound",
            ));
        }
        if self.max_partitions_per_topic == 0
            || usize::try_from(self.max_partitions_per_topic)
                .map_err(|_| BrokerError::InvalidConfig("partition bound overflow"))?
                > MAX_PARTITIONS_PER_REQUEST
        {
            return Err(BrokerError::InvalidConfig(
                "max_partitions_per_topic must be in 1..=Wire V1 partition bound",
            ));
        }
        if self.max_total_partitions == 0 {
            return Err(BrokerError::InvalidConfig(
                "max_total_partitions must be positive",
            ));
        }
        if self.frame_assembly_timeout.is_zero() {
            return Err(BrokerError::InvalidConfig(
                "frame_assembly_timeout must be positive",
            ));
        }
        if !(Duration::from_millis(10)..=Duration::from_secs(5))
            .contains(&self.p6_maintenance_interval)
        {
            return Err(BrokerError::InvalidConfig(
                "p6_maintenance_interval must be in 10ms..=5s",
            ));
        }
        self.group_coordinator
            .validate()
            .map_err(|_| BrokerError::InvalidConfig("invalid P6 group coordinator bounds"))?;
        self.offset_coordinator
            .validate()
            .map_err(|_| BrokerError::InvalidConfig("invalid P6 offset coordinator bounds"))?;
        let max_encoded_response =
            self.max_frame_len
                .checked_add(4)
                .ok_or(BrokerError::InvalidConfig(
                    "encoded response size bound overflows",
                ))?;
        let worst_case_pending = max_encoded_response
            .checked_mul(self.max_in_flight_per_connection)
            .and_then(|value| value.checked_add(MIN_WIRE_FRAME_LEN + 4))
            .ok_or(BrokerError::InvalidConfig(
                "pending-response byte bound overflows",
            ))?;
        if worst_case_pending > self.max_pending_response_bytes {
            return Err(BrokerError::InvalidConfig(
                "max_pending_response_bytes cannot bound configured in-flight responses",
            ));
        }
        if self.advertised_host.len() > 255 {
            return Err(BrokerError::InvalidConfig(
                "advertised host exceeds Wire V1 maximum",
            ));
        }
        if self.static_replication.is_some() && self.dynamic_controller.is_some() {
            return Err(BrokerError::InvalidConfig(
                "static replication and dynamic controller modes are mutually exclusive",
            ));
        }
        Ok(())
    }
}

pub struct BrokerHandle {
    local_addr: SocketAddr,
    state: Arc<BrokerState>,
    shutdown: Option<oneshot::Sender<()>>,
    task: tokio::task::JoinHandle<Result<()>>,
    metrics_shutdown: Option<oneshot::Sender<()>>,
    metrics_task: Option<tokio::task::JoinHandle<()>>,
    p6_maintenance_shutdown: Option<oneshot::Sender<()>>,
    p6_maintenance_task: Option<tokio::task::JoinHandle<()>>,
    p6_discovery_shutdown: Option<oneshot::Sender<()>>,
    p6_discovery_task: Option<tokio::task::JoinHandle<()>>,
    replication: Option<ReplicationRuntimeHandle>,
    groups: Option<GroupCoordinatorRuntime>,
    offsets: Option<OffsetCoordinatorRuntime>,
    dynamic: Option<DynamicControlRuntime>,
}

impl BrokerHandle {
    pub fn local_addr(&self) -> SocketAddr {
        self.local_addr
    }

    /// Executes one bounded P9-D delete-retention attempt. This is an explicit
    /// maintenance hook only; no background P9 cleaner/scheduler is activated.
    pub async fn run_delete_retention_once(
        &self,
        topic_id: TopicId,
        partition: u32,
        expected_leader_epoch: u64,
        protected_log_start_ceiling: Option<StreamOffset>,
        now_ms: i64,
    ) -> std::result::Result<Option<crate::RetentionAdvanceResult>, PublicError> {
        self.state
            .run_delete_retention_once(
                topic_id,
                partition,
                expected_leader_epoch,
                protected_log_start_ceiling,
                now_ms,
            )
            .await
    }

    /// Executes one explicit bounded P9-E keyed-compaction attempt. No
    /// background cleaner is started by this surface.
    pub async fn run_keyed_compaction_once(
        &self,
        topic_id: TopicId,
        partition: u32,
        expected_leader_epoch: u64,
        limits: CompactionLimits,
    ) -> std::result::Result<Option<crate::CompactionRunResult>, PublicError> {
        self.state
            .run_keyed_compaction_once(topic_id, partition, expected_leader_epoch, limits)
            .await
    }

    pub async fn produce_idempotent_internal(
        &self,
        topic_id: TopicId,
        partition: u32,
        expected_leader_epoch: u64,
        authority: ProducerAuthority,
        base_sequence: ProducerSequence,
        records: Vec<DataRecord>,
    ) -> std::result::Result<P7InternalProduceOutcome, P7InternalProduceError> {
        let dynamic = self.dynamic.as_ref().ok_or(P7InternalProduceError {
            error: PublicError::UnsupportedFeature,
            leader_broker_id: astralog_protocol::UNKNOWN_BROKER_ID,
            leader_epoch: astralog_protocol::UNKNOWN_EPOCH,
        })?;
        dynamic
            .produce_idempotent_internal(
                topic_id,
                partition,
                expected_leader_epoch,
                authority,
                base_sequence,
                records,
            )
            .await
    }

    pub async fn observe_partition_internal(
        &self,
        topic_id: TopicId,
        partition: u32,
    ) -> Result<crate::P7InternalPartitionObservation> {
        let dynamic = self.dynamic.as_ref().ok_or(BrokerError::InvalidConfig(
            "P7 partition observation requires dynamic-controller mode",
        ))?;
        dynamic
            .observe_partition_internal(topic_id, partition)
            .await
    }

    /// Read-only P9-G internal test hook; no wire API or Raft mutation.
    #[doc(hidden)]
    pub async fn observe_p9g_membership_internal(
        &self,
        topic_id: TopicId,
        partition: u32,
    ) -> Result<astralog_replication::MembershipObservation> {
        let dynamic = self.dynamic.as_ref().ok_or(BrokerError::InvalidConfig(
            "P9-G membership observation requires dynamic-controller mode",
        ))?;
        dynamic
            .observe_p9g_membership_internal(topic_id, partition)
            .await
    }

    pub async fn persist_producer_snapshot_internal(
        &self,
        topic_id: TopicId,
        partition: u32,
    ) -> Result<()> {
        let dynamic = self.dynamic.as_ref().ok_or(BrokerError::InvalidConfig(
            "producer snapshot checkpoint requires dynamic-controller mode",
        ))?;
        dynamic
            .persist_producer_snapshot_internal(topic_id, partition)
            .await
    }

    pub async fn request_preferred_leader(
        &self,
        request: PreferredLeaderRequest,
    ) -> Result<PreferredLeaderOutcome> {
        let dynamic = self.dynamic.as_ref().ok_or(BrokerError::InvalidConfig(
            "preferred-leader hook requires dynamic-controller mode",
        ))?;
        dynamic.request_preferred_leader(request).await
    }

    pub async fn ensure_p6_internal_topics(&self) -> Result<P6InternalTopics> {
        let dynamic = self.dynamic.as_ref().ok_or(BrokerError::InvalidConfig(
            "P6 internal-topic bootstrap requires dynamic-controller mode",
        ))?;
        dynamic.ensure_p6_internal_topics().await
    }

    pub async fn observe_group_coordinator(
        &self,
        group_id: &GroupId,
    ) -> Result<GroupCoordinatorObservation> {
        let dynamic = self.dynamic.as_ref().ok_or(BrokerError::InvalidConfig(
            "group coordinator observation requires dynamic-controller mode",
        ))?;
        dynamic.observe_group_coordinator(group_id).await
    }

    pub async fn validate_group_coordinator_authority(
        &self,
        group_id: &GroupId,
        expected_coordinator_epoch: u64,
    ) -> Result<GroupCoordinatorObservation> {
        let dynamic = self.dynamic.as_ref().ok_or(BrokerError::InvalidConfig(
            "group coordinator authority requires dynamic-controller mode",
        ))?;
        dynamic
            .validate_group_coordinator_authority(group_id, expected_coordinator_epoch)
            .await
    }

    pub async fn recover_group_state(
        &self,
        group_id: GroupId,
        expected_coordinator_epoch: u64,
    ) -> std::result::Result<Option<GroupRecordV1>, GroupCoordinatorError> {
        let groups = self.groups.as_ref().ok_or(GroupCoordinatorError::NotReady(
            "group coordinator runtime requires dynamic-controller mode",
        ))?;
        groups
            .client()
            .recover(group_id, expected_coordinator_epoch)
            .await
    }

    pub async fn commit_group_record(
        &self,
        expected_coordinator_epoch: u64,
        record: GroupRecordV1,
    ) -> std::result::Result<GroupCommitOutcome, GroupCoordinatorError> {
        let groups = self.groups.as_ref().ok_or(GroupCoordinatorError::NotReady(
            "group coordinator runtime requires dynamic-controller mode",
        ))?;
        groups
            .client()
            .commit(expected_coordinator_epoch, record)
            .await
    }

    pub async fn recover_offset_state_internal(
        &self,
        group_id: GroupId,
        expected_offsets_epoch: u64,
    ) -> std::result::Result<OffsetStateMachine, OffsetCoordinatorError> {
        let offsets = self
            .offsets
            .as_ref()
            .ok_or(OffsetCoordinatorError::NotReady(
                "offset coordinator runtime requires dynamic-controller mode",
            ))?;
        offsets
            .client()
            .recover(group_id, expected_offsets_epoch)
            .await
    }

    pub async fn observe_offset_shard_internal(
        &self,
        group_id: GroupId,
    ) -> std::result::Result<OffsetShardObservation, OffsetCoordinatorError> {
        let offsets = self
            .offsets
            .as_ref()
            .ok_or(OffsetCoordinatorError::NotReady(
                "offset coordinator runtime requires dynamic-controller mode",
            ))?;
        offsets.client().observe(group_id).await
    }

    pub async fn commit_offset_group_fence_internal(
        &self,
        expected_offsets_epoch: u64,
        fence: OffsetGroupFenceV1,
    ) -> std::result::Result<OffsetFenceCommitOutcome, OffsetCoordinatorError> {
        let offsets = self
            .offsets
            .as_ref()
            .ok_or(OffsetCoordinatorError::NotReady(
                "offset coordinator runtime requires dynamic-controller mode",
            ))?;
        offsets
            .client()
            .commit_group_fence(expected_offsets_epoch, fence)
            .await
    }

    pub async fn commit_offset_owner_fence_internal(
        &self,
        expected_offsets_epoch: u64,
        fence: OffsetOwnerFenceV1,
    ) -> std::result::Result<OffsetFenceCommitOutcome, OffsetCoordinatorError> {
        let offsets = self
            .offsets
            .as_ref()
            .ok_or(OffsetCoordinatorError::NotReady(
                "offset coordinator runtime requires dynamic-controller mode",
            ))?;
        offsets
            .client()
            .commit_owner_fence(expected_offsets_epoch, fence)
            .await
    }

    pub async fn commit_offsets_internal(
        &self,
        expected_offsets_epoch: u64,
        group: GroupRecordV1,
        authority: MemberAuthorityV0,
        values: Vec<CommitOffsetV0>,
    ) -> std::result::Result<OffsetCommitOutcome, OffsetCoordinatorError> {
        let offsets = self
            .offsets
            .as_ref()
            .ok_or(OffsetCoordinatorError::NotReady(
                "offset coordinator runtime requires dynamic-controller mode",
            ))?;
        offsets
            .client()
            .commit_offsets(expected_offsets_epoch, group, authority, values)
            .await
    }

    pub async fn fetch_offsets_internal(
        &self,
        group_id: GroupId,
        expected_offsets_epoch: u64,
        partitions: Vec<TopicPartitionV0>,
    ) -> std::result::Result<OffsetFetchOutcome, OffsetCoordinatorError> {
        let offsets = self
            .offsets
            .as_ref()
            .ok_or(OffsetCoordinatorError::NotReady(
                "offset coordinator runtime requires dynamic-controller mode",
            ))?;
        offsets
            .client()
            .fetch_offsets(group_id, expected_offsets_epoch, partitions)
            .await
    }

    pub async fn shutdown(mut self) -> Result<()> {
        if let Some(shutdown) = self.metrics_shutdown.take() {
            let _ = shutdown.send(());
        }
        if let Some(task) = self.metrics_task.take() {
            let _ = task.await;
        }
        if let Some(shutdown) = self.shutdown.take() {
            let _ = shutdown.send(());
        }
        if let Some(shutdown) = self.p6_maintenance_shutdown.take() {
            let _ = shutdown.send(());
        }
        if let Some(shutdown) = self.p6_discovery_shutdown.take() {
            let _ = shutdown.send(());
        }
        let result = self
            .task
            .await
            .map_err(|err| BrokerError::TaskJoin(err.to_string()))?;
        if let Some(task) = self.p6_maintenance_task.take() {
            let _ = task.await;
        }
        if let Some(task) = self.p6_discovery_task.take() {
            // Discovery performs only read-only recovery and leader-local
            // session installation. Cancel outstanding scans before tearing
            // down replication rather than waiting for a large replay.
            task.abort();
            let _ = task.await;
        }
        if let Some(replication) = self.replication.take() {
            replication.shutdown().await;
        }
        if let Some(groups) = self.groups.take() {
            groups.shutdown().await;
        }
        if let Some(offsets) = self.offsets.take() {
            offsets.shutdown().await;
        }
        if let Some(dynamic) = self.dynamic.take() {
            dynamic.shutdown().await;
        }
        result
    }
}

pub async fn start(mut config: BrokerConfig) -> Result<BrokerHandle> {
    config.validate()?;
    let metrics = Arc::new(BrokerMetrics::new(config.broker_id));
    // Bind at startup so port conflicts fail before broker state is mutated.
    let metrics_listener = if let Some(addr) = config.metrics_bind_addr {
        Some(TcpListener::bind(addr).await?)
    } else {
        None
    };
    let data_admission = DataAdmissionGate::new(DataAdmissionLimits {
        max_in_flight: config.max_data_in_flight,
        max_bytes: config.max_data_in_flight_bytes,
        low_watermark_bytes: config.data_memory_low_watermark_bytes,
        high_watermark_bytes: config.data_memory_high_watermark_bytes,
    })
    .map_err(BrokerError::InvalidConfig)?;
    let quota = QuotaGate::new(QuotaLimits {
        anonymous_rate_bytes_per_second: config.anonymous_quota_bytes_per_second,
        anonymous_burst_bytes: u64::try_from(config.anonymous_quota_burst_bytes)
            .map_err(|_| BrokerError::InvalidConfig("anonymous quota burst exceeds u64"))?,
        topic_rate_bytes_per_second: config.topic_quota_bytes_per_second,
        topic_burst_bytes: u64::try_from(config.topic_quota_burst_bytes)
            .map_err(|_| BrokerError::InvalidConfig("topic quota burst exceeds u64"))?,
        max_topic_buckets: config.max_topics,
        minimum_throttle_ms: config.overload_throttle_ms,
    })
    .map_err(BrokerError::InvalidConfig)?;
    let listener = TcpListener::bind(config.bind_addr).await?;
    let local_addr = listener.local_addr()?;
    if config.advertised_port == 0 {
        config.advertised_port = local_addr.port();
    }
    if config.advertised_host == "0.0.0.0" || config.advertised_host == "::" {
        return Err(BrokerError::InvalidConfig(
            "wildcard bind address cannot be advertised",
        ));
    }

    if let Some(replication) = &config.static_replication {
        replication.validate_for_broker(
            config.broker_id,
            &config.advertised_host,
            config.advertised_port,
        )?;
    }

    let (dynamic_control, dynamic_runtime) = if let Some(dynamic) = &config.dynamic_controller {
        let control = DynamicBrokerControl::start(
            dynamic,
            config.data_dir.clone(),
            config.broker_id,
            &config.advertised_host,
            config.advertised_port,
        )
        .await?;
        let runtime =
            DynamicControlRuntime::start(control.clone(), dynamic.metadata_refresh_interval);
        (Some(control), Some(runtime))
    } else {
        (None, None)
    };

    let (replication_client, replication_handle) = if let Some(dynamic) = &dynamic_control {
        (Some(dynamic.replication_client().await), None)
    } else if let Some(replication) = config.static_replication.clone() {
        let (client, handle) = start_static_replication(
            replication,
            config.data_dir.clone(),
            config.broker_id,
            &config.advertised_host,
            config.advertised_port,
        )
        .await?;
        (Some(client), Some(handle))
    } else {
        (None, None)
    };
    let group_coordinator = if dynamic_control.is_some() {
        let client = replication_client
            .as_ref()
            .ok_or(BrokerError::InvalidConfig(
                "dynamic group coordinator requires replication client",
            ))?
            .clone();
        Some(
            GroupCoordinatorRuntime::start(client, config.group_coordinator).map_err(|_| {
                BrokerError::InvalidConfig("failed to start bounded P6 group coordinator")
            })?,
        )
    } else {
        None
    };
    let offset_coordinator = if dynamic_control.is_some() {
        let client = replication_client
            .as_ref()
            .ok_or(BrokerError::InvalidConfig(
                "dynamic offset coordinator requires replication client",
            ))?
            .clone();
        Some(
            OffsetCoordinatorRuntime::start(client, config.offset_coordinator).map_err(|_| {
                BrokerError::InvalidConfig("failed to start bounded P6 offset coordinator")
            })?,
        )
    } else {
        None
    };
    let p6_service = match (
        dynamic_control.as_ref(),
        replication_client.as_ref(),
        group_coordinator.as_ref(),
        offset_coordinator.as_ref(),
    ) {
        (Some(dynamic), Some(replication), Some(groups), Some(offsets)) => {
            Some(Arc::new(P6Service::new(
                config.broker_id,
                dynamic.clone(),
                replication.clone(),
                groups.client(),
                offsets.client(),
            )))
        }
        _ => None,
    };
    let (p6_maintenance_shutdown, p6_maintenance_task) = if let Some(service) = &p6_service {
        let service = service.clone();
        let interval = config.p6_maintenance_interval;
        let (shutdown_tx, mut shutdown_rx) = oneshot::channel();
        let task = tokio::spawn(async move {
            let mut ticker = time::interval(interval);
            ticker.set_missed_tick_behavior(time::MissedTickBehavior::Delay);
            ticker.tick().await;
            loop {
                tokio::select! {
                    _ = &mut shutdown_rx => break,
                    _ = ticker.tick() => service.maintenance_once().await,
                }
            }
        });
        (Some(shutdown_tx), Some(task))
    } else {
        (None, None)
    };
    let (p6_discovery_shutdown, p6_discovery_task) = if let Some(service) = &p6_service {
        let service = service.clone();
        let (shutdown_tx, mut shutdown_rx) = oneshot::channel();
        let task = tokio::spawn(async move {
            let mut ticker = time::interval(Duration::from_millis(1_000));
            ticker.set_missed_tick_behavior(time::MissedTickBehavior::Delay);
            loop {
                tokio::select! {
                    _ = &mut shutdown_rx => break,
                    _ = ticker.tick() => service.discover_leadership_once().await,
                }
            }
        });
        (Some(shutdown_tx), Some(task))
    } else {
        (None, None)
    };

    let runtime = RuntimeConfig {
        data_dir: config.data_dir.clone(),
        broker_id: config.broker_id,
        advertised_host: config.advertised_host.clone(),
        advertised_port: config.advertised_port,
        max_frame_len: config.max_frame_len,
        max_topics: config.max_topics,
        max_partitions_per_topic: config.max_partitions_per_topic,
        max_total_partitions: config.max_total_partitions,
    };
    let static_replication = config.static_replication.clone();
    let state_dynamic = dynamic_control.clone();
    let state = tokio::task::spawn_blocking(move || {
        BrokerState::open(
            runtime,
            static_replication,
            replication_client,
            state_dynamic,
        )
    })
    .await
    .map_err(|err| BrokerError::TaskJoin(err.to_string()))??;

    let handle_state = state.clone();
    let (shutdown_tx, mut shutdown_rx) = oneshot::channel();
    let (metrics_shutdown, metrics_task) = if let Some(metrics_listener) = metrics_listener {
        let (signal, shutdown) = oneshot::channel();
        let metrics_state = metrics.clone();
        let router = Router::new().route(
            "/metrics",
            get(move || {
                let metrics_state = metrics_state.clone();
                async move {
                    (
                        [(
                            header::CONTENT_TYPE,
                            "text/plain; version=0.0.4; charset=utf-8",
                        )],
                        metrics_state.render(),
                    )
                }
            }),
        );
        let task = tokio::spawn(async move {
            let _ = axum::serve(metrics_listener, router)
                .with_graceful_shutdown(async move {
                    let _ = shutdown.await;
                })
                .await;
        });
        (Some(signal), Some(task))
    } else {
        (None, None)
    };
    let connection_limit = Arc::new(Semaphore::new(config.max_connections));
    let metrics_for_connections = metrics.clone();
    let task = tokio::spawn(async move {
        let mut connections = JoinSet::new();
        loop {
            while connections.try_join_next().is_some() {}
            tokio::select! {
                _ = &mut shutdown_rx => break,
                accepted = listener.accept() => {
                    let (stream, _) = accepted?;
                    let Ok(permit) = connection_limit.clone().try_acquire_owned() else {
                        drop(stream);
                        continue;
                    };
                    let state = state.clone();
                    let p6 = p6_service.clone();
                    let data_admission = data_admission.clone();
                    let quota = quota.clone();
                    let connection_config = config.clone();
                    let metrics = metrics_for_connections.clone();
                    connections.spawn(async move {
                        let _permit = permit;
                        let _ = serve_connection(
                            stream,
                            state,
                            p6,
                            data_admission,
                            quota,
                            connection_config,
                            metrics,
                        )
                        .await;
                    });
                }
            }
        }
        connections.abort_all();
        while connections.join_next().await.is_some() {}
        Ok(())
    });

    Ok(BrokerHandle {
        local_addr,
        state: handle_state,
        shutdown: Some(shutdown_tx),
        task,
        metrics_shutdown,
        metrics_task,
        p6_maintenance_shutdown,
        p6_maintenance_task,
        p6_discovery_shutdown,
        p6_discovery_task,
        replication: replication_handle,
        groups: group_coordinator,
        offsets: offset_coordinator,
        dynamic: dynamic_runtime,
    })
}

async fn serve_connection(
    mut stream: TcpStream,
    state: Arc<BrokerState>,
    p6: Option<Arc<P6Service>>,
    data_admission: Arc<DataAdmissionGate>,
    quota: Arc<QuotaGate>,
    config: BrokerConfig,
    metrics: Arc<BrokerMetrics>,
) -> Result<()> {
    stream.set_nodelay(true)?;
    let mut hello_bytes = [0_u8; 16];
    let hello_read = time::timeout(
        config.frame_assembly_timeout,
        stream.read_exact(&mut hello_bytes),
    )
    .await;
    if hello_read.is_err() {
        return Ok(());
    }
    if let Err(err) = hello_read.expect("checked timeout") {
        if err.kind() != std::io::ErrorKind::UnexpectedEof {
            return Err(err.into());
        }
        return Ok(());
    }
    let client_hello = match ClientHello::decode(&hello_bytes) {
        Ok(hello) => hello,
        Err(_) => {
            let response = ServerHello {
                selected_wire_version: 0,
                status: HelloStatus::MalformedHello,
            };
            stream.write_all(&response.encode()).await?;
            return Ok(());
        }
    };
    let server_hello = select_wire_version(client_hello, WIRE_VERSION, WIRE_VERSION);
    stream.write_all(&server_hello.encode()).await?;
    if server_hello.status != HelloStatus::Ok {
        return Ok(());
    }

    let (reader, writer) = stream.into_split();
    serve_framed_connection(
        reader,
        writer,
        state,
        p6,
        data_admission,
        quota,
        ConnectionContext { config, metrics },
    )
    .await
}

struct ConnectionContext {
    config: BrokerConfig,
    metrics: Arc<BrokerMetrics>,
}

async fn serve_framed_connection(
    mut reader: OwnedReadHalf,
    writer: OwnedWriteHalf,
    state: Arc<BrokerState>,
    p6: Option<Arc<P6Service>>,
    data_admission: Arc<DataAdmissionGate>,
    quota: Arc<QuotaGate>,
    context: ConnectionContext,
) -> Result<()> {
    let ConnectionContext { config, metrics } = context;
    let writer = Arc::new(Mutex::new(writer));
    let active_correlations = Arc::new(Mutex::new(HashSet::<u64>::new()));
    let permits = Arc::new(Semaphore::new(config.max_in_flight_per_connection));
    let mut decoder = FrameDecoder::new(config.max_frame_len, config.max_frame_len + 4)?;
    let mut scratch = vec![0_u8; 64 * 1024];
    let mut api_negotiated = false;
    let mut assembly_started: Option<Instant> = None;
    let mut tasks = JoinSet::new();

    loop {
        while tasks.try_join_next().is_some() {}
        let read_future = reader.read(&mut scratch);
        let read = if let Some(started) = assembly_started {
            let deadline = started + config.frame_assembly_timeout;
            match time::timeout_at(deadline, read_future).await {
                Ok(result) => result?,
                Err(_) => break,
            }
        } else {
            read_future.await?
        };
        if read == 0 {
            break;
        }
        if decoder.is_idle() {
            assembly_started = Some(Instant::now());
        }
        let frames = match decoder.push(&scratch[..read]) {
            Ok(frames) => frames,
            Err(_) => break,
        };
        assembly_started = if decoder.is_idle() {
            None
        } else {
            assembly_started.or_else(|| Some(Instant::now()))
        };

        for frame in frames {
            if frame.kind != FrameKind::Request {
                abort_tasks(&mut tasks).await;
                return Ok(());
            }

            if !api_negotiated {
                if frame.api_key == PRIVATE_P6_OFFSET_API
                    && frame.api_version == 0
                    && frame.flags & NO_RESPONSE_FLAG == 0
                {
                    let Some(service) = p6.as_ref() else {
                        let response = error_response(&frame, PublicError::UnsupportedApiKey);
                        write_response(&writer, response, config.max_frame_len).await?;
                        continue;
                    };
                    let response = service.handle_private_offset(&frame).await;
                    write_response(&writer, response, config.max_frame_len).await?;
                    continue;
                }
                if frame.api_key != ApiKey::ApiVersions as u16
                    || frame.api_version != 0
                    || frame.flags & NO_RESPONSE_FLAG != 0
                {
                    let response = error_response(&frame, PublicError::InvalidRequest);
                    write_response(&writer, response, config.max_frame_len).await?;
                    abort_tasks(&mut tasks).await;
                    return Ok(());
                }
                let response = handle_api_versions(&state, p6.as_deref(), &frame);
                match response {
                    Ok(response) => {
                        write_response(&writer, response, config.max_frame_len).await?;
                        api_negotiated = true;
                    }
                    Err(_) => {
                        let response = error_response(&frame, PublicError::MalformedRequest);
                        write_response(&writer, response, config.max_frame_len).await?;
                        abort_tasks(&mut tasks).await;
                        return Ok(());
                    }
                }
                continue;
            }

            if frame.flags & NO_RESPONSE_FLAG != 0 {
                if !validate_no_response_produce(&frame) {
                    abort_tasks(&mut tasks).await;
                    return Ok(());
                }
                let Ok(permit) = permits.clone().try_acquire_owned() else {
                    continue;
                };
                let Some(data_bytes) = data_plane_request_bytes(&frame) else {
                    continue;
                };
                let Ok(data_permit) = data_admission.try_acquire(data_bytes) else {
                    continue;
                };
                let state = state.clone();
                tasks.spawn(async move {
                    let _permit = permit;
                    let _data_permit = data_permit;
                    let _ = process_no_response(state, frame).await;
                });
                continue;
            }

            {
                let mut active = active_correlations.lock().await;
                if !active.insert(frame.correlation_id) {
                    abort_tasks(&mut tasks).await;
                    return Ok(());
                }
            }

            let Ok(permit) = permits.clone().try_acquire_owned() else {
                active_correlations
                    .lock()
                    .await
                    .remove(&frame.correlation_id);
                let response = throttled_error_response(
                    &frame,
                    PublicError::BrokerBusy,
                    config.overload_throttle_ms,
                );
                metrics.observe_response(&frame, &response, Duration::ZERO);
                write_response(&writer, response, config.max_frame_len).await?;
                continue;
            };

            let data_permit = if let Some(data_bytes) = data_plane_request_bytes(&frame) {
                match data_admission.try_acquire(data_bytes) {
                    Ok(permit) => Some(permit),
                    Err(_) => {
                        active_correlations
                            .lock()
                            .await
                            .remove(&frame.correlation_id);
                        let response = throttled_error_response(
                            &frame,
                            PublicError::BrokerBusy,
                            config.overload_throttle_ms,
                        );
                        metrics.observe_response(&frame, &response, Duration::ZERO);
                        write_response(&writer, response, config.max_frame_len).await?;
                        continue;
                    }
                }
            } else {
                None
            };
            if let Some(scope) = quota_scope(&frame)
                && let Err(rejection) = quota.try_consume(&scope.topics, scope.bytes)
            {
                active_correlations
                    .lock()
                    .await
                    .remove(&frame.correlation_id);
                let response = throttled_error_response(
                    &frame,
                    PublicError::BrokerBusy,
                    rejection.throttle_ms,
                );
                metrics.observe_response(&frame, &response, Duration::ZERO);
                write_response(&writer, response, config.max_frame_len).await?;
                continue;
            }

            let writer = writer.clone();
            let state = state.clone();
            let p6 = p6.clone();
            let active = active_correlations.clone();
            let max_frame_len = config.max_frame_len;
            let metrics = metrics.clone();
            tasks.spawn(async move {
                let _permit = permit;
                let _data_permit: Option<DataAdmissionPermit> = data_permit;
                let correlation_id = frame.correlation_id;
                let timeout = Duration::from_millis(u64::from(frame.timeout_ms));
                let started = StdInstant::now();
                let response = match time::timeout(
                    timeout,
                    dispatch_request(state, p6, &frame, max_frame_len),
                )
                .await
                {
                    Ok(Ok(response)) => response,
                    Ok(Err(error)) => error_response(&frame, error),
                    Err(_) => error_response(&frame, PublicError::RequestTimeout),
                };
                metrics.observe_response(&frame, &response, started.elapsed());
                let _ = write_response(&writer, response, max_frame_len).await;
                active.lock().await.remove(&correlation_id);
            });
        }
    }

    abort_tasks(&mut tasks).await;
    Ok(())
}

fn data_plane_request_bytes(frame: &WireFrame) -> Option<usize> {
    let api = ApiKey::try_from(frame.api_key).ok()?;
    matches!(api, ApiKey::Produce | ApiKey::Fetch | ApiKey::ListOffsets)
        .then(|| request_accounted_bytes(frame))
}

fn request_accounted_bytes(frame: &WireFrame) -> usize {
    let header_tag_bytes = if frame.header_tags.is_empty() {
        0
    } else {
        frame.header_tags.iter().fold(2_usize, |total, tag| {
            total
                .saturating_add(2)
                .saturating_add(4)
                .saturating_add(tag.value.len())
        })
    };
    MIN_WIRE_FRAME_LEN
        .saturating_add(header_tag_bytes)
        .saturating_add(frame.body.len())
}

#[derive(Debug)]
struct QuotaScope {
    topics: Vec<TopicId>,
    bytes: usize,
}

fn quota_scope(frame: &WireFrame) -> Option<QuotaScope> {
    let api = ApiKey::try_from(frame.api_key).ok()?;
    let mut topics = BTreeSet::new();
    match (api, frame.api_version) {
        (ApiKey::CreateTopic, 0) => {
            CreateTopicRequestV0::decode_body(&frame.body).ok()?;
        }
        (ApiKey::CreateTopic, 1) => {
            CreateTopicRequestV1::decode_body(&frame.body).ok()?;
        }
        (ApiKey::CreateTopic, 2) => {
            CreateTopicRequestV2::decode_body(&frame.body).ok()?;
        }
        (ApiKey::Produce, 0) => {
            let request = ProduceRequestV0::decode_body(&frame.body).ok()?;
            if request.acks == AckMode::None {
                return None;
            }
            topics.extend(
                request
                    .partitions
                    .into_iter()
                    .map(|partition| partition.topic_id),
            );
        }
        (ApiKey::Produce, 1) => {
            let request = ProduceRequestV1::decode_body(&frame.body).ok()?;
            topics.extend(
                request
                    .partitions
                    .into_iter()
                    .map(|partition| partition.topic_id),
            );
        }
        (ApiKey::Fetch, 0) => {
            let request = FetchRequestV0::decode_body(&frame.body).ok()?;
            topics.extend(
                request
                    .partitions
                    .into_iter()
                    .map(|partition| partition.topic_id),
            );
        }
        (ApiKey::ListOffsets, 0) => {
            let request = ListOffsetsRequestV0::decode_body(&frame.body).ok()?;
            topics.extend(request.queries.into_iter().map(|query| query.topic_id));
        }
        _ => return None,
    }
    Some(QuotaScope {
        topics: topics.into_iter().collect(),
        bytes: request_accounted_bytes(frame),
    })
}

async fn abort_tasks(tasks: &mut JoinSet<()>) {
    tasks.abort_all();
    while tasks.join_next().await.is_some() {}
}

fn handle_api_versions(
    state: &BrokerState,
    p6: Option<&P6Service>,
    frame: &WireFrame,
) -> Result<WireFrame> {
    let _request = ApiVersionsRequestV0::decode_body(&frame.body)?;
    Ok(success_response(
        frame,
        state
            .api_versions_response(
                p6.is_some_and(P6Service::ready),
                state.p7_ready(),
                state.p8_ready(),
                state.p9_ready(),
            )
            .encode_body()?,
    ))
}

fn validate_no_response_produce(frame: &WireFrame) -> bool {
    if frame.api_key != ApiKey::Produce as u16 || frame.api_version != 0 {
        return false;
    }
    ProduceRequestV0::decode_body(&frame.body)
        .map(|request| request.acks == AckMode::None)
        .unwrap_or(false)
}

async fn process_no_response(state: Arc<BrokerState>, frame: WireFrame) -> Result<()> {
    let request = ProduceRequestV0::decode_body(&frame.body)?;
    if request.acks != AckMode::None {
        return Err(BrokerError::Protocol(
            astralog_protocol::ProtocolError::InvalidFrame("NO_RESPONSE Produce must use acks=0"),
        ));
    }
    let _ = state.produce(request).await;
    Ok(())
}

async fn dispatch_request(
    state: Arc<BrokerState>,
    p6: Option<Arc<P6Service>>,
    frame: &WireFrame,
    max_frame_len: usize,
) -> std::result::Result<WireFrame, PublicError> {
    let api = ApiKey::try_from(frame.api_key).map_err(|_| PublicError::UnsupportedApiKey)?;
    if !matches!(
        api,
        ApiKey::ApiVersions
            | ApiKey::DescribeCluster
            | ApiKey::Metadata
            | ApiKey::CreateTopic
            | ApiKey::DeleteTopic
            | ApiKey::DescribeTopic
            | ApiKey::AlterTopicConfig
            | ApiKey::ReassignPartitions
            | ApiKey::Produce
            | ApiKey::InitProducer
            | ApiKey::Fetch
            | ApiKey::ListOffsets
            | ApiKey::JoinSubscribe
            | ApiKey::Heartbeat
            | ApiKey::RevokeAck
            | ApiKey::Leave
            | ApiKey::CommitOffsets
            | ApiKey::FetchOffsets
    ) {
        return Err(PublicError::UnsupportedApiKey);
    }
    if matches!(api, ApiKey::DescribeCluster | ApiKey::ReassignPartitions) && !state.dynamic_mode()
    {
        return Err(PublicError::UnsupportedApiKey);
    }
    if matches!(
        api,
        ApiKey::JoinSubscribe
            | ApiKey::Heartbeat
            | ApiKey::RevokeAck
            | ApiKey::Leave
            | ApiKey::CommitOffsets
            | ApiKey::FetchOffsets
    ) && !p6.as_ref().is_some_and(|service| service.ready())
    {
        return Err(PublicError::UnsupportedApiKey);
    }
    let p7_ready = state.p7_ready();
    let p8_ready = state.p8_ready();
    let p9_ready = state.p9_ready();
    if api == ApiKey::InitProducer && !p7_ready {
        return Err(PublicError::UnsupportedApiKey);
    }
    if api == ApiKey::Produce && frame.api_version == 1 && !p7_ready {
        return Err(PublicError::UnsupportedApiVersion);
    }
    if matches!(api, ApiKey::CreateTopic | ApiKey::DescribeTopic)
        && frame.api_version == 1
        && !p8_ready
    {
        return Err(PublicError::UnsupportedApiVersion);
    }
    if matches!(api, ApiKey::CreateTopic | ApiKey::DescribeTopic)
        && frame.api_version == 2
        && !p9_ready
    {
        return Err(PublicError::UnsupportedApiVersion);
    }
    if api == ApiKey::AlterTopicConfig && !p9_ready {
        return Err(PublicError::UnsupportedApiKey);
    }
    if (api == ApiKey::Produce && frame.api_version > 1)
        || (matches!(api, ApiKey::CreateTopic | ApiKey::DescribeTopic) && frame.api_version > 2)
        || (!matches!(
            api,
            ApiKey::Produce
                | ApiKey::CreateTopic
                | ApiKey::DescribeTopic
                | ApiKey::AlterTopicConfig
        ) && frame.api_version != 0)
    {
        return Err(PublicError::UnsupportedApiVersion);
    }
    match api {
        ApiKey::ApiVersions => {
            let _ = ApiVersionsRequestV0::decode_body(&frame.body)
                .map_err(|_| PublicError::MalformedRequest)?;
            let body = state
                .api_versions_response(
                    p6.as_ref().is_some_and(|service| service.ready()),
                    p7_ready,
                    p8_ready,
                    p9_ready,
                )
                .encode_body()
                .map_err(|_| PublicError::UnknownServerError)?;
            bounded_success_response(frame, body, max_frame_len)
        }
        ApiKey::DescribeCluster => {
            let _request = DescribeClusterRequestV0::decode_body(&frame.body)
                .map_err(|_| PublicError::MalformedRequest)?;
            let response = state.describe_cluster().await?;
            bounded_success_response(
                frame,
                response
                    .encode_body()
                    .map_err(|_| PublicError::UnknownServerError)?,
                max_frame_len,
            )
        }
        ApiKey::Metadata => {
            let request = MetadataRequestV0::decode_body(&frame.body)
                .map_err(|_| PublicError::MalformedRequest)?;
            let response = state
                .metadata(&request)
                .await
                .map_err(|_| PublicError::StorageError)?;
            bounded_success_response(
                frame,
                response
                    .encode_body()
                    .map_err(|_| PublicError::UnknownServerError)?,
                max_frame_len,
            )
        }
        ApiKey::CreateTopic => match frame.api_version {
            0 => {
                let request = CreateTopicRequestV0::decode_body(&frame.body)
                    .map_err(|_| PublicError::MalformedRequest)?;
                let response = state
                    .create_topic(
                        request.topic_name,
                        request.partition_count,
                        request.replication_factor,
                    )
                    .await?;
                bounded_success_response(
                    frame,
                    response
                        .encode_body()
                        .map_err(|_| PublicError::UnknownServerError)?,
                    max_frame_len,
                )
            }
            1 => {
                let request = CreateTopicRequestV1::decode_body(&frame.body)
                    .map_err(|_| PublicError::MalformedRequest)?;
                let response = state
                    .create_topic_v1(
                        request.topic_name,
                        request.partition_count,
                        request.replication_factor,
                        request.performance,
                    )
                    .await?;
                bounded_success_response(
                    frame,
                    response
                        .encode_body()
                        .map_err(|_| PublicError::UnknownServerError)?,
                    max_frame_len,
                )
            }
            2 => {
                let request = CreateTopicRequestV2::decode_body(&frame.body)
                    .map_err(|_| PublicError::MalformedRequest)?;
                let response = state
                    .create_topic_v2(
                        request.topic_name,
                        request.partition_count,
                        request.replication_factor,
                        request.performance,
                        request.retention,
                    )
                    .await?;
                bounded_success_response(
                    frame,
                    response
                        .encode_body()
                        .map_err(|_| PublicError::UnknownServerError)?,
                    max_frame_len,
                )
            }
            _ => Err(PublicError::UnsupportedApiVersion),
        },
        ApiKey::DeleteTopic => {
            let request = DeleteTopicRequestV0::decode_body(&frame.body)
                .map_err(|_| PublicError::MalformedRequest)?;
            let response = state.delete_topic(request.topic_name).await?;
            bounded_success_response(
                frame,
                response
                    .encode_body()
                    .map_err(|_| PublicError::UnknownServerError)?,
                max_frame_len,
            )
        }
        ApiKey::DescribeTopic => match frame.api_version {
            0 => {
                let request = DescribeTopicRequestV0::decode_body(&frame.body)
                    .map_err(|_| PublicError::MalformedRequest)?;
                let response = state.describe_topic(&request.topic_name).await?;
                bounded_success_response(
                    frame,
                    response
                        .encode_body()
                        .map_err(|_| PublicError::UnknownServerError)?,
                    max_frame_len,
                )
            }
            1 => {
                let request = DescribeTopicRequestV1::decode_body(&frame.body)
                    .map_err(|_| PublicError::MalformedRequest)?;
                let response = state.describe_topic_v1(&request.topic_name).await?;
                bounded_success_response(
                    frame,
                    response
                        .encode_body()
                        .map_err(|_| PublicError::UnknownServerError)?,
                    max_frame_len,
                )
            }
            2 => {
                let request = DescribeTopicRequestV2::decode_body(&frame.body)
                    .map_err(|_| PublicError::MalformedRequest)?;
                let response = state.describe_topic_v2(&request.topic_name).await?;
                bounded_success_response(
                    frame,
                    response
                        .encode_body()
                        .map_err(|_| PublicError::UnknownServerError)?,
                    max_frame_len,
                )
            }
            _ => Err(PublicError::UnsupportedApiVersion),
        },
        ApiKey::AlterTopicConfig => {
            let request = AlterTopicConfigRequestV0::decode_body(&frame.body)
                .map_err(|_| PublicError::MalformedRequest)?;
            let response = state
                .alter_topic_config(&request.topic_name, request.retention)
                .await?;
            bounded_success_response(
                frame,
                response
                    .encode_body()
                    .map_err(|_| PublicError::UnknownServerError)?,
                max_frame_len,
            )
        }
        ApiKey::ReassignPartitions => {
            let request = ReassignPartitionsRequestV0::decode_body(&frame.body)
                .map_err(|_| PublicError::MalformedRequest)?;
            let response = state.reassign_partitions(request).await?;
            bounded_success_response(
                frame,
                response
                    .encode_body()
                    .map_err(|_| PublicError::UnknownServerError)?,
                max_frame_len,
            )
        }
        ApiKey::Produce => match frame.api_version {
            0 => {
                let request =
                    ProduceRequestV0::decode_body(&frame.body).map_err(|err| match err {
                        astralog_protocol::ProtocolError::InvalidFrame("invalid ack mode") => {
                            PublicError::InvalidAckMode
                        }
                        _ => PublicError::MalformedRequest,
                    })?;
                if request.acks == AckMode::None {
                    return Err(PublicError::InvalidAckMode);
                }
                let response = state.produce(request).await;
                bounded_success_response(
                    frame,
                    response
                        .encode_body()
                        .map_err(|_| PublicError::UnknownServerError)?,
                    max_frame_len,
                )
            }
            1 => {
                let request =
                    ProduceRequestV1::decode_body(&frame.body).map_err(|error| match error {
                        astralog_protocol::ProtocolError::InvalidFrame(
                            "idempotent Produce requires quorum acknowledgements",
                        ) => PublicError::InvalidAckMode,
                        _ => PublicError::MalformedRequest,
                    })?;
                let response = state.produce_idempotent(request).await;
                bounded_success_response(
                    frame,
                    response
                        .encode_body()
                        .map_err(|_| PublicError::UnknownServerError)?,
                    max_frame_len,
                )
            }
            _ => Err(PublicError::UnsupportedApiVersion),
        },
        ApiKey::InitProducer => {
            let request = InitProducerRequestV0::decode_body(&frame.body)
                .map_err(|_| PublicError::InvalidRequest)?;
            let authority = state.init_producer(request.expected_authority).await?;
            bounded_success_response(
                frame,
                InitProducerResponseV0 {
                    authority,
                    tags: Vec::new(),
                }
                .encode_body()
                .map_err(|_| PublicError::UnknownServerError)?,
                max_frame_len,
            )
        }
        ApiKey::Fetch => {
            let request = FetchRequestV0::decode_body(&frame.body).map_err(|err| match err {
                astralog_protocol::ProtocolError::InvalidFrame("invalid isolation level") => {
                    PublicError::InvalidIsolationLevel
                }
                _ => PublicError::MalformedRequest,
            })?;
            if request.max_wait_ms > frame.timeout_ms {
                return Err(PublicError::InvalidRequest);
            }
            let response = state.fetch_long_poll(request).await;
            bounded_success_response(
                frame,
                response
                    .encode_body()
                    .map_err(|_| PublicError::UnknownServerError)?,
                max_frame_len,
            )
        }
        ApiKey::ListOffsets => {
            let request = ListOffsetsRequestV0::decode_body(&frame.body)
                .map_err(|_| PublicError::MalformedRequest)?;
            let response = state.list_offsets(request).await;
            bounded_success_response(
                frame,
                response
                    .encode_body()
                    .map_err(|_| PublicError::UnknownServerError)?,
                max_frame_len,
            )
        }
        ApiKey::JoinSubscribe => {
            let request = JoinSubscribeRequestV0::decode_body(&frame.body)
                .map_err(|_| PublicError::MalformedRequest)?;
            let service = p6.as_ref().ok_or(PublicError::UnsupportedApiKey)?;
            match service.join(request).await {
                Ok(response) => bounded_success_response(
                    frame,
                    response
                        .encode_body()
                        .map_err(|_| PublicError::UnknownServerError)?,
                    max_frame_len,
                ),
                Err(failure) => p6_error_response(api, frame, failure, max_frame_len),
            }
        }
        ApiKey::Heartbeat => {
            let request = HeartbeatRequestV0::decode_body(&frame.body)
                .map_err(|_| PublicError::MalformedRequest)?;
            let service = p6.as_ref().ok_or(PublicError::UnsupportedApiKey)?;
            match service.heartbeat(request).await {
                Ok(response) => bounded_success_response(
                    frame,
                    response
                        .encode_body()
                        .map_err(|_| PublicError::UnknownServerError)?,
                    max_frame_len,
                ),
                Err(failure) => p6_error_response(api, frame, failure, max_frame_len),
            }
        }
        ApiKey::RevokeAck => {
            let request = RevokeAckRequestV0::decode_body(&frame.body)
                .map_err(|_| PublicError::MalformedRequest)?;
            let service = p6.as_ref().ok_or(PublicError::UnsupportedApiKey)?;
            match service.revoke_ack(request).await {
                Ok(response) => bounded_success_response(
                    frame,
                    response
                        .encode_body()
                        .map_err(|_| PublicError::UnknownServerError)?,
                    max_frame_len,
                ),
                Err(failure) => p6_error_response(api, frame, failure, max_frame_len),
            }
        }
        ApiKey::Leave => {
            let request = LeaveRequestV0::decode_body(&frame.body)
                .map_err(|_| PublicError::MalformedRequest)?;
            let service = p6.as_ref().ok_or(PublicError::UnsupportedApiKey)?;
            match service.leave(request).await {
                Ok(response) => bounded_success_response(
                    frame,
                    response
                        .encode_body()
                        .map_err(|_| PublicError::UnknownServerError)?,
                    max_frame_len,
                ),
                Err(failure) => p6_error_response(api, frame, failure, max_frame_len),
            }
        }
        ApiKey::CommitOffsets => {
            let request = CommitOffsetsRequestV0::decode_body(&frame.body)
                .map_err(|_| PublicError::MalformedRequest)?;
            let service = p6.as_ref().ok_or(PublicError::UnsupportedApiKey)?;
            match service.commit_offsets(request).await {
                Ok(response) => bounded_success_response(
                    frame,
                    response
                        .encode_body()
                        .map_err(|_| PublicError::UnknownServerError)?,
                    max_frame_len,
                ),
                Err(failure) => p6_error_response(api, frame, failure, max_frame_len),
            }
        }
        ApiKey::FetchOffsets => {
            let request = FetchOffsetsRequestV0::decode_body(&frame.body)
                .map_err(|_| PublicError::MalformedRequest)?;
            let service = p6.as_ref().ok_or(PublicError::UnsupportedApiKey)?;
            match service.fetch_offsets(request).await {
                Ok(response) => bounded_success_response(
                    frame,
                    response
                        .encode_body()
                        .map_err(|_| PublicError::UnknownServerError)?,
                    max_frame_len,
                ),
                Err(failure) => p6_error_response(api, frame, failure, max_frame_len),
            }
        }
        _ => Err(PublicError::UnsupportedApiKey),
    }
}

fn p6_error_response(
    api: ApiKey,
    request: &WireFrame,
    failure: P6Failure,
    max_frame_len: usize,
) -> std::result::Result<WireFrame, PublicError> {
    let body = match api {
        ApiKey::JoinSubscribe => JoinSubscribeResponseV0 {
            hint: failure.hint,
            member_id: [0; 16],
            group_phase: 0,
            heartbeat_interval_ms: 0,
            active_assignments: Vec::new(),
            revocations: Vec::new(),
            tags: Vec::new(),
        }
        .encode_body(),
        ApiKey::Heartbeat | ApiKey::RevokeAck => HeartbeatResponseV0 {
            hint: failure.hint,
            group_phase: 0,
            heartbeat_interval_ms: 0,
            active_assignments: Vec::new(),
            revocations: Vec::new(),
            tags: Vec::new(),
        }
        .encode_body(),
        ApiKey::Leave => LeaveResponseV0 {
            hint: failure.hint,
            tags: Vec::new(),
        }
        .encode_body(),
        ApiKey::CommitOffsets => CommitOffsetsResponseV0 {
            hint: failure.hint,
            offsets_commit_index: 0,
            tags: Vec::new(),
        }
        .encode_body(),
        ApiKey::FetchOffsets => FetchOffsetsResponseV0 {
            hint: failure.hint,
            results: Vec::new(),
            tags: Vec::new(),
        }
        .encode_body(),
        _ => return Err(failure.error),
    }
    .map_err(|_| PublicError::UnknownServerError)?;
    bounded_error_body_response(request, failure.error, body, max_frame_len)
}

fn bounded_error_body_response(
    request: &WireFrame,
    error: PublicError,
    body: Vec<u8>,
    max_frame_len: usize,
) -> std::result::Result<WireFrame, PublicError> {
    let frame_len = MIN_WIRE_FRAME_LEN
        .checked_add(body.len())
        .ok_or(PublicError::FrameTooLarge)?;
    if frame_len > max_frame_len {
        return Err(PublicError::FrameTooLarge);
    }
    let mut response = success_response(request, body);
    response.error_code = error as u16;
    Ok(response)
}

fn success_response(request: &WireFrame, body: Vec<u8>) -> WireFrame {
    WireFrame {
        kind: FrameKind::Response,
        flags: 0,
        api_key: request.api_key,
        api_version: request.api_version,
        error_code: PublicError::None as u16,
        correlation_id: request.correlation_id,
        timeout_ms: 0,
        throttle_ms: 0,
        header_tags: Vec::new(),
        body,
    }
}

fn bounded_success_response(
    request: &WireFrame,
    body: Vec<u8>,
    max_frame_len: usize,
) -> std::result::Result<WireFrame, PublicError> {
    let frame_len = MIN_WIRE_FRAME_LEN
        .checked_add(body.len())
        .ok_or(PublicError::FrameTooLarge)?;
    if frame_len > max_frame_len {
        return Err(PublicError::FrameTooLarge);
    }
    Ok(success_response(request, body))
}

fn error_response(request: &WireFrame, error: PublicError) -> WireFrame {
    WireFrame {
        kind: FrameKind::Response,
        flags: 0,
        api_key: request.api_key,
        api_version: request.api_version,
        error_code: error as u16,
        correlation_id: request.correlation_id,
        timeout_ms: 0,
        throttle_ms: 0,
        header_tags: Vec::new(),
        body: Vec::new(),
    }
}

fn throttled_error_response(
    request: &WireFrame,
    error: PublicError,
    throttle_ms: u32,
) -> WireFrame {
    let mut response = error_response(request, error);
    response.throttle_ms = throttle_ms.max(1);
    response
}

async fn write_response(
    writer: &Arc<Mutex<OwnedWriteHalf>>,
    response: WireFrame,
    max_frame_len: usize,
) -> Result<()> {
    let bytes = encode_frame(&response, max_frame_len)?;
    let mut writer = writer.lock().await;
    writer.write_all(&bytes).await?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use astralog_client::{AstraClient, ClientConfig};
    use std::time::{SystemTime, UNIX_EPOCH};

    #[test]
    fn unauthenticated_client_listener_rejects_non_loopback() {
        let mut config = BrokerConfig::new(std::env::temp_dir(), "127.0.0.1:0".parse().unwrap());
        assert!(config.validate().is_ok());
        config.bind_addr = "0.0.0.0:9092".parse().unwrap();
        assert!(
            config.validate().is_err(),
            "wildcard listener exposes unauthenticated API"
        );
        config.bind_addr = "192.0.2.10:9092".parse().unwrap();
        assert!(
            config.validate().is_err(),
            "LAN listener exposes unauthenticated API"
        );
    }

    async fn p8g_read_frame(stream: &mut TcpStream, max_frame_len: usize) -> WireFrame {
        let mut prefix = [0_u8; 4];
        stream.read_exact(&mut prefix).await.unwrap();
        let declared = usize::try_from(u32::from_be_bytes(prefix)).unwrap();
        let mut encoded = vec![0_u8; declared + 4];
        encoded[..4].copy_from_slice(&prefix);
        stream.read_exact(&mut encoded[4..]).await.unwrap();
        astralog_protocol::decode_frame(&encoded, max_frame_len).unwrap()
    }

    async fn p8g_write_request(
        stream: &mut TcpStream,
        frame: WireFrame,
        max_frame_len: usize,
    ) -> WireFrame {
        let encoded = encode_frame(&frame, max_frame_len).unwrap();
        stream.write_all(&encoded).await.unwrap();
        p8g_read_frame(stream, max_frame_len).await
    }

    async fn p8g_ready_connection(addr: SocketAddr, max_frame_len: usize) -> TcpStream {
        let mut stream = TcpStream::connect(addr).await.unwrap();
        stream
            .write_all(&ClientHello::v1().encode().unwrap())
            .await
            .unwrap();
        let mut hello = [0_u8; 16];
        stream.read_exact(&mut hello).await.unwrap();
        assert_eq!(ServerHello::decode(&hello).unwrap().status, HelloStatus::Ok);
        let response = p8g_write_request(
            &mut stream,
            WireFrame {
                kind: FrameKind::Request,
                flags: 0,
                api_key: ApiKey::ApiVersions as u16,
                api_version: 0,
                error_code: 0,
                correlation_id: 1,
                timeout_ms: 1000,
                throttle_ms: 0,
                header_tags: Vec::new(),
                body: ApiVersionsRequestV0 {
                    client_software_name: "p8g-pressure".into(),
                    client_software_version: "1".into(),
                    tags: Vec::new(),
                }
                .encode_body()
                .unwrap(),
            },
            max_frame_len,
        )
        .await;
        assert_eq!(response.error_code, PublicError::None as u16);
        stream
    }

    #[test]
    fn pending_response_budget_must_bound_all_in_flight_frames() {
        let addr: SocketAddr = "127.0.0.1:0".parse().unwrap();
        let mut config = BrokerConfig::new(std::env::temp_dir(), addr);
        config.max_frame_len = 1024;
        config.max_in_flight_per_connection = 4;
        config.max_pending_response_bytes = 4159;
        assert!(config.validate().is_err());
        config.max_pending_response_bytes = 4160;
        assert!(config.validate().is_ok());
    }

    #[test]
    fn p8g_broker_data_watermarks_are_finite_ordered_and_fit_one_frame() {
        let addr: SocketAddr = "127.0.0.1:0".parse().unwrap();
        let mut config = BrokerConfig::new(std::env::temp_dir(), addr);
        assert!(config.validate().is_ok());

        config.data_memory_low_watermark_bytes = 0;
        assert!(config.validate().is_err());
        config.data_memory_low_watermark_bytes = 100;
        config.data_memory_high_watermark_bytes = 100;
        assert!(config.validate().is_err());
        config.data_memory_high_watermark_bytes = 200;
        config.max_data_in_flight_bytes = 150;
        assert!(config.validate().is_err());
        config.max_data_in_flight_bytes = config.max_frame_len.saturating_sub(1);
        config.data_memory_high_watermark_bytes = config.max_data_in_flight_bytes;
        assert!(config.validate().is_err());
    }

    #[test]
    fn p8g_data_admission_classification_reserves_global_capacity_for_control() {
        let request = WireFrame {
            kind: FrameKind::Request,
            flags: 0,
            api_key: ApiKey::Produce as u16,
            api_version: 0,
            error_code: 0,
            correlation_id: 1,
            timeout_ms: 1000,
            throttle_ms: 0,
            header_tags: Vec::new(),
            body: vec![0; 32],
        };
        assert_eq!(
            data_plane_request_bytes(&request),
            Some(MIN_WIRE_FRAME_LEN + 32)
        );
        for api in [ApiKey::Fetch, ApiKey::ListOffsets] {
            assert!(
                data_plane_request_bytes(&WireFrame {
                    api_key: api as u16,
                    ..request.clone()
                })
                .is_some()
            );
        }
        for api in [
            ApiKey::ApiVersions,
            ApiKey::Metadata,
            ApiKey::CreateTopic,
            ApiKey::Heartbeat,
        ] {
            assert!(
                data_plane_request_bytes(&WireFrame {
                    api_key: api as u16,
                    ..request.clone()
                })
                .is_none()
            );
        }
    }

    #[test]
    fn p8g_quota_scope_is_pre_mutation_and_response_throttle_uses_wire_header() {
        let topic = TopicId::new([0xA8; 16]);
        let produce = ProduceRequestV0 {
            acks: AckMode::Quorum,
            partitions: vec![astralog_protocol::api::ProducePartitionRequestV0 {
                topic_id: topic,
                partition: 0,
                leader_epoch: 1,
                records: vec![astralog_protocol::api::WireRecordV0 {
                    timestamp_ms: 1,
                    key: None,
                    value: Some(b"quota".to_vec()),
                    headers: Vec::new(),
                }],
            }],
            tags: Vec::new(),
        };
        let request = WireFrame {
            kind: FrameKind::Request,
            flags: 0,
            api_key: ApiKey::Produce as u16,
            api_version: 0,
            error_code: 0,
            correlation_id: 7,
            timeout_ms: 1000,
            throttle_ms: 0,
            header_tags: Vec::new(),
            body: produce.encode_body().unwrap(),
        };
        let scope = quota_scope(&request).unwrap();
        assert_eq!(scope.topics, vec![topic]);
        let throttled = throttled_error_response(&request, PublicError::BrokerBusy, 17);
        assert_eq!(throttled.error_code, PublicError::BrokerBusy as u16);
        assert_eq!(throttled.throttle_ms, 17);
        assert_eq!(request.throttle_ms, 0);

        let no_response = WireFrame {
            body: ProduceRequestV0 {
                acks: AckMode::None,
                ..produce
            }
            .encode_body()
            .unwrap(),
            ..request
        };
        assert!(quota_scope(&no_response).is_none());
    }

    #[tokio::test]
    async fn p8g_throttled_create_returns_hint_before_mutation() {
        let nonce = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let directory = std::env::temp_dir().join(format!(
            "astralog-p8g-quota-create-{}-{nonce}",
            std::process::id()
        ));
        let mut config = BrokerConfig::new(directory.clone(), "127.0.0.1:0".parse().unwrap());
        config.max_frame_len = 1024;
        config.anonymous_quota_bytes_per_second = 1;
        config.anonymous_quota_burst_bytes = 1024;
        config.topic_quota_bytes_per_second = 1;
        config.topic_quota_burst_bytes = 1024;
        config.overload_throttle_ms = 25;
        let broker = start(config).await.unwrap();

        let mut stream = TcpStream::connect(broker.local_addr()).await.unwrap();
        stream
            .write_all(&ClientHello::v1().encode().unwrap())
            .await
            .unwrap();
        let mut hello = [0_u8; 16];
        stream.read_exact(&mut hello).await.unwrap();
        assert_eq!(ServerHello::decode(&hello).unwrap().status, HelloStatus::Ok);

        let api_versions = WireFrame {
            kind: FrameKind::Request,
            flags: 0,
            api_key: ApiKey::ApiVersions as u16,
            api_version: 0,
            error_code: 0,
            correlation_id: 1,
            timeout_ms: 1000,
            throttle_ms: 0,
            header_tags: Vec::new(),
            body: ApiVersionsRequestV0 {
                client_software_name: "p8g-test".into(),
                client_software_version: "1".into(),
                tags: Vec::new(),
            }
            .encode_body()
            .unwrap(),
        };
        let api_response = p8g_write_request(&mut stream, api_versions, 1024).await;
        assert_eq!(api_response.error_code, PublicError::None as u16);

        let mut throttled_name = None;
        for ordinal in 0..12_u64 {
            let name = format!("p8g-{ordinal:02}-{}", "x".repeat(120));
            let request = WireFrame {
                kind: FrameKind::Request,
                flags: 0,
                api_key: ApiKey::CreateTopic as u16,
                api_version: 0,
                error_code: 0,
                correlation_id: ordinal + 2,
                timeout_ms: 1000,
                throttle_ms: 0,
                header_tags: Vec::new(),
                body: CreateTopicRequestV0 {
                    topic_name: name.clone(),
                    partition_count: 1,
                    replication_factor: 1,
                    tags: Vec::new(),
                }
                .encode_body()
                .unwrap(),
            };
            let response = p8g_write_request(&mut stream, request, 1024).await;
            if response.error_code == PublicError::BrokerBusy as u16 {
                assert!(response.throttle_ms >= 25);
                throttled_name = Some(name);
                break;
            }
            assert_eq!(response.error_code, PublicError::None as u16);
            assert_eq!(response.throttle_ms, 0);
        }
        let throttled_name =
            throttled_name.expect("quota must throttle before the bounded loop ends");

        let mut client_config = ClientConfig::new(broker.local_addr().to_string());
        client_config.max_frame_len = 1024;
        client_config.request_timeout = Duration::from_secs(2);
        let client = AstraClient::connect(client_config).await.unwrap();
        let metadata = client.metadata().await.unwrap();
        assert!(
            metadata
                .topics
                .iter()
                .all(|topic| topic.topic_name != throttled_name),
            "quota rejection must occur before CreateTopic mutation"
        );
        client.close().await;
        drop(stream);
        broker.shutdown().await.unwrap();
        let _ = std::fs::remove_dir_all(directory);
    }

    #[tokio::test]
    async fn p8g_throttled_produce_returns_hint_before_mutation() {
        use astralog_protocol::api::{
            CreateTopicResponseV0, FetchPartitionRequestV0, FetchResponseV0, IsolationLevel,
            ProduceResponseV0, WireRecordV0,
        };

        let nonce = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let directory = std::env::temp_dir().join(format!(
            "astralog-p8g-quota-produce-{}-{nonce}",
            std::process::id()
        ));
        let mut config = BrokerConfig::new(directory.clone(), "127.0.0.1:0".parse().unwrap());
        config.max_frame_len = 1024;
        config.anonymous_quota_bytes_per_second = 10_000;
        config.anonymous_quota_burst_bytes = 1024;
        config.topic_quota_bytes_per_second = 10_000;
        config.topic_quota_burst_bytes = 1024;
        config.overload_throttle_ms = 25;
        let broker = start(config).await.unwrap();

        let mut stream = p8g_ready_connection(broker.local_addr(), 1024).await;
        let created = p8g_write_request(
            &mut stream,
            WireFrame {
                kind: FrameKind::Request,
                flags: 0,
                api_key: ApiKey::CreateTopic as u16,
                api_version: 0,
                error_code: 0,
                correlation_id: 2,
                timeout_ms: 1000,
                throttle_ms: 0,
                header_tags: Vec::new(),
                body: CreateTopicRequestV0 {
                    topic_name: "p8g-produce-quota".into(),
                    partition_count: 1,
                    replication_factor: 1,
                    tags: Vec::new(),
                }
                .encode_body()
                .unwrap(),
            },
            1024,
        )
        .await;
        assert_eq!(created.error_code, PublicError::None as u16);
        let topic_id = CreateTopicResponseV0::decode_body(&created.body)
            .unwrap()
            .topic_id;

        let mut successful_values = Vec::new();
        let mut throttled_value = None;
        for ordinal in 0..8_u8 {
            let value = vec![ordinal; 400];
            let response = p8g_write_request(
                &mut stream,
                WireFrame {
                    kind: FrameKind::Request,
                    flags: 0,
                    api_key: ApiKey::Produce as u16,
                    api_version: 0,
                    error_code: 0,
                    correlation_id: 3 + u64::from(ordinal),
                    timeout_ms: 1000,
                    throttle_ms: 0,
                    header_tags: Vec::new(),
                    body: ProduceRequestV0 {
                        acks: AckMode::Quorum,
                        partitions: vec![astralog_protocol::api::ProducePartitionRequestV0 {
                            topic_id,
                            partition: 0,
                            leader_epoch: astralog_protocol::UNKNOWN_EPOCH,
                            records: vec![WireRecordV0 {
                                timestamp_ms: i64::from(ordinal),
                                key: None,
                                value: Some(value.clone()),
                                headers: Vec::new(),
                            }],
                        }],
                        tags: Vec::new(),
                    }
                    .encode_body()
                    .unwrap(),
                },
                1024,
            )
            .await;
            if response.error_code == PublicError::BrokerBusy as u16 {
                assert!(response.throttle_ms >= 25);
                assert!(response.body.is_empty());
                throttled_value = Some(value);
                break;
            }
            assert_eq!(response.error_code, PublicError::None as u16);
            assert_eq!(response.throttle_ms, 0);
            let produced = ProduceResponseV0::decode_body(&response.body).unwrap();
            assert_eq!(produced.partitions.len(), 1);
            assert_eq!(produced.partitions[0].error_code, PublicError::None as u16);
            successful_values.push(value);
        }
        let throttled_value =
            throttled_value.expect("quota must throttle a response-bearing Produce");
        assert!(!successful_values.is_empty());

        // Allow both anonymous and topic buckets to refill before the proof
        // Fetch; quota rejection itself must not have mutated the log.
        time::sleep(Duration::from_millis(150)).await;
        let mut fetched_values = Vec::new();
        let mut fetch_offset = 0_u64;
        let mut correlation_id = 100_u64;
        loop {
            let fetched = p8g_write_request(
                &mut stream,
                WireFrame {
                    kind: FrameKind::Request,
                    flags: 0,
                    api_key: ApiKey::Fetch as u16,
                    api_version: 0,
                    error_code: 0,
                    correlation_id,
                    timeout_ms: 1000,
                    throttle_ms: 0,
                    header_tags: Vec::new(),
                    body: FetchRequestV0 {
                        isolation: IsolationLevel::ReadCommitted,
                        min_bytes: 0,
                        max_wait_ms: 0,
                        partitions: vec![FetchPartitionRequestV0 {
                            topic_id,
                            partition: 0,
                            leader_epoch: astralog_protocol::UNKNOWN_EPOCH,
                            fetch_offset,
                            max_bytes: 900,
                            max_records: 16,
                        }],
                        tags: Vec::new(),
                    }
                    .encode_body()
                    .unwrap(),
                },
                1024,
            )
            .await;
            assert_eq!(fetched.error_code, PublicError::None as u16);
            let fetched = FetchResponseV0::decode_body(&fetched.body).unwrap();
            let partition = &fetched.partitions[0];
            for record in &partition.records {
                if let Some(value) = &record.record.value {
                    fetched_values.push(value.clone());
                }
            }
            if partition.next_offset >= partition.high_watermark {
                break;
            }
            assert!(partition.next_offset > fetch_offset);
            fetch_offset = partition.next_offset;
            correlation_id += 1;
        }
        assert_eq!(fetched_values, successful_values);
        assert!(!fetched_values.iter().any(|value| value == &throttled_value));

        drop(stream);
        broker.shutdown().await.unwrap();
        let _ = std::fs::remove_dir_all(directory);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn p8g_slow_consumer_and_producer_flood_preserve_control_and_bounds() {
        use astralog_protocol::api::{
            FetchPartitionRequestV0, FetchResponseV0, IsolationLevel, MetadataRequestV0,
            WireRecordV0,
        };

        let nonce = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let directory = std::env::temp_dir().join(format!(
            "astralog-p8g-pressure-{}-{nonce}",
            std::process::id()
        ));
        let mut config = BrokerConfig::new(directory.clone(), "127.0.0.1:0".parse().unwrap());
        config.max_data_in_flight = 1;
        config.overload_throttle_ms = 25;
        let broker = start(config).await.unwrap();

        let mut client_config = ClientConfig::new(broker.local_addr().to_string());
        client_config.request_timeout = Duration::from_secs(2);
        let client = AstraClient::connect(client_config).await.unwrap();
        let topic = client.create_topic("p8g-pressure", 1).await.unwrap();
        let records = (0..64_u8)
            .map(|ordinal| WireRecordV0 {
                timestamp_ms: i64::from(ordinal),
                key: None,
                value: Some(vec![ordinal; 256]),
                headers: Vec::new(),
            })
            .collect::<Vec<_>>();
        let produced = client
            .produce_partition(
                topic.topic_id,
                0,
                astralog_protocol::UNKNOWN_EPOCH,
                records,
                AckMode::Quorum,
            )
            .await
            .unwrap()
            .unwrap();
        assert_eq!(produced.partitions[0].error_code, PublicError::None as u16);

        let mut slow = p8g_ready_connection(broker.local_addr(), 16 * 1024 * 1024).await;
        let slow_fetch = FetchRequestV0 {
            isolation: IsolationLevel::ReadCommitted,
            min_bytes: 1024 * 1024,
            max_wait_ms: 400,
            partitions: vec![FetchPartitionRequestV0 {
                topic_id: topic.topic_id,
                partition: 0,
                leader_epoch: astralog_protocol::UNKNOWN_EPOCH,
                fetch_offset: 0,
                max_bytes: 512,
                max_records: 1,
            }],
            tags: Vec::new(),
        };
        let slow_frame = WireFrame {
            kind: FrameKind::Request,
            flags: 0,
            api_key: ApiKey::Fetch as u16,
            api_version: 0,
            error_code: 0,
            correlation_id: 10,
            timeout_ms: 1000,
            throttle_ms: 0,
            header_tags: Vec::new(),
            body: slow_fetch.encode_body().unwrap(),
        };
        slow.write_all(&encode_frame(&slow_frame, 16 * 1024 * 1024).unwrap())
            .await
            .unwrap();
        time::sleep(Duration::from_millis(50)).await;

        let mut pressure = p8g_ready_connection(broker.local_addr(), 16 * 1024 * 1024).await;
        let control_started = time::Instant::now();
        let metadata = p8g_write_request(
            &mut pressure,
            WireFrame {
                kind: FrameKind::Request,
                flags: 0,
                api_key: ApiKey::Metadata as u16,
                api_version: 0,
                error_code: 0,
                correlation_id: 20,
                timeout_ms: 1000,
                throttle_ms: 0,
                header_tags: Vec::new(),
                body: MetadataRequestV0 {
                    all_topics: true,
                    topic_names: Vec::new(),
                    tags: Vec::new(),
                }
                .encode_body()
                .unwrap(),
            },
            16 * 1024 * 1024,
        )
        .await;
        assert_eq!(metadata.error_code, PublicError::None as u16);
        assert!(control_started.elapsed() < Duration::from_millis(300));

        let busy = p8g_write_request(
            &mut pressure,
            WireFrame {
                correlation_id: 21,
                ..slow_frame.clone()
            },
            16 * 1024 * 1024,
        )
        .await;
        assert_eq!(busy.error_code, PublicError::BrokerBusy as u16);
        assert!(busy.throttle_ms >= 25);

        for ordinal in 0..64_u8 {
            let no_response = ProduceRequestV0 {
                acks: AckMode::None,
                partitions: vec![astralog_protocol::api::ProducePartitionRequestV0 {
                    topic_id: topic.topic_id,
                    partition: 0,
                    leader_epoch: astralog_protocol::UNKNOWN_EPOCH,
                    records: vec![WireRecordV0 {
                        timestamp_ms: 1000 + i64::from(ordinal),
                        key: None,
                        value: Some(format!("overload-{ordinal}").into_bytes()),
                        headers: Vec::new(),
                    }],
                }],
                tags: Vec::new(),
            };
            let frame = WireFrame {
                kind: FrameKind::Request,
                flags: NO_RESPONSE_FLAG,
                api_key: ApiKey::Produce as u16,
                api_version: 0,
                error_code: 0,
                correlation_id: 0,
                timeout_ms: 0,
                throttle_ms: 0,
                header_tags: Vec::new(),
                body: no_response.encode_body().unwrap(),
            };
            pressure
                .write_all(&encode_frame(&frame, 16 * 1024 * 1024).unwrap())
                .await
                .unwrap();
        }

        let slow_response = p8g_read_frame(&mut slow, 16 * 1024 * 1024).await;
        assert_eq!(slow_response.error_code, PublicError::None as u16);
        let slow_body = FetchResponseV0::decode_body(&slow_response.body).unwrap();
        assert_eq!(slow_body.partitions[0].records.len(), 1);
        assert_eq!(slow_body.partitions[0].high_watermark, 64);

        let final_fetch = client
            .fetch(FetchRequestV0 {
                isolation: IsolationLevel::ReadCommitted,
                min_bytes: 0,
                max_wait_ms: 0,
                partitions: vec![FetchPartitionRequestV0 {
                    topic_id: topic.topic_id,
                    partition: 0,
                    leader_epoch: astralog_protocol::UNKNOWN_EPOCH,
                    fetch_offset: 0,
                    max_bytes: 1024 * 1024,
                    max_records: 128,
                }],
                tags: Vec::new(),
            })
            .await
            .unwrap();
        assert_eq!(final_fetch.partitions[0].high_watermark, 64);
        assert_eq!(final_fetch.partitions[0].records.len(), 64);
        assert!(final_fetch.partitions[0].records.iter().all(|record| {
            record
                .record
                .value
                .as_deref()
                .is_none_or(|value| !value.starts_with(b"overload-"))
        }));

        client.close().await;
        drop(slow);
        drop(pressure);
        broker.shutdown().await.unwrap();
        let _ = std::fs::remove_dir_all(directory);
    }

    #[tokio::test]
    async fn p7_public_dispatch_is_unusable_before_committed_readiness() {
        const TEST_MAX_FRAME_LEN: usize = 16 * 1024 * 1024;
        let nonce = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let directory = std::env::temp_dir().join(format!(
            "astralog-p7h-dispatch-gate-{}-{nonce}",
            std::process::id()
        ));
        let state = BrokerState::open(
            RuntimeConfig {
                data_dir: directory.clone(),
                broker_id: 1,
                advertised_host: "127.0.0.1".into(),
                advertised_port: 9092,
                max_frame_len: TEST_MAX_FRAME_LEN,
                max_topics: 16,
                max_partitions_per_topic: 16,
                max_total_partitions: 64,
            },
            None,
            None,
            None,
        )
        .unwrap();
        let init = WireFrame {
            kind: FrameKind::Request,
            flags: 0,
            api_key: ApiKey::InitProducer as u16,
            api_version: 0,
            error_code: 0,
            correlation_id: 1,
            timeout_ms: 1_000,
            throttle_ms: 0,
            header_tags: Vec::new(),
            body: InitProducerRequestV0 {
                expected_authority: None,
                tags: Vec::new(),
            }
            .encode_body()
            .unwrap(),
        };
        assert_eq!(
            dispatch_request(state.clone(), None, &init, TEST_MAX_FRAME_LEN)
                .await
                .unwrap_err(),
            PublicError::UnsupportedApiKey
        );

        let produce_v1 = WireFrame {
            api_key: ApiKey::Produce as u16,
            api_version: 1,
            correlation_id: 2,
            body: Vec::new(),
            ..init
        };
        assert_eq!(
            dispatch_request(state, None, &produce_v1, TEST_MAX_FRAME_LEN)
                .await
                .unwrap_err(),
            PublicError::UnsupportedApiVersion
        );
        std::fs::remove_dir_all(directory).unwrap();
    }
}
