use std::{
    path::PathBuf,
    sync::{Arc, RwLock},
    time::Duration,
};

use astralog_common::{ProducerAuthority, ProducerId, ProducerSequence, TopicId};
use astralog_controller::{
    ApplyOutcome, AssignmentRecord, BrokerAuthority, BrokerHeartbeatRequest,
    BrokerHeartbeatResponse, BrokerLifecycle, ControllerClient, ControllerCommand, ControllerError,
    ControllerMetadataSnapshot, ControllerState, CreateTopicSpec, MetadataSyncResponse,
    MovementRecord, MovementStage, P7_PRODUCER_FEATURE_LEVEL, P8_PERFORMANCE_FEATURE_LEVEL,
    P9_RETENTION_FEATURE_LEVEL, ReassignmentResult, ReassignmentSpec, ReplicaProgressObservation,
    ReplicaProgressReport, TopicCreateResult, TopicPerformanceSpecV1, TopicRetentionSpecV1,
};
use astralog_groups::{
    InternalTopicDescriptor, InternalTopicKind, P6_INTERNAL_TOPICS, group_shard,
};
use astralog_protocol::{
    PublicError, UNKNOWN_BROKER_ID, UNKNOWN_EPOCH,
    api::{CoordinatorHintV0, GroupId},
};
use astralog_raft::{
    ConfigurationChange, EffectiveConfiguration, NodeId, Role, StableConfiguration,
};
use astralog_replication::{ControllerAuthorityEvidence, MembershipObservation};
use astralog_storage::DataRecord;
use tokio::{
    sync::{Mutex, Semaphore, oneshot},
    task::JoinHandle,
    time,
};

use crate::{
    BrokerError, Result,
    metadata_cache::BrokerMetadataCache,
    replication::{
        DynamicReplicationConfig, DynamicReplicationManager, IdempotentProduceAttemptError,
        ProduceResult, ReplicationClient, RoutingError, broker_stable_configuration,
        broker_to_node, node_to_broker_id,
    },
};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PreferredLeaderRequest {
    pub topic_id: astralog_common::TopicId,
    pub partition_id: u32,
    pub target_broker_id: u32,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PreferredLeaderDisposition {
    AlreadyPreferred,
    TransferObserved,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PreferredLeaderOutcome {
    pub topic_id: astralog_common::TopicId,
    pub partition_id: u32,
    pub target_broker_id: u32,
    pub observed_leader_epoch: u64,
    pub disposition: PreferredLeaderDisposition,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct P6InternalTopics {
    pub groups_topic_id: astralog_common::TopicId,
    pub offsets_topic_id: astralog_common::TopicId,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct GroupCoordinatorObservation {
    pub shard: u32,
    pub groups_topic_id: astralog_common::TopicId,
    pub hint: CoordinatorHintV0,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct P7InternalProduceOutcome {
    pub base_offset: u64,
    pub last_offset: u64,
    pub raft_index: u64,
    pub leader_broker_id: u32,
    pub leader_epoch: u64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct P7InternalProduceError {
    pub error: PublicError,
    pub leader_broker_id: u32,
    pub leader_epoch: u64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct P7InternalPartitionObservation {
    pub local_is_leader: bool,
    pub leader_broker_id: u32,
    pub leader_epoch: u64,
    pub durable_raft_index: u64,
    pub committed_raft_index: u64,
}

impl From<ProduceResult> for P7InternalProduceOutcome {
    fn from(value: ProduceResult) -> Self {
        Self {
            base_offset: value.base_offset,
            last_offset: value.last_offset,
            raft_index: value.raft_index,
            leader_broker_id: value.leader_broker_id,
            leader_epoch: value.leader_epoch,
        }
    }
}

impl From<RoutingError> for P7InternalProduceError {
    fn from(value: RoutingError) -> Self {
        Self {
            error: value.error,
            leader_broker_id: value.leader_broker_id,
            leader_epoch: value.leader_epoch,
        }
    }
}

fn controller_lookup_error(error: BrokerError) -> P7InternalProduceError {
    let public = match error {
        BrokerError::Controller(ControllerError::Busy(_))
        | BrokerError::Controller(ControllerError::LimitExceeded { .. }) => PublicError::BrokerBusy,
        BrokerError::Controller(ControllerError::NotLeader { .. }) => {
            PublicError::BrokerNotAvailable
        }
        _ => PublicError::BrokerNotAvailable,
    };
    P7InternalProduceError {
        error: public,
        leader_broker_id: UNKNOWN_BROKER_ID,
        leader_epoch: UNKNOWN_EPOCH,
    }
}

#[derive(Clone)]
pub struct DynamicControllerConfig {
    pub controllers: Vec<ControllerClient>,
    pub replication: DynamicReplicationConfig,
    pub metadata_refresh_interval: Duration,
    pub preferred_leader_timeout: Duration,
    pub max_pending_admin_operations: usize,
}

impl DynamicControllerConfig {
    pub fn new(controllers: Vec<ControllerClient>, replication: DynamicReplicationConfig) -> Self {
        Self {
            controllers,
            replication,
            metadata_refresh_interval: Duration::from_millis(250),
            preferred_leader_timeout: Duration::from_secs(5),
            max_pending_admin_operations: 256,
        }
    }

    fn validate(&self) -> Result<()> {
        if self.controllers.is_empty()
            || self.metadata_refresh_interval.is_zero()
            || self.preferred_leader_timeout.is_zero()
            || self.max_pending_admin_operations == 0
        {
            return Err(BrokerError::InvalidConfig(
                "dynamic controller configuration requires controllers and non-zero bounds",
            ));
        }
        let mut ids = self
            .controllers
            .iter()
            .map(ControllerClient::local_node)
            .collect::<Vec<_>>();
        ids.sort_unstable();
        if ids.windows(2).any(|window| window[0] == window[1]) {
            return Err(BrokerError::InvalidConfig(
                "dynamic controller client ids must be distinct",
            ));
        }
        Ok(())
    }
}

#[derive(Clone)]
pub(crate) struct ControllerRouter {
    controllers: Arc<Vec<ControllerClient>>,
}

impl ControllerRouter {
    fn new(mut controllers: Vec<ControllerClient>) -> Self {
        controllers.sort_by_key(ControllerClient::local_node);
        Self {
            controllers: Arc::new(controllers),
        }
    }

    pub(crate) async fn leader_id(&self) -> Option<u64> {
        self.find_leader()
            .await
            .map(|client| client.local_node().get())
    }

    async fn find_leader(&self) -> Option<ControllerClient> {
        for controller in self.controllers.iter() {
            if let Ok(status) = controller.status().await
                && !status.fatal
                && status.role == Role::Leader
            {
                return Some(controller.clone());
            }
        }
        None
    }

    async fn leader(&self) -> Result<ControllerClient> {
        self.find_leader()
            .await
            .ok_or(BrokerError::Controller(ControllerError::NotLeader {
                leader: None,
            }))
    }

    pub(crate) async fn metadata_snapshot(&self) -> Result<ControllerMetadataSnapshot> {
        for _ in 0..2 {
            let leader = self.leader().await?;
            match leader.metadata_snapshot().await {
                Ok(snapshot) => return Ok(snapshot),
                Err(ControllerError::NotLeader { .. }) => continue,
                Err(error) => return Err(error.into()),
            }
        }
        Err(BrokerError::Controller(ControllerError::NotLeader {
            leader: None,
        }))
    }

    pub(crate) async fn metadata_sync(&self, epoch: u64) -> Result<MetadataSyncResponse> {
        for _ in 0..2 {
            let leader = self.leader().await?;
            match leader.metadata_sync(epoch).await {
                Ok(response) => return Ok(response),
                Err(ControllerError::NotLeader { .. }) => continue,
                Err(error) => return Err(error.into()),
            }
        }
        Err(BrokerError::Controller(ControllerError::NotLeader {
            leader: None,
        }))
    }

    pub(crate) async fn producer_authority(
        &self,
        producer_id: ProducerId,
    ) -> Result<Option<ProducerAuthority>> {
        for _ in 0..2 {
            let leader = self.leader().await?;
            match leader.producer_authority(producer_id).await {
                Ok(authority) => return Ok(authority),
                Err(ControllerError::NotLeader { .. }) => continue,
                Err(error) => return Err(error.into()),
            }
        }
        Err(BrokerError::Controller(ControllerError::NotLeader {
            leader: None,
        }))
    }

    pub(crate) async fn init_producer(
        &self,
        expected_authority: Option<ProducerAuthority>,
    ) -> Result<ProducerAuthority> {
        for _ in 0..2 {
            let leader = self.leader().await?;
            match leader.init_producer(expected_authority).await {
                Ok(authority) => return Ok(authority),
                Err(ControllerError::NotLeader { .. }) => continue,
                Err(error) => return Err(error.into()),
            }
        }
        Err(BrokerError::Controller(ControllerError::NotLeader {
            leader: None,
        }))
    }

    pub(crate) async fn heartbeat(
        &self,
        request: BrokerHeartbeatRequest,
    ) -> Result<BrokerHeartbeatResponse> {
        for _ in 0..2 {
            let leader = self.leader().await?;
            match leader.heartbeat(request.clone()).await {
                Ok(response) => return Ok(response),
                Err(ControllerError::NotLeader { .. }) => continue,
                Err(error) => return Err(error.into()),
            }
        }
        Err(BrokerError::Controller(ControllerError::NotLeader {
            leader: None,
        }))
    }

    pub(crate) async fn replica_progress_observation(
        &self,
        broker_id: u32,
        topic_id: astralog_common::TopicId,
        partition_id: u32,
        generation: astralog_controller::AssignmentGeneration,
    ) -> Result<Option<ReplicaProgressObservation>> {
        for _ in 0..2 {
            let leader = self.leader().await?;
            match leader
                .replica_progress_observation(broker_id, topic_id, partition_id, generation)
                .await
            {
                Ok(observation) => return Ok(observation),
                Err(ControllerError::NotLeader { .. }) => continue,
                Err(error) => return Err(error.into()),
            }
        }
        Err(BrokerError::Controller(ControllerError::NotLeader {
            leader: None,
        }))
    }

    pub(crate) async fn create_topic(&self, spec: CreateTopicSpec) -> Result<TopicCreateResult> {
        for _ in 0..2 {
            let leader = self.leader().await?;
            match leader.create_topic(spec.clone()).await {
                Ok(result) => return Ok(result),
                Err(ControllerError::NotLeader { .. }) => continue,
                Err(error) => return Err(error.into()),
            }
        }
        Err(BrokerError::Controller(ControllerError::NotLeader {
            leader: None,
        }))
    }

    pub(crate) async fn create_topic_with_performance(
        &self,
        spec: CreateTopicSpec,
        performance: TopicPerformanceSpecV1,
    ) -> Result<TopicCreateResult> {
        for _ in 0..2 {
            let leader = self.leader().await?;
            match leader
                .create_topic_with_performance(spec.clone(), performance)
                .await
            {
                Ok(result) => return Ok(result),
                Err(ControllerError::NotLeader { .. }) => continue,
                Err(error) => return Err(error.into()),
            }
        }
        Err(BrokerError::Controller(ControllerError::NotLeader {
            leader: None,
        }))
    }

    pub(crate) async fn create_topic_with_retention(
        &self,
        spec: CreateTopicSpec,
        performance: TopicPerformanceSpecV1,
        retention: TopicRetentionSpecV1,
    ) -> Result<TopicCreateResult> {
        for _ in 0..2 {
            let leader = self.leader().await?;
            match leader
                .create_topic_with_retention(spec.clone(), performance, retention)
                .await
            {
                Ok(result) => return Ok(result),
                Err(ControllerError::NotLeader { .. }) => continue,
                Err(error) => return Err(error.into()),
            }
        }
        Err(BrokerError::Controller(ControllerError::NotLeader {
            leader: None,
        }))
    }

    pub(crate) async fn alter_topic_retention(
        &self,
        topic_id: TopicId,
        retention: TopicRetentionSpecV1,
    ) -> Result<ApplyOutcome> {
        for _ in 0..2 {
            let leader = self.leader().await?;
            match leader.alter_topic_retention(topic_id, retention).await {
                Ok(result) => return Ok(result),
                Err(ControllerError::NotLeader { .. }) => continue,
                Err(error) => return Err(error.into()),
            }
        }
        Err(BrokerError::Controller(ControllerError::NotLeader {
            leader: None,
        }))
    }

    pub(crate) async fn mutate(&self, command: ControllerCommand) -> Result<ApplyOutcome> {
        for _ in 0..2 {
            let leader = self.leader().await?;
            match leader.mutate(command.clone()).await {
                Ok(result) => return Ok(result),
                Err(ControllerError::NotLeader { .. }) => continue,
                Err(error) => return Err(error.into()),
            }
        }
        Err(BrokerError::Controller(ControllerError::NotLeader {
            leader: None,
        }))
    }

    pub(crate) async fn begin_reassignment(
        &self,
        spec: ReassignmentSpec,
    ) -> Result<ReassignmentResult> {
        for _ in 0..2 {
            let leader = self.leader().await?;
            match leader.begin_reassignment(spec).await {
                Ok(result) => return Ok(result),
                Err(ControllerError::NotLeader { .. }) => continue,
                Err(error) => return Err(error.into()),
            }
        }
        Err(BrokerError::Controller(ControllerError::NotLeader {
            leader: None,
        }))
    }
}

pub(crate) struct DynamicBrokerControl {
    local_broker_id: u32,
    cache: RwLock<BrokerMetadataCache>,
    publication_metadata_epoch: Arc<RwLock<u64>>,
    manager: Mutex<DynamicReplicationManager>,
    // Read-only P3 observations must not wait behind an entire runtime
    // shutdown/rebuild while the reconciliation owner holds `manager`.
    // ReplicationClient clones share one atomically replaced routing snapshot;
    // unavailable old actors fail closed during a replacement.
    observation_client: ReplicationClient,
    router: ControllerRouter,
    admin_slots: Arc<Semaphore>,
    preferred_leader_timeout: Duration,
}

impl DynamicBrokerControl {
    pub(crate) fn p7_ready(&self) -> bool {
        self.state_snapshot()
            .is_ok_and(|state| state.active_feature_level() >= P7_PRODUCER_FEATURE_LEVEL)
    }

    pub(crate) fn p8_ready(&self) -> bool {
        self.state_snapshot()
            .is_ok_and(|state| state.active_feature_level() >= P8_PERFORMANCE_FEATURE_LEVEL)
    }

    pub(crate) fn p9_ready(&self) -> bool {
        self.state_snapshot()
            .is_ok_and(|state| state.active_feature_level() >= P9_RETENTION_FEATURE_LEVEL)
    }

    pub(crate) async fn init_producer(
        &self,
        expected_authority: Option<ProducerAuthority>,
    ) -> Result<ProducerAuthority> {
        let _permit = self.acquire_admin().await?;
        self.router.init_producer(expected_authority).await
    }

    pub(crate) async fn start(
        config: &DynamicControllerConfig,
        data_dir: PathBuf,
        local_broker_id: u32,
        advertised_host: &str,
        advertised_port: u16,
    ) -> Result<Arc<Self>> {
        config.validate()?;
        let router = ControllerRouter::new(config.controllers.clone());
        let snapshot = router.metadata_snapshot().await?;
        let cache = BrokerMetadataCache::from_snapshot(snapshot)?;
        let state = cache.state().ok_or(BrokerError::Metadata(
            "controller bootstrap metadata cache is empty",
        ))?;
        let publication_metadata_epoch = Arc::new(RwLock::new(state.metadata_epoch()));
        let manager = DynamicReplicationManager::start(
            config.replication.clone(),
            data_dir,
            local_broker_id,
            advertised_host,
            advertised_port,
            state,
        )
        .await?;
        let observation_client = manager.client();
        Ok(Arc::new(Self {
            local_broker_id,
            cache: RwLock::new(cache),
            publication_metadata_epoch,
            manager: Mutex::new(manager),
            observation_client,
            router,
            admin_slots: Arc::new(Semaphore::new(config.max_pending_admin_operations)),
            preferred_leader_timeout: config.preferred_leader_timeout,
        }))
    }

    pub(crate) async fn replication_client(&self) -> ReplicationClient {
        self.manager.lock().await.client()
    }

    pub(crate) async fn produce_idempotent_internal(
        &self,
        topic_id: TopicId,
        partition: u32,
        expected_leader_epoch: u64,
        authority: ProducerAuthority,
        base_sequence: ProducerSequence,
        records: Vec<DataRecord>,
    ) -> std::result::Result<P7InternalProduceOutcome, P7InternalProduceError> {
        let client = self.manager.lock().await.client();
        let first = client
            .produce_idempotent(
                topic_id,
                partition,
                expected_leader_epoch,
                authority,
                base_sequence,
                records.clone(),
                ControllerAuthorityEvidence::NotChecked,
            )
            .await;
        match first {
            Ok(result) => return Ok(result.into()),
            Err(IdempotentProduceAttemptError::Routing(error)) => return Err(error.into()),
            Err(IdempotentProduceAttemptError::ControllerValidationRequired) => {}
        }

        let evidence = match self.router.producer_authority(authority.producer_id).await {
            Ok(Some(confirmed)) => ControllerAuthorityEvidence::Confirmed(confirmed),
            Ok(None) => ControllerAuthorityEvidence::Missing,
            Err(error) => return Err(controller_lookup_error(error)),
        };
        match client
            .produce_idempotent(
                topic_id,
                partition,
                expected_leader_epoch,
                authority,
                base_sequence,
                records,
                evidence,
            )
            .await
        {
            Ok(result) => Ok(result.into()),
            Err(IdempotentProduceAttemptError::Routing(error)) => Err(error.into()),
            Err(IdempotentProduceAttemptError::ControllerValidationRequired) => {
                Err(P7InternalProduceError {
                    error: PublicError::BrokerNotAvailable,
                    leader_broker_id: UNKNOWN_BROKER_ID,
                    leader_epoch: UNKNOWN_EPOCH,
                })
            }
        }
    }

    pub(crate) async fn observe_partition_internal(
        &self,
        topic_id: TopicId,
        partition: u32,
    ) -> Result<P7InternalPartitionObservation> {
        let observation = self
            .observation_client
            .membership_status(topic_id, partition)
            .await
            .ok_or(BrokerError::Metadata(
                "local partition actor is unavailable for P7 qualification observation",
            ))?;
        Ok(P7InternalPartitionObservation {
            local_is_leader: observation.role == Role::Leader,
            leader_broker_id: observation
                .leader_id
                .map(node_to_broker_id)
                .transpose()?
                .unwrap_or(UNKNOWN_BROKER_ID),
            leader_epoch: if observation.leader_epoch == 0 {
                UNKNOWN_EPOCH
            } else {
                observation.leader_epoch
            },
            durable_raft_index: observation.durable_last_index.get(),
            committed_raft_index: observation.durable_commit_index.get(),
        })
    }

    /// P9-G integration-only observation of the actor's *committed P3*
    /// effective configuration. Controller MovementStage is not equivalent to
    /// committed Raft Joint; never infer the four-voter minimum from it.
    pub(crate) async fn observe_p9g_membership_internal(
        &self,
        topic_id: TopicId,
        partition: u32,
    ) -> Result<astralog_replication::MembershipObservation> {
        self.observation_client
            .membership_status(topic_id, partition)
            .await
            .ok_or(BrokerError::Metadata(
                "P9-G local partition actor is unavailable for committed joint observation",
            ))
    }

    pub(crate) async fn persist_producer_snapshot_internal(
        &self,
        topic_id: TopicId,
        partition: u32,
    ) -> Result<()> {
        let client = self.manager.lock().await.client();
        client.persist_producer_snapshot(topic_id, partition).await
    }

    pub(crate) fn state_snapshot(&self) -> Result<ControllerState> {
        let cache = self
            .cache
            .read()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        cache.state().cloned().ok_or(BrokerError::Metadata(
            "broker metadata cache is uninitialized",
        ))
    }

    pub(crate) fn publication_metadata_epoch_guard(&self) -> Arc<RwLock<u64>> {
        Arc::clone(&self.publication_metadata_epoch)
    }

    pub(crate) fn router(&self) -> &ControllerRouter {
        &self.router
    }

    pub(crate) async fn acquire_admin(&self) -> Result<tokio::sync::OwnedSemaphorePermit> {
        self.admin_slots.clone().try_acquire_owned().map_err(|_| {
            BrokerError::Controller(ControllerError::Busy(
                "broker pending controller/admin operation limit reached",
            ))
        })
    }

    pub(crate) async fn ensure_p6_internal_topics(&self) -> Result<P6InternalTopics> {
        let _permit = self.acquire_admin().await?;
        for descriptor in P6_INTERNAL_TOPICS {
            let snapshot = self.router.metadata_snapshot().await?;
            if let Some(topic) = snapshot.state.topic_by_name(descriptor.name) {
                validate_internal_topic(&snapshot.state, topic, descriptor)?;
                continue;
            }

            let create = self
                .router
                .create_topic(CreateTopicSpec {
                    topic_name: descriptor.name.to_owned(),
                    partition_count: descriptor.partition_count,
                    replication_factor: descriptor.replication_factor,
                    failure_domain_policy: astralog_controller::FailureDomainPolicy::Preferred,
                })
                .await;
            if let Err(error) = create {
                let current = self.router.metadata_snapshot().await?;
                let Some(topic) = current.state.topic_by_name(descriptor.name) else {
                    return Err(error);
                };
                validate_internal_topic(&current.state, topic, descriptor)?;
            }
            self.refresh_snapshot().await?;
        }

        let authoritative = self.router.metadata_snapshot().await?;
        let topics = validate_p6_internal_topics(&authoritative.state)?;
        self.refresh_snapshot().await?;
        Ok(topics)
    }

    pub(crate) async fn observe_group_coordinator(
        &self,
        group_id: &GroupId,
    ) -> Result<GroupCoordinatorObservation> {
        let state = self.state_snapshot()?;
        let topics = validate_p6_internal_topics(&state)?;
        let shard = group_shard(group_id)
            .map_err(|_| BrokerError::Metadata("group shard mapping failed"))?;
        let client = self.manager.lock().await.client();
        let status =
            client
                .status(topics.groups_topic_id, shard)
                .await
                .ok_or(BrokerError::Metadata(
                    "local broker has no observed P3 state for group coordinator shard",
                ))?;
        Ok(GroupCoordinatorObservation {
            shard,
            groups_topic_id: topics.groups_topic_id,
            hint: CoordinatorHintV0 {
                coordinator_broker_id: status.leader_broker_id.unwrap_or(UNKNOWN_BROKER_ID),
                coordinator_epoch: if status.leader_epoch == 0 {
                    UNKNOWN_EPOCH
                } else {
                    status.leader_epoch
                },
                current_group_epoch: 0,
                current_member_epoch: 0,
            },
        })
    }

    pub(crate) async fn validate_group_coordinator_authority(
        &self,
        group_id: &GroupId,
        expected_coordinator_epoch: u64,
    ) -> Result<GroupCoordinatorObservation> {
        if expected_coordinator_epoch == 0 || expected_coordinator_epoch == UNKNOWN_EPOCH {
            return Err(BrokerError::Metadata(
                "group coordinator authority requires a concrete non-zero P3 term",
            ));
        }
        let observation = self.observe_group_coordinator(group_id).await?;
        if observation.hint.coordinator_broker_id != self.local_broker_id
            || observation.hint.coordinator_epoch != expected_coordinator_epoch
        {
            return Err(BrokerError::Metadata(
                "stale or non-local group coordinator authority",
            ));
        }
        let client = self.manager.lock().await.client();
        let status = client
            .status(observation.groups_topic_id, observation.shard)
            .await
            .ok_or(BrokerError::Metadata(
                "local group coordinator shard actor is unavailable",
            ))?;
        if status.role != Role::Leader
            || status.leader_broker_id != Some(self.local_broker_id)
            || status.leader_epoch != expected_coordinator_epoch
        {
            return Err(BrokerError::Metadata(
                "local broker is no longer the current P3 group coordinator",
            ));
        }
        Ok(observation)
    }

    pub(crate) async fn request_preferred_leader(
        &self,
        request: PreferredLeaderRequest,
    ) -> Result<PreferredLeaderOutcome> {
        let _permit = self.acquire_admin().await?;
        // Eligibility is checked against a fresh controller-authoritative snapshot,
        // but this explicit hook must not itself reconcile/rebuild local replica
        // actors. The periodic metadata loop remains the sole topology owner.
        let state = self.router.metadata_snapshot().await?.state;
        let expected_configuration = preferred_leader_configuration(&state, request)?;
        let target_node = broker_to_node(request.target_broker_id)?;
        let client = self.manager.lock().await.client();
        let deadline = time::Instant::now() + self.preferred_leader_timeout;
        let mut transfer_requested = false;
        loop {
            let observation = client
                .membership_status(request.topic_id, request.partition_id)
                .await
                .ok_or(BrokerError::InvalidConfig(
                    "local partition actor is unavailable for preferred-leader hook",
                ))?;
            validate_preferred_leader_membership(
                &expected_configuration,
                target_node,
                &observation,
            )?;
            let observed_leader = observation.leader_id.map(node_to_broker_id).transpose()?;
            if observed_leader == Some(request.target_broker_id) {
                return Ok(PreferredLeaderOutcome {
                    topic_id: request.topic_id,
                    partition_id: request.partition_id,
                    target_broker_id: request.target_broker_id,
                    observed_leader_epoch: observation.leader_epoch,
                    disposition: if transfer_requested {
                        PreferredLeaderDisposition::TransferObserved
                    } else {
                        PreferredLeaderDisposition::AlreadyPreferred
                    },
                });
            }

            if !transfer_requested {
                if observation.role != Role::Leader {
                    return Err(BrokerError::InvalidConfig(
                        "preferred-leader hook must be invoked on the current P3 leader",
                    ));
                }
                let caught_up = preferred_leader_target_caught_up(target_node, &observation);
                if !caught_up {
                    if time::Instant::now() >= deadline {
                        return Err(BrokerError::InvalidConfig(
                            "preferred-leader target did not catch up through the durable commit before bounded timeout",
                        ));
                    }
                    time::sleep(Duration::from_millis(20)).await;
                    continue;
                }
                // Eligibility is controller authority, while catch-up/leadership is P3
                // authority. Re-read committed controller state at the hand-off boundary
                // without mutating local topology, so a lifecycle/assignment/movement
                // change during catch-up cannot be ignored.
                let latest_state = self.router.metadata_snapshot().await?.state;
                if preferred_leader_configuration(&latest_state, request)? != expected_configuration
                {
                    return Err(BrokerError::InvalidConfig(
                        "preferred-leader assignment changed while target was catching up",
                    ));
                }
                client
                    .transfer_leadership(
                        request.topic_id,
                        request.partition_id,
                        request.target_broker_id,
                    )
                    .await?;
                transfer_requested = true;
            }

            if time::Instant::now() >= deadline {
                return Err(BrokerError::InvalidConfig(
                    "preferred-leader transfer result was not observed before bounded timeout",
                ));
            }
            time::sleep(Duration::from_millis(20)).await;
        }
    }

    pub(crate) async fn heartbeat(&self) -> Result<bool> {
        let state = self.state_snapshot()?;
        let broker = state
            .broker(self.local_broker_id)
            .ok_or(BrokerError::Metadata(
                "local broker is absent from committed controller metadata",
            ))?
            .clone();
        let client = self.manager.lock().await.client();
        let mut replica_progress = Vec::new();
        for group in client.local_groups() {
            let Some(assignment) = state.assignments().get(&(group.topic_id, group.partition))
            else {
                continue;
            };
            let Some(observation) = client
                .membership_status(group.topic_id, group.partition)
                .await
            else {
                continue;
            };
            replica_progress.push(ReplicaProgressReport {
                topic_id: group.topic_id,
                partition_id: group.partition,
                assignment_generation: assignment.assignment_generation,
                durable_raft_index: observation.durable_last_index.get(),
                committed_raft_index: observation.durable_commit_index.get(),
            });
        }
        replica_progress.sort_unstable();
        let response = self
            .router
            .heartbeat(BrokerHeartbeatRequest {
                authority: BrokerAuthority {
                    broker_id: broker.broker_id,
                    node_uuid: broker.node_uuid,
                    incarnation_epoch: broker.incarnation_epoch,
                },
                applied_metadata_epoch: state.metadata_epoch(),
                min_feature: broker.min_feature,
                max_feature: broker.max_feature,
                replica_progress,
                movement_progress: Vec::new(),
            })
            .await?;
        self.apply_sync(&response.metadata_sync).await
    }

    async fn apply_sync(&self, response: &MetadataSyncResponse) -> Result<bool> {
        let mut staged = self
            .cache
            .read()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .clone();
        staged.apply_sync(response)?;
        let next_state = staged
            .state()
            .ok_or(BrokerError::Metadata("refreshed metadata cache is empty"))?;
        // Announce newly observed committed metadata before any potentially
        // long topology reconciliation. A P9-E publisher holding the read side
        // across the final manifest switch either completes before this write
        // or observes the newer epoch and aborts.
        *self
            .publication_metadata_epoch
            .write()
            .unwrap_or_else(std::sync::PoisonError::into_inner) = next_state.metadata_epoch();
        let changed = {
            let mut manager = self.manager.lock().await;
            manager.reconcile(next_state).await?
        };
        *self
            .cache
            .write()
            .unwrap_or_else(std::sync::PoisonError::into_inner) = staged;
        Ok(changed)
    }

    pub(crate) async fn refresh(&self) -> Result<bool> {
        let staged = self
            .cache
            .read()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .clone();
        let response = self
            .router
            .metadata_sync(staged.applied_metadata_epoch())
            .await?;
        drop(staged);
        match self.apply_sync(&response).await {
            Ok(changed) => Ok(changed),
            Err(_) if matches!(response, MetadataSyncResponse::Deltas { .. }) => {
                // A missing retained epoch is converted to Snapshot by the
                // controller. If a syntactically delivered delta batch still
                // fails atomic broker-cache application, treat the incremental
                // history as unusable and recover from current committed
                // controller authority rather than retrying the same bad suffix.
                self.refresh_snapshot().await
            }
            Err(error) => Err(error),
        }
    }

    pub(crate) async fn refresh_snapshot(&self) -> Result<bool> {
        let snapshot = self.router.metadata_snapshot().await?;
        let mut staged = self
            .cache
            .read()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .clone();
        staged.install_snapshot(snapshot)?;
        let next_state = staged
            .state()
            .ok_or(BrokerError::Metadata("refreshed metadata cache is empty"))?;
        *self
            .publication_metadata_epoch
            .write()
            .unwrap_or_else(std::sync::PoisonError::into_inner) = next_state.metadata_epoch();
        let changed = {
            let mut manager = self.manager.lock().await;
            manager.reconcile(next_state).await?
        };
        *self
            .cache
            .write()
            .unwrap_or_else(std::sync::PoisonError::into_inner) = staged;
        Ok(changed)
    }

    pub(crate) async fn reconcile_movements(&self) -> Result<bool> {
        let state = self.state_snapshot()?;
        let Some(movement) = state
            .movements()
            .values()
            .find(|movement| movement.stage != MovementStage::Complete)
            .cloned()
        else {
            return Ok(false);
        };
        self.reconcile_movement(&state, &movement).await
    }

    async fn reconcile_movement(
        &self,
        state: &ControllerState,
        movement: &MovementRecord,
    ) -> Result<bool> {
        let assignment = state
            .assignments()
            .get(&(movement.topic_id, movement.partition_id))
            .ok_or(BrokerError::Metadata(
                "movement partition assignment is missing from broker cache",
            ))?;
        if movement.stage != MovementStage::Complete
            && assignment.assignment_generation != movement.expected_assignment_generation
        {
            return Err(BrokerError::Metadata(
                "movement assignment generation differs from committed intent",
            ));
        }

        match movement.stage {
            MovementStage::Planned => {
                self.advance_movement(
                    movement,
                    MovementStage::Planned,
                    MovementStage::LearnerRequested,
                )
                .await
            }
            MovementStage::LearnerRequested => {
                self.reconcile_learner_request(assignment, movement).await
            }
            MovementStage::LearnerCreated => {
                self.advance_movement(
                    movement,
                    MovementStage::LearnerCreated,
                    MovementStage::CatchingUp,
                )
                .await
            }
            MovementStage::CatchingUp => self.reconcile_catchup(movement).await,
            MovementStage::PromotionReady => self.reconcile_enter_joint(assignment, movement).await,
            MovementStage::EnterJointRequested => {
                self.reconcile_joint_commit(assignment, movement).await
            }
            MovementStage::JointCommitted => self.reconcile_leave_joint(assignment, movement).await,
            MovementStage::LeaveJointRequested => self.reconcile_new_stable(movement).await,
            MovementStage::NewStableCommitted => {
                self.reconcile_leadership_preference(movement).await
            }
            MovementStage::LeadershipTransfer => self.reconcile_leadership_transfer(movement).await,
            MovementStage::OldReplicaRemoveRequested => {
                self.reconcile_old_replica_removed(movement).await
            }
            MovementStage::OldReplicaRemoved => self.finalize_movement(movement).await,
            MovementStage::Complete => Ok(false),
        }
    }

    async fn reconcile_learner_request(
        &self,
        assignment: &AssignmentRecord,
        movement: &MovementRecord,
    ) -> Result<bool> {
        let client = self.manager.lock().await.client();
        let Some(observation) = client
            .membership_status(movement.topic_id, movement.partition_id)
            .await
        else {
            return Ok(false);
        };
        let old = broker_stable_configuration(&assignment.voters, &assignment.learners)?;
        let mut learner_ids = assignment.learners.clone();
        if !learner_ids.contains(&movement.destination_broker_id) {
            learner_ids.push(movement.destination_broker_id);
            learner_ids.sort_unstable();
        }
        let with_learner = broker_stable_configuration(&assignment.voters, &learner_ids)?;

        match &observation.effective_configuration {
            EffectiveConfiguration::Stable(current)
                if current == &with_learner
                    && observation.pending_configuration_index.is_none() =>
            {
                self.advance_movement(
                    movement,
                    MovementStage::LearnerRequested,
                    MovementStage::LearnerCreated,
                )
                .await
            }
            EffectiveConfiguration::Stable(current) if current == &old => {
                if observation.role != Role::Leader {
                    return Ok(false);
                }
                let _ = client
                    .reconfigure(
                        movement.topic_id,
                        movement.partition_id,
                        ConfigurationChange::EnterJoint {
                            old,
                            new: with_learner,
                        },
                    )
                    .await?;
                Ok(true)
            }
            EffectiveConfiguration::Joint(joint)
                if joint.old() == &old && joint.new_config() == &with_learner =>
            {
                if observation
                    .pending_configuration_index
                    .is_some_and(|index| index <= observation.durable_commit_index)
                {
                    if observation.role != Role::Leader {
                        return Ok(false);
                    }
                    let _ = client
                        .reconfigure(
                            movement.topic_id,
                            movement.partition_id,
                            ConfigurationChange::LeaveJoint { new: with_learner },
                        )
                        .await?;
                    return Ok(true);
                }
                Ok(false)
            }
            EffectiveConfiguration::Stable(current)
                if current == &target_configuration(movement)?
                    && observation.pending_configuration_index.is_none() =>
            {
                self.advance_movement(
                    movement,
                    MovementStage::LearnerRequested,
                    MovementStage::LearnerCreated,
                )
                .await
            }
            _ => Ok(false),
        }
    }

    async fn reconcile_catchup(&self, movement: &MovementRecord) -> Result<bool> {
        let client = self.manager.lock().await.client();
        let Some(observation) = client
            .membership_status(movement.topic_id, movement.partition_id)
            .await
        else {
            return Ok(false);
        };
        if observation.role != Role::Leader {
            return Ok(false);
        }
        let destination_node = broker_stable_configuration(&[movement.destination_broker_id], &[])?
            .voters()
            .next()
            .expect("single broker configuration has one voter");
        let p3_caught_up = observation
            .leader_match_indexes
            .get(&destination_node)
            .is_some_and(|index| *index >= observation.durable_commit_index);
        if !p3_caught_up {
            return Ok(false);
        }
        let reported = self
            .router
            .replica_progress_observation(
                movement.destination_broker_id,
                movement.topic_id,
                movement.partition_id,
                movement.expected_assignment_generation,
            )
            .await?;
        if !reported.is_some_and(|progress| {
            progress.report.durable_raft_index >= observation.durable_commit_index.get()
        }) {
            return Ok(false);
        }
        self.advance_movement(
            movement,
            MovementStage::CatchingUp,
            MovementStage::PromotionReady,
        )
        .await
    }

    async fn reconcile_enter_joint(
        &self,
        assignment: &AssignmentRecord,
        movement: &MovementRecord,
    ) -> Result<bool> {
        let client = self.manager.lock().await.client();
        let Some(observation) = client
            .membership_status(movement.topic_id, movement.partition_id)
            .await
        else {
            return Ok(false);
        };
        let with_learner = learner_configuration(assignment, movement)?;
        let target = target_configuration(movement)?;
        match &observation.effective_configuration {
            EffectiveConfiguration::Stable(current) if current == &with_learner => {
                if observation.role != Role::Leader {
                    return Ok(false);
                }
                let accepted = client
                    .reconfigure(
                        movement.topic_id,
                        movement.partition_id,
                        ConfigurationChange::EnterJoint {
                            old: with_learner,
                            new: target,
                        },
                    )
                    .await?;
                if !accepted {
                    return Ok(false);
                }
                self.advance_movement(
                    movement,
                    MovementStage::PromotionReady,
                    MovementStage::EnterJointRequested,
                )
                .await
            }
            EffectiveConfiguration::Joint(joint)
                if joint.old() == &with_learner && joint.new_config() == &target =>
            {
                self.advance_movement(
                    movement,
                    MovementStage::PromotionReady,
                    MovementStage::EnterJointRequested,
                )
                .await
            }
            EffectiveConfiguration::Stable(current)
                if current == &target && observation.pending_configuration_index.is_none() =>
            {
                self.advance_movement(
                    movement,
                    MovementStage::PromotionReady,
                    MovementStage::EnterJointRequested,
                )
                .await
            }
            _ => Ok(false),
        }
    }

    async fn reconcile_joint_commit(
        &self,
        assignment: &AssignmentRecord,
        movement: &MovementRecord,
    ) -> Result<bool> {
        let client = self.manager.lock().await.client();
        let Some(observation) = client
            .membership_status(movement.topic_id, movement.partition_id)
            .await
        else {
            return Ok(false);
        };
        let with_learner = learner_configuration(assignment, movement)?;
        let target = target_configuration(movement)?;
        let joint_committed = matches!(
            &observation.effective_configuration,
            EffectiveConfiguration::Joint(joint)
                if joint.old() == &with_learner
                    && joint.new_config() == &target
                    && observation
                        .pending_configuration_index
                        .is_some_and(|index| index <= observation.durable_commit_index)
        );
        let already_stable = matches!(
            &observation.effective_configuration,
            EffectiveConfiguration::Stable(current)
                if current == &target && observation.pending_configuration_index.is_none()
        );
        if joint_committed || already_stable {
            return self
                .advance_movement(
                    movement,
                    MovementStage::EnterJointRequested,
                    MovementStage::JointCommitted,
                )
                .await;
        }
        Ok(false)
    }

    async fn reconcile_leave_joint(
        &self,
        assignment: &AssignmentRecord,
        movement: &MovementRecord,
    ) -> Result<bool> {
        let client = self.manager.lock().await.client();
        let Some(observation) = client
            .membership_status(movement.topic_id, movement.partition_id)
            .await
        else {
            return Ok(false);
        };
        let with_learner = learner_configuration(assignment, movement)?;
        let target = target_configuration(movement)?;
        match &observation.effective_configuration {
            EffectiveConfiguration::Joint(joint)
                if joint.old() == &with_learner && joint.new_config() == &target =>
            {
                if !observation
                    .pending_configuration_index
                    .is_some_and(|index| index <= observation.durable_commit_index)
                {
                    return Ok(false);
                }
                if observation.role != Role::Leader {
                    return Ok(false);
                }
                let accepted = client
                    .reconfigure(
                        movement.topic_id,
                        movement.partition_id,
                        ConfigurationChange::LeaveJoint { new: target },
                    )
                    .await?;
                if !accepted {
                    return Ok(false);
                }
                self.advance_movement(
                    movement,
                    MovementStage::JointCommitted,
                    MovementStage::LeaveJointRequested,
                )
                .await
            }
            EffectiveConfiguration::Stable(current)
                if current == &target && observation.pending_configuration_index.is_none() =>
            {
                self.advance_movement(
                    movement,
                    MovementStage::JointCommitted,
                    MovementStage::LeaveJointRequested,
                )
                .await
            }
            _ => Ok(false),
        }
    }

    async fn reconcile_new_stable(&self, movement: &MovementRecord) -> Result<bool> {
        let client = self.manager.lock().await.client();
        let Some(observation) = client
            .membership_status(movement.topic_id, movement.partition_id)
            .await
        else {
            return Ok(false);
        };
        let target = target_configuration(movement)?;
        if !matches!(
            &observation.effective_configuration,
            EffectiveConfiguration::Stable(current)
                if current == &target && observation.pending_configuration_index.is_none()
        ) {
            return Ok(false);
        }
        self.advance_movement(
            movement,
            MovementStage::LeaveJointRequested,
            MovementStage::NewStableCommitted,
        )
        .await
    }

    async fn reconcile_leadership_preference(&self, movement: &MovementRecord) -> Result<bool> {
        let Some(preferred) = movement.preferred_leader else {
            return self
                .advance_movement(
                    movement,
                    MovementStage::NewStableCommitted,
                    MovementStage::OldReplicaRemoveRequested,
                )
                .await;
        };
        let client = self.manager.lock().await.client();
        let Some(observation) = client
            .membership_status(movement.topic_id, movement.partition_id)
            .await
        else {
            return Ok(false);
        };
        let observed_leader = observation.leader_id.map(node_to_broker_id).transpose()?;
        if observed_leader == Some(preferred) {
            return self
                .advance_movement(
                    movement,
                    MovementStage::NewStableCommitted,
                    MovementStage::OldReplicaRemoveRequested,
                )
                .await;
        }
        if observation.role != Role::Leader {
            return Ok(false);
        }
        client
            .transfer_leadership(movement.topic_id, movement.partition_id, preferred)
            .await?;
        self.advance_movement(
            movement,
            MovementStage::NewStableCommitted,
            MovementStage::LeadershipTransfer,
        )
        .await
    }

    async fn reconcile_leadership_transfer(&self, movement: &MovementRecord) -> Result<bool> {
        let Some(preferred) = movement.preferred_leader else {
            return self
                .advance_movement(
                    movement,
                    MovementStage::LeadershipTransfer,
                    MovementStage::OldReplicaRemoveRequested,
                )
                .await;
        };
        let client = self.manager.lock().await.client();
        let Some(observation) = client
            .membership_status(movement.topic_id, movement.partition_id)
            .await
        else {
            return Ok(false);
        };
        let observed_leader = observation.leader_id.map(node_to_broker_id).transpose()?;
        if observed_leader == Some(preferred) {
            return self
                .advance_movement(
                    movement,
                    MovementStage::LeadershipTransfer,
                    MovementStage::OldReplicaRemoveRequested,
                )
                .await;
        }
        if observation.role == Role::Leader {
            let _ = client
                .transfer_leadership(movement.topic_id, movement.partition_id, preferred)
                .await;
        }
        Ok(false)
    }

    async fn reconcile_old_replica_removed(&self, movement: &MovementRecord) -> Result<bool> {
        // OldReplicaRemoveRequested is reachable only after P3 has already
        // exposed the committed stable target voter set excluding the source.
        // The dynamic control loop applies metadata/topology reconciliation
        // before movement reconciliation on every tick, so observing this stage
        // also means this broker has already processed the source-removal intent.
        // No fresh P3 leader/membership observation is needed here: requiring
        // one adds no safety and can strand a crash-resumable movement after the
        // old source actor is deliberately removed. Duplicate advances remain
        // harmless because the controller command is exact-stage fenced.
        self.advance_movement(
            movement,
            MovementStage::OldReplicaRemoveRequested,
            MovementStage::OldReplicaRemoved,
        )
        .await
    }

    async fn finalize_movement(&self, movement: &MovementRecord) -> Result<bool> {
        let replacement = AssignmentRecord {
            topic_id: movement.topic_id,
            partition_id: movement.partition_id,
            assignment_generation: movement.expected_assignment_generation.checked_next()?,
            voters: movement.target_voters.clone(),
            learners: Vec::new(),
        };
        self.router
            .mutate(ControllerCommand::FinalizeMovement {
                movement_id: movement.movement_id,
                expected_stage: MovementStage::OldReplicaRemoved,
                replacement,
            })
            .await?;
        Ok(true)
    }

    async fn advance_movement(
        &self,
        movement: &MovementRecord,
        expected_stage: MovementStage,
        new_stage: MovementStage,
    ) -> Result<bool> {
        self.router
            .mutate(ControllerCommand::AdvanceMovement {
                movement_id: movement.movement_id,
                expected_stage,
                new_stage,
            })
            .await?;
        Ok(true)
    }

    async fn shutdown_replication(&self) {
        self.manager.lock().await.shutdown().await;
    }
}

fn learner_configuration(
    assignment: &AssignmentRecord,
    movement: &MovementRecord,
) -> Result<StableConfiguration> {
    let mut learners = assignment.learners.clone();
    if !learners.contains(&movement.destination_broker_id) {
        learners.push(movement.destination_broker_id);
        learners.sort_unstable();
    }
    broker_stable_configuration(&assignment.voters, &learners)
}

fn target_configuration(movement: &MovementRecord) -> Result<StableConfiguration> {
    broker_stable_configuration(&movement.target_voters, &[])
}

fn preferred_leader_configuration(
    state: &ControllerState,
    request: PreferredLeaderRequest,
) -> Result<StableConfiguration> {
    let assignment = state
        .assignments()
        .get(&(request.topic_id, request.partition_id))
        .ok_or(BrokerError::Metadata(
            "preferred-leader request references unknown assignment",
        ))?;
    if state.movements().values().any(|movement| {
        movement.stage != MovementStage::Complete
            && movement.topic_id == request.topic_id
            && movement.partition_id == request.partition_id
    }) {
        return Err(BrokerError::InvalidConfig(
            "preferred-leader hook is unavailable during active reassignment",
        ));
    }
    let target = state
        .broker(request.target_broker_id)
        .ok_or(BrokerError::InvalidConfig(
            "preferred-leader target broker is unknown",
        ))?;
    if !assignment.voters.contains(&request.target_broker_id)
        || assignment.learners.contains(&request.target_broker_id)
    {
        return Err(BrokerError::InvalidConfig(
            "preferred-leader target is not a committed assignment voter",
        ));
    }
    if target.lifecycle != BrokerLifecycle::Active {
        return Err(BrokerError::InvalidConfig(
            "preferred-leader target broker is not ACTIVE",
        ));
    }
    broker_stable_configuration(&assignment.voters, &assignment.learners)
}

fn validate_preferred_leader_membership(
    expected_configuration: &StableConfiguration,
    target: NodeId,
    observation: &MembershipObservation,
) -> Result<()> {
    if observation.pending_configuration_index.is_some()
        || !matches!(
            &observation.effective_configuration,
            EffectiveConfiguration::Stable(current) if current == expected_configuration
        )
    {
        return Err(BrokerError::InvalidConfig(
            "preferred-leader hook requires stable P3 membership matching controller assignment",
        ));
    }
    if !expected_configuration.contains_voter(target) {
        return Err(BrokerError::InvalidConfig(
            "preferred-leader target is not an eligible P3 voter",
        ));
    }
    Ok(())
}

fn preferred_leader_target_caught_up(target: NodeId, observation: &MembershipObservation) -> bool {
    let Some(target_match) = observation.leader_match_indexes.get(&target).copied() else {
        return false;
    };
    target_match >= observation.durable_last_index
}

fn validate_internal_topic(
    state: &ControllerState,
    topic: &astralog_controller::TopicRecord,
    descriptor: InternalTopicDescriptor,
) -> Result<()> {
    validate_internal_topic_shape(topic, descriptor)?;
    for partition in 0..descriptor.partition_count {
        let assignment = state
            .assignments()
            .get(&(topic.topic_id, partition))
            .ok_or(BrokerError::Metadata(
                "reserved P6 internal topic is missing an authoritative assignment",
            ))?;
        if assignment.voters.len() != usize::from(descriptor.replication_factor) {
            return Err(BrokerError::Metadata(
                "reserved P6 internal topic assignment does not match frozen RF3",
            ));
        }
    }
    Ok(())
}

fn validate_internal_topic_shape(
    topic: &astralog_controller::TopicRecord,
    descriptor: InternalTopicDescriptor,
) -> Result<()> {
    if topic.topic_name != descriptor.name
        || topic.partition_count != descriptor.partition_count
        || topic.replication_factor != descriptor.replication_factor
        || topic.lifecycle != astralog_controller::TopicLifecycle::Active
        || topic.failure_domain_policy != astralog_controller::FailureDomainPolicy::Preferred
    {
        return Err(BrokerError::Metadata(
            "reserved P6 internal topic does not match frozen topology",
        ));
    }
    Ok(())
}

fn validate_p6_internal_topics(state: &ControllerState) -> Result<P6InternalTopics> {
    let mut groups_topic_id = None;
    let mut offsets_topic_id = None;
    for descriptor in P6_INTERNAL_TOPICS {
        let topic = state
            .topic_by_name(descriptor.name)
            .ok_or(BrokerError::Metadata(
                "required P6 internal topic is absent from committed metadata",
            ))?;
        validate_internal_topic(state, topic, descriptor)?;
        match descriptor.kind {
            InternalTopicKind::Groups => groups_topic_id = Some(topic.topic_id),
            InternalTopicKind::Offsets => offsets_topic_id = Some(topic.topic_id),
        }
    }
    let groups_topic_id = groups_topic_id.ok_or(BrokerError::Metadata(
        "P6 groups internal topic descriptor is missing",
    ))?;
    let offsets_topic_id = offsets_topic_id.ok_or(BrokerError::Metadata(
        "P6 offsets internal topic descriptor is missing",
    ))?;
    if groups_topic_id == offsets_topic_id {
        return Err(BrokerError::Metadata(
            "P6 internal topics must have distinct TopicIds",
        ));
    }
    Ok(P6InternalTopics {
        groups_topic_id,
        offsets_topic_id,
    })
}

pub(crate) struct DynamicControlRuntime {
    shutdown: Option<oneshot::Sender<()>>,
    task: JoinHandle<()>,
    control: Arc<DynamicBrokerControl>,
}

impl DynamicControlRuntime {
    pub(crate) fn start(control: Arc<DynamicBrokerControl>, refresh_interval: Duration) -> Self {
        let (shutdown_tx, mut shutdown_rx) = oneshot::channel();
        let worker = control.clone();
        let task = tokio::spawn(async move {
            let mut interval = time::interval(refresh_interval);
            interval.set_missed_tick_behavior(time::MissedTickBehavior::Delay);
            loop {
                tokio::select! {
                    _ = &mut shutdown_rx => break,
                    _ = interval.tick() => {
                        // Controller unavailability is deliberately non-authoritative for
                        // existing data-plane actors. A failed refresh is observation-only.
                        let _ = worker.refresh().await;
                        let _ = worker.heartbeat().await;
                        // One committed movement action per broker tick keeps control work
                        // bounded. Every action is reconstructed from durable movement intent
                        // plus observed P3 state, so duplicate retries are safe after crashes.
                        let _ = worker.reconcile_movements().await;
                    }
                }
            }
        });
        Self {
            shutdown: Some(shutdown_tx),
            task,
            control,
        }
    }

    pub(crate) async fn request_preferred_leader(
        &self,
        request: PreferredLeaderRequest,
    ) -> Result<PreferredLeaderOutcome> {
        self.control.request_preferred_leader(request).await
    }

    pub(crate) async fn ensure_p6_internal_topics(&self) -> Result<P6InternalTopics> {
        self.control.ensure_p6_internal_topics().await
    }

    pub(crate) async fn observe_group_coordinator(
        &self,
        group_id: &GroupId,
    ) -> Result<GroupCoordinatorObservation> {
        self.control.observe_group_coordinator(group_id).await
    }

    pub(crate) async fn validate_group_coordinator_authority(
        &self,
        group_id: &GroupId,
        expected_coordinator_epoch: u64,
    ) -> Result<GroupCoordinatorObservation> {
        self.control
            .validate_group_coordinator_authority(group_id, expected_coordinator_epoch)
            .await
    }

    pub(crate) async fn produce_idempotent_internal(
        &self,
        topic_id: TopicId,
        partition: u32,
        expected_leader_epoch: u64,
        authority: ProducerAuthority,
        base_sequence: ProducerSequence,
        records: Vec<DataRecord>,
    ) -> std::result::Result<P7InternalProduceOutcome, P7InternalProduceError> {
        self.control
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

    pub(crate) async fn observe_partition_internal(
        &self,
        topic_id: TopicId,
        partition: u32,
    ) -> Result<P7InternalPartitionObservation> {
        self.control
            .observe_partition_internal(topic_id, partition)
            .await
    }

    pub(crate) async fn observe_p9g_membership_internal(
        &self,
        topic_id: TopicId,
        partition: u32,
    ) -> Result<MembershipObservation> {
        self.control
            .observe_p9g_membership_internal(topic_id, partition)
            .await
    }

    pub(crate) async fn persist_producer_snapshot_internal(
        &self,
        topic_id: TopicId,
        partition: u32,
    ) -> Result<()> {
        self.control
            .persist_producer_snapshot_internal(topic_id, partition)
            .await
    }

    pub(crate) async fn shutdown(mut self) {
        if let Some(shutdown) = self.shutdown.take() {
            let _ = shutdown.send(());
        }
        let _ = self.task.await;
        self.control.shutdown_replication().await;
    }
}

#[cfg(test)]
mod p5m_tests {
    use std::collections::BTreeMap;

    use astralog_raft::LogIndex;

    use super::*;

    fn leader_observation(
        configuration: StableConfiguration,
        target: NodeId,
        target_match: Option<LogIndex>,
    ) -> MembershipObservation {
        let mut leader_match_indexes = BTreeMap::new();
        leader_match_indexes.insert(NodeId::new(2).unwrap(), LogIndex::new(9));
        if let Some(target_match) = target_match {
            leader_match_indexes.insert(target, target_match);
        }
        MembershipObservation {
            role: Role::Leader,
            leader_id: Some(NodeId::new(2).unwrap()),
            leader_epoch: 7,
            durable_commit_index: LogIndex::new(8),
            durable_last_index: LogIndex::new(9),
            effective_configuration: EffectiveConfiguration::Stable(configuration),
            pending_configuration_index: None,
            leader_match_indexes,
        }
    }

    #[test]
    fn preferred_leader_target_requires_stable_caught_up_voter() {
        let configuration = broker_stable_configuration(&[1, 2, 3], &[]).unwrap();
        let target = broker_to_node(2).unwrap();

        let caught_up = leader_observation(configuration.clone(), target, Some(LogIndex::new(9)));
        validate_preferred_leader_membership(&configuration, target, &caught_up).unwrap();
        assert!(preferred_leader_target_caught_up(target, &caught_up));

        let lagging = leader_observation(configuration.clone(), target, Some(LogIndex::new(8)));
        validate_preferred_leader_membership(&configuration, target, &lagging).unwrap();
        assert!(!preferred_leader_target_caught_up(target, &lagging));

        let no_progress = leader_observation(configuration.clone(), target, None);
        validate_preferred_leader_membership(&configuration, target, &no_progress).unwrap();
        assert!(!preferred_leader_target_caught_up(target, &no_progress));

        let non_voter = broker_to_node(4).unwrap();
        assert!(
            validate_preferred_leader_membership(&configuration, non_voter, &caught_up).is_err()
        );

        let mut unstable = caught_up.clone();
        unstable.pending_configuration_index = Some(LogIndex::new(9));
        assert!(validate_preferred_leader_membership(&configuration, target, &unstable).is_err());

        let mut follower = caught_up;
        follower.role = Role::Follower;
        validate_preferred_leader_membership(&configuration, target, &follower).unwrap();
        assert!(preferred_leader_target_caught_up(target, &follower));
    }

    #[test]
    fn p6_internal_topic_shape_must_match_frozen_topology_exactly() {
        let descriptor = P6_INTERNAL_TOPICS[0];
        let mut topic = astralog_controller::TopicRecord {
            topic_id: astralog_common::TopicId::new([7; 16]),
            topic_name: descriptor.name.to_owned(),
            partition_count: descriptor.partition_count,
            replication_factor: descriptor.replication_factor,
            lifecycle: astralog_controller::TopicLifecycle::Active,
            failure_domain_policy: astralog_controller::FailureDomainPolicy::Preferred,
        };
        validate_internal_topic_shape(&topic, descriptor).unwrap();

        topic.partition_count = 63;
        assert!(validate_internal_topic_shape(&topic, descriptor).is_err());
        topic.partition_count = descriptor.partition_count;
        topic.replication_factor = 1;
        assert!(validate_internal_topic_shape(&topic, descriptor).is_err());
        topic.replication_factor = descriptor.replication_factor;
        topic.lifecycle = astralog_controller::TopicLifecycle::Deleting;
        assert!(validate_internal_topic_shape(&topic, descriptor).is_err());
        topic.lifecycle = astralog_controller::TopicLifecycle::Active;
        topic.failure_domain_policy = astralog_controller::FailureDomainPolicy::Disabled;
        assert!(validate_internal_topic_shape(&topic, descriptor).is_err());
    }
}
