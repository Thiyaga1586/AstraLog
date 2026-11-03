use std::{
    collections::HashMap,
    path::{Path, PathBuf},
    sync::{
        Arc, Mutex as StdMutex,
        atomic::{AtomicBool, AtomicU64, Ordering},
    },
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use astralog_common::{PartitionId, ProducerAuthority, RaftTerm, StreamOffset, TopicId};
use astralog_controller::{
    BrokerLifecycle, CleanupPolicyV1 as ControllerCleanupPolicyV1,
    CompressionPolicyV1 as ControllerCompressionPolicyV1, ControllerCommand, ControllerError,
    FailureDomainPolicy, P9_RETENTION_FEATURE_LEVEL, ReassignmentSpec,
    SubscriptionProtectionV1 as ControllerSubscriptionProtectionV1, TopicLifecycle,
    TopicPerformanceSpecV1, TopicRetentionSpecV1,
};
use astralog_groups::is_reserved_internal_topic_name;
use astralog_protocol::{
    ApiKey, P9_RETENTION_COMPACTION_FEATURE_KEY, PublicError, UNKNOWN_BROKER_ID, UNKNOWN_EPOCH,
    api::{
        AckMode, AlterTopicConfigResponseV0, ApiVersionRange, ApiVersionsResponseV0,
        BrokerMetadataV0, CreateTopicResponseV0, CreateTopicResponseV1, CreateTopicResponseV2,
        DeleteTopicResponseV0, DescribeClusterResponseV0, DescribeTopicResponseV0,
        DescribeTopicResponseV1, DescribeTopicResponseV2, FeatureRange, FetchPartitionResponseV0,
        FetchRecordV0, FetchRequestV0, FetchResponseV0, ListOffsetsRequestV0,
        ListOffsetsResponseV0, ListOffsetsResultV0, MetadataPartitionV0, MetadataRequestV0,
        MetadataResponseV0, MetadataTopicV0, ProducePartitionResponseV0,
        ProducePartitionResponseV1, ProduceRequestV0, ProduceRequestV1, ProduceResponseV0,
        ProduceResponseV1, ReassignPartitionResultV0, ReassignPartitionsRequestV0,
        ReassignPartitionsResponseV0, TopicCleanupPolicyV1, TopicCompressionV1,
        TopicPerformanceConfigV1, TopicRetentionConfigV1, TopicRetentionPatchV1, WireHeaderV0,
        WireRecordV0, validate_topic_name,
    },
};
use astralog_replication::AckRequirement;
use astralog_storage::{
    CompactionLimits, Compression, DataRecord, Frame, PartitionLog, PartitionLogConfig,
    RecordBatch, TombstoneRemovalGate, UserHeader,
};
use tokio::sync::{Mutex, Notify, RwLock};

use crate::{
    BrokerError, Result,
    catalog::{Catalog, CatalogSnapshot, TopicDefinition},
    controller::DynamicBrokerControl,
    replication::{
        CompactionRunResult, ReplicationClient, RetentionAdvanceResult, StaticReplicationConfig,
    },
};

pub(crate) const P2_LEADER_EPOCH: u64 = 0;
type PartitionKey = (TopicId, u32);
type PartitionHandle = Arc<StdMutex<PartitionLog>>;
type PartitionMap = HashMap<PartitionKey, PartitionHandle>;

#[derive(Default)]
struct DataChangeSignal {
    notify: Notify,
    epoch: AtomicU64,
    wake_pending: AtomicBool,
    #[cfg(test)]
    emitted_wakeups: AtomicU64,
}

impl DataChangeSignal {
    fn publish(self: &Arc<Self>) {
        self.epoch.fetch_add(1, Ordering::Release);
        if self.wake_pending.swap(true, Ordering::AcqRel) {
            return;
        }
        let signal = Arc::clone(self);
        tokio::spawn(async move {
            // Coalesce a burst of commits that complete in the same scheduler
            // turn while retaining the monotonic epoch as the lost-wakeup
            // fence used by Fetch long polling.
            tokio::task::yield_now().await;
            signal.wake_pending.store(false, Ordering::Release);
            #[cfg(test)]
            signal.emitted_wakeups.fetch_add(1, Ordering::Relaxed);
            signal.notify.notify_waiters();
        });
    }
}

#[derive(Debug, Clone)]
pub(crate) struct RuntimeConfig {
    pub data_dir: PathBuf,
    pub broker_id: u32,
    pub advertised_host: String,
    pub advertised_port: u16,
    pub max_frame_len: usize,
    pub max_topics: usize,
    pub max_partitions_per_topic: u32,
    pub max_total_partitions: usize,
}

pub(crate) struct BrokerState {
    runtime: RuntimeConfig,
    catalog: Arc<StdMutex<Catalog>>,
    partitions: RwLock<PartitionMap>,
    admin_lock: Mutex<()>,
    partition_open_lock: Mutex<()>,
    data_signal: Arc<DataChangeSignal>,
    replication: Option<ReplicationClient>,
    dynamic: Option<Arc<DynamicBrokerControl>>,
}

impl BrokerState {
    pub(crate) const fn dynamic_mode(&self) -> bool {
        self.dynamic.is_some()
    }

    /// Executes one bounded P9-D delete-retention attempt. This is deliberately
    /// not a scheduler: P9-H owns background cleaner queues and cadence.
    ///
    /// The plan is fenced by both current partition leader epoch and the exact
    /// controller metadata epoch/policy snapshot. Any intervening controller
    /// mutation aborts before the replicated floor proposal.
    pub(crate) async fn run_delete_retention_once(
        &self,
        topic_id: TopicId,
        partition: u32,
        expected_leader_epoch: u64,
        protected_log_start_ceiling: Option<StreamOffset>,
        now_ms: i64,
    ) -> std::result::Result<Option<RetentionAdvanceResult>, PublicError> {
        let dynamic = self
            .dynamic
            .as_ref()
            .ok_or(PublicError::BrokerNotAvailable)?;
        let replication = self
            .replication
            .as_ref()
            .ok_or(PublicError::BrokerNotAvailable)?;
        let before = dynamic
            .state_snapshot()
            .map_err(|_| PublicError::BrokerNotAvailable)?;
        if before.active_feature_level() < P9_RETENTION_FEATURE_LEVEL {
            return Err(PublicError::UnsupportedApiVersion);
        }
        let topic = before
            .topics()
            .get(&topic_id)
            .ok_or(PublicError::UnknownTopic)?;
        if topic.lifecycle != TopicLifecycle::Active {
            return Err(PublicError::TopicDeleting);
        }
        if partition >= topic.partition_count {
            return Err(PublicError::UnknownPartition);
        }
        let policy = before
            .topic_retention_policy(topic_id)
            .copied()
            .ok_or(PublicError::UnknownTopic)?;
        if policy.cleanup_policy == ControllerCleanupPolicyV1::Compact {
            return Ok(None);
        }
        if policy.subscription_protection
            == ControllerSubscriptionProtectionV1::ReservedForProtectedFloor
            && protected_log_start_ceiling.is_none()
        {
            // Required provider unavailable: fail toward keeping more data.
            return Ok(None);
        }
        let policy_epoch = before.metadata_epoch();
        replication
            .snapshot_committed_for_retention(topic_id, partition, expected_leader_epoch)
            .await
            .map_err(|error| error.error)?;
        let plan = replication
            .plan_delete_retention(
                topic_id,
                partition,
                expected_leader_epoch,
                now_ms,
                policy.retention_ms,
                policy.retention_bytes,
                protected_log_start_ceiling,
            )
            .await
            .map_err(|error| error.error)?;
        if plan.leader_epoch != expected_leader_epoch {
            return Err(PublicError::StaleEpoch);
        }

        // Re-read controller authority after potentially expensive disk planning.
        // Global metadata epoch is a deliberately conservative P9-D policy epoch:
        // even an unrelated metadata change causes a retry rather than deletion
        // under stale controller evidence.
        let after = dynamic
            .state_snapshot()
            .map_err(|_| PublicError::BrokerNotAvailable)?;
        let unchanged_topic = after
            .topics()
            .get(&topic_id)
            .is_some_and(|current| current == topic);
        let unchanged_policy = after.topic_retention_policy(topic_id).copied() == Some(policy);
        if !retention_policy_fence_is_current(
            policy_epoch,
            after.metadata_epoch(),
            unchanged_topic,
            unchanged_policy,
        ) {
            return Err(PublicError::StaleEpoch);
        }

        if plan.plan.proposed_log_start_offset == plan.plan.current_log_start_offset
            && plan.plan.current_log_start_offset == StreamOffset::new(0)
        {
            return Ok(None);
        }
        replication
            .advance_log_start(
                topic_id,
                partition,
                expected_leader_epoch,
                plan.plan.base_manifest_generation,
                plan.plan.proposed_log_start_offset,
            )
            .await
            .map_err(|error| error.error)
            .map(Some)
    }

    /// Executes one bounded P9-E keyed-compaction attempt. This remains an
    /// explicit maintenance hook: P9-H owns cleaner queues, cadence and
    /// throttling. P9-F/G producer-checkpoint and tombstone-removal behavior is
    /// intentionally absent.
    pub(crate) async fn run_keyed_compaction_once(
        &self,
        topic_id: TopicId,
        partition: u32,
        expected_leader_epoch: u64,
        limits: CompactionLimits,
    ) -> std::result::Result<Option<CompactionRunResult>, PublicError> {
        let dynamic = self
            .dynamic
            .as_ref()
            .ok_or(PublicError::BrokerNotAvailable)?;
        let replication = self
            .replication
            .as_ref()
            .ok_or(PublicError::BrokerNotAvailable)?;
        let before = dynamic
            .state_snapshot()
            .map_err(|_| PublicError::BrokerNotAvailable)?;
        if before.active_feature_level() < P9_RETENTION_FEATURE_LEVEL {
            return Err(PublicError::UnsupportedApiVersion);
        }
        let topic = before
            .topics()
            .get(&topic_id)
            .ok_or(PublicError::UnknownTopic)?;
        if topic.lifecycle != TopicLifecycle::Active {
            return Err(PublicError::TopicDeleting);
        }
        if partition >= topic.partition_count {
            return Err(PublicError::UnknownPartition);
        }
        let policy = before
            .topic_retention_policy(topic_id)
            .copied()
            .ok_or(PublicError::UnknownTopic)?;
        if policy.cleanup_policy == ControllerCleanupPolicyV1::Delete {
            return Ok(None);
        }
        let policy_epoch = before.metadata_epoch();

        // P9-F first moves any producer-recovery authority that would be lost
        // by compaction into the ordinary quorum-committed maintenance log.
        // No-op when no producer state exists or the latest checkpoint already
        // covers it. Only after that commit do we persist a P3 snapshot floor,
        // ensuring both checkpoint and reclaimable source history are beneath
        // the cleaning boundary.
        replication
            .checkpoint_producer_state(topic_id, partition, expected_leader_epoch)
            .await
            .map_err(|error| error.error)?;

        // P9-E/F may rewrite only committed history below a persisted P3
        // snapshot floor. Reuse the already qualified leader-fenced snapshot
        // path; this does not add a new authority mechanism.
        replication
            .snapshot_committed_for_retention(topic_id, partition, expected_leader_epoch)
            .await
            .map_err(|error| error.error)?;
        // A pre-epoch or unrepresentable wall clock must not be converted to
        // i64::MAX for P9-G grace: that would make a fresh tombstone appear
        // arbitrarily old. If time cannot be established, ordinary keyed COW
        // remains available but physical tombstone removal stays disabled.
        let tombstone_removal = if policy.cleanup_policy == ControllerCleanupPolicyV1::CompactDelete
        {
            p9g_tombstone_gate_from_clock(SystemTime::now(), policy.tombstone_grace_ms)
        } else {
            None
        };
        let planned = replication
            .plan_keyed_compaction(
                topic_id,
                partition,
                expected_leader_epoch,
                policy_epoch,
                limits,
                tombstone_removal,
            )
            .await
            .map_err(|error| error.error)?;
        if planned.leader_epoch != expected_leader_epoch {
            return Err(PublicError::StaleEpoch);
        }

        // A cleaner plan can be expensive. Force one controller sync, then
        // re-read authority before any COW publication so committed
        // policy/lifecycle drift invalidates the plan even if the periodic
        // metadata task has not run yet.
        dynamic
            .refresh()
            .await
            .map_err(|_| PublicError::BrokerNotAvailable)?;
        let after = dynamic
            .state_snapshot()
            .map_err(|_| PublicError::BrokerNotAvailable)?;
        let unchanged_topic = after
            .topics()
            .get(&topic_id)
            .is_some_and(|current| current == topic);
        let unchanged_policy = after.topic_retention_policy(topic_id).copied() == Some(policy);
        if !retention_policy_fence_is_current(
            policy_epoch,
            after.metadata_epoch(),
            unchanged_topic,
            unchanged_policy,
        ) {
            return Err(PublicError::StaleEpoch);
        }
        if planned.plan.is_empty() {
            return Ok(None);
        }
        let publication_metadata_epoch = dynamic.publication_metadata_epoch_guard();
        replication
            .execute_keyed_compaction(
                topic_id,
                partition,
                expected_leader_epoch,
                planned.plan,
                policy_epoch,
                publication_metadata_epoch,
            )
            .await
            .map_err(|error| error.error)
            .map(Some)
    }

    pub(crate) fn open(
        runtime: RuntimeConfig,
        static_replication: Option<StaticReplicationConfig>,
        replication: Option<ReplicationClient>,
        dynamic: Option<Arc<DynamicBrokerControl>>,
    ) -> Result<Arc<Self>> {
        let catalog_dir = runtime.data_dir.join("metadata");
        let mut catalog = if let Some(dynamic) = &dynamic {
            let state = dynamic.state_snapshot()?;
            let local = state
                .broker(runtime.broker_id)
                .ok_or(BrokerError::InvalidConfig(
                    "local broker missing from committed controller metadata",
                ))?;
            Catalog::open_or_create_with_identity(
                &catalog_dir,
                runtime.broker_id,
                *state.cluster_id(),
                local.node_uuid,
            )?
        } else if let Some(config) = &static_replication {
            let local =
                config
                    .local_broker(runtime.broker_id)
                    .ok_or(BrokerError::InvalidConfig(
                        "local broker missing from static replication topology",
                    ))?;
            Catalog::open_or_create_with_identity(
                &catalog_dir,
                runtime.broker_id,
                config.cluster_id,
                local.node_uuid,
            )?
        } else {
            Catalog::open_or_create(&catalog_dir, runtime.broker_id)?
        };
        if dynamic.is_none() {
            catalog.bump_incarnation()?;
        }
        if let Some(config) = &static_replication {
            for topic in &config.topics {
                let definition = config.topic_definition(topic)?;
                catalog.ensure_topic(definition)?;
            }
        }
        let snapshot = catalog.snapshot();
        if dynamic.is_some() {
            return Ok(Arc::new(Self {
                runtime,
                catalog: Arc::new(StdMutex::new(catalog)),
                partitions: RwLock::new(HashMap::new()),
                admin_lock: Mutex::new(()),
                partition_open_lock: Mutex::new(()),
                data_signal: Arc::new(DataChangeSignal::default()),
                replication,
                dynamic,
            }));
        }
        if snapshot.topics.len() > runtime.max_topics {
            return Err(BrokerError::InvalidConfig(
                "durable topic count exceeds configured maximum",
            ));
        }
        let mut total_partitions = 0_usize;
        for topic in &snapshot.topics {
            if topic.partition_count > runtime.max_partitions_per_topic {
                return Err(BrokerError::InvalidConfig(
                    "durable topic partition count exceeds configured maximum",
                ));
            }
            total_partitions = total_partitions
                .checked_add(
                    usize::try_from(topic.partition_count)
                        .map_err(|_| BrokerError::InvalidConfig("partition count overflow"))?,
                )
                .ok_or(BrokerError::InvalidConfig("total partition count overflow"))?;
        }
        if total_partitions > runtime.max_total_partitions {
            return Err(BrokerError::InvalidConfig(
                "durable total partition count exceeds configured maximum",
            ));
        }
        let mut partitions = HashMap::new();
        for topic in &snapshot.topics {
            if static_replication
                .as_ref()
                .is_some_and(|config| config.topic_by_id(topic.topic_id).is_some())
            {
                continue;
            }
            for partition in 0..topic.partition_count {
                let log = open_partition_log(&runtime.data_dir, topic, partition)?;
                partitions.insert((topic.topic_id, partition), Arc::new(StdMutex::new(log)));
            }
        }
        Ok(Arc::new(Self {
            runtime,
            catalog: Arc::new(StdMutex::new(catalog)),
            partitions: RwLock::new(partitions),
            admin_lock: Mutex::new(()),
            partition_open_lock: Mutex::new(()),
            data_signal: Arc::new(DataChangeSignal::default()),
            replication,
            dynamic,
        }))
    }

    pub(crate) fn p7_ready(&self) -> bool {
        self.dynamic
            .as_ref()
            .is_some_and(|dynamic| dynamic.p7_ready())
    }

    pub(crate) fn p8_ready(&self) -> bool {
        self.dynamic
            .as_ref()
            .is_some_and(|dynamic| dynamic.p8_ready())
    }

    pub(crate) fn p9_ready(&self) -> bool {
        self.dynamic
            .as_ref()
            .is_some_and(|dynamic| dynamic.p9_ready())
    }

    pub(crate) fn api_versions_response(
        &self,
        p6_ready: bool,
        p7_ready: bool,
        p8_ready: bool,
        p9_ready: bool,
    ) -> ApiVersionsResponseV0 {
        const BASE_APIS: &[ApiKey] = &[
            ApiKey::ApiVersions,
            ApiKey::Metadata,
            ApiKey::CreateTopic,
            ApiKey::DeleteTopic,
            ApiKey::DescribeTopic,
            ApiKey::Produce,
            ApiKey::Fetch,
            ApiKey::ListOffsets,
        ];
        let mut implemented_apis = BASE_APIS.to_vec();
        if self.dynamic.is_some() {
            implemented_apis.push(ApiKey::DescribeCluster);
            implemented_apis.push(ApiKey::ReassignPartitions);
        }
        if p6_ready {
            implemented_apis.extend([
                ApiKey::JoinSubscribe,
                ApiKey::Heartbeat,
                ApiKey::RevokeAck,
                ApiKey::Leave,
                ApiKey::CommitOffsets,
                ApiKey::FetchOffsets,
            ]);
        }
        if p7_ready {
            implemented_apis.push(ApiKey::InitProducer);
        }
        if p9_ready {
            implemented_apis.push(ApiKey::AlterTopicConfig);
        }
        implemented_apis.sort_by_key(|api| *api as u16);
        ApiVersionsResponseV0 {
            server_software_name: "astralog-broker".into(),
            server_software_version: env!("CARGO_PKG_VERSION").into(),
            selected_wire_version: 1,
            max_frame_len: u32::try_from(self.runtime.max_frame_len).unwrap_or(u32::MAX),
            apis: implemented_apis
                .iter()
                .map(|api| ApiVersionRange {
                    api_key: *api as u16,
                    min_api_version: 0,
                    max_api_version: match *api {
                        ApiKey::Produce if p7_ready => 1,
                        ApiKey::CreateTopic | ApiKey::DescribeTopic if p9_ready => 2,
                        ApiKey::CreateTopic | ApiKey::DescribeTopic if p8_ready => 1,
                        _ => 0,
                    },
                })
                .collect(),
            features: {
                let mut features = vec![
                    FeatureRange {
                        feature_key: 1,
                        min_supported_version: 1,
                        max_supported_version: 1,
                        active_version: 1,
                    },
                    FeatureRange {
                        feature_key: 2,
                        min_supported_version: 1,
                        max_supported_version: 1,
                        active_version: 1,
                    },
                    FeatureRange {
                        feature_key: 3,
                        min_supported_version: 1,
                        max_supported_version: 1,
                        active_version: 1,
                    },
                    FeatureRange {
                        feature_key: 12,
                        min_supported_version: 1,
                        max_supported_version: 1,
                        active_version: if p6_ready { 1 } else { u16::MAX },
                    },
                    FeatureRange {
                        feature_key: 14,
                        min_supported_version: 1,
                        max_supported_version: 1,
                        active_version: if p8_ready { 1 } else { u16::MAX },
                    },
                    FeatureRange {
                        feature_key: P9_RETENTION_COMPACTION_FEATURE_KEY,
                        min_supported_version: 1,
                        max_supported_version: 1,
                        active_version: if p9_ready { 1 } else { u16::MAX },
                    },
                ];
                if p7_ready {
                    features.push(FeatureRange {
                        feature_key: 11,
                        min_supported_version: 1,
                        max_supported_version: 1,
                        active_version: 1,
                    });
                }
                features.sort_by_key(|feature| feature.feature_key);
                features
            },
            tags: Vec::new(),
        }
    }

    pub(crate) async fn init_producer(
        &self,
        expected_authority: Option<ProducerAuthority>,
    ) -> std::result::Result<ProducerAuthority, PublicError> {
        let dynamic = self
            .dynamic
            .as_ref()
            .ok_or(PublicError::UnsupportedFeature)?;
        if !dynamic.p7_ready() {
            return Err(PublicError::ClusterFeatureTooOld);
        }
        dynamic
            .init_producer(expected_authority)
            .await
            .map_err(p7_controller_public_error)
    }

    pub(crate) async fn produce_idempotent(
        self: &Arc<Self>,
        request: ProduceRequestV1,
    ) -> ProduceResponseV1 {
        let authority = request.authority;
        let Some(dynamic) = self.dynamic.as_ref() else {
            return ProduceResponseV1 {
                partitions: request
                    .partitions
                    .into_iter()
                    .map(|partition| ProducePartitionResponseV1 {
                        topic_id: partition.topic_id,
                        partition: partition.partition,
                        error_code: PublicError::UnsupportedFeature as u16,
                        base_offset: u64::MAX,
                        last_offset: u64::MAX,
                        leader_broker_id: UNKNOWN_BROKER_ID,
                        leader_epoch: UNKNOWN_EPOCH,
                    })
                    .collect(),
                tags: Vec::new(),
            };
        };
        let mut results = Vec::with_capacity(request.partitions.len());
        for partition in request.partitions {
            let topic_id = partition.topic_id;
            let partition_id = partition.partition;
            let records = partition
                .records
                .into_iter()
                .map(|record| wire_to_data_record(record, 0))
                .collect::<Vec<_>>();
            match dynamic
                .produce_idempotent_internal(
                    topic_id,
                    partition_id,
                    partition.leader_epoch,
                    authority,
                    partition.base_sequence,
                    records,
                )
                .await
            {
                Ok(outcome) => {
                    self.data_signal.publish();
                    results.push(ProducePartitionResponseV1 {
                        topic_id,
                        partition: partition_id,
                        error_code: PublicError::None as u16,
                        base_offset: outcome.base_offset,
                        last_offset: outcome.last_offset,
                        leader_broker_id: outcome.leader_broker_id,
                        leader_epoch: outcome.leader_epoch,
                    });
                }
                Err(error) => results.push(ProducePartitionResponseV1 {
                    topic_id,
                    partition: partition_id,
                    error_code: error.error as u16,
                    base_offset: u64::MAX,
                    last_offset: u64::MAX,
                    leader_broker_id: error.leader_broker_id,
                    leader_epoch: error.leader_epoch,
                }),
            }
        }
        ProduceResponseV1 {
            partitions: results,
            tags: Vec::new(),
        }
    }

    pub(crate) async fn metadata(&self, request: &MetadataRequestV0) -> Result<MetadataResponseV0> {
        if self.dynamic.is_some() {
            return self.dynamic_metadata(request).await;
        }
        let snapshot = self.catalog_snapshot().await?;
        let mut topics = Vec::new();
        if request.all_topics {
            for topic in &snapshot.topics {
                topics.push(self.topic_metadata(topic, PublicError::None).await);
            }
        } else {
            for name in &request.topic_names {
                if let Some(topic) = snapshot.topics.iter().find(|topic| topic.name == *name) {
                    topics.push(self.topic_metadata(topic, PublicError::None).await);
                } else {
                    topics.push(MetadataTopicV0 {
                        topic_error: PublicError::UnknownTopic as u16,
                        topic_name: name.clone(),
                        topic_id: TopicId::new([0; 16]),
                        partition_count: 0,
                        replication_factor: 0,
                        partitions: Vec::new(),
                    });
                }
            }
        }
        let brokers = if let Some(replication) = &self.replication {
            let topology = replication.topology_snapshot();
            topology
                .brokers
                .iter()
                .map(|broker| BrokerMetadataV0 {
                    broker_id: broker.broker_id,
                    node_uuid: broker.node_uuid,
                    incarnation_epoch: broker.transport_incarnation,
                    host: broker.client_host.clone(),
                    port: broker.client_port,
                })
                .collect()
        } else {
            vec![BrokerMetadataV0 {
                broker_id: snapshot.broker_id,
                node_uuid: snapshot.node_uuid,
                incarnation_epoch: snapshot.incarnation_epoch,
                host: self.runtime.advertised_host.clone(),
                port: self.runtime.advertised_port,
            }]
        };
        Ok(MetadataResponseV0 {
            cluster_id: snapshot.cluster_id,
            metadata_epoch: snapshot.metadata_epoch,
            brokers,
            topics,
            tags: Vec::new(),
        })
    }

    async fn dynamic_metadata(&self, request: &MetadataRequestV0) -> Result<MetadataResponseV0> {
        let dynamic = self.dynamic.as_ref().ok_or(BrokerError::InvalidConfig(
            "dynamic metadata requested in static mode",
        ))?;
        let state = dynamic.state_snapshot()?;
        let brokers = state
            .brokers()
            .values()
            .filter(|broker| broker.lifecycle != BrokerLifecycle::Removed)
            .map(|broker| BrokerMetadataV0 {
                broker_id: broker.broker_id,
                node_uuid: broker.node_uuid,
                incarnation_epoch: broker.incarnation_epoch,
                host: broker.client_host.clone(),
                port: broker.client_port,
            })
            .collect::<Vec<_>>();

        let mut topics = Vec::new();
        if request.all_topics {
            for topic in state.topics().values() {
                topics.push(self.dynamic_topic_metadata(&state, topic).await);
            }
        } else {
            for name in &request.topic_names {
                if let Some(topic) = state.topic_by_name(name) {
                    topics.push(self.dynamic_topic_metadata(&state, topic).await);
                } else {
                    topics.push(MetadataTopicV0 {
                        topic_error: PublicError::UnknownTopic as u16,
                        topic_name: name.clone(),
                        topic_id: TopicId::new([0; 16]),
                        partition_count: 0,
                        replication_factor: 0,
                        partitions: Vec::new(),
                    });
                }
            }
        }
        Ok(MetadataResponseV0 {
            cluster_id: *state.cluster_id(),
            metadata_epoch: state.metadata_epoch(),
            brokers,
            topics,
            tags: Vec::new(),
        })
    }

    async fn dynamic_topic_metadata(
        &self,
        state: &astralog_controller::ControllerState,
        topic: &astralog_controller::TopicRecord,
    ) -> MetadataTopicV0 {
        let mut partitions = Vec::with_capacity(topic.partition_count as usize);
        for partition in 0..topic.partition_count {
            let assignment = state.assignments().get(&(topic.topic_id, partition));
            let status = if let Some(replication) = &self.replication
                && replication.is_local_replica(topic.topic_id, partition)
            {
                replication.status(topic.topic_id, partition).await
            } else {
                None
            };
            partitions.push(MetadataPartitionV0 {
                partition,
                partition_error: if assignment.is_some() {
                    PublicError::None as u16
                } else {
                    PublicError::UnknownPartition as u16
                },
                leader_broker_id: status
                    .and_then(|status| status.leader_broker_id)
                    .unwrap_or(UNKNOWN_BROKER_ID),
                leader_epoch: status
                    .map(|status| status.leader_epoch)
                    .filter(|epoch| *epoch != 0)
                    .unwrap_or(UNKNOWN_EPOCH),
                replica_broker_ids: assignment
                    .map(|assignment| assignment.voters.clone())
                    .unwrap_or_default(),
            });
        }
        MetadataTopicV0 {
            topic_error: if topic.lifecycle == TopicLifecycle::Deleting {
                PublicError::TopicDeleting as u16
            } else {
                PublicError::None as u16
            },
            topic_name: topic.topic_name.clone(),
            topic_id: topic.topic_id,
            partition_count: topic.partition_count,
            replication_factor: topic.replication_factor,
            partitions,
        }
    }

    pub(crate) async fn create_topic(
        self: &Arc<Self>,
        name: String,
        partition_count: u32,
        replication_factor: u16,
    ) -> std::result::Result<CreateTopicResponseV0, PublicError> {
        if is_reserved_internal_topic_name(&name) {
            return Err(PublicError::InvalidTopic);
        }
        if let Some(dynamic) = &self.dynamic {
            if validate_topic_name(&name).is_err() {
                return Err(PublicError::InvalidTopic);
            }
            if partition_count == 0 || partition_count > self.runtime.max_partitions_per_topic {
                return Err(PublicError::InvalidRequest);
            }
            if !matches!(replication_factor, 1 | 3 | 5) {
                return Err(PublicError::InvalidReplicationFactor);
            }
            let state = dynamic
                .state_snapshot()
                .map_err(|_| PublicError::BrokerNotAvailable)?;
            if state.topic_by_name(&name).is_some() {
                return Err(PublicError::TopicAlreadyExists);
            }
            let _permit = dynamic
                .acquire_admin()
                .await
                .map_err(|_| PublicError::BrokerBusy)?;
            let result = dynamic
                .router()
                .create_topic(astralog_controller::CreateTopicSpec {
                    topic_name: name,
                    partition_count,
                    replication_factor,
                    failure_domain_policy: FailureDomainPolicy::Disabled,
                })
                .await
                .map_err(controller_public_error)?;
            dynamic
                .refresh_snapshot()
                .await
                .map_err(controller_public_error)?;
            return Ok(CreateTopicResponseV0 {
                topic_id: result.topic.topic_id,
                metadata_epoch: result.outcome.metadata_epoch,
                tags: Vec::new(),
            });
        }
        if validate_topic_name(&name).is_err() {
            return Err(PublicError::InvalidTopic);
        }
        if partition_count == 0 {
            return Err(PublicError::InvalidRequest);
        }
        if partition_count > self.runtime.max_partitions_per_topic {
            return Err(PublicError::BrokerBusy);
        }
        if replication_factor != 1 {
            return Err(PublicError::InvalidReplicationFactor);
        }

        let _admin_guard = self.admin_lock.lock().await;
        let snapshot = self
            .catalog_snapshot()
            .await
            .map_err(|_| PublicError::StorageError)?;
        if snapshot.topics.len() >= self.runtime.max_topics {
            return Err(PublicError::BrokerBusy);
        }
        let mut current_partitions = 0_usize;
        for topic in &snapshot.topics {
            current_partitions = current_partitions
                .checked_add(
                    usize::try_from(topic.partition_count)
                        .map_err(|_| PublicError::StorageError)?,
                )
                .ok_or(PublicError::StorageError)?;
        }
        let requested =
            usize::try_from(partition_count).map_err(|_| PublicError::InvalidRequest)?;
        let Some(total_partitions) = current_partitions.checked_add(requested) else {
            return Err(PublicError::StorageError);
        };
        if total_partitions > self.runtime.max_total_partitions {
            return Err(PublicError::BrokerBusy);
        }

        let catalog = self.catalog.clone();
        let name_for_create = name.clone();
        let topic = match tokio::task::spawn_blocking(move || {
            let mut catalog = lock_catalog(&catalog)?;
            if catalog.topic_by_name(&name_for_create).is_some() {
                return Err(BrokerError::Catalog("topic already exists"));
            }
            catalog.create_topic(name_for_create, partition_count, replication_factor)
        })
        .await
        {
            Ok(Ok(topic)) => topic,
            Ok(Err(BrokerError::Catalog("topic already exists"))) => {
                return Err(PublicError::TopicAlreadyExists);
            }
            Ok(Err(_)) | Err(_) => return Err(PublicError::StorageError),
        };

        for partition in 0..topic.partition_count {
            if self
                .ensure_partition(topic.clone(), partition)
                .await
                .is_err()
            {
                return Err(PublicError::StorageError);
            }
        }
        let metadata_epoch = self
            .catalog_snapshot()
            .await
            .map_err(|_| PublicError::StorageError)?
            .metadata_epoch;
        Ok(CreateTopicResponseV0 {
            topic_id: topic.topic_id,
            metadata_epoch,
            tags: Vec::new(),
        })
    }

    pub(crate) async fn create_topic_v1(
        self: &Arc<Self>,
        name: String,
        partition_count: u32,
        replication_factor: u16,
        performance: TopicPerformanceConfigV1,
    ) -> std::result::Result<CreateTopicResponseV1, PublicError> {
        if is_reserved_internal_topic_name(&name) || validate_topic_name(&name).is_err() {
            return Err(PublicError::InvalidTopic);
        }
        performance
            .validate()
            .map_err(|_| PublicError::InvalidRequest)?;
        let dynamic = self
            .dynamic
            .as_ref()
            .ok_or(PublicError::UnsupportedFeature)?;
        if !dynamic.p8_ready() {
            return Err(PublicError::ClusterFeatureTooOld);
        }
        if partition_count == 0 || partition_count > self.runtime.max_partitions_per_topic {
            return Err(PublicError::InvalidRequest);
        }
        if !matches!(replication_factor, 1 | 3 | 5) {
            return Err(PublicError::InvalidReplicationFactor);
        }
        let state = dynamic
            .state_snapshot()
            .map_err(|_| PublicError::BrokerNotAvailable)?;
        if state.topic_by_name(&name).is_some() {
            return Err(PublicError::TopicAlreadyExists);
        }
        let _permit = dynamic
            .acquire_admin()
            .await
            .map_err(|_| PublicError::BrokerBusy)?;
        let result = dynamic
            .router()
            .create_topic_with_performance(
                astralog_controller::CreateTopicSpec {
                    topic_name: name,
                    partition_count,
                    replication_factor,
                    failure_domain_policy: FailureDomainPolicy::Disabled,
                },
                controller_performance_spec(performance),
            )
            .await
            .map_err(controller_public_error)?;
        dynamic
            .refresh_snapshot()
            .await
            .map_err(controller_public_error)?;
        let state = dynamic
            .state_snapshot()
            .map_err(|_| PublicError::BrokerNotAvailable)?;
        let committed = state
            .topic_performance_policy(result.topic.topic_id)
            .copied()
            .ok_or(PublicError::UnknownServerError)?;
        Ok(CreateTopicResponseV1 {
            topic_id: result.topic.topic_id,
            metadata_epoch: result.outcome.metadata_epoch,
            performance: protocol_performance(committed),
            tags: Vec::new(),
        })
    }

    pub(crate) async fn create_topic_v2(
        self: &Arc<Self>,
        name: String,
        partition_count: u32,
        replication_factor: u16,
        performance: TopicPerformanceConfigV1,
        retention: TopicRetentionConfigV1,
    ) -> std::result::Result<CreateTopicResponseV2, PublicError> {
        if is_reserved_internal_topic_name(&name) || validate_topic_name(&name).is_err() {
            return Err(PublicError::InvalidTopic);
        }
        performance
            .validate()
            .map_err(|_| PublicError::InvalidRequest)?;
        retention
            .validate()
            .map_err(|_| PublicError::InvalidRequest)?;
        let dynamic = self
            .dynamic
            .as_ref()
            .ok_or(PublicError::UnsupportedFeature)?;
        if !dynamic.p9_ready() {
            return Err(PublicError::ClusterFeatureTooOld);
        }
        if partition_count == 0 || partition_count > self.runtime.max_partitions_per_topic {
            return Err(PublicError::InvalidRequest);
        }
        if !matches!(replication_factor, 1 | 3 | 5) {
            return Err(PublicError::InvalidReplicationFactor);
        }
        let state = dynamic
            .state_snapshot()
            .map_err(|_| PublicError::BrokerNotAvailable)?;
        if state.topic_by_name(&name).is_some() {
            return Err(PublicError::TopicAlreadyExists);
        }
        let _permit = dynamic
            .acquire_admin()
            .await
            .map_err(|_| PublicError::BrokerBusy)?;
        let result = dynamic
            .router()
            .create_topic_with_retention(
                astralog_controller::CreateTopicSpec {
                    topic_name: name,
                    partition_count,
                    replication_factor,
                    failure_domain_policy: FailureDomainPolicy::Disabled,
                },
                controller_performance_spec(performance),
                controller_retention_spec(retention),
            )
            .await
            .map_err(controller_public_error)?;
        dynamic
            .refresh_snapshot()
            .await
            .map_err(controller_public_error)?;
        let state = dynamic
            .state_snapshot()
            .map_err(|_| PublicError::BrokerNotAvailable)?;
        let committed_performance = state
            .topic_performance_policy(result.topic.topic_id)
            .copied()
            .ok_or(PublicError::UnknownServerError)?;
        let committed_retention = state
            .topic_retention_policy(result.topic.topic_id)
            .copied()
            .ok_or(PublicError::UnknownServerError)?;
        Ok(CreateTopicResponseV2 {
            topic_id: result.topic.topic_id,
            metadata_epoch: result.outcome.metadata_epoch,
            performance: protocol_performance(committed_performance),
            retention: protocol_retention(committed_retention),
            tags: Vec::new(),
        })
    }

    pub(crate) async fn delete_topic(
        self: &Arc<Self>,
        name: String,
    ) -> std::result::Result<DeleteTopicResponseV0, PublicError> {
        if is_reserved_internal_topic_name(&name) {
            return Err(PublicError::InvalidTopic);
        }
        if let Some(dynamic) = &self.dynamic {
            let _permit = dynamic
                .acquire_admin()
                .await
                .map_err(|_| PublicError::BrokerBusy)?;
            let state = dynamic
                .state_snapshot()
                .map_err(|_| PublicError::BrokerNotAvailable)?;
            let topic = state
                .topic_by_name(&name)
                .cloned()
                .ok_or(PublicError::UnknownTopic)?;
            if topic.lifecycle == TopicLifecycle::Active {
                dynamic
                    .router()
                    .mutate(ControllerCommand::BeginDeleteTopic {
                        topic_id: topic.topic_id,
                        expected_topic_lifecycle: TopicLifecycle::Active,
                    })
                    .await
                    .map_err(controller_public_error)?;
                dynamic
                    .refresh_snapshot()
                    .await
                    .map_err(controller_public_error)?;
            }
            let outcome = dynamic
                .router()
                .mutate(ControllerCommand::FinalizeDeleteTopic {
                    topic_id: topic.topic_id,
                    expected_topic_lifecycle: TopicLifecycle::Deleting,
                })
                .await
                .map_err(controller_public_error)?;
            dynamic
                .refresh_snapshot()
                .await
                .map_err(controller_public_error)?;
            return Ok(DeleteTopicResponseV0 {
                deleted_topic_id: topic.topic_id,
                metadata_epoch: outcome.metadata_epoch,
                tags: Vec::new(),
            });
        }
        if self.replication.as_ref().is_some_and(|replication| {
            replication
                .topology_snapshot()
                .topic_by_name(&name)
                .is_some()
        }) {
            return Err(PublicError::UnsupportedFeature);
        }
        let _admin_guard = self.admin_lock.lock().await;
        let catalog = self.catalog.clone();
        let deleted = match tokio::task::spawn_blocking(move || {
            let mut catalog = lock_catalog(&catalog)?;
            catalog.delete_topic(&name)
        })
        .await
        {
            Ok(Ok(topic)) => topic,
            Ok(Err(BrokerError::Catalog("unknown topic"))) => {
                return Err(PublicError::UnknownTopic);
            }
            Ok(Err(_)) | Err(_) => return Err(PublicError::StorageError),
        };

        {
            let mut partitions = self.partitions.write().await;
            partitions.retain(|(topic_id, _), _| *topic_id != deleted.topic_id);
        }
        let metadata_epoch = self
            .catalog_snapshot()
            .await
            .map_err(|_| PublicError::StorageError)?
            .metadata_epoch;
        Ok(DeleteTopicResponseV0 {
            deleted_topic_id: deleted.topic_id,
            metadata_epoch,
            tags: Vec::new(),
        })
    }

    pub(crate) async fn describe_topic(
        &self,
        name: &str,
    ) -> std::result::Result<DescribeTopicResponseV0, PublicError> {
        if let Some(dynamic) = &self.dynamic {
            let state = dynamic
                .state_snapshot()
                .map_err(|_| PublicError::BrokerNotAvailable)?;
            let topic = state.topic_by_name(name).ok_or(PublicError::UnknownTopic)?;
            return Ok(DescribeTopicResponseV0 {
                topic_name: topic.topic_name.clone(),
                topic_id: topic.topic_id,
                partition_count: topic.partition_count,
                replication_factor: topic.replication_factor,
                metadata_epoch: state.metadata_epoch(),
                tags: Vec::new(),
            });
        }
        let snapshot = self
            .catalog_snapshot()
            .await
            .map_err(|_| PublicError::StorageError)?;
        let topic = snapshot
            .topics
            .into_iter()
            .find(|topic| topic.name == name)
            .ok_or(PublicError::UnknownTopic)?;
        Ok(DescribeTopicResponseV0 {
            topic_name: topic.name,
            topic_id: topic.topic_id,
            partition_count: topic.partition_count,
            replication_factor: topic.replication_factor,
            metadata_epoch: snapshot.metadata_epoch,
            tags: Vec::new(),
        })
    }

    pub(crate) async fn describe_topic_v1(
        &self,
        name: &str,
    ) -> std::result::Result<DescribeTopicResponseV1, PublicError> {
        let dynamic = self
            .dynamic
            .as_ref()
            .ok_or(PublicError::UnsupportedFeature)?;
        if !dynamic.p8_ready() {
            return Err(PublicError::ClusterFeatureTooOld);
        }
        let state = dynamic
            .state_snapshot()
            .map_err(|_| PublicError::BrokerNotAvailable)?;
        let topic = state.topic_by_name(name).ok_or(PublicError::UnknownTopic)?;
        let performance = state
            .topic_performance_policy(topic.topic_id)
            .copied()
            .ok_or(PublicError::UnknownServerError)?;
        Ok(DescribeTopicResponseV1 {
            topic_name: topic.topic_name.clone(),
            topic_id: topic.topic_id,
            partition_count: topic.partition_count,
            replication_factor: topic.replication_factor,
            metadata_epoch: state.metadata_epoch(),
            performance: protocol_performance(performance),
            tags: Vec::new(),
        })
    }

    pub(crate) async fn describe_topic_v2(
        &self,
        name: &str,
    ) -> std::result::Result<DescribeTopicResponseV2, PublicError> {
        let dynamic = self
            .dynamic
            .as_ref()
            .ok_or(PublicError::UnsupportedFeature)?;
        if !dynamic.p9_ready() {
            return Err(PublicError::ClusterFeatureTooOld);
        }
        let state = dynamic
            .state_snapshot()
            .map_err(|_| PublicError::BrokerNotAvailable)?;
        let topic = state.topic_by_name(name).ok_or(PublicError::UnknownTopic)?;
        let performance = state
            .topic_performance_policy(topic.topic_id)
            .copied()
            .ok_or(PublicError::UnknownServerError)?;
        let retention = state
            .topic_retention_policy(topic.topic_id)
            .copied()
            .ok_or(PublicError::UnknownServerError)?;
        Ok(DescribeTopicResponseV2 {
            topic_name: topic.topic_name.clone(),
            topic_id: topic.topic_id,
            partition_count: topic.partition_count,
            replication_factor: topic.replication_factor,
            metadata_epoch: state.metadata_epoch(),
            performance: protocol_performance(performance),
            retention: protocol_retention(retention),
            tags: Vec::new(),
        })
    }

    pub(crate) async fn alter_topic_config(
        &self,
        name: &str,
        patch: TopicRetentionPatchV1,
    ) -> std::result::Result<AlterTopicConfigResponseV0, PublicError> {
        let dynamic = self
            .dynamic
            .as_ref()
            .ok_or(PublicError::UnsupportedFeature)?;
        if !dynamic.p9_ready() {
            return Err(PublicError::ClusterFeatureTooOld);
        }
        let state = dynamic
            .state_snapshot()
            .map_err(|_| PublicError::BrokerNotAvailable)?;
        let topic = state.topic_by_name(name).ok_or(PublicError::UnknownTopic)?;
        if topic.lifecycle != TopicLifecycle::Active {
            return Err(PublicError::TopicDeleting);
        }
        let current = state
            .topic_retention_policy(topic.topic_id)
            .copied()
            .ok_or(PublicError::UnknownServerError)?;
        let current_protocol = protocol_retention(current);
        let next = patch
            .apply_to(current_protocol)
            .map_err(|_| PublicError::InvalidRequest)?;
        let topic_id = topic.topic_id;
        let _permit = dynamic
            .acquire_admin()
            .await
            .map_err(|_| PublicError::BrokerBusy)?;
        let outcome = dynamic
            .router()
            .alter_topic_retention(topic_id, controller_retention_spec(next))
            .await
            .map_err(controller_public_error)?;
        dynamic
            .refresh_snapshot()
            .await
            .map_err(controller_public_error)?;
        let state = dynamic
            .state_snapshot()
            .map_err(|_| PublicError::BrokerNotAvailable)?;
        let committed = state
            .topic_retention_policy(topic_id)
            .copied()
            .ok_or(PublicError::UnknownServerError)?;
        Ok(AlterTopicConfigResponseV0 {
            topic_id,
            metadata_epoch: outcome.metadata_epoch,
            retention: protocol_retention(committed),
            tags: Vec::new(),
        })
    }

    pub(crate) async fn describe_cluster(
        &self,
    ) -> std::result::Result<DescribeClusterResponseV0, PublicError> {
        let dynamic = self
            .dynamic
            .as_ref()
            .ok_or(PublicError::UnsupportedFeature)?;
        let state = dynamic
            .state_snapshot()
            .map_err(|_| PublicError::BrokerNotAvailable)?;
        let controller_leader_id = dynamic.router().leader_id().await.unwrap_or(0);
        let brokers = state
            .brokers()
            .values()
            .filter(|broker| broker.lifecycle != BrokerLifecycle::Removed)
            .map(|broker| BrokerMetadataV0 {
                broker_id: broker.broker_id,
                node_uuid: broker.node_uuid,
                incarnation_epoch: broker.incarnation_epoch,
                host: broker.client_host.clone(),
                port: broker.client_port,
            })
            .collect();
        Ok(DescribeClusterResponseV0 {
            cluster_id: *state.cluster_id(),
            metadata_epoch: state.metadata_epoch(),
            active_feature_level: state.active_feature_level(),
            controller_leader_id,
            brokers,
            tags: Vec::new(),
        })
    }

    pub(crate) async fn reassign_partitions(
        &self,
        request: ReassignPartitionsRequestV0,
    ) -> std::result::Result<ReassignPartitionsResponseV0, PublicError> {
        let dynamic = self
            .dynamic
            .as_ref()
            .ok_or(PublicError::UnsupportedFeature)?;
        let _permit = dynamic
            .acquire_admin()
            .await
            .map_err(|_| PublicError::BrokerBusy)?;
        let mut results = Vec::with_capacity(request.partitions.len());
        for partition in request.partitions {
            let preferred_leader = (partition.preferred_leader_broker_id != UNKNOWN_BROKER_ID)
                .then_some(partition.preferred_leader_broker_id);
            let result = dynamic
                .router()
                .begin_reassignment(ReassignmentSpec {
                    topic_id: partition.topic_id,
                    partition_id: partition.partition,
                    source_broker_id: partition.source_broker_id,
                    destination_broker_id: partition.destination_broker_id,
                    preferred_leader,
                })
                .await
                .map_err(controller_public_error)?;
            results.push(ReassignPartitionResultV0 {
                topic_id: result.movement.topic_id,
                partition: result.movement.partition_id,
                movement_id: *result.movement.movement_id.as_bytes(),
                metadata_epoch: result.outcome.metadata_epoch,
            });
        }
        dynamic
            .refresh_snapshot()
            .await
            .map_err(controller_public_error)?;
        Ok(ReassignPartitionsResponseV0 {
            results,
            tags: Vec::new(),
        })
    }

    pub(crate) async fn produce(self: &Arc<Self>, request: ProduceRequestV0) -> ProduceResponseV0 {
        let now_ms = now_ms();
        let mut results = Vec::with_capacity(request.partitions.len());
        for partition in request.partitions {
            let topic_id = partition.topic_id;
            let partition_id = partition.partition;
            let result = self
                .produce_partition(partition, request.acks, now_ms)
                .await
                .unwrap_or_else(|error| ProducePartitionResponseV0 {
                    topic_id,
                    partition: partition_id,
                    error_code: error as u16,
                    base_offset: u64::MAX,
                    last_offset: u64::MAX,
                    leader_broker_id: self.runtime.broker_id,
                    leader_epoch: P2_LEADER_EPOCH,
                });
            results.push(result);
        }
        ProduceResponseV0 {
            partitions: results,
            tags: Vec::new(),
        }
    }

    pub(crate) async fn fetch_long_poll(
        self: &Arc<Self>,
        request: FetchRequestV0,
    ) -> FetchResponseV0 {
        let wait = Duration::from_millis(u64::from(request.max_wait_ms));
        let started = tokio::time::Instant::now();
        loop {
            let observed_epoch = self.data_signal.epoch.load(Ordering::Acquire);
            let notified = self.data_signal.notify.notified();
            tokio::pin!(notified);
            notified.as_mut().enable();
            let (response, record_bytes, has_error) = self.fetch_once(&request).await;
            if has_error
                || request.max_wait_ms == 0
                || record_bytes >= usize::try_from(request.min_bytes).unwrap_or(usize::MAX)
            {
                return response;
            }
            let elapsed = started.elapsed();
            if elapsed >= wait {
                return response;
            }
            if self.data_signal.epoch.load(Ordering::Acquire) != observed_epoch {
                continue;
            }
            if tokio::time::timeout(wait - elapsed, notified)
                .await
                .is_err()
            {
                return response;
            }
        }
    }

    pub(crate) async fn list_offsets(
        self: &Arc<Self>,
        request: ListOffsetsRequestV0,
    ) -> ListOffsetsResponseV0 {
        let mut results = Vec::with_capacity(request.queries.len());
        for query in request.queries {
            let topic_id = query.topic_id;
            let partition = query.partition;
            let result = if let Some(replication) = &self.replication
                && replication.contains_group(topic_id, partition)
            {
                let expected_epoch =
                    (query.leader_epoch != UNKNOWN_EPOCH).then_some(query.leader_epoch);
                match replication
                    .list_offset(
                        topic_id,
                        partition,
                        expected_epoch,
                        query.timestamp_selector,
                    )
                    .await
                {
                    Ok(value) => ListOffsetsResultV0 {
                        topic_id,
                        partition,
                        error_code: PublicError::None as u16,
                        leader_broker_id: value.leader_broker_id,
                        leader_epoch: value.leader_epoch,
                        offset: value.offset,
                        timestamp_ms: value.timestamp_ms,
                    },
                    Err(error) => ListOffsetsResultV0 {
                        topic_id,
                        partition,
                        error_code: error.error as u16,
                        leader_broker_id: error.leader_broker_id,
                        leader_epoch: error.leader_epoch,
                        offset: u64::MAX,
                        timestamp_ms: -1,
                    },
                }
            } else {
                match self
                    .partition_handle_for(topic_id, partition, query.leader_epoch)
                    .await
                {
                    Ok(handle) => match tokio::task::spawn_blocking(move || {
                        scan_list_offset(&handle, query.timestamp_selector)
                    })
                    .await
                    {
                        Ok(Ok((offset, timestamp_ms))) => ListOffsetsResultV0 {
                            topic_id,
                            partition,
                            error_code: PublicError::None as u16,
                            leader_broker_id: self.runtime.broker_id,
                            leader_epoch: P2_LEADER_EPOCH,
                            offset,
                            timestamp_ms,
                        },
                        Ok(Err(_)) | Err(_) => ListOffsetsResultV0 {
                            topic_id,
                            partition,
                            error_code: PublicError::StorageError as u16,
                            leader_broker_id: self.runtime.broker_id,
                            leader_epoch: P2_LEADER_EPOCH,
                            offset: u64::MAX,
                            timestamp_ms: -1,
                        },
                    },
                    Err(error) => ListOffsetsResultV0 {
                        topic_id,
                        partition,
                        error_code: error as u16,
                        leader_broker_id: if error == PublicError::UnknownTopic {
                            UNKNOWN_BROKER_ID
                        } else {
                            self.runtime.broker_id
                        },
                        leader_epoch: if error == PublicError::UnknownTopic {
                            UNKNOWN_EPOCH
                        } else {
                            P2_LEADER_EPOCH
                        },
                        offset: u64::MAX,
                        timestamp_ms: -1,
                    },
                }
            };
            results.push(result);
        }
        ListOffsetsResponseV0 {
            results,
            tags: Vec::new(),
        }
    }

    async fn produce_partition(
        self: &Arc<Self>,
        partition: astralog_protocol::api::ProducePartitionRequestV0,
        acks: AckMode,
        now_ms: i64,
    ) -> std::result::Result<ProducePartitionResponseV0, PublicError> {
        if partition.records.is_empty() {
            return Err(PublicError::InvalidRequest);
        }
        let topic = self
            .topic_by_id(partition.topic_id)
            .await
            .map_err(|_| PublicError::StorageError)?
            .ok_or(PublicError::UnknownTopic)?;
        if partition.partition >= topic.partition_count {
            return Err(PublicError::UnknownPartition);
        }
        if let Some(replication) = &self.replication
            && replication.contains_group(partition.topic_id, partition.partition)
        {
            let expected_epoch =
                (partition.leader_epoch != UNKNOWN_EPOCH).then_some(partition.leader_epoch);
            let records = partition
                .records
                .into_iter()
                .map(|record| wire_to_data_record(record, now_ms))
                .collect::<Vec<_>>();
            let ack = match acks {
                AckMode::None => AckRequirement::None,
                AckMode::Leader => AckRequirement::Leader,
                AckMode::Quorum => AckRequirement::Quorum,
            };
            let response = match replication
                .produce(
                    partition.topic_id,
                    partition.partition,
                    expected_epoch,
                    records,
                    ack,
                )
                .await
            {
                Ok(result) => {
                    if acks == AckMode::Quorum {
                        self.data_signal.publish();
                    }
                    ProducePartitionResponseV0 {
                        topic_id: partition.topic_id,
                        partition: partition.partition,
                        error_code: PublicError::None as u16,
                        base_offset: result.base_offset,
                        last_offset: result.last_offset,
                        leader_broker_id: result.leader_broker_id,
                        leader_epoch: result.leader_epoch,
                    }
                }
                Err(error) => ProducePartitionResponseV0 {
                    topic_id: partition.topic_id,
                    partition: partition.partition,
                    error_code: error.error as u16,
                    base_offset: u64::MAX,
                    last_offset: u64::MAX,
                    leader_broker_id: error.leader_broker_id,
                    leader_epoch: error.leader_epoch,
                },
            };
            return Ok(response);
        }
        if partition.leader_epoch != UNKNOWN_EPOCH && partition.leader_epoch != P2_LEADER_EPOCH {
            return Err(PublicError::StaleEpoch);
        }
        let handle = self
            .ensure_partition(topic, partition.partition)
            .await
            .map_err(|_| PublicError::StorageError)?;
        let records: Vec<DataRecord> = partition
            .records
            .into_iter()
            .map(|record| wire_to_data_record(record, now_ms))
            .collect();
        let record_count = records.len();
        let outcome = tokio::task::spawn_blocking(move || -> Result<(u64, u64)> {
            let mut log = handle.lock().map_err(|_| {
                BrokerError::Storage(astralog_storage::StorageError::InvalidFormat(
                    "partition lock poisoned",
                ))
            })?;
            let base_offset = log.next_stream_offset().get();
            let raft_index = log.next_raft_index();
            let batch = Frame::Data(RecordBatch {
                raft_term: RaftTerm::new(1),
                raft_index,
                base_stream_offset: StreamOffset::new(base_offset),
                producer: None,
                compression: Compression::None,
                records,
            });
            log.append(&batch, now_ms)?;
            if acks != AckMode::None {
                log.flush_data()?;
                log.mark_committed(raft_index.get())?;
            }
            let last_offset = base_offset
                .checked_add(
                    u64::try_from(record_count)
                        .map_err(|_| BrokerError::Catalog("record count overflow"))?
                        .saturating_sub(1),
                )
                .ok_or(BrokerError::Catalog("stream offset overflow"))?;
            Ok((base_offset, last_offset))
        })
        .await
        .map_err(|_| PublicError::StorageError)?
        .map_err(|_| PublicError::StorageError)?;

        if acks != AckMode::None {
            self.data_signal.publish();
        }
        Ok(ProducePartitionResponseV0 {
            topic_id: partition.topic_id,
            partition: partition.partition,
            error_code: PublicError::None as u16,
            base_offset: outcome.0,
            last_offset: outcome.1,
            leader_broker_id: self.runtime.broker_id,
            leader_epoch: P2_LEADER_EPOCH,
        })
    }

    async fn fetch_once(&self, request: &FetchRequestV0) -> (FetchResponseV0, usize, bool) {
        let mut results = Vec::with_capacity(request.partitions.len());
        let mut record_bytes = 0_usize;
        let mut has_error = false;
        let fixed_body_bytes = 6_usize.saturating_add(request.partitions.len().saturating_mul(62));
        let mut remaining_record_bytes = self
            .runtime
            .max_frame_len
            .saturating_sub(44)
            .saturating_sub(fixed_body_bytes);
        for query in &request.partitions {
            if let Some(replication) = &self.replication
                && replication.contains_group(query.topic_id, query.partition)
            {
                let expected_epoch =
                    (query.leader_epoch != UNKNOWN_EPOCH).then_some(query.leader_epoch);
                let max_records = usize::try_from(query.max_records).unwrap_or(usize::MAX);
                let max_bytes = usize::try_from(query.max_bytes)
                    .unwrap_or(usize::MAX)
                    .min(remaining_record_bytes);
                let result = match replication
                    .fetch(
                        query.topic_id,
                        query.partition,
                        expected_epoch,
                        StreamOffset::new(query.fetch_offset),
                        max_records,
                        max_bytes,
                    )
                    .await
                {
                    Ok(scan)
                        if query.fetch_offset < scan.log_start_offset
                            || query.fetch_offset > scan.high_watermark =>
                    {
                        has_error = true;
                        FetchPartitionResponseV0 {
                            topic_id: query.topic_id,
                            partition: query.partition,
                            error_code: PublicError::OffsetOutOfRange as u16,
                            leader_broker_id: scan.leader_broker_id,
                            leader_epoch: scan.leader_epoch,
                            log_start_offset: scan.log_start_offset,
                            high_watermark: scan.high_watermark,
                            next_offset: query.fetch_offset,
                            records: Vec::new(),
                        }
                    }
                    Ok(scan) => {
                        let mut used = 0_usize;
                        let mut conversion_error = false;
                        let mut wire_records = Vec::with_capacity(scan.records.len());
                        for stored in scan.records {
                            let wire = match into_wire_record(stored.record) {
                                Ok(wire) => wire,
                                Err(_) => {
                                    has_error = true;
                                    conversion_error = true;
                                    break;
                                }
                            };
                            let encoded_size = 8_usize.saturating_add(wire_record_size(&wire));
                            if used.saturating_add(encoded_size) > max_bytes {
                                break;
                            }
                            used = used.saturating_add(encoded_size);
                            wire_records.push(FetchRecordV0 {
                                stream_offset: stored.offset.get(),
                                record: wire,
                            });
                        }
                        record_bytes = record_bytes.saturating_add(used);
                        remaining_record_bytes = remaining_record_bytes.saturating_sub(used);
                        let next_offset = wire_records
                            .last()
                            .and_then(|record| record.stream_offset.checked_add(1))
                            .unwrap_or(query.fetch_offset);
                        FetchPartitionResponseV0 {
                            topic_id: query.topic_id,
                            partition: query.partition,
                            error_code: if conversion_error {
                                PublicError::StorageError as u16
                            } else {
                                PublicError::None as u16
                            },
                            leader_broker_id: scan.leader_broker_id,
                            leader_epoch: scan.leader_epoch,
                            log_start_offset: scan.log_start_offset,
                            high_watermark: scan.high_watermark,
                            next_offset,
                            records: wire_records,
                        }
                    }
                    Err(error) => {
                        has_error = true;
                        FetchPartitionResponseV0 {
                            topic_id: query.topic_id,
                            partition: query.partition,
                            error_code: error.error as u16,
                            leader_broker_id: error.leader_broker_id,
                            leader_epoch: error.leader_epoch,
                            log_start_offset: 0,
                            high_watermark: 0,
                            next_offset: query.fetch_offset,
                            records: Vec::new(),
                        }
                    }
                };
                results.push(result);
                continue;
            }
            let result = match self
                .partition_handle_for(query.topic_id, query.partition, query.leader_epoch)
                .await
            {
                Ok(handle) => {
                    let fetch_offset = query.fetch_offset;
                    let max_records = usize::try_from(query.max_records).unwrap_or(usize::MAX);
                    let max_bytes = usize::try_from(query.max_bytes)
                        .unwrap_or(usize::MAX)
                        .min(remaining_record_bytes);
                    match tokio::task::spawn_blocking(move || {
                        scan_fetch_partition(&handle, fetch_offset, max_records, max_bytes)
                    })
                    .await
                    {
                        Ok(Ok(scan))
                            if query.fetch_offset < scan.log_start_offset
                                || query.fetch_offset > scan.high_watermark =>
                        {
                            has_error = true;
                            FetchPartitionResponseV0 {
                                topic_id: query.topic_id,
                                partition: query.partition,
                                error_code: PublicError::OffsetOutOfRange as u16,
                                leader_broker_id: self.runtime.broker_id,
                                leader_epoch: P2_LEADER_EPOCH,
                                log_start_offset: scan.log_start_offset,
                                high_watermark: scan.high_watermark,
                                next_offset: query.fetch_offset,
                                records: Vec::new(),
                            }
                        }
                        Ok(Ok(scan)) => {
                            record_bytes = record_bytes.saturating_add(scan.used_bytes);
                            remaining_record_bytes =
                                remaining_record_bytes.saturating_sub(scan.used_bytes);
                            let next_offset = scan
                                .records
                                .last()
                                .and_then(|record| record.stream_offset.checked_add(1))
                                .unwrap_or(query.fetch_offset);
                            FetchPartitionResponseV0 {
                                topic_id: query.topic_id,
                                partition: query.partition,
                                error_code: PublicError::None as u16,
                                leader_broker_id: self.runtime.broker_id,
                                leader_epoch: P2_LEADER_EPOCH,
                                log_start_offset: scan.log_start_offset,
                                high_watermark: scan.high_watermark,
                                next_offset,
                                records: scan.records,
                            }
                        }
                        Ok(Err(_)) | Err(_) => {
                            has_error = true;
                            FetchPartitionResponseV0 {
                                topic_id: query.topic_id,
                                partition: query.partition,
                                error_code: PublicError::StorageError as u16,
                                leader_broker_id: self.runtime.broker_id,
                                leader_epoch: P2_LEADER_EPOCH,
                                log_start_offset: 0,
                                high_watermark: 0,
                                next_offset: query.fetch_offset,
                                records: Vec::new(),
                            }
                        }
                    }
                }
                Err(error) => {
                    has_error = true;
                    FetchPartitionResponseV0 {
                        topic_id: query.topic_id,
                        partition: query.partition,
                        error_code: error as u16,
                        leader_broker_id: if error == PublicError::UnknownTopic {
                            UNKNOWN_BROKER_ID
                        } else {
                            self.runtime.broker_id
                        },
                        leader_epoch: if error == PublicError::UnknownTopic {
                            UNKNOWN_EPOCH
                        } else {
                            P2_LEADER_EPOCH
                        },
                        log_start_offset: 0,
                        high_watermark: 0,
                        next_offset: query.fetch_offset,
                        records: Vec::new(),
                    }
                }
            };
            results.push(result);
        }
        (
            FetchResponseV0 {
                partitions: results,
                tags: Vec::new(),
            },
            record_bytes,
            has_error,
        )
    }

    async fn partition_handle_for(
        &self,
        topic_id: TopicId,
        partition: u32,
        leader_epoch: u64,
    ) -> std::result::Result<PartitionHandle, PublicError> {
        let topic = self
            .topic_by_id(topic_id)
            .await
            .map_err(|_| PublicError::StorageError)?
            .ok_or(PublicError::UnknownTopic)?;
        if partition >= topic.partition_count {
            return Err(PublicError::UnknownPartition);
        }
        if self
            .replication
            .as_ref()
            .is_some_and(|replication| replication.contains_group(topic_id, partition))
        {
            return Err(PublicError::NotLeader);
        }
        if self.dynamic.is_some() {
            return Err(PublicError::NotLeader);
        }
        if leader_epoch != UNKNOWN_EPOCH && leader_epoch != P2_LEADER_EPOCH {
            return Err(PublicError::StaleEpoch);
        }
        self.ensure_partition(topic, partition)
            .await
            .map_err(|_| PublicError::StorageError)
    }

    async fn ensure_partition(
        &self,
        topic: TopicDefinition,
        partition: u32,
    ) -> Result<PartitionHandle> {
        if self
            .replication
            .as_ref()
            .is_some_and(|replication| replication.contains_group(topic.topic_id, partition))
        {
            return Err(BrokerError::InvalidConfig(
                "static replicated partition cannot be opened through legacy P2 storage path",
            ));
        }
        if let Some(existing) = self
            .partitions
            .read()
            .await
            .get(&(topic.topic_id, partition))
            .cloned()
        {
            return Ok(existing);
        }
        let _open_guard = self.partition_open_lock.lock().await;
        if let Some(existing) = self
            .partitions
            .read()
            .await
            .get(&(topic.topic_id, partition))
            .cloned()
        {
            return Ok(existing);
        }
        let data_dir = self.runtime.data_dir.clone();
        let topic_for_open = topic.clone();
        let opened = tokio::task::spawn_blocking(move || {
            open_partition_log(&data_dir, &topic_for_open, partition)
        })
        .await
        .map_err(|err| BrokerError::TaskJoin(err.to_string()))??;
        let opened = Arc::new(StdMutex::new(opened));
        let mut partitions = self.partitions.write().await;
        Ok(partitions
            .entry((topic.topic_id, partition))
            .or_insert_with(|| opened.clone())
            .clone())
    }

    async fn catalog_snapshot(&self) -> Result<CatalogSnapshot> {
        let catalog = self.catalog.clone();
        tokio::task::spawn_blocking(move || {
            let catalog = lock_catalog(&catalog)?;
            Ok(catalog.snapshot())
        })
        .await
        .map_err(|err| BrokerError::TaskJoin(err.to_string()))?
    }

    async fn topic_by_id(&self, topic_id: TopicId) -> Result<Option<TopicDefinition>> {
        if let Some(dynamic) = &self.dynamic {
            let state = dynamic.state_snapshot()?;
            return Ok(state.topics().get(&topic_id).map(|topic| TopicDefinition {
                topic_id: topic.topic_id,
                name: topic.topic_name.clone(),
                partition_count: topic.partition_count,
                replication_factor: topic.replication_factor,
            }));
        }
        let catalog = self.catalog.clone();
        tokio::task::spawn_blocking(move || {
            let catalog = lock_catalog(&catalog)?;
            Ok(catalog.topic_by_id(topic_id))
        })
        .await
        .map_err(|err| BrokerError::TaskJoin(err.to_string()))?
    }

    async fn topic_metadata(&self, topic: &TopicDefinition, error: PublicError) -> MetadataTopicV0 {
        let mut partitions =
            Vec::with_capacity(usize::try_from(topic.partition_count).unwrap_or(1024));
        for partition in 0..topic.partition_count {
            if let Some(replication) = &self.replication
                && replication.contains_group(topic.topic_id, partition)
            {
                let replicas = replication
                    .replicas(topic.topic_id, partition)
                    .unwrap_or_default();
                let status = if replication.is_local_replica(topic.topic_id, partition) {
                    replication.status(topic.topic_id, partition).await
                } else {
                    None
                };
                partitions.push(MetadataPartitionV0 {
                    partition,
                    partition_error: PublicError::None as u16,
                    leader_broker_id: status
                        .and_then(|status| status.leader_broker_id)
                        .unwrap_or(UNKNOWN_BROKER_ID),
                    leader_epoch: status
                        .map(|status| status.leader_epoch)
                        .filter(|epoch| *epoch != 0)
                        .unwrap_or(UNKNOWN_EPOCH),
                    replica_broker_ids: replicas,
                });
                continue;
            }
            partitions.push(MetadataPartitionV0 {
                partition,
                partition_error: PublicError::None as u16,
                leader_broker_id: self.runtime.broker_id,
                leader_epoch: P2_LEADER_EPOCH,
                replica_broker_ids: vec![self.runtime.broker_id],
            });
        }
        MetadataTopicV0 {
            topic_error: error as u16,
            topic_name: topic.name.clone(),
            topic_id: topic.topic_id,
            partition_count: topic.partition_count,
            replication_factor: topic.replication_factor,
            partitions,
        }
    }
}

fn retention_policy_fence_is_current(
    planned_metadata_epoch: u64,
    current_metadata_epoch: u64,
    topic_unchanged: bool,
    policy_unchanged: bool,
) -> bool {
    planned_metadata_epoch == current_metadata_epoch && topic_unchanged && policy_unchanged
}

fn controller_performance_spec(performance: TopicPerformanceConfigV1) -> TopicPerformanceSpecV1 {
    TopicPerformanceSpecV1 {
        compression: match performance.compression {
            TopicCompressionV1::None => ControllerCompressionPolicyV1::None,
            TopicCompressionV1::Lz4 => ControllerCompressionPolicyV1::Lz4,
            TopicCompressionV1::Zstd => ControllerCompressionPolicyV1::Zstd,
        },
        max_record_bytes: performance.max_record_bytes,
        max_batch_records: performance.max_batch_records,
        max_batch_uncompressed_bytes: performance.max_batch_uncompressed_bytes,
    }
}

fn protocol_performance(
    performance: astralog_controller::TopicPerformancePolicyV1,
) -> TopicPerformanceConfigV1 {
    TopicPerformanceConfigV1 {
        compression: match performance.compression {
            ControllerCompressionPolicyV1::None => TopicCompressionV1::None,
            ControllerCompressionPolicyV1::Lz4 => TopicCompressionV1::Lz4,
            ControllerCompressionPolicyV1::Zstd => TopicCompressionV1::Zstd,
        },
        max_record_bytes: performance.max_record_bytes,
        max_batch_records: performance.max_batch_records,
        max_batch_uncompressed_bytes: performance.max_batch_uncompressed_bytes,
    }
}

fn controller_retention_spec(retention: TopicRetentionConfigV1) -> TopicRetentionSpecV1 {
    TopicRetentionSpecV1 {
        cleanup_policy: match retention.cleanup_policy {
            TopicCleanupPolicyV1::Delete => ControllerCleanupPolicyV1::Delete,
            TopicCleanupPolicyV1::Compact => ControllerCleanupPolicyV1::Compact,
            TopicCleanupPolicyV1::CompactDelete => ControllerCleanupPolicyV1::CompactDelete,
        },
        retention_ms: retention.retention_ms,
        retention_bytes: retention.retention_bytes,
        tombstone_grace_ms: retention.tombstone_grace_ms,
        subscription_protection: ControllerSubscriptionProtectionV1::Disabled,
    }
}

fn protocol_retention(
    retention: astralog_controller::TopicRetentionPolicyV1,
) -> TopicRetentionConfigV1 {
    TopicRetentionConfigV1 {
        cleanup_policy: match retention.cleanup_policy {
            ControllerCleanupPolicyV1::Delete => TopicCleanupPolicyV1::Delete,
            ControllerCleanupPolicyV1::Compact => TopicCleanupPolicyV1::Compact,
            ControllerCleanupPolicyV1::CompactDelete => TopicCleanupPolicyV1::CompactDelete,
        },
        retention_ms: retention.retention_ms,
        retention_bytes: retention.retention_bytes,
        tombstone_grace_ms: retention.tombstone_grace_ms,
    }
}

fn controller_public_error(error: BrokerError) -> PublicError {
    match error {
        BrokerError::Controller(ControllerError::NotLeader { .. }) => PublicError::NotLeader,
        BrokerError::Controller(ControllerError::Busy(_)) => PublicError::BrokerBusy,
        BrokerError::Controller(ControllerError::LimitExceeded { .. }) => PublicError::BrokerBusy,
        BrokerError::Controller(ControllerError::Invalid(_))
        | BrokerError::Controller(ControllerError::InvalidOwned(_)) => PublicError::InvalidRequest,
        BrokerError::Metadata(_) => PublicError::StaleEpoch,
        BrokerError::Io(_)
        | BrokerError::Storage(_)
        | BrokerError::Replication(_)
        | BrokerError::Raft(_) => PublicError::BrokerNotAvailable,
        _ => PublicError::UnknownServerError,
    }
}

fn p7_controller_public_error(error: BrokerError) -> PublicError {
    match error {
        BrokerError::Controller(ControllerError::Invalid("unknown producer session")) => {
            PublicError::UnknownProducer
        }
        BrokerError::Controller(ControllerError::Invalid("stale producer epoch")) => {
            PublicError::ProducerFenced
        }
        BrokerError::Controller(ControllerError::Invalid(
            "irreversible behavior is not enabled by active cluster feature level",
        )) => PublicError::ClusterFeatureTooOld,
        BrokerError::Controller(ControllerError::Busy(
            "InitProducer response was lost"
            | "controller leadership changed before InitProducer outcome was returned"
            | "controller node stopped before InitProducer outcome was known",
        )) => PublicError::RequestTimeout,
        BrokerError::Controller(ControllerError::Busy(_))
        | BrokerError::Controller(ControllerError::LimitExceeded { .. }) => PublicError::BrokerBusy,
        BrokerError::Controller(ControllerError::NotLeader { .. }) => {
            PublicError::BrokerNotAvailable
        }
        other => controller_public_error(other),
    }
}

#[derive(Debug, Clone)]
struct FetchScan {
    log_start_offset: u64,
    high_watermark: u64,
    used_bytes: usize,
    records: Vec<FetchRecordV0>,
}

fn scan_fetch_partition(
    handle: &PartitionHandle,
    fetch_offset: u64,
    max_records: usize,
    max_bytes: usize,
) -> Result<FetchScan> {
    let log = handle
        .lock()
        .map_err(|_| BrokerError::Catalog("partition lock poisoned"))?;
    let log_start_offset = log.committed_log_start_offset()?.get();
    let high_watermark = log.committed_stream_high_watermark()?.get();
    let (stored, used_bytes) = log.read_committed_records_from_bounded(
        StreamOffset::new(fetch_offset),
        max_records,
        max_bytes,
    )?;
    let mut records = Vec::with_capacity(stored.len());
    for stored in stored {
        records.push(FetchRecordV0 {
            stream_offset: stored.offset.get(),
            record: into_wire_record(stored.record)?,
        });
    }
    Ok(FetchScan {
        log_start_offset,
        high_watermark,
        used_bytes,
        records,
    })
}

fn scan_list_offset(handle: &PartitionHandle, timestamp_selector: i64) -> Result<(u64, i64)> {
    let log = handle
        .lock()
        .map_err(|_| BrokerError::Catalog("partition lock poisoned"))?;
    Ok(log.list_committed_offset(timestamp_selector)?)
}

fn into_wire_record(record: DataRecord) -> Result<WireRecordV0> {
    let headers = record
        .headers
        .into_iter()
        .map(|header| {
            let key = String::from_utf8(header.name)
                .map_err(|_| BrokerError::Catalog("stored record header key is not UTF-8"))?;
            Ok(WireHeaderV0 {
                key,
                value: header.value,
            })
        })
        .collect::<Result<Vec<_>>>()?;
    Ok(WireRecordV0 {
        timestamp_ms: record.timestamp_ms,
        key: record.key,
        value: record.value,
        headers,
    })
}

fn wire_to_data_record(record: WireRecordV0, now_ms: i64) -> DataRecord {
    DataRecord {
        timestamp_ms: if record.timestamp_ms == -1 {
            now_ms
        } else {
            record.timestamp_ms
        },
        key: record.key,
        value: record.value,
        headers: record
            .headers
            .into_iter()
            .map(|header| UserHeader {
                name: header.key.into_bytes(),
                value: header.value,
            })
            .collect(),
    }
}

fn wire_record_size(record: &WireRecordV0) -> usize {
    let mut size = 8_usize;
    size = size.saturating_add(4 + record.key.as_ref().map_or(0, Vec::len));
    size = size.saturating_add(4 + record.value.as_ref().map_or(0, Vec::len));
    size = size.saturating_add(2);
    for header in &record.headers {
        size = size.saturating_add(2 + header.key.len());
        size = size.saturating_add(4 + header.value.as_ref().map_or(0, Vec::len));
    }
    size
}

fn lock_catalog(catalog: &Arc<StdMutex<Catalog>>) -> Result<std::sync::MutexGuard<'_, Catalog>> {
    catalog
        .lock()
        .map_err(|_| BrokerError::Catalog("catalog lock poisoned"))
}

fn open_partition_log(
    data_dir: &Path,
    topic: &TopicDefinition,
    partition: u32,
) -> Result<PartitionLog> {
    let directory = partition_directory(data_dir, topic.topic_id, partition);
    Ok(PartitionLog::open_or_create(
        &directory,
        topic.topic_id,
        PartitionId::new(partition),
        now_ms(),
        PartitionLogConfig::default(),
    )?)
}

fn partition_directory(data_dir: &Path, topic_id: TopicId, partition: u32) -> PathBuf {
    let mut topic_hex = String::with_capacity(32);
    for byte in topic_id.as_bytes() {
        use std::fmt::Write as _;
        let _ = write!(&mut topic_hex, "{byte:02x}");
    }
    data_dir
        .join("partitions")
        .join(topic_hex)
        .join(format!("{partition:010}"))
}

fn now_ms() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .ok()
        .and_then(|duration| i64::try_from(duration.as_millis()).ok())
        .unwrap_or(i64::MAX)
}

/// Unlike the legacy timestamp fallback used elsewhere, P9-G tombstone
/// reclaim cannot use a forged MAX time on clock conversion failure.
fn p9g_tombstone_gate_from_clock(clock: SystemTime, grace_ms: u64) -> Option<TombstoneRemovalGate> {
    let elapsed = clock.duration_since(UNIX_EPOCH).ok()?;
    let now_ms = i64::try_from(elapsed.as_millis()).ok()?;
    (grace_ms > 0).then_some(TombstoneRemovalGate { now_ms, grace_ms })
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::{SystemTime, UNIX_EPOCH};

    fn temp_dir(name: &str) -> PathBuf {
        let nonce = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        std::env::temp_dir().join(format!(
            "astralog-state-{name}-{}-{nonce}",
            std::process::id()
        ))
    }

    fn runtime(dir: PathBuf) -> RuntimeConfig {
        RuntimeConfig {
            data_dir: dir,
            broker_id: 1,
            advertised_host: "127.0.0.1".into(),
            advertised_port: 9092,
            max_frame_len: 16 * 1024 * 1024,
            max_topics: 1024,
            max_partitions_per_topic: 1024,
            max_total_partitions: 16_384,
        }
    }

    #[test]
    fn p9d_policy_epoch_fence_rejects_any_controller_or_topic_policy_drift() {
        assert!(retention_policy_fence_is_current(9, 9, true, true));
        assert!(!retention_policy_fence_is_current(9, 10, true, true));
        assert!(!retention_policy_fence_is_current(9, 9, false, true));
        assert!(!retention_policy_fence_is_current(9, 9, true, false));
    }

    #[test]
    fn p9g_tombstone_clock_gate_fails_closed_before_epoch_and_for_zero_grace() {
        let before_epoch = UNIX_EPOCH.checked_sub(Duration::from_millis(1)).unwrap();
        assert_eq!(
            p9g_tombstone_gate_from_clock(before_epoch, 100),
            None,
            "pre-UNIX-epoch clock must never become MAX and authorize deletion"
        );
        assert_eq!(
            p9g_tombstone_gate_from_clock(UNIX_EPOCH, 100),
            Some(TombstoneRemovalGate {
                now_ms: 0,
                grace_ms: 100,
            }),
            "epoch zero is a valid timestamp but does not itself prove segment grace"
        );
        assert_eq!(
            p9g_tombstone_gate_from_clock(
                UNIX_EPOCH.checked_add(Duration::from_millis(120)).unwrap(),
                100,
            ),
            Some(TombstoneRemovalGate {
                now_ms: 120,
                grace_ms: 100,
            }),
        );
        assert_eq!(
            p9g_tombstone_gate_from_clock(UNIX_EPOCH, 0),
            None,
            "broker should preserve controller's strictly positive grace contract"
        );
    }

    #[test]
    fn p7_api_advertisement_is_readiness_gated_without_changing_legacy_produce() {
        let dir = temp_dir("p7-api-advertisement");
        let state = BrokerState::open(runtime(dir.clone()), None, None, None).unwrap();

        let inactive = state.api_versions_response(false, false, false, false);
        assert!(
            inactive
                .apis
                .iter()
                .all(|api| api.api_key != ApiKey::InitProducer as u16)
        );
        let inactive_produce = inactive
            .apis
            .iter()
            .find(|api| api.api_key == ApiKey::Produce as u16)
            .unwrap();
        assert_eq!(inactive_produce.min_api_version, 0);
        assert_eq!(inactive_produce.max_api_version, 0);
        assert!(
            inactive
                .features
                .iter()
                .all(|feature| feature.feature_key != 11)
        );

        let active = state.api_versions_response(false, true, false, false);
        let init = active
            .apis
            .iter()
            .find(|api| api.api_key == ApiKey::InitProducer as u16)
            .unwrap();
        assert_eq!((init.min_api_version, init.max_api_version), (0, 0));
        let active_produce = active
            .apis
            .iter()
            .find(|api| api.api_key == ApiKey::Produce as u16)
            .unwrap();
        assert_eq!(
            (
                active_produce.min_api_version,
                active_produce.max_api_version
            ),
            (0, 1)
        );
        let feature = active
            .features
            .iter()
            .find(|feature| feature.feature_key == 11)
            .unwrap();
        assert_eq!(
            (
                feature.min_supported_version,
                feature.max_supported_version,
                feature.active_version,
            ),
            (1, 1, 1)
        );

        let p8_inactive = inactive
            .features
            .iter()
            .find(|feature| feature.feature_key == 14)
            .unwrap();
        assert_eq!(
            (
                p8_inactive.min_supported_version,
                p8_inactive.max_supported_version,
                p8_inactive.active_version,
            ),
            (1, 1, u16::MAX)
        );
        for api_key in [ApiKey::CreateTopic, ApiKey::DescribeTopic] {
            let legacy = inactive
                .apis
                .iter()
                .find(|api| api.api_key == api_key as u16)
                .unwrap();
            assert_eq!((legacy.min_api_version, legacy.max_api_version), (0, 0));
        }

        let p8 = state.api_versions_response(false, true, true, false);
        let feature = p8
            .features
            .iter()
            .find(|feature| feature.feature_key == 14)
            .unwrap();
        assert_eq!(
            (
                feature.min_supported_version,
                feature.max_supported_version,
                feature.active_version,
            ),
            (1, 1, 1)
        );
        for api_key in [ApiKey::CreateTopic, ApiKey::DescribeTopic] {
            let version = p8
                .apis
                .iter()
                .find(|api| api.api_key == api_key as u16)
                .unwrap();
            assert_eq!((version.min_api_version, version.max_api_version), (0, 1));
        }
        assert!(
            p8.apis
                .iter()
                .all(|api| api.api_key != ApiKey::AlterTopicConfig as u16)
        );

        let p9 = state.api_versions_response(false, true, true, true);
        let feature = p9
            .features
            .iter()
            .find(|feature| feature.feature_key == 15)
            .unwrap();
        assert_eq!(
            (
                feature.min_supported_version,
                feature.max_supported_version,
                feature.active_version,
            ),
            (1, 1, 1)
        );
        for api_key in [ApiKey::CreateTopic, ApiKey::DescribeTopic] {
            let version = p9
                .apis
                .iter()
                .find(|api| api.api_key == api_key as u16)
                .unwrap();
            assert_eq!((version.min_api_version, version.max_api_version), (0, 2));
        }
        let alter = p9
            .apis
            .iter()
            .find(|api| api.api_key == ApiKey::AlterTopicConfig as u16)
            .unwrap();
        assert_eq!((alter.min_api_version, alter.max_api_version), (0, 0));
        std::fs::remove_dir_all(dir).unwrap();
    }

    #[test]
    fn p7_init_producer_controller_errors_use_frozen_public_taxonomy() {
        assert_eq!(
            p7_controller_public_error(BrokerError::Controller(ControllerError::Invalid(
                "unknown producer session",
            ))),
            PublicError::UnknownProducer
        );
        assert_eq!(
            p7_controller_public_error(BrokerError::Controller(ControllerError::Invalid(
                "stale producer epoch",
            ))),
            PublicError::ProducerFenced
        );
        assert_eq!(
            p7_controller_public_error(BrokerError::Controller(ControllerError::Invalid(
                "irreversible behavior is not enabled by active cluster feature level",
            ))),
            PublicError::ClusterFeatureTooOld
        );
        assert_eq!(
            p7_controller_public_error(BrokerError::Controller(ControllerError::Busy(
                "bounded controller work",
            ))),
            PublicError::BrokerBusy
        );
        assert_eq!(
            p7_controller_public_error(BrokerError::Controller(ControllerError::NotLeader {
                leader: None,
            })),
            PublicError::BrokerNotAvailable
        );
        assert_eq!(
            p7_controller_public_error(BrokerError::Controller(ControllerError::Busy(
                "controller leadership changed before InitProducer outcome was returned",
            ))),
            PublicError::RequestTimeout
        );
    }

    #[tokio::test]
    async fn committed_fetch_hides_acks_zero_until_a_durable_commit_advances() {
        let dir = temp_dir("commit-boundary");
        let state = BrokerState::open(runtime(dir.clone()), None, None, None).unwrap();
        let created = state.create_topic("orders".into(), 1, 1).await.unwrap();

        let no_ack = ProduceRequestV0 {
            acks: AckMode::None,
            partitions: vec![astralog_protocol::api::ProducePartitionRequestV0 {
                topic_id: created.topic_id,
                partition: 0,
                leader_epoch: UNKNOWN_EPOCH,
                records: vec![WireRecordV0 {
                    timestamp_ms: 10,
                    key: Some(b"a".to_vec()),
                    value: Some(b"one".to_vec()),
                    headers: Vec::new(),
                }],
            }],
            tags: Vec::new(),
        };
        state.produce(no_ack).await;

        let fetch = FetchRequestV0 {
            isolation: astralog_protocol::api::IsolationLevel::ReadCommitted,
            min_bytes: 0,
            max_wait_ms: 0,
            partitions: vec![astralog_protocol::api::FetchPartitionRequestV0 {
                topic_id: created.topic_id,
                partition: 0,
                leader_epoch: UNKNOWN_EPOCH,
                fetch_offset: 0,
                max_bytes: 1024,
                max_records: 10,
            }],
            tags: Vec::new(),
        };
        assert!(
            state.fetch_long_poll(fetch.clone()).await.partitions[0]
                .records
                .is_empty()
        );

        let durable = ProduceRequestV0 {
            acks: AckMode::Quorum,
            partitions: vec![astralog_protocol::api::ProducePartitionRequestV0 {
                topic_id: created.topic_id,
                partition: 0,
                leader_epoch: UNKNOWN_EPOCH,
                records: vec![WireRecordV0 {
                    timestamp_ms: 11,
                    key: Some(b"b".to_vec()),
                    value: Some(b"two".to_vec()),
                    headers: Vec::new(),
                }],
            }],
            tags: Vec::new(),
        };
        state.produce(durable).await;
        let response = state.fetch_long_poll(fetch).await;
        assert_eq!(response.partitions[0].records.len(), 2);
        assert_eq!(response.partitions[0].high_watermark, 2);
        let _ = std::fs::remove_dir_all(dir);
    }

    #[tokio::test]
    async fn p8f_data_change_signal_coalesces_bursts_without_losing_epoch() {
        let signal = Arc::new(DataChangeSignal::default());
        for _ in 0..64 {
            signal.publish();
        }
        assert_eq!(signal.epoch.load(Ordering::Acquire), 64);
        tokio::task::yield_now().await;
        tokio::task::yield_now().await;
        assert_eq!(signal.emitted_wakeups.load(Ordering::Relaxed), 1);

        signal.publish();
        tokio::task::yield_now().await;
        tokio::task::yield_now().await;
        assert_eq!(signal.epoch.load(Ordering::Acquire), 65);
        assert_eq!(signal.emitted_wakeups.load(Ordering::Relaxed), 2);
    }

    #[test]
    fn p8f_move_based_fetch_assembly_is_wire_identical() {
        use astralog_protocol::api::BodyCodec;

        let stored = DataRecord {
            timestamp_ms: 123,
            key: Some(b"key".to_vec()),
            value: Some(b"value".to_vec()),
            headers: vec![UserHeader {
                name: b"header".to_vec(),
                value: Some(b"payload".to_vec()),
            }],
        };
        let reference = WireRecordV0 {
            timestamp_ms: stored.timestamp_ms,
            key: stored.key.clone(),
            value: stored.value.clone(),
            headers: vec![WireHeaderV0 {
                key: "header".to_owned(),
                value: Some(b"payload".to_vec()),
            }],
        };
        let optimized = into_wire_record(stored).unwrap();
        assert_eq!(optimized, reference);

        let response = |record: WireRecordV0| FetchResponseV0 {
            partitions: vec![FetchPartitionResponseV0 {
                topic_id: TopicId::new([0xF1; 16]),
                partition: 3,
                error_code: PublicError::None as u16,
                leader_broker_id: 7,
                leader_epoch: 11,
                log_start_offset: 0,
                high_watermark: 9,
                next_offset: 9,
                records: vec![FetchRecordV0 {
                    stream_offset: 8,
                    record,
                }],
            }],
            tags: Vec::new(),
        };
        assert_eq!(
            response(optimized).encode_body().unwrap(),
            response(reference).encode_body().unwrap()
        );
    }

    #[tokio::test]
    async fn p8f_fetch_byte_budget_stops_read_ahead_without_hiding_high_watermark() {
        let dir = temp_dir("p8f-fetch-byte-budget");
        let state = BrokerState::open(runtime(dir.clone()), None, None, None).unwrap();
        let created = state.create_topic("p8f-fetch".into(), 1, 1).await.unwrap();
        let wire = WireRecordV0 {
            timestamp_ms: 77,
            key: Some(b"k".to_vec()),
            value: Some(vec![0xAB; 128]),
            headers: vec![WireHeaderV0 {
                key: "h".into(),
                value: Some(b"v".to_vec()),
            }],
        };
        state
            .produce(ProduceRequestV0 {
                acks: AckMode::Quorum,
                partitions: vec![astralog_protocol::api::ProducePartitionRequestV0 {
                    topic_id: created.topic_id,
                    partition: 0,
                    leader_epoch: UNKNOWN_EPOCH,
                    records: vec![wire.clone(), wire.clone(), wire.clone(), wire.clone()],
                }],
                tags: Vec::new(),
            })
            .await;

        let one_record_budget = u32::try_from(8 + wire_record_size(&wire)).unwrap();
        let response = state
            .fetch_long_poll(FetchRequestV0 {
                isolation: astralog_protocol::api::IsolationLevel::ReadCommitted,
                min_bytes: 0,
                max_wait_ms: 0,
                partitions: vec![astralog_protocol::api::FetchPartitionRequestV0 {
                    topic_id: created.topic_id,
                    partition: 0,
                    leader_epoch: UNKNOWN_EPOCH,
                    fetch_offset: 0,
                    max_bytes: one_record_budget,
                    max_records: 100,
                }],
                tags: Vec::new(),
            })
            .await;
        let partition = &response.partitions[0];
        assert_eq!(partition.records.len(), 1);
        assert_eq!(partition.next_offset, 1);
        assert_eq!(partition.high_watermark, 4);
        assert!(
            8 + wire_record_size(&partition.records[0].record)
                <= usize::try_from(one_record_budget).unwrap()
        );
        let _ = std::fs::remove_dir_all(dir);
    }
}
