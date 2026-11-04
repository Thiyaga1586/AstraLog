use std::{
    collections::{BTreeMap, BTreeSet},
    net::SocketAddr,
    path::{Path, PathBuf},
    sync::{Arc, RwLock, RwLockReadGuard, RwLockWriteGuard},
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use astralog_common::{PartitionId, ProducerAuthority, ProducerSequence, StreamOffset, TopicId};
use astralog_controller::{
    BrokerLifecycle, CompressionPolicyV1, ControllerState, MovementStage,
    P8_PERFORMANCE_FEATURE_LEVEL, P9_RETENTION_FEATURE_LEVEL, TopicLifecycle,
};
use astralog_groups::is_reserved_internal_topic_name;
use astralog_protocol::{PublicError, UNKNOWN_BROKER_ID, UNKNOWN_EPOCH, api::validate_topic_name};
use astralog_raft::{
    ConfigurationChange, NodeId, ProposalId, Role, StableConfiguration, TimingConfig,
};
use astralog_replication::{
    AckRequirement, CleanProgressReport, CleanProgressTracker, CommandLimits,
    ControllerAuthorityEvidence, GroupKey, IdempotentProduceError, IdempotentProposalDisposition,
    MembershipObservation, PeerDescriptor, PeerInboundFrame, PeerTransportConfig,
    PeerTransportHandle, PeerTransportSender, ProposalOutcome, RebootstrapLimits,
    RebootstrapLimitsV2, ReplicaLimits, ReplicatedPartition, TransportLimits, VoterIdentity,
    decode_rebootstrap_package, decode_rebootstrap_package_v2, encode_rebootstrap_package,
    encode_rebootstrap_package_v2, start_peer_transport,
};
use astralog_storage::{
    CompactionLimits, CompactionOutcome, CompactionPlan, Compression, DataRecord,
    DeleteRetentionPlan, PartitionLogConfig, StoredRecord, TombstoneRemovalGate,
};
use tokio::{
    sync::{broadcast, mpsc, oneshot},
    task::JoinHandle,
    time,
};

use crate::{BrokerError, Result, catalog::TopicDefinition};

const DEFAULT_CONTROL_QUEUE: usize = 1024;
const DEFAULT_CLIENT_QUEUE: usize = 1024;
const DEFAULT_PERSISTENCE_GROUP_TOKENS: usize = 16;
const DEFAULT_PERSISTENCE_GROUP_BYTES: usize = 64 * 1024 * 1024;
const DEFAULT_PERSISTENCE_GROUP_DELAY_MS: u64 = 100;
const P9_PROGRESS_REPORT_MAX_AGE_TICKS: u64 = 10;

/// Controller-committed P5 identities only. Static P4 topology never enables
/// P9-G progress authority. The manager recreates actors on controller refresh.
#[derive(Debug, Clone, PartialEq, Eq)]
struct P9ProgressAuthority {
    assignment_generation: u64,
    /// Stable voters from the *committed P5 assignment*, not simply the P3
    /// effective configuration. During leave-joint the new P3 stable set can
    /// precede controller FinalizeMovement; the old committed P5 voter cannot
    /// be excluded from cleaning authority until that finalization commits.
    committed_p5_voters: BTreeSet<NodeId>,
    /// Committed identity pool: assignment voters plus active committed
    /// movement target identities, selected by the current P3 effective set.
    voter_identities: BTreeMap<NodeId, VoterIdentity>,
    local_incarnation: u64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StaticBrokerConfig {
    pub broker_id: u32,
    pub node_uuid: [u8; 16],
    pub client_host: String,
    pub client_port: u16,
    pub raft_address: SocketAddr,
    pub transport_incarnation: u64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StaticTopicConfig {
    pub name: String,
    pub topic_id: TopicId,
    pub partition_replicas: Vec<Vec<u32>>,
}

impl StaticTopicConfig {
    pub fn partition_count(&self) -> u32 {
        u32::try_from(self.partition_replicas.len()).unwrap_or(u32::MAX)
    }

    pub fn replication_factor(&self) -> Option<u16> {
        self.partition_replicas
            .first()
            .and_then(|replicas| u16::try_from(replicas.len()).ok())
    }
}

#[derive(Debug, Clone)]
pub struct StaticReplicationConfig {
    pub cluster_id: [u8; 16],
    pub raft_bind_addr: SocketAddr,
    pub brokers: Vec<StaticBrokerConfig>,
    pub blocked_broker_ids: BTreeSet<u32>,
    pub topics: Vec<StaticTopicConfig>,
    topic_compression: BTreeMap<TopicId, Compression>,
    actor_brokers: BTreeMap<GroupKey, BTreeSet<u32>>,
    bootstrap_learners: BTreeMap<GroupKey, Vec<u32>>,
    progress_authorities: BTreeMap<GroupKey, P9ProgressAuthority>,
    rebootstrap_v2_enabled: bool,
    pub tick_interval: Duration,
    pub heartbeat_interval_ticks: u64,
    pub election_timeout_min_ticks: u64,
    pub election_timeout_max_ticks: u64,
    pub max_control_queue: usize,
    pub max_client_queue: usize,
    pub transport_limits: TransportLimits,
    pub max_transport_inbound_frames: usize,
    pub max_transport_outbound_frames_per_peer: usize,
    pub max_transport_outbound_bytes_per_peer: usize,
    pub reconnect_min_delay: Duration,
    pub reconnect_max_delay: Duration,
    pub replica_limits: ReplicaLimits,
    pub command_limits: CommandLimits,
    pub rebootstrap_limits: RebootstrapLimits,
    pub partition_log: PartitionLogConfig,
}

#[derive(Debug, Clone)]
pub struct DynamicReplicationConfig {
    pub raft_bind_addr: SocketAddr,
    pub tick_interval: Duration,
    pub heartbeat_interval_ticks: u64,
    pub election_timeout_min_ticks: u64,
    pub election_timeout_max_ticks: u64,
    pub max_control_queue: usize,
    pub max_client_queue: usize,
    pub transport_limits: TransportLimits,
    pub max_transport_inbound_frames: usize,
    pub max_transport_outbound_frames_per_peer: usize,
    pub max_transport_outbound_bytes_per_peer: usize,
    pub reconnect_min_delay: Duration,
    pub reconnect_max_delay: Duration,
    pub replica_limits: ReplicaLimits,
    pub command_limits: CommandLimits,
    pub rebootstrap_limits: RebootstrapLimits,
    pub partition_log: PartitionLogConfig,
    /// Explicit homogeneous P9-G rollout opt-in. Disabled until controller
    /// feature activation and peer capability have been independently checked.
    /// Older P9-F peers cannot decode the new transport sideband message kind.
    pub enable_p9g_progress_reports: bool,
    /// Explicit homogeneous V2-install capability. Disabled by default.
    pub enable_p9g_rebootstrap_v2: bool,
}

impl DynamicReplicationConfig {
    pub fn new(raft_bind_addr: SocketAddr) -> Self {
        let defaults =
            StaticReplicationConfig::new([1; 16], raft_bind_addr, Vec::new(), Vec::new());
        Self {
            raft_bind_addr,
            tick_interval: defaults.tick_interval,
            heartbeat_interval_ticks: defaults.heartbeat_interval_ticks,
            election_timeout_min_ticks: defaults.election_timeout_min_ticks,
            election_timeout_max_ticks: defaults.election_timeout_max_ticks,
            max_control_queue: defaults.max_control_queue,
            max_client_queue: defaults.max_client_queue,
            transport_limits: defaults.transport_limits,
            max_transport_inbound_frames: defaults.max_transport_inbound_frames,
            max_transport_outbound_frames_per_peer: defaults.max_transport_outbound_frames_per_peer,
            max_transport_outbound_bytes_per_peer: defaults.max_transport_outbound_bytes_per_peer,
            reconnect_min_delay: defaults.reconnect_min_delay,
            reconnect_max_delay: defaults.reconnect_max_delay,
            replica_limits: defaults.replica_limits,
            command_limits: defaults.command_limits,
            rebootstrap_limits: defaults.rebootstrap_limits,
            partition_log: defaults.partition_log,
            enable_p9g_progress_reports: false,
            enable_p9g_rebootstrap_v2: false,
        }
    }
}

pub(crate) struct DynamicReplicationManager {
    config: DynamicReplicationConfig,
    data_dir: PathBuf,
    local_broker_id: u32,
    advertised_host: String,
    advertised_port: u16,
    client: ReplicationClient,
    runtime: Option<ReplicationRuntimeHandle>,
}

impl DynamicReplicationManager {
    pub(crate) async fn start(
        config: DynamicReplicationConfig,
        data_dir: PathBuf,
        local_broker_id: u32,
        advertised_host: &str,
        advertised_port: u16,
        state: &ControllerState,
    ) -> Result<Self> {
        let topology = build_dynamic_topology(
            &config,
            state,
            local_broker_id,
            advertised_host,
            advertised_port,
        )
        .await?;
        let (client, runtime) = start_controller_replication(
            topology,
            data_dir.clone(),
            local_broker_id,
            advertised_host,
            advertised_port,
        )
        .await?;
        Ok(Self {
            config,
            data_dir,
            local_broker_id,
            advertised_host: advertised_host.to_owned(),
            advertised_port,
            client,
            runtime: Some(runtime),
        })
    }

    pub(crate) fn client(&self) -> ReplicationClient {
        self.client.clone()
    }

    pub(crate) async fn reconcile(&mut self, state: &ControllerState) -> Result<bool> {
        let next = build_dynamic_topology(
            &self.config,
            state,
            self.local_broker_id,
            &self.advertised_host,
            self.advertised_port,
        )
        .await?;
        let current = self.client.topology_snapshot();
        if current.cluster_id == next.cluster_id
            && current.brokers == next.brokers
            && current.blocked_broker_ids == next.blocked_broker_ids
            && current.topics == next.topics
            && current.topic_compression == next.topic_compression
            && current.actor_brokers == next.actor_brokers
            && current.bootstrap_learners == next.bootstrap_learners
            && current.progress_authorities == next.progress_authorities
            && current.rebootstrap_v2_enabled == next.rebootstrap_v2_enabled
        {
            return Ok(false);
        }

        // Publishing the final controller assignment after P3 has already reached
        // the matching stable configuration changes metadata routing, not the live
        // actor/transport set. Preserve those actors (and any requested preferred
        // leader) instead of forcing a needless election by rebuilding the runtime.
        let current_local_groups = local_groups(&current, self.local_broker_id)?;
        let next_local_groups = local_groups(&next, self.local_broker_id)?;
        if current.cluster_id == next.cluster_id
            && current.brokers == next.brokers
            && current.blocked_broker_ids == next.blocked_broker_ids
            && current.topic_compression == next.topic_compression
            && current_local_groups == next_local_groups
            && current.progress_authorities == next.progress_authorities
            && current.rebootstrap_v2_enabled == next.rebootstrap_v2_enabled
        {
            next.validate_for_broker_impl(
                self.local_broker_id,
                &self.advertised_host,
                self.advertised_port,
                true,
            )?;
            self.client.replace_topology(next);
            return Ok(true);
        }

        let previous = (*current).clone();
        if let Some(runtime) = self.runtime.take() {
            runtime.shutdown().await;
        }
        match start_controller_replication(
            next,
            self.data_dir.clone(),
            self.local_broker_id,
            &self.advertised_host,
            self.advertised_port,
        )
        .await
        {
            Ok((replacement, runtime)) => {
                self.client.replace_routing_from(&replacement)?;
                self.runtime = Some(runtime);
                Ok(true)
            }
            Err(error) => {
                if let Ok((replacement, runtime)) = start_controller_replication(
                    previous,
                    self.data_dir.clone(),
                    self.local_broker_id,
                    &self.advertised_host,
                    self.advertised_port,
                )
                .await
                {
                    let _ = self.client.replace_routing_from(&replacement);
                    self.runtime = Some(runtime);
                }
                Err(error)
            }
        }
    }

    pub(crate) async fn shutdown(&mut self) {
        if let Some(runtime) = self.runtime.take() {
            runtime.shutdown().await;
        }
    }
}

async fn build_dynamic_topology(
    config: &DynamicReplicationConfig,
    state: &ControllerState,
    local_broker_id: u32,
    advertised_host: &str,
    advertised_port: u16,
) -> Result<StaticReplicationConfig> {
    let mut brokers = Vec::with_capacity(state.brokers().len());
    let mut blocked_broker_ids = BTreeSet::new();
    for broker in state.brokers().values() {
        if matches!(
            broker.lifecycle,
            BrokerLifecycle::Fenced | BrokerLifecycle::Removed
        ) {
            blocked_broker_ids.insert(broker.broker_id);
        }
        let raft_address = if broker.broker_id == local_broker_id {
            if broker.client_host != advertised_host || broker.client_port != advertised_port {
                return Err(BrokerError::InvalidConfig(
                    "local advertised endpoint differs from committed controller broker record",
                ));
            }
            let committed =
                resolve_internal_endpoint(&broker.internal_host, broker.internal_port).await?;
            if committed != config.raft_bind_addr {
                return Err(BrokerError::InvalidConfig(
                    "local Raft bind address differs from committed controller endpoint",
                ));
            }
            committed
        } else {
            resolve_internal_endpoint(&broker.internal_host, broker.internal_port).await?
        };
        brokers.push(StaticBrokerConfig {
            broker_id: broker.broker_id,
            node_uuid: broker.node_uuid,
            client_host: broker.client_host.clone(),
            client_port: broker.client_port,
            raft_address,
            transport_incarnation: broker.incarnation_epoch,
        });
    }

    let mut topics = Vec::with_capacity(state.topics().len());
    let mut topic_compression = BTreeMap::new();
    let mut actor_brokers = BTreeMap::new();
    let mut bootstrap_learners = BTreeMap::new();
    let mut progress_authorities = BTreeMap::new();
    for topic in state.topics().values() {
        if topic.lifecycle != TopicLifecycle::Active && topic.lifecycle != TopicLifecycle::Deleting
        {
            continue;
        }
        let mut partition_replicas = Vec::with_capacity(topic.partition_count as usize);
        for partition in 0..topic.partition_count {
            let assignment = state
                .assignments()
                .get(&(topic.topic_id, partition))
                .ok_or(BrokerError::InvalidConfig(
                    "controller topic is missing a committed partition assignment",
                ))?;
            let group = GroupKey {
                topic_id: topic.topic_id,
                partition,
            };
            let mut eligible = BTreeMap::new();
            for broker_id in &assignment.voters {
                let broker = state
                    .brokers()
                    .get(broker_id)
                    .ok_or(BrokerError::InvalidConfig(
                        "P9 progress authority missing committed P5 broker",
                    ))?;
                eligible.insert(
                    broker_to_node(*broker_id)?,
                    VoterIdentity {
                        broker_id: *broker_id,
                        incarnation: broker.incarnation_epoch,
                    },
                );
            }
            // A committed P5 reassignment can put Raft into joint
            // consensus while the stable controller assignment still lists
            // the old voters. Keep a *pool* of identities from the committed
            // movement intent; the current effective P3 configuration below
            // selects the exact voter union. The destination is not treated
            // as eligible while it remains merely a learner.
            if let Some(movement) = state.movements().values().find(|movement| {
                movement.topic_id == topic.topic_id
                    && movement.partition_id == partition
                    && movement.stage != MovementStage::Complete
            }) {
                for broker_id in &movement.target_voters {
                    let broker =
                        state
                            .brokers()
                            .get(broker_id)
                            .ok_or(BrokerError::InvalidConfig(
                                "P9 movement target has no committed P5 broker identity",
                            ))?;
                    eligible.insert(
                        broker_to_node(*broker_id)?,
                        VoterIdentity {
                            broker_id: *broker_id,
                            incarnation: broker.incarnation_epoch,
                        },
                    );
                }
            }
            let local_incarnation = state
                .brokers()
                .get(&local_broker_id)
                .map_or(0, |broker| broker.incarnation_epoch);
            if config.enable_p9g_progress_reports
                && state.active_feature_level() >= P9_RETENTION_FEATURE_LEVEL
                && local_incarnation > 0
            {
                let committed_p5_voters = assignment
                    .voters
                    .iter()
                    .copied()
                    .map(broker_to_node)
                    .collect::<Result<BTreeSet<_>>>()?;
                progress_authorities.insert(
                    group,
                    P9ProgressAuthority {
                        assignment_generation: assignment.assignment_generation.get(),
                        committed_p5_voters,
                        voter_identities: eligible,
                        local_incarnation,
                    },
                );
            }
            let mut actors = assignment
                .voters
                .iter()
                .chain(&assignment.learners)
                .copied()
                .collect::<BTreeSet<_>>();
            if let Some(movement) = state.movements().values().find(|movement| {
                movement.topic_id == topic.topic_id
                    && movement.partition_id == partition
                    && movement.stage != MovementStage::Complete
            }) {
                match movement.stage {
                    MovementStage::Planned => {}
                    MovementStage::LearnerRequested
                    | MovementStage::LearnerCreated
                    | MovementStage::CatchingUp
                    | MovementStage::PromotionReady
                    | MovementStage::EnterJointRequested
                    | MovementStage::JointCommitted
                    | MovementStage::LeaveJointRequested
                    | MovementStage::NewStableCommitted
                    | MovementStage::LeadershipTransfer => {
                        actors.insert(movement.destination_broker_id);
                    }
                    MovementStage::OldReplicaRemoveRequested | MovementStage::OldReplicaRemoved => {
                        actors = movement.target_voters.iter().copied().collect();
                    }
                    MovementStage::Complete => unreachable!("complete movement filtered above"),
                }
            }
            actor_brokers.insert(group, actors);
            bootstrap_learners.insert(group, assignment.learners.clone());
            partition_replicas.push(assignment.voters.clone());
        }
        topics.push(StaticTopicConfig {
            name: topic.topic_name.clone(),
            topic_id: topic.topic_id,
            partition_replicas,
        });
        if state.active_feature_level() >= P8_PERFORMANCE_FEATURE_LEVEL {
            let policy = state.topic_performance_policy(topic.topic_id).ok_or(
                BrokerError::InvalidConfig(
                    "active controller topic is missing P8 performance policy",
                ),
            )?;
            let compression = match policy.compression {
                CompressionPolicyV1::None => Compression::None,
                CompressionPolicyV1::Lz4 => Compression::Lz4,
                CompressionPolicyV1::Zstd => Compression::Zstd,
            };
            topic_compression.insert(topic.topic_id, compression);
        }
    }

    let mut topology =
        StaticReplicationConfig::new(*state.cluster_id(), config.raft_bind_addr, brokers, topics);
    topology.blocked_broker_ids = blocked_broker_ids;
    topology.topic_compression = topic_compression;
    topology.actor_brokers = actor_brokers;
    topology.bootstrap_learners = bootstrap_learners;
    topology.progress_authorities = progress_authorities;
    topology.rebootstrap_v2_enabled = config.enable_p9g_rebootstrap_v2
        && state.active_feature_level() >= P9_RETENTION_FEATURE_LEVEL
        && state
            .brokers()
            .values()
            .all(|broker| broker.max_feature >= P9_RETENTION_FEATURE_LEVEL);
    topology.tick_interval = config.tick_interval;
    topology.heartbeat_interval_ticks = config.heartbeat_interval_ticks;
    topology.election_timeout_min_ticks = config.election_timeout_min_ticks;
    topology.election_timeout_max_ticks = config.election_timeout_max_ticks;
    topology.max_control_queue = config.max_control_queue;
    topology.max_client_queue = config.max_client_queue;
    topology.transport_limits = config.transport_limits;
    topology.max_transport_inbound_frames = config.max_transport_inbound_frames;
    topology.max_transport_outbound_frames_per_peer = config.max_transport_outbound_frames_per_peer;
    topology.max_transport_outbound_bytes_per_peer = config.max_transport_outbound_bytes_per_peer;
    topology.reconnect_min_delay = config.reconnect_min_delay;
    topology.reconnect_max_delay = config.reconnect_max_delay;
    topology.replica_limits = config.replica_limits;
    topology.command_limits = config.command_limits;
    topology.rebootstrap_limits = config.rebootstrap_limits;
    topology.partition_log = config.partition_log;
    topology.validate_for_broker_impl(local_broker_id, advertised_host, advertised_port, true)?;
    Ok(topology)
}

async fn resolve_internal_endpoint(host: &str, port: u16) -> Result<SocketAddr> {
    if host.is_empty() || port == 0 {
        return Err(BrokerError::InvalidConfig(
            "committed internal broker endpoint is incomplete",
        ));
    }
    let mut addresses = tokio::net::lookup_host((host, port))
        .await?
        .collect::<Vec<_>>();
    addresses.sort_unstable();
    addresses.dedup();
    addresses
        .into_iter()
        .next()
        .ok_or(BrokerError::InvalidConfig(
            "committed internal broker endpoint resolved to no addresses",
        ))
}

impl StaticReplicationConfig {
    pub fn new(
        cluster_id: [u8; 16],
        raft_bind_addr: SocketAddr,
        brokers: Vec<StaticBrokerConfig>,
        topics: Vec<StaticTopicConfig>,
    ) -> Self {
        Self {
            cluster_id,
            raft_bind_addr,
            brokers,
            blocked_broker_ids: BTreeSet::new(),
            topics,
            topic_compression: BTreeMap::new(),
            actor_brokers: BTreeMap::new(),
            bootstrap_learners: BTreeMap::new(),
            progress_authorities: BTreeMap::new(),
            rebootstrap_v2_enabled: false,
            tick_interval: Duration::from_millis(100),
            heartbeat_interval_ticks: 1,
            election_timeout_min_ticks: 5,
            election_timeout_max_ticks: 10,
            max_control_queue: DEFAULT_CONTROL_QUEUE,
            max_client_queue: DEFAULT_CLIENT_QUEUE,
            transport_limits: TransportLimits::default(),
            max_transport_inbound_frames: 4096,
            max_transport_outbound_frames_per_peer: 4096,
            max_transport_outbound_bytes_per_peer: 64 * 1024 * 1024,
            reconnect_min_delay: Duration::from_millis(25),
            reconnect_max_delay: Duration::from_secs(1),
            replica_limits: ReplicaLimits {
                max_pending_persistence_batches: DEFAULT_PERSISTENCE_GROUP_TOKENS,
                max_pending_persistence_bytes: DEFAULT_PERSISTENCE_GROUP_BYTES,
                max_persistence_group_delay_ms: DEFAULT_PERSISTENCE_GROUP_DELAY_MS,
                ..ReplicaLimits::default()
            },
            command_limits: CommandLimits::default(),
            rebootstrap_limits: RebootstrapLimits::default(),
            partition_log: PartitionLogConfig::default(),
        }
    }

    pub fn validate_for_broker(
        &self,
        local_broker_id: u32,
        advertised_host: &str,
        advertised_port: u16,
    ) -> Result<()> {
        self.validate_for_broker_impl(local_broker_id, advertised_host, advertised_port, false)
    }

    fn validate_for_broker_impl(
        &self,
        local_broker_id: u32,
        advertised_host: &str,
        advertised_port: u16,
        allow_reserved_internal_topics: bool,
    ) -> Result<()> {
        if self.cluster_id == [0; 16] {
            return Err(BrokerError::InvalidConfig(
                "static replication cluster_id must be non-zero",
            ));
        }
        if self.tick_interval.is_zero() || self.max_control_queue == 0 || self.max_client_queue == 0
        {
            return Err(BrokerError::InvalidConfig(
                "static replication runtime bounds must be non-zero",
            ));
        }
        if self.tick_interval.as_millis()
            > u128::from(self.replica_limits.max_persistence_group_delay_ms)
        {
            return Err(BrokerError::InvalidConfig(
                "replication tick interval exceeds persistence group-delay bound",
            ));
        }
        TimingConfig::new(
            self.heartbeat_interval_ticks,
            self.election_timeout_min_ticks,
            self.election_timeout_max_ticks,
        )?;
        let mut broker_ids = BTreeSet::new();
        let mut node_uuids = BTreeSet::new();
        let mut raft_addresses = BTreeSet::new();
        for broker in &self.brokers {
            if broker.broker_id == u32::MAX {
                return Err(BrokerError::InvalidConfig(
                    "u32::MAX broker id is reserved by P4 NodeId mapping",
                ));
            }
            if broker.node_uuid == [0; 16]
                || broker.client_host.is_empty()
                || broker.client_port == 0
                || broker.transport_incarnation == 0
            {
                return Err(BrokerError::InvalidConfig(
                    "static broker identity/address is incomplete",
                ));
            }
            if !broker_ids.insert(broker.broker_id)
                || !node_uuids.insert(broker.node_uuid)
                || !raft_addresses.insert(broker.raft_address)
            {
                return Err(BrokerError::InvalidConfig(
                    "static broker ids, node UUIDs, and Raft addresses must be unique",
                ));
            }
        }
        if self
            .blocked_broker_ids
            .iter()
            .any(|broker_id| !broker_ids.contains(broker_id))
        {
            return Err(BrokerError::InvalidConfig(
                "blocked broker id is absent from replication topology",
            ));
        }
        let local = self
            .brokers
            .iter()
            .find(|broker| broker.broker_id == local_broker_id)
            .ok_or(BrokerError::InvalidConfig(
                "local broker id is absent from static replication topology",
            ))?;
        if local.client_host != advertised_host || local.client_port != advertised_port {
            return Err(BrokerError::InvalidConfig(
                "local advertised client endpoint does not match static topology",
            ));
        }
        if local.raft_address != self.raft_bind_addr {
            return Err(BrokerError::InvalidConfig(
                "local Raft bind address must match static topology Raft address",
            ));
        }

        let mut names = BTreeSet::new();
        let mut topic_ids = BTreeSet::new();
        for topic in &self.topics {
            let valid_name = validate_topic_name(&topic.name).is_ok()
                || (allow_reserved_internal_topics && is_reserved_internal_topic_name(&topic.name));
            if !valid_name || topic.partition_replicas.is_empty() {
                return Err(BrokerError::InvalidConfig(
                    "static topic name/partition definition is invalid",
                ));
            }
            if !names.insert(topic.name.clone()) || !topic_ids.insert(topic.topic_id) {
                return Err(BrokerError::InvalidConfig(
                    "static topic names and ids must be unique",
                ));
            }
            let expected_rf = topic.partition_replicas[0].len();
            if !matches!(expected_rf, 1 | 3 | 5) {
                return Err(BrokerError::InvalidConfig(
                    "P4 static replication factor must be 1, 3, or 5",
                ));
            }
            for replicas in &topic.partition_replicas {
                if replicas.len() != expected_rf {
                    return Err(BrokerError::InvalidConfig(
                        "all partitions in one static topic must share replication factor",
                    ));
                }
                let mut seen = BTreeSet::new();
                for broker_id in replicas {
                    if !broker_ids.contains(broker_id) || !seen.insert(*broker_id) {
                        return Err(BrokerError::InvalidConfig(
                            "static partition replica list references unknown/duplicate broker",
                        ));
                    }
                }
            }
        }
        Ok(())
    }

    pub fn local_broker(&self, broker_id: u32) -> Option<&StaticBrokerConfig> {
        self.brokers
            .iter()
            .find(|broker| broker.broker_id == broker_id)
    }

    pub fn topic_by_id(&self, topic_id: TopicId) -> Option<&StaticTopicConfig> {
        self.topics.iter().find(|topic| topic.topic_id == topic_id)
    }

    pub fn topic_by_name(&self, name: &str) -> Option<&StaticTopicConfig> {
        self.topics.iter().find(|topic| topic.name == name)
    }

    pub fn topic_definition(&self, topic: &StaticTopicConfig) -> Result<TopicDefinition> {
        Ok(TopicDefinition {
            topic_id: topic.topic_id,
            name: topic.name.clone(),
            partition_count: u32::try_from(topic.partition_replicas.len())
                .map_err(|_| BrokerError::InvalidConfig("static partition count overflow"))?,
            replication_factor: topic
                .replication_factor()
                .ok_or(BrokerError::InvalidConfig(
                    "static replication factor overflow",
                ))?,
        })
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct PartitionStatus {
    pub role: Role,
    pub leader_broker_id: Option<u32>,
    pub leader_epoch: u64,
    /// False for a newly elected leader until its current-term barrier commits.
    pub current_term_committed: bool,
    pub log_start_offset: u64,
    pub high_watermark: u64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct ProduceResult {
    pub base_offset: u64,
    pub last_offset: u64,
    pub raft_index: u64,
    pub leader_broker_id: u32,
    pub leader_epoch: u64,
}

#[derive(Debug, Clone)]
pub(crate) struct FetchResult {
    pub records: Vec<StoredRecord>,
    pub log_start_offset: u64,
    pub high_watermark: u64,
    pub leader_broker_id: u32,
    pub leader_epoch: u64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct RetentionPlanResult {
    pub plan: DeleteRetentionPlan,
    pub leader_epoch: u64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct CompactionPlanResult {
    pub plan: CompactionPlan,
    pub leader_epoch: u64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct CompactionRunResult {
    pub outcome: CompactionOutcome,
    pub leader_broker_id: u32,
    pub leader_epoch: u64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RetentionAdvanceResult {
    pub log_start_offset: u64,
    pub reclaimed_segments: usize,
    pub physical_cleanup_deferred: bool,
    pub leader_broker_id: u32,
    pub leader_epoch: u64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct ListOffsetResult {
    pub offset: u64,
    pub timestamp_ms: i64,
    pub leader_broker_id: u32,
    pub leader_epoch: u64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct RoutingError {
    pub error: PublicError,
    pub leader_broker_id: u32,
    pub leader_epoch: u64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum IdempotentProduceAttemptError {
    ControllerValidationRequired,
    Routing(RoutingError),
}

impl RoutingError {
    fn from_status(error: PublicError, status: PartitionStatus) -> Self {
        Self {
            error,
            leader_broker_id: status.leader_broker_id.unwrap_or(UNKNOWN_BROKER_ID),
            leader_epoch: if status.leader_epoch == 0 {
                UNKNOWN_EPOCH
            } else {
                status.leader_epoch
            },
        }
    }

    fn unavailable(error: PublicError) -> Self {
        Self {
            error,
            leader_broker_id: UNKNOWN_BROKER_ID,
            leader_epoch: UNKNOWN_EPOCH,
        }
    }
}

#[derive(Clone)]
struct PartitionActorHandle {
    control: mpsc::Sender<ActorControl>,
    client: mpsc::Sender<ActorClient>,
}

#[derive(Clone)]
pub(crate) struct ReplicationClient {
    local_broker_id: u32,
    routing: Arc<RwLock<ReplicationRouting>>,
}

#[derive(Clone)]
struct ReplicationRouting {
    topology: Arc<StaticReplicationConfig>,
    actors: Arc<BTreeMap<GroupKey, PartitionActorHandle>>,
}

pub(crate) struct ReplicationRuntimeHandle {
    shutdown: broadcast::Sender<()>,
    transport: PeerTransportHandle,
    tasks: Vec<JoinHandle<()>>,
}

impl ReplicationRuntimeHandle {
    pub(crate) async fn shutdown(self) {
        let _ = self.shutdown.send(());
        for task in self.tasks {
            let _ = task.await;
        }
        self.transport.shutdown().await;
    }
}

enum ActorControl {
    Tick {
        now_ms: i64,
    },
    Inbound {
        source: NodeId,
        source_incarnation: u64,
        connection_generation: u64,
        message: Option<Box<astralog_raft::RaftMessage>>,
        rebootstrap: Option<Vec<u8>>,
        clean_progress: Option<CleanProgressReport>,
    },
}

enum ActorClient {
    Status {
        response: oneshot::Sender<PartitionStatus>,
    },
    MembershipStatus {
        response: oneshot::Sender<MembershipObservation>,
    },
    Reconfigure {
        change: ConfigurationChange,
        response: oneshot::Sender<Result<bool>>,
    },
    TransferLeadership {
        target: NodeId,
        response: oneshot::Sender<Result<()>>,
    },
    PersistProducerSnapshot {
        response: oneshot::Sender<Result<()>>,
    },
    CheckpointProducerState {
        expected_epoch: u64,
        response: oneshot::Sender<std::result::Result<Option<u64>, RoutingError>>,
    },
    SnapshotCommittedForRetention {
        expected_epoch: u64,
        response: oneshot::Sender<std::result::Result<(), RoutingError>>,
    },
    PlanDeleteRetention {
        expected_epoch: u64,
        now_ms: i64,
        retention_ms: Option<u64>,
        retention_bytes: Option<u64>,
        protected_log_start_ceiling: Option<StreamOffset>,
        response: oneshot::Sender<std::result::Result<RetentionPlanResult, RoutingError>>,
    },
    AdvanceLogStart {
        expected_epoch: u64,
        expected_manifest_generation: u64,
        new_log_start_offset: StreamOffset,
        response: oneshot::Sender<std::result::Result<RetentionAdvanceResult, RoutingError>>,
    },
    PlanKeyedCompaction {
        expected_epoch: u64,
        policy_epoch: u64,
        limits: CompactionLimits,
        tombstone_removal: Option<TombstoneRemovalGate>,
        response: oneshot::Sender<std::result::Result<CompactionPlanResult, RoutingError>>,
    },
    ExecuteKeyedCompaction {
        expected_epoch: u64,
        plan: CompactionPlan,
        current_policy_epoch: u64,
        publication_metadata_epoch: Arc<RwLock<u64>>,
        response: oneshot::Sender<std::result::Result<CompactionRunResult, RoutingError>>,
    },
    Produce {
        expected_epoch: Option<u64>,
        records: Vec<DataRecord>,
        ack: AckRequirement,
        response: oneshot::Sender<std::result::Result<ProduceResult, RoutingError>>,
    },
    ProduceIdempotent {
        expected_epoch: u64,
        authority: ProducerAuthority,
        base_sequence: ProducerSequence,
        records: Vec<DataRecord>,
        controller_evidence: ControllerAuthorityEvidence,
        response:
            oneshot::Sender<std::result::Result<ProduceResult, IdempotentProduceAttemptError>>,
    },
    Fetch {
        expected_epoch: Option<u64>,
        start: StreamOffset,
        max_records: usize,
        max_bytes: usize,
        response: oneshot::Sender<std::result::Result<FetchResult, RoutingError>>,
    },
    ListOffset {
        expected_epoch: Option<u64>,
        timestamp_selector: i64,
        response: oneshot::Sender<std::result::Result<ListOffsetResult, RoutingError>>,
    },
}

struct PendingProduce {
    response: oneshot::Sender<std::result::Result<ProduceResult, RoutingError>>,
    base_offset: u64,
    last_offset: u64,
    raft_index: u64,
    leader_epoch: u64,
}

struct PendingIdempotentProduce {
    responses:
        Vec<oneshot::Sender<std::result::Result<ProduceResult, IdempotentProduceAttemptError>>>,
    base_offset: u64,
    last_offset: u64,
    raft_index: u64,
    leader_epoch: u64,
}

struct PendingRetentionAdvance {
    response: oneshot::Sender<std::result::Result<RetentionAdvanceResult, RoutingError>>,
    expected_manifest_generation: u64,
    requested_log_start_offset: StreamOffset,
    leader_epoch: u64,
}

struct PendingProducerCheckpoint {
    response: oneshot::Sender<std::result::Result<Option<u64>, RoutingError>>,
    raft_index: u64,
}

impl ReplicationClient {
    pub(crate) fn topology_snapshot(&self) -> Arc<StaticReplicationConfig> {
        self.routing_read().topology.clone()
    }

    pub(crate) fn replace_routing_from(&self, replacement: &Self) -> Result<()> {
        if self.local_broker_id != replacement.local_broker_id {
            return Err(BrokerError::InvalidConfig(
                "cannot replace replication routing from a different local broker",
            ));
        }
        let next = replacement.routing_read().clone();
        *self.routing_write() = next;
        Ok(())
    }

    fn replace_topology(&self, topology: StaticReplicationConfig) {
        self.routing_write().topology = Arc::new(topology);
    }

    pub(crate) fn contains_group(&self, topic_id: TopicId, partition: u32) -> bool {
        self.routing_read()
            .topology
            .topic_by_id(topic_id)
            .is_some_and(|topic| {
                usize::try_from(partition)
                    .ok()
                    .is_some_and(|index| index < topic.partition_replicas.len())
            })
    }

    pub(crate) fn is_local_replica(&self, topic_id: TopicId, partition: u32) -> bool {
        self.routing_read().actors.contains_key(&GroupKey {
            topic_id,
            partition,
        })
    }

    pub(crate) fn replicas(&self, topic_id: TopicId, partition: u32) -> Option<Vec<u32>> {
        let routing = self.routing_read();
        let topic = routing.topology.topic_by_id(topic_id)?;
        let index = usize::try_from(partition).ok()?;
        topic.partition_replicas.get(index).cloned()
    }

    pub(crate) async fn status(
        &self,
        topic_id: TopicId,
        partition: u32,
    ) -> Option<PartitionStatus> {
        let actor = self
            .routing_read()
            .actors
            .get(&GroupKey {
                topic_id,
                partition,
            })
            .cloned()?;
        let (response, receiver) = oneshot::channel();
        actor
            .client
            .try_send(ActorClient::Status { response })
            .ok()?;
        receiver.await.ok()
    }

    pub(crate) async fn membership_status(
        &self,
        topic_id: TopicId,
        partition: u32,
    ) -> Option<MembershipObservation> {
        let actor = self
            .routing_read()
            .actors
            .get(&GroupKey {
                topic_id,
                partition,
            })
            .cloned()?;
        let (response, receiver) = oneshot::channel();
        actor
            .client
            .try_send(ActorClient::MembershipStatus { response })
            .ok()?;
        receiver.await.ok()
    }

    pub(crate) async fn reconfigure(
        &self,
        topic_id: TopicId,
        partition: u32,
        change: ConfigurationChange,
    ) -> Result<bool> {
        let actor = self
            .routing_read()
            .actors
            .get(&GroupKey {
                topic_id,
                partition,
            })
            .cloned()
            .ok_or(BrokerError::InvalidConfig(
                "local partition actor is unavailable for reconfiguration",
            ))?;
        let (response, receiver) = oneshot::channel();
        actor
            .client
            .try_send(ActorClient::Reconfigure { change, response })
            .map_err(|_| BrokerError::InvalidConfig("partition control queue is full"))?;
        receiver
            .await
            .map_err(|_| BrokerError::InvalidConfig("partition reconfiguration response lost"))?
    }

    pub(crate) async fn transfer_leadership(
        &self,
        topic_id: TopicId,
        partition: u32,
        target_broker_id: u32,
    ) -> Result<()> {
        let actor = self
            .routing_read()
            .actors
            .get(&GroupKey {
                topic_id,
                partition,
            })
            .cloned()
            .ok_or(BrokerError::InvalidConfig(
                "local partition actor is unavailable for leadership transfer",
            ))?;
        let target = broker_to_node(target_broker_id)?;
        let (response, receiver) = oneshot::channel();
        actor
            .client
            .try_send(ActorClient::TransferLeadership { target, response })
            .map_err(|_| BrokerError::InvalidConfig("partition control queue is full"))?;
        receiver
            .await
            .map_err(|_| BrokerError::InvalidConfig("leadership transfer response lost"))?
    }

    pub(crate) async fn persist_producer_snapshot(
        &self,
        topic_id: TopicId,
        partition: u32,
    ) -> Result<()> {
        let actor = self
            .routing_read()
            .actors
            .get(&GroupKey {
                topic_id,
                partition,
            })
            .cloned()
            .ok_or(BrokerError::InvalidConfig(
                "local partition actor is unavailable for producer snapshot checkpoint",
            ))?;
        let (response, receiver) = oneshot::channel();
        actor
            .client
            .try_send(ActorClient::PersistProducerSnapshot { response })
            .map_err(|_| BrokerError::InvalidConfig("partition control queue is full"))?;
        receiver
            .await
            .map_err(|_| BrokerError::InvalidConfig("producer snapshot checkpoint response lost"))?
    }

    pub(crate) async fn checkpoint_producer_state(
        &self,
        topic_id: TopicId,
        partition: u32,
        expected_epoch: u64,
    ) -> std::result::Result<Option<u64>, RoutingError> {
        let actor = self
            .routing_read()
            .actors
            .get(&GroupKey {
                topic_id,
                partition,
            })
            .cloned()
            .ok_or_else(|| self.not_local_routing_error(topic_id, partition))?;
        let (response, receiver) = oneshot::channel();
        actor
            .client
            .try_send(ActorClient::CheckpointProducerState {
                expected_epoch,
                response,
            })
            .map_err(|_| RoutingError::unavailable(PublicError::BrokerBusy))?;
        receiver
            .await
            .unwrap_or_else(|_| Err(RoutingError::unavailable(PublicError::BrokerNotAvailable)))
    }

    #[allow(clippy::too_many_arguments)]
    pub(crate) async fn plan_delete_retention(
        &self,
        topic_id: TopicId,
        partition: u32,
        expected_epoch: u64,
        now_ms: i64,
        retention_ms: Option<u64>,
        retention_bytes: Option<u64>,
        protected_log_start_ceiling: Option<StreamOffset>,
    ) -> std::result::Result<RetentionPlanResult, RoutingError> {
        let actor = self
            .routing_read()
            .actors
            .get(&GroupKey {
                topic_id,
                partition,
            })
            .cloned()
            .ok_or_else(|| self.not_local_routing_error(topic_id, partition))?;
        let (response, receiver) = oneshot::channel();
        actor
            .client
            .try_send(ActorClient::PlanDeleteRetention {
                expected_epoch,
                now_ms,
                retention_ms,
                retention_bytes,
                protected_log_start_ceiling,
                response,
            })
            .map_err(|_| RoutingError::unavailable(PublicError::BrokerBusy))?;
        receiver
            .await
            .unwrap_or_else(|_| Err(RoutingError::unavailable(PublicError::BrokerNotAvailable)))
    }

    pub(crate) async fn snapshot_committed_for_retention(
        &self,
        topic_id: TopicId,
        partition: u32,
        expected_epoch: u64,
    ) -> std::result::Result<(), RoutingError> {
        let actor = self
            .routing_read()
            .actors
            .get(&GroupKey {
                topic_id,
                partition,
            })
            .cloned()
            .ok_or_else(|| self.not_local_routing_error(topic_id, partition))?;
        let (response, receiver) = oneshot::channel();
        actor
            .client
            .try_send(ActorClient::SnapshotCommittedForRetention {
                expected_epoch,
                response,
            })
            .map_err(|_| RoutingError::unavailable(PublicError::BrokerBusy))?;
        receiver
            .await
            .unwrap_or_else(|_| Err(RoutingError::unavailable(PublicError::BrokerNotAvailable)))
    }

    pub(crate) async fn advance_log_start(
        &self,
        topic_id: TopicId,
        partition: u32,
        expected_epoch: u64,
        expected_manifest_generation: u64,
        new_log_start_offset: StreamOffset,
    ) -> std::result::Result<RetentionAdvanceResult, RoutingError> {
        let actor = self
            .routing_read()
            .actors
            .get(&GroupKey {
                topic_id,
                partition,
            })
            .cloned()
            .ok_or_else(|| self.not_local_routing_error(topic_id, partition))?;
        let (response, receiver) = oneshot::channel();
        actor
            .client
            .try_send(ActorClient::AdvanceLogStart {
                expected_epoch,
                expected_manifest_generation,
                new_log_start_offset,
                response,
            })
            .map_err(|_| RoutingError::unavailable(PublicError::BrokerBusy))?;
        receiver
            .await
            .unwrap_or_else(|_| Err(RoutingError::unavailable(PublicError::BrokerNotAvailable)))
    }

    pub(crate) async fn plan_keyed_compaction(
        &self,
        topic_id: TopicId,
        partition: u32,
        expected_epoch: u64,
        policy_epoch: u64,
        limits: CompactionLimits,
        tombstone_removal: Option<TombstoneRemovalGate>,
    ) -> std::result::Result<CompactionPlanResult, RoutingError> {
        let actor = self
            .routing_read()
            .actors
            .get(&GroupKey {
                topic_id,
                partition,
            })
            .cloned()
            .ok_or_else(|| self.not_local_routing_error(topic_id, partition))?;
        let (response, receiver) = oneshot::channel();
        actor
            .client
            .try_send(ActorClient::PlanKeyedCompaction {
                expected_epoch,
                policy_epoch,
                limits,
                tombstone_removal,
                response,
            })
            .map_err(|_| RoutingError::unavailable(PublicError::BrokerBusy))?;
        receiver
            .await
            .unwrap_or_else(|_| Err(RoutingError::unavailable(PublicError::BrokerNotAvailable)))
    }

    pub(crate) async fn execute_keyed_compaction(
        &self,
        topic_id: TopicId,
        partition: u32,
        expected_epoch: u64,
        plan: CompactionPlan,
        current_policy_epoch: u64,
        publication_metadata_epoch: Arc<RwLock<u64>>,
    ) -> std::result::Result<CompactionRunResult, RoutingError> {
        let actor = self
            .routing_read()
            .actors
            .get(&GroupKey {
                topic_id,
                partition,
            })
            .cloned()
            .ok_or_else(|| self.not_local_routing_error(topic_id, partition))?;
        let (response, receiver) = oneshot::channel();
        actor
            .client
            .try_send(ActorClient::ExecuteKeyedCompaction {
                expected_epoch,
                plan,
                current_policy_epoch,
                publication_metadata_epoch,
                response,
            })
            .map_err(|_| RoutingError::unavailable(PublicError::BrokerBusy))?;
        receiver
            .await
            .unwrap_or_else(|_| Err(RoutingError::unavailable(PublicError::BrokerNotAvailable)))
    }

    pub(crate) fn local_groups(&self) -> Vec<GroupKey> {
        self.routing_read().actors.keys().copied().collect()
    }

    pub(crate) async fn produce(
        &self,
        topic_id: TopicId,
        partition: u32,
        expected_epoch: Option<u64>,
        records: Vec<DataRecord>,
        ack: AckRequirement,
    ) -> std::result::Result<ProduceResult, RoutingError> {
        let actor = self
            .routing_read()
            .actors
            .get(&GroupKey {
                topic_id,
                partition,
            })
            .cloned()
            .ok_or_else(|| self.not_local_routing_error(topic_id, partition))?;
        let (response, receiver) = oneshot::channel();
        actor
            .client
            .try_send(ActorClient::Produce {
                expected_epoch,
                records,
                ack,
                response,
            })
            .map_err(|_| RoutingError::unavailable(PublicError::BrokerBusy))?;
        receiver
            .await
            .unwrap_or_else(|_| Err(RoutingError::unavailable(PublicError::BrokerNotAvailable)))
    }

    #[allow(clippy::too_many_arguments)]
    pub(crate) async fn produce_idempotent(
        &self,
        topic_id: TopicId,
        partition: u32,
        expected_epoch: u64,
        authority: ProducerAuthority,
        base_sequence: ProducerSequence,
        records: Vec<DataRecord>,
        controller_evidence: ControllerAuthorityEvidence,
    ) -> std::result::Result<ProduceResult, IdempotentProduceAttemptError> {
        let actor = self
            .routing_read()
            .actors
            .get(&GroupKey {
                topic_id,
                partition,
            })
            .cloned()
            .ok_or_else(|| {
                IdempotentProduceAttemptError::Routing(
                    self.not_local_routing_error(topic_id, partition),
                )
            })?;
        let (response, receiver) = oneshot::channel();
        actor
            .client
            .try_send(ActorClient::ProduceIdempotent {
                expected_epoch,
                authority,
                base_sequence,
                records,
                controller_evidence,
                response,
            })
            .map_err(|_| {
                IdempotentProduceAttemptError::Routing(RoutingError::unavailable(
                    PublicError::BrokerBusy,
                ))
            })?;
        receiver.await.unwrap_or_else(|_| {
            Err(IdempotentProduceAttemptError::Routing(
                RoutingError::unavailable(PublicError::BrokerNotAvailable),
            ))
        })
    }

    pub(crate) async fn fetch(
        &self,
        topic_id: TopicId,
        partition: u32,
        expected_epoch: Option<u64>,
        start: StreamOffset,
        max_records: usize,
        max_bytes: usize,
    ) -> std::result::Result<FetchResult, RoutingError> {
        let actor = self
            .routing_read()
            .actors
            .get(&GroupKey {
                topic_id,
                partition,
            })
            .cloned()
            .ok_or_else(|| self.not_local_routing_error(topic_id, partition))?;
        let (response, receiver) = oneshot::channel();
        actor
            .client
            .try_send(ActorClient::Fetch {
                expected_epoch,
                start,
                max_records,
                max_bytes,
                response,
            })
            .map_err(|_| RoutingError::unavailable(PublicError::BrokerBusy))?;
        receiver
            .await
            .unwrap_or_else(|_| Err(RoutingError::unavailable(PublicError::BrokerNotAvailable)))
    }

    pub(crate) async fn list_offset(
        &self,
        topic_id: TopicId,
        partition: u32,
        expected_epoch: Option<u64>,
        timestamp_selector: i64,
    ) -> std::result::Result<ListOffsetResult, RoutingError> {
        let actor = self
            .routing_read()
            .actors
            .get(&GroupKey {
                topic_id,
                partition,
            })
            .cloned()
            .ok_or_else(|| self.not_local_routing_error(topic_id, partition))?;
        let (response, receiver) = oneshot::channel();
        actor
            .client
            .try_send(ActorClient::ListOffset {
                expected_epoch,
                timestamp_selector,
                response,
            })
            .map_err(|_| RoutingError::unavailable(PublicError::BrokerBusy))?;
        receiver
            .await
            .unwrap_or_else(|_| Err(RoutingError::unavailable(PublicError::BrokerNotAvailable)))
    }

    fn not_local_routing_error(&self, topic_id: TopicId, partition: u32) -> RoutingError {
        let _ = (topic_id, partition);
        RoutingError {
            error: PublicError::NotLeader,
            leader_broker_id: UNKNOWN_BROKER_ID,
            leader_epoch: UNKNOWN_EPOCH,
        }
    }

    fn routing_read(&self) -> RwLockReadGuard<'_, ReplicationRouting> {
        self.routing
            .read()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }

    fn routing_write(&self) -> RwLockWriteGuard<'_, ReplicationRouting> {
        self.routing
            .write()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }
}

pub(crate) async fn start_static_replication(
    config: StaticReplicationConfig,
    data_dir: PathBuf,
    local_broker_id: u32,
    advertised_host: &str,
    advertised_port: u16,
) -> Result<(ReplicationClient, ReplicationRuntimeHandle)> {
    start_replication_runtime(
        config,
        data_dir,
        local_broker_id,
        advertised_host,
        advertised_port,
        false,
    )
    .await
}

async fn start_controller_replication(
    config: StaticReplicationConfig,
    data_dir: PathBuf,
    local_broker_id: u32,
    advertised_host: &str,
    advertised_port: u16,
) -> Result<(ReplicationClient, ReplicationRuntimeHandle)> {
    start_replication_runtime(
        config,
        data_dir,
        local_broker_id,
        advertised_host,
        advertised_port,
        true,
    )
    .await
}

async fn start_replication_runtime(
    config: StaticReplicationConfig,
    data_dir: PathBuf,
    local_broker_id: u32,
    advertised_host: &str,
    advertised_port: u16,
    allow_reserved_internal_topics: bool,
) -> Result<(ReplicationClient, ReplicationRuntimeHandle)> {
    config.validate_for_broker_impl(
        local_broker_id,
        advertised_host,
        advertised_port,
        allow_reserved_internal_topics,
    )?;
    let topology = Arc::new(config);
    let local = topology
        .local_broker(local_broker_id)
        .ok_or(BrokerError::InvalidConfig("missing local static broker"))?;
    let timing = TimingConfig::new(
        topology.heartbeat_interval_ticks,
        topology.election_timeout_min_ticks,
        topology.election_timeout_max_ticks,
    )?;

    let allowed_groups = local_groups(&topology, local_broker_id)?;
    let peers = topology
        .brokers
        .iter()
        .filter(|broker| {
            broker.broker_id != local_broker_id
                && !topology.blocked_broker_ids.contains(&broker.broker_id)
        })
        .map(|broker| {
            (
                broker.broker_id,
                PeerDescriptor {
                    broker_id: broker.broker_id,
                    incarnation: broker.transport_incarnation,
                    address: broker.raft_address,
                },
            )
        })
        .collect::<BTreeMap<_, _>>();
    let transport_config = PeerTransportConfig {
        local_broker_id,
        local_incarnation: local.transport_incarnation,
        bind_addr: topology.raft_bind_addr,
        peers,
        allowed_groups: allowed_groups.clone(),
        limits: topology.transport_limits,
        max_inbound_frames: topology.max_transport_inbound_frames,
        max_outbound_frames_per_peer: topology.max_transport_outbound_frames_per_peer,
        max_outbound_bytes_per_peer: topology.max_transport_outbound_bytes_per_peer,
        reconnect_min_delay: topology.reconnect_min_delay,
        reconnect_max_delay: topology.reconnect_max_delay,
    };
    let (transport, inbound) = start_peer_transport(transport_config).await?;
    let transport_sender = transport.sender();
    let (shutdown, _) = broadcast::channel::<()>(1);
    let mut tasks = Vec::new();
    let mut actors = BTreeMap::new();

    for group in allowed_groups {
        let assignment = assignment_for_group(&topology, group)?;
        let voters = assignment
            .iter()
            .map(|broker_id| broker_to_node(*broker_id))
            .collect::<Result<Vec<_>>>()?;
        let learners = topology
            .bootstrap_learners
            .get(&group)
            .cloned()
            .unwrap_or_default()
            .into_iter()
            .map(broker_to_node)
            .collect::<Result<Vec<_>>>()?;
        let configuration = StableConfiguration::new(voters, learners)?;
        let local_node = broker_to_node(local_broker_id)?;
        let directory = partition_directory(&data_dir, group.topic_id, group.partition);
        let command_limits = topology.command_limits;
        let replica_limits = topology.replica_limits;
        let partition_log = topology.partition_log;
        let data_compression = topology.topic_compression.get(&group.topic_id).copied();
        let initial_timeout = sample_election_timeout(timing)?;
        let open_directory = directory.clone();
        let replica = tokio::task::spawn_blocking(move || {
            let mut replica = ReplicatedPartition::open(
                &open_directory,
                group.topic_id,
                PartitionId::new(group.partition),
                local_node,
                now_ms(),
                partition_log,
                configuration,
                timing,
                initial_timeout,
                command_limits,
                replica_limits,
            )?;
            if let Some(data_compression) = data_compression {
                replica.set_data_compression(data_compression);
            }
            Ok::<_, astralog_replication::ReplicationError>(replica)
        })
        .await
        .map_err(|error| BrokerError::TaskJoin(error.to_string()))??;
        let (control_tx, control_rx) = mpsc::channel(topology.max_control_queue);
        let (client_tx, client_rx) = mpsc::channel(topology.max_client_queue);
        actors.insert(
            group,
            PartitionActorHandle {
                control: control_tx,
                client: client_tx,
            },
        );
        let actor_transport = transport_sender.clone();
        let progress_authority = topology.progress_authorities.get(&group).cloned();
        let actor_shutdown = shutdown.subscribe();
        tasks.push(tokio::spawn(run_partition_actor(
            group,
            local_broker_id,
            timing,
            replica,
            actor_transport,
            topology.rebootstrap_limits,
            topology.rebootstrap_v2_enabled,
            progress_authority,
            control_rx,
            client_rx,
            actor_shutdown,
        )));
    }

    let actors = Arc::new(actors);
    let router_actors = actors.clone();
    let router_shutdown = shutdown.subscribe();
    tasks.push(tokio::spawn(route_inbound(
        inbound,
        router_actors,
        router_shutdown,
    )));

    let tick_actors = actors.clone();
    let tick_shutdown = shutdown.subscribe();
    let tick_interval = topology.tick_interval;
    tasks.push(tokio::spawn(run_ticks(
        tick_actors,
        tick_interval,
        tick_shutdown,
    )));

    Ok((
        ReplicationClient {
            local_broker_id,
            routing: Arc::new(RwLock::new(ReplicationRouting { topology, actors })),
        },
        ReplicationRuntimeHandle {
            shutdown,
            transport,
            tasks,
        },
    ))
}

/// Best-effort P9-G sideband. Only dynamic P5 committed assignments enable
/// reporting; transport is bounded and this never submits a maintenance
/// proposal while tombstone/rebootstrap qualification is incomplete.
fn observe_p9_clean_progress_on_tick(
    group: GroupKey,
    local_broker_id: u32,
    replica: &ReplicatedPartition,
    transport: &PeerTransportSender,
    authority: &P9ProgressAuthority,
    tracker: &mut Option<CleanProgressTracker>,
) {
    let observation = replica.membership_observation();
    if authority.assignment_generation == 0
        || authority.local_incarnation == 0
        || observation.leader_epoch == 0
        || !p9g_effective_voters_carry_committed_p5_authority(
            &observation.effective_configuration,
            authority,
        )
    {
        *tracker = None;
        return;
    }
    let is_voter = observation
        .effective_configuration
        .contains_voter(replica.local_node());
    if observation.role == Role::Leader {
        if tracker.as_ref().is_none_or(|current| {
            !current.is_current_for_voter_identities(
                group.topic_id,
                PartitionId::new(group.partition),
                observation.leader_epoch,
                authority.assignment_generation,
                &observation.effective_configuration,
                &authority.voter_identities,
            )
        }) {
            *tracker = CleanProgressTracker::from_committed_assignment(
                group.topic_id,
                PartitionId::new(group.partition),
                observation.leader_epoch,
                authority.assignment_generation,
                &observation.effective_configuration,
                &authority.voter_identities,
            )
            .ok();
        }
        if let Some(current) = tracker.as_mut() {
            current.advance_observation_tick(P9_PROGRESS_REPORT_MAX_AGE_TICKS);
        }
        if is_voter {
            let report = CleanProgressReport {
                topic_id: group.topic_id,
                partition_id: PartitionId::new(group.partition),
                term: observation.leader_epoch,
                leader_epoch: observation.leader_epoch,
                source_broker_id: local_broker_id,
                source_incarnation: authority.local_incarnation,
                assignment_generation: authority.assignment_generation,
                clean_compacted_offset: replica.local_clean_compacted_offset(),
            };
            if let (Some(current), Ok(high)) =
                (tracker.as_mut(), replica.committed_stream_high_watermark())
            {
                let _ = current.accept(
                    replica.local_node(),
                    local_broker_id,
                    authority.local_incarnation,
                    report,
                    high,
                );
            }
        }
    } else {
        *tracker = None;
        if !is_voter {
            return; // learners cannot contribute to the safe minimum
        }
        let Some(leader) = observation.leader_id else {
            return;
        };
        if !authority.voter_identities.contains_key(&leader) {
            return;
        }
        let Ok(target_broker) = node_to_broker_id(leader) else {
            return;
        };
        if target_broker == local_broker_id {
            return;
        }
        let report = CleanProgressReport {
            topic_id: group.topic_id,
            partition_id: PartitionId::new(group.partition),
            term: observation.leader_epoch,
            leader_epoch: observation.leader_epoch,
            source_broker_id: local_broker_id,
            source_incarnation: authority.local_incarnation,
            assignment_generation: authority.assignment_generation,
            clean_compacted_offset: replica.local_clean_compacted_offset(),
        };
        // A full or disconnected bounded sideband queue drops the report;
        // periodic ticks retry. Control Raft traffic remains independent.
        let _ = transport.try_send_clean_progress(target_broker, group, report);
    }
}

/// A P3 EnterJoint commit can add the movement destination before a new P5
/// assignment is published, so the effective P3 union may be a superset.
/// Conversely, after P3 LeaveJoint commits but *before* controller
/// FinalizeMovement, the old P5 voter must not disappear from the all-voter
/// cleaning proof. In that interval the safe candidate is blocked, not
/// optimistically recomputed from the smaller P3 stable set.
fn p9g_effective_voters_carry_committed_p5_authority(
    effective: &astralog_raft::EffectiveConfiguration,
    authority: &P9ProgressAuthority,
) -> bool {
    let actual_p3_voters = effective.voters();
    authority.committed_p5_voters.is_subset(&actual_p3_voters)
        && actual_p3_voters
            .iter()
            .all(|node| authority.voter_identities.contains_key(node))
}

async fn route_inbound(
    mut inbound: mpsc::Receiver<PeerInboundFrame>,
    actors: Arc<BTreeMap<GroupKey, PartitionActorHandle>>,
    mut shutdown: broadcast::Receiver<()>,
) {
    loop {
        let frame = tokio::select! {
            _ = shutdown.recv() => return,
            frame = inbound.recv() => frame,
        };
        let Some(inbound_frame) = frame else {
            return;
        };
        let connection_generation = inbound_frame.connection_generation;
        let frame = inbound_frame.frame;
        let group = GroupKey {
            topic_id: frame.topic_id,
            partition: frame.partition,
        };
        let Some(actor) = actors.get(&group) else {
            continue;
        };
        let Ok(source) = broker_to_node(frame.source_broker_id) else {
            continue;
        };
        let is_progress_sideband = frame.clean_progress.is_some();
        let command = ActorControl::Inbound {
            source,
            source_incarnation: frame.source_incarnation,
            connection_generation,
            message: frame.message.map(Box::new),
            rebootstrap: frame.rebootstrap,
            clean_progress: frame.clean_progress,
        };
        if is_progress_sideband {
            // Never let optional progress reports block the actor's Raft
            // control channel. Missing reports fail closed at the leader.
            let _ = actor.control.try_send(command);
            continue;
        }
        if actor.control.send(command).await.is_err() {
            return;
        }
    }
}

async fn run_ticks(
    actors: Arc<BTreeMap<GroupKey, PartitionActorHandle>>,
    tick_interval: Duration,
    mut shutdown: broadcast::Receiver<()>,
) {
    let mut ticker = time::interval(tick_interval);
    ticker.set_missed_tick_behavior(time::MissedTickBehavior::Delay);
    loop {
        tokio::select! {
            _ = shutdown.recv() => return,
            _ = ticker.tick() => {
                let now = now_ms();
                for actor in actors.values() {
                    if actor.control.send(ActorControl::Tick { now_ms: now }).await.is_err() {
                        return;
                    }
                }
            }
        }
    }
}

#[allow(clippy::too_many_arguments)]
async fn run_partition_actor(
    group: GroupKey,
    local_broker_id: u32,
    timing: TimingConfig,
    mut replica: ReplicatedPartition,
    transport: PeerTransportSender,
    rebootstrap_limits: RebootstrapLimits,
    rebootstrap_v2_enabled: bool,
    progress_authority: Option<P9ProgressAuthority>,
    mut control: mpsc::Receiver<ActorControl>,
    mut client: mpsc::Receiver<ActorClient>,
    mut shutdown: broadcast::Receiver<()>,
) {
    let mut pending = BTreeMap::<ProposalId, PendingProduce>::new();
    let mut pending_idempotent = BTreeMap::<ProposalId, PendingIdempotentProduce>::new();
    let mut pending_retention = BTreeMap::<ProposalId, PendingRetentionAdvance>::new();
    let mut pending_producer_checkpoint = BTreeMap::<ProposalId, PendingProducerCheckpoint>::new();
    let mut transport_congested = false;
    let mut progress_tracker: Option<CleanProgressTracker> = None;
    let mut pending_compaction_safe: Option<ProposalId> = None;
    // P9-G only: bounded attempts per manifest + persisted P3 snapshot floor
    // to independently reverify a missing current-generation sidecar. A
    // failed first attempt may race an append/roll and must not strand RF3
    // forever. No value from an old generation is ever accepted as authority.
    let mut attempted_clean_revalidation: Option<(u64, u64, u32)> = None;
    // Control-plane traffic has priority, but an uninterrupted Raft/progress
    // stream must not permanently starve partition observation and other
    // client commands. Admit one queued client command after a bounded burst
    // of control messages; shutdown retains unconditional first priority.
    let mut consecutive_controls = 0_u8;
    loop {
        let client_priority = consecutive_controls >= 16 && !client.is_empty();
        tokio::select! {
            biased;
            _ = shutdown.recv() => return,
            command = control.recv(), if !client_priority => {
                consecutive_controls = consecutive_controls.saturating_add(1);
                let Some(command) = command else { return; };
                let result = match command {
                    ActorControl::Tick { now_ms } => {
                        let result = sample_election_timeout(timing)
                            .and_then(|sample| replica.tick(sample, now_ms).map_err(BrokerError::from));
                        if result.is_ok() && let Some(authority) = &progress_authority {
                            // On a harmless generation roll, the previously
                            // proved local sidecar is deliberately stale. The
                            // positive RF3 safe minimum would otherwise wait
                            // for a value that can only be restored once safe
                            // is already committed (a circular gate). Use the
                            // bounded full-prefix verifier instead, at most
                            // once per 51 actor ticks for the same pair.
                            // Failure remains conservative: report zero.
                            if rebootstrap_v2_enabled
                                && replica.local_clean_compacted_offset().get() == 0
                                && let Some(generation) = replica.manifest_generation()
                            {
                                let key = (generation, replica.persisted_snapshot_floor_raft_index());
                                let should_retry = match attempted_clean_revalidation {
                                    Some((generation, floor, count))
                                        if (generation, floor) == key && count < 50 =>
                                    {
                                        attempted_clean_revalidation =
                                            Some((generation, floor, count + 1));
                                        false
                                    }
                                    _ => true,
                                };
                                if should_retry {
                                    attempted_clean_revalidation = Some((key.0, key.1, 0));
                                    let _ = replica.revalidate_local_clean_progress_for_current_generation();
                                }
                            }
                                // Missing/offline peers retain their blocking
                                // place in the committed P5 voter set.
                                observe_p9_clean_progress_on_tick(
                                    group, local_broker_id, &replica, &transport,
                                    authority, &mut progress_tracker,
                                );
                            // Safe-offset replication is enabled only if
                            // this cluster also opted into P3-fenced retained
                            // V2 rebootstrap for any replica that falls behind.
                            if rebootstrap_v2_enabled {
                                if let Some(id) = pending_compaction_safe
                                    && replica.take_completed(id).is_some()
                                {
                                    pending_compaction_safe = None;
                                }
                                if pending_compaction_safe.is_none()
                                    && !transport_congested
                                    && replica.role() == Role::Leader
                                    && let Some(current) = &progress_tracker
                                    && let Ok(Some(receipt)) = replica.propose_compaction_safe_from_reports(
                                        replica.leader_epoch(), authority.assignment_generation,
                                        current, now_ms,
                                    )
                                {
                                    pending_compaction_safe = Some(receipt.proposal_id);
                                }
                            }
                        }
                        result
                    }
                    ActorControl::Inbound {
                        source,
                        source_incarnation,
                        connection_generation,
                        message,
                        rebootstrap,
                        clean_progress,
                    } => {
                        #[cfg(debug_assertions)]
                        if rebootstrap_v2_enabled
                            && std::env::var_os("ASTRALOG_P9G_TRACE").is_some()
                        {
                            let mode = match (&rebootstrap, &message) {
                                (Some(bytes), _) if bytes.starts_with(b"ASTRBTP2") => "recv_v2",
                                (Some(_), _) => "recv_v1",
                                (None, Some(boxed))
                                    if matches!(
                                        boxed.as_ref(),
                                        astralog_raft::RaftMessage::AppendEntriesRequest(_)
                                    ) => "recv_append",
                                _ => "other",
                            };
                            if mode != "other" {
                                eprintln!(
                                    "P9G_SNAPSHOT_ROUTE decision={mode} broker={local_broker_id} source={source:?} group={group:?} manifest={:?}",
                                    replica.manifest_generation(),
                                );
                            }
                        }
                        match replica.observe_peer_transport(
                            source,
                            source_incarnation,
                            connection_generation,
                        ) {
                            Ok(false) => Ok(()),
                            Err(error) => Err(BrokerError::from(error)),
                            Ok(true) => if let Some(report) = clean_progress {
                                if let Some(tracker) = progress_tracker.as_mut()
                                    && let Ok(high) = replica.committed_stream_high_watermark() {
                                        let _ = tracker.accept(
                                            source, report.source_broker_id, source_incarnation,
                                            report, high,
                                        );
                                }
                                Ok(())
                            } else if let Some(message) = message {
                                match rebootstrap {
                                Some(bytes) if bytes.starts_with(b"ASTRBTP2") => {
                                    if !rebootstrap_v2_enabled {
                                        Err(BrokerError::InvalidConfig(
                                            "P9-G V2 rebootstrap received before homogeneous opt-in",
                                        ))
                                    } else {
                                        decode_rebootstrap_package_v2(&bytes, RebootstrapLimitsV2::default())
                                            .map_err(BrokerError::from)
                                            .and_then(|package| {
                                                replica.step_with_rebootstrap_v2(source, *message, package, now_ms())
                                                    .map_err(BrokerError::from)
                                            })
                                    }
                                }
                                Some(bytes) => decode_rebootstrap_package(&bytes, rebootstrap_limits)
                                    .map_err(BrokerError::from)
                                    .and_then(|package| {
                                        replica
                                            .step_with_rebootstrap(source, *message, package, now_ms())
                                            .map_err(BrokerError::from)
                                    }),
                                None => replica
                                    .step(source, *message, now_ms())
                                    .map_err(BrokerError::from),
                                }
                            } else {
                                Err(BrokerError::InvalidConfig("invalid empty P9 transport frame"))
                            },
                        }
                    }
                };
                let flushed = flush_outbound(
                        group,
                        &mut replica,
                        &transport,
                        rebootstrap_limits,
                        rebootstrap_v2_enabled,
                    );
                if result.is_err() || flushed.is_err() {
                    #[cfg(debug_assertions)]
                    if progress_authority.is_some() {
                        eprintln!(
                            "P9G_REPLICATION_ACTOR_FATAL broker={local_broker_id} topic={:?} partition={} control_error={:?} flush_error={:?} replica_fatal={} reason={:?} membership={:?}",
                            group.topic_id,
                            group.partition,
                            result.as_ref().err(),
                            flushed.as_ref().err(),
                            replica.is_fatal(),
                            replica.fatal_reason(),
                            replica.membership_observation()
                        );
                    }
                    fail_pending(&mut replica, &mut pending, PublicError::BrokerNotAvailable);
                    fail_pending_idempotent(
                        &mut replica,
                        &mut pending_idempotent,
                        PublicError::BrokerNotAvailable,
                    );
                    fail_pending_retention(
                        &mut replica,
                        &mut pending_retention,
                        PublicError::BrokerNotAvailable,
                    );
                    fail_pending_producer_checkpoint(
                        &mut replica,
                        &mut pending_producer_checkpoint,
                        PublicError::BrokerNotAvailable,
                    );
                    return;
                }
                if let Some(congested) = flushed.unwrap() {
                    transport_congested = congested;
                }
                complete_pending(local_broker_id, &mut replica, &mut pending);
                complete_pending_idempotent(
                    local_broker_id,
                    &mut replica,
                    &mut pending_idempotent,
                );
                complete_pending_retention(
                    local_broker_id,
                    &mut replica,
                    &mut pending_retention,
                );
                complete_pending_producer_checkpoint(
                    &mut replica,
                    &mut pending_producer_checkpoint,
                );
                if let Some(id) = pending_compaction_safe
                    && replica.take_completed(id).is_some()
                {
                    pending_compaction_safe = None;
                }
            }
            command = client.recv() => {
                consecutive_controls = 0;
                let Some(command) = command else { return; };
                // A P9-G replica with an unproven current retained generation
                // must stay alive for authenticated higher-term rebootstrap,
                // but the old P3 leader role is NOT serving authority. In
                // particular Fetch/ListOffset bypass Raft proposals and must
                // not expose data whose safely removed tombstone may be
                // absent locally. Keep only raw P3 observations available;
                // dropping every other response is an explicit unavailable
                // outcome, never an acknowledgement or a stale-data read.
                let quarantine = rebootstrap_v2_enabled
                    && progress_authority.is_some()
                    && replica
                        .is_rebootstrap_required_for_compaction()
                        .unwrap_or(true);
                if quarantine
                    && !matches!(
                        &command,
                        ActorClient::Status { .. } | ActorClient::MembershipStatus { .. }
                    )
                {
                    continue;
                }
                let handled = handle_client_command(
                    local_broker_id,
                    &mut replica,
                    &mut pending,
                    &mut pending_idempotent,
                    &mut pending_retention,
                    &mut pending_producer_checkpoint,
                    command,
                    !transport_congested,
                    rebootstrap_v2_enabled && progress_authority.is_some(),
                );
                if let Err(error) = handled {
                    #[cfg(debug_assertions)]
                    if progress_authority.is_some() {
                        eprintln!(
                            "P9G_REPLICATION_ACTOR_FATAL broker={local_broker_id} topic={:?} partition={} client_error={error:?}",
                            group.topic_id,
                            group.partition
                        );
                    }
                    fail_pending(&mut replica, &mut pending, PublicError::BrokerNotAvailable);
                    fail_pending_idempotent(
                        &mut replica,
                        &mut pending_idempotent,
                        PublicError::BrokerNotAvailable,
                    );
                    fail_pending_retention(
                        &mut replica,
                        &mut pending_retention,
                        PublicError::BrokerNotAvailable,
                    );
                    fail_pending_producer_checkpoint(
                        &mut replica,
                        &mut pending_producer_checkpoint,
                        PublicError::BrokerNotAvailable,
                    );
                    return;
                }
                let flushed = flush_outbound(
                        group,
                        &mut replica,
                        &transport,
                        rebootstrap_limits,
                        rebootstrap_v2_enabled,
                    );
                if flushed.is_err() {
                    #[cfg(debug_assertions)]
                    if progress_authority.is_some() {
                        eprintln!(
                            "P9G_REPLICATION_ACTOR_FATAL broker={local_broker_id} topic={:?} partition={} client_flush_error={:?}",
                            group.topic_id,
                            group.partition,
                            flushed.as_ref().err()
                        );
                    }
                    fail_pending(&mut replica, &mut pending, PublicError::BrokerNotAvailable);
                    fail_pending_idempotent(
                        &mut replica,
                        &mut pending_idempotent,
                        PublicError::BrokerNotAvailable,
                    );
                    fail_pending_retention(
                        &mut replica,
                        &mut pending_retention,
                        PublicError::BrokerNotAvailable,
                    );
                    fail_pending_producer_checkpoint(
                        &mut replica,
                        &mut pending_producer_checkpoint,
                        PublicError::BrokerNotAvailable,
                    );
                    return;
                }
                if let Some(congested) = flushed.unwrap() {
                    transport_congested = congested;
                }
                complete_pending(local_broker_id, &mut replica, &mut pending);
                complete_pending_idempotent(
                    local_broker_id,
                    &mut replica,
                    &mut pending_idempotent,
                );
                complete_pending_retention(
                    local_broker_id,
                    &mut replica,
                    &mut pending_retention,
                );
                complete_pending_producer_checkpoint(
                    &mut replica,
                    &mut pending_producer_checkpoint,
                );
            }
        }
    }
}

#[allow(clippy::too_many_arguments)]
fn handle_client_command(
    local_broker_id: u32,
    replica: &mut ReplicatedPartition,
    pending: &mut BTreeMap<ProposalId, PendingProduce>,
    pending_idempotent: &mut BTreeMap<ProposalId, PendingIdempotentProduce>,
    pending_retention: &mut BTreeMap<ProposalId, PendingRetentionAdvance>,
    pending_producer_checkpoint: &mut BTreeMap<ProposalId, PendingProducerCheckpoint>,
    command: ActorClient,
    allow_produce: bool,
    allow_tombstone_removal: bool,
) -> Result<()> {
    match command {
        ActorClient::Status { response } => {
            let _ = response.send(partition_status(replica));
        }
        ActorClient::MembershipStatus { response } => {
            let _ = response.send(replica.membership_observation());
        }
        ActorClient::Reconfigure { change, response } => {
            let result = replica
                .propose_configuration_change(change, now_ms())
                .map_err(BrokerError::from);
            let _ = response.send(result);
        }
        ActorClient::TransferLeadership { target, response } => {
            let result = replica
                .transfer_leadership(target, now_ms())
                .map_err(BrokerError::from);
            let _ = response.send(result);
        }
        ActorClient::PersistProducerSnapshot { response } => {
            let result = replica
                .persist_producer_snapshot()
                .map_err(BrokerError::from);
            let _ = response.send(result);
        }
        ActorClient::CheckpointProducerState {
            expected_epoch,
            response,
        } => {
            let status = partition_status(replica);
            if let Err(error) = validate_leader_request(Some(expected_epoch), status) {
                let _ = response.send(Err(error));
                return Ok(());
            }
            let receipt = match replica.propose_producer_state_checkpoint(expected_epoch, now_ms())
            {
                Ok(None) => {
                    let _ = response.send(Ok(None));
                    return Ok(());
                }
                Ok(Some(receipt)) => receipt,
                Err(_) => {
                    let _ = response.send(Err(RoutingError::from_status(
                        PublicError::StorageError,
                        partition_status(replica),
                    )));
                    return Ok(());
                }
            };
            match replica
                .proposal_outcome(receipt.proposal_id)
                .unwrap_or(ProposalOutcome::Rejected)
            {
                ProposalOutcome::Pending => {
                    pending_producer_checkpoint.insert(
                        receipt.proposal_id,
                        PendingProducerCheckpoint {
                            response,
                            raft_index: receipt.raft_index.get(),
                        },
                    );
                }
                ProposalOutcome::QuorumCommitted => {
                    let _ = replica.take_completed(receipt.proposal_id);
                    let _ = response.send(Ok(Some(receipt.raft_index.get())));
                }
                ProposalOutcome::LocalDurable => {
                    let _ = replica.take_completed(receipt.proposal_id);
                    let _ = response.send(Err(RoutingError::from_status(
                        PublicError::UnknownServerError,
                        partition_status(replica),
                    )));
                }
                ProposalOutcome::LeadershipLost | ProposalOutcome::Rejected => {
                    let _ = replica.take_completed(receipt.proposal_id);
                    let _ = response.send(Err(RoutingError::from_status(
                        PublicError::NotLeader,
                        partition_status(replica),
                    )));
                }
            }
        }
        ActorClient::SnapshotCommittedForRetention {
            expected_epoch,
            response,
        } => {
            let status = partition_status(replica);
            if let Err(error) = validate_leader_request(Some(expected_epoch), status) {
                let _ = response.send(Err(error));
                return Ok(());
            }
            let result = replica
                .snapshot_committed_prefix_for_retention(expected_epoch, now_ms())
                .map(|_| ())
                .map_err(|_| {
                    RoutingError::from_status(PublicError::StorageError, partition_status(replica))
                });
            let _ = response.send(result);
        }
        ActorClient::PlanDeleteRetention {
            expected_epoch,
            now_ms,
            retention_ms,
            retention_bytes,
            protected_log_start_ceiling,
            response,
        } => {
            let status = partition_status(replica);
            if let Err(error) = validate_leader_request(Some(expected_epoch), status) {
                let _ = response.send(Err(error));
                return Ok(());
            }
            match replica.plan_delete_retention(
                now_ms,
                retention_ms,
                retention_bytes,
                protected_log_start_ceiling,
            ) {
                Ok(plan) => {
                    let _ = response.send(Ok(RetentionPlanResult {
                        plan,
                        leader_epoch: status.leader_epoch,
                    }));
                }
                Err(_) => {
                    let _ = response.send(Err(RoutingError::from_status(
                        PublicError::StorageError,
                        partition_status(replica),
                    )));
                }
            }
        }
        ActorClient::AdvanceLogStart {
            expected_epoch,
            expected_manifest_generation,
            new_log_start_offset,
            response,
        } => {
            let status = partition_status(replica);
            if let Err(error) = validate_leader_request(Some(expected_epoch), status) {
                let _ = response.send(Err(error));
                return Ok(());
            }
            if replica.manifest_generation() != Some(expected_manifest_generation) {
                let _ = response.send(Err(RoutingError::from_status(
                    PublicError::StaleEpoch,
                    status,
                )));
                return Ok(());
            }
            match replica.committed_log_start_offset() {
                Ok(current) if current == new_log_start_offset => {
                    let result = finalize_retention_advance(
                        local_broker_id,
                        replica,
                        expected_manifest_generation,
                        new_log_start_offset,
                        status.leader_epoch,
                    );
                    let _ = response.send(result);
                    return Ok(());
                }
                Ok(current) if new_log_start_offset < current => {
                    let _ = response.send(Err(RoutingError::from_status(
                        PublicError::InvalidRequest,
                        status,
                    )));
                    return Ok(());
                }
                Err(_) => {
                    let _ = response.send(Err(RoutingError::from_status(
                        PublicError::StorageError,
                        status,
                    )));
                    return Ok(());
                }
                Ok(_) => {}
            }
            let receipt = match replica.propose_log_start_offset(
                expected_epoch,
                new_log_start_offset,
                now_ms(),
            ) {
                Ok(receipt) => receipt,
                Err(_) => {
                    let _ = response.send(Err(RoutingError::from_status(
                        PublicError::StorageError,
                        partition_status(replica),
                    )));
                    return Ok(());
                }
            };
            match replica
                .proposal_outcome(receipt.proposal_id)
                .unwrap_or(ProposalOutcome::Rejected)
            {
                ProposalOutcome::Pending => {
                    pending_retention.insert(
                        receipt.proposal_id,
                        PendingRetentionAdvance {
                            response,
                            expected_manifest_generation,
                            requested_log_start_offset: new_log_start_offset,
                            leader_epoch: status.leader_epoch,
                        },
                    );
                }
                ProposalOutcome::QuorumCommitted => {
                    let _ = replica.take_completed(receipt.proposal_id);
                    let result = finalize_retention_advance(
                        local_broker_id,
                        replica,
                        expected_manifest_generation,
                        new_log_start_offset,
                        status.leader_epoch,
                    );
                    let _ = response.send(result);
                }
                ProposalOutcome::LocalDurable => {
                    let _ = replica.take_completed(receipt.proposal_id);
                    let _ = response.send(Err(RoutingError::from_status(
                        PublicError::UnknownServerError,
                        partition_status(replica),
                    )));
                }
                ProposalOutcome::LeadershipLost | ProposalOutcome::Rejected => {
                    let _ = replica.take_completed(receipt.proposal_id);
                    let _ = response.send(Err(RoutingError::from_status(
                        PublicError::NotLeader,
                        partition_status(replica),
                    )));
                }
            }
        }
        ActorClient::PlanKeyedCompaction {
            expected_epoch,
            policy_epoch,
            limits,
            tombstone_removal,
            response,
        } => {
            let status = partition_status(replica);
            // During mixed-version/P9-F rollouts, ordinary value COW still
            // works, but the CompactDelete tombstone gate remains *absent*
            // until the entire committed-P5 + V2 rebootstrap path is enabled.
            let tombstone_removal = allow_tombstone_removal
                .then_some(tombstone_removal)
                .flatten();
            if let Err(error) = validate_leader_request(Some(expected_epoch), status) {
                let _ = response.send(Err(error));
                return Ok(());
            }
            match replica.plan_keyed_compaction_with_tombstone_gate(
                policy_epoch,
                limits,
                tombstone_removal,
            ) {
                Ok(plan) => {
                    let _ = response.send(Ok(CompactionPlanResult {
                        plan,
                        leader_epoch: status.leader_epoch,
                    }));
                }
                Err(_) => {
                    let _ = response.send(Err(RoutingError::from_status(
                        PublicError::StorageError,
                        partition_status(replica),
                    )));
                }
            }
        }
        ActorClient::ExecuteKeyedCompaction {
            expected_epoch,
            plan,
            current_policy_epoch,
            publication_metadata_epoch,
            response,
        } => {
            let status = partition_status(replica);
            if let Err(error) = validate_leader_request(Some(expected_epoch), status) {
                let _ = response.send(Err(error));
                return Ok(());
            }
            match replica.execute_keyed_compaction_with_publication_guard(
                &plan,
                current_policy_epoch,
                publication_metadata_epoch.as_ref(),
            ) {
                Ok(outcome) => {
                    let _ = response.send(Ok(CompactionRunResult {
                        outcome,
                        leader_broker_id: local_broker_id,
                        leader_epoch: status.leader_epoch,
                    }));
                }
                Err(_) => {
                    let _ = response.send(Err(RoutingError::from_status(
                        PublicError::StorageError,
                        partition_status(replica),
                    )));
                }
            }
        }
        ActorClient::Produce {
            expected_epoch,
            records,
            ack,
            response,
        } => {
            let status = partition_status(replica);
            if !allow_produce {
                let _ = response.send(Err(RoutingError::from_status(
                    PublicError::BrokerBusy,
                    status,
                )));
                return Ok(());
            }
            if let Err(error) = validate_leader_request(expected_epoch, status) {
                let _ = response.send(Err(error));
                return Ok(());
            }
            let receipt = match replica.propose_records(records, ack, now_ms()) {
                Ok(receipt) => receipt,
                Err(_) => {
                    let _ = response.send(Err(RoutingError::from_status(
                        PublicError::StorageError,
                        partition_status(replica),
                    )));
                    return Ok(());
                }
            };
            let outcome = replica
                .proposal_outcome(receipt.proposal_id)
                .unwrap_or(ProposalOutcome::Rejected);
            match outcome {
                ProposalOutcome::Pending => {
                    pending.insert(
                        receipt.proposal_id,
                        PendingProduce {
                            response,
                            base_offset: receipt.base_offset.get(),
                            last_offset: receipt.last_offset.get(),
                            raft_index: receipt.raft_index.get(),
                            leader_epoch: status.leader_epoch,
                        },
                    );
                }
                ProposalOutcome::LocalDurable | ProposalOutcome::QuorumCommitted => {
                    let _ = replica.take_completed(receipt.proposal_id);
                    let _ = response.send(Ok(ProduceResult {
                        base_offset: receipt.base_offset.get(),
                        last_offset: receipt.last_offset.get(),
                        raft_index: receipt.raft_index.get(),
                        leader_broker_id: local_broker_id,
                        leader_epoch: status.leader_epoch,
                    }));
                }
                ProposalOutcome::LeadershipLost | ProposalOutcome::Rejected => {
                    let _ = replica.take_completed(receipt.proposal_id);
                    let _ = response.send(Err(RoutingError::from_status(
                        PublicError::NotLeader,
                        partition_status(replica),
                    )));
                }
            }
        }
        ActorClient::ProduceIdempotent {
            expected_epoch,
            authority,
            base_sequence,
            records,
            controller_evidence,
            response,
        } => {
            let status = partition_status(replica);
            if !allow_produce {
                let _ = response.send(Err(IdempotentProduceAttemptError::Routing(
                    RoutingError::from_status(PublicError::BrokerBusy, status),
                )));
                return Ok(());
            }
            let disposition = match replica.propose_idempotent_records(
                expected_epoch,
                authority,
                base_sequence,
                records,
                AckRequirement::Quorum,
                controller_evidence,
                now_ms(),
            ) {
                Ok(disposition) => disposition,
                Err(IdempotentProduceError::ControllerValidationRequired) => {
                    let _ = response.send(Err(
                        IdempotentProduceAttemptError::ControllerValidationRequired,
                    ));
                    return Ok(());
                }
                Err(error) => {
                    let _ = response.send(Err(IdempotentProduceAttemptError::Routing(
                        routing_error_for_idempotent(error, partition_status(replica)),
                    )));
                    return Ok(());
                }
            };
            match disposition {
                IdempotentProposalDisposition::Duplicate(retry) => {
                    let _ = response.send(Ok(ProduceResult {
                        base_offset: retry.base_stream_offset.get(),
                        last_offset: retry.last_stream_offset.get(),
                        raft_index: retry.raft_index.get(),
                        leader_broker_id: local_broker_id,
                        leader_epoch: status.leader_epoch,
                    }));
                }
                IdempotentProposalDisposition::Coalesced(receipt) => {
                    if let Some(pending) = pending_idempotent.get_mut(&receipt.proposal_id) {
                        pending.responses.push(response);
                    } else {
                        pending_idempotent.insert(
                            receipt.proposal_id,
                            PendingIdempotentProduce {
                                responses: vec![response],
                                base_offset: receipt.base_offset.get(),
                                last_offset: receipt.last_offset.get(),
                                raft_index: receipt.raft_index.get(),
                                leader_epoch: status.leader_epoch,
                            },
                        );
                    }
                }
                IdempotentProposalDisposition::Proposed(receipt) => {
                    let outcome = replica
                        .proposal_outcome(receipt.proposal_id)
                        .unwrap_or(ProposalOutcome::Rejected);
                    match outcome {
                        ProposalOutcome::Pending => {
                            pending_idempotent.insert(
                                receipt.proposal_id,
                                PendingIdempotentProduce {
                                    responses: vec![response],
                                    base_offset: receipt.base_offset.get(),
                                    last_offset: receipt.last_offset.get(),
                                    raft_index: receipt.raft_index.get(),
                                    leader_epoch: status.leader_epoch,
                                },
                            );
                        }
                        ProposalOutcome::QuorumCommitted => {
                            let _ = replica.take_completed(receipt.proposal_id);
                            let _ = response.send(Ok(ProduceResult {
                                base_offset: receipt.base_offset.get(),
                                last_offset: receipt.last_offset.get(),
                                raft_index: receipt.raft_index.get(),
                                leader_broker_id: local_broker_id,
                                leader_epoch: status.leader_epoch,
                            }));
                        }
                        ProposalOutcome::LocalDurable => {
                            let _ = replica.take_completed(receipt.proposal_id);
                            let _ = response.send(Err(IdempotentProduceAttemptError::Routing(
                                RoutingError::from_status(
                                    PublicError::UnknownServerError,
                                    partition_status(replica),
                                ),
                            )));
                        }
                        ProposalOutcome::LeadershipLost | ProposalOutcome::Rejected => {
                            let _ = replica.take_completed(receipt.proposal_id);
                            let _ = response.send(Err(IdempotentProduceAttemptError::Routing(
                                RoutingError::from_status(
                                    PublicError::NotLeader,
                                    partition_status(replica),
                                ),
                            )));
                        }
                    }
                }
            }
        }
        ActorClient::Fetch {
            expected_epoch,
            start,
            max_records,
            max_bytes,
            response,
        } => {
            let status = partition_status(replica);
            if let Err(error) = validate_leader_request(expected_epoch, status) {
                let _ = response.send(Err(error));
                return Ok(());
            }
            match replica.read_committed_records_bounded(start, max_records, max_bytes) {
                Ok((records, _)) => {
                    let _ = response.send(Ok(FetchResult {
                        records,
                        log_start_offset: status.log_start_offset,
                        high_watermark: status.high_watermark,
                        leader_broker_id: local_broker_id,
                        leader_epoch: status.leader_epoch,
                    }));
                }
                Err(_) => {
                    let _ = response.send(Err(RoutingError::from_status(
                        PublicError::StorageError,
                        partition_status(replica),
                    )));
                }
            }
        }
        ActorClient::ListOffset {
            expected_epoch,
            timestamp_selector,
            response,
        } => {
            let status = partition_status(replica);
            if let Err(error) = validate_leader_request(expected_epoch, status) {
                let _ = response.send(Err(error));
                return Ok(());
            }
            match replica.list_committed_offset(timestamp_selector) {
                Ok((offset, timestamp_ms)) => {
                    let _ = response.send(Ok(ListOffsetResult {
                        offset,
                        timestamp_ms,
                        leader_broker_id: local_broker_id,
                        leader_epoch: status.leader_epoch,
                    }));
                }
                Err(_) => {
                    let _ = response.send(Err(RoutingError::from_status(
                        PublicError::StorageError,
                        partition_status(replica),
                    )));
                }
            }
        }
    }
    Ok(())
}

fn finalize_retention_advance(
    local_broker_id: u32,
    replica: &mut ReplicatedPartition,
    expected_manifest_generation: u64,
    requested_log_start_offset: StreamOffset,
    leader_epoch: u64,
) -> std::result::Result<RetentionAdvanceResult, RoutingError> {
    let floor = replica.committed_log_start_offset().map_err(|_| {
        RoutingError::from_status(PublicError::StorageError, partition_status(replica))
    })?;
    if floor < requested_log_start_offset {
        return Err(RoutingError::from_status(
            PublicError::UnknownServerError,
            partition_status(replica),
        ));
    }
    let (reclaimed_segments, physical_cleanup_deferred) =
        if replica.manifest_generation() == Some(expected_manifest_generation) {
            match replica.publish_delete_retention_generation(expected_manifest_generation) {
                Ok(count) => (count, false),
                Err(_) => (0, true),
            }
        } else {
            (0, true)
        };
    Ok(RetentionAdvanceResult {
        log_start_offset: floor.get(),
        reclaimed_segments,
        physical_cleanup_deferred,
        leader_broker_id: local_broker_id,
        leader_epoch,
    })
}

fn complete_pending_retention(
    local_broker_id: u32,
    replica: &mut ReplicatedPartition,
    pending: &mut BTreeMap<ProposalId, PendingRetentionAdvance>,
) {
    let ids = pending.keys().copied().collect::<Vec<_>>();
    for id in ids {
        if pending
            .get(&id)
            .is_some_and(|waiter| waiter.response.is_closed())
        {
            let _ = replica.forget_proposal(id);
            pending.remove(&id);
            continue;
        }
        let Some(outcome) = replica.proposal_outcome(id) else {
            pending.remove(&id);
            continue;
        };
        if outcome == ProposalOutcome::Pending {
            continue;
        }
        let _ = replica.take_completed(id);
        let Some(waiter) = pending.remove(&id) else {
            continue;
        };
        match outcome {
            ProposalOutcome::QuorumCommitted => {
                let result = finalize_retention_advance(
                    local_broker_id,
                    replica,
                    waiter.expected_manifest_generation,
                    waiter.requested_log_start_offset,
                    waiter.leader_epoch,
                );
                let _ = waiter.response.send(result);
            }
            ProposalOutcome::LocalDurable => {
                let _ = waiter.response.send(Err(RoutingError::from_status(
                    PublicError::UnknownServerError,
                    partition_status(replica),
                )));
            }
            ProposalOutcome::LeadershipLost | ProposalOutcome::Rejected => {
                let _ = waiter.response.send(Err(RoutingError::from_status(
                    PublicError::NotLeader,
                    partition_status(replica),
                )));
            }
            ProposalOutcome::Pending => {}
        }
    }
}

fn fail_pending_retention(
    replica: &mut ReplicatedPartition,
    pending: &mut BTreeMap<ProposalId, PendingRetentionAdvance>,
    error: PublicError,
) {
    let status = partition_status(replica);
    for (id, waiter) in std::mem::take(pending) {
        let _ = replica.forget_proposal(id);
        let _ = waiter
            .response
            .send(Err(RoutingError::from_status(error, status)));
    }
}

fn complete_pending_producer_checkpoint(
    replica: &mut ReplicatedPartition,
    pending: &mut BTreeMap<ProposalId, PendingProducerCheckpoint>,
) {
    let ids = pending.keys().copied().collect::<Vec<_>>();
    for id in ids {
        if pending
            .get(&id)
            .is_some_and(|waiter| waiter.response.is_closed())
        {
            let _ = replica.forget_proposal(id);
            pending.remove(&id);
            continue;
        }
        let Some(outcome) = replica.proposal_outcome(id) else {
            pending.remove(&id);
            continue;
        };
        if outcome == ProposalOutcome::Pending {
            continue;
        }
        let _ = replica.take_completed(id);
        let Some(waiter) = pending.remove(&id) else {
            continue;
        };
        match outcome {
            ProposalOutcome::QuorumCommitted => {
                let _ = waiter.response.send(Ok(Some(waiter.raft_index)));
            }
            ProposalOutcome::LocalDurable => {
                let _ = waiter.response.send(Err(RoutingError::from_status(
                    PublicError::UnknownServerError,
                    partition_status(replica),
                )));
            }
            ProposalOutcome::LeadershipLost | ProposalOutcome::Rejected => {
                let _ = waiter.response.send(Err(RoutingError::from_status(
                    PublicError::NotLeader,
                    partition_status(replica),
                )));
            }
            ProposalOutcome::Pending => {}
        }
    }
}

fn fail_pending_producer_checkpoint(
    replica: &mut ReplicatedPartition,
    pending: &mut BTreeMap<ProposalId, PendingProducerCheckpoint>,
    error: PublicError,
) {
    let status = partition_status(replica);
    for (id, waiter) in std::mem::take(pending) {
        let _ = replica.forget_proposal(id);
        let _ = waiter
            .response
            .send(Err(RoutingError::from_status(error, status)));
    }
}

fn complete_pending(
    local_broker_id: u32,
    replica: &mut ReplicatedPartition,
    pending: &mut BTreeMap<ProposalId, PendingProduce>,
) {
    let ids = pending.keys().copied().collect::<Vec<_>>();
    for id in ids {
        let sender_closed = pending
            .get(&id)
            .is_some_and(|waiter| waiter.response.is_closed());
        if sender_closed {
            let _ = replica.forget_proposal(id);
            pending.remove(&id);
            continue;
        }
        let Some(outcome) = replica.proposal_outcome(id) else {
            pending.remove(&id);
            continue;
        };
        if outcome == ProposalOutcome::Pending {
            continue;
        }
        let _ = replica.take_completed(id);
        let Some(waiter) = pending.remove(&id) else {
            continue;
        };
        match outcome {
            ProposalOutcome::QuorumCommitted | ProposalOutcome::LocalDurable => {
                let _ = waiter.response.send(Ok(ProduceResult {
                    base_offset: waiter.base_offset,
                    last_offset: waiter.last_offset,
                    raft_index: waiter.raft_index,
                    leader_broker_id: local_broker_id,
                    leader_epoch: waiter.leader_epoch,
                }));
            }
            ProposalOutcome::LeadershipLost | ProposalOutcome::Rejected => {
                let _ = waiter.response.send(Err(RoutingError::from_status(
                    PublicError::NotLeader,
                    partition_status(replica),
                )));
            }
            ProposalOutcome::Pending => {}
        }
    }
}

fn fail_pending(
    replica: &mut ReplicatedPartition,
    pending: &mut BTreeMap<ProposalId, PendingProduce>,
    error: PublicError,
) {
    let status = partition_status(replica);
    for (id, waiter) in std::mem::take(pending) {
        let _ = replica.forget_proposal(id);
        let _ = waiter
            .response
            .send(Err(RoutingError::from_status(error, status)));
    }
}

fn complete_pending_idempotent(
    local_broker_id: u32,
    replica: &mut ReplicatedPartition,
    pending: &mut BTreeMap<ProposalId, PendingIdempotentProduce>,
) {
    let ids = pending.keys().copied().collect::<Vec<_>>();
    for id in ids {
        if let Some(waiter) = pending.get_mut(&id) {
            waiter.responses.retain(|response| !response.is_closed());
        }
        let Some(outcome) = replica.proposal_outcome(id) else {
            pending.remove(&id);
            continue;
        };
        if outcome == ProposalOutcome::Pending {
            continue;
        }
        let _ = replica.take_completed(id);
        let Some(waiter) = pending.remove(&id) else {
            continue;
        };
        match outcome {
            ProposalOutcome::QuorumCommitted => {
                let result = ProduceResult {
                    base_offset: waiter.base_offset,
                    last_offset: waiter.last_offset,
                    raft_index: waiter.raft_index,
                    leader_broker_id: local_broker_id,
                    leader_epoch: waiter.leader_epoch,
                };
                for response in waiter.responses {
                    let _ = response.send(Ok(result));
                }
            }
            ProposalOutcome::LocalDurable => {
                let error = RoutingError::from_status(
                    PublicError::UnknownServerError,
                    partition_status(replica),
                );
                for response in waiter.responses {
                    let _ = response.send(Err(IdempotentProduceAttemptError::Routing(error)));
                }
            }
            ProposalOutcome::LeadershipLost | ProposalOutcome::Rejected => {
                let error =
                    RoutingError::from_status(PublicError::NotLeader, partition_status(replica));
                for response in waiter.responses {
                    let _ = response.send(Err(IdempotentProduceAttemptError::Routing(error)));
                }
            }
            ProposalOutcome::Pending => {}
        }
    }
}

fn fail_pending_idempotent(
    replica: &mut ReplicatedPartition,
    pending: &mut BTreeMap<ProposalId, PendingIdempotentProduce>,
    error: PublicError,
) {
    let status = partition_status(replica);
    for (id, waiter) in std::mem::take(pending) {
        let _ = replica.forget_proposal(id);
        let routing = RoutingError::from_status(error, status);
        for response in waiter.responses {
            let _ = response.send(Err(IdempotentProduceAttemptError::Routing(routing)));
        }
    }
}

fn routing_error_for_idempotent(
    error: IdempotentProduceError,
    status: PartitionStatus,
) -> RoutingError {
    let public = match error {
        IdempotentProduceError::ControllerValidationRequired => PublicError::BrokerNotAvailable,
        IdempotentProduceError::UnknownProducer => PublicError::UnknownProducer,
        IdempotentProduceError::ProducerFenced => PublicError::ProducerFenced,
        IdempotentProduceError::OutOfOrderSequence => PublicError::OutOfOrderSequence,
        IdempotentProduceError::InvalidAckMode => PublicError::InvalidAckMode,
        IdempotentProduceError::InvalidTimestamp => PublicError::InvalidRequest,
        IdempotentProduceError::NotLeader => PublicError::NotLeader,
        IdempotentProduceError::StaleLeaderEpoch => PublicError::StaleEpoch,
        IdempotentProduceError::Busy(_) => PublicError::BrokerBusy,
        IdempotentProduceError::Replication(
            astralog_replication::ReplicationError::Busy(_)
            | astralog_replication::ReplicationError::LimitExceeded { .. },
        ) => PublicError::BrokerBusy,
        IdempotentProduceError::Replication(_) => PublicError::StorageError,
    };
    RoutingError::from_status(public, status)
}

fn flush_outbound(
    group: GroupKey,
    replica: &mut ReplicatedPartition,
    transport: &PeerTransportSender,
    rebootstrap_limits: RebootstrapLimits,
    rebootstrap_v2_enabled: bool,
) -> Result<Option<bool>> {
    let outbound = replica.drain_outbound().collect::<Vec<_>>();
    if outbound.is_empty() {
        return Ok(None);
    }
    let mut congested = false;
    for message in outbound {
        let target_broker = node_to_broker(message.target)?;
        let result = if let astralog_raft::RaftMessage::InstallSnapshotRequest(request) =
            &message.message
        {
            // Opt-in diagnostics only: distinguish legitimate V1 recovery,
            // V2 transfer, and conservative V2 deferral without changing
            // Raft safety or retry semantics. Set ASTRALOG_P9G_TRACE=1 only
            // when reproducing a suspected P9-G rebootstrap liveness issue.
            #[cfg(debug_assertions)]
            let trace = std::env::var_os("ASTRALOG_P9G_TRACE").is_some();
            // P9-G opt-in does not convert historical P4 V1 partitions into
            // manifest-governed history. Until a P9 COW/adoption publishes
            // an authoritative manifest, the existing contiguous P4
            // RebootstrapPackage V1 remains the only valid source format.
            let manifest_governed_v2 =
                rebootstrap_v2_enabled && replica.manifest_generation().is_some();
            // A retained V2 generation is deliberately exported only at one
            // exact committed/snapshotted Raft boundary. A live leader may
            // have committed more entries since P3 queued this request. The
            // old request is then OBSOLETE, not a corrupt source and not a
            // reason to kill the actor. Advance the durable P3 snapshot, and
            // let the next normal Raft heartbeat send its new request. While
            // an uncommitted suffix exists, also defer instead of producing
            // a package whose P3 snapshot identity cannot cover its files.
            if manifest_governed_v2 {
                let committed = replica.durable_commit_index();
                if committed > request.snapshot.last_included_index {
                    #[cfg(debug_assertions)]
                    if trace {
                        eprintln!(
                            "P9G_SNAPSHOT_ROUTE decision=defer_refresh target={target_broker} group={group:?} request={:?} committed={committed:?} manifest={:?}",
                            request.snapshot.last_included_index,
                            replica.manifest_generation(),
                        );
                    }
                    // A joint configuration or an in-flight snapshot may
                    // prohibit a refresh right now; neither grants a V2
                    // publication. Future Raft ticks retry this path.
                    let _ = replica
                        .snapshot_committed_prefix_for_retention(replica.leader_epoch(), now_ms());
                    continue;
                }
                if replica.membership_observation().durable_last_index
                    > request.snapshot.last_included_index
                {
                    #[cfg(debug_assertions)]
                    if trace {
                        eprintln!(
                            "P9G_SNAPSHOT_ROUTE decision=defer_uncommitted_tail target={target_broker} group={group:?} request={:?} durable_last={:?} manifest={:?}",
                            request.snapshot.last_included_index,
                            replica.membership_observation().durable_last_index,
                            replica.manifest_generation(),
                        );
                    }
                    continue;
                }
            }
            #[cfg(debug_assertions)]
            if trace {
                eprintln!(
                    "P9G_SNAPSHOT_ROUTE decision={} target={target_broker} group={group:?} request={:?} committed={:?} manifest={:?}",
                    if manifest_governed_v2 {
                        "send_v2"
                    } else {
                        "send_v1"
                    },
                    request.snapshot.last_included_index,
                    replica.durable_commit_index(),
                    replica.manifest_generation(),
                );
            }
            let encoded = if manifest_governed_v2 {
                let limits = RebootstrapLimitsV2::default();
                let package = replica.build_rebootstrap_package_v2(&request.snapshot, limits)?;
                encode_rebootstrap_package_v2(&package, limits)?
            } else {
                let package = replica.build_rebootstrap_package(&request.snapshot)?;
                encode_rebootstrap_package(&package, rebootstrap_limits)?
            };
            transport.try_send_rebootstrap(target_broker, group, message.message, encoded)
        } else {
            transport.try_send(target_broker, group, message.message)
        };
        match result {
            Ok(()) => {}
            Err(astralog_replication::ReplicationError::Busy(_)) => {
                congested = true;
            }
            Err(error) => return Err(error.into()),
        }
    }
    Ok(Some(congested))
}

fn partition_status(replica: &ReplicatedPartition) -> PartitionStatus {
    let role = replica.role();
    let leader_node = if role == Role::Leader {
        Some(replica.local_node())
    } else {
        replica.leader()
    };
    PartitionStatus {
        role,
        leader_broker_id: leader_node.and_then(|node| node_to_broker(node).ok()),
        leader_epoch: replica.leader_epoch(),
        current_term_committed: replica.current_term_committed(),
        log_start_offset: replica
            .committed_log_start_offset()
            .map(StreamOffset::get)
            .unwrap_or(0),
        high_watermark: replica
            .committed_stream_high_watermark()
            .map(StreamOffset::get)
            .unwrap_or(0),
    }
}

fn validate_leader_request(
    expected_epoch: Option<u64>,
    status: PartitionStatus,
) -> std::result::Result<(), RoutingError> {
    if expected_epoch.is_some_and(|epoch| epoch != status.leader_epoch) {
        return Err(RoutingError::from_status(PublicError::StaleEpoch, status));
    }
    if status.role != Role::Leader {
        return Err(RoutingError::from_status(PublicError::NotLeader, status));
    }
    Ok(())
}

fn local_groups(
    topology: &StaticReplicationConfig,
    local_broker_id: u32,
) -> Result<BTreeSet<GroupKey>> {
    if topology.blocked_broker_ids.contains(&local_broker_id) {
        return Ok(BTreeSet::new());
    }
    if !topology.actor_brokers.is_empty() {
        return Ok(topology
            .actor_brokers
            .iter()
            .filter_map(|(group, brokers)| brokers.contains(&local_broker_id).then_some(*group))
            .collect());
    }
    let mut groups = BTreeSet::new();
    for topic in &topology.topics {
        for (partition, replicas) in topic.partition_replicas.iter().enumerate() {
            if replicas.contains(&local_broker_id) {
                groups.insert(GroupKey {
                    topic_id: topic.topic_id,
                    partition: u32::try_from(partition).map_err(|_| {
                        BrokerError::InvalidConfig("static partition index overflow")
                    })?,
                });
            }
        }
    }
    Ok(groups)
}

fn assignment_for_group(topology: &StaticReplicationConfig, group: GroupKey) -> Result<&[u32]> {
    let topic = topology
        .topic_by_id(group.topic_id)
        .ok_or(BrokerError::InvalidConfig("unknown static topic id"))?;
    let index = usize::try_from(group.partition)
        .map_err(|_| BrokerError::InvalidConfig("static partition index overflow"))?;
    topic
        .partition_replicas
        .get(index)
        .map(Vec::as_slice)
        .ok_or(BrokerError::InvalidConfig("unknown static partition"))
}

pub(crate) fn broker_to_node(broker_id: u32) -> Result<NodeId> {
    if broker_id == u32::MAX {
        return Err(BrokerError::InvalidConfig(
            "u32::MAX broker id is reserved by P4 NodeId mapping",
        ));
    }
    Ok(NodeId::new(u64::from(broker_id) + 1)?)
}

pub(crate) fn broker_stable_configuration(
    voters: &[u32],
    learners: &[u32],
) -> Result<StableConfiguration> {
    let voters = voters
        .iter()
        .copied()
        .map(broker_to_node)
        .collect::<Result<Vec<_>>>()?;
    let learners = learners
        .iter()
        .copied()
        .map(broker_to_node)
        .collect::<Result<Vec<_>>>()?;
    Ok(StableConfiguration::new(voters, learners)?)
}

pub(crate) fn node_to_broker_id(node: NodeId) -> Result<u32> {
    node_to_broker(node)
}

fn node_to_broker(node: NodeId) -> Result<u32> {
    let raw = node
        .get()
        .checked_sub(1)
        .ok_or(BrokerError::InvalidConfig("invalid P4 NodeId mapping"))?;
    u32::try_from(raw).map_err(|_| BrokerError::InvalidConfig("P4 NodeId exceeds broker id range"))
}

fn sample_election_timeout(timing: TimingConfig) -> Result<u64> {
    let mut bytes = [0_u8; 8];
    getrandom::fill(&mut bytes)?;
    let raw = u64::from_le_bytes(bytes);
    let width = timing
        .election_timeout_max
        .checked_sub(timing.election_timeout_min)
        .and_then(|delta| delta.checked_add(1))
        .ok_or(BrokerError::InvalidConfig(
            "election timeout range overflow",
        ))?;
    Ok(timing.election_timeout_min + raw % width)
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

#[cfg(test)]
mod p5j_tests {
    use std::{
        fs,
        net::TcpListener,
        sync::atomic::{AtomicU64, Ordering},
    };

    use astralog_controller::{
        AssignmentGeneration, AssignmentRecord, BrokerLifecycle, ControllerCommand,
        FailureDomainPolicy, MovementId, MovementStage,
    };

    use super::*;

    static TEMP_ID: AtomicU64 = AtomicU64::new(1);

    fn temp_dir(label: &str) -> PathBuf {
        let id = TEMP_ID.fetch_add(1, Ordering::Relaxed);
        let path =
            std::env::temp_dir().join(format!("astralog-p5j-{label}-{}-{id}", std::process::id()));
        let _ = fs::remove_dir_all(&path);
        fs::create_dir_all(&path).unwrap();
        path
    }

    fn reserve_addr() -> SocketAddr {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        listener.local_addr().unwrap()
    }

    fn register(broker_id: u32, client_port: u16, raft: SocketAddr) -> ControllerCommand {
        ControllerCommand::RegisterBroker {
            broker_id,
            node_uuid: [broker_id as u8; 16],
            initial_incarnation: 1,
            client_host: "127.0.0.1".into(),
            client_port,
            internal_host: raft.ip().to_string(),
            internal_port: raft.port(),
            rack: Some(format!("rack-{broker_id}")),
            zone: Some(format!("zone-{broker_id}")),
            min_feature: 1,
            max_feature: 1,
        }
    }

    fn activate(broker_id: u32) -> ControllerCommand {
        ControllerCommand::SetBrokerLifecycle {
            broker_id,
            expected_incarnation: 1,
            expected_lifecycle: BrokerLifecycle::Joining,
            new_lifecycle: BrokerLifecycle::Active,
        }
    }

    #[test]
    fn p8e_static_runtime_rejects_tick_slower_than_group_deadline() {
        let raft = reserve_addr();
        let broker = StaticBrokerConfig {
            broker_id: 1,
            node_uuid: [1; 16],
            client_host: "127.0.0.1".into(),
            client_port: 19092,
            raft_address: raft,
            transport_incarnation: 1,
        };
        let mut config = StaticReplicationConfig::new([1; 16], raft, vec![broker], Vec::new());
        assert_eq!(
            config.replica_limits.max_pending_persistence_batches,
            DEFAULT_PERSISTENCE_GROUP_TOKENS
        );
        assert_eq!(
            config.replica_limits.max_pending_persistence_bytes,
            DEFAULT_PERSISTENCE_GROUP_BYTES
        );
        assert_eq!(
            config.replica_limits.max_persistence_group_delay_ms,
            DEFAULT_PERSISTENCE_GROUP_DELAY_MS
        );
        config.tick_interval =
            Duration::from_millis(DEFAULT_PERSISTENCE_GROUP_DELAY_MS.saturating_add(1));
        assert!(matches!(
            config.validate_for_broker(1, "127.0.0.1", 19092),
            Err(BrokerError::InvalidConfig(
                "replication tick interval exceeds persistence group-delay bound"
            ))
        ));
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn dynamic_manager_consumes_assignments_updates_incarnation_and_preserves_orphans() {
        let root = temp_dir("dynamic-manager");
        let local_raft = reserve_addr();
        let remote_raft = reserve_addr();
        let topic_id = TopicId::new([9; 16]);
        let movement_id = MovementId::new([7; 16]).unwrap();
        let mut state = ControllerState::new([5; 16], 1, 1).unwrap();
        state.apply(1, &register(1, 19092, local_raft)).unwrap();
        state.apply(2, &activate(1)).unwrap();
        state.apply(3, &register(2, 29092, remote_raft)).unwrap();
        state.apply(4, &activate(2)).unwrap();
        state
            .apply(
                5,
                &ControllerCommand::CreateTopic {
                    topic_id,
                    topic_name: "orders".into(),
                    partition_count: 1,
                    replication_factor: 1,
                    failure_domain_policy: FailureDomainPolicy::Disabled,
                    assignments: vec![AssignmentRecord {
                        topic_id,
                        partition_id: 0,
                        assignment_generation: AssignmentGeneration::new(1).unwrap(),
                        voters: vec![1],
                        learners: Vec::new(),
                    }],
                },
            )
            .unwrap();

        let mut config = DynamicReplicationConfig::new(local_raft);
        config.tick_interval = Duration::from_millis(10);
        let mut manager =
            DynamicReplicationManager::start(config, root.clone(), 1, "127.0.0.1", 19092, &state)
                .await
                .unwrap();
        let client = manager.client();
        assert!(client.is_local_replica(topic_id, 0));
        let orphan_path = partition_directory(&root, topic_id, 0);
        assert!(orphan_path.exists());

        state
            .apply(
                6,
                &ControllerCommand::RejoinBroker {
                    broker_id: 2,
                    node_uuid: [2; 16],
                    expected_incarnation: 1,
                    new_incarnation: 2,
                    client_host: "127.0.0.1".into(),
                    client_port: 29092,
                    internal_host: remote_raft.ip().to_string(),
                    internal_port: remote_raft.port(),
                    rack: Some("rack-2".into()),
                    zone: Some("zone-2".into()),
                    min_feature: 1,
                    max_feature: 1,
                },
            )
            .unwrap();
        assert!(manager.reconcile(&state).await.unwrap());
        assert_eq!(
            client
                .topology_snapshot()
                .brokers
                .iter()
                .find(|broker| broker.broker_id == 2)
                .unwrap()
                .transport_incarnation,
            2
        );

        state
            .apply(
                7,
                &ControllerCommand::BeginMovement {
                    movement_id,
                    topic_id,
                    partition_id: 0,
                    expected_assignment_generation: AssignmentGeneration::new(1).unwrap(),
                    source_broker_id: 1,
                    destination_broker_id: 2,
                    target_voters: vec![2],
                    preferred_leader: None,
                },
            )
            .unwrap();
        assert!(!manager.reconcile(&state).await.unwrap());
        assert!(client.is_local_replica(topic_id, 0));

        let stages = [
            (MovementStage::Planned, MovementStage::LearnerRequested),
            (
                MovementStage::LearnerRequested,
                MovementStage::LearnerCreated,
            ),
            (MovementStage::LearnerCreated, MovementStage::CatchingUp),
            (MovementStage::CatchingUp, MovementStage::PromotionReady),
            (
                MovementStage::PromotionReady,
                MovementStage::EnterJointRequested,
            ),
            (
                MovementStage::EnterJointRequested,
                MovementStage::JointCommitted,
            ),
            (
                MovementStage::JointCommitted,
                MovementStage::LeaveJointRequested,
            ),
            (
                MovementStage::LeaveJointRequested,
                MovementStage::NewStableCommitted,
            ),
            (
                MovementStage::NewStableCommitted,
                MovementStage::OldReplicaRemoveRequested,
            ),
            (
                MovementStage::OldReplicaRemoveRequested,
                MovementStage::OldReplicaRemoved,
            ),
        ];
        let mut index = 8_u64;
        for (from, to) in stages {
            state
                .apply(
                    index,
                    &ControllerCommand::AdvanceMovement {
                        movement_id,
                        expected_stage: from,
                        new_stage: to,
                    },
                )
                .unwrap();
            index += 1;
        }
        state
            .apply(
                index,
                &ControllerCommand::FinalizeMovement {
                    movement_id,
                    expected_stage: MovementStage::OldReplicaRemoved,
                    replacement: AssignmentRecord {
                        topic_id,
                        partition_id: 0,
                        assignment_generation: AssignmentGeneration::new(2).unwrap(),
                        voters: vec![2],
                        learners: Vec::new(),
                    },
                },
            )
            .unwrap();
        assert!(manager.reconcile(&state).await.unwrap());
        assert!(!client.is_local_replica(topic_id, 0));
        assert!(
            orphan_path.exists(),
            "removed replica bytes must remain orphaned"
        );

        manager.shutdown().await;
        fs::remove_dir_all(root).unwrap();
    }

    #[tokio::test]
    async fn dynamic_topology_fences_committed_fenced_broker_without_rewriting_voter_identity() {
        let local_raft = reserve_addr();
        let remote_raft = reserve_addr();
        let topic_id = TopicId::new([8; 16]);
        let mut state = ControllerState::new([5; 16], 1, 1).unwrap();
        state.apply(1, &register(1, 19092, local_raft)).unwrap();
        state.apply(2, &activate(1)).unwrap();
        state.apply(3, &register(2, 29092, remote_raft)).unwrap();
        state.apply(4, &activate(2)).unwrap();
        state
            .apply(
                5,
                &ControllerCommand::CreateTopic {
                    topic_id,
                    topic_name: "audit".into(),
                    partition_count: 1,
                    replication_factor: 1,
                    failure_domain_policy: FailureDomainPolicy::Disabled,
                    assignments: vec![AssignmentRecord {
                        topic_id,
                        partition_id: 0,
                        assignment_generation: AssignmentGeneration::new(1).unwrap(),
                        voters: vec![1],
                        learners: Vec::new(),
                    }],
                },
            )
            .unwrap();
        state
            .apply(
                6,
                &ControllerCommand::SetBrokerLifecycle {
                    broker_id: 2,
                    expected_incarnation: 1,
                    expected_lifecycle: BrokerLifecycle::Active,
                    new_lifecycle: BrokerLifecycle::Fenced,
                },
            )
            .unwrap();

        let topology = build_dynamic_topology(
            &DynamicReplicationConfig::new(local_raft),
            &state,
            1,
            "127.0.0.1",
            19092,
        )
        .await
        .unwrap();
        assert!(topology.blocked_broker_ids.contains(&2));
        assert!(topology.brokers.iter().any(|broker| broker.broker_id == 2));
        assert_eq!(topology.topics[0].partition_replicas[0], vec![1]);
    }

    #[tokio::test]
    async fn p9g_manager_restarts_actor_on_feature_activation_without_assignment_change() {
        let root = temp_dir("p9g-controller-activation-refresh");
        let local_raft = reserve_addr();
        let topic_id = TopicId::new([0xc7; 16]);
        let group = GroupKey {
            topic_id,
            partition: 0,
        };
        let mut state = ControllerState::new([5; 16], 1, 1).unwrap();
        let mut registered = register(1, 19092, local_raft);
        if let ControllerCommand::RegisterBroker { max_feature, .. } = &mut registered {
            *max_feature = 5;
        }
        state.apply(1, &registered).unwrap();
        state.apply(2, &activate(1)).unwrap();
        state
            .apply(
                3,
                &ControllerCommand::CreateTopic {
                    topic_id,
                    topic_name: "p9g-enable-rf1".into(),
                    partition_count: 1,
                    replication_factor: 1,
                    failure_domain_policy: FailureDomainPolicy::Disabled,
                    assignments: vec![AssignmentRecord {
                        topic_id,
                        partition_id: 0,
                        assignment_generation: AssignmentGeneration::new(1).unwrap(),
                        voters: vec![1],
                        learners: Vec::new(),
                    }],
                },
            )
            .unwrap();
        let mut config = DynamicReplicationConfig::new(local_raft);
        config.tick_interval = Duration::from_millis(10);
        config.enable_p9g_progress_reports = true;
        config.enable_p9g_rebootstrap_v2 = true;
        let mut manager =
            DynamicReplicationManager::start(config, root.clone(), 1, "127.0.0.1", 19092, &state)
                .await
                .unwrap();
        let before = manager.client().topology_snapshot();
        assert!(before.progress_authorities.is_empty());
        assert!(!before.rebootstrap_v2_enabled);
        state
            .apply(
                4,
                &ControllerCommand::SetFeatureLevel {
                    expected_feature_level: 1,
                    new_feature_level: 5,
                },
            )
            .unwrap();
        assert!(
            manager.reconcile(&state).await.unwrap(),
            "new committed P9 authority requires rebuilding existing actor, not a routing-only noop"
        );
        let after = manager.client().topology_snapshot();
        assert_eq!(before.brokers, after.brokers);
        assert_eq!(before.topics, after.topics);
        assert_eq!(after.progress_authorities[&group].assignment_generation, 1);
        assert!(after.rebootstrap_v2_enabled);
        assert!(
            !manager.reconcile(&state).await.unwrap(),
            "unchanged P9 authority must avoid gratuitous restarts"
        );
        manager.shutdown().await;
        fs::remove_dir_all(root).unwrap();
    }

    #[tokio::test]
    async fn p9g_controller_rf3_sideband_requires_opt_in_feature_and_new_incarnation() {
        let addresses = [reserve_addr(), reserve_addr(), reserve_addr()];
        let topic_id = TopicId::new([0xa7; 16]);
        let group = GroupKey {
            topic_id,
            partition: 0,
        };
        let mut state = ControllerState::new([5; 16], 1, 1).unwrap();
        for broker_id in 1..=3_u32 {
            let idx = usize::try_from(broker_id - 1).unwrap();
            let mut registration = register(broker_id, 19092 + broker_id as u16, addresses[idx]);
            if let ControllerCommand::RegisterBroker { max_feature, .. } = &mut registration {
                *max_feature = 5;
            }
            state
                .apply(u64::from(broker_id) * 2 - 1, &registration)
                .unwrap();
            state
                .apply(u64::from(broker_id) * 2, &activate(broker_id))
                .unwrap();
        }
        state
            .apply(
                7,
                &ControllerCommand::CreateTopic {
                    topic_id,
                    topic_name: "rf3-sideband".into(),
                    partition_count: 1,
                    replication_factor: 3,
                    failure_domain_policy: FailureDomainPolicy::Disabled,
                    assignments: vec![AssignmentRecord {
                        topic_id,
                        partition_id: 0,
                        assignment_generation: AssignmentGeneration::new(1).unwrap(),
                        voters: vec![1, 2, 3],
                        learners: Vec::new(),
                    }],
                },
            )
            .unwrap();
        let mut config = DynamicReplicationConfig::new(addresses[0]);
        assert!(!config.enable_p9g_progress_reports);
        let prior = build_dynamic_topology(&config, &state, 1, "127.0.0.1", 19093)
            .await
            .unwrap();
        assert!(prior.progress_authorities.is_empty());
        config.enable_p9g_progress_reports = true;
        let unactivated = build_dynamic_topology(&config, &state, 1, "127.0.0.1", 19093)
            .await
            .unwrap();
        assert!(
            unactivated.progress_authorities.is_empty(),
            "P5 alone does not activate P9-G"
        );
        state
            .apply(
                8,
                &ControllerCommand::SetFeatureLevel {
                    expected_feature_level: 1,
                    new_feature_level: 5,
                },
            )
            .unwrap();
        let activated = build_dynamic_topology(&config, &state, 1, "127.0.0.1", 19093)
            .await
            .unwrap();
        let authority = activated.progress_authorities.get(&group).unwrap();
        assert_eq!(authority.assignment_generation, 1);
        assert_eq!(authority.voter_identities.len(), 3);
        for broker_id in 1..=3_u32 {
            assert_eq!(
                authority.voter_identities[&broker_to_node(broker_id).unwrap()],
                VoterIdentity {
                    broker_id,
                    incarnation: 1
                }
            );
        }
        let remote = addresses[1];
        state
            .apply(
                9,
                &ControllerCommand::RejoinBroker {
                    broker_id: 2,
                    node_uuid: [2; 16],
                    expected_incarnation: 1,
                    new_incarnation: 2,
                    client_host: "127.0.0.1".into(),
                    client_port: 19094,
                    internal_host: remote.ip().to_string(),
                    internal_port: remote.port(),
                    rack: Some("rack-2".into()),
                    zone: Some("zone-2".into()),
                    min_feature: 1,
                    max_feature: 5,
                },
            )
            .unwrap();
        let rotated = build_dynamic_topology(&config, &state, 1, "127.0.0.1", 19093)
            .await
            .unwrap();
        assert_eq!(
            rotated.progress_authorities[&group].voter_identities[&broker_to_node(2).unwrap()]
                .incarnation,
            2
        );
        assert_eq!(
            authority.voter_identities[&broker_to_node(2).unwrap()].incarnation,
            1,
            "old static P5 evidence must not mutate when authority refreshes"
        );
    }

    #[tokio::test]
    async fn p9g_committed_movement_identity_pool_covers_joint_union_and_post_exclusion() {
        let addresses = [
            reserve_addr(),
            reserve_addr(),
            reserve_addr(),
            reserve_addr(),
        ];
        let topic_id = TopicId::new([0xb7; 16]);
        let group = GroupKey {
            topic_id,
            partition: 0,
        };
        let mut state = ControllerState::new([5; 16], 1, 1).unwrap();
        let mut index = 1_u64;
        for broker_id in 1..=4_u32 {
            let mut registration = register(
                broker_id,
                20000 + broker_id as u16,
                addresses[usize::try_from(broker_id - 1).unwrap()],
            );
            if let ControllerCommand::RegisterBroker { max_feature, .. } = &mut registration {
                *max_feature = 5;
            }
            state.apply(index, &registration).unwrap();
            index += 1;
            state.apply(index, &activate(broker_id)).unwrap();
            index += 1;
        }
        state
            .apply(
                index,
                &ControllerCommand::CreateTopic {
                    topic_id,
                    topic_name: "p9g-p5-joint-pool".into(),
                    partition_count: 1,
                    replication_factor: 3,
                    failure_domain_policy: FailureDomainPolicy::Disabled,
                    assignments: vec![AssignmentRecord {
                        topic_id,
                        partition_id: 0,
                        assignment_generation: AssignmentGeneration::new(1).unwrap(),
                        voters: vec![1, 2, 3],
                        learners: vec![],
                    }],
                },
            )
            .unwrap();
        index += 1;
        state
            .apply(
                index,
                &ControllerCommand::SetFeatureLevel {
                    expected_feature_level: 1,
                    new_feature_level: 5,
                },
            )
            .unwrap();
        index += 1;

        let mut config = DynamicReplicationConfig::new(addresses[0]);
        config.enable_p9g_progress_reports = true;
        config.enable_p9g_rebootstrap_v2 = true;
        let before = build_dynamic_topology(&config, &state, 1, "127.0.0.1", 20001)
            .await
            .unwrap();
        assert_eq!(
            before.progress_authorities[&group].voter_identities.len(),
            3,
        );

        let movement_id = MovementId::new([0x72; 16]).unwrap();
        state
            .apply(
                index,
                &ControllerCommand::BeginMovement {
                    movement_id,
                    topic_id,
                    partition_id: 0,
                    expected_assignment_generation: AssignmentGeneration::new(1).unwrap(),
                    source_broker_id: 1,
                    destination_broker_id: 4,
                    target_voters: vec![2, 3, 4],
                    preferred_leader: Some(2),
                },
            )
            .unwrap();
        index += 1;
        let mut stage = MovementStage::Planned;
        for next in [
            MovementStage::LearnerRequested,
            MovementStage::LearnerCreated,
            MovementStage::CatchingUp,
            MovementStage::PromotionReady,
            MovementStage::EnterJointRequested,
            MovementStage::JointCommitted,
        ] {
            state
                .apply(
                    index,
                    &ControllerCommand::AdvanceMovement {
                        movement_id,
                        expected_stage: stage,
                        new_stage: next,
                    },
                )
                .unwrap();
            stage = next;
            index += 1;
        }
        let during = build_dynamic_topology(&config, &state, 1, "127.0.0.1", 20001)
            .await
            .unwrap();
        let authority = &during.progress_authorities[&group];
        assert_eq!(authority.assignment_generation, 1);
        assert_eq!(authority.voter_identities.len(), 4);
        let old = StableConfiguration::new(
            [
                broker_to_node(1).unwrap(),
                broker_to_node(2).unwrap(),
                broker_to_node(3).unwrap(),
            ],
            [],
        )
        .unwrap();
        let new = StableConfiguration::new(
            [
                broker_to_node(2).unwrap(),
                broker_to_node(3).unwrap(),
                broker_to_node(4).unwrap(),
            ],
            [],
        )
        .unwrap();
        let joint = astralog_raft::EffectiveConfiguration::Joint(
            astralog_raft::JointConfiguration::new(old.clone(), new.clone()).unwrap(),
        );
        assert!(
            p9g_effective_voters_carry_committed_p5_authority(&joint, authority),
            "joint P3 union must retain all still-committed P5 voters"
        );
        let mut reports = CleanProgressTracker::from_committed_assignment(
            topic_id,
            PartitionId::new(0),
            17,
            authority.assignment_generation,
            &joint,
            &authority.voter_identities,
        )
        .unwrap();
        assert_eq!(reports.required_voter_count(), 4);
        for broker_id in 1..=3_u32 {
            let report = CleanProgressReport {
                topic_id,
                partition_id: PartitionId::new(0),
                term: 17,
                leader_epoch: 17,
                source_broker_id: broker_id,
                source_incarnation: 1,
                assignment_generation: 1,
                clean_compacted_offset: StreamOffset::new(50),
            };
            assert!(reports.accept(
                broker_to_node(broker_id).unwrap(),
                broker_id,
                1,
                report,
                StreamOffset::new(70),
            ));
        }
        assert_eq!(
            reports.candidate_safe_offset(),
            None,
            "P3 joint voter destination is mandatory even though P5 stable assignment is still old"
        );
        let destination = CleanProgressReport {
            topic_id,
            partition_id: PartitionId::new(0),
            term: 17,
            leader_epoch: 17,
            source_broker_id: 4,
            source_incarnation: 1,
            assignment_generation: 1,
            clean_compacted_offset: StreamOffset::new(30),
        };
        assert!(
            !reports.accept(
                broker_to_node(4).unwrap(),
                4,
                2,
                destination,
                StreamOffset::new(70),
            ),
            "forged destination incarnation must not unblock joint safety"
        );
        assert_eq!(reports.candidate_safe_offset(), None);
        assert!(reports.accept(
            broker_to_node(4).unwrap(),
            4,
            1,
            destination,
            StreamOffset::new(70),
        ));
        assert_eq!(reports.candidate_safe_offset(), Some(StreamOffset::new(30)));
        assert!(reports.is_current_for_voter_identities(
            topic_id,
            PartitionId::new(0),
            17,
            1,
            &joint,
            &authority.voter_identities,
        ));
        let stable_old = astralog_raft::EffectiveConfiguration::Stable(old);
        assert!(
            p9g_effective_voters_carry_committed_p5_authority(&stable_old, authority),
            "original P3 stable configuration remains eligible before joint transition"
        );
        assert!(
            !reports.is_current_for_voter_identities(
                topic_id,
                PartitionId::new(0),
                17,
                1,
                &stable_old,
                &authority.voter_identities,
            ),
            "a joint report cannot be silently reused after P3 voter-set transition"
        );
        let stable_new = astralog_raft::EffectiveConfiguration::Stable(new);
        assert!(
            !p9g_effective_voters_carry_committed_p5_authority(&stable_new, authority),
            "P3 LeaveJoint must NOT exclude old voter from tombstone safety before committed P5 FinalizeMovement"
        );
        let next = CleanProgressTracker::from_committed_assignment(
            topic_id,
            PartitionId::new(0),
            17,
            1,
            &stable_new,
            &authority.voter_identities,
        )
        .unwrap();
        assert_eq!(next.required_voter_count(), 3);
        assert_eq!(
            next.candidate_safe_offset(),
            None,
            "joint evidence must not be inherited across committed P3 stable transition"
        );
        let mut premature = next;
        for broker_id in [2, 3, 4] {
            assert!(premature.accept(
                broker_to_node(broker_id).unwrap(),
                broker_id,
                1,
                CleanProgressReport {
                    assignment_generation: 1,
                    source_broker_id: broker_id,
                    clean_compacted_offset: StreamOffset::new(30),
                    ..destination
                },
                StreamOffset::new(70)
            ));
        }
        assert_eq!(
            premature.candidate_safe_offset(),
            Some(StreamOffset::new(30))
        );
        assert!(
            !p9g_effective_voters_carry_committed_p5_authority(&stable_new, authority),
            "even complete NEW-P3-only progress must be blocked while OLD-P5 voter is still assigned"
        );

        // Complete the committed controller movement. Before FinalizeMovement,
        // source broker 1 remains part of P5's original assignment and P3's
        // joint voter union. Only after the atomic assignment generation bump
        // may its absence stop blocking *future* safe-boundary proposals.
        for later in [
            MovementStage::LeaveJointRequested,
            MovementStage::NewStableCommitted,
            MovementStage::LeadershipTransfer,
            MovementStage::OldReplicaRemoveRequested,
            MovementStage::OldReplicaRemoved,
        ] {
            state
                .apply(
                    index,
                    &ControllerCommand::AdvanceMovement {
                        movement_id,
                        expected_stage: stage,
                        new_stage: later,
                    },
                )
                .unwrap();
            stage = later;
            index += 1;
        }
        state
            .apply(
                index,
                &ControllerCommand::FinalizeMovement {
                    movement_id,
                    expected_stage: MovementStage::OldReplicaRemoved,
                    replacement: AssignmentRecord {
                        topic_id,
                        partition_id: 0,
                        assignment_generation: AssignmentGeneration::new(2).unwrap(),
                        voters: vec![2, 3, 4],
                        learners: vec![],
                    },
                },
            )
            .unwrap();
        let committed = build_dynamic_topology(&config, &state, 1, "127.0.0.1", 20001)
            .await
            .unwrap();
        let replacement = &committed.progress_authorities[&group];
        assert_eq!(replacement.assignment_generation, 2);
        assert_eq!(replacement.voter_identities.len(), 3);
        assert!(
            p9g_effective_voters_carry_committed_p5_authority(&stable_new, replacement),
            "only committed P5 FinalizeMovement authorizes new-stable voter set to form safety minimum"
        );
        assert!(
            !replacement
                .voter_identities
                .contains_key(&broker_to_node(1).unwrap()),
            "removed voter can be excluded only by a committed P5 generation change"
        );
        let mut post = CleanProgressTracker::from_committed_assignment(
            topic_id,
            PartitionId::new(0),
            17,
            replacement.assignment_generation,
            &stable_new,
            &replacement.voter_identities,
        )
        .unwrap();
        assert_eq!(post.candidate_safe_offset(), None);
        for broker_id in [2, 3] {
            assert!(post.accept(
                broker_to_node(broker_id).unwrap(),
                broker_id,
                1,
                CleanProgressReport {
                    topic_id,
                    partition_id: PartitionId::new(0),
                    term: 17,
                    leader_epoch: 17,
                    source_broker_id: broker_id,
                    source_incarnation: 1,
                    assignment_generation: 2,
                    clean_compacted_offset: StreamOffset::new(50),
                },
                StreamOffset::new(70),
            ));
        }
        assert!(
            !post.accept(
                broker_to_node(1).unwrap(),
                1,
                1,
                CleanProgressReport {
                    assignment_generation: 2,
                    source_broker_id: 1,
                    ..destination
                },
                StreamOffset::new(70),
            ),
            "old removed voter cannot create or replenish P5-generation-2 safe evidence"
        );
        assert_eq!(post.candidate_safe_offset(), None);
        assert!(!post.accept(
            broker_to_node(4).unwrap(),
            4,
            1,
            destination,
            StreamOffset::new(70),
        ));
        assert_eq!(post.candidate_safe_offset(), None);
        assert!(post.accept(
            broker_to_node(4).unwrap(),
            4,
            1,
            CleanProgressReport {
                assignment_generation: 2,
                ..destination
            },
            StreamOffset::new(70),
        ));
        assert_eq!(post.candidate_safe_offset(), Some(StreamOffset::new(30)));
    }
}
