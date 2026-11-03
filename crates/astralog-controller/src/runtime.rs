use std::{
    collections::{BTreeMap, BTreeSet, VecDeque},
    path::PathBuf,
    time::Duration,
};

use astralog_common::{ProducerAuthority, ProducerEpoch, ProducerId};
use astralog_raft::{
    Effect, EffectiveConfiguration, EntryKind, Input, LogIndex, NodeId, PersistenceFailure,
    PersistenceOp, ProposalId, RaftCore, RaftMessage, Role, SnapshotId, SnapshotMetadata,
    StableConfiguration, Term, TimingConfig,
};
use tokio::{
    sync::{mpsc, oneshot},
    task::JoinHandle,
    time,
};

use crate::movement::MovementObservations;
use crate::reassignment::{
    PreparedReassignment, prepare_reassignment, validate_reassignment_capacity,
};
use crate::topic::{
    PreparedTopicCreate, PreparedTopicExpansion, prepare_create, prepare_expansion,
};
use crate::{
    ApplyOutcome, BrokerAuthority, BrokerHeartbeatRequest, BrokerHeartbeatResponse,
    BrokerLivenessObservation, BrokerRecord, CommandLimits, ControllerCommand, ControllerError,
    ControllerLogCommand, ControllerMetadataSnapshot, ControllerNodeId,
    ControllerPeerTransportConfig, ControllerPeerTransportSender, ControllerPersistence,
    ControllerPersistenceConfig, ControllerSnapshotPackage, ControllerState, CreateTopicSpec,
    FailureDetectorConfig, FeatureGate, FeatureRange, HeartbeatLimits, MetadataSyncResponse,
    MovementId, P7_PRODUCER_FEATURE_LEVEL, P8_PERFORMANCE_FEATURE_LEVEL,
    P9_RETENTION_FEATURE_LEVEL, ProducerIdRange, ProducerSessionCommandV2, ReassignmentResult,
    ReassignmentSpec, ReplicaProgressObservation, Result, TopicCreateResult, TopicDeletionResult,
    TopicExpansionResult, TopicExpansionSpec, TopicLifecycle, TopicPerformanceCommandV3,
    TopicPerformanceSpecV1, TopicRecord, TopicRetentionCommandV4, TopicRetentionPolicyV1,
    TopicRetentionSpecV1, decode_controller_log_command, decode_controller_snapshot_package,
    encode_controller_log_command, encode_controller_snapshot_package, heartbeat::FailureDetector,
    metadata::MetadataDisseminator, start_controller_peer_transport, validate_controller_bootstrap,
};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ControllerRuntimeLimits {
    pub max_pending_mutations: usize,
    pub max_pending_mutation_bytes: usize,
    pub max_outbound_messages: usize,
    pub max_outbound_bytes: usize,
    pub max_active_movements: usize,
    pub max_active_movements_per_broker: usize,
}

impl Default for ControllerRuntimeLimits {
    fn default() -> Self {
        Self {
            max_pending_mutations: 4096,
            max_pending_mutation_bytes: 64 * 1024 * 1024,
            max_outbound_messages: 4096,
            max_outbound_bytes: 64 * 1024 * 1024,
            max_active_movements: 64,
            max_active_movements_per_broker: 8,
        }
    }
}

impl ControllerRuntimeLimits {
    fn validate(self) -> Result<()> {
        if self.max_pending_mutations == 0
            || self.max_pending_mutation_bytes == 0
            || self.max_outbound_messages == 0
            || self.max_outbound_bytes == 0
            || self.max_active_movements == 0
            || self.max_active_movements_per_broker == 0
        {
            return Err(ControllerError::Invalid(
                "controller runtime limits must be non-zero",
            ));
        }
        Ok(())
    }
}

#[derive(Debug, Clone)]
pub struct ControllerRuntimeConfig {
    pub local_node: ControllerNodeId,
    pub directory: PathBuf,
    pub static_configuration: StableConfiguration,
    pub controller_feature_ranges: BTreeMap<ControllerNodeId, FeatureRange>,
    pub initial_state: ControllerState,
    pub persistence: ControllerPersistenceConfig,
    pub timing: TimingConfig,
    pub randomized_election_timeout: u64,
    pub limits: ControllerRuntimeLimits,
    pub heartbeat_limits: HeartbeatLimits,
    pub failure_detector: FailureDetectorConfig,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct MutationReceipt {
    pub proposal_id: ProposalId,
    pub raft_index: LogIndex,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MutationOutcome {
    Pending,
    Committed(ApplyOutcome),
    Rejected,
    LeadershipLost,
}

#[derive(Debug)]
struct PendingMutation {
    receipt: MutationReceipt,
    command: ControllerLogCommand,
    encoded_bytes: usize,
    outcome: MutationOutcome,
}

#[derive(Debug, Clone)]
struct ControllerOutbound {
    target: ControllerNodeId,
    message: RaftMessage,
    snapshot_package: Option<Vec<u8>>,
    estimated_bytes: usize,
}

pub struct ControllerRuntime {
    local_node: ControllerNodeId,
    core: RaftCore,
    persistence: ControllerPersistence,
    state: ControllerState,
    validation_state: ControllerState,
    controller_feature_ranges: BTreeMap<ControllerNodeId, FeatureRange>,
    command_limits: CommandLimits,
    snapshot_limits: crate::SnapshotLimits,
    limits: ControllerRuntimeLimits,
    heartbeat_limits: HeartbeatLimits,
    failure_detector: FailureDetector,
    movement_observations: MovementObservations,
    metadata_disseminator: MetadataDisseminator,
    next_proposal_id: u64,
    pending: BTreeMap<ProposalId, PendingMutation>,
    pending_bytes: usize,
    outbound: VecDeque<ControllerOutbound>,
    outbound_bytes: usize,
    transport_congested: bool,
    pending_snapshot: Option<ControllerSnapshotPackage>,
    fatal: Option<String>,
}

impl ControllerRuntime {
    pub fn open(config: ControllerRuntimeConfig) -> Result<Self> {
        config.limits.validate()?;
        config.heartbeat_limits.validate()?;
        config.failure_detector.validate()?;
        if !config
            .static_configuration
            .contains_voter(NodeId::new(config.local_node.get())?)
        {
            return Err(ControllerError::Invalid(
                "local controller is not in static controller voter set",
            ));
        }
        validate_controller_feature_ranges(
            &config.static_configuration,
            &config.controller_feature_ranges,
        )?;
        validate_controller_bootstrap(
            &config.directory,
            *config.initial_state.cluster_id(),
            &config.static_configuration,
        )?;
        let (persistence, recovered, state) = ControllerPersistence::open(
            &config.directory,
            config.static_configuration,
            config.initial_state,
            config.persistence,
        )?;
        validate_active_feature_level(
            &config.controller_feature_ranges,
            state.active_feature_level(),
        )?;
        let mut core = RaftCore::from_recovered(
            NodeId::new(config.local_node.get())?,
            recovered,
            config.timing,
            config.randomized_election_timeout,
        )?;
        finish_recovery_replay(&mut core)?;
        let validation_state = state.clone();
        let failure_detector = FailureDetector::new(config.failure_detector)?;
        let movement_observations = MovementObservations::default();
        let metadata_disseminator = MetadataDisseminator::new();
        Ok(Self {
            local_node: config.local_node,
            core,
            persistence,
            state,
            validation_state,
            controller_feature_ranges: config.controller_feature_ranges,
            command_limits: config.persistence.command_limits,
            snapshot_limits: config.persistence.snapshot_limits,
            limits: config.limits,
            heartbeat_limits: config.heartbeat_limits,
            failure_detector,
            movement_observations,
            metadata_disseminator,
            next_proposal_id: 1,
            pending: BTreeMap::new(),
            pending_bytes: 0,
            outbound: VecDeque::new(),
            outbound_bytes: 0,
            transport_congested: false,
            pending_snapshot: None,
            fatal: None,
        })
    }

    pub const fn local_node(&self) -> ControllerNodeId {
        self.local_node
    }

    pub fn role(&self) -> Role {
        self.core.volatile_state().role
    }

    pub fn leader(&self) -> Option<ControllerNodeId> {
        self.core
            .volatile_state()
            .leader_id
            .and_then(|node| ControllerNodeId::new(node.get()).ok())
    }

    pub fn term(&self) -> Term {
        self.core.hard_state().current_term
    }

    pub const fn state(&self) -> &ControllerState {
        &self.state
    }

    pub fn local_feature_range(&self) -> FeatureRange {
        *self
            .controller_feature_ranges
            .get(&self.local_node)
            .expect("controller feature map is validated at runtime open")
    }

    pub fn controller_feature_ranges(&self) -> &BTreeMap<ControllerNodeId, FeatureRange> {
        &self.controller_feature_ranges
    }

    pub fn broker_record(&self, broker_id: u32) -> Option<&BrokerRecord> {
        self.state.broker(broker_id)
    }

    pub fn validate_broker_authority(&self, authority: BrokerAuthority) -> Result<&BrokerRecord> {
        if self.role() != Role::Leader {
            return Err(ControllerError::NotLeader {
                leader: self.leader().map(ControllerNodeId::get),
            });
        }
        self.state.validate_broker_authority(authority)
    }

    pub fn failure_detector_tick(&mut self, tick: u64) -> Result<()> {
        if self.role() != Role::Leader {
            return Err(ControllerError::NotLeader {
                leader: self.leader().map(ControllerNodeId::get),
            });
        }
        self.failure_detector.inject_tick(tick)
    }

    pub fn heartbeat(
        &mut self,
        request: BrokerHeartbeatRequest,
    ) -> Result<BrokerHeartbeatResponse> {
        if self.role() != Role::Leader {
            return Err(ControllerError::NotLeader {
                leader: self.leader().map(ControllerNodeId::get),
            });
        }
        request.validate_shape(self.heartbeat_limits)?;
        let broker = self.state.validate_broker_authority(request.authority)?;
        request.validate_against_committed(broker, self.state.metadata_epoch())?;
        let metadata_sync = self
            .metadata_disseminator
            .response_for(&self.state, request.applied_metadata_epoch)?;
        let mut staged_detector = self.failure_detector.clone();
        let mut staged_movement_observations = self.movement_observations.clone();
        let observed_at_tick = staged_detector.observe_heartbeat(request.authority)?;
        staged_movement_observations.observe_heartbeat(&self.state, &request)?;
        self.failure_detector = staged_detector;
        self.movement_observations = staged_movement_observations;
        Ok(BrokerHeartbeatResponse {
            controller_id: self.local_node,
            controller_term: self.term().get(),
            controller_metadata_epoch: self.state.metadata_epoch(),
            observed_at_tick,
            metadata_sync,
        })
    }

    pub fn metadata_sync(&self, applied_epoch: u64) -> Result<MetadataSyncResponse> {
        if self.role() != Role::Leader {
            return Err(ControllerError::NotLeader {
                leader: self.leader().map(ControllerNodeId::get),
            });
        }
        self.metadata_disseminator
            .response_for(&self.state, applied_epoch)
    }

    pub fn metadata_snapshot(&self) -> Result<ControllerMetadataSnapshot> {
        if self.role() != Role::Leader {
            return Err(ControllerError::NotLeader {
                leader: self.leader().map(ControllerNodeId::get),
            });
        }
        Ok(ControllerMetadataSnapshot::from_state(&self.state))
    }

    pub fn broker_liveness(&self, broker_id: u32) -> Result<BrokerLivenessObservation> {
        if self.role() != Role::Leader {
            return Err(ControllerError::NotLeader {
                leader: self.leader().map(ControllerNodeId::get),
            });
        }
        let broker = self
            .state
            .broker(broker_id)
            .ok_or(ControllerError::Invalid(
                "unknown broker for liveness observation",
            ))?;
        Ok(self.failure_detector.observation(broker))
    }

    pub fn planner_liveness_snapshot(&self) -> Result<Vec<BrokerLivenessObservation>> {
        if self.role() != Role::Leader {
            return Err(ControllerError::NotLeader {
                leader: self.leader().map(ControllerNodeId::get),
            });
        }
        Ok(self
            .state
            .brokers()
            .values()
            .map(|broker| self.failure_detector.observation(broker))
            .collect())
    }

    pub fn replica_progress_observation(
        &self,
        broker_id: u32,
        topic_id: astralog_common::TopicId,
        partition_id: u32,
        generation: crate::AssignmentGeneration,
    ) -> Result<Option<ReplicaProgressObservation>> {
        if self.role() != Role::Leader {
            return Err(ControllerError::NotLeader {
                leader: self.leader().map(ControllerNodeId::get),
            });
        }
        Ok(self.movement_observations.replica_progress(
            &self.state,
            broker_id,
            topic_id,
            partition_id,
            generation,
        ))
    }

    pub fn topic_record(&self, topic_id: astralog_common::TopicId) -> Option<&TopicRecord> {
        self.state.topics().get(&topic_id)
    }

    pub fn topic_performance_policy(
        &self,
        topic_id: astralog_common::TopicId,
    ) -> Option<&crate::TopicPerformancePolicyV1> {
        self.state.topic_performance_policy(topic_id)
    }

    pub fn topic_retention_policy(
        &self,
        topic_id: astralog_common::TopicId,
    ) -> Option<&TopicRetentionPolicyV1> {
        self.state.topic_retention_policy(topic_id)
    }

    pub fn producer_authority(&self, producer_id: ProducerId) -> Option<ProducerAuthority> {
        self.state.producer_authority(producer_id)
    }

    fn prepare_topic_create(
        &self,
        spec: &CreateTopicSpec,
    ) -> Result<(ControllerCommand, PreparedTopicCreate)> {
        if self.role() != Role::Leader {
            return Err(ControllerError::NotLeader {
                leader: self.leader().map(ControllerNodeId::get),
            });
        }
        let topic_id = self.generate_topic_id()?;
        let liveness = self
            .validation_state
            .brokers()
            .values()
            .map(|broker| self.failure_detector.observation(broker))
            .collect::<Vec<_>>();
        let prepared = prepare_create(&self.validation_state, &liveness, topic_id, spec)?;
        let command = ControllerCommand::CreateTopic {
            topic_id,
            topic_name: prepared.topic.topic_name.clone(),
            partition_count: prepared.topic.partition_count,
            replication_factor: prepared.topic.replication_factor,
            failure_domain_policy: prepared.topic.failure_domain_policy,
            assignments: prepared.plan.assignments.clone(),
        };
        Ok((command, prepared))
    }

    fn prepare_topic_create_with_performance(
        &self,
        spec: &CreateTopicSpec,
        performance: TopicPerformanceSpecV1,
    ) -> Result<(TopicPerformanceCommandV3, PreparedTopicCreate)> {
        if self.role() != Role::Leader {
            return Err(ControllerError::NotLeader {
                leader: self.leader().map(ControllerNodeId::get),
            });
        }
        FeatureGate::new(P8_PERFORMANCE_FEATURE_LEVEL)?
            .require(self.validation_state.active_feature_level())?;
        let topic_id = self.generate_topic_id()?;
        let liveness = self
            .validation_state
            .brokers()
            .values()
            .map(|broker| self.failure_detector.observation(broker))
            .collect::<Vec<_>>();
        let prepared = prepare_create(&self.validation_state, &liveness, topic_id, spec)?;
        let policy = performance.into_policy(topic_id)?;
        let base = ControllerCommand::CreateTopic {
            topic_id,
            topic_name: prepared.topic.topic_name.clone(),
            partition_count: prepared.topic.partition_count,
            replication_factor: prepared.topic.replication_factor,
            failure_domain_policy: prepared.topic.failure_domain_policy,
            assignments: prepared.plan.assignments.clone(),
        };
        Ok((
            TopicPerformanceCommandV3::CreateTopicWithPerformance {
                base,
                performance: policy,
            },
            prepared,
        ))
    }

    fn prepare_topic_create_with_retention(
        &self,
        spec: &CreateTopicSpec,
        performance: TopicPerformanceSpecV1,
        retention: TopicRetentionSpecV1,
    ) -> Result<(TopicRetentionCommandV4, PreparedTopicCreate)> {
        if self.role() != Role::Leader {
            return Err(ControllerError::NotLeader {
                leader: self.leader().map(ControllerNodeId::get),
            });
        }
        FeatureGate::new(P9_RETENTION_FEATURE_LEVEL)?
            .require(self.validation_state.active_feature_level())?;
        let topic_id = self.generate_topic_id()?;
        let liveness = self
            .validation_state
            .brokers()
            .values()
            .map(|broker| self.failure_detector.observation(broker))
            .collect::<Vec<_>>();
        let prepared = prepare_create(&self.validation_state, &liveness, topic_id, spec)?;
        let performance = performance.into_policy(topic_id)?;
        let retention = retention.into_policy(topic_id)?;
        let base = ControllerCommand::CreateTopic {
            topic_id,
            topic_name: prepared.topic.topic_name.clone(),
            partition_count: prepared.topic.partition_count,
            replication_factor: prepared.topic.replication_factor,
            failure_domain_policy: prepared.topic.failure_domain_policy,
            assignments: prepared.plan.assignments.clone(),
        };
        Ok((
            TopicRetentionCommandV4::CreateTopicWithPerformanceAndRetention {
                base,
                performance,
                retention,
            },
            prepared,
        ))
    }

    fn prepare_topic_retention_alter(
        &self,
        topic_id: astralog_common::TopicId,
        retention: TopicRetentionSpecV1,
    ) -> Result<TopicRetentionCommandV4> {
        if self.role() != Role::Leader {
            return Err(ControllerError::NotLeader {
                leader: self.leader().map(ControllerNodeId::get),
            });
        }
        FeatureGate::new(P9_RETENTION_FEATURE_LEVEL)?
            .require(self.validation_state.active_feature_level())?;
        let topic = self
            .validation_state
            .topics()
            .get(&topic_id)
            .ok_or(ControllerError::Invalid("unknown topic"))?;
        if topic.lifecycle != TopicLifecycle::Active {
            return Err(ControllerError::Invalid(
                "retention policy mutation requires ACTIVE topic",
            ));
        }
        let expected = self
            .validation_state
            .topic_retention_policy(topic_id)
            .copied()
            .ok_or(ControllerError::Invalid("topic retention policy missing"))?;
        let new = retention.into_policy(topic_id)?;
        Ok(TopicRetentionCommandV4::AlterTopicRetentionPolicy {
            topic_id,
            expected,
            new,
        })
    }

    fn prepare_topic_expansion(
        &self,
        spec: &TopicExpansionSpec,
    ) -> Result<(ControllerCommand, PreparedTopicExpansion)> {
        if self.role() != Role::Leader {
            return Err(ControllerError::NotLeader {
                leader: self.leader().map(ControllerNodeId::get),
            });
        }
        let liveness = self
            .validation_state
            .brokers()
            .values()
            .map(|broker| self.failure_detector.observation(broker))
            .collect::<Vec<_>>();
        let prepared = prepare_expansion(&self.validation_state, &liveness, spec)?;
        let command = ControllerCommand::ExpandTopic {
            topic_id: prepared.topic_id,
            expected_partition_count: spec.expected_partition_count,
            new_partition_count: spec.new_partition_count,
            assignments: prepared.plan.assignments.clone(),
        };
        Ok((command, prepared))
    }

    fn generate_topic_id(&self) -> Result<astralog_common::TopicId> {
        for _ in 0..16 {
            let mut bytes = [0_u8; 16];
            getrandom::fill(&mut bytes).map_err(|error| {
                ControllerError::InvalidOwned(format!("topic id entropy failure: {error}"))
            })?;
            bytes[6] = (bytes[6] & 0x0f) | 0x40;
            bytes[8] = (bytes[8] & 0x3f) | 0x80;
            let topic_id = astralog_common::TopicId::new(bytes);
            if !self.validation_state.topics().contains_key(&topic_id) {
                return Ok(topic_id);
            }
        }
        Err(ControllerError::Invalid(
            "failed to allocate a unique topic id after bounded retries",
        ))
    }

    fn prepare_reassignment(&self, spec: ReassignmentSpec) -> Result<PreparedReassignment> {
        if self.role() != Role::Leader {
            return Err(ControllerError::NotLeader {
                leader: self.leader().map(ControllerNodeId::get),
            });
        }
        validate_reassignment_capacity(
            &self.validation_state,
            spec,
            self.limits.max_active_movements,
            self.limits.max_active_movements_per_broker,
        )?;
        let movement_id = self.generate_movement_id()?;
        prepare_reassignment(&self.validation_state, movement_id, spec)
    }

    fn generate_movement_id(&self) -> Result<MovementId> {
        for _ in 0..16 {
            let mut bytes = [0_u8; 16];
            getrandom::fill(&mut bytes).map_err(|error| {
                ControllerError::InvalidOwned(format!("movement id entropy failure: {error}"))
            })?;
            bytes[6] = (bytes[6] & 0x0f) | 0x40;
            bytes[8] = (bytes[8] & 0x3f) | 0x80;
            let movement_id = MovementId::new(bytes)?;
            if !self.validation_state.movements().contains_key(&movement_id) {
                return Ok(movement_id);
            }
        }
        Err(ControllerError::Invalid(
            "failed to allocate a unique movement id after bounded retries",
        ))
    }

    pub fn configured_voters(&self) -> BTreeSet<ControllerNodeId> {
        self.core
            .effective_configuration()
            .voters()
            .into_iter()
            .map(|node| ControllerNodeId::new(node.get()).expect("P3 NodeId is non-zero"))
            .collect()
    }

    pub fn is_fatal(&self) -> bool {
        self.fatal.is_some() || self.core.is_stopped()
    }

    pub fn fatal_reason(&self) -> Option<&str> {
        self.fatal.as_deref()
    }

    pub fn tick(&mut self, reset_election_timeout: u64) -> Result<()> {
        self.handle(Input::Tick {
            reset_election_timeout,
        })
    }

    pub fn step(
        &mut self,
        source: ControllerNodeId,
        message: RaftMessage,
        snapshot_package: Option<Vec<u8>>,
    ) -> Result<()> {
        if message_sender(&message) != NodeId::new(source.get())? {
            return Err(ControllerError::Invalid(
                "controller transport source does not match Raft message identity",
            ));
        }
        match (&message, snapshot_package) {
            (RaftMessage::InstallSnapshotRequest(request), Some(bytes)) => {
                let package = decode_controller_snapshot_package(&bytes, self.snapshot_limits)?;
                if package.metadata != request.snapshot {
                    return Err(ControllerError::Invalid(
                        "controller snapshot package metadata does not match InstallSnapshot",
                    ));
                }
                self.pending_snapshot = Some(package);
            }
            (RaftMessage::InstallSnapshotRequest(_), None) => {
                return Err(ControllerError::Invalid(
                    "controller InstallSnapshot requires state snapshot package",
                ));
            }
            (_, Some(_)) => {
                return Err(ControllerError::Invalid(
                    "controller snapshot package requires InstallSnapshotRequest",
                ));
            }
            (_, None) => {}
        }
        let result = self.handle(Input::Step(message));
        if self.pending_snapshot.is_some() {
            self.pending_snapshot = None;
        }
        result
    }

    pub fn propose(&mut self, command: ControllerCommand) -> Result<MutationReceipt> {
        self.propose_log_command(ControllerLogCommand::V1(command))
    }

    pub fn propose_producer_session(
        &mut self,
        command: ProducerSessionCommandV2,
    ) -> Result<MutationReceipt> {
        self.propose_log_command(ControllerLogCommand::ProducerV2(command))
    }

    pub fn propose_topic_performance(
        &mut self,
        command: TopicPerformanceCommandV3,
    ) -> Result<MutationReceipt> {
        self.propose_log_command(ControllerLogCommand::PerformanceV3(command))
    }

    pub fn propose_topic_retention(
        &mut self,
        command: TopicRetentionCommandV4,
    ) -> Result<MutationReceipt> {
        self.propose_log_command(ControllerLogCommand::RetentionV4(command))
    }

    fn propose_log_command(&mut self, command: ControllerLogCommand) -> Result<MutationReceipt> {
        if self.is_fatal() {
            return Err(ControllerError::Invalid(
                "controller runtime is fatal/non-serving",
            ));
        }
        if self.role() != Role::Leader {
            return Err(ControllerError::NotLeader {
                leader: self.leader().map(ControllerNodeId::get),
            });
        }
        if self.transport_congested {
            return Err(ControllerError::Busy(
                "controller transport is congested; retry after control traffic drains",
            ));
        }
        if self.pending.len() >= self.limits.max_pending_mutations {
            return Err(ControllerError::Busy(
                "controller pending mutation count limit reached",
            ));
        }
        if let ControllerLogCommand::V1(ControllerCommand::SetFeatureLevel {
            new_feature_level,
            ..
        }) = &command
        {
            self.validate_controller_feature_level(*new_feature_level)?;
        }
        let payload = encode_controller_log_command(&command, self.command_limits)?;
        if self
            .pending_bytes
            .checked_add(payload.len())
            .is_none_or(|bytes| bytes > self.limits.max_pending_mutation_bytes)
        {
            return Err(ControllerError::Busy(
                "controller pending mutation byte limit reached",
            ));
        }
        let proposal_id = ProposalId::new(self.next_proposal_id);
        self.next_proposal_id = self
            .next_proposal_id
            .checked_add(1)
            .ok_or(ControllerError::Invalid("controller proposal id overflow"))?;
        let raft_index = self
            .core
            .stable_log()
            .last()
            .map_or(self.core.snapshot().last_included_index, |entry| {
                entry.index
            })
            .checked_next()?;
        let receipt = MutationReceipt {
            proposal_id,
            raft_index,
        };
        // Prevalidate against the deterministic speculative state containing
        // previously accepted-but-not-yet-applied proposals. This prevents a
        // syntactically valid but semantically impossible membership command
        // from entering the replicated controller log.
        match &command {
            ControllerLogCommand::V1(command) => {
                self.validation_state.apply(raft_index.get(), command)?;
            }
            ControllerLogCommand::ProducerV2(command) => {
                self.validation_state
                    .apply_producer_session(raft_index.get(), *command)?;
            }
            ControllerLogCommand::PerformanceV3(command) => {
                self.validation_state
                    .apply_topic_performance(raft_index.get(), command)?;
            }
            ControllerLogCommand::RetentionV4(command) => {
                self.validation_state
                    .apply_topic_retention(raft_index.get(), command)?;
            }
        }
        self.pending_bytes += payload.len();
        self.pending.insert(
            proposal_id,
            PendingMutation {
                receipt,
                command,
                encoded_bytes: payload.len(),
                outcome: MutationOutcome::Pending,
            },
        );
        if let Err(error) = self.handle(Input::Propose {
            id: proposal_id,
            payload,
        }) {
            self.remove_pending(proposal_id);
            self.rebuild_validation_state()?;
            return Err(error);
        }
        Ok(receipt)
    }

    fn prepare_producer_session(
        &self,
        expected_authority: Option<ProducerAuthority>,
    ) -> Result<(ProducerSessionCommandV2, ProducerAuthority)> {
        if self.role() != Role::Leader {
            return Err(ControllerError::NotLeader {
                leader: self.leader().map(ControllerNodeId::get),
            });
        }
        FeatureGate::new(P7_PRODUCER_FEATURE_LEVEL)?
            .require(self.validation_state.active_feature_level())?;
        match expected_authority {
            None => {
                let producer_id =
                    ProducerId::new(self.validation_state.next_unallocated_producer_id()).ok_or(
                        ControllerError::Invalid("producer id allocator is exhausted"),
                    )?;
                let producer_epoch = ProducerEpoch::new(1).expect("epoch one is valid");
                Ok((
                    ProducerSessionCommandV2::InitializeSession {
                        producer_id,
                        producer_epoch,
                    },
                    ProducerAuthority::new(producer_id, producer_epoch),
                ))
            }
            Some(authority) => {
                let current = self
                    .validation_state
                    .producer_authority(authority.producer_id)
                    .ok_or(ControllerError::Invalid("unknown producer session"))?;
                if current != authority {
                    return Err(ControllerError::Invalid("stale producer epoch"));
                }
                let new_epoch = authority
                    .producer_epoch
                    .checked_next()
                    .ok_or(ControllerError::Invalid("producer epoch overflow"))?;
                Ok((
                    ProducerSessionCommandV2::BumpEpoch {
                        producer_id: authority.producer_id,
                        expected_epoch: authority.producer_epoch,
                        new_epoch,
                    },
                    ProducerAuthority::new(authority.producer_id, new_epoch),
                ))
            }
        }
    }

    fn validate_controller_feature_level(&self, level: u16) -> Result<()> {
        if self
            .controller_feature_ranges
            .values()
            .any(|range| !range.supports(level))
        {
            return Err(ControllerError::Invalid(
                "controller voter does not support requested cluster feature level",
            ));
        }
        Ok(())
    }

    pub fn mutation_outcome(&self, id: ProposalId) -> Option<MutationOutcome> {
        self.pending.get(&id).map(|pending| pending.outcome)
    }

    pub fn take_completed(&mut self, id: ProposalId) -> Option<MutationOutcome> {
        let outcome = self.pending.get(&id)?.outcome;
        if outcome == MutationOutcome::Pending {
            return None;
        }
        self.remove_pending(id);
        Some(outcome)
    }

    pub fn request_snapshot(&mut self, index: LogIndex) -> Result<()> {
        if index != self.core.volatile_state().last_applied {
            return Err(ControllerError::Invalid(
                "controller runtime snapshots only the fully applied state",
            ));
        }
        self.handle(Input::RequestSnapshot(index))
    }

    pub fn note_transport_congested(&mut self) {
        self.transport_congested = true;
    }

    fn handle(&mut self, input: Input) -> Result<()> {
        let previous_role = self.role();
        self.core.handle_input(input)?;
        self.drive_effects()?;
        let current_role = self.role();
        if previous_role != current_role
            && (previous_role == Role::Leader || current_role == Role::Leader)
        {
            self.failure_detector.reset_for_leadership_change();
            self.movement_observations.reset_for_leadership_change();
        }
        if previous_role == Role::Leader && current_role != Role::Leader {
            for pending in self.pending.values_mut() {
                if pending.outcome == MutationOutcome::Pending {
                    pending.outcome = MutationOutcome::LeadershipLost;
                }
            }
            self.rebuild_validation_state()?;
        }
        Ok(())
    }

    fn drive_effects(&mut self) -> Result<()> {
        loop {
            let effects = self.core.drain_effects().collect::<Vec<_>>();
            if effects.is_empty() {
                break;
            }
            for effect in effects {
                match effect {
                    Effect::Persist(batch) => {
                        let installs_snapshot = batch.operations.iter().any(|operation| {
                            matches!(operation, PersistenceOp::InstallSnapshot(_))
                        });
                        let package = installs_snapshot
                            .then(|| self.pending_snapshot.take())
                            .flatten();
                        match self.persistence.execute(&batch, package.as_ref()) {
                            Ok(()) => self
                                .core
                                .handle_input(Input::PersistenceCompleted(batch.token))?,
                            Err(error) => {
                                let message = error.to_string();
                                self.core.handle_input(Input::PersistenceFailed {
                                    token: batch.token,
                                    error: PersistenceFailure::new(message.clone()),
                                })?;
                                self.fatal = Some(message);
                            }
                        }
                    }
                    Effect::Send { target, message } => self.enqueue_outbound(target, message)?,
                    Effect::Apply(entries) => self.apply_entries(entries)?,
                    Effect::BuildSnapshot(index) => self.finish_snapshot_build(index)?,
                    Effect::InstallSnapshotToStateMachine(metadata) => {
                        if self.persistence.snapshot_package().metadata != metadata {
                            return Err(ControllerError::Invalid(
                                "P3 installed snapshot differs from durable controller snapshot",
                            ));
                        }
                        self.state = self.persistence.snapshot_package().snapshot.state.clone();
                        self.metadata_disseminator.clear();
                        self.rebuild_validation_state()?;
                    }
                    Effect::RoleChanged { .. } => {}
                    Effect::ProposalResult { id, accepted, .. } => {
                        if !accepted {
                            if let Some(pending) = self.pending.get_mut(&id) {
                                pending.outcome = MutationOutcome::Rejected;
                            }
                            self.rebuild_validation_state()?;
                        }
                    }
                    Effect::PreVoteQuorumReached { prospective_term } => {
                        self.core
                            .handle_input(Input::BeginElection { prospective_term })?;
                    }
                    Effect::ElectionQuorumReached { term } => {
                        self.core.handle_input(Input::WinElection { term })?;
                    }
                    Effect::Fatal(reason) => self.fatal = Some(reason),
                }
            }
        }
        Ok(())
    }

    fn apply_entries(&mut self, entries: Vec<astralog_raft::LogEntry>) -> Result<()> {
        let Some(last) = entries.last().map(|entry| entry.index) else {
            return Ok(());
        };
        for entry in entries {
            match entry.kind {
                EntryKind::Application(payload) => {
                    let command = decode_controller_log_command(&payload, self.command_limits)?;
                    let outcome = match &command {
                        ControllerLogCommand::V1(command) => {
                            let outcome = self.state.apply(entry.index.get(), command)?;
                            self.metadata_disseminator
                                .record(entry.index.get(), command.clone())?;
                            outcome
                        }
                        ControllerLogCommand::ProducerV2(command) => self
                            .state
                            .apply_producer_session(entry.index.get(), *command)?,
                        ControllerLogCommand::PerformanceV3(command) => {
                            let outcome = self
                                .state
                                .apply_topic_performance(entry.index.get(), command)?;
                            // V3 topic-performance state is not representable as a frozen V1
                            // metadata delta. Clearing retained V1 history forces any broker
                            // behind this epoch to receive an authoritative full snapshot.
                            self.metadata_disseminator.clear();
                            outcome
                        }
                        ControllerLogCommand::RetentionV4(command) => {
                            let outcome = self
                                .state
                                .apply_topic_retention(entry.index.get(), command)?;
                            // P9 V4 state is not representable as a frozen V1 metadata delta.
                            // Force lagging brokers onto the authoritative full-state snapshot.
                            self.metadata_disseminator.clear();
                            outcome
                        }
                    };
                    if let Some((_, pending)) = self
                        .pending
                        .iter_mut()
                        .find(|(_, pending)| pending.receipt.raft_index == entry.index)
                    {
                        pending.outcome = MutationOutcome::Committed(outcome);
                    }
                }
                EntryKind::LeaderBarrier => {}
                EntryKind::Configuration(_) => {
                    return Err(ControllerError::Invalid(
                        "dynamic controller membership entry reached P5 state machine",
                    ));
                }
            }
        }
        self.rebuild_validation_state()?;
        self.core.handle_input(Input::AppliedThrough(last))?;
        Ok(())
    }

    fn finish_snapshot_build(&mut self, index: LogIndex) -> Result<()> {
        if index != self.core.volatile_state().last_applied {
            return Err(ControllerError::Invalid(
                "controller snapshot build is not at fully applied index",
            ));
        }
        let term = term_at(&self.core, index).ok_or(ControllerError::Invalid(
            "controller snapshot boundary term is unavailable",
        ))?;
        let EffectiveConfiguration::Stable(configuration) = self.core.effective_configuration()
        else {
            return Err(ControllerError::Invalid(
                "controller snapshot requested during joint configuration",
            ));
        };
        let mut id = [0_u8; 16];
        id[..8].copy_from_slice(&index.get().to_le_bytes());
        id[8..].copy_from_slice(&term.get().to_le_bytes());
        let metadata =
            SnapshotMetadata::new(index, term, configuration.clone(), SnapshotId::new(id))?;
        let package = self
            .persistence
            .build_snapshot_package(&self.state, &metadata)?;
        self.pending_snapshot = Some(package);
        self.core.handle_input(Input::SnapshotBuilt(metadata))?;
        Ok(())
    }

    fn enqueue_outbound(&mut self, target: NodeId, message: RaftMessage) -> Result<()> {
        let target = ControllerNodeId::new(target.get())?;
        let snapshot_package = if let RaftMessage::InstallSnapshotRequest(request) = &message {
            if self.persistence.snapshot_package().metadata != request.snapshot {
                return Err(ControllerError::Invalid(
                    "P3 requested controller snapshot not present in local persistence",
                ));
            }
            Some(encode_controller_snapshot_package(
                self.persistence.snapshot_package(),
                self.snapshot_limits,
            )?)
        } else {
            None
        };
        let estimated_bytes = estimate_message_bytes(&message)
            .saturating_add(snapshot_package.as_ref().map_or(0, Vec::len));
        if self.outbound.len() >= self.limits.max_outbound_messages
            || self
                .outbound_bytes
                .checked_add(estimated_bytes)
                .is_none_or(|bytes| bytes > self.limits.max_outbound_bytes)
        {
            self.transport_congested = true;
            return Ok(());
        }
        self.outbound_bytes += estimated_bytes;
        self.outbound.push_back(ControllerOutbound {
            target,
            message,
            snapshot_package,
            estimated_bytes,
        });
        Ok(())
    }

    fn drain_outbound(&mut self) -> Vec<ControllerOutbound> {
        self.transport_congested = false;
        self.outbound_bytes = 0;
        self.outbound.drain(..).collect()
    }

    fn remove_pending(&mut self, id: ProposalId) {
        if let Some(pending) = self.pending.remove(&id) {
            self.pending_bytes = self.pending_bytes.saturating_sub(pending.encoded_bytes);
        }
    }

    fn rebuild_validation_state(&mut self) -> Result<()> {
        let mut state = self.state.clone();
        let mut pending = self
            .pending
            .values()
            .filter(|pending| pending.outcome == MutationOutcome::Pending)
            .collect::<Vec<_>>();
        pending.sort_by_key(|pending| pending.receipt.raft_index);
        for pending in pending {
            match &pending.command {
                ControllerLogCommand::V1(command) => {
                    state.apply(pending.receipt.raft_index.get(), command)?;
                }
                ControllerLogCommand::ProducerV2(command) => {
                    state.apply_producer_session(pending.receipt.raft_index.get(), *command)?;
                }
                ControllerLogCommand::PerformanceV3(command) => {
                    state.apply_topic_performance(pending.receipt.raft_index.get(), command)?;
                }
                ControllerLogCommand::RetentionV4(command) => {
                    state.apply_topic_retention(pending.receipt.raft_index.get(), command)?;
                }
            }
        }
        self.validation_state = state;
        Ok(())
    }
}

fn validate_controller_feature_ranges(
    configuration: &StableConfiguration,
    ranges: &BTreeMap<ControllerNodeId, FeatureRange>,
) -> Result<()> {
    let expected = configuration
        .voters()
        .map(|node| ControllerNodeId::new(node.get()))
        .collect::<Result<BTreeSet<_>>>()?;
    let actual = ranges.keys().copied().collect::<BTreeSet<_>>();
    if expected != actual {
        return Err(ControllerError::Invalid(
            "controller feature advertisements differ from static controller voter set",
        ));
    }
    Ok(())
}

fn validate_active_feature_level(
    ranges: &BTreeMap<ControllerNodeId, FeatureRange>,
    active_feature_level: u16,
) -> Result<()> {
    if ranges
        .values()
        .any(|range| !range.supports(active_feature_level))
    {
        return Err(ControllerError::Invalid(
            "controller voter does not support active cluster feature level",
        ));
    }
    Ok(())
}

fn finish_recovery_replay(core: &mut RaftCore) -> Result<()> {
    let effects = core.drain_effects().collect::<Vec<_>>();
    for effect in effects {
        match effect {
            Effect::Apply(entries) => {
                if let Some(last) = entries.last() {
                    core.handle_input(Input::AppliedThrough(last.index))?;
                }
            }
            other => {
                return Err(ControllerError::InvalidOwned(format!(
                    "unexpected P3 effect during controller recovery: {other:?}"
                )));
            }
        }
    }
    Ok(())
}

fn term_at(core: &RaftCore, index: LogIndex) -> Option<Term> {
    if index == core.snapshot().last_included_index {
        return Some(core.snapshot().last_included_term);
    }
    core.stable_log()
        .iter()
        .find(|entry| entry.index == index)
        .map(|entry| entry.term)
}

fn message_sender(message: &RaftMessage) -> NodeId {
    match message {
        RaftMessage::PreVoteRequest(value) => value.candidate_id,
        RaftMessage::PreVoteResponse(value) => value.voter_id,
        RaftMessage::RequestVoteRequest(value) => value.candidate_id,
        RaftMessage::RequestVoteResponse(value) => value.voter_id,
        RaftMessage::AppendEntriesRequest(value) => value.leader_id,
        RaftMessage::AppendEntriesResponse(value) => value.follower_id,
        RaftMessage::InstallSnapshotRequest(value) => value.leader_id,
        RaftMessage::InstallSnapshotResponse(value) => value.follower_id,
        RaftMessage::TimeoutNow(value) => value.leader_id,
    }
}

fn estimate_message_bytes(message: &RaftMessage) -> usize {
    match message {
        RaftMessage::AppendEntriesRequest(request) => request
            .entries
            .iter()
            .map(|entry| match &entry.kind {
                EntryKind::Application(payload) => 64_usize.saturating_add(payload.len()),
                EntryKind::LeaderBarrier => 64,
                EntryKind::Configuration(_) => 256,
            })
            .fold(128_usize, usize::saturating_add),
        _ => 256,
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ControllerStatus {
    pub local_node: ControllerNodeId,
    pub role: Role,
    pub leader: Option<ControllerNodeId>,
    pub term: Term,
    pub metadata_epoch: u64,
    pub active_feature_level: u16,
    pub min_feature: u16,
    pub max_feature: u16,
    pub fatal: bool,
}

#[derive(Debug, Clone)]
pub struct ControllerNodeConfig {
    pub runtime: ControllerRuntimeConfig,
    pub transport: ControllerPeerTransportConfig,
    pub tick_period: Duration,
    pub election_timeout_sample: u64,
    pub max_control_requests: usize,
}

enum NodeControl {
    Mutate {
        command: ControllerCommand,
        response: oneshot::Sender<Result<ApplyOutcome>>,
    },
    InitProducer {
        expected_authority: Option<ProducerAuthority>,
        response: oneshot::Sender<Result<ProducerAuthority>>,
    },
    Status {
        response: oneshot::Sender<ControllerStatus>,
    },
    BrokerRecord {
        broker_id: u32,
        response: oneshot::Sender<Option<BrokerRecord>>,
    },
    ValidateBrokerAuthority {
        authority: BrokerAuthority,
        response: oneshot::Sender<Result<BrokerRecord>>,
    },
    FailureDetectorTick {
        tick: u64,
        response: oneshot::Sender<Result<()>>,
    },
    Heartbeat {
        request: BrokerHeartbeatRequest,
        response: oneshot::Sender<Result<BrokerHeartbeatResponse>>,
    },
    BrokerLiveness {
        broker_id: u32,
        response: oneshot::Sender<Result<BrokerLivenessObservation>>,
    },
    PlannerLivenessSnapshot {
        response: oneshot::Sender<Result<Vec<BrokerLivenessObservation>>>,
    },
    ReplicaProgressObservation {
        broker_id: u32,
        topic_id: astralog_common::TopicId,
        partition_id: u32,
        generation: crate::AssignmentGeneration,
        response: oneshot::Sender<Result<Option<ReplicaProgressObservation>>>,
    },
    MetadataSnapshot {
        response: oneshot::Sender<Result<ControllerMetadataSnapshot>>,
    },
    MetadataSync {
        applied_epoch: u64,
        response: oneshot::Sender<Result<MetadataSyncResponse>>,
    },
    TopicRecord {
        topic_id: astralog_common::TopicId,
        response: oneshot::Sender<Option<TopicRecord>>,
    },
    TopicPerformancePolicy {
        topic_id: astralog_common::TopicId,
        response: oneshot::Sender<Option<crate::TopicPerformancePolicyV1>>,
    },
    TopicRetentionPolicy {
        topic_id: astralog_common::TopicId,
        response: oneshot::Sender<Option<TopicRetentionPolicyV1>>,
    },
    ProducerAuthority {
        producer_id: ProducerId,
        response: oneshot::Sender<Option<ProducerAuthority>>,
    },
    CreateTopic {
        spec: CreateTopicSpec,
        response: oneshot::Sender<Result<TopicCreateResult>>,
    },
    CreateTopicWithPerformance {
        spec: CreateTopicSpec,
        performance: TopicPerformanceSpecV1,
        response: oneshot::Sender<Result<TopicCreateResult>>,
    },
    CreateTopicWithRetention {
        spec: CreateTopicSpec,
        performance: TopicPerformanceSpecV1,
        retention: TopicRetentionSpecV1,
        response: oneshot::Sender<Result<TopicCreateResult>>,
    },
    AlterTopicRetention {
        topic_id: astralog_common::TopicId,
        retention: TopicRetentionSpecV1,
        response: oneshot::Sender<Result<ApplyOutcome>>,
    },
    ExpandTopic {
        spec: TopicExpansionSpec,
        response: oneshot::Sender<Result<TopicExpansionResult>>,
    },
    BeginReassignment {
        spec: ReassignmentSpec,
        response: oneshot::Sender<Result<ReassignmentResult>>,
    },
    Shutdown {
        response: oneshot::Sender<()>,
    },
}

enum PendingTopicOperation {
    Create {
        prepared: PreparedTopicCreate,
        response: oneshot::Sender<Result<TopicCreateResult>>,
    },
    Expand {
        prepared: PreparedTopicExpansion,
        response: oneshot::Sender<Result<TopicExpansionResult>>,
    },
}

struct PendingReassignment {
    movement_id: MovementId,
    response: oneshot::Sender<Result<ReassignmentResult>>,
}

struct PendingProducerSession {
    authority: ProducerAuthority,
    response: oneshot::Sender<Result<ProducerAuthority>>,
}

#[derive(Clone)]
pub struct ControllerClient {
    local_node: ControllerNodeId,
    control: mpsc::Sender<NodeControl>,
}

impl ControllerClient {
    pub const fn local_node(&self) -> ControllerNodeId {
        self.local_node
    }

    pub async fn status(&self) -> Result<ControllerStatus> {
        let (tx, rx) = oneshot::channel();
        self.control
            .send(NodeControl::Status { response: tx })
            .await
            .map_err(|_| ControllerError::Busy("controller node actor is closed"))?;
        rx.await
            .map_err(|_| ControllerError::Busy("controller status response was lost"))
    }

    pub async fn metadata_snapshot(&self) -> Result<ControllerMetadataSnapshot> {
        let (tx, rx) = oneshot::channel();
        self.control
            .send(NodeControl::MetadataSnapshot { response: tx })
            .await
            .map_err(|_| ControllerError::Busy("controller node actor is closed"))?;
        rx.await
            .map_err(|_| ControllerError::Busy("controller metadata snapshot response was lost"))?
    }

    pub async fn producer_authority(
        &self,
        producer_id: ProducerId,
    ) -> Result<Option<ProducerAuthority>> {
        let (tx, rx) = oneshot::channel();
        self.control
            .send(NodeControl::ProducerAuthority {
                producer_id,
                response: tx,
            })
            .await
            .map_err(|_| ControllerError::Busy("controller node actor is closed"))?;
        rx.await
            .map_err(|_| ControllerError::Busy("controller producer authority response was lost"))
    }

    pub async fn metadata_sync(&self, applied_epoch: u64) -> Result<MetadataSyncResponse> {
        let (tx, rx) = oneshot::channel();
        self.control
            .send(NodeControl::MetadataSync {
                applied_epoch,
                response: tx,
            })
            .await
            .map_err(|_| ControllerError::Busy("controller node actor is closed"))?;
        rx.await
            .map_err(|_| ControllerError::Busy("controller metadata sync response was lost"))?
    }

    pub async fn heartbeat(
        &self,
        request: BrokerHeartbeatRequest,
    ) -> Result<BrokerHeartbeatResponse> {
        let (tx, rx) = oneshot::channel();
        self.control
            .send(NodeControl::Heartbeat {
                request,
                response: tx,
            })
            .await
            .map_err(|_| ControllerError::Busy("controller node actor is closed"))?;
        rx.await
            .map_err(|_| ControllerError::Busy("broker heartbeat response was lost"))?
    }

    pub async fn replica_progress_observation(
        &self,
        broker_id: u32,
        topic_id: astralog_common::TopicId,
        partition_id: u32,
        generation: crate::AssignmentGeneration,
    ) -> Result<Option<ReplicaProgressObservation>> {
        let (tx, rx) = oneshot::channel();
        self.control
            .send(NodeControl::ReplicaProgressObservation {
                broker_id,
                topic_id,
                partition_id,
                generation,
                response: tx,
            })
            .await
            .map_err(|_| ControllerError::Busy("controller node actor is closed"))?;
        rx.await.map_err(|_| {
            ControllerError::Busy("controller replica-progress observation response was lost")
        })?
    }

    pub async fn mutate(&self, command: ControllerCommand) -> Result<ApplyOutcome> {
        let (tx, rx) = oneshot::channel();
        self.control
            .send(NodeControl::Mutate {
                command,
                response: tx,
            })
            .await
            .map_err(|_| ControllerError::Busy("controller node actor is closed"))?;
        rx.await
            .map_err(|_| ControllerError::Busy("controller mutation response was lost"))?
    }

    pub async fn bump_feature_level(&self, new_feature_level: u16) -> Result<ApplyOutcome> {
        let status = self.status().await?;
        if status.role != Role::Leader {
            return Err(ControllerError::NotLeader {
                leader: status.leader.map(ControllerNodeId::get),
            });
        }
        self.mutate(ControllerCommand::SetFeatureLevel {
            expected_feature_level: status.active_feature_level,
            new_feature_level,
        })
        .await
    }

    /// Allocate one bounded contiguous producer-ID block. The generic mutation
    /// waiter resolves only after the command is quorum committed and applied;
    /// if this response is lost, the caller must treat the outcome as ambiguous
    /// and request a fresh block rather than attempting to reuse the old IDs.
    pub async fn allocate_producer_ids(&self, allocation_count: u32) -> Result<ProducerIdRange> {
        let outcome = self
            .mutate(ControllerCommand::AllocateProducerIds { allocation_count })
            .await?;
        outcome.producer_ids.ok_or(ControllerError::Invalid(
            "committed producer-id allocation returned no range",
        ))
    }

    /// Initialize a new producer session when `expected_authority` is `None`, or
    /// CAS-bump an existing producer epoch when it is present. Success is returned
    /// only after the V2 command is quorum committed and applied by controller Raft.
    pub async fn init_producer(
        &self,
        expected_authority: Option<ProducerAuthority>,
    ) -> Result<ProducerAuthority> {
        let (tx, rx) = oneshot::channel();
        self.control
            .send(NodeControl::InitProducer {
                expected_authority,
                response: tx,
            })
            .await
            .map_err(|_| ControllerError::Busy("controller node actor is closed"))?;
        rx.await
            .map_err(|_| ControllerError::Busy("InitProducer response was lost"))?
    }

    pub async fn create_topic(&self, spec: CreateTopicSpec) -> Result<TopicCreateResult> {
        let (tx, rx) = oneshot::channel();
        self.control
            .send(NodeControl::CreateTopic { spec, response: tx })
            .await
            .map_err(|_| ControllerError::Busy("controller node actor is closed"))?;
        rx.await
            .map_err(|_| ControllerError::Busy("controller create-topic response was lost"))?
    }

    pub async fn create_topic_with_performance(
        &self,
        spec: CreateTopicSpec,
        performance: TopicPerformanceSpecV1,
    ) -> Result<TopicCreateResult> {
        let (tx, rx) = oneshot::channel();
        self.control
            .send(NodeControl::CreateTopicWithPerformance {
                spec,
                performance,
                response: tx,
            })
            .await
            .map_err(|_| ControllerError::Busy("controller node actor is closed"))?;
        rx.await.map_err(|_| {
            ControllerError::Busy("controller create-topic-with-performance response was lost")
        })?
    }

    pub async fn create_topic_with_retention(
        &self,
        spec: CreateTopicSpec,
        performance: TopicPerformanceSpecV1,
        retention: TopicRetentionSpecV1,
    ) -> Result<TopicCreateResult> {
        let (tx, rx) = oneshot::channel();
        self.control
            .send(NodeControl::CreateTopicWithRetention {
                spec,
                performance,
                retention,
                response: tx,
            })
            .await
            .map_err(|_| ControllerError::Busy("controller node actor is closed"))?;
        rx.await.map_err(|_| {
            ControllerError::Busy("controller create-topic-with-retention response was lost")
        })?
    }

    pub async fn alter_topic_retention(
        &self,
        topic_id: astralog_common::TopicId,
        retention: TopicRetentionSpecV1,
    ) -> Result<ApplyOutcome> {
        let (tx, rx) = oneshot::channel();
        self.control
            .send(NodeControl::AlterTopicRetention {
                topic_id,
                retention,
                response: tx,
            })
            .await
            .map_err(|_| ControllerError::Busy("controller node actor is closed"))?;
        rx.await.map_err(|_| {
            ControllerError::Busy("controller alter-topic-retention response was lost")
        })?
    }

    pub async fn begin_reassignment(&self, spec: ReassignmentSpec) -> Result<ReassignmentResult> {
        let (tx, rx) = oneshot::channel();
        self.control
            .send(NodeControl::BeginReassignment { spec, response: tx })
            .await
            .map_err(|_| ControllerError::Busy("controller node actor is closed"))?;
        rx.await
            .map_err(|_| ControllerError::Busy("controller reassignment response was lost"))?
    }
}

pub struct ControllerNodeHandle {
    local_node: ControllerNodeId,
    local_addr: std::net::SocketAddr,
    control: mpsc::Sender<NodeControl>,
    task: JoinHandle<()>,
}

impl ControllerNodeHandle {
    pub fn client(&self) -> ControllerClient {
        ControllerClient {
            local_node: self.local_node,
            control: self.control.clone(),
        }
    }

    pub const fn local_node(&self) -> ControllerNodeId {
        self.local_node
    }

    pub const fn local_addr(&self) -> std::net::SocketAddr {
        self.local_addr
    }

    pub async fn mutate(&self, command: ControllerCommand) -> Result<ApplyOutcome> {
        let (tx, rx) = oneshot::channel();
        self.control
            .send(NodeControl::Mutate {
                command,
                response: tx,
            })
            .await
            .map_err(|_| ControllerError::Busy("controller node actor is closed"))?;
        rx.await
            .map_err(|_| ControllerError::Busy("controller mutation response was lost"))?
    }

    pub async fn bump_feature_level(&self, new_feature_level: u16) -> Result<ApplyOutcome> {
        self.client().bump_feature_level(new_feature_level).await
    }

    pub async fn allocate_producer_ids(&self, allocation_count: u32) -> Result<ProducerIdRange> {
        self.client().allocate_producer_ids(allocation_count).await
    }

    pub async fn init_producer(
        &self,
        expected_authority: Option<ProducerAuthority>,
    ) -> Result<ProducerAuthority> {
        self.client().init_producer(expected_authority).await
    }

    pub async fn status(&self) -> Result<ControllerStatus> {
        let (tx, rx) = oneshot::channel();
        self.control
            .send(NodeControl::Status { response: tx })
            .await
            .map_err(|_| ControllerError::Busy("controller node actor is closed"))?;
        rx.await
            .map_err(|_| ControllerError::Busy("controller status response was lost"))
    }

    pub async fn broker_record(&self, broker_id: u32) -> Result<Option<BrokerRecord>> {
        let (tx, rx) = oneshot::channel();
        self.control
            .send(NodeControl::BrokerRecord {
                broker_id,
                response: tx,
            })
            .await
            .map_err(|_| ControllerError::Busy("controller node actor is closed"))?;
        rx.await
            .map_err(|_| ControllerError::Busy("controller broker record response was lost"))
    }

    pub async fn validate_broker_authority(
        &self,
        authority: BrokerAuthority,
    ) -> Result<BrokerRecord> {
        let (tx, rx) = oneshot::channel();
        self.control
            .send(NodeControl::ValidateBrokerAuthority {
                authority,
                response: tx,
            })
            .await
            .map_err(|_| ControllerError::Busy("controller node actor is closed"))?;
        rx.await
            .map_err(|_| ControllerError::Busy("controller broker authority response was lost"))?
    }

    pub async fn failure_detector_tick(&self, tick: u64) -> Result<()> {
        let (tx, rx) = oneshot::channel();
        self.control
            .send(NodeControl::FailureDetectorTick { tick, response: tx })
            .await
            .map_err(|_| ControllerError::Busy("controller node actor is closed"))?;
        rx.await
            .map_err(|_| ControllerError::Busy("failure-detector tick response was lost"))?
    }

    pub async fn heartbeat(
        &self,
        request: BrokerHeartbeatRequest,
    ) -> Result<BrokerHeartbeatResponse> {
        let (tx, rx) = oneshot::channel();
        self.control
            .send(NodeControl::Heartbeat {
                request,
                response: tx,
            })
            .await
            .map_err(|_| ControllerError::Busy("controller node actor is closed"))?;
        rx.await
            .map_err(|_| ControllerError::Busy("broker heartbeat response was lost"))?
    }

    pub async fn broker_liveness(&self, broker_id: u32) -> Result<BrokerLivenessObservation> {
        let (tx, rx) = oneshot::channel();
        self.control
            .send(NodeControl::BrokerLiveness {
                broker_id,
                response: tx,
            })
            .await
            .map_err(|_| ControllerError::Busy("controller node actor is closed"))?;
        rx.await
            .map_err(|_| ControllerError::Busy("broker liveness response was lost"))?
    }

    pub async fn planner_liveness_snapshot(&self) -> Result<Vec<BrokerLivenessObservation>> {
        let (tx, rx) = oneshot::channel();
        self.control
            .send(NodeControl::PlannerLivenessSnapshot { response: tx })
            .await
            .map_err(|_| ControllerError::Busy("controller node actor is closed"))?;
        rx.await
            .map_err(|_| ControllerError::Busy("planner liveness response was lost"))?
    }

    pub async fn metadata_snapshot(&self) -> Result<ControllerMetadataSnapshot> {
        self.client().metadata_snapshot().await
    }

    pub async fn metadata_sync(&self, applied_epoch: u64) -> Result<MetadataSyncResponse> {
        self.client().metadata_sync(applied_epoch).await
    }

    pub async fn topic_record(
        &self,
        topic_id: astralog_common::TopicId,
    ) -> Result<Option<TopicRecord>> {
        let (tx, rx) = oneshot::channel();
        self.control
            .send(NodeControl::TopicRecord {
                topic_id,
                response: tx,
            })
            .await
            .map_err(|_| ControllerError::Busy("controller node actor is closed"))?;
        rx.await
            .map_err(|_| ControllerError::Busy("controller topic record response was lost"))
    }

    pub async fn topic_performance_policy(
        &self,
        topic_id: astralog_common::TopicId,
    ) -> Result<Option<crate::TopicPerformancePolicyV1>> {
        let (tx, rx) = oneshot::channel();
        self.control
            .send(NodeControl::TopicPerformancePolicy {
                topic_id,
                response: tx,
            })
            .await
            .map_err(|_| ControllerError::Busy("controller node actor is closed"))?;
        rx.await
            .map_err(|_| ControllerError::Busy("controller topic-performance response was lost"))
    }

    pub async fn topic_retention_policy(
        &self,
        topic_id: astralog_common::TopicId,
    ) -> Result<Option<TopicRetentionPolicyV1>> {
        let (tx, rx) = oneshot::channel();
        self.control
            .send(NodeControl::TopicRetentionPolicy {
                topic_id,
                response: tx,
            })
            .await
            .map_err(|_| ControllerError::Busy("controller node actor is closed"))?;
        rx.await
            .map_err(|_| ControllerError::Busy("controller topic-retention response was lost"))
    }

    pub async fn producer_authority(
        &self,
        producer_id: ProducerId,
    ) -> Result<Option<ProducerAuthority>> {
        let (tx, rx) = oneshot::channel();
        self.control
            .send(NodeControl::ProducerAuthority {
                producer_id,
                response: tx,
            })
            .await
            .map_err(|_| ControllerError::Busy("controller node actor is closed"))?;
        rx.await
            .map_err(|_| ControllerError::Busy("controller producer authority response was lost"))
    }

    pub async fn create_topic(&self, spec: CreateTopicSpec) -> Result<TopicCreateResult> {
        let (tx, rx) = oneshot::channel();
        self.control
            .send(NodeControl::CreateTopic { spec, response: tx })
            .await
            .map_err(|_| ControllerError::Busy("controller node actor is closed"))?;
        rx.await
            .map_err(|_| ControllerError::Busy("controller create-topic response was lost"))?
    }

    pub async fn create_topic_with_performance(
        &self,
        spec: CreateTopicSpec,
        performance: TopicPerformanceSpecV1,
    ) -> Result<TopicCreateResult> {
        self.client()
            .create_topic_with_performance(spec, performance)
            .await
    }

    pub async fn create_topic_with_retention(
        &self,
        spec: CreateTopicSpec,
        performance: TopicPerformanceSpecV1,
        retention: TopicRetentionSpecV1,
    ) -> Result<TopicCreateResult> {
        self.client()
            .create_topic_with_retention(spec, performance, retention)
            .await
    }

    pub async fn alter_topic_retention(
        &self,
        topic_id: astralog_common::TopicId,
        retention: TopicRetentionSpecV1,
    ) -> Result<ApplyOutcome> {
        self.client()
            .alter_topic_retention(topic_id, retention)
            .await
    }

    pub async fn expand_topic(&self, spec: TopicExpansionSpec) -> Result<TopicExpansionResult> {
        let (tx, rx) = oneshot::channel();
        self.control
            .send(NodeControl::ExpandTopic { spec, response: tx })
            .await
            .map_err(|_| ControllerError::Busy("controller node actor is closed"))?;
        rx.await
            .map_err(|_| ControllerError::Busy("controller expand-topic response was lost"))?
    }

    pub async fn begin_reassignment(&self, spec: ReassignmentSpec) -> Result<ReassignmentResult> {
        self.client().begin_reassignment(spec).await
    }

    pub async fn begin_delete_topic(
        &self,
        topic_id: astralog_common::TopicId,
    ) -> Result<TopicDeletionResult> {
        let outcome = self
            .mutate(ControllerCommand::BeginDeleteTopic {
                topic_id,
                expected_topic_lifecycle: TopicLifecycle::Active,
            })
            .await?;
        Ok(TopicDeletionResult {
            outcome,
            topic_id,
            lifecycle: Some(TopicLifecycle::Deleting),
        })
    }

    pub async fn finalize_delete_topic(
        &self,
        topic_id: astralog_common::TopicId,
    ) -> Result<TopicDeletionResult> {
        let outcome = self
            .mutate(ControllerCommand::FinalizeDeleteTopic {
                topic_id,
                expected_topic_lifecycle: TopicLifecycle::Deleting,
            })
            .await?;
        Ok(TopicDeletionResult {
            outcome,
            topic_id,
            lifecycle: None,
        })
    }

    pub async fn shutdown(self) {
        let (tx, rx) = oneshot::channel();
        let _ = self
            .control
            .send(NodeControl::Shutdown { response: tx })
            .await;
        let _ = rx.await;
        let _ = self.task.await;
    }
}

pub async fn start_controller_node(config: ControllerNodeConfig) -> Result<ControllerNodeHandle> {
    if config.tick_period.is_zero() || config.max_control_requests == 0 {
        return Err(ControllerError::Invalid(
            "controller node tick/control bounds must be non-zero",
        ));
    }
    if config.runtime.local_node != config.transport.local_controller_id
        || config.runtime.initial_state.cluster_id() != &config.transport.cluster_id
    {
        return Err(ControllerError::Invalid(
            "controller runtime and transport identities do not match",
        ));
    }
    let mut runtime = ControllerRuntime::open(config.runtime)?;
    let expected_peers = runtime
        .configured_voters()
        .into_iter()
        .filter(|node| *node != runtime.local_node())
        .collect::<BTreeSet<_>>();
    let actual_peers = config
        .transport
        .peers
        .keys()
        .copied()
        .collect::<BTreeSet<_>>();
    if expected_peers != actual_peers {
        return Err(ControllerError::Invalid(
            "controller transport peer set differs from static P3 voter set",
        ));
    }
    let (transport, mut inbound) = start_controller_peer_transport(config.transport).await?;
    let local_addr = transport.local_addr();
    let local_node = runtime.local_node();
    let sender = transport.sender();
    let (control_tx, mut control_rx) = mpsc::channel(config.max_control_requests);
    let tick_period = config.tick_period;
    let election_timeout_sample = config.election_timeout_sample;
    let task = tokio::spawn(async move {
        let mut interval = time::interval(tick_period);
        interval.set_missed_tick_behavior(time::MissedTickBehavior::Delay);
        let mut waiters = BTreeMap::<ProposalId, oneshot::Sender<Result<ApplyOutcome>>>::new();
        let mut producer_waiters = BTreeMap::<ProposalId, PendingProducerSession>::new();
        let mut topic_waiters = BTreeMap::<ProposalId, PendingTopicOperation>::new();
        let mut reassignment_waiters = BTreeMap::<ProposalId, PendingReassignment>::new();
        loop {
            let mut shutdown_response = None;
            tokio::select! {
                _ = interval.tick() => {
                    if runtime.tick(election_timeout_sample).is_err() {
                        break;
                    }
                }
                frame = inbound.recv() => {
                    let Some(frame) = frame else { break; };
                    let source = match ControllerNodeId::new(frame.source_controller_id) {
                        Ok(source) => source,
                        Err(_) => continue,
                    };
                    if runtime.step(source, frame.message, frame.snapshot_package).is_err() {
                        continue;
                    }
                }
                control = control_rx.recv() => {
                    let Some(control) = control else { break; };
                    match control {
                        NodeControl::Mutate { command, response } => {
                            match runtime.propose(command) {
                                Ok(receipt) => { waiters.insert(receipt.proposal_id, response); }
                                Err(error) => { let _ = response.send(Err(error)); }
                            }
                        }
                        NodeControl::InitProducer { expected_authority, response } => {
                            match runtime.prepare_producer_session(expected_authority) {
                                Ok((command, authority)) => match runtime.propose_producer_session(command) {
                                    Ok(receipt) => {
                                        producer_waiters.insert(
                                            receipt.proposal_id,
                                            PendingProducerSession { authority, response },
                                        );
                                    }
                                    Err(error) => { let _ = response.send(Err(error)); }
                                },
                                Err(error) => { let _ = response.send(Err(error)); }
                            }
                        }
                        NodeControl::Status { response } => {
                            let _ = response.send(status_of(&runtime));
                        }
                        NodeControl::BrokerRecord { broker_id, response } => {
                            let _ = response.send(runtime.broker_record(broker_id).cloned());
                        }
                        NodeControl::ValidateBrokerAuthority { authority, response } => {
                            let result = runtime.validate_broker_authority(authority).cloned();
                            let _ = response.send(result);
                        }
                        NodeControl::FailureDetectorTick { tick, response } => {
                            let _ = response.send(runtime.failure_detector_tick(tick));
                        }
                        NodeControl::Heartbeat { request, response } => {
                            let _ = response.send(runtime.heartbeat(request));
                        }
                        NodeControl::BrokerLiveness { broker_id, response } => {
                            let _ = response.send(runtime.broker_liveness(broker_id));
                        }
                        NodeControl::PlannerLivenessSnapshot { response } => {
                            let _ = response.send(runtime.planner_liveness_snapshot());
                        }
                        NodeControl::ReplicaProgressObservation {
                            broker_id,
                            topic_id,
                            partition_id,
                            generation,
                            response,
                        } => {
                            let _ = response.send(runtime.replica_progress_observation(
                                broker_id,
                                topic_id,
                                partition_id,
                                generation,
                            ));
                        }
                        NodeControl::MetadataSnapshot { response } => {
                            let _ = response.send(runtime.metadata_snapshot());
                        }
                        NodeControl::MetadataSync { applied_epoch, response } => {
                            let _ = response.send(runtime.metadata_sync(applied_epoch));
                        }
                        NodeControl::TopicRecord { topic_id, response } => {
                            let _ = response.send(runtime.topic_record(topic_id).cloned());
                        }
                        NodeControl::TopicPerformancePolicy { topic_id, response } => {
                            let _ =
                                response.send(runtime.topic_performance_policy(topic_id).copied());
                        }
                        NodeControl::TopicRetentionPolicy { topic_id, response } => {
                            let _ = response.send(runtime.topic_retention_policy(topic_id).copied());
                        }
                        NodeControl::ProducerAuthority { producer_id, response } => {
                            let _ = response.send(runtime.producer_authority(producer_id));
                        }
                        NodeControl::CreateTopic { spec, response } => {
                            match runtime.prepare_topic_create(&spec) {
                                Ok((command, prepared)) => match runtime.propose(command) {
                                    Ok(receipt) => {
                                        topic_waiters.insert(
                                            receipt.proposal_id,
                                            PendingTopicOperation::Create { prepared, response },
                                        );
                                    }
                                    Err(error) => { let _ = response.send(Err(error)); }
                                },
                                Err(error) => { let _ = response.send(Err(error)); }
                            }
                        }
                        NodeControl::CreateTopicWithPerformance {
                            spec,
                            performance,
                            response,
                        } => {
                            match runtime.prepare_topic_create_with_performance(&spec, performance) {
                                Ok((command, prepared)) => {
                                    match runtime.propose_topic_performance(command) {
                                        Ok(receipt) => {
                                            topic_waiters.insert(
                                                receipt.proposal_id,
                                                PendingTopicOperation::Create { prepared, response },
                                            );
                                        }
                                        Err(error) => {
                                            let _ = response.send(Err(error));
                                        }
                                    }
                                }
                                Err(error) => {
                                    let _ = response.send(Err(error));
                                }
                            }
                        }
                        NodeControl::CreateTopicWithRetention {
                            spec,
                            performance,
                            retention,
                            response,
                        } => {
                            match runtime.prepare_topic_create_with_retention(
                                &spec,
                                performance,
                                retention,
                            ) {
                                Ok((command, prepared)) => {
                                    match runtime.propose_topic_retention(command) {
                                        Ok(receipt) => {
                                            topic_waiters.insert(
                                                receipt.proposal_id,
                                                PendingTopicOperation::Create { prepared, response },
                                            );
                                        }
                                        Err(error) => {
                                            let _ = response.send(Err(error));
                                        }
                                    }
                                }
                                Err(error) => {
                                    let _ = response.send(Err(error));
                                }
                            }
                        }
                        NodeControl::AlterTopicRetention {
                            topic_id,
                            retention,
                            response,
                        } => {
                            match runtime.prepare_topic_retention_alter(topic_id, retention) {
                                Ok(command) => match runtime.propose_topic_retention(command) {
                                    Ok(receipt) => {
                                        waiters.insert(receipt.proposal_id, response);
                                    }
                                    Err(error) => {
                                        let _ = response.send(Err(error));
                                    }
                                },
                                Err(error) => {
                                    let _ = response.send(Err(error));
                                }
                            }
                        }
                        NodeControl::ExpandTopic { spec, response } => {
                            match runtime.prepare_topic_expansion(&spec) {
                                Ok((command, prepared)) => match runtime.propose(command) {
                                    Ok(receipt) => {
                                        topic_waiters.insert(
                                            receipt.proposal_id,
                                            PendingTopicOperation::Expand { prepared, response },
                                        );
                                    }
                                    Err(error) => { let _ = response.send(Err(error)); }
                                },
                                Err(error) => { let _ = response.send(Err(error)); }
                            }
                        }
                        NodeControl::BeginReassignment { spec, response } => {
                            match runtime.prepare_reassignment(spec) {
                                Ok(prepared) => {
                                    let PreparedReassignment { movement_id, command } = prepared;
                                    match runtime.propose(command) {
                                        Ok(receipt) => {
                                            reassignment_waiters.insert(
                                                receipt.proposal_id,
                                                PendingReassignment { movement_id, response },
                                            );
                                        }
                                        Err(error) => { let _ = response.send(Err(error)); }
                                    }
                                }
                                Err(error) => { let _ = response.send(Err(error)); }
                            }
                        }
                        NodeControl::Shutdown { response } => {
                            shutdown_response = Some(response);
                        }
                    }
                }
            }
            flush_outbound(&mut runtime, &sender);
            complete_waiters(&mut runtime, &mut waiters);
            complete_producer_waiters(&mut runtime, &mut producer_waiters);
            complete_topic_waiters(&mut runtime, &mut topic_waiters);
            complete_reassignment_waiters(&mut runtime, &mut reassignment_waiters);
            if let Some(response) = shutdown_response {
                let _ = response.send(());
                break;
            }
            if runtime.is_fatal() {
                break;
            }
        }
        for (_, waiter) in waiters {
            let _ = waiter.send(Err(ControllerError::Busy(
                "controller node stopped before mutation outcome was known",
            )));
        }
        for (_, waiter) in producer_waiters {
            let _ = waiter.response.send(Err(ControllerError::Busy(
                "controller node stopped before InitProducer outcome was known",
            )));
        }
        for (_, waiter) in topic_waiters {
            waiter.fail(ControllerError::Busy(
                "controller node stopped before topic mutation outcome was known",
            ));
        }
        for (_, waiter) in reassignment_waiters {
            let _ = waiter.response.send(Err(ControllerError::Busy(
                "controller node stopped before reassignment outcome was known",
            )));
        }
        transport.shutdown().await;
    });
    Ok(ControllerNodeHandle {
        local_node,
        local_addr,
        control: control_tx,
        task,
    })
}

fn status_of(runtime: &ControllerRuntime) -> ControllerStatus {
    let range = runtime.local_feature_range();
    ControllerStatus {
        local_node: runtime.local_node(),
        role: runtime.role(),
        leader: runtime.leader(),
        term: runtime.term(),
        metadata_epoch: runtime.state().metadata_epoch(),
        active_feature_level: runtime.state().active_feature_level(),
        min_feature: range.min(),
        max_feature: range.max(),
        fatal: runtime.is_fatal(),
    }
}

fn flush_outbound(runtime: &mut ControllerRuntime, sender: &ControllerPeerTransportSender) {
    for outbound in runtime.drain_outbound() {
        let _estimated = outbound.estimated_bytes;
        if sender
            .try_send(outbound.target, outbound.message, outbound.snapshot_package)
            .is_err()
        {
            runtime.note_transport_congested();
        }
    }
}

fn complete_waiters(
    runtime: &mut ControllerRuntime,
    waiters: &mut BTreeMap<ProposalId, oneshot::Sender<Result<ApplyOutcome>>>,
) {
    let ids = waiters.keys().copied().collect::<Vec<_>>();
    for id in ids {
        let Some(outcome) = runtime.mutation_outcome(id) else {
            continue;
        };
        let result = match outcome {
            MutationOutcome::Pending => continue,
            MutationOutcome::Committed(outcome) => Ok(outcome),
            MutationOutcome::Rejected => Err(ControllerError::Invalid(
                "controller mutation was rejected before commit",
            )),
            MutationOutcome::LeadershipLost => Err(ControllerError::Busy(
                "controller leadership changed before mutation outcome was returned",
            )),
        };
        runtime.take_completed(id);
        if let Some(waiter) = waiters.remove(&id) {
            let _ = waiter.send(result);
        }
    }
}

fn complete_producer_waiters(
    runtime: &mut ControllerRuntime,
    waiters: &mut BTreeMap<ProposalId, PendingProducerSession>,
) {
    let ids = waiters.keys().copied().collect::<Vec<_>>();
    for id in ids {
        let Some(outcome) = runtime.mutation_outcome(id) else {
            continue;
        };
        if outcome == MutationOutcome::Pending {
            continue;
        }
        let waiter = waiters
            .remove(&id)
            .expect("producer waiter id came from map");
        let result = match outcome {
            MutationOutcome::Committed(_) => {
                if runtime
                    .state()
                    .producer_authority(waiter.authority.producer_id)
                    == Some(waiter.authority)
                {
                    Ok(waiter.authority)
                } else {
                    Err(ControllerError::Invalid(
                        "committed producer session authority is missing or regressed",
                    ))
                }
            }
            MutationOutcome::Rejected => Err(ControllerError::Invalid(
                "InitProducer mutation was rejected before commit",
            )),
            MutationOutcome::LeadershipLost => Err(ControllerError::Busy(
                "controller leadership changed before InitProducer outcome was returned",
            )),
            MutationOutcome::Pending => unreachable!("pending handled above"),
        };
        runtime.take_completed(id);
        let _ = waiter.response.send(result);
    }
}

fn complete_topic_waiters(
    runtime: &mut ControllerRuntime,
    waiters: &mut BTreeMap<ProposalId, PendingTopicOperation>,
) {
    let ids = waiters.keys().copied().collect::<Vec<_>>();
    for id in ids {
        let Some(outcome) = runtime.mutation_outcome(id) else {
            continue;
        };
        if outcome == MutationOutcome::Pending {
            continue;
        }
        let waiter = waiters.remove(&id).expect("topic waiter id came from map");
        runtime.take_completed(id);
        waiter.complete(outcome);
    }
}

fn complete_reassignment_waiters(
    runtime: &mut ControllerRuntime,
    waiters: &mut BTreeMap<ProposalId, PendingReassignment>,
) {
    let ids = waiters.keys().copied().collect::<Vec<_>>();
    for id in ids {
        let Some(outcome) = runtime.mutation_outcome(id) else {
            continue;
        };
        if outcome == MutationOutcome::Pending {
            continue;
        }
        let waiter = waiters
            .remove(&id)
            .expect("reassignment waiter id came from map");
        runtime.take_completed(id);
        let result = match outcome {
            MutationOutcome::Committed(outcome) => runtime
                .state()
                .movements()
                .get(&waiter.movement_id)
                .cloned()
                .ok_or(ControllerError::Invalid(
                    "committed reassignment movement is missing from controller state",
                ))
                .map(|movement| ReassignmentResult { outcome, movement }),
            MutationOutcome::Rejected => Err(ControllerError::Invalid(
                "controller reassignment mutation was rejected before commit",
            )),
            MutationOutcome::LeadershipLost => Err(ControllerError::Busy(
                "controller leadership changed before reassignment outcome was returned",
            )),
            MutationOutcome::Pending => unreachable!("pending outcome was handled above"),
        };
        let _ = waiter.response.send(result);
    }
}

impl PendingTopicOperation {
    fn complete(self, outcome: MutationOutcome) {
        match (self, outcome) {
            (Self::Create { prepared, response }, MutationOutcome::Committed(outcome)) => {
                let _ = response.send(Ok(TopicCreateResult {
                    outcome,
                    topic: prepared.topic,
                    assignments: prepared.plan.assignments,
                    degraded_failure_domain: prepared.plan.degraded_failure_domain,
                }));
            }
            (Self::Expand { prepared, response }, MutationOutcome::Committed(outcome)) => {
                let _ = response.send(Ok(TopicExpansionResult {
                    outcome,
                    topic_id: prepared.topic_id,
                    assignments: prepared.plan.assignments,
                    degraded_failure_domain: prepared.plan.degraded_failure_domain,
                    partitioning_epoch_change: prepared.partitioning_epoch_change,
                }));
            }
            (Self::Create { response, .. }, MutationOutcome::Rejected) => {
                let _ = response.send(Err(ControllerError::Invalid(
                    "controller create-topic mutation was rejected before commit",
                )));
            }
            (Self::Expand { response, .. }, MutationOutcome::Rejected) => {
                let _ = response.send(Err(ControllerError::Invalid(
                    "controller expand-topic mutation was rejected before commit",
                )));
            }
            (Self::Create { response, .. }, MutationOutcome::LeadershipLost) => {
                let _ = response.send(Err(ControllerError::Busy(
                    "controller leadership changed before create-topic outcome was returned",
                )));
            }
            (Self::Expand { response, .. }, MutationOutcome::LeadershipLost) => {
                let _ = response.send(Err(ControllerError::Busy(
                    "controller leadership changed before expand-topic outcome was returned",
                )));
            }
            (_, MutationOutcome::Pending) => {
                unreachable!("pending topic operation is not completed")
            }
        }
    }

    fn fail(self, error: ControllerError) {
        match self {
            Self::Create { response, .. } => {
                let _ = response.send(Err(error));
            }
            Self::Expand { response, .. } => {
                let _ = response.send(Err(error));
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use std::{
        fs,
        net::{Ipv4Addr, SocketAddr},
        path::{Path, PathBuf},
        sync::atomic::{AtomicU32, Ordering},
        time::{SystemTime, UNIX_EPOCH},
    };

    use super::*;
    use crate::{
        BrokerAuthority, BrokerLifecycle, BrokerLifecycleChange, BrokerRegistration, BrokerRejoin,
        ControllerPeerDescriptor, CreateTopicSpec, FailureDomainPolicy,
        MAX_PRODUCER_IDS_PER_ALLOCATION, TopicExpansionSpec,
    };

    fn temp_dir(label: &str) -> PathBuf {
        let nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let path = std::env::temp_dir().join(format!(
            "astralog-p5d-{label}-{}-{nanos}",
            std::process::id()
        ));
        fs::create_dir_all(&path).unwrap();
        path
    }

    fn reserve_addr() -> SocketAddr {
        static NEXT_LOOPBACK: AtomicU32 = AtomicU32::new(0);
        let slot = NEXT_LOOPBACK.fetch_add(1, Ordering::Relaxed);
        let process = std::process::id();
        let second = u8::try_from((process >> 8) & 0xff).unwrap();
        let third = u8::try_from(process & 0xff).unwrap();
        let fourth = u8::try_from(1 + slot % 250).unwrap();
        let listener =
            std::net::TcpListener::bind((Ipv4Addr::new(127, second, third, fourth), 0)).unwrap();
        let address = listener.local_addr().unwrap();
        drop(listener);
        address
    }

    fn cid(value: u64) -> ControllerNodeId {
        ControllerNodeId::new(value).unwrap()
    }

    fn voters() -> StableConfiguration {
        StableConfiguration::new(
            [
                NodeId::new(1).unwrap(),
                NodeId::new(2).unwrap(),
                NodeId::new(3).unwrap(),
            ],
            [],
        )
        .unwrap()
    }

    fn raw_runtime_config(dir: &Path, id: u64, timeout: u64) -> ControllerRuntimeConfig {
        let controller_feature_ranges = (1..=3_u64)
            .map(|node| (cid(node), FeatureRange::new(1, 3).unwrap()))
            .collect();
        ControllerRuntimeConfig {
            local_node: cid(id),
            directory: dir.to_path_buf(),
            static_configuration: voters(),
            controller_feature_ranges,
            initial_state: ControllerState::new([5; 16], 1, 1_000).unwrap(),
            persistence: ControllerPersistenceConfig::default(),
            timing: TimingConfig::new(1, 5, 10).unwrap(),
            randomized_election_timeout: timeout,
            limits: ControllerRuntimeLimits::default(),
            heartbeat_limits: HeartbeatLimits::default(),
            failure_detector: FailureDetectorConfig::default(),
        }
    }

    fn runtime_config(dir: &Path, id: u64, timeout: u64) -> ControllerRuntimeConfig {
        crate::initialize_controller_bootstrap(dir, [5; 16], &voters()).unwrap();
        raw_runtime_config(dir, id, timeout)
    }

    #[test]
    fn controller_runtime_refuses_to_serve_before_bootstrap_identity_is_durable() {
        let dir = temp_dir("bootstrap-required");
        assert!(ControllerRuntime::open(raw_runtime_config(&dir, 1, 5)).is_err());
        crate::initialize_controller_bootstrap(&dir, [5; 16], &voters()).unwrap();
        let runtime = ControllerRuntime::open(raw_runtime_config(&dir, 1, 5)).unwrap();
        assert_eq!(runtime.role(), Role::Follower);
        drop(runtime);
        fs::remove_dir_all(dir).unwrap();
    }

    #[test]
    fn controller_runtime_fails_closed_on_foreign_cluster_or_static_voter_set() {
        let dir = temp_dir("bootstrap-mismatch");
        crate::initialize_controller_bootstrap(&dir, [5; 16], &voters()).unwrap();

        let mut foreign_cluster = raw_runtime_config(&dir, 1, 5);
        foreign_cluster.initial_state = ControllerState::new([6; 16], 1, 1_000).unwrap();
        assert!(ControllerRuntime::open(foreign_cluster).is_err());

        let mut foreign_voters = raw_runtime_config(&dir, 1, 5);
        foreign_voters.static_configuration = StableConfiguration::new(
            [
                NodeId::new(1).unwrap(),
                NodeId::new(2).unwrap(),
                NodeId::new(4).unwrap(),
            ],
            [],
        )
        .unwrap();
        assert!(ControllerRuntime::open(foreign_voters).is_err());
        fs::remove_dir_all(dir).unwrap();
    }

    #[test]
    fn controller_runtime_requires_complete_compatible_feature_advertisements() {
        let dir = temp_dir("feature-advertisements");
        crate::initialize_controller_bootstrap(&dir, [5; 16], &voters()).unwrap();

        let mut missing = raw_runtime_config(&dir, 1, 5);
        missing.controller_feature_ranges.remove(&cid(3));
        assert!(ControllerRuntime::open(missing).is_err());

        let mut incompatible = raw_runtime_config(&dir, 1, 5);
        incompatible.controller_feature_ranges = (1..=3_u64)
            .map(|node| (cid(node), FeatureRange::new(2, 3).unwrap()))
            .collect();
        assert!(ControllerRuntime::open(incompatible).is_err());

        let runtime = ControllerRuntime::open(raw_runtime_config(&dir, 1, 5)).unwrap();
        assert_eq!(
            runtime.local_feature_range(),
            FeatureRange::new(1, 3).unwrap()
        );
        assert_eq!(runtime.controller_feature_ranges().len(), 3);
        drop(runtime);
        fs::remove_dir_all(dir).unwrap();
    }

    #[test]
    fn p9_level_five_requires_every_controller_voter_to_advertise_support() {
        let supported = (1..=3_u64)
            .map(|node| (cid(node), FeatureRange::new(1, 5).unwrap()))
            .collect::<BTreeMap<_, _>>();
        validate_active_feature_level(&supported, P9_RETENTION_FEATURE_LEVEL).unwrap();

        let mut one_old = supported;
        one_old.insert(cid(3), FeatureRange::new(1, 4).unwrap());
        assert!(validate_active_feature_level(&one_old, P9_RETENTION_FEATURE_LEVEL).is_err());
        validate_active_feature_level(&one_old, P8_PERFORMANCE_FEATURE_LEVEL).unwrap();
    }

    #[test]
    fn recovered_controller_runtime_starts_as_follower_and_source_identity_is_fenced() {
        let dir = temp_dir("recovery");
        let mut runtime = ControllerRuntime::open(runtime_config(&dir, 1, 5)).unwrap();
        assert_eq!(runtime.role(), Role::Follower);
        let message = RaftMessage::PreVoteRequest(astralog_raft::PreVoteRequest {
            prospective_term: Term::new(1),
            candidate_id: NodeId::new(2).unwrap(),
            last_log_index: LogIndex::ZERO,
            last_log_term: Term::ZERO,
        });
        assert!(runtime.step(cid(3), message, None).is_err());
        drop(runtime);
        let reopened = ControllerRuntime::open(runtime_config(&dir, 1, 5)).unwrap();
        assert_eq!(reopened.state().metadata_epoch(), 0);
        fs::remove_dir_all(dir).unwrap();
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn real_three_node_controller_elects_commits_fails_over_and_rejoins() {
        let root = temp_dir("cluster");
        let addresses = [reserve_addr(), reserve_addr(), reserve_addr()];
        let make_config = |id: u64| {
            let mut peers = BTreeMap::new();
            for peer in 1..=3_u64 {
                if peer != id {
                    peers.insert(
                        cid(peer),
                        ControllerPeerDescriptor {
                            controller_id: cid(peer),
                            address: addresses[usize::try_from(peer - 1).unwrap()],
                        },
                    );
                }
            }
            let timeout = 4 + id;
            ControllerNodeConfig {
                runtime: runtime_config(&root.join(format!("node-{id}")), id, timeout),
                transport: ControllerPeerTransportConfig {
                    local_controller_id: cid(id),
                    cluster_id: [5; 16],
                    bind_addr: addresses[usize::try_from(id - 1).unwrap()],
                    peers,
                    limits: crate::ControllerTransportLimits::default(),
                    max_inbound_frames: 1024,
                    max_outbound_frames_per_peer: 1024,
                    max_outbound_bytes_per_peer: 16 * 1024 * 1024,
                    reconnect_min_delay: Duration::from_millis(5),
                    reconnect_max_delay: Duration::from_millis(50),
                },
                tick_period: Duration::from_millis(10),
                election_timeout_sample: timeout,
                max_control_requests: 128,
            }
        };

        let mut nodes = BTreeMap::new();
        for id in 1..=3_u64 {
            nodes.insert(
                cid(id),
                start_controller_node(make_config(id)).await.unwrap(),
            );
        }
        let (leader, first_term) = wait_for_leader(&nodes, None, Term::ZERO).await;
        let follower = nodes.keys().copied().find(|node| *node != leader).unwrap();
        assert!(matches!(
            nodes[&follower]
                .mutate(ControllerCommand::AllocateProducerIds {
                    allocation_count: 10
                })
                .await,
            Err(ControllerError::NotLeader { .. })
        ));
        let (committing_leader, first) = mutate_on_current_leader(
            &nodes,
            ControllerCommand::AllocateProducerIds {
                allocation_count: 10,
            },
        )
        .await;
        let first_range = first.producer_ids.unwrap();
        assert!(first_range.first >= 1_000);
        assert_eq!((first_range.first - 1_000) % 10, 0);
        assert_eq!(first_range.last, first_range.first + 9);
        wait_for_epoch(&nodes, first.metadata_epoch, Duration::from_secs(5)).await;

        let lost_leader = nodes.remove(&committing_leader).unwrap();
        lost_leader.shutdown().await;
        let (_new_leader, new_term) =
            wait_for_leader(&nodes, Some(committing_leader), first_term).await;
        assert!(new_term > first_term);
        let (_, second) = mutate_on_current_leader(
            &nodes,
            ControllerCommand::AllocateProducerIds {
                allocation_count: 10,
            },
        )
        .await;
        let second_range = second.producer_ids.unwrap();
        assert!(second_range.first > first_range.last);
        assert_eq!((second_range.first - 1_000) % 10, 0);
        assert_eq!(second_range.last, second_range.first + 9);

        let restarted = start_controller_node(make_config(committing_leader.get()))
            .await
            .unwrap();
        nodes.insert(committing_leader, restarted);
        wait_for_epoch(&nodes, second.metadata_epoch, Duration::from_secs(5)).await;
        let status = nodes[&committing_leader].status().await.unwrap();
        assert!(!status.fatal);
        assert!(status.term >= new_term);

        for (_, node) in nodes {
            node.shutdown().await;
        }
        fs::remove_dir_all(root).unwrap();
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn controller_minority_isolation_cannot_commit_metadata_mutation() {
        let root = temp_dir("minority-isolation");
        let addresses = [reserve_addr(), reserve_addr(), reserve_addr()];
        let make_config = |id: u64| {
            let mut peers = BTreeMap::new();
            for peer in 1..=3_u64 {
                if peer != id {
                    peers.insert(
                        cid(peer),
                        ControllerPeerDescriptor {
                            controller_id: cid(peer),
                            address: addresses[usize::try_from(peer - 1).unwrap()],
                        },
                    );
                }
            }
            let timeout = 5 + id;
            ControllerNodeConfig {
                runtime: runtime_config(&root.join(format!("node-{id}")), id, timeout),
                transport: ControllerPeerTransportConfig {
                    local_controller_id: cid(id),
                    cluster_id: [5; 16],
                    bind_addr: addresses[usize::try_from(id - 1).unwrap()],
                    peers,
                    limits: crate::ControllerTransportLimits::default(),
                    max_inbound_frames: 256,
                    max_outbound_frames_per_peer: 256,
                    max_outbound_bytes_per_peer: 4 * 1024 * 1024,
                    reconnect_min_delay: Duration::from_millis(5),
                    reconnect_max_delay: Duration::from_millis(50),
                },
                tick_period: Duration::from_millis(10),
                election_timeout_sample: timeout,
                max_control_requests: 32,
            }
        };

        let mut nodes = BTreeMap::new();
        for id in 1..=3_u64 {
            nodes.insert(
                cid(id),
                start_controller_node(make_config(id)).await.unwrap(),
            );
        }
        let (leader, _) = wait_for_leader(&nodes, None, Term::ZERO).await;
        let committed = nodes[&leader].allocate_producer_ids(1).await.unwrap();
        assert_eq!(committed.first, 1_000);
        let committed_epoch = nodes[&leader].status().await.unwrap().metadata_epoch;
        wait_for_epoch(&nodes, committed_epoch, Duration::from_secs(5)).await;

        let victims = nodes
            .keys()
            .copied()
            .filter(|node| *node != leader)
            .collect::<Vec<_>>();
        for victim in victims {
            nodes.remove(&victim).unwrap().shutdown().await;
        }

        let mutation = time::timeout(
            Duration::from_secs(3),
            nodes[&leader].allocate_producer_ids(1),
        )
        .await;
        if let Ok(result) = mutation {
            assert!(result.is_err(), "minority controller committed a mutation");
        }
        time::sleep(Duration::from_millis(250)).await;
        assert_eq!(
            nodes[&leader].status().await.unwrap().metadata_epoch,
            committed_epoch,
            "minority isolation advanced committed controller metadata"
        );

        nodes.remove(&leader).unwrap().shutdown().await;
        fs::remove_dir_all(root).unwrap();
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 6)]
    async fn bounded_admin_pressure_does_not_starve_raft_or_heartbeat_control() {
        let root = temp_dir("control-priority");
        let addresses = [reserve_addr(), reserve_addr(), reserve_addr()];
        let make_config = |id: u64| {
            let mut peers = BTreeMap::new();
            for peer in 1..=3_u64 {
                if peer != id {
                    peers.insert(
                        cid(peer),
                        ControllerPeerDescriptor {
                            controller_id: cid(peer),
                            address: addresses[usize::try_from(peer - 1).unwrap()],
                        },
                    );
                }
            }
            let timeout = 6 + id;
            let mut runtime = runtime_config(&root.join(format!("node-{id}")), id, timeout);
            runtime.limits.max_pending_mutations = 32;
            ControllerNodeConfig {
                runtime,
                transport: ControllerPeerTransportConfig {
                    local_controller_id: cid(id),
                    cluster_id: [5; 16],
                    bind_addr: addresses[usize::try_from(id - 1).unwrap()],
                    peers,
                    limits: crate::ControllerTransportLimits::default(),
                    max_inbound_frames: 256,
                    max_outbound_frames_per_peer: 256,
                    max_outbound_bytes_per_peer: 4 * 1024 * 1024,
                    reconnect_min_delay: Duration::from_millis(5),
                    reconnect_max_delay: Duration::from_millis(50),
                },
                tick_period: Duration::from_millis(10),
                election_timeout_sample: timeout,
                max_control_requests: 8,
            }
        };

        let mut nodes = BTreeMap::new();
        for id in 1..=3_u64 {
            nodes.insert(
                cid(id),
                start_controller_node(make_config(id)).await.unwrap(),
            );
        }
        let registration = BrokerRegistration {
            broker_id: 1,
            node_uuid: [1; 16],
            client_host: "broker-1".into(),
            client_port: 9_001,
            internal_host: "raft-1".into(),
            internal_port: 10_001,
            rack: Some("rack-1".into()),
            zone: Some("zone-1".into()),
            min_feature: 1,
            max_feature: 1,
        };
        let (_, registered) = mutate_on_current_leader(&nodes, registration.into_command()).await;
        let (_, activated) = mutate_on_current_leader(
            &nodes,
            ControllerCommand::SetBrokerLifecycle {
                broker_id: 1,
                expected_incarnation: 1,
                expected_lifecycle: BrokerLifecycle::Joining,
                new_lifecycle: BrokerLifecycle::Active,
            },
        )
        .await;
        assert!(activated.metadata_epoch > registered.metadata_epoch);

        let leader =
            wait_for_leader_epoch(&nodes, activated.metadata_epoch, Duration::from_secs(5)).await;
        let leader_client = nodes[&leader].client();
        let mut pressure = Vec::new();
        for _ in 0..24 {
            let client = leader_client.clone();
            pressure.push(tokio::spawn(async move {
                client.allocate_producer_ids(1).await
            }));
        }

        let heartbeat_request = BrokerHeartbeatRequest {
            authority: BrokerAuthority {
                broker_id: 1,
                node_uuid: [1; 16],
                incarnation_epoch: 1,
            },
            applied_metadata_epoch: activated.metadata_epoch,
            min_feature: 1,
            max_feature: 1,
            replica_progress: Vec::new(),
            movement_progress: Vec::new(),
        };
        let heartbeat_deadline = time::Instant::now() + Duration::from_secs(5);
        loop {
            let current =
                wait_for_leader_epoch(&nodes, activated.metadata_epoch, Duration::from_secs(5))
                    .await;
            match nodes[&current].failure_detector_tick(10_000).await {
                Ok(()) => {}
                Err(ControllerError::NotLeader { .. } | ControllerError::Busy(_)) => {
                    assert!(time::Instant::now() < heartbeat_deadline);
                    continue;
                }
                Err(error) => panic!("failure-detector setup failed under pressure: {error}"),
            }
            match time::timeout(
                Duration::from_secs(3),
                nodes[&current].heartbeat(heartbeat_request.clone()),
            )
            .await
            .expect("heartbeat starved behind bounded admin pressure")
            {
                Ok(_) => break,
                Err(ControllerError::NotLeader { .. } | ControllerError::Busy(_)) => {}
                Err(ControllerError::Invalid(
                    "failure-detector tick is not initialized for current controller leadership",
                )) => {}
                Err(error) => panic!("heartbeat failed under bounded admin pressure: {error}"),
            }
            assert!(
                time::Instant::now() < heartbeat_deadline,
                "heartbeat could not observe a current initialized controller leader under bounded admin pressure"
            );
        }

        for task in pressure {
            let _ = time::timeout(Duration::from_secs(5), task)
                .await
                .expect("bounded admin request never completed");
        }
        let current =
            wait_for_leader_epoch(&nodes, activated.metadata_epoch, Duration::from_secs(5)).await;
        assert!(nodes[&current].status().await.unwrap().term > Term::ZERO);

        for (_, node) in nodes {
            node.shutdown().await;
        }
        fs::remove_dir_all(root).unwrap();
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn committed_mutation_with_lost_response_is_ambiguous_but_ids_are_not_reused() {
        let root = temp_dir("lost-response");
        let addresses = [reserve_addr(), reserve_addr(), reserve_addr()];
        let make_config = |id: u64| {
            let mut peers = BTreeMap::new();
            for peer in 1..=3_u64 {
                if peer != id {
                    peers.insert(
                        cid(peer),
                        ControllerPeerDescriptor {
                            controller_id: cid(peer),
                            address: addresses[usize::try_from(peer - 1).unwrap()],
                        },
                    );
                }
            }
            let timeout = 4 + id;
            ControllerNodeConfig {
                runtime: runtime_config(&root.join(format!("node-{id}")), id, timeout),
                transport: ControllerPeerTransportConfig {
                    local_controller_id: cid(id),
                    cluster_id: [5; 16],
                    bind_addr: addresses[usize::try_from(id - 1).unwrap()],
                    peers,
                    limits: crate::ControllerTransportLimits::default(),
                    max_inbound_frames: 1024,
                    max_outbound_frames_per_peer: 1024,
                    max_outbound_bytes_per_peer: 16 * 1024 * 1024,
                    reconnect_min_delay: Duration::from_millis(5),
                    reconnect_max_delay: Duration::from_millis(50),
                },
                tick_period: Duration::from_millis(10),
                election_timeout_sample: timeout,
                max_control_requests: 128,
            }
        };
        let mut nodes = BTreeMap::new();
        for id in 1..=3_u64 {
            nodes.insert(
                cid(id),
                start_controller_node(make_config(id)).await.unwrap(),
            );
        }
        let (leader, _) = wait_for_leader(&nodes, None, Term::ZERO).await;
        let (tx, rx) = oneshot::channel();
        nodes[&leader]
            .control
            .send(NodeControl::Mutate {
                command: ControllerCommand::AllocateProducerIds {
                    allocation_count: 10,
                },
                response: tx,
            })
            .await
            .unwrap();
        // Do not consume the response. First prove that some current leader has
        // applied epoch 2, which is only possible after P3 quorum commit. Then
        // drop the still-unread response to model a committed response lost in
        // transit. Requiring every follower to converge here is unnecessary for
        // the ambiguity property and made this test sensitive to unrelated CI
        // scheduling delays.
        wait_for_leader_epoch(&nodes, 2, Duration::from_secs(15)).await;
        drop(rx);
        let (_, retry) = mutate_on_current_leader(
            &nodes,
            ControllerCommand::AllocateProducerIds {
                allocation_count: 10,
            },
        )
        .await;
        let retry_range = retry.producer_ids.unwrap();
        assert!(retry_range.first >= 1_010);
        assert_eq!((retry_range.first - 1_000) % 10, 0);
        assert_eq!(retry_range.last, retry_range.first + 9);
        for (_, node) in nodes {
            node.shutdown().await;
        }
        fs::remove_dir_all(root).unwrap();
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn producer_id_allocator_is_bounded_commit_ordered_non_reusing_and_restart_durable() {
        let root = temp_dir("producer-ids");
        let addresses = [reserve_addr(), reserve_addr(), reserve_addr()];
        let make_config = |id: u64| {
            let mut peers = BTreeMap::new();
            for peer in 1..=3_u64 {
                if peer != id {
                    peers.insert(
                        cid(peer),
                        ControllerPeerDescriptor {
                            controller_id: cid(peer),
                            address: addresses[usize::try_from(peer - 1).unwrap()],
                        },
                    );
                }
            }
            let timeout = 4 + id;
            ControllerNodeConfig {
                runtime: runtime_config(&root.join(format!("node-{id}")), id, timeout),
                transport: ControllerPeerTransportConfig {
                    local_controller_id: cid(id),
                    cluster_id: [5; 16],
                    bind_addr: addresses[usize::try_from(id - 1).unwrap()],
                    peers,
                    limits: crate::ControllerTransportLimits::default(),
                    max_inbound_frames: 1024,
                    max_outbound_frames_per_peer: 1024,
                    max_outbound_bytes_per_peer: 16 * 1024 * 1024,
                    reconnect_min_delay: Duration::from_millis(5),
                    reconnect_max_delay: Duration::from_millis(50),
                },
                tick_period: Duration::from_millis(10),
                election_timeout_sample: timeout,
                max_control_requests: 128,
            }
        };

        let mut nodes = BTreeMap::new();
        for id in 1..=3_u64 {
            nodes.insert(
                cid(id),
                start_controller_node(make_config(id)).await.unwrap(),
            );
        }
        let (leader, first_term) = wait_for_leader(&nodes, None, Term::ZERO).await;
        let follower = nodes.keys().copied().find(|node| *node != leader).unwrap();
        assert!(matches!(
            nodes[&follower].allocate_producer_ids(1).await,
            Err(ControllerError::NotLeader { .. })
        ));
        assert!(nodes[&leader].allocate_producer_ids(0).await.is_err());
        assert!(
            nodes[&leader]
                .allocate_producer_ids(MAX_PRODUCER_IDS_PER_ALLOCATION + 1)
                .await
                .is_err()
        );

        let before_epoch = nodes[&leader].status().await.unwrap().metadata_epoch;
        let first = nodes[&leader].allocate_producer_ids(7).await.unwrap();
        assert_eq!(
            first,
            ProducerIdRange {
                first: 1_000,
                last: 1_006,
            }
        );
        let committed_epoch = nodes[&leader].status().await.unwrap().metadata_epoch;
        assert!(committed_epoch > before_epoch);
        wait_for_epoch(&nodes, committed_epoch, Duration::from_secs(5)).await;
        let snapshot_leader =
            wait_for_leader_epoch(&nodes, committed_epoch, Duration::from_secs(5)).await;
        assert_eq!(
            nodes[&snapshot_leader]
                .metadata_snapshot()
                .await
                .unwrap()
                .state
                .next_unallocated_producer_id(),
            1_007
        );

        // Send a second allocation through the raw control path and deliberately
        // discard its response only after a controller leader has applied the
        // next metadata epoch. The committed block is therefore consumed even
        // though the caller never learns its exact response.
        let (allocation_leader, _) = wait_for_leader(&nodes, None, Term::ZERO).await;
        let (tx, rx) = oneshot::channel();
        nodes[&allocation_leader]
            .control
            .send(NodeControl::Mutate {
                command: ControllerCommand::AllocateProducerIds {
                    allocation_count: 5,
                },
                response: tx,
            })
            .await
            .unwrap();
        let applied_leader =
            wait_for_leader_epoch(&nodes, committed_epoch + 1, Duration::from_secs(15)).await;
        assert_eq!(
            nodes[&applied_leader]
                .metadata_snapshot()
                .await
                .unwrap()
                .state
                .next_unallocated_producer_id(),
            1_012
        );
        drop(rx);

        // Fail the controller that observed the committed lost-response block.
        // The replacement leader must allocate strictly after that gap.
        let stopped = nodes.remove(&applied_leader).unwrap();
        stopped.shutdown().await;
        let (replacement, replacement_term) =
            wait_for_leader(&nodes, Some(applied_leader), first_term).await;
        assert!(replacement_term > first_term);
        let retry = nodes[&replacement].allocate_producer_ids(5).await.unwrap();
        assert_eq!(
            retry,
            ProducerIdRange {
                first: 1_012,
                last: 1_016,
            }
        );

        // Rejoin the failed voter, let the committed allocator state converge,
        // then shut down all voters and recover only from durable controller
        // state. The next block must continue at 1_017 after the full restart.
        let restarted = start_controller_node(make_config(applied_leader.get()))
            .await
            .unwrap();
        nodes.insert(applied_leader, restarted);
        let retry_epoch = nodes[&replacement].status().await.unwrap().metadata_epoch;
        wait_for_epoch(&nodes, retry_epoch, Duration::from_secs(5)).await;
        for (_, node) in nodes {
            node.shutdown().await;
        }

        let mut recovered = BTreeMap::new();
        for id in 1..=3_u64 {
            recovered.insert(
                cid(id),
                start_controller_node(make_config(id)).await.unwrap(),
            );
        }
        let (recovered_leader, _) = wait_for_leader(&recovered, None, Term::ZERO).await;
        let after_restart = recovered[&recovered_leader]
            .allocate_producer_ids(1)
            .await
            .unwrap();
        assert_eq!(
            after_restart,
            ProducerIdRange {
                first: 1_017,
                last: 1_017,
            }
        );

        for (_, node) in recovered {
            node.shutdown().await;
        }
        fs::remove_dir_all(root).unwrap();
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 6)]
    async fn p7_init_producer_is_commit_ordered_fenced_and_durable_across_failover() {
        let root = temp_dir("p7-init-producer");
        let addresses = [reserve_addr(), reserve_addr(), reserve_addr()];
        let make_config = |id: u64| {
            let mut peers = BTreeMap::new();
            for peer in 1..=3_u64 {
                if peer != id {
                    peers.insert(
                        cid(peer),
                        ControllerPeerDescriptor {
                            controller_id: cid(peer),
                            address: addresses[usize::try_from(peer - 1).unwrap()],
                        },
                    );
                }
            }
            let timeout = 4 + id;
            let mut runtime = runtime_config(&root.join(format!("node-{id}")), id, timeout);
            runtime.initial_state =
                ControllerState::new([5; 16], P7_PRODUCER_FEATURE_LEVEL, 1_000).unwrap();
            ControllerNodeConfig {
                runtime,
                transport: ControllerPeerTransportConfig {
                    local_controller_id: cid(id),
                    cluster_id: [5; 16],
                    bind_addr: addresses[usize::try_from(id - 1).unwrap()],
                    peers,
                    limits: crate::ControllerTransportLimits::default(),
                    max_inbound_frames: 1024,
                    max_outbound_frames_per_peer: 1024,
                    max_outbound_bytes_per_peer: 16 * 1024 * 1024,
                    reconnect_min_delay: Duration::from_millis(5),
                    reconnect_max_delay: Duration::from_millis(50),
                },
                tick_period: Duration::from_millis(10),
                election_timeout_sample: timeout,
                max_control_requests: 128,
            }
        };

        let mut nodes = BTreeMap::new();
        for id in 1..=3_u64 {
            nodes.insert(
                cid(id),
                start_controller_node(make_config(id)).await.unwrap(),
            );
        }

        let (leader, first_term) = wait_for_leader(&nodes, None, Term::ZERO).await;
        let follower = nodes.keys().copied().find(|node| *node != leader).unwrap();
        assert!(matches!(
            nodes[&follower].init_producer(None).await,
            Err(ControllerError::NotLeader { .. })
        ));

        let first = nodes[&leader].init_producer(None).await.unwrap();
        assert_eq!(first.producer_id.get(), 1_000);
        assert_eq!(first.producer_epoch.get(), 1);
        assert_eq!(
            nodes[&leader]
                .producer_authority(first.producer_id)
                .await
                .unwrap(),
            Some(first),
            "InitProducer success must observe committed/applied authority before response"
        );
        let first_epoch = nodes[&leader].status().await.unwrap().metadata_epoch;
        wait_for_epoch(&nodes, first_epoch, Duration::from_secs(10)).await;

        let second_epoch = nodes[&leader].init_producer(Some(first)).await.unwrap();
        assert_eq!(second_epoch.producer_id, first.producer_id);
        assert_eq!(second_epoch.producer_epoch.get(), 2);
        assert!(nodes[&leader].init_producer(Some(first)).await.is_err());

        // Model a committed new-session response that is lost in transit. The
        // authority must remain consumed; a fresh initialization uses the next ID.
        let (lost_new_leader, _) = wait_for_leader(&nodes, None, Term::ZERO).await;
        let (tx, rx) = oneshot::channel();
        nodes[&lost_new_leader]
            .control
            .send(NodeControl::InitProducer {
                expected_authority: None,
                response: tx,
            })
            .await
            .unwrap();
        let lost_new = ProducerAuthority::new(
            ProducerId::new(1_001).unwrap(),
            ProducerEpoch::new(1).unwrap(),
        );
        wait_for_producer_authority(&nodes, lost_new, Duration::from_secs(15)).await;
        drop(rx);
        let after_lost_new = init_producer_on_current_leader(&nodes, None).await;
        assert_eq!(after_lost_new.producer_id.get(), 1_002);
        assert_eq!(after_lost_new.producer_epoch.get(), 1);

        // Model a committed epoch-bump response that is lost. Retrying the stale
        // expected epoch must fail rather than incrementing/reusing authority.
        let (lost_bump_leader, _) = wait_for_leader(&nodes, None, Term::ZERO).await;
        let (tx, rx) = oneshot::channel();
        nodes[&lost_bump_leader]
            .control
            .send(NodeControl::InitProducer {
                expected_authority: Some(second_epoch),
                response: tx,
            })
            .await
            .unwrap();
        let third_epoch =
            ProducerAuthority::new(second_epoch.producer_id, ProducerEpoch::new(3).unwrap());
        wait_for_producer_authority(&nodes, third_epoch, Duration::from_secs(15)).await;
        drop(rx);
        assert!(
            init_producer_rejected_on_current_leader(&nodes, Some(second_epoch))
                .await
                .is_err()
        );

        let (old_leader, old_term) = wait_for_leader(&nodes, None, Term::ZERO).await;
        nodes.remove(&old_leader).unwrap().shutdown().await;
        let (new_leader, new_term) = wait_for_leader(&nodes, Some(old_leader), old_term).await;
        assert!(new_term > old_term.max(first_term));
        assert_eq!(
            nodes[&new_leader]
                .producer_authority(third_epoch.producer_id)
                .await
                .unwrap(),
            Some(third_epoch)
        );
        let fourth_epoch = nodes[&new_leader]
            .init_producer(Some(third_epoch))
            .await
            .unwrap();
        assert_eq!(fourth_epoch.producer_epoch.get(), 4);
        let committed_epoch = nodes[&new_leader].status().await.unwrap().metadata_epoch;

        let restarted = start_controller_node(make_config(old_leader.get()))
            .await
            .unwrap();
        nodes.insert(old_leader, restarted);
        wait_for_epoch(&nodes, committed_epoch, Duration::from_secs(15)).await;
        for node in nodes.values() {
            assert_eq!(
                node.producer_authority(fourth_epoch.producer_id)
                    .await
                    .unwrap(),
                Some(fourth_epoch)
            );
            assert_eq!(
                node.producer_authority(lost_new.producer_id).await.unwrap(),
                Some(lost_new)
            );
            assert_eq!(
                node.producer_authority(after_lost_new.producer_id)
                    .await
                    .unwrap(),
                Some(after_lost_new)
            );
        }

        for (_, node) in nodes {
            node.shutdown().await;
        }
        fs::remove_dir_all(root).unwrap();
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn broker_membership_incarnation_and_lifecycle_are_committed_and_fenced_across_failover()
    {
        let root = temp_dir("membership");
        let addresses = [reserve_addr(), reserve_addr(), reserve_addr()];
        let make_config = |id: u64| {
            let mut peers = BTreeMap::new();
            for peer in 1..=3_u64 {
                if peer != id {
                    peers.insert(
                        cid(peer),
                        ControllerPeerDescriptor {
                            controller_id: cid(peer),
                            address: addresses[usize::try_from(peer - 1).unwrap()],
                        },
                    );
                }
            }
            let timeout = 4 + id;
            ControllerNodeConfig {
                runtime: runtime_config(&root.join(format!("node-{id}")), id, timeout),
                transport: ControllerPeerTransportConfig {
                    local_controller_id: cid(id),
                    cluster_id: [5; 16],
                    bind_addr: addresses[usize::try_from(id - 1).unwrap()],
                    peers,
                    limits: crate::ControllerTransportLimits::default(),
                    max_inbound_frames: 1024,
                    max_outbound_frames_per_peer: 1024,
                    max_outbound_bytes_per_peer: 16 * 1024 * 1024,
                    reconnect_min_delay: Duration::from_millis(5),
                    reconnect_max_delay: Duration::from_millis(50),
                },
                tick_period: Duration::from_millis(50),
                election_timeout_sample: timeout,
                max_control_requests: 128,
            }
        };
        let registration = BrokerRegistration {
            broker_id: 10,
            node_uuid: [10; 16],
            client_host: "broker-10".into(),
            client_port: 9092,
            internal_host: "broker-10-internal".into(),
            internal_port: 9093,
            rack: Some("rack-a".into()),
            zone: Some("zone-a".into()),
            min_feature: 1,
            max_feature: 3,
        };

        let mut nodes = BTreeMap::new();
        for id in 1..=3_u64 {
            nodes.insert(
                cid(id),
                start_controller_node(make_config(id)).await.unwrap(),
            );
        }
        let (leader, registered_outcome) =
            mutate_on_current_leader(&nodes, registration.clone().into_command()).await;
        let registered = nodes[&leader].broker_record(10).await.unwrap().unwrap();
        assert_eq!(registered.incarnation_epoch, 1);
        assert_eq!(registered.lifecycle, BrokerLifecycle::Joining);
        wait_for_epoch(
            &nodes,
            registered_outcome.metadata_epoch,
            Duration::from_secs(5),
        )
        .await;
        let incarnation_one = BrokerAuthority {
            broker_id: 10,
            node_uuid: [10; 16],
            incarnation_epoch: 1,
        };
        assert!(
            validate_authority_on_current_leader(&nodes, incarnation_one)
                .await
                .is_ok()
        );

        let before_rejection = registered_outcome.metadata_epoch;
        let mut duplicate_id = registration.clone();
        duplicate_id.node_uuid = [11; 16];
        assert!(
            mutate_rejected_on_current_leader(&nodes, duplicate_id.into_command())
                .await
                .is_err()
        );
        let mut duplicate_uuid = registration.clone();
        duplicate_uuid.broker_id = 11;
        assert!(
            mutate_rejected_on_current_leader(&nodes, duplicate_uuid.into_command())
                .await
                .is_err()
        );
        let (leader, _) = wait_for_leader(&nodes, None, Term::ZERO).await;
        assert_eq!(
            nodes[&leader].status().await.unwrap().metadata_epoch,
            before_rejection
        );

        let (leader, activated_outcome) = mutate_on_current_leader(
            &nodes,
            BrokerLifecycleChange {
                broker_id: 10,
                expected_incarnation: 1,
                expected_lifecycle: BrokerLifecycle::Joining,
                new_lifecycle: BrokerLifecycle::Active,
            }
            .into_command(),
        )
        .await;
        assert!(activated_outcome.metadata_epoch > registered_outcome.metadata_epoch);
        assert_eq!(
            nodes[&leader]
                .broker_record(10)
                .await
                .unwrap()
                .unwrap()
                .lifecycle,
            BrokerLifecycle::Active
        );

        let foreign_rejoin = BrokerRejoin {
            broker_id: 10,
            node_uuid: [99; 16],
            expected_incarnation: 1,
            client_host: "stolen".into(),
            client_port: 9092,
            internal_host: "stolen-internal".into(),
            internal_port: 9093,
            rack: None,
            zone: None,
            min_feature: 1,
            max_feature: 3,
        };
        assert!(
            mutate_rejected_on_current_leader(&nodes, foreign_rejoin.into_command().unwrap())
                .await
                .is_err()
        );

        let rejoin_command = BrokerRejoin {
            broker_id: 10,
            node_uuid: [10; 16],
            expected_incarnation: 1,
            client_host: "broker-10-new".into(),
            client_port: 9192,
            internal_host: "broker-10-new-internal".into(),
            internal_port: 9193,
            rack: Some("rack-b".into()),
            zone: Some("zone-b".into()),
            min_feature: 1,
            max_feature: 3,
        }
        .into_command()
        .unwrap();
        let (leader, rejoined_outcome) = mutate_on_current_leader(&nodes, rejoin_command).await;
        let rejoined = nodes[&leader].broker_record(10).await.unwrap().unwrap();
        assert_eq!(rejoined.incarnation_epoch, 2);
        assert_eq!(rejoined.lifecycle, BrokerLifecycle::Active);
        // The response is emitted only after commit+apply, so incarnation 1 is
        // already fenced when the client observes incarnation 2 success.
        assert!(
            validate_authority_on_current_leader(&nodes, incarnation_one)
                .await
                .is_err()
        );
        let incarnation_two = BrokerAuthority {
            incarnation_epoch: 2,
            ..incarnation_one
        };
        assert!(
            validate_authority_on_current_leader(&nodes, incarnation_two)
                .await
                .is_ok()
        );
        wait_for_epoch(
            &nodes,
            rejoined_outcome.metadata_epoch,
            Duration::from_secs(5),
        )
        .await;

        let (leader_to_stop, term_to_replace) = wait_for_leader(&nodes, None, Term::ZERO).await;
        let stopped = nodes.remove(&leader_to_stop).unwrap();
        stopped.shutdown().await;
        let (_new_leader, new_term) =
            wait_for_leader(&nodes, Some(leader_to_stop), term_to_replace).await;
        assert!(new_term > term_to_replace);
        assert!(
            validate_authority_on_current_leader(&nodes, incarnation_one)
                .await
                .is_err()
        );
        assert!(
            validate_authority_on_current_leader(&nodes, incarnation_two)
                .await
                .is_ok()
        );

        let (_, draining_outcome) = mutate_on_current_leader(
            &nodes,
            BrokerLifecycleChange {
                broker_id: 10,
                expected_incarnation: 2,
                expected_lifecycle: BrokerLifecycle::Active,
                new_lifecycle: BrokerLifecycle::Draining,
            }
            .into_command(),
        )
        .await;
        let (leader, _) = wait_for_leader(&nodes, None, Term::ZERO).await;
        assert_eq!(
            nodes[&leader]
                .broker_record(10)
                .await
                .unwrap()
                .unwrap()
                .lifecycle,
            BrokerLifecycle::Draining
        );
        let (_, fenced_outcome) = mutate_on_current_leader(
            &nodes,
            BrokerLifecycleChange {
                broker_id: 10,
                expected_incarnation: 2,
                expected_lifecycle: BrokerLifecycle::Draining,
                new_lifecycle: BrokerLifecycle::Fenced,
            }
            .into_command(),
        )
        .await;
        assert!(fenced_outcome.metadata_epoch > draining_outcome.metadata_epoch);
        assert!(
            validate_authority_on_current_leader(&nodes, incarnation_two)
                .await
                .is_err()
        );
        assert!(
            mutate_rejected_on_current_leader(
                &nodes,
                BrokerRejoin {
                    broker_id: 10,
                    node_uuid: [10; 16],
                    expected_incarnation: 2,
                    client_host: "broker-10".into(),
                    client_port: 9092,
                    internal_host: "broker-10-internal".into(),
                    internal_port: 9093,
                    rack: None,
                    zone: None,
                    min_feature: 1,
                    max_feature: 3,
                }
                .into_command()
                .unwrap(),
            )
            .await
            .is_err()
        );

        let (_, removed_outcome) = mutate_on_current_leader(
            &nodes,
            BrokerLifecycleChange {
                broker_id: 10,
                expected_incarnation: 2,
                expected_lifecycle: BrokerLifecycle::Fenced,
                new_lifecycle: BrokerLifecycle::Removed,
            }
            .into_command(),
        )
        .await;
        let (leader, _) = wait_for_leader(&nodes, None, Term::ZERO).await;
        assert_eq!(
            nodes[&leader]
                .broker_record(10)
                .await
                .unwrap()
                .unwrap()
                .lifecycle,
            BrokerLifecycle::Removed
        );
        assert!(
            mutate_rejected_on_current_leader(
                &nodes,
                BrokerLifecycleChange {
                    broker_id: 10,
                    expected_incarnation: 2,
                    expected_lifecycle: BrokerLifecycle::Removed,
                    new_lifecycle: BrokerLifecycle::Active,
                }
                .into_command(),
            )
            .await
            .is_err()
        );
        wait_for_epoch(
            &nodes,
            removed_outcome.metadata_epoch,
            Duration::from_secs(5),
        )
        .await;

        let restarted = start_controller_node(make_config(leader_to_stop.get()))
            .await
            .unwrap();
        nodes.insert(leader_to_stop, restarted);
        wait_for_epoch(
            &nodes,
            removed_outcome.metadata_epoch,
            Duration::from_secs(5),
        )
        .await;
        assert_eq!(
            nodes[&leader_to_stop]
                .broker_record(10)
                .await
                .unwrap()
                .unwrap()
                .lifecycle,
            BrokerLifecycle::Removed
        );

        for (_, node) in nodes {
            node.shutdown().await;
        }
        fs::remove_dir_all(root).unwrap();
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 6)]
    async fn broker_incarnation_crash_boundaries_preserve_only_committed_authority() {
        let root = temp_dir("membership-crash-boundaries");
        let addresses = [reserve_addr(), reserve_addr(), reserve_addr()];
        let make_config = |id: u64| {
            let mut peers = BTreeMap::new();
            for peer in 1..=3_u64 {
                if peer != id {
                    peers.insert(
                        cid(peer),
                        ControllerPeerDescriptor {
                            controller_id: cid(peer),
                            address: addresses[usize::try_from(peer - 1).unwrap()],
                        },
                    );
                }
            }
            let timeout = 5 + id;
            ControllerNodeConfig {
                runtime: runtime_config(&root.join(format!("node-{id}")), id, timeout),
                transport: ControllerPeerTransportConfig {
                    local_controller_id: cid(id),
                    cluster_id: [5; 16],
                    bind_addr: addresses[usize::try_from(id - 1).unwrap()],
                    peers,
                    limits: crate::ControllerTransportLimits::default(),
                    max_inbound_frames: 512,
                    max_outbound_frames_per_peer: 512,
                    max_outbound_bytes_per_peer: 8 * 1024 * 1024,
                    reconnect_min_delay: Duration::from_millis(5),
                    reconnect_max_delay: Duration::from_millis(50),
                },
                tick_period: Duration::from_millis(10),
                election_timeout_sample: timeout,
                max_control_requests: 64,
            }
        };

        let mut nodes = BTreeMap::new();
        for id in 1..=3_u64 {
            nodes.insert(
                cid(id),
                start_controller_node(make_config(id)).await.unwrap(),
            );
        }
        let registration = BrokerRegistration {
            broker_id: 10,
            node_uuid: [10; 16],
            client_host: "broker-10".into(),
            client_port: 9_092,
            internal_host: "broker-10-internal".into(),
            internal_port: 9_093,
            rack: Some("rack-a".into()),
            zone: Some("zone-a".into()),
            min_feature: 1,
            max_feature: 3,
        };
        let (_, registered) =
            mutate_on_current_leader(&nodes, registration.clone().into_command()).await;
        let (_, activated) = mutate_on_current_leader(
            &nodes,
            BrokerLifecycleChange {
                broker_id: 10,
                expected_incarnation: 1,
                expected_lifecycle: BrokerLifecycle::Joining,
                new_lifecycle: BrokerLifecycle::Active,
            }
            .into_command(),
        )
        .await;
        assert!(activated.metadata_epoch > registered.metadata_epoch);
        wait_for_epoch(&nodes, activated.metadata_epoch, Duration::from_secs(5)).await;

        let incarnation_one = BrokerAuthority {
            broker_id: 10,
            node_uuid: [10; 16],
            incarnation_epoch: 1,
        };
        assert!(
            validate_authority_on_current_leader(&nodes, incarnation_one)
                .await
                .is_ok()
        );

        // Crash-before-commit: isolate the current leader, let it accept a local
        // rejoin proposal without quorum, then kill it. The surviving durable
        // majority must retain incarnation 1 as the only authority.
        let (isolated_leader, _) = wait_for_leader(&nodes, None, Term::ZERO).await;
        let followers = nodes
            .keys()
            .copied()
            .filter(|node| *node != isolated_leader)
            .collect::<Vec<_>>();
        for follower in &followers {
            nodes.remove(follower).unwrap().shutdown().await;
        }
        let rejoin = BrokerRejoin {
            broker_id: 10,
            node_uuid: [10; 16],
            expected_incarnation: 1,
            client_host: "broker-10-v2".into(),
            client_port: 9_192,
            internal_host: "broker-10-v2-internal".into(),
            internal_port: 9_193,
            rack: Some("rack-b".into()),
            zone: Some("zone-b".into()),
            min_feature: 1,
            max_feature: 3,
        }
        .into_command()
        .unwrap();
        let (tx, rx) = oneshot::channel();
        nodes[&isolated_leader]
            .control
            .send(NodeControl::Mutate {
                command: rejoin.clone(),
                response: tx,
            })
            .await
            .unwrap();
        time::sleep(Duration::from_millis(75)).await;
        assert_eq!(
            nodes[&isolated_leader]
                .status()
                .await
                .unwrap()
                .metadata_epoch,
            activated.metadata_epoch,
            "isolated leader committed broker incarnation without quorum"
        );
        drop(rx);
        nodes.remove(&isolated_leader).unwrap().shutdown().await;

        for follower in &followers {
            nodes.insert(
                *follower,
                start_controller_node(make_config(follower.get()))
                    .await
                    .unwrap(),
            );
        }
        let (surviving_leader, _) = wait_for_leader(&nodes, None, Term::ZERO).await;
        let after_uncommitted = nodes[&surviving_leader]
            .broker_record(10)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(after_uncommitted.incarnation_epoch, 1);
        assert!(
            validate_authority_on_current_leader(&nodes, incarnation_one)
                .await
                .is_ok()
        );

        // Rejoin the crashed voter so its old uncommitted suffix is reconciled,
        // then commit incarnation 2 while deliberately discarding the response.
        nodes.insert(
            isolated_leader,
            start_controller_node(make_config(isolated_leader.get()))
                .await
                .unwrap(),
        );
        wait_for_epoch(&nodes, activated.metadata_epoch, Duration::from_secs(5)).await;
        let (commit_leader, _) = wait_for_leader(&nodes, None, Term::ZERO).await;
        let (commit_tx, commit_rx) = oneshot::channel();
        nodes[&commit_leader]
            .control
            .send(NodeControl::Mutate {
                command: rejoin,
                response: commit_tx,
            })
            .await
            .unwrap();

        let deadline = time::Instant::now() + Duration::from_secs(10);
        let applied_leader = loop {
            let leader =
                wait_for_leader_epoch(&nodes, activated.metadata_epoch, Duration::from_secs(5))
                    .await;
            if nodes[&leader]
                .broker_record(10)
                .await
                .unwrap()
                .is_some_and(|broker| broker.incarnation_epoch == 2)
            {
                break leader;
            }
            assert!(
                time::Instant::now() < deadline,
                "committed incarnation 2 was not observed before response-loss fault"
            );
            time::sleep(Duration::from_millis(20)).await;
        };
        drop(commit_rx);
        nodes.remove(&applied_leader).unwrap().shutdown().await;
        let (_replacement, _) = wait_for_leader(&nodes, Some(applied_leader), Term::ZERO).await;

        let incarnation_two = BrokerAuthority {
            incarnation_epoch: 2,
            ..incarnation_one
        };
        assert!(
            validate_authority_on_current_leader(&nodes, incarnation_one)
                .await
                .is_err(),
            "lost rejoin response resurrected old incarnation authority"
        );
        assert!(
            validate_authority_on_current_leader(&nodes, incarnation_two)
                .await
                .is_ok(),
            "committed incarnation did not survive response loss and leader death"
        );
        let (heartbeat_leader, _) = wait_for_leader(&nodes, None, Term::ZERO).await;
        nodes[&heartbeat_leader]
            .failure_detector_tick(50_000)
            .await
            .unwrap();
        let applied_metadata_epoch = nodes[&heartbeat_leader]
            .status()
            .await
            .unwrap()
            .metadata_epoch;
        let stale_heartbeat = BrokerHeartbeatRequest {
            authority: incarnation_one,
            applied_metadata_epoch,
            min_feature: 1,
            max_feature: 3,
            replica_progress: Vec::new(),
            movement_progress: Vec::new(),
        };
        assert!(
            nodes[&heartbeat_leader]
                .heartbeat(stale_heartbeat.clone())
                .await
                .is_err(),
            "old process heartbeat was accepted after committed rejoin"
        );
        nodes[&heartbeat_leader]
            .heartbeat(BrokerHeartbeatRequest {
                authority: incarnation_two,
                ..stale_heartbeat
            })
            .await
            .unwrap();

        for (_, node) in nodes {
            node.shutdown().await;
        }
        fs::remove_dir_all(root).unwrap();
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 8)]
    async fn controller_leader_failure_at_every_movement_stage_preserves_durable_workflow() {
        let root = temp_dir("movement-stage-failover");
        let addresses = [reserve_addr(), reserve_addr(), reserve_addr()];
        let make_config = |id: u64| {
            let mut peers = BTreeMap::new();
            for peer in 1..=3_u64 {
                if peer != id {
                    peers.insert(
                        cid(peer),
                        ControllerPeerDescriptor {
                            controller_id: cid(peer),
                            address: addresses[usize::try_from(peer - 1).unwrap()],
                        },
                    );
                }
            }
            let timeout = 6 + id;
            ControllerNodeConfig {
                runtime: runtime_config(&root.join(format!("node-{id}")), id, timeout),
                transport: ControllerPeerTransportConfig {
                    local_controller_id: cid(id),
                    cluster_id: [5; 16],
                    bind_addr: addresses[usize::try_from(id - 1).unwrap()],
                    peers,
                    limits: crate::ControllerTransportLimits::default(),
                    max_inbound_frames: 512,
                    max_outbound_frames_per_peer: 512,
                    max_outbound_bytes_per_peer: 8 * 1024 * 1024,
                    reconnect_min_delay: Duration::from_millis(5),
                    reconnect_max_delay: Duration::from_millis(50),
                },
                tick_period: Duration::from_millis(10),
                election_timeout_sample: timeout,
                max_control_requests: 64,
            }
        };

        let mut nodes = BTreeMap::new();
        for id in 1..=3_u64 {
            nodes.insert(
                cid(id),
                start_controller_node(make_config(id)).await.unwrap(),
            );
        }
        for broker_id in 1..=4_u32 {
            mutate_on_current_leader(
                &nodes,
                ControllerCommand::RegisterBroker {
                    broker_id,
                    node_uuid: [u8::try_from(broker_id).unwrap(); 16],
                    initial_incarnation: 1,
                    client_host: format!("b{broker_id}"),
                    client_port: 9_000 + u16::try_from(broker_id).unwrap(),
                    internal_host: format!("r{broker_id}"),
                    internal_port: 10_000 + u16::try_from(broker_id).unwrap(),
                    rack: Some(format!("rack-{broker_id}")),
                    zone: Some(format!("zone-{broker_id}")),
                    min_feature: 1,
                    max_feature: 1,
                },
            )
            .await;
            mutate_on_current_leader(
                &nodes,
                ControllerCommand::SetBrokerLifecycle {
                    broker_id,
                    expected_incarnation: 1,
                    expected_lifecycle: BrokerLifecycle::Joining,
                    new_lifecycle: BrokerLifecycle::Active,
                },
            )
            .await;
        }
        let topic_id = astralog_common::TopicId::new([0x31; 16]);
        mutate_on_current_leader(
            &nodes,
            ControllerCommand::CreateTopic {
                topic_id,
                topic_name: "stage-failover".into(),
                partition_count: 1,
                replication_factor: 3,
                failure_domain_policy: crate::FailureDomainPolicy::Disabled,
                assignments: vec![crate::AssignmentRecord {
                    topic_id,
                    partition_id: 0,
                    assignment_generation: crate::AssignmentGeneration::new(1).unwrap(),
                    voters: vec![1, 2, 3],
                    learners: Vec::new(),
                }],
            },
        )
        .await;
        let movement_id = MovementId::new([0x32; 16]).unwrap();
        mutate_on_current_leader(
            &nodes,
            ControllerCommand::BeginMovement {
                movement_id,
                topic_id,
                partition_id: 0,
                expected_assignment_generation: crate::AssignmentGeneration::new(1).unwrap(),
                source_broker_id: 1,
                destination_broker_id: 4,
                target_voters: vec![2, 3, 4],
                preferred_leader: Some(4),
            },
        )
        .await;

        let stages = [
            crate::MovementStage::Planned,
            crate::MovementStage::LearnerRequested,
            crate::MovementStage::LearnerCreated,
            crate::MovementStage::CatchingUp,
            crate::MovementStage::PromotionReady,
            crate::MovementStage::EnterJointRequested,
            crate::MovementStage::JointCommitted,
            crate::MovementStage::LeaveJointRequested,
            crate::MovementStage::NewStableCommitted,
            crate::MovementStage::LeadershipTransfer,
            crate::MovementStage::OldReplicaRemoveRequested,
            crate::MovementStage::OldReplicaRemoved,
            crate::MovementStage::Complete,
        ];

        for (index, stage) in stages.iter().copied().enumerate() {
            let (leader, term) = wait_for_leader(&nodes, None, Term::ZERO).await;
            let before = nodes[&leader].metadata_snapshot().await.unwrap();
            assert_eq!(before.state.movements()[&movement_id].stage, stage);
            let epoch = before.epoch;

            nodes.remove(&leader).unwrap().shutdown().await;
            let (replacement, replacement_term) = wait_for_leader(&nodes, Some(leader), term).await;
            assert!(replacement_term > term);
            let recovered = nodes[&replacement].metadata_snapshot().await.unwrap();
            assert_eq!(recovered.epoch, epoch);
            assert_eq!(recovered.state.movements()[&movement_id].stage, stage);

            nodes.insert(
                leader,
                start_controller_node(make_config(leader.get()))
                    .await
                    .unwrap(),
            );
            wait_for_epoch(&nodes, epoch, Duration::from_secs(5)).await;

            if stage == crate::MovementStage::Complete {
                break;
            }
            if stage == crate::MovementStage::OldReplicaRemoved {
                mutate_on_current_leader(
                    &nodes,
                    ControllerCommand::FinalizeMovement {
                        movement_id,
                        expected_stage: crate::MovementStage::OldReplicaRemoved,
                        replacement: crate::AssignmentRecord {
                            topic_id,
                            partition_id: 0,
                            assignment_generation: crate::AssignmentGeneration::new(2).unwrap(),
                            voters: vec![2, 3, 4],
                            learners: Vec::new(),
                        },
                    },
                )
                .await;
            } else {
                let next = stages[index + 1];
                mutate_on_current_leader(
                    &nodes,
                    ControllerCommand::AdvanceMovement {
                        movement_id,
                        expected_stage: stage,
                        new_stage: next,
                    },
                )
                .await;
            }
        }

        let (leader, _) = wait_for_leader(&nodes, None, Term::ZERO).await;
        let final_state = nodes[&leader].metadata_snapshot().await.unwrap().state;
        assert_eq!(
            final_state.movements()[&movement_id].stage,
            crate::MovementStage::Complete
        );
        assert_eq!(
            final_state.assignments()[&(topic_id, 0)].voters,
            vec![2, 3, 4]
        );
        assert_eq!(
            final_state.assignments()[&(topic_id, 0)]
                .assignment_generation
                .get(),
            2
        );

        for (_, node) in nodes {
            node.shutdown().await;
        }
        fs::remove_dir_all(root).unwrap();
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn feature_level_is_explicit_compatible_monotonic_and_durable_across_failover() {
        let root = temp_dir("feature-level");
        let addresses = [reserve_addr(), reserve_addr(), reserve_addr()];
        let ranges = BTreeMap::from([
            (cid(1), FeatureRange::new(1, 3).unwrap()),
            (cid(2), FeatureRange::new(1, 2).unwrap()),
            (cid(3), FeatureRange::new(1, 2).unwrap()),
        ]);
        let make_config = |id: u64, advertised: &BTreeMap<ControllerNodeId, FeatureRange>| {
            let mut peers = BTreeMap::new();
            for peer in 1..=3_u64 {
                if peer != id {
                    peers.insert(
                        cid(peer),
                        ControllerPeerDescriptor {
                            controller_id: cid(peer),
                            address: addresses[usize::try_from(peer - 1).unwrap()],
                        },
                    );
                }
            }
            let timeout = 4 + id;
            let mut runtime = runtime_config(&root.join(format!("node-{id}")), id, timeout);
            runtime.controller_feature_ranges = advertised.clone();
            ControllerNodeConfig {
                runtime,
                transport: ControllerPeerTransportConfig {
                    local_controller_id: cid(id),
                    cluster_id: [5; 16],
                    bind_addr: addresses[usize::try_from(id - 1).unwrap()],
                    peers,
                    limits: crate::ControllerTransportLimits::default(),
                    max_inbound_frames: 1024,
                    max_outbound_frames_per_peer: 1024,
                    max_outbound_bytes_per_peer: 16 * 1024 * 1024,
                    reconnect_min_delay: Duration::from_millis(5),
                    reconnect_max_delay: Duration::from_millis(50),
                },
                tick_period: Duration::from_millis(20),
                election_timeout_sample: timeout,
                max_control_requests: 128,
            }
        };

        let mut nodes = BTreeMap::new();
        for id in 1..=3_u64 {
            nodes.insert(
                cid(id),
                start_controller_node(make_config(id, &ranges))
                    .await
                    .unwrap(),
            );
        }
        let (_leader, first_term) = wait_for_leader(&nodes, None, Term::ZERO).await;
        for (id, node) in &nodes {
            let status = node.status().await.unwrap();
            let range = ranges[id];
            assert_eq!(status.active_feature_level, 1);
            assert_eq!(status.min_feature, range.min());
            assert_eq!(status.max_feature, range.max());
        }

        let broker = BrokerRegistration {
            broker_id: 30,
            node_uuid: [30; 16],
            client_host: "broker-30".into(),
            client_port: 9092,
            internal_host: "broker-30-internal".into(),
            internal_port: 9093,
            rack: Some("rack-feature".into()),
            zone: Some("zone-feature".into()),
            min_feature: 1,
            max_feature: 3,
        };
        let (_, registered) = mutate_on_current_leader(&nodes, broker.clone().into_command()).await;
        let (_, activated) = mutate_on_current_leader(
            &nodes,
            BrokerLifecycleChange {
                broker_id: 30,
                expected_incarnation: 1,
                expected_lifecycle: BrokerLifecycle::Joining,
                new_lifecycle: BrokerLifecycle::Active,
            }
            .into_command(),
        )
        .await;
        assert!(activated.metadata_epoch > registered.metadata_epoch);

        let limited_joining = BrokerRegistration {
            broker_id: 31,
            node_uuid: [31; 16],
            client_host: "broker-31".into(),
            client_port: 9192,
            internal_host: "broker-31-internal".into(),
            internal_port: 9193,
            rack: None,
            zone: None,
            min_feature: 1,
            max_feature: 1,
        };
        let (_, limited_registered) =
            mutate_on_current_leader(&nodes, limited_joining.into_command()).await;

        let (leader, _) = wait_for_leader(&nodes, None, Term::ZERO).await;
        nodes[&leader].failure_detector_tick(1).await.unwrap();
        nodes[&leader]
            .heartbeat(BrokerHeartbeatRequest {
                authority: BrokerAuthority {
                    broker_id: 30,
                    node_uuid: [30; 16],
                    incarnation_epoch: 1,
                },
                applied_metadata_epoch: limited_registered.metadata_epoch,
                min_feature: 1,
                max_feature: 3,
                replica_progress: Vec::new(),
                movement_progress: Vec::new(),
            })
            .await
            .unwrap();

        // Every active broker supports level 3, but controller voters 2 and 3 do not.
        // Controller compatibility therefore rejects the bump before it enters Raft.
        assert!(nodes[&leader].bump_feature_level(3).await.is_err());
        assert_eq!(
            nodes[&leader].status().await.unwrap().active_feature_level,
            1
        );

        let bumped = nodes[&leader].bump_feature_level(2).await.unwrap();
        wait_for_epoch(&nodes, bumped.metadata_epoch, Duration::from_secs(5)).await;
        for node in nodes.values() {
            assert_eq!(node.status().await.unwrap().active_feature_level, 2);
        }
        nodes[&leader]
            .metadata_snapshot()
            .await
            .unwrap()
            .state
            .require_feature(crate::FeatureGate::new(2).unwrap())
            .unwrap();

        // A JOINING broker may advertise an older maximum, but it cannot become
        // serving authority after the committed cluster level moves beyond it.
        assert!(
            mutate_rejected_on_current_leader(
                &nodes,
                BrokerLifecycleChange {
                    broker_id: 31,
                    expected_incarnation: 1,
                    expected_lifecycle: BrokerLifecycle::Joining,
                    new_lifecycle: BrokerLifecycle::Active,
                }
                .into_command(),
            )
            .await
            .is_err()
        );

        assert!(nodes[&leader].bump_feature_level(2).await.is_err());
        assert!(nodes[&leader].bump_feature_level(1).await.is_err());

        let stopped = nodes.remove(&leader).unwrap();
        stopped.shutdown().await;
        let (new_leader, new_term) = wait_for_leader(&nodes, Some(leader), first_term).await;
        assert!(new_term > first_term);
        assert_eq!(
            nodes[&new_leader]
                .status()
                .await
                .unwrap()
                .active_feature_level,
            2
        );
        assert!(nodes[&new_leader].bump_feature_level(3).await.is_err());

        let restarted = start_controller_node(make_config(leader.get(), &ranges))
            .await
            .unwrap();
        nodes.insert(leader, restarted);
        wait_for_epoch(&nodes, bumped.metadata_epoch, Duration::from_secs(5)).await;
        assert_eq!(
            nodes[&leader].status().await.unwrap().active_feature_level,
            2
        );

        for (_, node) in nodes {
            node.shutdown().await;
        }

        // An older controller advertisement cannot rejoin and serve a cluster whose
        // durable active level has already advanced beyond that controller's range.
        let mut incompatible = ranges.clone();
        incompatible.insert(cid(1), FeatureRange::new(1, 1).unwrap());
        assert!(
            start_controller_node(make_config(1, &incompatible))
                .await
                .is_err()
        );
        fs::remove_dir_all(root).unwrap();
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn p8_topic_performance_policy_commits_fails_over_and_recovers_on_rf3() {
        let root = temp_dir("p8-performance-rf3");
        let addresses = [reserve_addr(), reserve_addr(), reserve_addr()];
        let ranges = (1..=3_u64)
            .map(|id| (cid(id), FeatureRange::new(1, 4).unwrap()))
            .collect::<BTreeMap<_, _>>();
        let make_config = |id: u64| {
            let mut peers = BTreeMap::new();
            for peer in 1..=3_u64 {
                if peer != id {
                    peers.insert(
                        cid(peer),
                        ControllerPeerDescriptor {
                            controller_id: cid(peer),
                            address: addresses[usize::try_from(peer - 1).unwrap()],
                        },
                    );
                }
            }
            let timeout = 4 + id;
            let mut runtime = runtime_config(&root.join(format!("node-{id}")), id, timeout);
            runtime.controller_feature_ranges = ranges.clone();
            ControllerNodeConfig {
                runtime,
                transport: ControllerPeerTransportConfig {
                    local_controller_id: cid(id),
                    cluster_id: [5; 16],
                    bind_addr: addresses[usize::try_from(id - 1).unwrap()],
                    peers,
                    limits: crate::ControllerTransportLimits::default(),
                    max_inbound_frames: 1024,
                    max_outbound_frames_per_peer: 1024,
                    max_outbound_bytes_per_peer: 16 * 1024 * 1024,
                    reconnect_min_delay: Duration::from_millis(5),
                    reconnect_max_delay: Duration::from_millis(50),
                },
                tick_period: Duration::from_millis(20),
                election_timeout_sample: timeout,
                max_control_requests: 128,
            }
        };

        let mut nodes = BTreeMap::new();
        for id in 1..=3_u64 {
            nodes.insert(
                cid(id),
                start_controller_node(make_config(id)).await.unwrap(),
            );
        }
        let (_initial_leader, first_term) = wait_for_leader(&nodes, None, Term::ZERO).await;

        let (_, registered) = mutate_on_current_leader(
            &nodes,
            BrokerRegistration {
                broker_id: 1,
                node_uuid: [1; 16],
                client_host: "broker-1".into(),
                client_port: 9092,
                internal_host: "broker-1-internal".into(),
                internal_port: 9093,
                rack: Some("rack-1".into()),
                zone: Some("zone-1".into()),
                min_feature: 1,
                max_feature: 4,
            }
            .into_command(),
        )
        .await;
        let (_, activated) = mutate_on_current_leader(
            &nodes,
            BrokerLifecycleChange {
                broker_id: 1,
                expected_incarnation: 1,
                expected_lifecycle: BrokerLifecycle::Joining,
                new_lifecycle: BrokerLifecycle::Active,
            }
            .into_command(),
        )
        .await;
        wait_for_epoch(&nodes, activated.metadata_epoch, Duration::from_secs(5)).await;

        let (leader, _) = wait_for_leader(&nodes, None, Term::ZERO).await;
        nodes[&leader].failure_detector_tick(10).await.unwrap();
        nodes[&leader]
            .heartbeat(BrokerHeartbeatRequest {
                authority: BrokerAuthority {
                    broker_id: 1,
                    node_uuid: [1; 16],
                    incarnation_epoch: 1,
                },
                applied_metadata_epoch: registered.metadata_epoch,
                min_feature: 1,
                max_feature: 4,
                replica_progress: Vec::new(),
                movement_progress: Vec::new(),
            })
            .await
            .unwrap();

        let bumped = nodes[&leader].bump_feature_level(4).await.unwrap();
        wait_for_epoch(&nodes, bumped.metadata_epoch, Duration::from_secs(5)).await;
        for node in nodes.values() {
            assert_eq!(node.status().await.unwrap().active_feature_level, 4);
        }

        let performance = TopicPerformanceSpecV1 {
            compression: crate::CompressionPolicyV1::Lz4,
            max_record_bytes: 1024 * 1024,
            max_batch_records: 512,
            max_batch_uncompressed_bytes: 4 * 1024 * 1024,
        };
        let created = nodes[&leader]
            .create_topic_with_performance(
                CreateTopicSpec {
                    topic_name: "p8-rf3".into(),
                    partition_count: 1,
                    replication_factor: 1,
                    failure_domain_policy: FailureDomainPolicy::Disabled,
                },
                performance,
            )
            .await
            .unwrap();
        wait_for_epoch(
            &nodes,
            created.outcome.metadata_epoch,
            Duration::from_secs(5),
        )
        .await;
        let expected = performance.into_policy(created.topic.topic_id).unwrap();
        for node in nodes.values() {
            assert_eq!(
                node.topic_performance_policy(created.topic.topic_id)
                    .await
                    .unwrap(),
                Some(expected)
            );
        }

        let stopped = nodes.remove(&leader).unwrap();
        stopped.shutdown().await;
        let (successor, successor_term) = wait_for_leader(&nodes, Some(leader), first_term).await;
        assert!(successor_term > first_term);
        let successor_state = nodes[&successor].metadata_snapshot().await.unwrap().state;
        assert_eq!(successor_state.active_feature_level(), 4);
        assert_eq!(
            successor_state.topic_performance_policy(created.topic.topic_id),
            Some(&expected)
        );

        let restarted = start_controller_node(make_config(leader.get()))
            .await
            .unwrap();
        nodes.insert(leader, restarted);
        wait_for_epoch(
            &nodes,
            created.outcome.metadata_epoch,
            Duration::from_secs(5),
        )
        .await;
        assert_eq!(
            nodes[&leader].status().await.unwrap().active_feature_level,
            4
        );
        assert_eq!(
            nodes[&leader]
                .topic_performance_policy(created.topic.topic_id)
                .await
                .unwrap(),
            Some(expected)
        );

        for (_, node) in nodes {
            node.shutdown().await;
        }
        fs::remove_dir_all(root).unwrap();
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn p9_retention_policy_commits_alters_fails_over_and_recovers_on_rf3() {
        let root = temp_dir("p9-retention-rf3");
        let addresses = [reserve_addr(), reserve_addr(), reserve_addr()];
        let ranges = (1..=3_u64)
            .map(|id| (cid(id), FeatureRange::new(1, 5).unwrap()))
            .collect::<BTreeMap<_, _>>();
        let make_config = |id: u64| {
            let mut peers = BTreeMap::new();
            for peer in 1..=3_u64 {
                if peer != id {
                    peers.insert(
                        cid(peer),
                        ControllerPeerDescriptor {
                            controller_id: cid(peer),
                            address: addresses[usize::try_from(peer - 1).unwrap()],
                        },
                    );
                }
            }
            let timeout = 4 + id;
            let mut runtime = runtime_config(&root.join(format!("node-{id}")), id, timeout);
            runtime.controller_feature_ranges = ranges.clone();
            ControllerNodeConfig {
                runtime,
                transport: ControllerPeerTransportConfig {
                    local_controller_id: cid(id),
                    cluster_id: [5; 16],
                    bind_addr: addresses[usize::try_from(id - 1).unwrap()],
                    peers,
                    limits: crate::ControllerTransportLimits::default(),
                    max_inbound_frames: 1024,
                    max_outbound_frames_per_peer: 1024,
                    max_outbound_bytes_per_peer: 16 * 1024 * 1024,
                    reconnect_min_delay: Duration::from_millis(5),
                    reconnect_max_delay: Duration::from_millis(50),
                },
                tick_period: Duration::from_millis(20),
                election_timeout_sample: timeout,
                max_control_requests: 128,
            }
        };

        let mut nodes = BTreeMap::new();
        for id in 1..=3_u64 {
            nodes.insert(
                cid(id),
                start_controller_node(make_config(id)).await.unwrap(),
            );
        }
        let (_initial_leader, first_term) = wait_for_leader(&nodes, None, Term::ZERO).await;

        let (_, registered) = mutate_on_current_leader(
            &nodes,
            BrokerRegistration {
                broker_id: 1,
                node_uuid: [1; 16],
                client_host: "broker-1".into(),
                client_port: 9092,
                internal_host: "broker-1-internal".into(),
                internal_port: 9093,
                rack: Some("rack-1".into()),
                zone: Some("zone-1".into()),
                min_feature: 1,
                max_feature: 5,
            }
            .into_command(),
        )
        .await;
        let (_, activated) = mutate_on_current_leader(
            &nodes,
            BrokerLifecycleChange {
                broker_id: 1,
                expected_incarnation: 1,
                expected_lifecycle: BrokerLifecycle::Joining,
                new_lifecycle: BrokerLifecycle::Active,
            }
            .into_command(),
        )
        .await;
        wait_for_epoch(&nodes, activated.metadata_epoch, Duration::from_secs(5)).await;

        let (leader, _) = wait_for_leader(&nodes, None, Term::ZERO).await;
        nodes[&leader].failure_detector_tick(10).await.unwrap();
        nodes[&leader]
            .heartbeat(BrokerHeartbeatRequest {
                authority: BrokerAuthority {
                    broker_id: 1,
                    node_uuid: [1; 16],
                    incarnation_epoch: 1,
                },
                applied_metadata_epoch: registered.metadata_epoch,
                min_feature: 1,
                max_feature: 5,
                replica_progress: Vec::new(),
                movement_progress: Vec::new(),
            })
            .await
            .unwrap();

        let bumped = nodes[&leader].bump_feature_level(5).await.unwrap();
        wait_for_epoch(&nodes, bumped.metadata_epoch, Duration::from_secs(5)).await;
        for node in nodes.values() {
            assert_eq!(node.status().await.unwrap().active_feature_level, 5);
        }

        let performance = TopicPerformanceSpecV1 {
            compression: crate::CompressionPolicyV1::Zstd,
            max_record_bytes: 1024 * 1024,
            max_batch_records: 256,
            max_batch_uncompressed_bytes: 2 * 1024 * 1024,
        };
        let retention = TopicRetentionSpecV1 {
            cleanup_policy: crate::CleanupPolicyV1::CompactDelete,
            retention_ms: Some(60_000),
            retention_bytes: Some(1_000_000),
            tombstone_grace_ms: 30_000,
            subscription_protection: crate::SubscriptionProtectionV1::Disabled,
        };
        let created = nodes[&leader]
            .create_topic_with_retention(
                CreateTopicSpec {
                    topic_name: "p9-rf3".into(),
                    partition_count: 1,
                    replication_factor: 1,
                    failure_domain_policy: FailureDomainPolicy::Disabled,
                },
                performance,
                retention,
            )
            .await
            .unwrap();
        wait_for_epoch(
            &nodes,
            created.outcome.metadata_epoch,
            Duration::from_secs(5),
        )
        .await;
        let expected = retention.into_policy(created.topic.topic_id).unwrap();
        for node in nodes.values() {
            assert_eq!(
                node.topic_retention_policy(created.topic.topic_id)
                    .await
                    .unwrap(),
                Some(expected)
            );
        }

        let altered_spec = TopicRetentionSpecV1 {
            cleanup_policy: crate::CleanupPolicyV1::Delete,
            retention_ms: None,
            retention_bytes: Some(4096),
            tombstone_grace_ms: 45_000,
            subscription_protection: crate::SubscriptionProtectionV1::Disabled,
        };
        let altered = nodes[&leader]
            .alter_topic_retention(created.topic.topic_id, altered_spec)
            .await
            .unwrap();
        wait_for_epoch(&nodes, altered.metadata_epoch, Duration::from_secs(5)).await;
        let expected_altered = altered_spec.into_policy(created.topic.topic_id).unwrap();

        let stopped = nodes.remove(&leader).unwrap();
        stopped.shutdown().await;
        let (successor, successor_term) = wait_for_leader(&nodes, Some(leader), first_term).await;
        assert!(successor_term > first_term);
        let successor_state = nodes[&successor].metadata_snapshot().await.unwrap().state;
        assert_eq!(successor_state.active_feature_level(), 5);
        assert_eq!(
            successor_state.topic_retention_policy(created.topic.topic_id),
            Some(&expected_altered)
        );

        let restarted = start_controller_node(make_config(leader.get()))
            .await
            .unwrap();
        nodes.insert(leader, restarted);
        wait_for_epoch(&nodes, altered.metadata_epoch, Duration::from_secs(5)).await;
        assert_eq!(
            nodes[&leader].status().await.unwrap().active_feature_level,
            5
        );
        assert_eq!(
            nodes[&leader]
                .topic_retention_policy(created.topic.topic_id)
                .await
                .unwrap(),
            Some(expected_altered)
        );

        for (_, node) in nodes {
            node.shutdown().await;
        }
        fs::remove_dir_all(root).unwrap();
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn heartbeat_liveness_is_volatile_fenced_and_reset_across_controller_failover() {
        let root = temp_dir("heartbeat");
        let addresses = [reserve_addr(), reserve_addr(), reserve_addr()];
        let make_config = |id: u64| {
            let mut peers = BTreeMap::new();
            for peer in 1..=3_u64 {
                if peer != id {
                    peers.insert(
                        cid(peer),
                        ControllerPeerDescriptor {
                            controller_id: cid(peer),
                            address: addresses[usize::try_from(peer - 1).unwrap()],
                        },
                    );
                }
            }
            let timeout = 4 + id;
            ControllerNodeConfig {
                runtime: runtime_config(&root.join(format!("node-{id}")), id, timeout),
                transport: ControllerPeerTransportConfig {
                    local_controller_id: cid(id),
                    cluster_id: [5; 16],
                    bind_addr: addresses[usize::try_from(id - 1).unwrap()],
                    peers,
                    limits: crate::ControllerTransportLimits::default(),
                    max_inbound_frames: 1024,
                    max_outbound_frames_per_peer: 1024,
                    max_outbound_bytes_per_peer: 16 * 1024 * 1024,
                    reconnect_min_delay: Duration::from_millis(5),
                    reconnect_max_delay: Duration::from_millis(50),
                },
                tick_period: Duration::from_millis(50),
                election_timeout_sample: timeout,
                max_control_requests: 128,
            }
        };

        let mut nodes = BTreeMap::new();
        for id in 1..=3_u64 {
            nodes.insert(
                cid(id),
                start_controller_node(make_config(id)).await.unwrap(),
            );
        }

        let registration = BrokerRegistration {
            broker_id: 20,
            node_uuid: [20; 16],
            client_host: "broker-20".into(),
            client_port: 9092,
            internal_host: "broker-20-internal".into(),
            internal_port: 9093,
            rack: Some("rack-h".into()),
            zone: Some("zone-h".into()),
            min_feature: 1,
            max_feature: 3,
        };
        let (_, registered) =
            mutate_on_current_leader(&nodes, registration.clone().into_command()).await;
        let (leader, activated) = mutate_on_current_leader(
            &nodes,
            BrokerLifecycleChange {
                broker_id: 20,
                expected_incarnation: 1,
                expected_lifecycle: BrokerLifecycle::Joining,
                new_lifecycle: BrokerLifecycle::Active,
            }
            .into_command(),
        )
        .await;
        assert!(activated.metadata_epoch > registered.metadata_epoch);
        wait_for_epoch(&nodes, activated.metadata_epoch, Duration::from_secs(5)).await;

        let authority = BrokerAuthority {
            broker_id: 20,
            node_uuid: [20; 16],
            incarnation_epoch: 1,
        };
        let heartbeat = BrokerHeartbeatRequest {
            authority,
            applied_metadata_epoch: activated.metadata_epoch,
            min_feature: 1,
            max_feature: 3,
            replica_progress: Vec::new(),
            movement_progress: Vec::new(),
        };
        let follower = nodes.keys().copied().find(|node| *node != leader).unwrap();
        assert!(matches!(
            nodes[&follower].heartbeat(heartbeat.clone()).await,
            Err(ControllerError::NotLeader { .. })
        ));

        nodes[&leader].failure_detector_tick(100).await.unwrap();
        let response = nodes[&leader].heartbeat(heartbeat.clone()).await.unwrap();
        assert_eq!(response.observed_at_tick, 100);
        assert_eq!(response.controller_metadata_epoch, activated.metadata_epoch);
        assert!(matches!(
            response.metadata_sync,
            crate::MetadataSyncResponse::UpToDate { controller_epoch }
                if controller_epoch == activated.metadata_epoch
        ));

        assert_eq!(
            activated.metadata_epoch,
            registered.metadata_epoch + 1,
            "same-leader broker lifecycle mutation should be a contiguous metadata delta"
        );
        let lagging = BrokerHeartbeatRequest {
            applied_metadata_epoch: registered.metadata_epoch,
            ..heartbeat.clone()
        };
        let lagging_response = nodes[&leader].heartbeat(lagging).await.unwrap();
        let crate::MetadataSyncResponse::Deltas {
            controller_epoch,
            deltas,
        } = lagging_response.metadata_sync
        else {
            panic!("expected bounded metadata delta response");
        };
        assert_eq!(controller_epoch, activated.metadata_epoch);
        assert_eq!(deltas.len(), 1);
        assert_eq!(deltas[0].epoch, activated.metadata_epoch);

        let bootstrap_lag = BrokerHeartbeatRequest {
            applied_metadata_epoch: 0,
            ..heartbeat.clone()
        };
        let bootstrap_response = nodes[&leader].heartbeat(bootstrap_lag).await.unwrap();
        assert!(matches!(
            bootstrap_response.metadata_sync,
            crate::MetadataSyncResponse::Snapshot {
                controller_epoch,
                ref snapshot,
            } if controller_epoch == activated.metadata_epoch
                && snapshot.epoch == activated.metadata_epoch
        ));
        let healthy = nodes[&leader].broker_liveness(20).await.unwrap();
        assert_eq!(healthy.state, crate::BrokerLivenessState::Healthy);
        assert!(healthy.available_for_new_placement());

        let stale = BrokerHeartbeatRequest {
            authority: BrokerAuthority {
                incarnation_epoch: 2,
                ..authority
            },
            ..heartbeat.clone()
        };
        assert!(nodes[&leader].heartbeat(stale).await.is_err());
        assert_eq!(
            nodes[&leader]
                .broker_liveness(20)
                .await
                .unwrap()
                .last_seen_tick,
            Some(100)
        );

        nodes[&leader].failure_detector_tick(103).await.unwrap();
        assert_eq!(
            nodes[&leader].broker_liveness(20).await.unwrap().state,
            crate::BrokerLivenessState::Suspect
        );
        nodes[&leader].failure_detector_tick(110).await.unwrap();
        let unavailable = nodes[&leader].broker_liveness(20).await.unwrap();
        assert_eq!(unavailable.state, crate::BrokerLivenessState::Unavailable);
        assert!(!unavailable.available_for_new_placement());
        let planner = nodes[&leader].planner_liveness_snapshot().await.unwrap();
        assert_eq!(planner.len(), 1);
        assert!(!planner[0].available_for_new_placement());

        // Suspicion is advisory only. It must not revoke committed membership
        // authority or create a metadata-log mutation.
        assert!(
            nodes[&leader]
                .validate_broker_authority(authority)
                .await
                .is_ok()
        );
        assert_eq!(
            nodes[&leader].status().await.unwrap().metadata_epoch,
            activated.metadata_epoch
        );
        assert_eq!(
            nodes[&leader]
                .broker_record(20)
                .await
                .unwrap()
                .unwrap()
                .lifecycle,
            BrokerLifecycle::Active
        );

        nodes[&leader].heartbeat(heartbeat.clone()).await.unwrap();
        assert_eq!(
            nodes[&leader].broker_liveness(20).await.unwrap().state,
            crate::BrokerLivenessState::Healthy
        );

        let old_term = nodes[&leader].status().await.unwrap().term;
        let stopped = nodes.remove(&leader).unwrap();
        stopped.shutdown().await;
        let (new_leader, new_term) = wait_for_leader(&nodes, Some(leader), old_term).await;
        assert!(new_term > old_term);

        // The replacement leader inherits committed BrokerRecord authority but
        // never inherits another controller's volatile clock/last_seen state.
        let reset = nodes[&new_leader].broker_liveness(20).await.unwrap();
        assert_eq!(reset.state, crate::BrokerLivenessState::Unknown);
        assert_eq!(reset.last_seen_tick, None);
        nodes[&new_leader].failure_detector_tick(1).await.unwrap();
        assert_eq!(
            nodes[&new_leader].broker_liveness(20).await.unwrap().state,
            crate::BrokerLivenessState::Unknown
        );
        let response = nodes[&new_leader].heartbeat(heartbeat).await.unwrap();
        assert_eq!(response.observed_at_tick, 1);
        assert_eq!(
            nodes[&new_leader].broker_liveness(20).await.unwrap().state,
            crate::BrokerLivenessState::Healthy
        );
        assert_eq!(
            nodes[&new_leader].status().await.unwrap().metadata_epoch,
            activated.metadata_epoch
        );

        let restarted = start_controller_node(make_config(leader.get()))
            .await
            .unwrap();
        nodes.insert(leader, restarted);
        wait_for_epoch(&nodes, activated.metadata_epoch, Duration::from_secs(5)).await;
        assert_eq!(
            nodes[&leader]
                .broker_record(20)
                .await
                .unwrap()
                .unwrap()
                .lifecycle,
            BrokerLifecycle::Active
        );

        for (_, node) in nodes {
            node.shutdown().await;
        }
        fs::remove_dir_all(root).unwrap();
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn topic_metadata_and_deterministic_placement_commit_expand_delete_and_survive_failover()
    {
        let root = temp_dir("topic-placement");
        let addresses = [reserve_addr(), reserve_addr(), reserve_addr()];
        let make_config = |id: u64| {
            let mut peers = BTreeMap::new();
            for peer in 1..=3_u64 {
                if peer != id {
                    peers.insert(
                        cid(peer),
                        ControllerPeerDescriptor {
                            controller_id: cid(peer),
                            address: addresses[usize::try_from(peer - 1).unwrap()],
                        },
                    );
                }
            }
            let timeout = 4 + id;
            ControllerNodeConfig {
                runtime: runtime_config(&root.join(format!("node-{id}")), id, timeout),
                transport: ControllerPeerTransportConfig {
                    local_controller_id: cid(id),
                    cluster_id: [5; 16],
                    bind_addr: addresses[usize::try_from(id - 1).unwrap()],
                    peers,
                    limits: crate::ControllerTransportLimits::default(),
                    max_inbound_frames: 1024,
                    max_outbound_frames_per_peer: 1024,
                    max_outbound_bytes_per_peer: 16 * 1024 * 1024,
                    reconnect_min_delay: Duration::from_millis(5),
                    reconnect_max_delay: Duration::from_millis(50),
                },
                tick_period: Duration::from_millis(50),
                election_timeout_sample: timeout,
                max_control_requests: 128,
            }
        };

        let mut nodes = BTreeMap::new();
        for id in 1..=3_u64 {
            nodes.insert(
                cid(id),
                start_controller_node(make_config(id)).await.unwrap(),
            );
        }

        let mut final_membership_epoch = 0;
        for broker_id in 1..=5_u32 {
            let registration = BrokerRegistration {
                broker_id,
                node_uuid: [u8::try_from(broker_id).unwrap(); 16],
                client_host: format!("broker-{broker_id}"),
                client_port: 9000 + u16::try_from(broker_id).unwrap(),
                internal_host: format!("broker-{broker_id}-internal"),
                internal_port: 10_000 + u16::try_from(broker_id).unwrap(),
                rack: Some(format!("rack-{broker_id}")),
                zone: Some(format!("zone-{broker_id}")),
                min_feature: 1,
                max_feature: 3,
            };
            let (_, registered) =
                mutate_on_current_leader(&nodes, registration.into_command()).await;
            let (_, activated) = mutate_on_current_leader(
                &nodes,
                BrokerLifecycleChange {
                    broker_id,
                    expected_incarnation: 1,
                    expected_lifecycle: BrokerLifecycle::Joining,
                    new_lifecycle: BrokerLifecycle::Active,
                }
                .into_command(),
            )
            .await;
            assert!(activated.metadata_epoch > registered.metadata_epoch);
            final_membership_epoch = activated.metadata_epoch;
        }
        wait_for_epoch(&nodes, final_membership_epoch, Duration::from_secs(5)).await;

        let (leader, _) = wait_for_leader(&nodes, None, Term::ZERO).await;
        nodes[&leader].failure_detector_tick(100).await.unwrap();
        // Broker 5 deliberately remains Unknown. Placement must consume only
        // current-incarnation Healthy observations from P5-G.
        for broker_id in 1..=4_u32 {
            nodes[&leader]
                .heartbeat(BrokerHeartbeatRequest {
                    authority: BrokerAuthority {
                        broker_id,
                        node_uuid: [u8::try_from(broker_id).unwrap(); 16],
                        incarnation_epoch: 1,
                    },
                    applied_metadata_epoch: final_membership_epoch,
                    min_feature: 1,
                    max_feature: 3,
                    replica_progress: Vec::new(),
                    movement_progress: Vec::new(),
                })
                .await
                .unwrap();
        }

        let follower = nodes.keys().copied().find(|node| *node != leader).unwrap();
        let spec = CreateTopicSpec {
            topic_name: "orders".into(),
            partition_count: 6,
            replication_factor: 3,
            failure_domain_policy: FailureDomainPolicy::Required,
        };
        assert!(matches!(
            nodes[&follower].create_topic(spec.clone()).await,
            Err(ControllerError::NotLeader { .. })
        ));

        let created = nodes[&leader].create_topic(spec.clone()).await.unwrap();
        assert_eq!(created.topic.topic_name, "orders");
        assert_eq!(created.topic.partition_count, 6);
        assert_eq!(created.assignments.len(), 6);
        assert!(!created.degraded_failure_domain);
        assert!(created.assignments.iter().all(|assignment| {
            assignment.voters.len() == 3
                && assignment
                    .voters
                    .windows(2)
                    .all(|window| window[0] < window[1])
                && assignment.voters.iter().all(|broker| *broker <= 4)
        }));
        wait_for_epoch(
            &nodes,
            created.outcome.metadata_epoch,
            Duration::from_secs(5),
        )
        .await;

        // Name uniqueness is controller authority, not broker-local catalog state.
        assert!(nodes[&leader].create_topic(spec.clone()).await.is_err());

        // Required RF=5 cannot silently consume Unknown broker 5 or weaken RF.
        assert!(
            nodes[&leader]
                .create_topic(CreateTopicSpec {
                    topic_name: "requires-five".into(),
                    partition_count: 1,
                    replication_factor: 5,
                    failure_domain_policy: FailureDomainPolicy::Required,
                })
                .await
                .is_err()
        );

        let expanded = nodes[&leader]
            .expand_topic(TopicExpansionSpec {
                topic_id: created.topic.topic_id,
                expected_partition_count: 6,
                new_partition_count: 8,
            })
            .await
            .unwrap();
        assert_eq!(
            expanded
                .assignments
                .iter()
                .map(|assignment| assignment.partition_id)
                .collect::<Vec<_>>(),
            vec![6, 7]
        );
        assert_eq!(
            expanded.partitioning_epoch_change.previous_partition_count,
            6
        );
        assert_eq!(expanded.partitioning_epoch_change.new_partition_count, 8);
        assert!(
            expanded
                .partitioning_epoch_change
                .keyed_partition_choice_may_change
        );
        wait_for_epoch(
            &nodes,
            expanded.outcome.metadata_epoch,
            Duration::from_secs(5),
        )
        .await;
        assert_eq!(
            nodes[&leader]
                .topic_record(created.topic.topic_id)
                .await
                .unwrap()
                .unwrap()
                .partition_count,
            8
        );

        let deleting = nodes[&leader]
            .begin_delete_topic(created.topic.topic_id)
            .await
            .unwrap();
        assert_eq!(deleting.lifecycle, Some(TopicLifecycle::Deleting));
        assert!(
            nodes[&leader]
                .expand_topic(TopicExpansionSpec {
                    topic_id: created.topic.topic_id,
                    expected_partition_count: 8,
                    new_partition_count: 9,
                })
                .await
                .is_err()
        );
        let deleted = nodes[&leader]
            .finalize_delete_topic(created.topic.topic_id)
            .await
            .unwrap();
        assert_eq!(deleted.lifecycle, None);
        assert!(
            nodes[&leader]
                .topic_record(created.topic.topic_id)
                .await
                .unwrap()
                .is_none()
        );

        let recreated = nodes[&leader].create_topic(spec).await.unwrap();
        assert_ne!(recreated.topic.topic_id, created.topic.topic_id);
        wait_for_epoch(
            &nodes,
            recreated.outcome.metadata_epoch,
            Duration::from_secs(5),
        )
        .await;

        let old_leader = leader;
        let old_term = nodes[&old_leader].status().await.unwrap().term;
        let stopped = nodes.remove(&old_leader).unwrap();
        stopped.shutdown().await;
        let (new_leader, new_term) = wait_for_leader(&nodes, Some(old_leader), old_term).await;
        assert!(new_term > old_term);
        assert!(
            nodes[&new_leader]
                .topic_record(created.topic.topic_id)
                .await
                .unwrap()
                .is_none()
        );
        let durable = nodes[&new_leader]
            .topic_record(recreated.topic.topic_id)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(durable.topic_name, "orders");
        assert_eq!(durable.partition_count, 6);

        let restarted = start_controller_node(make_config(old_leader.get()))
            .await
            .unwrap();
        nodes.insert(old_leader, restarted);
        wait_for_epoch(
            &nodes,
            recreated.outcome.metadata_epoch,
            Duration::from_secs(5),
        )
        .await;
        assert_eq!(
            nodes[&old_leader]
                .topic_record(recreated.topic.topic_id)
                .await
                .unwrap()
                .unwrap(),
            durable
        );

        for (_, node) in nodes {
            node.shutdown().await;
        }
        fs::remove_dir_all(root).unwrap();
    }

    async fn wait_for_leader(
        nodes: &BTreeMap<ControllerNodeId, ControllerNodeHandle>,
        excluded: Option<ControllerNodeId>,
        minimum_term: Term,
    ) -> (ControllerNodeId, Term) {
        let deadline = time::Instant::now() + Duration::from_secs(5);
        let mut last_observation = None;
        let mut consecutive = 0_u8;
        loop {
            let mut observed = None;
            for (id, node) in nodes {
                if Some(*id) == excluded {
                    continue;
                }
                if let Ok(status) = node.status().await
                    && status.role == Role::Leader
                    && status.term > minimum_term
                {
                    observed = Some((*id, status.term));
                    break;
                }
            }
            match observed {
                Some(value) if last_observation == Some(value) => {
                    consecutive = consecutive.saturating_add(1);
                    if consecutive >= 5 {
                        return value;
                    }
                }
                Some(value) => {
                    last_observation = Some(value);
                    consecutive = 1;
                }
                None => {
                    last_observation = None;
                    consecutive = 0;
                }
            }
            assert!(
                time::Instant::now() < deadline,
                "P5-D leader election timed out"
            );
            time::sleep(Duration::from_millis(20)).await;
        }
    }

    async fn wait_for_epoch(
        nodes: &BTreeMap<ControllerNodeId, ControllerNodeHandle>,
        minimum_epoch: u64,
        timeout: Duration,
    ) {
        let deadline = time::Instant::now() + timeout;
        loop {
            let mut all = true;
            for node in nodes.values() {
                match node.status().await {
                    Ok(status) if status.metadata_epoch >= minimum_epoch => {}
                    _ => all = false,
                }
            }
            if all {
                return;
            }
            assert!(
                time::Instant::now() < deadline,
                "P5-D metadata convergence timed out"
            );
            time::sleep(Duration::from_millis(20)).await;
        }
    }

    async fn wait_for_leader_epoch(
        nodes: &BTreeMap<ControllerNodeId, ControllerNodeHandle>,
        minimum_epoch: u64,
        timeout: Duration,
    ) -> ControllerNodeId {
        let deadline = time::Instant::now() + timeout;
        loop {
            for (id, node) in nodes {
                if let Ok(status) = node.status().await
                    && status.role == Role::Leader
                    && status.metadata_epoch >= minimum_epoch
                {
                    return *id;
                }
            }
            assert!(
                time::Instant::now() < deadline,
                "P5-D committed metadata was not observed on a controller leader"
            );
            time::sleep(Duration::from_millis(20)).await;
        }
    }

    async fn mutate_on_current_leader(
        nodes: &BTreeMap<ControllerNodeId, ControllerNodeHandle>,
        command: ControllerCommand,
    ) -> (ControllerNodeId, ApplyOutcome) {
        let deadline = time::Instant::now() + Duration::from_secs(5);
        loop {
            let (leader, _) = wait_for_leader(nodes, None, Term::ZERO).await;
            match nodes[&leader].mutate(command.clone()).await {
                Ok(outcome) => return (leader, outcome),
                Err(ControllerError::NotLeader { .. } | ControllerError::Busy(_)) => {}
                Err(error) => {
                    if let Some(resolved) = reconcile_committed_membership(nodes, &command).await {
                        return resolved;
                    }
                    panic!("unexpected controller mutation failure: {error}");
                }
            }
            assert!(
                time::Instant::now() < deadline,
                "P5-D mutation did not obtain a committed response"
            );
            time::sleep(Duration::from_millis(20)).await;
        }
    }

    async fn reconcile_committed_membership(
        nodes: &BTreeMap<ControllerNodeId, ControllerNodeHandle>,
        command: &ControllerCommand,
    ) -> Option<(ControllerNodeId, ApplyOutcome)> {
        let broker_id = match command {
            ControllerCommand::RegisterBroker { broker_id, .. }
            | ControllerCommand::RejoinBroker { broker_id, .. }
            | ControllerCommand::SetBrokerLifecycle { broker_id, .. } => *broker_id,
            _ => return None,
        };
        let (leader, _) = wait_for_leader(nodes, None, Term::ZERO).await;
        let broker = nodes[&leader].broker_record(broker_id).await.ok()??;
        let matches = match command {
            ControllerCommand::RegisterBroker {
                node_uuid,
                initial_incarnation,
                client_host,
                client_port,
                internal_host,
                internal_port,
                rack,
                zone,
                min_feature,
                max_feature,
                ..
            } => {
                broker.node_uuid == *node_uuid
                    && broker.incarnation_epoch == *initial_incarnation
                    && broker.lifecycle == BrokerLifecycle::Joining
                    && broker.client_host == *client_host
                    && broker.client_port == *client_port
                    && broker.internal_host == *internal_host
                    && broker.internal_port == *internal_port
                    && broker.rack == *rack
                    && broker.zone == *zone
                    && broker.min_feature == *min_feature
                    && broker.max_feature == *max_feature
            }
            ControllerCommand::RejoinBroker {
                node_uuid,
                new_incarnation,
                client_host,
                client_port,
                internal_host,
                internal_port,
                rack,
                zone,
                min_feature,
                max_feature,
                ..
            } => {
                broker.node_uuid == *node_uuid
                    && broker.incarnation_epoch == *new_incarnation
                    && broker.client_host == *client_host
                    && broker.client_port == *client_port
                    && broker.internal_host == *internal_host
                    && broker.internal_port == *internal_port
                    && broker.rack == *rack
                    && broker.zone == *zone
                    && broker.min_feature == *min_feature
                    && broker.max_feature == *max_feature
            }
            ControllerCommand::SetBrokerLifecycle {
                expected_incarnation,
                new_lifecycle,
                ..
            } => {
                broker.incarnation_epoch == *expected_incarnation
                    && broker.lifecycle == *new_lifecycle
            }
            _ => false,
        };
        if !matches {
            return None;
        }
        let status = nodes[&leader].status().await.ok()?;
        Some((
            leader,
            ApplyOutcome {
                metadata_epoch: status.metadata_epoch,
                producer_ids: None,
            },
        ))
    }

    async fn mutate_rejected_on_current_leader(
        nodes: &BTreeMap<ControllerNodeId, ControllerNodeHandle>,
        command: ControllerCommand,
    ) -> Result<ApplyOutcome> {
        let deadline = time::Instant::now() + Duration::from_secs(5);
        loop {
            let (leader, _) = wait_for_leader(nodes, None, Term::ZERO).await;
            match nodes[&leader].mutate(command.clone()).await {
                Err(ControllerError::NotLeader { .. } | ControllerError::Busy(_)) => {}
                other => return other,
            }
            assert!(
                time::Instant::now() < deadline,
                "P5-F rejected mutation did not reach a stable controller leader"
            );
            time::sleep(Duration::from_millis(20)).await;
        }
    }

    async fn init_producer_on_current_leader(
        nodes: &BTreeMap<ControllerNodeId, ControllerNodeHandle>,
        expected_authority: Option<ProducerAuthority>,
    ) -> ProducerAuthority {
        let deadline = time::Instant::now() + Duration::from_secs(10);
        loop {
            let (leader, _) = wait_for_leader(nodes, None, Term::ZERO).await;
            match nodes[&leader].init_producer(expected_authority).await {
                Ok(authority) => return authority,
                Err(ControllerError::NotLeader { .. } | ControllerError::Busy(_)) => {}
                Err(error) => panic!("unexpected InitProducer failure: {error}"),
            }
            assert!(
                time::Instant::now() < deadline,
                "P7-C InitProducer did not obtain a committed response"
            );
            time::sleep(Duration::from_millis(20)).await;
        }
    }

    async fn init_producer_rejected_on_current_leader(
        nodes: &BTreeMap<ControllerNodeId, ControllerNodeHandle>,
        expected_authority: Option<ProducerAuthority>,
    ) -> Result<ProducerAuthority> {
        let deadline = time::Instant::now() + Duration::from_secs(10);
        loop {
            let (leader, _) = wait_for_leader(nodes, None, Term::ZERO).await;
            match nodes[&leader].init_producer(expected_authority).await {
                Err(ControllerError::NotLeader { .. } | ControllerError::Busy(_)) => {}
                other => return other,
            }
            assert!(
                time::Instant::now() < deadline,
                "P7-C rejected InitProducer did not reach a stable leader"
            );
            time::sleep(Duration::from_millis(20)).await;
        }
    }

    async fn wait_for_producer_authority(
        nodes: &BTreeMap<ControllerNodeId, ControllerNodeHandle>,
        authority: ProducerAuthority,
        timeout: Duration,
    ) {
        let deadline = time::Instant::now() + timeout;
        loop {
            let mut observed = false;
            for node in nodes.values() {
                if node
                    .producer_authority(authority.producer_id)
                    .await
                    .ok()
                    .flatten()
                    == Some(authority)
                {
                    observed = true;
                    break;
                }
            }
            if observed {
                return;
            }
            assert!(
                time::Instant::now() < deadline,
                "P7-C committed producer authority was not observed"
            );
            time::sleep(Duration::from_millis(20)).await;
        }
    }

    async fn validate_authority_on_current_leader(
        nodes: &BTreeMap<ControllerNodeId, ControllerNodeHandle>,
        authority: BrokerAuthority,
    ) -> Result<BrokerRecord> {
        let deadline = time::Instant::now() + Duration::from_secs(5);
        loop {
            let (leader, _) = wait_for_leader(nodes, None, Term::ZERO).await;
            match nodes[&leader].validate_broker_authority(authority).await {
                Err(ControllerError::NotLeader { .. } | ControllerError::Busy(_)) => {}
                other => return other,
            }
            assert!(
                time::Instant::now() < deadline,
                "P5-F broker authority validation did not reach a stable leader"
            );
            time::sleep(Duration::from_millis(20)).await;
        }
    }
}
