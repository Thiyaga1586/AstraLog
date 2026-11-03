use std::collections::BTreeMap;

use astralog_common::{ProducerAuthority, ProducerEpoch, ProducerId, TopicId};

use crate::snapshot::canonical_snapshot_worst_case_bytes;
use crate::snapshot_v2::MAX_PRODUCER_SESSIONS;
use crate::{
    AssignmentRecord, BrokerAuthority, BrokerLifecycle, BrokerRecord, CommandLimits,
    ControllerCommand, ControllerError, ControllerSnapshot, FailureDomainPolicy, FeatureGate,
    MAX_CONTROLLER_ASSIGNMENTS, MAX_CONTROLLER_BROKERS, MAX_CONTROLLER_MOVEMENTS,
    MAX_CONTROLLER_TOPICS, MAX_TOPIC_PARTITIONS, MovementId, MovementRecord, MovementStage,
    ProducerSessionCommandV2, Result, SnapshotLimits, TopicLifecycle, TopicPerformanceCommandV3,
    TopicPerformancePolicyV1, TopicRecord, TopicRetentionCommandV4, TopicRetentionPolicyV1,
    encode_command, encode_controller_snapshot, encode_producer_session_command_v2,
    encode_topic_performance_command_v3, encode_topic_retention_command_v4,
};

pub const P7_PRODUCER_FEATURE_LEVEL: u16 = 3;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ProducerIdRange {
    pub first: u64,
    pub last: u64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ApplyOutcome {
    pub metadata_epoch: u64,
    pub producer_ids: Option<ProducerIdRange>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ControllerState {
    cluster_id: [u8; 16],
    active_feature_level: u16,
    metadata_epoch: u64,
    next_unallocated_producer_id: u64,
    producer_sessions: BTreeMap<ProducerId, ProducerEpoch>,
    topic_performance: BTreeMap<TopicId, TopicPerformancePolicyV1>,
    topic_retention: BTreeMap<TopicId, TopicRetentionPolicyV1>,
    brokers: BTreeMap<u32, BrokerRecord>,
    topics: BTreeMap<TopicId, TopicRecord>,
    topic_names: BTreeMap<String, TopicId>,
    assignments: BTreeMap<(TopicId, u32), AssignmentRecord>,
    movements: BTreeMap<MovementId, MovementRecord>,
}

pub(crate) struct SnapshotStateParts {
    pub cluster_id: [u8; 16],
    pub active_feature_level: u16,
    pub metadata_epoch: u64,
    pub next_unallocated_producer_id: u64,
    pub brokers: BTreeMap<u32, BrokerRecord>,
    pub topics: BTreeMap<TopicId, TopicRecord>,
    pub assignments: BTreeMap<(TopicId, u32), AssignmentRecord>,
    pub movements: BTreeMap<MovementId, MovementRecord>,
}

impl ControllerState {
    pub fn new(
        cluster_id: [u8; 16],
        active_feature_level: u16,
        next_unallocated_producer_id: u64,
    ) -> Result<Self> {
        if cluster_id == [0; 16] {
            return Err(ControllerError::Invalid("cluster id zero is reserved"));
        }
        if active_feature_level == 0 {
            return Err(ControllerError::Invalid(
                "active feature level zero is reserved",
            ));
        }
        if next_unallocated_producer_id == 0 {
            return Err(ControllerError::Invalid(
                "producer id zero is reserved in controller allocator",
            ));
        }
        Ok(Self {
            cluster_id,
            active_feature_level,
            metadata_epoch: 0,
            next_unallocated_producer_id,
            producer_sessions: BTreeMap::new(),
            topic_performance: BTreeMap::new(),
            topic_retention: BTreeMap::new(),
            brokers: BTreeMap::new(),
            topics: BTreeMap::new(),
            topic_names: BTreeMap::new(),
            assignments: BTreeMap::new(),
            movements: BTreeMap::new(),
        })
    }

    pub const fn cluster_id(&self) -> &[u8; 16] {
        &self.cluster_id
    }

    pub const fn active_feature_level(&self) -> u16 {
        self.active_feature_level
    }

    pub fn require_feature(&self, gate: FeatureGate) -> Result<()> {
        gate.require(self.active_feature_level)
    }

    pub const fn metadata_epoch(&self) -> u64 {
        self.metadata_epoch
    }

    pub const fn next_unallocated_producer_id(&self) -> u64 {
        self.next_unallocated_producer_id
    }

    pub fn producer_sessions(&self) -> &BTreeMap<ProducerId, ProducerEpoch> {
        &self.producer_sessions
    }

    pub fn topic_performance_policies(&self) -> &BTreeMap<TopicId, TopicPerformancePolicyV1> {
        &self.topic_performance
    }

    pub fn topic_performance_policy(&self, topic_id: TopicId) -> Option<&TopicPerformancePolicyV1> {
        self.topic_performance.get(&topic_id)
    }

    pub fn topic_retention_policies(&self) -> &BTreeMap<TopicId, TopicRetentionPolicyV1> {
        &self.topic_retention
    }

    pub fn topic_retention_policy(&self, topic_id: TopicId) -> Option<&TopicRetentionPolicyV1> {
        self.topic_retention.get(&topic_id)
    }

    pub fn producer_authority(&self, producer_id: ProducerId) -> Option<ProducerAuthority> {
        self.producer_sessions
            .get(&producer_id)
            .copied()
            .map(|producer_epoch| ProducerAuthority::new(producer_id, producer_epoch))
    }

    pub fn brokers(&self) -> &BTreeMap<u32, BrokerRecord> {
        &self.brokers
    }

    pub fn broker(&self, broker_id: u32) -> Option<&BrokerRecord> {
        self.brokers.get(&broker_id)
    }

    pub fn validate_broker_authority(&self, authority: BrokerAuthority) -> Result<&BrokerRecord> {
        let broker = self
            .brokers
            .get(&authority.broker_id)
            .ok_or(ControllerError::Invalid("unknown broker authority"))?;
        if broker.node_uuid != authority.node_uuid {
            return Err(ControllerError::Invalid(
                "broker authority node UUID does not match committed identity",
            ));
        }
        if broker.incarnation_epoch != authority.incarnation_epoch {
            return Err(ControllerError::Invalid("stale broker incarnation"));
        }
        if matches!(
            broker.lifecycle,
            BrokerLifecycle::Fenced | BrokerLifecycle::Removed
        ) {
            return Err(ControllerError::Invalid(
                "broker lifecycle has no current authority",
            ));
        }
        Ok(broker)
    }

    pub fn topics(&self) -> &BTreeMap<TopicId, TopicRecord> {
        &self.topics
    }

    pub fn assignments(&self) -> &BTreeMap<(TopicId, u32), AssignmentRecord> {
        &self.assignments
    }

    pub fn movements(&self) -> &BTreeMap<MovementId, MovementRecord> {
        &self.movements
    }

    pub fn topic_by_name(&self, name: &str) -> Option<&TopicRecord> {
        self.topic_names
            .get(name)
            .and_then(|topic_id| self.topics.get(topic_id))
    }

    pub(crate) fn from_snapshot_parts(parts: SnapshotStateParts) -> Result<Self> {
        let mut state = Self::new(
            parts.cluster_id,
            parts.active_feature_level,
            parts.next_unallocated_producer_id,
        )?;
        state.metadata_epoch = parts.metadata_epoch;
        state.brokers = parts.brokers;
        state.topics = parts.topics;
        state.topic_performance = state
            .topics
            .keys()
            .copied()
            .map(|topic_id| (topic_id, TopicPerformancePolicyV1::default_for(topic_id)))
            .collect();
        state.topic_retention = state
            .topics
            .keys()
            .copied()
            .map(|topic_id| (topic_id, TopicRetentionPolicyV1::default_for(topic_id)))
            .collect();
        state.assignments = parts.assignments;
        state.movements = parts.movements;
        state.producer_sessions = BTreeMap::new();
        state.validate_snapshot_state()?;
        state.topic_names = state
            .topics
            .values()
            .map(|topic| (topic.topic_name.clone(), topic.topic_id))
            .collect();
        Ok(state)
    }

    pub(crate) fn install_producer_sessions(
        &mut self,
        producer_sessions: BTreeMap<ProducerId, ProducerEpoch>,
    ) -> Result<()> {
        self.producer_sessions = producer_sessions;
        self.validate_snapshot_state()
    }

    pub(crate) fn install_topic_performance_policies(
        &mut self,
        policies: BTreeMap<TopicId, TopicPerformancePolicyV1>,
    ) -> Result<()> {
        self.topic_performance = policies;
        self.validate_snapshot_state()
    }

    pub(crate) fn install_topic_retention_policies(
        &mut self,
        policies: BTreeMap<TopicId, TopicRetentionPolicyV1>,
    ) -> Result<()> {
        self.topic_retention = policies;
        self.validate_snapshot_state()
    }

    fn validate_snapshot_state(&self) -> Result<()> {
        if self.topic_performance.len() != self.topics.len()
            || self.topics.keys().any(|topic_id| {
                self.topic_performance
                    .get(topic_id)
                    .is_none_or(|policy| policy.topic_id != *topic_id || policy.validate().is_err())
            })
            || self
                .topic_performance
                .keys()
                .any(|topic_id| !self.topics.contains_key(topic_id))
        {
            return Err(ControllerError::Invalid(
                "invalid topic performance table in controller snapshot",
            ));
        }
        if self.topic_retention.len() != self.topics.len()
            || self.topics.keys().any(|topic_id| {
                self.topic_retention
                    .get(topic_id)
                    .is_none_or(|policy| policy.topic_id != *topic_id || policy.validate().is_err())
            })
            || self
                .topic_retention
                .keys()
                .any(|topic_id| !self.topics.contains_key(topic_id))
        {
            return Err(ControllerError::Invalid(
                "invalid topic retention table in controller snapshot",
            ));
        }
        if self.producer_sessions.len() > MAX_PRODUCER_SESSIONS
            || self
                .producer_sessions
                .keys()
                .any(|producer_id| producer_id.get() >= self.next_unallocated_producer_id)
        {
            return Err(ControllerError::Invalid(
                "invalid producer session table in controller snapshot",
            ));
        }
        if self.metadata_epoch == 0
            && (!self.brokers.is_empty()
                || !self.topics.is_empty()
                || !self.assignments.is_empty()
                || !self.movements.is_empty())
        {
            return Err(ControllerError::Invalid(
                "non-empty controller snapshot cannot have metadata epoch zero",
            ));
        }
        let mut uuids = std::collections::BTreeSet::new();
        for (broker_id, broker) in &self.brokers {
            if *broker_id == 0
                || *broker_id == u32::MAX
                || broker.broker_id != *broker_id
                || broker.node_uuid == [0; 16]
                || broker.incarnation_epoch == 0
                || broker.client_host.is_empty()
                || broker.internal_host.is_empty()
                || broker.client_port == 0
                || broker.internal_port == 0
                || broker.min_feature == 0
                || broker.min_feature > broker.max_feature
                || (matches!(
                    broker.lifecycle,
                    BrokerLifecycle::Active | BrokerLifecycle::Draining
                ) && (self.active_feature_level < broker.min_feature
                    || self.active_feature_level > broker.max_feature))
                || !uuids.insert(broker.node_uuid)
            {
                return Err(ControllerError::Invalid(
                    "invalid or duplicate broker record in controller snapshot",
                ));
            }
        }

        let mut names = std::collections::BTreeSet::new();
        for (topic_id, topic) in &self.topics {
            if topic.topic_id != *topic_id
                || crate::topic::validate_controller_topic_name_and_topology(
                    &topic.topic_name,
                    topic.partition_count,
                    topic.replication_factor,
                    topic.failure_domain_policy,
                )
                .is_err()
                || topic.partition_count == 0
                || topic.partition_count > MAX_TOPIC_PARTITIONS
                || !matches!(topic.replication_factor, 1 | 3 | 5)
                || !names.insert(topic.topic_name.clone())
            {
                return Err(ControllerError::Invalid(
                    "invalid or duplicate topic record in controller snapshot",
                ));
            }
            for partition_id in 0..topic.partition_count {
                if !self.assignments.contains_key(&(*topic_id, partition_id)) {
                    return Err(ControllerError::Invalid(
                        "controller snapshot topic is missing partition assignment",
                    ));
                }
            }
        }

        for ((topic_id, partition_id), assignment) in &self.assignments {
            let topic = self.topics.get(topic_id).ok_or(ControllerError::Invalid(
                "snapshot assignment references unknown topic",
            ))?;
            if assignment.topic_id != *topic_id
                || assignment.partition_id != *partition_id
                || *partition_id >= topic.partition_count
                || assignment.assignment_generation.get() == 0
                || assignment.voters.len() != usize::from(topic.replication_factor)
                || !is_sorted_unique_nonempty(&assignment.voters)
                || !is_sorted_unique(&assignment.learners)
                || assignment
                    .learners
                    .iter()
                    .any(|broker| assignment.voters.binary_search(broker).is_ok())
                || assignment
                    .voters
                    .iter()
                    .chain(&assignment.learners)
                    .any(|broker| !self.brokers.contains_key(broker))
            {
                return Err(ControllerError::Invalid(
                    "invalid assignment record in controller snapshot",
                ));
            }
        }

        for (movement_id, movement) in &self.movements {
            let topic = self
                .topics
                .get(&movement.topic_id)
                .ok_or(ControllerError::Invalid(
                    "snapshot movement references unknown topic",
                ))?;
            let assignment = self
                .assignments
                .get(&(movement.topic_id, movement.partition_id))
                .ok_or(ControllerError::Invalid(
                    "snapshot movement references unknown assignment",
                ))?;
            if movement.movement_id != *movement_id
                || movement.source_broker_id == movement.destination_broker_id
                || !self.brokers.contains_key(&movement.source_broker_id)
                || !self.brokers.contains_key(&movement.destination_broker_id)
                || !is_sorted_unique_nonempty(&movement.target_voters)
                || movement.target_voters.len() != usize::from(topic.replication_factor)
                || movement
                    .target_voters
                    .iter()
                    .any(|broker| !self.brokers.contains_key(broker))
                || movement
                    .preferred_leader
                    .is_some_and(|broker| !movement.target_voters.contains(&broker))
                || if movement.stage == MovementStage::Complete {
                    assignment.voters != movement.target_voters
                        || assignment.assignment_generation.get()
                            != movement
                                .expected_assignment_generation
                                .get()
                                .saturating_add(1)
                } else {
                    assignment.assignment_generation != movement.expected_assignment_generation
                        || !assignment.voters.contains(&movement.source_broker_id)
                        || assignment.voters.contains(&movement.destination_broker_id)
                        || assignment
                            .learners
                            .contains(&movement.destination_broker_id)
                }
            {
                return Err(ControllerError::Invalid(
                    "invalid movement record in controller snapshot",
                ));
            }
        }
        Ok(())
    }

    pub fn apply(&mut self, log_index: u64, command: &ControllerCommand) -> Result<ApplyOutcome> {
        if log_index == 0 || log_index <= self.metadata_epoch {
            return Err(ControllerError::Invalid(
                "controller metadata command index must strictly increase",
            ));
        }

        // State-machine validity must not depend on node-local tunables. The V1
        // frozen command limits are therefore revalidated here with the single
        // canonical V1 limit set before any state mutation.
        encode_command(command, CommandLimits::default())?;

        let mut staged = self.clone();
        let producer_ids = staged.apply_inner(command)?;
        staged.metadata_epoch = log_index;
        *self = staged;
        Ok(ApplyOutcome {
            metadata_epoch: log_index,
            producer_ids,
        })
    }

    pub fn apply_producer_session(
        &mut self,
        log_index: u64,
        command: ProducerSessionCommandV2,
    ) -> Result<ApplyOutcome> {
        if log_index == 0 || log_index <= self.metadata_epoch {
            return Err(ControllerError::Invalid(
                "controller metadata command index must strictly increase",
            ));
        }
        FeatureGate::new(P7_PRODUCER_FEATURE_LEVEL)?.require(self.active_feature_level)?;
        encode_producer_session_command_v2(command)?;

        let mut staged = self.clone();
        match command {
            ProducerSessionCommandV2::InitializeSession {
                producer_id,
                producer_epoch,
            } => {
                if producer_epoch.get() != 1 {
                    return Err(ControllerError::Invalid(
                        "new producer session must begin at epoch one",
                    ));
                }
                if producer_id.get() != staged.next_unallocated_producer_id {
                    return Err(ControllerError::Invalid(
                        "new producer session must consume the next durable producer id",
                    ));
                }
                ensure_producer_session_capacity(staged.producer_sessions.len(), 1)?;
                let next = staged
                    .next_unallocated_producer_id
                    .checked_add(1)
                    .ok_or(ControllerError::Invalid("producer id allocator overflow"))?;
                if staged
                    .producer_sessions
                    .insert(producer_id, producer_epoch)
                    .is_some()
                {
                    return Err(ControllerError::Invalid(
                        "producer session id is already authoritative",
                    ));
                }
                staged.next_unallocated_producer_id = next;
                staged.ensure_producer_snapshot_budget()?;
            }
            ProducerSessionCommandV2::BumpEpoch {
                producer_id,
                expected_epoch,
                new_epoch,
            } => {
                let current = staged
                    .producer_sessions
                    .get_mut(&producer_id)
                    .ok_or(ControllerError::Invalid("unknown producer session"))?;
                if *current != expected_epoch {
                    return Err(ControllerError::Invalid("stale producer epoch"));
                }
                if expected_epoch.checked_next() != Some(new_epoch) {
                    return Err(ControllerError::Invalid(
                        "producer epoch bump must advance by exactly one",
                    ));
                }
                *current = new_epoch;
            }
        }
        staged.metadata_epoch = log_index;
        *self = staged;
        Ok(ApplyOutcome {
            metadata_epoch: log_index,
            producer_ids: None,
        })
    }

    pub fn apply_topic_performance(
        &mut self,
        log_index: u64,
        command: &TopicPerformanceCommandV3,
    ) -> Result<ApplyOutcome> {
        if log_index == 0 || log_index <= self.metadata_epoch {
            return Err(ControllerError::Invalid(
                "controller metadata command index must strictly increase",
            ));
        }
        FeatureGate::new(crate::P8_PERFORMANCE_FEATURE_LEVEL)?
            .require(self.active_feature_level)?;
        encode_topic_performance_command_v3(command, CommandLimits::default())?;

        let mut staged = self.clone();
        match command {
            TopicPerformanceCommandV3::CreateTopicWithPerformance { base, performance } => {
                let ControllerCommand::CreateTopic { topic_id, .. } = base else {
                    return Err(ControllerError::Invalid(
                        "P8 V3 command must wrap V1 CreateTopic",
                    ));
                };
                if *topic_id != performance.topic_id {
                    return Err(ControllerError::Invalid(
                        "P8 V3 topic policy id differs from CreateTopic id",
                    ));
                }
                performance.validate()?;
                staged.apply_inner(base)?;
                staged.topic_performance.insert(*topic_id, *performance);
                staged.validate_snapshot_state()?;
                // P8 V3 adds a fixed policy record per topic outside the
                // frozen V1/V2 snapshot bytes. Admission must therefore prove
                // the complete V3 state still fits the hard controller
                // snapshot budget before the command becomes authoritative.
                crate::encode_controller_state_snapshot(&staged, SnapshotLimits::default())?;
            }
        }
        staged.metadata_epoch = log_index;
        *self = staged;
        Ok(ApplyOutcome {
            metadata_epoch: log_index,
            producer_ids: None,
        })
    }

    pub fn apply_topic_retention(
        &mut self,
        log_index: u64,
        command: &TopicRetentionCommandV4,
    ) -> Result<ApplyOutcome> {
        if log_index == 0 || log_index <= self.metadata_epoch {
            return Err(ControllerError::Invalid(
                "controller metadata command index must strictly increase",
            ));
        }
        FeatureGate::new(crate::P9_RETENTION_FEATURE_LEVEL)?.require(self.active_feature_level)?;
        encode_topic_retention_command_v4(command, CommandLimits::default())?;

        let mut staged = self.clone();
        match command {
            TopicRetentionCommandV4::CreateTopicWithPerformanceAndRetention {
                base,
                performance,
                retention,
            } => {
                let ControllerCommand::CreateTopic { topic_id, .. } = base else {
                    return Err(ControllerError::Invalid(
                        "P9 V4 command must wrap V1 CreateTopic",
                    ));
                };
                if *topic_id != performance.topic_id || *topic_id != retention.topic_id {
                    return Err(ControllerError::Invalid(
                        "P9 V4 topic policy ids differ from CreateTopic id",
                    ));
                }
                performance.validate()?;
                retention.validate()?;
                staged.apply_inner(base)?;
                staged.topic_performance.insert(*topic_id, *performance);
                staged.topic_retention.insert(*topic_id, *retention);
            }
            TopicRetentionCommandV4::AlterTopicRetentionPolicy {
                topic_id,
                expected,
                new,
            } => {
                if *topic_id != expected.topic_id || *topic_id != new.topic_id {
                    return Err(ControllerError::Invalid(
                        "P9 V4 alter policy ids differ from topic id",
                    ));
                }
                expected.validate()?;
                new.validate()?;
                let topic = staged
                    .topics
                    .get(topic_id)
                    .ok_or(ControllerError::Invalid("unknown topic"))?;
                if topic.lifecycle != TopicLifecycle::Active {
                    return Err(ControllerError::Invalid(
                        "retention policy mutation requires ACTIVE topic",
                    ));
                }
                let current = staged
                    .topic_retention
                    .get(topic_id)
                    .copied()
                    .ok_or(ControllerError::Invalid("topic retention policy missing"))?;
                if current != *expected {
                    return Err(ControllerError::Invalid("stale topic retention policy"));
                }
                staged.topic_retention.insert(*topic_id, *new);
            }
        }
        staged.validate_snapshot_state()?;
        crate::encode_controller_state_snapshot(&staged, SnapshotLimits::default())?;
        staged.metadata_epoch = log_index;
        *self = staged;
        Ok(ApplyOutcome {
            metadata_epoch: log_index,
            producer_ids: None,
        })
    }

    fn ensure_producer_snapshot_budget(&self) -> Result<()> {
        let limits = SnapshotLimits::default();
        let base = encode_controller_snapshot(&ControllerSnapshot::new(self.clone()), limits)?;
        ensure_producer_snapshot_budget_with_base(
            base.len(),
            self.producer_sessions.len(),
            limits.max_snapshot_bytes,
        )
    }

    fn apply_inner(&mut self, command: &ControllerCommand) -> Result<Option<ProducerIdRange>> {
        match command {
            ControllerCommand::RegisterBroker {
                broker_id,
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
            } => {
                if self.brokers.contains_key(broker_id) {
                    return Err(ControllerError::Invalid("broker id already registered"));
                }
                if self
                    .brokers
                    .values()
                    .any(|broker| broker.node_uuid == *node_uuid)
                {
                    return Err(ControllerError::Invalid("node UUID already registered"));
                }
                ensure_capacity(
                    self.brokers.len(),
                    1,
                    MAX_CONTROLLER_BROKERS,
                    "controller broker records",
                )?;
                ensure_snapshot_budget(
                    self.brokers.len() + 1,
                    self.topics.len(),
                    self.assignments.len(),
                    self.movements.len(),
                )?;
                self.brokers.insert(
                    *broker_id,
                    BrokerRecord {
                        broker_id: *broker_id,
                        node_uuid: *node_uuid,
                        incarnation_epoch: *initial_incarnation,
                        lifecycle: BrokerLifecycle::Joining,
                        client_host: client_host.clone(),
                        client_port: *client_port,
                        internal_host: internal_host.clone(),
                        internal_port: *internal_port,
                        rack: rack.clone(),
                        zone: zone.clone(),
                        min_feature: *min_feature,
                        max_feature: *max_feature,
                    },
                );
            }
            ControllerCommand::RejoinBroker {
                broker_id,
                node_uuid,
                expected_incarnation,
                new_incarnation,
                client_host,
                client_port,
                internal_host,
                internal_port,
                rack,
                zone,
                min_feature,
                max_feature,
            } => {
                let broker = self
                    .brokers
                    .get_mut(broker_id)
                    .ok_or(ControllerError::Invalid("unknown broker for rejoin"))?;
                if broker.node_uuid != *node_uuid {
                    return Err(ControllerError::Invalid(
                        "broker id belongs to another node UUID",
                    ));
                }
                if broker.incarnation_epoch != *expected_incarnation {
                    return Err(ControllerError::Invalid("stale broker incarnation"));
                }
                if matches!(
                    broker.lifecycle,
                    BrokerLifecycle::Fenced | BrokerLifecycle::Removed
                ) {
                    return Err(ControllerError::Invalid(
                        "fenced/removed broker cannot rejoin implicitly",
                    ));
                }
                broker.incarnation_epoch = *new_incarnation;
                broker.client_host.clone_from(client_host);
                broker.client_port = *client_port;
                broker.internal_host.clone_from(internal_host);
                broker.internal_port = *internal_port;
                broker.rack.clone_from(rack);
                broker.zone.clone_from(zone);
                broker.min_feature = *min_feature;
                broker.max_feature = *max_feature;
            }
            ControllerCommand::SetBrokerLifecycle {
                broker_id,
                expected_incarnation,
                expected_lifecycle,
                new_lifecycle,
            } => {
                let broker = self
                    .brokers
                    .get_mut(broker_id)
                    .ok_or(ControllerError::Invalid("unknown broker"))?;
                if broker.incarnation_epoch != *expected_incarnation {
                    return Err(ControllerError::Invalid("stale broker incarnation"));
                }
                if broker.lifecycle != *expected_lifecycle {
                    return Err(ControllerError::Invalid("stale broker lifecycle"));
                }
                if !valid_broker_lifecycle_transition(*expected_lifecycle, *new_lifecycle) {
                    return Err(ControllerError::Invalid(
                        "illegal broker lifecycle transition",
                    ));
                }
                if *new_lifecycle == BrokerLifecycle::Active
                    && (self.active_feature_level < broker.min_feature
                        || self.active_feature_level > broker.max_feature)
                {
                    return Err(ControllerError::Invalid(
                        "broker cannot become ACTIVE at unsupported cluster feature level",
                    ));
                }
                broker.lifecycle = *new_lifecycle;
            }
            ControllerCommand::CreateTopic {
                topic_id,
                topic_name,
                partition_count,
                replication_factor,
                failure_domain_policy,
                assignments,
            } => {
                if self.topics.contains_key(topic_id) || self.topic_names.contains_key(topic_name) {
                    return Err(ControllerError::Invalid("topic id or name already exists"));
                }
                ensure_capacity(
                    self.topics.len(),
                    1,
                    MAX_CONTROLLER_TOPICS,
                    "controller topic records",
                )?;
                ensure_capacity(
                    self.assignments.len(),
                    assignments.len(),
                    MAX_CONTROLLER_ASSIGNMENTS,
                    "controller assignment records",
                )?;
                ensure_snapshot_budget(
                    self.brokers.len(),
                    self.topics.len() + 1,
                    self.assignments.len() + assignments.len(),
                    self.movements.len(),
                )?;
                for assignment in assignments {
                    if assignment.assignment_generation.get() != 1 {
                        return Err(ControllerError::Invalid(
                            "initial assignment generation must be one",
                        ));
                    }
                    self.validate_assignment_brokers_active(assignment)?;
                    if *failure_domain_policy == FailureDomainPolicy::Required {
                        self.validate_required_failure_domains(assignment)?;
                    }
                }
                let topic = TopicRecord {
                    topic_id: *topic_id,
                    topic_name: topic_name.clone(),
                    partition_count: *partition_count,
                    replication_factor: *replication_factor,
                    lifecycle: TopicLifecycle::Active,
                    failure_domain_policy: *failure_domain_policy,
                };
                let performance = TopicPerformancePolicyV1::default_for(*topic_id);
                let retention = TopicRetentionPolicyV1::default_for(*topic_id);
                self.topic_names.insert(topic_name.clone(), *topic_id);
                self.topics.insert(*topic_id, topic);
                self.topic_performance.insert(*topic_id, performance);
                self.topic_retention.insert(*topic_id, retention);
                for assignment in assignments {
                    self.assignments
                        .insert((*topic_id, assignment.partition_id), assignment.clone());
                }
            }
            ControllerCommand::BeginDeleteTopic {
                topic_id,
                expected_topic_lifecycle,
            } => {
                let topic = self
                    .topics
                    .get_mut(topic_id)
                    .ok_or(ControllerError::Invalid("unknown topic"))?;
                if topic.lifecycle != *expected_topic_lifecycle
                    || topic.lifecycle != TopicLifecycle::Active
                {
                    return Err(ControllerError::Invalid("stale topic lifecycle"));
                }
                topic.lifecycle = TopicLifecycle::Deleting;
            }
            ControllerCommand::FinalizeDeleteTopic {
                topic_id,
                expected_topic_lifecycle,
            } => {
                let topic = self
                    .topics
                    .get(topic_id)
                    .ok_or(ControllerError::Invalid("unknown topic"))?;
                if topic.lifecycle != *expected_topic_lifecycle
                    || topic.lifecycle != TopicLifecycle::Deleting
                {
                    return Err(ControllerError::Invalid("topic is not deleting"));
                }
                if self.movements.values().any(|movement| {
                    movement.topic_id == *topic_id && movement.stage != MovementStage::Complete
                }) {
                    return Err(ControllerError::Invalid(
                        "topic cannot be finalized while movement is active",
                    ));
                }
                let name = topic.topic_name.clone();
                self.topics.remove(topic_id);
                self.topic_performance.remove(topic_id);
                self.topic_retention.remove(topic_id);
                self.topic_names.remove(&name);
                self.assignments.retain(|(id, _), _| id != topic_id);
                self.movements
                    .retain(|_, movement| movement.topic_id != *topic_id);
            }
            ControllerCommand::ExpandTopic {
                topic_id,
                expected_partition_count,
                new_partition_count,
                assignments,
            } => {
                let topic = self
                    .topics
                    .get(topic_id)
                    .ok_or(ControllerError::Invalid("unknown topic"))?;
                if topic.lifecycle != TopicLifecycle::Active
                    || topic.partition_count != *expected_partition_count
                {
                    return Err(ControllerError::Invalid(
                        "stale topic state for partition expansion",
                    ));
                }
                ensure_capacity(
                    self.assignments.len(),
                    assignments.len(),
                    MAX_CONTROLLER_ASSIGNMENTS,
                    "controller assignment records",
                )?;
                ensure_snapshot_budget(
                    self.brokers.len(),
                    self.topics.len(),
                    self.assignments.len() + assignments.len(),
                    self.movements.len(),
                )?;
                for assignment in assignments {
                    if assignment.assignment_generation.get() != 1
                        || assignment.voters.len() != usize::from(topic.replication_factor)
                    {
                        return Err(ControllerError::Invalid(
                            "expanded partition assignment is incompatible with topic",
                        ));
                    }
                    self.validate_assignment_brokers_active(assignment)?;
                    if topic.failure_domain_policy == FailureDomainPolicy::Required {
                        self.validate_required_failure_domains(assignment)?;
                    }
                }
                for assignment in assignments {
                    if self
                        .assignments
                        .insert((*topic_id, assignment.partition_id), assignment.clone())
                        .is_some()
                    {
                        return Err(ControllerError::Invalid(
                            "partition expansion would replace an existing assignment",
                        ));
                    }
                }
                self.topics
                    .get_mut(topic_id)
                    .expect("topic checked above")
                    .partition_count = *new_partition_count;
            }
            ControllerCommand::BeginMovement {
                movement_id,
                topic_id,
                partition_id,
                expected_assignment_generation,
                source_broker_id,
                destination_broker_id,
                target_voters,
                preferred_leader,
            } => {
                if self.movements.contains_key(movement_id)
                    || self.movements.values().any(|movement| {
                        movement.topic_id == *topic_id
                            && movement.partition_id == *partition_id
                            && movement.stage != MovementStage::Complete
                    })
                {
                    return Err(ControllerError::Invalid(
                        "partition already has an active movement",
                    ));
                }
                ensure_capacity(
                    self.movements.len(),
                    1,
                    MAX_CONTROLLER_MOVEMENTS,
                    "controller movement records",
                )?;
                ensure_snapshot_budget(
                    self.brokers.len(),
                    self.topics.len(),
                    self.assignments.len(),
                    self.movements.len() + 1,
                )?;
                let topic = self
                    .topics
                    .get(topic_id)
                    .ok_or(ControllerError::Invalid("unknown movement topic"))?;
                if topic.lifecycle != TopicLifecycle::Active
                    || target_voters.len() != usize::from(topic.replication_factor)
                {
                    return Err(ControllerError::Invalid(
                        "movement target is incompatible with topic",
                    ));
                }
                let assignment = self
                    .assignments
                    .get(&(*topic_id, *partition_id))
                    .ok_or(ControllerError::Invalid("unknown movement partition"))?;
                if assignment.assignment_generation != *expected_assignment_generation
                    || !assignment.voters.contains(source_broker_id)
                    || assignment.voters.contains(destination_broker_id)
                    || assignment.learners.contains(destination_broker_id)
                {
                    return Err(ControllerError::Invalid(
                        "stale or invalid movement source/destination",
                    ));
                }
                self.validate_broker_active(*destination_broker_id)?;
                for voter in target_voters {
                    self.validate_broker_active(*voter)?;
                }
                let expected_targets = assignment
                    .voters
                    .iter()
                    .copied()
                    .filter(|broker| broker != source_broker_id)
                    .chain(core::iter::once(*destination_broker_id))
                    .collect::<std::collections::BTreeSet<_>>()
                    .into_iter()
                    .collect::<Vec<_>>();
                if expected_targets != *target_voters
                    || preferred_leader.is_some_and(|leader| !target_voters.contains(&leader))
                {
                    return Err(ControllerError::Invalid(
                        "movement target voters do not represent one-for-one replacement",
                    ));
                }
                self.movements.insert(
                    *movement_id,
                    MovementRecord {
                        movement_id: *movement_id,
                        topic_id: *topic_id,
                        partition_id: *partition_id,
                        expected_assignment_generation: *expected_assignment_generation,
                        source_broker_id: *source_broker_id,
                        destination_broker_id: *destination_broker_id,
                        stage: MovementStage::Planned,
                        target_voters: target_voters.clone(),
                        preferred_leader: *preferred_leader,
                    },
                );
            }
            ControllerCommand::AdvanceMovement {
                movement_id,
                expected_stage,
                new_stage,
            } => {
                let movement = self
                    .movements
                    .get_mut(movement_id)
                    .ok_or(ControllerError::Invalid("unknown movement"))?;
                if movement.stage != *expected_stage {
                    return Err(ControllerError::Invalid("stale movement stage"));
                }
                if !valid_movement_transition(*expected_stage, *new_stage) {
                    return Err(ControllerError::Invalid(
                        "illegal movement stage transition",
                    ));
                }
                movement.stage = *new_stage;
            }
            ControllerCommand::FinalizeMovement {
                movement_id,
                expected_stage,
                replacement,
            } => {
                let movement = self
                    .movements
                    .get(movement_id)
                    .ok_or(ControllerError::Invalid("unknown movement"))?
                    .clone();
                if movement.stage != *expected_stage
                    || movement.stage != MovementStage::OldReplicaRemoved
                    || replacement.topic_id != movement.topic_id
                    || replacement.partition_id != movement.partition_id
                    || !replacement.learners.is_empty()
                    || replacement.voters != movement.target_voters
                {
                    return Err(ControllerError::Invalid(
                        "movement finalization does not match committed movement intent",
                    ));
                }
                let current = self
                    .assignments
                    .get(&(movement.topic_id, movement.partition_id))
                    .ok_or(ControllerError::Invalid("movement assignment disappeared"))?;
                if current.assignment_generation != movement.expected_assignment_generation
                    || replacement.assignment_generation
                        != current.assignment_generation.checked_next()?
                {
                    return Err(ControllerError::Invalid(
                        "movement finalization has stale assignment generation",
                    ));
                }
                for voter in &replacement.voters {
                    self.validate_broker_active(*voter)?;
                }
                self.assignments.insert(
                    (movement.topic_id, movement.partition_id),
                    replacement.clone(),
                );
                self.movements
                    .get_mut(movement_id)
                    .expect("movement checked above")
                    .stage = MovementStage::Complete;
            }
            ControllerCommand::SetFeatureLevel {
                expected_feature_level,
                new_feature_level,
            } => {
                if self.active_feature_level != *expected_feature_level {
                    return Err(ControllerError::Invalid("stale cluster feature level"));
                }
                if *new_feature_level <= *expected_feature_level {
                    return Err(ControllerError::Invalid(
                        "cluster feature level must strictly increase",
                    ));
                }
                if self.brokers.values().any(|broker| {
                    matches!(
                        broker.lifecycle,
                        BrokerLifecycle::Active | BrokerLifecycle::Draining
                    ) && (*new_feature_level < broker.min_feature
                        || *new_feature_level > broker.max_feature)
                }) {
                    return Err(ControllerError::Invalid(
                        "active broker does not support requested feature level",
                    ));
                }
                self.active_feature_level = *new_feature_level;
            }
            ControllerCommand::AllocateProducerIds { allocation_count } => {
                let first = self.next_unallocated_producer_id;
                let next = first
                    .checked_add(u64::from(*allocation_count))
                    .ok_or(ControllerError::Invalid("producer id allocator overflow"))?;
                let last = next - 1;
                self.next_unallocated_producer_id = next;
                return Ok(Some(ProducerIdRange { first, last }));
            }
        }
        Ok(None)
    }

    fn validate_assignment_brokers_active(&self, assignment: &AssignmentRecord) -> Result<()> {
        for broker_id in assignment.voters.iter().chain(&assignment.learners) {
            self.validate_broker_active(*broker_id)?;
        }
        Ok(())
    }

    fn validate_broker_active(&self, broker_id: u32) -> Result<()> {
        let broker = self
            .brokers
            .get(&broker_id)
            .ok_or(ControllerError::Invalid(
                "assignment references unknown broker",
            ))?;
        if broker.lifecycle != BrokerLifecycle::Active {
            return Err(ControllerError::Invalid(
                "new placement requires ACTIVE broker",
            ));
        }
        if self.active_feature_level < broker.min_feature
            || self.active_feature_level > broker.max_feature
        {
            return Err(ControllerError::Invalid(
                "broker does not support active cluster feature level",
            ));
        }
        Ok(())
    }

    fn validate_required_failure_domains(&self, assignment: &AssignmentRecord) -> Result<()> {
        let mut racks = std::collections::BTreeSet::new();
        let mut zones = std::collections::BTreeSet::new();
        for broker_id in &assignment.voters {
            let broker = self.brokers.get(broker_id).ok_or(ControllerError::Invalid(
                "required placement references unknown broker",
            ))?;
            let rack = broker.rack.as_ref().ok_or(ControllerError::Invalid(
                "required placement broker is missing rack label",
            ))?;
            let zone = broker.zone.as_ref().ok_or(ControllerError::Invalid(
                "required placement broker is missing zone label",
            ))?;
            if !racks.insert(rack.as_str()) || !zones.insert(zone.as_str()) {
                return Err(ControllerError::Invalid(
                    "required placement must use distinct racks and zones",
                ));
            }
        }
        Ok(())
    }
}

fn is_sorted_unique(values: &[u32]) -> bool {
    values.windows(2).all(|window| window[0] < window[1])
}

fn ensure_capacity(
    current: usize,
    additional: usize,
    max: usize,
    what: &'static str,
) -> Result<()> {
    let actual = current
        .checked_add(additional)
        .ok_or(ControllerError::LimitExceeded {
            what,
            actual: usize::MAX,
            max,
        })?;
    if actual > max {
        return Err(ControllerError::LimitExceeded { what, actual, max });
    }
    Ok(())
}

fn ensure_producer_session_capacity(current: usize, additional: usize) -> Result<()> {
    ensure_capacity(
        current,
        additional,
        MAX_PRODUCER_SESSIONS,
        "controller producer sessions",
    )
}

fn ensure_producer_snapshot_budget_with_base(
    base_snapshot_bytes: usize,
    producer_session_count: usize,
    max_snapshot_bytes: usize,
) -> Result<()> {
    let session_bytes = producer_session_count
        .checked_mul(12)
        .ok_or(ControllerError::Invalid("producer session bytes overflow"))?;
    let actual = 16_usize
        .checked_add(base_snapshot_bytes)
        .and_then(|value| value.checked_add(session_bytes))
        .and_then(|value| value.checked_add(4))
        .ok_or(ControllerError::Invalid(
            "controller snapshot V2 length overflow",
        ))?;
    if actual > max_snapshot_bytes {
        return Err(ControllerError::LimitExceeded {
            what: "controller snapshot bytes",
            actual,
            max: max_snapshot_bytes,
        });
    }
    Ok(())
}

fn ensure_snapshot_budget(
    brokers: usize,
    topics: usize,
    assignments: usize,
    movements: usize,
) -> Result<()> {
    let actual = canonical_snapshot_worst_case_bytes(brokers, topics, assignments, movements)
        .ok_or(ControllerError::LimitExceeded {
            what: "controller snapshot worst-case bytes",
            actual: usize::MAX,
            max: crate::MAX_CONTROLLER_SNAPSHOT_BYTES,
        })?;
    if actual > crate::MAX_CONTROLLER_SNAPSHOT_BYTES {
        return Err(ControllerError::LimitExceeded {
            what: "controller snapshot worst-case bytes",
            actual,
            max: crate::MAX_CONTROLLER_SNAPSHOT_BYTES,
        });
    }
    Ok(())
}

#[cfg(test)]
pub(crate) fn ensure_capacity_for_test(
    current: usize,
    additional: usize,
    max: usize,
) -> Result<()> {
    ensure_capacity(current, additional, max, "controller test capacity")
}

#[cfg(test)]
pub(crate) fn ensure_snapshot_budget_for_test(
    brokers: usize,
    topics: usize,
    assignments: usize,
    movements: usize,
) -> Result<()> {
    ensure_snapshot_budget(brokers, topics, assignments, movements)
}

fn is_sorted_unique_nonempty(values: &[u32]) -> bool {
    !values.is_empty() && is_sorted_unique(values)
}

fn valid_broker_lifecycle_transition(from: BrokerLifecycle, to: BrokerLifecycle) -> bool {
    matches!(
        (from, to),
        (BrokerLifecycle::Joining, BrokerLifecycle::Active)
            | (BrokerLifecycle::Joining, BrokerLifecycle::Fenced)
            | (BrokerLifecycle::Joining, BrokerLifecycle::Removed)
            | (BrokerLifecycle::Active, BrokerLifecycle::Draining)
            | (BrokerLifecycle::Active, BrokerLifecycle::Fenced)
            | (BrokerLifecycle::Draining, BrokerLifecycle::Removed)
            | (BrokerLifecycle::Draining, BrokerLifecycle::Fenced)
            | (BrokerLifecycle::Fenced, BrokerLifecycle::Removed)
    )
}

fn valid_movement_transition(from: MovementStage, to: MovementStage) -> bool {
    matches!(
        (from, to),
        (MovementStage::Planned, MovementStage::LearnerRequested)
            | (
                MovementStage::LearnerRequested,
                MovementStage::LearnerCreated
            )
            | (MovementStage::LearnerCreated, MovementStage::CatchingUp)
            | (MovementStage::CatchingUp, MovementStage::PromotionReady)
            | (
                MovementStage::PromotionReady,
                MovementStage::EnterJointRequested
            )
            | (
                MovementStage::EnterJointRequested,
                MovementStage::JointCommitted
            )
            | (
                MovementStage::JointCommitted,
                MovementStage::LeaveJointRequested
            )
            | (
                MovementStage::LeaveJointRequested,
                MovementStage::NewStableCommitted
            )
            | (
                MovementStage::NewStableCommitted,
                MovementStage::LeadershipTransfer
            )
            | (
                MovementStage::NewStableCommitted,
                MovementStage::OldReplicaRemoveRequested
            )
            | (
                MovementStage::LeadershipTransfer,
                MovementStage::OldReplicaRemoveRequested
            )
            | (
                MovementStage::OldReplicaRemoveRequested,
                MovementStage::OldReplicaRemoved
            )
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{AssignmentGeneration, FailureDomainPolicy, MovementId};

    fn register(broker_id: u32, uuid: u8) -> ControllerCommand {
        ControllerCommand::RegisterBroker {
            broker_id,
            node_uuid: [uuid; 16],
            initial_incarnation: 1,
            client_host: format!("broker-{broker_id}"),
            client_port: 9000 + u16::try_from(broker_id).unwrap(),
            internal_host: format!("broker-{broker_id}-internal"),
            internal_port: 10000 + u16::try_from(broker_id).unwrap(),
            rack: Some(format!("rack-{broker_id}")),
            zone: Some(format!("zone-{broker_id}")),
            min_feature: 1,
            max_feature: 3,
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

    fn assignment(
        topic: TopicId,
        partition: u32,
        generation: u64,
        voters: Vec<u32>,
    ) -> AssignmentRecord {
        AssignmentRecord {
            topic_id: topic,
            partition_id: partition,
            assignment_generation: AssignmentGeneration::new(generation).unwrap(),
            voters,
            learners: Vec::new(),
        }
    }

    fn active_three_broker_state() -> ControllerState {
        let mut state = ControllerState::new([9; 16], 1, 1_000).unwrap();
        let mut index = 1;
        for id in 1..=3 {
            state
                .apply(index, &register(id, u8::try_from(id).unwrap()))
                .unwrap();
            index += 1;
            state.apply(index, &activate(id)).unwrap();
            index += 1;
        }
        state
    }

    #[test]
    fn p8_feature_four_and_v3_create_are_commit_gated_and_atomic() {
        let mut unsupported = ControllerState::new([0x71; 16], 1, 1).unwrap();
        unsupported.apply(1, &register(1, 1)).unwrap();
        unsupported.apply(2, &activate(1)).unwrap();
        let before = unsupported.clone();
        assert!(
            unsupported
                .apply(
                    3,
                    &ControllerCommand::SetFeatureLevel {
                        expected_feature_level: 1,
                        new_feature_level: 4,
                    },
                )
                .is_err()
        );
        assert_eq!(unsupported, before);

        let mut state = ControllerState::new([0x72; 16], 1, 1).unwrap();
        let mut registration = register(1, 1);
        if let ControllerCommand::RegisterBroker { max_feature, .. } = &mut registration {
            *max_feature = 4;
        }
        state.apply(1, &registration).unwrap();
        state.apply(2, &activate(1)).unwrap();
        let topic_id = TopicId::new([0x73; 16]);
        let base = ControllerCommand::CreateTopic {
            topic_id,
            topic_name: "p8-state".into(),
            partition_count: 1,
            replication_factor: 1,
            failure_domain_policy: FailureDomainPolicy::Disabled,
            assignments: vec![AssignmentRecord {
                topic_id,
                partition_id: 0,
                assignment_generation: AssignmentGeneration::new(1).unwrap(),
                voters: vec![1],
                learners: vec![],
            }],
        };
        let command = TopicPerformanceCommandV3::CreateTopicWithPerformance {
            base,
            performance: TopicPerformancePolicyV1::new(
                topic_id,
                crate::CompressionPolicyV1::Lz4,
                1024,
                10,
                4096,
            )
            .unwrap(),
        };
        let before = state.clone();
        assert!(state.apply_topic_performance(3, &command).is_err());
        assert_eq!(state, before);

        state
            .apply(
                3,
                &ControllerCommand::SetFeatureLevel {
                    expected_feature_level: 1,
                    new_feature_level: 4,
                },
            )
            .unwrap();
        state.apply_topic_performance(4, &command).unwrap();
        assert_eq!(state.metadata_epoch(), 4);
        assert_eq!(
            state
                .topic_performance_policy(topic_id)
                .unwrap()
                .compression,
            crate::CompressionPolicyV1::Lz4
        );
    }

    #[test]
    fn p9_feature_five_create_alter_and_delete_are_commit_gated_and_atomic() {
        let mut unsupported = ControllerState::new([0x73; 16], 1, 1).unwrap();
        let mut max_four = register(1, 1);
        if let ControllerCommand::RegisterBroker { max_feature, .. } = &mut max_four {
            *max_feature = 4;
        }
        unsupported.apply(1, &max_four).unwrap();
        unsupported.apply(2, &activate(1)).unwrap();
        let before = unsupported.clone();
        assert!(
            unsupported
                .apply(
                    3,
                    &ControllerCommand::SetFeatureLevel {
                        expected_feature_level: 1,
                        new_feature_level: 5,
                    },
                )
                .is_err()
        );
        assert_eq!(unsupported, before);

        let mut state = ControllerState::new([0x74; 16], 1, 1).unwrap();
        let mut registration = register(1, 1);
        if let ControllerCommand::RegisterBroker { max_feature, .. } = &mut registration {
            *max_feature = 5;
        }
        state.apply(1, &registration).unwrap();
        state.apply(2, &activate(1)).unwrap();

        let topic_id = TopicId::new([0x75; 16]);
        let create = TopicRetentionCommandV4::CreateTopicWithPerformanceAndRetention {
            base: ControllerCommand::CreateTopic {
                topic_id,
                topic_name: "p9-state".into(),
                partition_count: 1,
                replication_factor: 1,
                failure_domain_policy: FailureDomainPolicy::Disabled,
                assignments: vec![AssignmentRecord {
                    topic_id,
                    partition_id: 0,
                    assignment_generation: AssignmentGeneration::new(1).unwrap(),
                    voters: vec![1],
                    learners: vec![],
                }],
            },
            performance: TopicPerformancePolicyV1::new(
                topic_id,
                crate::CompressionPolicyV1::Zstd,
                2048,
                16,
                16 * 1024,
            )
            .unwrap(),
            retention: TopicRetentionPolicyV1::new(
                topic_id,
                crate::CleanupPolicyV1::CompactDelete,
                Some(60_000),
                Some(1_000_000),
                30_000,
                crate::SubscriptionProtectionV1::Disabled,
            )
            .unwrap(),
        };
        let before = state.clone();
        assert!(state.apply_topic_retention(3, &create).is_err());
        assert_eq!(state, before);

        state
            .apply(
                3,
                &ControllerCommand::SetFeatureLevel {
                    expected_feature_level: 1,
                    new_feature_level: 5,
                },
            )
            .unwrap();
        state.apply_topic_retention(4, &create).unwrap();
        assert_eq!(
            state
                .topic_retention_policy(topic_id)
                .unwrap()
                .cleanup_policy,
            crate::CleanupPolicyV1::CompactDelete
        );

        let current = *state.topic_retention_policy(topic_id).unwrap();
        let next = TopicRetentionPolicyV1::new(
            topic_id,
            crate::CleanupPolicyV1::Delete,
            None,
            Some(4096),
            current.tombstone_grace_ms,
            crate::SubscriptionProtectionV1::Disabled,
        )
        .unwrap();
        let stale = TopicRetentionCommandV4::AlterTopicRetentionPolicy {
            topic_id,
            expected: TopicRetentionPolicyV1::default_for(topic_id),
            new: next,
        };
        let before = state.clone();
        assert!(state.apply_topic_retention(5, &stale).is_err());
        assert_eq!(state, before);

        state
            .apply_topic_retention(
                5,
                &TopicRetentionCommandV4::AlterTopicRetentionPolicy {
                    topic_id,
                    expected: current,
                    new: next,
                },
            )
            .unwrap();
        assert_eq!(state.topic_retention_policy(topic_id), Some(&next));

        state
            .apply(
                6,
                &ControllerCommand::BeginDeleteTopic {
                    topic_id,
                    expected_topic_lifecycle: TopicLifecycle::Active,
                },
            )
            .unwrap();
        let before = state.clone();
        assert!(
            state
                .apply_topic_retention(
                    7,
                    &TopicRetentionCommandV4::AlterTopicRetentionPolicy {
                        topic_id,
                        expected: next,
                        new: current,
                    },
                )
                .is_err()
        );
        assert_eq!(state, before);
        state
            .apply(
                7,
                &ControllerCommand::FinalizeDeleteTopic {
                    topic_id,
                    expected_topic_lifecycle: TopicLifecycle::Deleting,
                },
            )
            .unwrap();
        assert!(state.topic_retention_policy(topic_id).is_none());
        assert!(state.topic_performance_policy(topic_id).is_none());
    }

    #[test]
    fn same_ordered_commands_produce_identical_state() {
        let commands = [register(1, 1), activate(1), register(2, 2), activate(2)];
        let mut a = ControllerState::new([1; 16], 1, 100).unwrap();
        let mut b = a.clone();
        for (ordinal, command) in commands.iter().enumerate() {
            let index = u64::try_from(ordinal + 1).unwrap();
            assert_eq!(
                a.apply(index, command).unwrap(),
                b.apply(index, command).unwrap()
            );
        }
        assert_eq!(a, b);
    }

    #[test]
    fn failed_transition_is_atomic_and_does_not_advance_metadata_epoch() {
        let mut state = ControllerState::new([1; 16], 1, 100).unwrap();
        state.apply(1, &register(1, 1)).unwrap();
        let before = state.clone();
        let stale = ControllerCommand::SetBrokerLifecycle {
            broker_id: 1,
            expected_incarnation: 99,
            expected_lifecycle: BrokerLifecycle::Joining,
            new_lifecycle: BrokerLifecycle::Active,
        };
        assert!(state.apply(2, &stale).is_err());
        assert_eq!(state, before);
    }

    #[test]
    fn rejoin_strictly_increments_incarnation_and_stale_rejoin_fails() {
        let mut state = ControllerState::new([1; 16], 1, 100).unwrap();
        state.apply(1, &register(1, 1)).unwrap();
        state.apply(2, &activate(1)).unwrap();
        let rejoin = ControllerCommand::RejoinBroker {
            broker_id: 1,
            node_uuid: [1; 16],
            expected_incarnation: 1,
            new_incarnation: 2,
            client_host: "new-host".into(),
            client_port: 9092,
            internal_host: "new-internal".into(),
            internal_port: 9093,
            rack: Some("rack-1".into()),
            zone: Some("zone-1".into()),
            min_feature: 1,
            max_feature: 3,
        };
        state.apply(3, &rejoin).unwrap();
        assert_eq!(state.brokers()[&1].incarnation_epoch, 2);
        assert!(state.apply(4, &rejoin).is_err());
        assert_eq!(state.metadata_epoch(), 3);
    }

    #[test]
    fn topic_creation_requires_active_known_replicas_and_is_atomic_with_assignments() {
        let mut state = active_three_broker_state();
        let topic = TopicId::new([3; 16]);
        let create = ControllerCommand::CreateTopic {
            topic_id: topic,
            topic_name: "orders".into(),
            partition_count: 2,
            replication_factor: 3,
            failure_domain_policy: FailureDomainPolicy::Required,
            assignments: vec![
                assignment(topic, 0, 1, vec![1, 2, 3]),
                assignment(topic, 1, 1, vec![1, 2, 3]),
            ],
        };
        state.apply(7, &create).unwrap();
        assert_eq!(state.topic_by_name("orders").unwrap().topic_id, topic);
        assert_eq!(state.assignments().len(), 2);
    }

    #[test]
    fn movement_is_serialized_and_finalization_advances_assignment_generation() {
        let mut state = ControllerState::new([7; 16], 1, 1_000).unwrap();
        let mut index = 1;
        for id in 1..=4 {
            state
                .apply(index, &register(id, u8::try_from(id).unwrap()))
                .unwrap();
            index += 1;
            state.apply(index, &activate(id)).unwrap();
            index += 1;
        }
        let topic = TopicId::new([4; 16]);
        state
            .apply(
                index,
                &ControllerCommand::CreateTopic {
                    topic_id: topic,
                    topic_name: "orders".into(),
                    partition_count: 1,
                    replication_factor: 3,
                    failure_domain_policy: FailureDomainPolicy::Preferred,
                    assignments: vec![assignment(topic, 0, 1, vec![1, 2, 3])],
                },
            )
            .unwrap();
        index += 1;
        let movement_id = MovementId::new([8; 16]).unwrap();
        state
            .apply(
                index,
                &ControllerCommand::BeginMovement {
                    movement_id,
                    topic_id: topic,
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
        for (from, to) in [
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
        ] {
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
                    replacement: assignment(topic, 0, 2, vec![2, 3, 4]),
                },
            )
            .unwrap();
        assert_eq!(
            state.assignments()[&(topic, 0)].assignment_generation.get(),
            2
        );
        assert_eq!(
            state.movements()[&movement_id].stage,
            MovementStage::Complete
        );
    }

    #[test]
    fn feature_level_rejects_unsupported_active_broker() {
        let mut state = ControllerState::new([1; 16], 1, 100).unwrap();
        let mut limited = register(1, 1);
        if let ControllerCommand::RegisterBroker { max_feature, .. } = &mut limited {
            *max_feature = 1;
        }
        state.apply(1, &limited).unwrap();
        state.apply(2, &activate(1)).unwrap();
        assert!(
            state
                .apply(
                    3,
                    &ControllerCommand::SetFeatureLevel {
                        expected_feature_level: 1,
                        new_feature_level: 2,
                    },
                )
                .is_err()
        );
        assert_eq!(state.active_feature_level(), 1);
    }

    #[test]
    fn feature_level_changes_only_explicitly_monotonically_and_drives_gate() {
        let mut state = ControllerState::new([1; 16], 1, 100).unwrap();
        let gate = FeatureGate::new(2).unwrap();
        assert!(state.require_feature(gate).is_err());

        state.apply(1, &register(1, 1)).unwrap();
        state.apply(2, &activate(1)).unwrap();
        assert_eq!(state.active_feature_level(), 1);
        assert!(state.require_feature(gate).is_err());

        state
            .apply(
                3,
                &ControllerCommand::SetFeatureLevel {
                    expected_feature_level: 1,
                    new_feature_level: 2,
                },
            )
            .unwrap();
        assert_eq!(state.active_feature_level(), 2);
        state.require_feature(gate).unwrap();

        let before = state.clone();
        assert!(
            state
                .apply(
                    4,
                    &ControllerCommand::SetFeatureLevel {
                        expected_feature_level: 2,
                        new_feature_level: 1,
                    },
                )
                .is_err()
        );
        assert_eq!(state, before);
    }

    #[test]
    fn producer_id_ranges_are_monotonic_non_overlapping_and_lost_ranges_are_not_reused() {
        let mut state = ControllerState::new([1; 16], 1, 10_000).unwrap();
        let first = state
            .apply(
                1,
                &ControllerCommand::AllocateProducerIds {
                    allocation_count: 100,
                },
            )
            .unwrap()
            .producer_ids
            .unwrap();
        let second = state
            .apply(
                2,
                &ControllerCommand::AllocateProducerIds {
                    allocation_count: 100,
                },
            )
            .unwrap()
            .producer_ids
            .unwrap();
        assert_eq!(
            first,
            ProducerIdRange {
                first: 10_000,
                last: 10_099
            }
        );
        assert_eq!(
            second,
            ProducerIdRange {
                first: 10_100,
                last: 10_199
            }
        );
    }

    #[test]
    fn metadata_index_must_strictly_advance() {
        let mut state = ControllerState::new([1; 16], 1, 100).unwrap();
        state.apply(5, &register(1, 1)).unwrap();
        assert!(state.apply(5, &activate(1)).is_err());
        assert!(state.apply(4, &activate(1)).is_err());
        assert_eq!(state.metadata_epoch(), 5);
    }

    #[test]
    fn duplicate_broker_id_or_node_uuid_is_rejected_atomically() {
        let mut state = ControllerState::new([1; 16], 1, 100).unwrap();
        state.apply(1, &register(1, 1)).unwrap();
        let before = state.clone();
        assert!(state.apply(2, &register(1, 2)).is_err());
        assert_eq!(state, before);
        assert!(state.apply(2, &register(2, 1)).is_err());
        assert_eq!(state, before);
    }

    #[test]
    fn broker_lifecycle_cannot_resurrect_removed_authority() {
        let mut state = ControllerState::new([1; 16], 1, 100).unwrap();
        state.apply(1, &register(1, 1)).unwrap();
        state
            .apply(
                2,
                &ControllerCommand::SetBrokerLifecycle {
                    broker_id: 1,
                    expected_incarnation: 1,
                    expected_lifecycle: BrokerLifecycle::Joining,
                    new_lifecycle: BrokerLifecycle::Removed,
                },
            )
            .unwrap();
        let before = state.clone();
        assert!(
            state
                .apply(
                    3,
                    &ControllerCommand::SetBrokerLifecycle {
                        broker_id: 1,
                        expected_incarnation: 1,
                        expected_lifecycle: BrokerLifecycle::Removed,
                        new_lifecycle: BrokerLifecycle::Active,
                    },
                )
                .is_err()
        );
        assert_eq!(state, before);
    }

    #[test]
    fn broker_authority_requires_exact_uuid_current_incarnation_and_serving_lifecycle() {
        let mut state = ControllerState::new([1; 16], 1, 100).unwrap();
        state.apply(1, &register(1, 1)).unwrap();
        let current = BrokerAuthority {
            broker_id: 1,
            node_uuid: [1; 16],
            incarnation_epoch: 1,
        };
        assert_eq!(
            state.validate_broker_authority(current).unwrap().lifecycle,
            BrokerLifecycle::Joining
        );
        assert!(
            state
                .validate_broker_authority(BrokerAuthority {
                    incarnation_epoch: 2,
                    ..current
                })
                .is_err()
        );
        assert!(
            state
                .validate_broker_authority(BrokerAuthority {
                    node_uuid: [9; 16],
                    ..current
                })
                .is_err()
        );
        state
            .apply(
                2,
                &ControllerCommand::SetBrokerLifecycle {
                    broker_id: 1,
                    expected_incarnation: 1,
                    expected_lifecycle: BrokerLifecycle::Joining,
                    new_lifecycle: BrokerLifecycle::Fenced,
                },
            )
            .unwrap();
        assert!(state.validate_broker_authority(current).is_err());
    }

    #[test]
    fn activation_requires_support_for_current_cluster_feature_level() {
        let mut state = ControllerState::new([1; 16], 2, 100).unwrap();
        let mut registration = register(1, 1);
        if let ControllerCommand::RegisterBroker {
            min_feature,
            max_feature,
            ..
        } = &mut registration
        {
            *min_feature = 1;
            *max_feature = 1;
        }
        state.apply(1, &registration).unwrap();
        let before = state.clone();
        assert!(state.apply(2, &activate(1)).is_err());
        assert_eq!(state, before);
    }

    #[test]
    fn topic_creation_rejects_unknown_or_non_active_assignment_broker() {
        let mut state = ControllerState::new([1; 16], 1, 100).unwrap();
        state.apply(1, &register(1, 1)).unwrap();
        state.apply(2, &register(2, 2)).unwrap();
        state.apply(3, &activate(1)).unwrap();
        state.apply(4, &activate(2)).unwrap();
        let topic = TopicId::new([6; 16]);
        let create = ControllerCommand::CreateTopic {
            topic_id: topic,
            topic_name: "unsafe".into(),
            partition_count: 1,
            replication_factor: 3,
            failure_domain_policy: FailureDomainPolicy::Disabled,
            assignments: vec![assignment(topic, 0, 1, vec![1, 2, 3])],
        };
        let before = state.clone();
        assert!(state.apply(5, &create).is_err());
        assert_eq!(state, before);

        state.apply(5, &register(3, 3)).unwrap();
        let before = state.clone();
        assert!(state.apply(6, &create).is_err());
        assert_eq!(state, before);
    }

    #[test]
    fn required_topic_creation_rejects_duplicate_or_missing_failure_domains_atomically() {
        let mut state = active_three_broker_state();
        state.brokers.get_mut(&2).unwrap().rack = Some("rack-1".into());
        let topic = TopicId::new([11; 16]);
        let create = ControllerCommand::CreateTopic {
            topic_id: topic,
            topic_name: "required-domains".into(),
            partition_count: 1,
            replication_factor: 3,
            failure_domain_policy: FailureDomainPolicy::Required,
            assignments: vec![assignment(topic, 0, 1, vec![1, 2, 3])],
        };
        let before = state.clone();
        assert!(state.apply(7, &create).is_err());
        assert_eq!(state, before);

        state.brokers.get_mut(&2).unwrap().rack = Some("rack-2".into());
        state.brokers.get_mut(&3).unwrap().zone = None;
        let before = state.clone();
        assert!(state.apply(7, &create).is_err());
        assert_eq!(state, before);
    }

    #[test]
    fn movement_stage_cannot_skip_required_safety_stages() {
        let mut state = ControllerState::new([7; 16], 1, 1_000).unwrap();
        let mut index = 1;
        for id in 1..=4 {
            state
                .apply(index, &register(id, u8::try_from(id).unwrap()))
                .unwrap();
            index += 1;
            state.apply(index, &activate(id)).unwrap();
            index += 1;
        }
        let topic = TopicId::new([7; 16]);
        state
            .apply(
                index,
                &ControllerCommand::CreateTopic {
                    topic_id: topic,
                    topic_name: "movement".into(),
                    partition_count: 1,
                    replication_factor: 3,
                    failure_domain_policy: FailureDomainPolicy::Disabled,
                    assignments: vec![assignment(topic, 0, 1, vec![1, 2, 3])],
                },
            )
            .unwrap();
        index += 1;
        let movement_id = MovementId::new([9; 16]).unwrap();
        state
            .apply(
                index,
                &ControllerCommand::BeginMovement {
                    movement_id,
                    topic_id: topic,
                    partition_id: 0,
                    expected_assignment_generation: AssignmentGeneration::new(1).unwrap(),
                    source_broker_id: 1,
                    destination_broker_id: 4,
                    target_voters: vec![2, 3, 4],
                    preferred_leader: None,
                },
            )
            .unwrap();
        index += 1;
        let before = state.clone();
        assert!(
            state
                .apply(
                    index,
                    &ControllerCommand::AdvanceMovement {
                        movement_id,
                        expected_stage: MovementStage::Planned,
                        new_stage: MovementStage::PromotionReady,
                    },
                )
                .is_err()
        );
        assert_eq!(state, before);
    }

    #[test]
    fn producer_id_overflow_is_fatal_to_command_but_atomic_to_state() {
        let mut state = ControllerState::new([1; 16], 1, u64::MAX - 1).unwrap();
        let before = state.clone();
        assert!(
            state
                .apply(
                    1,
                    &ControllerCommand::AllocateProducerIds {
                        allocation_count: 2,
                    },
                )
                .is_err()
        );
        assert_eq!(state, before);
    }

    #[test]
    fn p7_producer_session_initialize_and_epoch_bump_are_atomic_and_fenced() {
        let mut state = ControllerState::new([1; 16], P7_PRODUCER_FEATURE_LEVEL, 100).unwrap();
        let producer_id = ProducerId::new(100).unwrap();
        let epoch_one = ProducerEpoch::new(1).unwrap();
        let initialized = state
            .apply_producer_session(
                1,
                ProducerSessionCommandV2::InitializeSession {
                    producer_id,
                    producer_epoch: epoch_one,
                },
            )
            .unwrap();
        assert_eq!(initialized.metadata_epoch, 1);
        assert_eq!(initialized.producer_ids, None);
        assert_eq!(state.next_unallocated_producer_id(), 101);
        assert_eq!(
            state.producer_authority(producer_id),
            Some(ProducerAuthority::new(producer_id, epoch_one))
        );

        let epoch_two = epoch_one.checked_next().unwrap();
        state
            .apply_producer_session(
                2,
                ProducerSessionCommandV2::BumpEpoch {
                    producer_id,
                    expected_epoch: epoch_one,
                    new_epoch: epoch_two,
                },
            )
            .unwrap();
        assert_eq!(
            state.producer_authority(producer_id),
            Some(ProducerAuthority::new(producer_id, epoch_two))
        );

        let before = state.clone();
        assert!(
            state
                .apply_producer_session(
                    3,
                    ProducerSessionCommandV2::BumpEpoch {
                        producer_id,
                        expected_epoch: epoch_one,
                        new_epoch: epoch_two,
                    },
                )
                .is_err()
        );
        assert_eq!(state, before);
    }

    #[test]
    fn p7_producer_session_feature_unknown_and_overflow_fail_without_mutation() {
        let producer_id = ProducerId::new(7).unwrap();
        let epoch_one = ProducerEpoch::new(1).unwrap();

        let mut feature_old = ControllerState::new([1; 16], 2, 7).unwrap();
        let before = feature_old.clone();
        assert!(
            feature_old
                .apply_producer_session(
                    1,
                    ProducerSessionCommandV2::InitializeSession {
                        producer_id,
                        producer_epoch: epoch_one,
                    },
                )
                .is_err()
        );
        assert_eq!(feature_old, before);

        let mut state = ControllerState::new([1; 16], P7_PRODUCER_FEATURE_LEVEL, 100).unwrap();
        let before = state.clone();
        assert!(
            state
                .apply_producer_session(
                    1,
                    ProducerSessionCommandV2::BumpEpoch {
                        producer_id,
                        expected_epoch: epoch_one,
                        new_epoch: epoch_one.checked_next().unwrap(),
                    },
                )
                .is_err()
        );
        assert_eq!(state, before);

        let max_epoch = ProducerEpoch::new(u32::MAX).unwrap();
        state.producer_sessions.insert(producer_id, max_epoch);
        state.next_unallocated_producer_id = producer_id.get() + 1;
        let before = state.clone();
        assert!(max_epoch.checked_next().is_none());
        assert!(
            encode_producer_session_command_v2(ProducerSessionCommandV2::BumpEpoch {
                producer_id,
                expected_epoch: max_epoch,
                new_epoch: epoch_one,
            })
            .is_err()
        );
        assert_eq!(state, before);
    }

    #[test]
    fn p7_producer_session_and_snapshot_bounds_fail_closed_before_mutation() {
        assert!(ensure_producer_session_capacity(MAX_PRODUCER_SESSIONS - 1, 1).is_ok());
        assert!(ensure_producer_session_capacity(MAX_PRODUCER_SESSIONS, 1).is_err());

        let max = crate::MAX_CONTROLLER_SNAPSHOT_BYTES;
        assert!(ensure_producer_snapshot_budget_with_base(40, 1, max).is_ok());
        assert!(
            ensure_producer_snapshot_budget_with_base(max, 1, max).is_err(),
            "V2 envelope/session bytes must be admitted before mutation"
        );
    }
}
