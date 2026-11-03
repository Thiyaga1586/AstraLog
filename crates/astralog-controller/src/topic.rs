use astralog_common::TopicId;
use astralog_groups::{
    P6_INTERNAL_TOPIC_PARTITIONS, P6_INTERNAL_TOPIC_REPLICATION_FACTOR,
    is_reserved_internal_topic_name,
};
use astralog_protocol::api::validate_topic_name;

use crate::{
    ApplyOutcome, AssignmentRecord, ControllerError, ControllerState, FailureDomainPolicy,
    MAX_TOPIC_PARTITIONS, PlacementInputSnapshot, PlacementPlan, Result, TopicLifecycle,
    TopicRecord, plan_assignments,
};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CreateTopicSpec {
    pub topic_name: String,
    pub partition_count: u32,
    pub replication_factor: u16,
    pub failure_domain_policy: FailureDomainPolicy,
}

impl CreateTopicSpec {
    pub(crate) fn validate(&self) -> Result<()> {
        validate_controller_topic_name_and_topology(
            &self.topic_name,
            self.partition_count,
            self.replication_factor,
            self.failure_domain_policy,
        )?;
        validate_partition_count(self.partition_count)?;
        validate_replication_factor(self.replication_factor)
    }
}

pub(crate) fn validate_controller_topic_name_and_topology(
    topic_name: &str,
    partition_count: u32,
    replication_factor: u16,
    failure_domain_policy: FailureDomainPolicy,
) -> Result<()> {
    if is_reserved_internal_topic_name(topic_name) {
        if partition_count != P6_INTERNAL_TOPIC_PARTITIONS
            || replication_factor != P6_INTERNAL_TOPIC_REPLICATION_FACTOR
            || failure_domain_policy != FailureDomainPolicy::Preferred
        {
            return Err(ControllerError::Invalid(
                "reserved P6 internal topic must use frozen 64/RF3/Preferred topology",
            ));
        }
        return Ok(());
    }
    validate_topic_name(topic_name).map_err(|_| ControllerError::Invalid("invalid topic name"))
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TopicExpansionSpec {
    pub topic_id: TopicId,
    pub expected_partition_count: u32,
    pub new_partition_count: u32,
}

impl TopicExpansionSpec {
    pub(crate) fn validate(&self) -> Result<()> {
        if *self.topic_id.as_bytes() == [0; 16] {
            return Err(ControllerError::Invalid("topic id zero is reserved"));
        }
        validate_partition_count(self.expected_partition_count)?;
        validate_partition_count(self.new_partition_count)?;
        if self.new_partition_count <= self.expected_partition_count {
            return Err(ControllerError::Invalid(
                "topic expansion must strictly increase partition count",
            ));
        }
        Ok(())
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PartitioningEpochChange {
    pub previous_partition_count: u32,
    pub new_partition_count: u32,
    pub keyed_partition_choice_may_change: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TopicCreateResult {
    pub outcome: ApplyOutcome,
    pub topic: TopicRecord,
    pub assignments: Vec<AssignmentRecord>,
    pub degraded_failure_domain: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TopicExpansionResult {
    pub outcome: ApplyOutcome,
    pub topic_id: TopicId,
    pub assignments: Vec<AssignmentRecord>,
    pub degraded_failure_domain: bool,
    pub partitioning_epoch_change: PartitioningEpochChange,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct TopicDeletionResult {
    pub outcome: ApplyOutcome,
    pub topic_id: TopicId,
    pub lifecycle: Option<TopicLifecycle>,
}

#[derive(Debug, Clone)]
pub(crate) struct PreparedTopicCreate {
    pub topic: TopicRecord,
    pub plan: PlacementPlan,
}

#[derive(Debug, Clone)]
pub(crate) struct PreparedTopicExpansion {
    pub topic_id: TopicId,
    pub plan: PlacementPlan,
    pub partitioning_epoch_change: PartitioningEpochChange,
}

pub(crate) fn prepare_create(
    state: &ControllerState,
    liveness: &[crate::BrokerLivenessObservation],
    topic_id: TopicId,
    spec: &CreateTopicSpec,
) -> Result<PreparedTopicCreate> {
    spec.validate()?;
    if *topic_id.as_bytes() == [0; 16] || state.topics().contains_key(&topic_id) {
        return Err(ControllerError::Invalid(
            "topic id is zero or already exists",
        ));
    }
    if state.topic_by_name(&spec.topic_name).is_some() {
        return Err(ControllerError::Invalid("topic name already exists"));
    }
    let snapshot = PlacementInputSnapshot::from_controller_state(state, topic_id, liveness)?;
    let plan = plan_assignments(
        &snapshot,
        0,
        spec.partition_count,
        spec.replication_factor,
        spec.failure_domain_policy,
    )?;
    Ok(PreparedTopicCreate {
        topic: TopicRecord {
            topic_id,
            topic_name: spec.topic_name.clone(),
            partition_count: spec.partition_count,
            replication_factor: spec.replication_factor,
            lifecycle: TopicLifecycle::Active,
            failure_domain_policy: spec.failure_domain_policy,
        },
        plan,
    })
}

pub(crate) fn prepare_expansion(
    state: &ControllerState,
    liveness: &[crate::BrokerLivenessObservation],
    spec: &TopicExpansionSpec,
) -> Result<PreparedTopicExpansion> {
    spec.validate()?;
    let topic = state
        .topics()
        .get(&spec.topic_id)
        .ok_or(ControllerError::Invalid("unknown topic for expansion"))?;
    if topic.lifecycle != TopicLifecycle::Active
        || topic.partition_count != spec.expected_partition_count
    {
        return Err(ControllerError::Invalid(
            "stale topic state for partition expansion",
        ));
    }
    let snapshot = PlacementInputSnapshot::from_controller_state(state, spec.topic_id, liveness)?;
    let plan = plan_assignments(
        &snapshot,
        spec.expected_partition_count,
        spec.new_partition_count,
        topic.replication_factor,
        topic.failure_domain_policy,
    )?;
    Ok(PreparedTopicExpansion {
        topic_id: spec.topic_id,
        plan,
        partitioning_epoch_change: PartitioningEpochChange {
            previous_partition_count: spec.expected_partition_count,
            new_partition_count: spec.new_partition_count,
            keyed_partition_choice_may_change: true,
        },
    })
}

pub(crate) fn validate_partition_count(partition_count: u32) -> Result<()> {
    if partition_count == 0 || partition_count > MAX_TOPIC_PARTITIONS {
        return Err(ControllerError::InvalidOwned(format!(
            "partition count must be in 1..={MAX_TOPIC_PARTITIONS}"
        )));
    }
    Ok(())
}

pub(crate) fn validate_replication_factor(replication_factor: u16) -> Result<()> {
    if !matches!(replication_factor, 1 | 3 | 5) {
        return Err(ControllerError::Invalid(
            "replication factor must be exactly 1, 3, or 5",
        ));
    }
    Ok(())
}
