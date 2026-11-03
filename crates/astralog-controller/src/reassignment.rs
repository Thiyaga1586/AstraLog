use astralog_common::TopicId;

use crate::{
    ApplyOutcome, ControllerCommand, ControllerError, ControllerState, MovementId, MovementRecord,
    Result,
};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ReassignmentSpec {
    pub topic_id: TopicId,
    pub partition_id: u32,
    pub source_broker_id: u32,
    pub destination_broker_id: u32,
    pub preferred_leader: Option<u32>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ReassignmentResult {
    pub outcome: ApplyOutcome,
    pub movement: MovementRecord,
}

#[derive(Debug, Clone)]
pub(crate) struct PreparedReassignment {
    pub movement_id: MovementId,
    pub command: ControllerCommand,
}

pub(crate) fn prepare_reassignment(
    state: &ControllerState,
    movement_id: MovementId,
    spec: ReassignmentSpec,
) -> Result<PreparedReassignment> {
    if spec.topic_id.as_bytes() == &[0; 16]
        || spec.source_broker_id == spec.destination_broker_id
        || spec.source_broker_id == 0
        || spec.destination_broker_id == 0
        || spec.source_broker_id == u32::MAX
        || spec.destination_broker_id == u32::MAX
    {
        return Err(ControllerError::Invalid(
            "invalid reassignment source/destination identity",
        ));
    }
    let assignment = state
        .assignments()
        .get(&(spec.topic_id, spec.partition_id))
        .ok_or(ControllerError::Invalid("unknown reassignment partition"))?;
    if !assignment.learners.is_empty() {
        return Err(ControllerError::Invalid(
            "reassignment requires a stable assignment without controller learners",
        ));
    }
    if !assignment.voters.contains(&spec.source_broker_id)
        || assignment.voters.contains(&spec.destination_broker_id)
        || assignment.learners.contains(&spec.destination_broker_id)
    {
        return Err(ControllerError::Invalid(
            "reassignment source/destination does not match current assignment",
        ));
    }
    if state.movements().values().any(|movement| {
        movement.topic_id == spec.topic_id
            && movement.partition_id == spec.partition_id
            && movement.stage != crate::MovementStage::Complete
    }) {
        return Err(ControllerError::Invalid(
            "partition already has an active movement",
        ));
    }
    let mut target_voters = assignment
        .voters
        .iter()
        .copied()
        .filter(|broker| *broker != spec.source_broker_id)
        .chain(core::iter::once(spec.destination_broker_id))
        .collect::<Vec<_>>();
    target_voters.sort_unstable();
    target_voters.dedup();
    if target_voters.len() != assignment.voters.len()
        || spec
            .preferred_leader
            .is_some_and(|leader| !target_voters.contains(&leader))
    {
        return Err(ControllerError::Invalid(
            "reassignment target voter set is invalid",
        ));
    }

    Ok(PreparedReassignment {
        movement_id,
        command: ControllerCommand::BeginMovement {
            movement_id,
            topic_id: spec.topic_id,
            partition_id: spec.partition_id,
            expected_assignment_generation: assignment.assignment_generation,
            source_broker_id: spec.source_broker_id,
            destination_broker_id: spec.destination_broker_id,
            target_voters,
            preferred_leader: spec.preferred_leader,
        },
    })
}

pub(crate) fn validate_reassignment_capacity(
    state: &ControllerState,
    spec: ReassignmentSpec,
    max_active_movements: usize,
    max_active_movements_per_broker: usize,
) -> Result<()> {
    if max_active_movements == 0 || max_active_movements_per_broker == 0 {
        return Err(ControllerError::Invalid(
            "movement concurrency limits must be non-zero",
        ));
    }
    let active = state
        .movements()
        .values()
        .filter(|movement| movement.stage != crate::MovementStage::Complete)
        .collect::<Vec<_>>();
    if active.len() >= max_active_movements {
        return Err(ControllerError::LimitExceeded {
            what: "active replica movements",
            actual: active.len().saturating_add(1),
            max: max_active_movements,
        });
    }
    for broker_id in [spec.source_broker_id, spec.destination_broker_id] {
        let count = active
            .iter()
            .filter(|movement| {
                movement.source_broker_id == broker_id
                    || movement.destination_broker_id == broker_id
            })
            .count();
        if count >= max_active_movements_per_broker {
            return Err(ControllerError::LimitExceeded {
                what: "active replica movements per broker",
                actual: count.saturating_add(1),
                max: max_active_movements_per_broker,
            });
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use crate::{
        AssignmentGeneration, AssignmentRecord, BrokerLifecycle, ControllerCommand,
        FailureDomainPolicy, MovementId,
    };

    use super::*;

    fn seeded_state() -> (ControllerState, TopicId) {
        let topic_id = TopicId::new([4; 16]);
        let mut state = ControllerState::new([1; 16], 1, 1).unwrap();
        let mut epoch = 1_u64;
        for broker_id in 1..=5_u32 {
            state
                .apply(
                    epoch,
                    &ControllerCommand::RegisterBroker {
                        broker_id,
                        node_uuid: [u8::try_from(broker_id).unwrap(); 16],
                        initial_incarnation: 1,
                        client_host: format!("b{broker_id}"),
                        client_port: 9000 + u16::try_from(broker_id).unwrap(),
                        internal_host: format!("b{broker_id}"),
                        internal_port: 10000 + u16::try_from(broker_id).unwrap(),
                        rack: None,
                        zone: None,
                        min_feature: 1,
                        max_feature: 1,
                    },
                )
                .unwrap();
            epoch += 1;
            state
                .apply(
                    epoch,
                    &ControllerCommand::SetBrokerLifecycle {
                        broker_id,
                        expected_incarnation: 1,
                        expected_lifecycle: BrokerLifecycle::Joining,
                        new_lifecycle: BrokerLifecycle::Active,
                    },
                )
                .unwrap();
            epoch += 1;
        }
        state
            .apply(
                epoch,
                &ControllerCommand::CreateTopic {
                    topic_id,
                    topic_name: "capacity".into(),
                    partition_count: 2,
                    replication_factor: 3,
                    failure_domain_policy: FailureDomainPolicy::Disabled,
                    assignments: vec![
                        AssignmentRecord {
                            topic_id,
                            partition_id: 0,
                            assignment_generation: AssignmentGeneration::new(1).unwrap(),
                            voters: vec![1, 2, 3],
                            learners: Vec::new(),
                        },
                        AssignmentRecord {
                            topic_id,
                            partition_id: 1,
                            assignment_generation: AssignmentGeneration::new(1).unwrap(),
                            voters: vec![1, 2, 3],
                            learners: Vec::new(),
                        },
                    ],
                },
            )
            .unwrap();
        epoch += 1;
        state
            .apply(
                epoch,
                &ControllerCommand::BeginMovement {
                    movement_id: MovementId::new([8; 16]).unwrap(),
                    topic_id,
                    partition_id: 0,
                    expected_assignment_generation: AssignmentGeneration::new(1).unwrap(),
                    source_broker_id: 1,
                    destination_broker_id: 4,
                    target_voters: vec![2, 3, 4],
                    preferred_leader: None,
                },
            )
            .unwrap();
        (state, topic_id)
    }

    #[test]
    fn global_and_per_broker_movement_bounds_fail_closed() {
        let (state, topic_id) = seeded_state();
        let spec = ReassignmentSpec {
            topic_id,
            partition_id: 1,
            source_broker_id: 1,
            destination_broker_id: 5,
            preferred_leader: None,
        };
        assert!(matches!(
            validate_reassignment_capacity(&state, spec, 1, 8),
            Err(ControllerError::LimitExceeded {
                what: "active replica movements",
                ..
            })
        ));
        assert!(matches!(
            validate_reassignment_capacity(&state, spec, 8, 1),
            Err(ControllerError::LimitExceeded {
                what: "active replica movements per broker",
                ..
            })
        ));
        validate_reassignment_capacity(&state, spec, 8, 2).unwrap();
    }
}
