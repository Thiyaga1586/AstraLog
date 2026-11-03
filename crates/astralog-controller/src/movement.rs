use std::collections::BTreeMap;

use astralog_common::TopicId;

use crate::{
    AssignmentGeneration, BrokerAuthority, BrokerHeartbeatRequest, ControllerError,
    ControllerState, MovementId, MovementProgressReport, MovementStage, ReplicaProgressReport,
    Result,
};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ReplicaProgressObservation {
    pub authority: BrokerAuthority,
    pub report: ReplicaProgressReport,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct MovementProgressObservation {
    pub authority: BrokerAuthority,
    pub report: MovementProgressReport,
}

#[derive(Debug, Clone, Default)]
pub(crate) struct MovementObservations {
    replicas: BTreeMap<(u32, TopicId, u32), ReplicaProgressObservation>,
    movements: BTreeMap<(u32, MovementId), MovementProgressObservation>,
}

impl MovementObservations {
    pub(crate) fn reset_for_leadership_change(&mut self) {
        self.replicas.clear();
        self.movements.clear();
    }

    pub(crate) fn observe_heartbeat(
        &mut self,
        state: &ControllerState,
        request: &BrokerHeartbeatRequest,
    ) -> Result<()> {
        for report in &request.replica_progress {
            validate_replica_report(state, request.authority, *report)?;
        }
        for report in &request.movement_progress {
            validate_movement_report(state, request.authority, *report)?;
        }
        for report in &request.replica_progress {
            self.replicas.insert(
                (
                    request.authority.broker_id,
                    report.topic_id,
                    report.partition_id,
                ),
                ReplicaProgressObservation {
                    authority: request.authority,
                    report: *report,
                },
            );
        }
        for report in &request.movement_progress {
            self.movements.insert(
                (request.authority.broker_id, report.movement_id),
                MovementProgressObservation {
                    authority: request.authority,
                    report: *report,
                },
            );
        }
        Ok(())
    }

    pub(crate) fn replica_progress(
        &self,
        state: &ControllerState,
        broker_id: u32,
        topic_id: TopicId,
        partition_id: u32,
        generation: AssignmentGeneration,
    ) -> Option<ReplicaProgressObservation> {
        let current = state.broker(broker_id)?;
        let observation = *self.replicas.get(&(broker_id, topic_id, partition_id))?;
        (observation.authority.node_uuid == current.node_uuid
            && observation.authority.incarnation_epoch == current.incarnation_epoch
            && observation.report.assignment_generation == generation)
            .then_some(observation)
    }

    #[cfg(test)]
    pub(crate) fn movement_progress(
        &self,
        state: &ControllerState,
        broker_id: u32,
        movement_id: MovementId,
    ) -> Option<MovementProgressObservation> {
        let current = state.broker(broker_id)?;
        let observation = *self.movements.get(&(broker_id, movement_id))?;
        (observation.authority.node_uuid == current.node_uuid
            && observation.authority.incarnation_epoch == current.incarnation_epoch)
            .then_some(observation)
    }
}

fn validate_replica_report(
    state: &ControllerState,
    authority: BrokerAuthority,
    report: ReplicaProgressReport,
) -> Result<()> {
    let assignment = state
        .assignments()
        .get(&(report.topic_id, report.partition_id))
        .ok_or(ControllerError::Invalid(
            "replica progress references unknown assignment",
        ))?;
    if report.assignment_generation != assignment.assignment_generation {
        return Err(ControllerError::Invalid(
            "replica progress has stale assignment generation",
        ));
    }
    let assigned = assignment.voters.contains(&authority.broker_id)
        || assignment.learners.contains(&authority.broker_id);
    let movement_destination = state.movements().values().any(|movement| {
        movement.stage != MovementStage::Complete
            && movement.topic_id == report.topic_id
            && movement.partition_id == report.partition_id
            && movement.expected_assignment_generation == report.assignment_generation
            && movement.destination_broker_id == authority.broker_id
    });
    if !assigned && !movement_destination {
        return Err(ControllerError::Invalid(
            "replica progress broker is not assigned to the partition movement",
        ));
    }
    Ok(())
}

fn validate_movement_report(
    state: &ControllerState,
    authority: BrokerAuthority,
    report: MovementProgressReport,
) -> Result<()> {
    let movement = state
        .movements()
        .get(&report.movement_id)
        .ok_or(ControllerError::Invalid(
            "movement progress references unknown movement",
        ))?;
    if movement.topic_id != report.topic_id
        || movement.partition_id != report.partition_id
        || movement.expected_assignment_generation != report.assignment_generation
    {
        return Err(ControllerError::Invalid(
            "movement progress identity or generation is stale",
        ));
    }
    if authority.broker_id != movement.source_broker_id
        && authority.broker_id != movement.destination_broker_id
        && !movement.target_voters.contains(&authority.broker_id)
    {
        return Err(ControllerError::Invalid(
            "movement progress broker is unrelated to committed movement",
        ));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use crate::{AssignmentRecord, BrokerLifecycle, ControllerCommand, FailureDomainPolicy};

    use super::*;

    fn seeded_state() -> (ControllerState, TopicId, MovementId) {
        let topic = TopicId::new([7; 16]);
        let movement = MovementId::new([8; 16]).unwrap();
        let mut state = ControllerState::new([1; 16], 1, 1).unwrap();
        let mut epoch = 1_u64;
        for broker_id in 1..=4 {
            state
                .apply(
                    epoch,
                    &ControllerCommand::RegisterBroker {
                        broker_id,
                        node_uuid: [u8::try_from(broker_id).unwrap(); 16],
                        initial_incarnation: 1,
                        client_host: format!("broker-{broker_id}"),
                        client_port: 9000 + u16::try_from(broker_id).unwrap(),
                        internal_host: format!("broker-{broker_id}"),
                        internal_port: 10000 + u16::try_from(broker_id).unwrap(),
                        rack: Some(format!("r{broker_id}")),
                        zone: Some(format!("z{broker_id}")),
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
                    topic_id: topic,
                    topic_name: "movement-observation".into(),
                    partition_count: 1,
                    replication_factor: 3,
                    failure_domain_policy: FailureDomainPolicy::Disabled,
                    assignments: vec![AssignmentRecord {
                        topic_id: topic,
                        partition_id: 0,
                        assignment_generation: AssignmentGeneration::new(1).unwrap(),
                        voters: vec![1, 2, 3],
                        learners: Vec::new(),
                    }],
                },
            )
            .unwrap();
        epoch += 1;
        state
            .apply(
                epoch,
                &ControllerCommand::BeginMovement {
                    movement_id: movement,
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
        (state, topic, movement)
    }

    #[test]
    fn progress_is_fenced_by_incarnation_generation_and_movement_identity() {
        let (state, topic, movement_id) = seeded_state();
        let authority = BrokerAuthority {
            broker_id: 4,
            node_uuid: [4; 16],
            incarnation_epoch: 1,
        };
        let request = BrokerHeartbeatRequest {
            authority,
            applied_metadata_epoch: state.metadata_epoch(),
            min_feature: 1,
            max_feature: 1,
            replica_progress: vec![ReplicaProgressReport {
                topic_id: topic,
                partition_id: 0,
                assignment_generation: AssignmentGeneration::new(1).unwrap(),
                durable_raft_index: 9,
                committed_raft_index: 8,
            }],
            movement_progress: vec![MovementProgressReport {
                movement_id,
                topic_id: topic,
                partition_id: 0,
                assignment_generation: AssignmentGeneration::new(1).unwrap(),
                observed_stage: MovementStage::CatchingUp,
            }],
        };
        let mut observations = MovementObservations::default();
        observations.observe_heartbeat(&state, &request).unwrap();
        assert_eq!(
            observations
                .replica_progress(&state, 4, topic, 0, AssignmentGeneration::new(1).unwrap())
                .unwrap()
                .report
                .durable_raft_index,
            9
        );
        assert!(
            observations
                .movement_progress(&state, 4, movement_id)
                .is_some()
        );

        let mut stale_generation = request.clone();
        stale_generation.replica_progress[0].assignment_generation =
            AssignmentGeneration::new(2).unwrap();
        assert!(
            observations
                .observe_heartbeat(&state, &stale_generation)
                .is_err()
        );

        let mut stale_movement = request;
        stale_movement.movement_progress[0].movement_id = MovementId::new([9; 16]).unwrap();
        assert!(
            observations
                .observe_heartbeat(&state, &stale_movement)
                .is_err()
        );

        let mut rejoined = state.clone();
        let next_epoch = rejoined.metadata_epoch() + 1;
        rejoined
            .apply(
                next_epoch,
                &ControllerCommand::RejoinBroker {
                    broker_id: 4,
                    node_uuid: [4; 16],
                    expected_incarnation: 1,
                    new_incarnation: 2,
                    client_host: "broker-4".into(),
                    client_port: 9004,
                    internal_host: "broker-4".into(),
                    internal_port: 10004,
                    rack: Some("r4".into()),
                    zone: Some("z4".into()),
                    min_feature: 1,
                    max_feature: 1,
                },
            )
            .unwrap();
        assert!(
            observations
                .replica_progress(
                    &rejoined,
                    4,
                    topic,
                    0,
                    AssignmentGeneration::new(1).unwrap()
                )
                .is_none(),
            "progress from the superseded broker incarnation must lose authority immediately"
        );
    }
}
