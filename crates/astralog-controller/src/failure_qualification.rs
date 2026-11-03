use std::collections::{BTreeMap, BTreeSet};

use astralog_common::TopicId;

use crate::{
    AssignmentGeneration, AssignmentRecord, BrokerAuthority, BrokerLifecycle,
    BrokerLivenessObservation, BrokerLivenessState, CommandLimits, ControllerCommand,
    ControllerError, ControllerSnapshot, ControllerState, FailureDomainPolicy,
    MAX_CONTROLLER_ASSIGNMENTS, MAX_CONTROLLER_BROKERS, MAX_CONTROLLER_MOVEMENTS,
    MAX_CONTROLLER_SNAPSHOT_BYTES, MAX_CONTROLLER_TOPICS, MAX_METADATA_DELTA_RESPONSE_BYTES,
    MAX_METADATA_DELTAS_PER_RESPONSE, MAX_RETAINED_METADATA_BYTES, MAX_RETAINED_METADATA_DELTAS,
    MovementId, MovementStage, PlacementBrokerSnapshot, PlacementInputSnapshot, SnapshotLimits,
    decode_controller_snapshot, encode_command, encode_controller_snapshot, plan_assignments,
};

#[derive(Debug, Clone)]
struct DeterministicControllerSim {
    state: ControllerState,
    next_epoch: u64,
    trace: Vec<String>,
}

impl DeterministicControllerSim {
    fn new() -> Self {
        Self {
            state: ControllerState::new([0x5a; 16], 1, 10_000).unwrap(),
            next_epoch: 1,
            trace: Vec::new(),
        }
    }

    fn apply(&mut self, command: ControllerCommand) -> crate::ApplyOutcome {
        let epoch = self.next_epoch;
        let canonical = encode_command(&command, CommandLimits::default()).unwrap();
        let outcome = self.state.apply(epoch, &command).unwrap();
        self.trace.push(format!(
            "{epoch}:{:08x}:{}",
            crc32c::crc32c(&canonical),
            canonical.len()
        ));
        self.next_epoch += 1;
        outcome
    }

    fn reject(&mut self, command: ControllerCommand) -> ControllerError {
        let before = self.state.clone();
        let epoch = self.next_epoch;
        let error = self.state.apply(epoch, &command).unwrap_err();
        assert_eq!(
            self.state, before,
            "rejected command mutated controller state"
        );
        assert_eq!(self.state.metadata_epoch(), epoch - 1);
        error
    }
}

#[derive(Debug, Clone, Copy)]
struct SeededRng(u64);

impl SeededRng {
    fn new(seed: u64) -> Self {
        Self(seed.max(1))
    }

    fn next_u64(&mut self) -> u64 {
        let mut value = self.0;
        value ^= value << 13;
        value ^= value >> 7;
        value ^= value << 17;
        self.0 = value;
        value
    }

    fn index(&mut self, len: usize) -> usize {
        usize::try_from(self.next_u64() % u64::try_from(len).unwrap()).unwrap()
    }

    fn shuffle<T>(&mut self, values: &mut [T]) {
        for upper in (1..values.len()).rev() {
            let index = self.index(upper + 1);
            values.swap(index, upper);
        }
    }
}

fn register_active_brokers(sim: &mut DeterministicControllerSim, count: u32) {
    register_active_brokers_from(sim, 1, count);
}

fn register_active_brokers_from(sim: &mut DeterministicControllerSim, first: u32, last: u32) {
    for broker_id in first..=last {
        sim.apply(ControllerCommand::RegisterBroker {
            broker_id,
            node_uuid: [u8::try_from(broker_id).unwrap(); 16],
            initial_incarnation: 1,
            client_host: format!("broker-{broker_id}"),
            client_port: 9_000 + u16::try_from(broker_id).unwrap(),
            internal_host: format!("raft-{broker_id}"),
            internal_port: 10_000 + u16::try_from(broker_id).unwrap(),
            rack: Some(format!("rack-{broker_id}")),
            zone: Some(format!("zone-{broker_id}")),
            min_feature: 1,
            max_feature: 3,
        });
        sim.apply(ControllerCommand::SetBrokerLifecycle {
            broker_id,
            expected_incarnation: 1,
            expected_lifecycle: BrokerLifecycle::Joining,
            new_lifecycle: BrokerLifecycle::Active,
        });
    }
}

fn placement_broker(
    broker_id: u32,
    rack: Option<&str>,
    zone: Option<&str>,
) -> PlacementBrokerSnapshot {
    PlacementBrokerSnapshot {
        broker_id,
        rack: rack.map(str::to_owned),
        zone: zone.map(str::to_owned),
        lifecycle: BrokerLifecycle::Active,
        min_feature: 1,
        max_feature: 3,
        liveness: BrokerLivenessState::Healthy,
        current_replica_count: 0,
        topic_replica_count: 0,
    }
}

fn healthy_liveness(state: &ControllerState) -> Vec<BrokerLivenessObservation> {
    state
        .brokers()
        .values()
        .map(|broker| BrokerLivenessObservation {
            authority: BrokerAuthority {
                broker_id: broker.broker_id,
                node_uuid: broker.node_uuid,
                incarnation_epoch: broker.incarnation_epoch,
            },
            state: BrokerLivenessState::Healthy,
            last_seen_tick: Some(100),
            elapsed_ticks: Some(0),
        })
        .collect()
}

fn create_planned_topic(
    sim: &mut DeterministicControllerSim,
    topic_id: TopicId,
    topic_name: String,
    partitions: u32,
    policy: FailureDomainPolicy,
) -> Vec<AssignmentRecord> {
    let input = PlacementInputSnapshot::from_controller_state(
        &sim.state,
        topic_id,
        &healthy_liveness(&sim.state),
    )
    .unwrap();
    let plan = plan_assignments(&input, 0, partitions, 3, policy).unwrap();
    let assignments = plan.assignments.clone();
    sim.apply(ControllerCommand::CreateTopic {
        topic_id,
        topic_name,
        partition_count: partitions,
        replication_factor: 3,
        failure_domain_policy: policy,
        assignments: plan.assignments,
    });
    assignments
}

fn assert_core_invariants(state: &ControllerState) {
    let mut uuids = BTreeSet::new();
    for (broker_id, broker) in state.brokers() {
        assert_eq!(*broker_id, broker.broker_id);
        assert!(broker.incarnation_epoch > 0);
        assert!(uuids.insert(broker.node_uuid));
    }
    for ((topic_id, partition_id), assignment) in state.assignments() {
        assert_eq!(*topic_id, assignment.topic_id);
        assert_eq!(*partition_id, assignment.partition_id);
        assert!(assignment.assignment_generation.get() > 0);
        assert!(
            assignment
                .voters
                .windows(2)
                .all(|window| window[0] < window[1])
        );
        assert!(
            assignment
                .learners
                .windows(2)
                .all(|window| window[0] < window[1])
        );
        assert!(
            assignment
                .voters
                .iter()
                .all(|broker| !assignment.learners.contains(broker))
        );
        assert!(
            assignment
                .voters
                .iter()
                .chain(&assignment.learners)
                .all(|broker| state.brokers().contains_key(broker))
        );
    }
}

fn run_seeded_controller_campaign(seed: u64) -> DeterministicControllerSim {
    let mut sim = DeterministicControllerSim::new();
    register_active_brokers(&mut sim, 9);
    for topic_number in 1..=4_u8 {
        create_planned_topic(
            &mut sim,
            TopicId::new([topic_number; 16]),
            format!("seeded-{topic_number}"),
            8,
            FailureDomainPolicy::Required,
        );
    }

    let mut rng = SeededRng::new(seed);
    let mut last_incarnations = sim
        .state
        .brokers()
        .iter()
        .map(|(id, broker)| (*id, broker.incarnation_epoch))
        .collect::<BTreeMap<_, _>>();
    let mut last_producer_id = None;

    for step in 0..96_u32 {
        match rng.next_u64() % 3 {
            0 => {
                let broker_id = u32::try_from(rng.index(9) + 1).unwrap();
                let broker = sim.state.brokers()[&broker_id].clone();
                let new_incarnation = broker.incarnation_epoch.checked_add(1).unwrap();
                sim.apply(ControllerCommand::RejoinBroker {
                    broker_id,
                    node_uuid: broker.node_uuid,
                    expected_incarnation: broker.incarnation_epoch,
                    new_incarnation,
                    client_host: broker.client_host,
                    client_port: broker.client_port,
                    internal_host: broker.internal_host,
                    internal_port: broker.internal_port,
                    rack: broker.rack,
                    zone: broker.zone,
                    min_feature: broker.min_feature,
                    max_feature: broker.max_feature,
                });
                assert!(new_incarnation > last_incarnations[&broker_id]);
                last_incarnations.insert(broker_id, new_incarnation);
            }
            1 => {
                let count = u32::try_from((rng.next_u64() % 32) + 1).unwrap();
                let range = sim
                    .apply(ControllerCommand::AllocateProducerIds {
                        allocation_count: count,
                    })
                    .producer_ids
                    .unwrap();
                if let Some(previous) = last_producer_id {
                    assert!(range.first > previous);
                }
                last_producer_id = Some(range.last);
            }
            _ => {
                let broker_id = u32::try_from(rng.index(9) + 1).unwrap();
                let broker = sim.state.brokers()[&broker_id].clone();
                let before = sim.state.clone();
                let error = sim.reject(ControllerCommand::RejoinBroker {
                    broker_id,
                    node_uuid: broker.node_uuid,
                    expected_incarnation: broker.incarnation_epoch.saturating_sub(1),
                    new_incarnation: broker.incarnation_epoch,
                    client_host: broker.client_host,
                    client_port: broker.client_port,
                    internal_host: broker.internal_host,
                    internal_port: broker.internal_port,
                    rack: broker.rack,
                    zone: broker.zone,
                    min_feature: broker.min_feature,
                    max_feature: broker.max_feature,
                });
                assert!(matches!(error, ControllerError::Invalid(_)));
                assert_eq!(sim.state, before);
            }
        }
        assert_core_invariants(&sim.state);
        assert_eq!(sim.state.metadata_epoch(), sim.next_epoch - 1);
        assert!(sim.state.brokers().len() <= MAX_CONTROLLER_BROKERS);
        assert!(sim.state.topics().len() <= MAX_CONTROLLER_TOPICS);
        assert!(sim.state.assignments().len() <= MAX_CONTROLLER_ASSIGNMENTS);
        assert!(sim.state.movements().len() <= MAX_CONTROLLER_MOVEMENTS);
        assert!(step < 96);
    }
    sim
}

fn movement_seed() -> (DeterministicControllerSim, TopicId, MovementId) {
    let mut sim = DeterministicControllerSim::new();
    register_active_brokers(&mut sim, 4);
    let topic_id = TopicId::new([0x44; 16]);
    sim.apply(ControllerCommand::CreateTopic {
        topic_id,
        topic_name: "movement-model".into(),
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
    });
    let movement_id = MovementId::new([0x55; 16]).unwrap();
    sim.apply(ControllerCommand::BeginMovement {
        movement_id,
        topic_id,
        partition_id: 0,
        expected_assignment_generation: AssignmentGeneration::new(1).unwrap(),
        source_broker_id: 1,
        destination_broker_id: 4,
        target_voters: vec![2, 3, 4],
        preferred_leader: Some(4),
    });
    (sim, topic_id, movement_id)
}

const MOVEMENT_PREFIX: &[(MovementStage, MovementStage)] = &[
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
        MovementStage::LeadershipTransfer,
    ),
    (
        MovementStage::LeadershipTransfer,
        MovementStage::OldReplicaRemoveRequested,
    ),
    (
        MovementStage::OldReplicaRemoveRequested,
        MovementStage::OldReplicaRemoved,
    ),
];

#[test]
fn deterministic_controller_state_machine_simulator_is_reproducible() {
    let left = run_seeded_controller_campaign(0x5eed_f00d_cafe_babe);
    let right = run_seeded_controller_campaign(0x5eed_f00d_cafe_babe);
    assert_eq!(left.state, right.state);
    assert_eq!(left.trace, right.trace);

    let mut different = run_seeded_controller_campaign(0x5eed_f00d_cafe_babf);
    assert_ne!(left.trace, different.trace);

    let duplicate_id = different.state.brokers()[&1].clone();
    let before = different.state.clone();
    assert!(
        different
            .reject(ControllerCommand::RegisterBroker {
                broker_id: 1,
                node_uuid: [0xaa; 16],
                initial_incarnation: 1,
                client_host: duplicate_id.client_host,
                client_port: duplicate_id.client_port,
                internal_host: duplicate_id.internal_host,
                internal_port: duplicate_id.internal_port,
                rack: duplicate_id.rack,
                zone: duplicate_id.zone,
                min_feature: 1,
                max_feature: 3,
            })
            .to_string()
            .contains("already registered")
    );
    assert_eq!(different.state, before);
}

#[test]
fn seeded_multi_topic_placement_campaign_is_exactly_repeatable() {
    let topic_id = TopicId::new([0x77; 16]);
    let baseline_brokers = (1..=12_u32)
        .map(|broker_id| PlacementBrokerSnapshot {
            broker_id,
            rack: Some(format!("rack-{broker_id}")),
            zone: Some(format!("zone-{broker_id}")),
            lifecycle: BrokerLifecycle::Active,
            min_feature: 1,
            max_feature: 3,
            liveness: BrokerLivenessState::Healthy,
            current_replica_count: u64::from(broker_id % 4),
            topic_replica_count: u64::from(broker_id % 3),
        })
        .collect::<Vec<_>>();
    let baseline = plan_assignments(
        &PlacementInputSnapshot {
            active_feature_level: 1,
            topic_id,
            brokers: baseline_brokers.clone(),
        },
        0,
        64,
        3,
        FailureDomainPolicy::Required,
    )
    .unwrap();

    let mut rng = SeededRng::new(0xdecaf_bad5eed);
    for _ in 0..128 {
        let mut reordered = baseline_brokers.clone();
        rng.shuffle(&mut reordered);
        let candidate = plan_assignments(
            &PlacementInputSnapshot {
                active_feature_level: 1,
                topic_id,
                brokers: reordered,
            },
            0,
            64,
            3,
            FailureDomainPolicy::Required,
        )
        .unwrap();
        assert_eq!(candidate, baseline);
    }
}

#[test]
fn bounded_movement_model_survives_restart_at_every_stage_and_never_skips_safety() {
    for crash_after in 0..=MOVEMENT_PREFIX.len() {
        let (mut sim, topic_id, movement_id) = movement_seed();
        for (index, (from, to)) in MOVEMENT_PREFIX.iter().copied().enumerate() {
            if index == crash_after {
                let bytes = encode_controller_snapshot(
                    &ControllerSnapshot::new(sim.state.clone()),
                    SnapshotLimits::default(),
                )
                .unwrap();
                sim.state = decode_controller_snapshot(&bytes, SnapshotLimits::default())
                    .unwrap()
                    .state;
                sim.next_epoch = sim.state.metadata_epoch() + 1;
            }

            let before = sim.state.clone();
            if matches!(
                from,
                MovementStage::Planned
                    | MovementStage::LearnerRequested
                    | MovementStage::LearnerCreated
            ) {
                assert!(
                    sim.state
                        .apply(
                            sim.next_epoch,
                            &ControllerCommand::AdvanceMovement {
                                movement_id,
                                expected_stage: from,
                                new_stage: MovementStage::PromotionReady,
                            },
                        )
                        .is_err()
                );
                assert_eq!(sim.state, before, "premature promotion mutated model state");
            }

            let transition = ControllerCommand::AdvanceMovement {
                movement_id,
                expected_stage: from,
                new_stage: to,
            };
            sim.apply(transition.clone());
            let after_transition = sim.state.clone();
            assert!(sim.state.apply(sim.next_epoch, &transition).is_err());
            assert_eq!(
                sim.state, after_transition,
                "duplicate reconciliation delivery changed durable movement state"
            );
            assert_eq!(
                sim.state.assignments()[&(topic_id, 0)]
                    .assignment_generation
                    .get(),
                1,
                "assignment generation changed before finalization"
            );
        }

        let before_final = sim.state.clone();
        assert!(
            sim.state
                .apply(
                    sim.next_epoch,
                    &ControllerCommand::FinalizeMovement {
                        movement_id,
                        expected_stage: MovementStage::LeadershipTransfer,
                        replacement: AssignmentRecord {
                            topic_id,
                            partition_id: 0,
                            assignment_generation: AssignmentGeneration::new(2).unwrap(),
                            voters: vec![2, 3, 4],
                            learners: Vec::new(),
                        },
                    },
                )
                .is_err()
        );
        assert_eq!(
            sim.state, before_final,
            "source removal finalized too early"
        );

        sim.apply(ControllerCommand::FinalizeMovement {
            movement_id,
            expected_stage: MovementStage::OldReplicaRemoved,
            replacement: AssignmentRecord {
                topic_id,
                partition_id: 0,
                assignment_generation: AssignmentGeneration::new(2).unwrap(),
                voters: vec![2, 3, 4],
                learners: Vec::new(),
            },
        });
        assert_eq!(
            sim.state.movements()[&movement_id].stage,
            MovementStage::Complete
        );
        assert_eq!(
            sim.state.assignments()[&(topic_id, 0)].voters,
            vec![2, 3, 4]
        );
        assert_eq!(
            sim.state.assignments()[&(topic_id, 0)]
                .assignment_generation
                .get(),
            2
        );
    }
}

#[test]
fn empty_broker_and_failure_domain_placement_edges_are_explicit() {
    let mut sim = DeterministicControllerSim::new();
    register_active_brokers(&mut sim, 3);
    let topic_id = TopicId::new([0x66; 16]);
    create_planned_topic(
        &mut sim,
        topic_id,
        "empty-broker".into(),
        4,
        FailureDomainPolicy::Required,
    );
    let before = sim.state.assignments().clone();
    register_active_brokers_from(&mut sim, 4, 4);
    assert_eq!(
        sim.state.assignments(),
        &before,
        "adding an empty broker must not rewrite committed assignments"
    );
    assert!(
        sim.state
            .assignments()
            .values()
            .all(|assignment| !assignment.voters.contains(&4))
    );

    let insufficient = PlacementInputSnapshot {
        active_feature_level: 1,
        topic_id: TopicId::new([0x67; 16]),
        brokers: vec![
            placement_broker(1, Some("r1"), Some("z1")),
            placement_broker(2, Some("r2"), Some("z2")),
        ],
    };
    assert!(matches!(
        plan_assignments(&insufficient, 0, 1, 3, FailureDomainPolicy::Disabled),
        Err(ControllerError::InvalidOwned(message)) if message.contains("not enough healthy ACTIVE brokers")
    ));

    let insufficient_domains = PlacementInputSnapshot {
        active_feature_level: 1,
        topic_id: TopicId::new([0x68; 16]),
        brokers: vec![
            placement_broker(1, Some("same"), Some("z1")),
            placement_broker(2, Some("same"), Some("z2")),
            placement_broker(3, Some("same"), Some("z3")),
        ],
    };
    assert!(
        plan_assignments(
            &insufficient_domains,
            0,
            1,
            3,
            FailureDomainPolicy::Required,
        )
        .is_err()
    );
    let preferred = plan_assignments(
        &insufficient_domains,
        0,
        1,
        3,
        FailureDomainPolicy::Preferred,
    )
    .unwrap();
    assert!(preferred.degraded_failure_domain);
    assert_eq!(preferred.assignments[0].voters.len(), 3);
}

#[test]
fn controller_resource_bounds_are_canonical_and_finite() {
    let snapshot = SnapshotLimits::default();
    assert_eq!(snapshot.max_brokers, MAX_CONTROLLER_BROKERS);
    assert_eq!(snapshot.max_topics, MAX_CONTROLLER_TOPICS);
    assert_eq!(snapshot.max_assignments, MAX_CONTROLLER_ASSIGNMENTS);
    assert_eq!(snapshot.max_movements, MAX_CONTROLLER_MOVEMENTS);
    assert_eq!(snapshot.max_snapshot_bytes, MAX_CONTROLLER_SNAPSHOT_BYTES);
    let metadata_bounds = [
        MAX_RETAINED_METADATA_DELTAS,
        MAX_RETAINED_METADATA_BYTES,
        MAX_METADATA_DELTAS_PER_RESPONSE,
        MAX_METADATA_DELTA_RESPONSE_BYTES,
    ];
    assert!(metadata_bounds.iter().all(|bound| *bound > 0));

    // The state-machine capacity helper is also exercised without allocating
    // hundreds of thousands of real records.
    assert!(
        super::state::ensure_capacity_for_test(
            MAX_CONTROLLER_BROKERS - 1,
            1,
            MAX_CONTROLLER_BROKERS
        )
        .is_ok()
    );
    assert!(
        super::state::ensure_capacity_for_test(MAX_CONTROLLER_BROKERS, 1, MAX_CONTROLLER_BROKERS)
            .is_err()
    );
    assert!(super::state::ensure_capacity_for_test(usize::MAX, 1, usize::MAX).is_err());
    assert!(super::state::ensure_snapshot_budget_for_test(1, 1, 1, 1).is_ok());
    assert!(
        super::state::ensure_snapshot_budget_for_test(
            MAX_CONTROLLER_BROKERS,
            MAX_CONTROLLER_TOPICS,
            MAX_CONTROLLER_ASSIGNMENTS,
            MAX_CONTROLLER_MOVEMENTS,
        )
        .is_err(),
        "combined controller-state maxima must fail closed before exceeding the 64 MiB snapshot/transport envelope"
    );
}
