use std::{cmp::Reverse, collections::BTreeSet};

use astralog_common::TopicId;

use crate::{
    AssignmentGeneration, AssignmentRecord, BrokerLifecycle, BrokerLivenessObservation,
    BrokerLivenessState, ControllerError, ControllerState, FailureDomainPolicy,
    MAX_TOPIC_PARTITIONS, Result,
};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PlacementBrokerSnapshot {
    pub broker_id: u32,
    pub rack: Option<String>,
    pub zone: Option<String>,
    pub lifecycle: BrokerLifecycle,
    pub min_feature: u16,
    pub max_feature: u16,
    pub liveness: BrokerLivenessState,
    pub current_replica_count: u64,
    pub topic_replica_count: u64,
}

impl PlacementBrokerSnapshot {
    fn eligible(&self, active_feature_level: u16) -> bool {
        self.lifecycle == BrokerLifecycle::Active
            && self.min_feature <= active_feature_level
            && active_feature_level <= self.max_feature
            && self.liveness == BrokerLivenessState::Healthy
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PlacementInputSnapshot {
    pub active_feature_level: u16,
    pub topic_id: TopicId,
    pub brokers: Vec<PlacementBrokerSnapshot>,
}

impl PlacementInputSnapshot {
    pub fn from_controller_state(
        state: &ControllerState,
        topic_id: TopicId,
        liveness: &[BrokerLivenessObservation],
    ) -> Result<Self> {
        if *topic_id.as_bytes() == [0; 16] {
            return Err(ControllerError::Invalid(
                "placement topic id zero is reserved",
            ));
        }

        let mut observations = std::collections::BTreeMap::new();
        for observation in liveness {
            if observations
                .insert(observation.authority.broker_id, *observation)
                .is_some()
            {
                return Err(ControllerError::Invalid(
                    "placement liveness snapshot contains duplicate broker",
                ));
            }
        }

        let mut brokers = state
            .brokers()
            .values()
            .map(|broker| {
                let observation = observations.get(&broker.broker_id).copied();
                let liveness = observation
                    .filter(|value| {
                        value.authority.node_uuid == broker.node_uuid
                            && value.authority.incarnation_epoch == broker.incarnation_epoch
                    })
                    .map_or(BrokerLivenessState::Unknown, |value| value.state);
                PlacementBrokerSnapshot {
                    broker_id: broker.broker_id,
                    rack: broker.rack.clone(),
                    zone: broker.zone.clone(),
                    lifecycle: broker.lifecycle,
                    min_feature: broker.min_feature,
                    max_feature: broker.max_feature,
                    liveness,
                    current_replica_count: 0,
                    topic_replica_count: 0,
                }
            })
            .collect::<Vec<_>>();

        let broker_indexes = brokers
            .iter()
            .enumerate()
            .map(|(index, broker)| (broker.broker_id, index))
            .collect::<std::collections::BTreeMap<_, _>>();
        for assignment in state.assignments().values() {
            for broker_id in assignment.voters.iter().chain(&assignment.learners) {
                let Some(index) = broker_indexes.get(broker_id).copied() else {
                    return Err(ControllerError::Invalid(
                        "placement snapshot assignment references unknown broker",
                    ));
                };
                brokers[index].current_replica_count = brokers[index]
                    .current_replica_count
                    .checked_add(1)
                    .ok_or(ControllerError::Invalid("placement replica count overflow"))?;
                if assignment.topic_id == topic_id {
                    brokers[index].topic_replica_count =
                        brokers[index].topic_replica_count.checked_add(1).ok_or(
                            ControllerError::Invalid("placement topic replica count overflow"),
                        )?;
                }
            }
        }
        brokers.sort_by_key(|broker| broker.broker_id);

        Ok(Self {
            active_feature_level: state.active_feature_level(),
            topic_id,
            brokers,
        })
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PlacementPlan {
    pub assignments: Vec<AssignmentRecord>,
    pub degraded_failure_domain: bool,
}

pub fn plan_assignments(
    snapshot: &PlacementInputSnapshot,
    first_partition: u32,
    end_partition: u32,
    replication_factor: u16,
    policy: FailureDomainPolicy,
) -> Result<PlacementPlan> {
    if first_partition >= end_partition || end_partition > MAX_TOPIC_PARTITIONS {
        return Err(ControllerError::Invalid(
            "placement partition range is empty or exceeds the V1 topic bound",
        ));
    }
    if !matches!(replication_factor, 1 | 3 | 5) {
        return Err(ControllerError::Invalid(
            "placement replication factor must be exactly 1, 3, or 5",
        ));
    }

    let rf = usize::from(replication_factor);
    let mut brokers = snapshot.brokers.clone();
    let eligible_count = brokers
        .iter()
        .filter(|broker| broker.eligible(snapshot.active_feature_level))
        .count();
    if eligible_count < rf {
        return Err(ControllerError::InvalidOwned(format!(
            "not enough healthy ACTIVE brokers for RF={replication_factor}: eligible={eligible_count}"
        )));
    }

    let mut assignments = Vec::with_capacity(
        usize::try_from(end_partition - first_partition)
            .map_err(|_| ControllerError::Invalid("placement partition count overflow"))?,
    );
    let mut degraded_failure_domain = false;

    for partition_id in first_partition..end_partition {
        let mut selected = BTreeSet::new();
        let mut selected_racks = BTreeSet::new();
        let mut selected_zones = BTreeSet::new();

        for _ in 0..rf {
            let candidate_index = brokers
                .iter()
                .enumerate()
                .filter(|(_, broker)| {
                    broker.eligible(snapshot.active_feature_level)
                        && !selected.contains(&broker.broker_id)
                        && match policy {
                            FailureDomainPolicy::Required => {
                                broker.rack.as_ref().is_some_and(|rack| !selected_racks.contains(rack))
                                    && broker
                                        .zone
                                        .as_ref()
                                        .is_some_and(|zone| !selected_zones.contains(zone))
                            }
                            FailureDomainPolicy::Disabled | FailureDomainPolicy::Preferred => true,
                        }
                })
                .min_by_key(|(_, broker)| {
                    let domain_gain = match policy {
                        FailureDomainPolicy::Disabled => 0_u8,
                        FailureDomainPolicy::Preferred | FailureDomainPolicy::Required => {
                            u8::from(
                                broker
                                    .rack
                                    .as_ref()
                                    .is_some_and(|rack| !selected_racks.contains(rack)),
                            ) + u8::from(
                                broker
                                    .zone
                                    .as_ref()
                                    .is_some_and(|zone| !selected_zones.contains(zone)),
                            )
                        }
                    };
                    (
                        Reverse(domain_gain),
                        broker.current_replica_count,
                        broker.topic_replica_count,
                        broker.broker_id,
                    )
                })
                .map(|(index, _)| index)
                .ok_or_else(|| {
                    if policy == FailureDomainPolicy::Required {
                        ControllerError::InvalidOwned(format!(
                            "required rack/zone placement is unsatisfiable for RF={replication_factor}"
                        ))
                    } else {
                        ControllerError::InvalidOwned(format!(
                            "no eligible distinct broker remains for RF={replication_factor}"
                        ))
                    }
                })?;

            let candidate = &brokers[candidate_index];
            selected.insert(candidate.broker_id);
            if let Some(rack) = &candidate.rack {
                selected_racks.insert(rack.clone());
            }
            if let Some(zone) = &candidate.zone {
                selected_zones.insert(zone.clone());
            }
        }

        if policy == FailureDomainPolicy::Preferred
            && (selected_racks.len() != rf || selected_zones.len() != rf)
        {
            degraded_failure_domain = true;
        }

        let mut voters = selected.into_iter().collect::<Vec<_>>();
        voters.sort_unstable();
        for broker_id in &voters {
            let broker = brokers
                .iter_mut()
                .find(|broker| broker.broker_id == *broker_id)
                .expect("selected placement broker exists in snapshot");
            broker.current_replica_count = broker
                .current_replica_count
                .checked_add(1)
                .ok_or(ControllerError::Invalid("placement replica count overflow"))?;
            broker.topic_replica_count =
                broker
                    .topic_replica_count
                    .checked_add(1)
                    .ok_or(ControllerError::Invalid(
                        "placement topic replica count overflow",
                    ))?;
        }
        assignments.push(AssignmentRecord {
            topic_id: snapshot.topic_id,
            partition_id,
            assignment_generation: AssignmentGeneration::new(1)
                .expect("initial assignment generation one is valid"),
            voters,
            learners: Vec::new(),
        });
    }

    Ok(PlacementPlan {
        assignments,
        degraded_failure_domain,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn broker(
        id: u32,
        rack: Option<&str>,
        zone: Option<&str>,
        count: u64,
    ) -> PlacementBrokerSnapshot {
        PlacementBrokerSnapshot {
            broker_id: id,
            rack: rack.map(str::to_owned),
            zone: zone.map(str::to_owned),
            lifecycle: BrokerLifecycle::Active,
            min_feature: 1,
            max_feature: 3,
            liveness: BrokerLivenessState::Healthy,
            current_replica_count: count,
            topic_replica_count: 0,
        }
    }

    fn snapshot(mut brokers: Vec<PlacementBrokerSnapshot>) -> PlacementInputSnapshot {
        brokers.sort_by_key(|broker| broker.broker_id);
        PlacementInputSnapshot {
            active_feature_level: 1,
            topic_id: TopicId::new([7; 16]),
            brokers,
        }
    }

    #[test]
    fn required_policy_uses_distinct_racks_and_zones() {
        let input = snapshot(vec![
            broker(1, Some("r1"), Some("z1"), 0),
            broker(2, Some("r2"), Some("z2"), 0),
            broker(3, Some("r3"), Some("z3"), 0),
            broker(4, Some("r1"), Some("z4"), 0),
        ]);
        let plan = plan_assignments(&input, 0, 1, 3, FailureDomainPolicy::Required).unwrap();
        assert_eq!(plan.assignments[0].voters, vec![1, 2, 3]);
        assert!(!plan.degraded_failure_domain);
    }

    #[test]
    fn required_policy_never_silently_weakens_domains() {
        let input = snapshot(vec![
            broker(1, Some("r1"), Some("z1"), 0),
            broker(2, Some("r1"), Some("z2"), 0),
            broker(3, None, Some("z3"), 0),
        ]);
        assert!(plan_assignments(&input, 0, 1, 3, FailureDomainPolicy::Required).is_err());
    }

    #[test]
    fn preferred_policy_fallback_is_explicit() {
        let input = snapshot(vec![
            broker(1, Some("r1"), Some("z1"), 0),
            broker(2, Some("r1"), Some("z2"), 0),
            broker(3, Some("r2"), Some("z2"), 0),
        ]);
        let plan = plan_assignments(&input, 0, 1, 3, FailureDomainPolicy::Preferred).unwrap();
        assert!(plan.degraded_failure_domain);
        assert_eq!(plan.assignments[0].voters.len(), 3);
    }

    #[test]
    fn lifecycle_feature_and_liveness_gate_new_placement() {
        let mut joining = broker(2, Some("r2"), Some("z2"), 0);
        joining.lifecycle = BrokerLifecycle::Joining;
        let mut suspect = broker(3, Some("r3"), Some("z3"), 0);
        suspect.liveness = BrokerLivenessState::Suspect;
        let mut incompatible = broker(4, Some("r4"), Some("z4"), 0);
        incompatible.min_feature = 2;
        let input = snapshot(vec![
            broker(1, Some("r1"), Some("z1"), 0),
            joining,
            suspect,
            incompatible,
        ]);
        let plan = plan_assignments(&input, 0, 1, 1, FailureDomainPolicy::Disabled).unwrap();
        assert_eq!(plan.assignments[0].voters, vec![1]);
    }

    #[test]
    fn scoring_balances_global_then_topic_local_counts_with_broker_id_tiebreak() {
        let mut input = snapshot(vec![
            broker(3, Some("r3"), Some("z3"), 1),
            broker(1, Some("r1"), Some("z1"), 0),
            broker(2, Some("r2"), Some("z2"), 0),
        ]);
        input.brokers[0].topic_replica_count = 1;
        let plan = plan_assignments(&input, 0, 1, 1, FailureDomainPolicy::Disabled).unwrap();
        assert_eq!(plan.assignments[0].voters, vec![2]);
    }

    #[test]
    fn planner_is_deterministic_for_reordered_candidate_input() {
        let candidates = vec![
            broker(5, Some("r5"), Some("z5"), 0),
            broker(1, Some("r1"), Some("z1"), 0),
            broker(4, Some("r4"), Some("z4"), 0),
            broker(2, Some("r2"), Some("z2"), 0),
            broker(3, Some("r3"), Some("z3"), 0),
        ];
        let expected = plan_assignments(
            &snapshot(candidates.clone()),
            0,
            16,
            3,
            FailureDomainPolicy::Required,
        )
        .unwrap();
        for shift in 0..candidates.len() {
            let mut reordered = candidates.clone();
            reordered.rotate_left(shift);
            assert_eq!(
                plan_assignments(
                    &snapshot(reordered),
                    0,
                    16,
                    3,
                    FailureDomainPolicy::Required,
                )
                .unwrap(),
                expected
            );
        }
    }

    #[test]
    fn every_assignment_uses_distinct_brokers_and_required_racks() {
        let input = snapshot(
            (1..=5)
                .map(|id| broker(id, Some(&format!("r{id}")), Some(&format!("z{id}")), 0))
                .collect(),
        );
        let plan = plan_assignments(&input, 0, 64, 3, FailureDomainPolicy::Required).unwrap();
        for assignment in &plan.assignments {
            assert_eq!(assignment.voters.len(), 3);
            assert!(
                assignment
                    .voters
                    .windows(2)
                    .all(|window| window[0] < window[1])
            );
            let racks = assignment
                .voters
                .iter()
                .map(|id| {
                    input
                        .brokers
                        .iter()
                        .find(|broker| broker.broker_id == *id)
                        .unwrap()
                        .rack
                        .as_deref()
                        .unwrap()
                })
                .collect::<BTreeSet<_>>();
            assert_eq!(racks.len(), 3);
        }
    }
}
