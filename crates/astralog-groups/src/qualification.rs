use std::collections::BTreeMap;

use astralog_common::TopicId;
use astralog_protocol::api::{
    CommitOffsetV0, GroupEpoch, GroupId, GroupStateVersion, MAX_OFFSETS_PER_REQUEST,
    MAX_SUBSCRIPTION_TOPICS, MemberAuthorityV0, MemberEpoch, MemberId, TopicPartitionV0,
};

use crate::{
    CooperativeError, CooperativeStateMachine, GroupCommand, GroupMutationPlan, GroupPhase,
    GroupRecordV1, GroupStateMachine, InternalRecordV1, MemberRecordV1, OffsetGroupFenceV1,
    OffsetStateMachine, uniform_sticky_assign,
};

fn member(byte: u8) -> MemberId {
    MemberId::new([byte; 16]).unwrap()
}

fn topic() -> TopicId {
    TopicId::new([42; 16])
}

fn record(member_id: MemberId) -> MemberRecordV1 {
    MemberRecordV1 {
        member_id,
        instance_id: None,
        member_epoch: MemberEpoch::new(1).unwrap(),
        session_timeout_ms: 30_000,
        subscriptions: vec![topic()],
    }
}

fn finish_transition(prepared: GroupRecordV1) -> GroupRecordV1 {
    let mut cooperative = CooperativeStateMachine::from_group_record(prepared).unwrap();
    let group_fence = cooperative.required_group_fence().unwrap();
    cooperative
        .apply_committed_group_fence(group_fence)
        .unwrap();
    cooperative.activate_prepared_epoch().unwrap();

    loop {
        if cooperative.current().phase == GroupPhase::Stable {
            return cooperative.current().clone();
        }

        if let Some(owner) = cooperative
            .current()
            .pending_revocations
            .values()
            .next()
            .copied()
        {
            let revoked = cooperative
                .current()
                .pending_revocations
                .iter()
                .filter_map(|(partition, candidate)| (*candidate == owner).then_some(*partition))
                .collect::<Vec<_>>();
            let current = cooperative.current();
            let authority = astralog_protocol::api::MemberAuthorityV0 {
                group_id: current.group_id.clone(),
                member_id: owner,
                group_epoch: GroupEpoch::new(current.active_group_epoch).unwrap(),
                member_epoch: current.members[&owner].member_epoch,
            };
            cooperative.revoke_ack(&authority, &revoked).unwrap();
        }

        for fence in cooperative.required_owner_fences().unwrap() {
            cooperative.apply_committed_owner_fence(fence).unwrap();
        }
        match cooperative.activate_fenced_targets() {
            Ok(_) => {}
            Err(CooperativeError::NoReadyActivation)
                if !cooperative.current().pending_revocations.is_empty() => {}
            Err(error) => panic!("seeded churn transition could not make progress: {error}"),
        }
    }
}

fn seeded_churn(seed: u64) -> Vec<Vec<u8>> {
    let group_id = GroupId::new(format!("seeded-churn-{seed}")).unwrap();
    let counts = BTreeMap::from([(topic(), 6)]);
    let first = member(1);
    let initial_members = BTreeMap::from([(first, record(first))]);
    let target = uniform_sticky_assign(&initial_members, &BTreeMap::new(), &counts)
        .unwrap()
        .assignments;
    let mut machine = GroupStateMachine::new(group_id.clone());
    let prepared = machine
        .apply(GroupCommand::AdmitMember {
            expected_group_epoch: 0,
            allocated_member_id: first,
            instance_id: None,
            session_timeout_ms: 30_000,
            subscriptions: vec![topic()],
            plan: GroupMutationPlan {
                target_assignments: target,
            },
        })
        .unwrap();
    let mut current = finish_transition(prepared);
    let mut trace = vec![
        InternalRecordV1::GroupState(current.clone())
            .encode()
            .unwrap(),
    ];
    let mut next_member = 2_u8;
    let mut rng = seed | 1;

    for _ in 0..32 {
        rng = rng
            .wrapping_mul(6_364_136_223_846_793_005)
            .wrapping_add(1_442_695_040_888_963_407);
        let should_join =
            current.members.len() == 1 || (current.members.len() < 4 && (rng >> 63) == 0);
        let mut state = GroupStateMachine::from_snapshot(current.clone()).unwrap();
        let command = if should_join {
            let member_id = member(next_member);
            next_member = next_member.checked_add(1).expect("bounded churn member id");
            let mut members = current.members.clone();
            members.insert(member_id, record(member_id));
            let target = uniform_sticky_assign(&members, &current.active_assignments, &counts)
                .unwrap()
                .assignments;
            GroupCommand::AdmitMember {
                expected_group_epoch: current.active_group_epoch,
                allocated_member_id: member_id,
                instance_id: None,
                session_timeout_ms: 30_000,
                subscriptions: vec![topic()],
                plan: GroupMutationPlan {
                    target_assignments: target,
                },
            }
        } else {
            let index =
                usize::try_from(rng % u64::try_from(current.members.len()).unwrap()).unwrap();
            let member_id = *current.members.keys().nth(index).unwrap();
            let member_epoch = current.members[&member_id].member_epoch;
            let mut members = current.members.clone();
            members.remove(&member_id);
            let target = uniform_sticky_assign(&members, &current.active_assignments, &counts)
                .unwrap()
                .assignments;
            GroupCommand::Leave {
                expected_group_epoch: GroupEpoch::new(current.active_group_epoch).unwrap(),
                member_id,
                expected_member_epoch: member_epoch,
                plan: GroupMutationPlan {
                    target_assignments: target,
                },
            }
        };
        current = finish_transition(state.apply(command).unwrap());

        assert_eq!(current.phase, GroupPhase::Stable);
        assert_eq!(current.active_assignments, current.target_assignments);
        assert_eq!(current.active_assignments.len(), 6);
        let mut loads = current
            .members
            .keys()
            .copied()
            .map(|member_id| (member_id, 0_usize))
            .collect::<BTreeMap<_, _>>();
        for (topic_partition, owner) in &current.active_assignments {
            assert_eq!(topic_partition.topic_id, topic());
            assert!(current.members.contains_key(owner));
            *loads.get_mut(owner).unwrap() += 1;
        }
        let min = loads.values().copied().min().unwrap();
        let max = loads.values().copied().max().unwrap();
        assert!(max - min <= 1);
        trace.push(
            InternalRecordV1::GroupState(current.clone())
                .encode()
                .unwrap(),
        );
    }
    trace
}

#[test]
fn seeded_membership_churn_is_reproducible_and_preserves_cooperative_invariants() {
    for seed in 1..=32_u64 {
        let first = seeded_churn(seed);
        let second = seeded_churn(seed);
        assert_eq!(
            first, second,
            "seed {seed} produced a non-deterministic trace"
        );
    }
}

#[test]
fn resource_shape_one_over_limits_fail_without_partial_state_change() {
    let group_id = GroupId::new("resource-shapes").unwrap();
    let owner = member(1);
    let assigned = TopicPartitionV0 {
        topic_id: topic(),
        partition: 0,
    };
    let stable = GroupRecordV1 {
        group_id: group_id.clone(),
        state_version: GroupStateVersion::new(1).unwrap(),
        active_group_epoch: 1,
        pending_group_epoch: None,
        phase: GroupPhase::Stable,
        members: BTreeMap::from([(owner, record(owner))]),
        active_assignments: BTreeMap::from([(assigned, owner)]),
        target_assignments: BTreeMap::from([(assigned, owner)]),
        pending_revocations: BTreeMap::new(),
    };

    let mut too_many_subscriptions = (0..=MAX_SUBSCRIPTION_TOPICS)
        .map(|index| {
            let mut bytes = [0_u8; 16];
            bytes[..8].copy_from_slice(&u64::try_from(index + 1).unwrap().to_be_bytes());
            TopicId::new(bytes)
        })
        .collect::<Vec<_>>();
    too_many_subscriptions.sort_unstable();
    let mut group_state = GroupStateMachine::from_snapshot(stable.clone()).unwrap();
    let before_group = group_state.current().unwrap().clone();
    assert!(
        group_state
            .apply(GroupCommand::UpdateSubscription {
                expected_group_epoch: GroupEpoch::new(1).unwrap(),
                member_id: owner,
                expected_member_epoch: MemberEpoch::new(1).unwrap(),
                subscriptions: too_many_subscriptions,
                plan: GroupMutationPlan {
                    target_assignments: stable.active_assignments.clone(),
                },
            })
            .is_err()
    );
    assert_eq!(group_state.current().unwrap(), &before_group);

    let mut offsets = OffsetStateMachine::new(group_id.clone());
    offsets
        .apply_committed_group_fence(OffsetGroupFenceV1 {
            group_id: group_id.clone(),
            group_epoch: GroupEpoch::new(1).unwrap(),
            source_group_state_version: GroupStateVersion::new(1).unwrap(),
            owners: BTreeMap::from([(assigned, (owner, MemberEpoch::new(1).unwrap()))]),
        })
        .unwrap();
    let authority = MemberAuthorityV0 {
        group_id,
        member_id: owner,
        group_epoch: GroupEpoch::new(1).unwrap(),
        member_epoch: MemberEpoch::new(1).unwrap(),
    };
    let oversized_offsets = (0..=MAX_OFFSETS_PER_REQUEST)
        .map(|index| CommitOffsetV0 {
            topic_id: assigned.topic_id,
            partition: u32::try_from(index).unwrap(),
            next_offset: u64::try_from(index).unwrap(),
            metadata: String::new(),
        })
        .collect::<Vec<_>>();
    assert!(
        offsets
            .prepare_commit(&stable, &authority, &oversized_offsets, 1)
            .is_err()
    );
    assert!(offsets.values().is_empty());
}
