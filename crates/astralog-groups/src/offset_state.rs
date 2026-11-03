use std::{collections::BTreeMap, fmt};

use astralog_protocol::{
    PublicError,
    api::{
        CommitOffsetV0, GroupId, MAX_GROUP_PARTITIONS, MAX_OFFSETS_PER_REQUEST, MemberAuthorityV0,
        TopicPartitionV0,
    },
};

use crate::{
    GroupRecordV1, InternalRecordV1, OffsetGroupFenceV1, OffsetOwnerFenceV1, OffsetValueV1,
};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum OffsetApplyOutcome {
    Applied,
    Idempotent,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum OffsetStateError {
    WrongGroup,
    GroupFenceRegression,
    GroupFenceConflict,
    OwnerFenceConflict,
    FenceMismatch(&'static str),
    MissingGroupFence,
    GroupEpochStale { expected: u64, actual: u64 },
    UnknownMember,
    InvalidOffsetCommit(&'static str),
    Codec(String),
}

impl fmt::Display for OffsetStateError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::WrongGroup => f.write_str("offset state targets the wrong group"),
            Self::GroupFenceRegression => f.write_str("offset group fence regressed"),
            Self::GroupFenceConflict => f.write_str("conflicting offset group fence"),
            Self::OwnerFenceConflict => f.write_str("conflicting offset owner fence"),
            Self::FenceMismatch(message) => write!(f, "offset fence mismatch: {message}"),
            Self::MissingGroupFence => f.write_str("offset state has no committed group fence"),
            Self::GroupEpochStale { expected, actual } => write!(
                f,
                "stale offset group epoch: expected {expected}, current {actual}"
            ),
            Self::UnknownMember => f.write_str("unknown offset-commit member authority"),
            Self::InvalidOffsetCommit(message) => write!(f, "invalid offset commit: {message}"),
            Self::Codec(message) => write!(f, "invalid offset record: {message}"),
        }
    }
}

impl std::error::Error for OffsetStateError {}

impl OffsetStateError {
    pub fn public_error(&self) -> Option<PublicError> {
        match self {
            Self::GroupEpochStale { .. } => Some(PublicError::GroupEpochStale),
            Self::UnknownMember => Some(PublicError::UnknownMember),
            Self::InvalidOffsetCommit(_)
            | Self::FenceMismatch(_)
            | Self::MissingGroupFence
            | Self::GroupFenceRegression
            | Self::GroupFenceConflict
            | Self::OwnerFenceConflict => Some(PublicError::InvalidOffsetCommit),
            Self::WrongGroup | Self::Codec(_) => None,
        }
    }
}

pub type OffsetStateResult<T> = core::result::Result<T, OffsetStateError>;

#[derive(Debug, Clone)]
pub struct OffsetStateMachine {
    group_id: GroupId,
    group_fence: Option<OffsetGroupFenceV1>,
    owner_fences: BTreeMap<TopicPartitionV0, OffsetOwnerFenceV1>,
    values: BTreeMap<TopicPartitionV0, OffsetValueV1>,
}

impl OffsetStateMachine {
    pub fn new(group_id: GroupId) -> Self {
        Self {
            group_id,
            group_fence: None,
            owner_fences: BTreeMap::new(),
            values: BTreeMap::new(),
        }
    }

    pub fn group_id(&self) -> &GroupId {
        &self.group_id
    }

    pub fn group_fence(&self) -> Option<&OffsetGroupFenceV1> {
        self.group_fence.as_ref()
    }

    pub fn owner_fence(&self, partition: TopicPartitionV0) -> Option<&OffsetOwnerFenceV1> {
        self.owner_fences.get(&partition)
    }

    pub fn owner_fences(&self) -> &BTreeMap<TopicPartitionV0, OffsetOwnerFenceV1> {
        &self.owner_fences
    }

    pub fn value(&self, partition: TopicPartitionV0) -> Option<&OffsetValueV1> {
        self.values.get(&partition)
    }

    pub fn values(&self) -> &BTreeMap<TopicPartitionV0, OffsetValueV1> {
        &self.values
    }

    pub fn apply_committed_group_fence(
        &mut self,
        fence: OffsetGroupFenceV1,
    ) -> OffsetStateResult<OffsetApplyOutcome> {
        validate_record(InternalRecordV1::OffsetGroupFence(fence.clone()))?;
        if fence.group_id != self.group_id {
            return Err(OffsetStateError::WrongGroup);
        }
        if let Some(current) = &self.group_fence {
            if fence.group_epoch < current.group_epoch {
                return Err(OffsetStateError::GroupFenceRegression);
            }
            if fence.group_epoch == current.group_epoch {
                return if fence == *current {
                    Ok(OffsetApplyOutcome::Idempotent)
                } else {
                    Err(OffsetStateError::GroupFenceConflict)
                };
            }
            if fence.source_group_state_version <= current.source_group_state_version {
                return Err(OffsetStateError::FenceMismatch(
                    "newer group epoch must cite a newer group-state version",
                ));
            }
        }
        self.group_fence = Some(fence);
        self.owner_fences.clear();
        Ok(OffsetApplyOutcome::Applied)
    }

    pub fn apply_committed_owner_fence(
        &mut self,
        fence: OffsetOwnerFenceV1,
    ) -> OffsetStateResult<OffsetApplyOutcome> {
        validate_record(InternalRecordV1::OffsetOwnerFence(fence.clone()))?;
        if fence.group_id != self.group_id {
            return Err(OffsetStateError::WrongGroup);
        }
        let group_fence = self
            .group_fence
            .as_ref()
            .ok_or(OffsetStateError::MissingGroupFence)?;
        if fence.group_epoch != group_fence.group_epoch {
            return Err(OffsetStateError::GroupEpochStale {
                expected: fence.group_epoch.get(),
                actual: group_fence.group_epoch.get(),
            });
        }
        if fence.source_group_state_version <= group_fence.source_group_state_version {
            return Err(OffsetStateError::FenceMismatch(
                "owner fence must cite group state after its epoch fence source",
            ));
        }
        if let Some(current) = self.owner_fences.get(&fence.topic_partition) {
            return if *current == fence {
                Ok(OffsetApplyOutcome::Idempotent)
            } else {
                Err(OffsetStateError::OwnerFenceConflict)
            };
        }
        if self.owner_fences.len() >= MAX_GROUP_PARTITIONS {
            return Err(OffsetStateError::InvalidOffsetCommit(
                "owner-fence live-state bound exceeded",
            ));
        }
        self.owner_fences.insert(fence.topic_partition, fence);
        Ok(OffsetApplyOutcome::Applied)
    }

    pub fn prepare_commit(
        &self,
        group: &GroupRecordV1,
        authority: &MemberAuthorityV0,
        offsets: &[CommitOffsetV0],
        commit_timestamp_ms: i64,
    ) -> OffsetStateResult<Vec<OffsetValueV1>> {
        if group.group_id != self.group_id || authority.group_id != self.group_id {
            return Err(OffsetStateError::WrongGroup);
        }
        validate_commit_shape(offsets)?;
        if authority.group_epoch.get() != group.active_group_epoch {
            return Err(OffsetStateError::GroupEpochStale {
                expected: authority.group_epoch.get(),
                actual: group.active_group_epoch,
            });
        }
        let member = group
            .members
            .get(&authority.member_id)
            .ok_or(OffsetStateError::UnknownMember)?;
        if member.member_epoch != authority.member_epoch {
            return Err(OffsetStateError::UnknownMember);
        }
        let group_fence = self
            .group_fence
            .as_ref()
            .ok_or(OffsetStateError::MissingGroupFence)?;
        if authority.group_epoch != group_fence.group_epoch {
            return Err(OffsetStateError::GroupEpochStale {
                expected: authority.group_epoch.get(),
                actual: group_fence.group_epoch.get(),
            });
        }

        let new_value_count = offsets
            .iter()
            .filter(|offset| {
                let partition = TopicPartitionV0 {
                    topic_id: offset.topic_id,
                    partition: offset.partition,
                };
                !self.values.contains_key(&partition)
            })
            .count();
        if self
            .values
            .len()
            .checked_add(new_value_count)
            .is_none_or(|count| count > MAX_GROUP_PARTITIONS)
        {
            return Err(OffsetStateError::InvalidOffsetCommit(
                "offset logical-state bound would be exceeded by atomic batch",
            ));
        }

        let mut values = Vec::with_capacity(offsets.len());
        for offset in offsets {
            let partition = TopicPartitionV0 {
                topic_id: offset.topic_id,
                partition: offset.partition,
            };
            if group.active_assignments.get(&partition) != Some(&authority.member_id) {
                return Err(OffsetStateError::InvalidOffsetCommit(
                    "partition is not active on this member",
                ));
            }
            if self.effective_owner(partition)
                != Some((authority.member_id, authority.member_epoch))
            {
                return Err(OffsetStateError::InvalidOffsetCommit(
                    "durable owner fence does not match member authority",
                ));
            }
            let value = OffsetValueV1 {
                group_id: self.group_id.clone(),
                topic_partition: partition,
                next_offset: offset.next_offset,
                metadata: offset.metadata.clone(),
                commit_timestamp_ms,
                group_epoch: authority.group_epoch,
                member_id: authority.member_id,
                member_epoch: authority.member_epoch,
            };
            validate_record(InternalRecordV1::OffsetValue(value.clone()))?;
            values.push(value);
        }
        Ok(values)
    }

    pub fn apply_committed_value(
        &mut self,
        value: OffsetValueV1,
    ) -> OffsetStateResult<OffsetApplyOutcome> {
        validate_record(InternalRecordV1::OffsetValue(value.clone()))?;
        if value.group_id != self.group_id {
            return Err(OffsetStateError::WrongGroup);
        }
        let group_fence = self
            .group_fence
            .as_ref()
            .ok_or(OffsetStateError::MissingGroupFence)?;
        if value.group_epoch != group_fence.group_epoch {
            return Err(OffsetStateError::GroupEpochStale {
                expected: value.group_epoch.get(),
                actual: group_fence.group_epoch.get(),
            });
        }
        if self.effective_owner(value.topic_partition)
            != Some((value.member_id, value.member_epoch))
        {
            return Err(OffsetStateError::InvalidOffsetCommit(
                "committed offset value violates durable owner fence",
            ));
        }
        if self.values.len() >= MAX_GROUP_PARTITIONS
            && !self.values.contains_key(&value.topic_partition)
        {
            return Err(OffsetStateError::InvalidOffsetCommit(
                "offset logical-state bound exceeded",
            ));
        }
        let outcome = if self
            .values
            .get(&value.topic_partition)
            .is_some_and(|current| current == &value)
        {
            OffsetApplyOutcome::Idempotent
        } else {
            OffsetApplyOutcome::Applied
        };
        self.values.insert(value.topic_partition, value);
        Ok(outcome)
    }

    pub fn effective_owner(
        &self,
        partition: TopicPartitionV0,
    ) -> Option<(
        astralog_protocol::api::MemberId,
        astralog_protocol::api::MemberEpoch,
    )> {
        if let Some(fence) = self.owner_fences.get(&partition) {
            return Some((fence.member_id, fence.member_epoch));
        }
        self.group_fence
            .as_ref()
            .and_then(|fence| fence.owners.get(&partition).copied())
    }
}

fn validate_commit_shape(offsets: &[CommitOffsetV0]) -> OffsetStateResult<()> {
    if offsets.is_empty() || offsets.len() > MAX_OFFSETS_PER_REQUEST {
        return Err(OffsetStateError::InvalidOffsetCommit(
            "offset count is outside frozen request bound",
        ));
    }
    let mut previous = None;
    for offset in offsets {
        let current = TopicPartitionV0 {
            topic_id: offset.topic_id,
            partition: offset.partition,
        };
        if previous.is_some_and(|value| value >= current) {
            return Err(OffsetStateError::InvalidOffsetCommit(
                "offset entries are not canonical ascending unique",
            ));
        }
        previous = Some(current);
    }
    Ok(())
}

fn validate_record(record: InternalRecordV1) -> OffsetStateResult<()> {
    record
        .encode()
        .map(|_| ())
        .map_err(|error| OffsetStateError::Codec(error.to_string()))
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use astralog_common::TopicId;
    use astralog_protocol::api::{GroupEpoch, GroupStateVersion, MemberEpoch, MemberId};

    use super::*;
    use crate::{GroupPhase, MemberRecordV1};

    fn member(byte: u8) -> MemberId {
        MemberId::new([byte; 16]).unwrap()
    }

    fn tp(partition: u32) -> TopicPartitionV0 {
        TopicPartitionV0 {
            topic_id: TopicId::new([1; 16]),
            partition,
        }
    }

    fn stable_group() -> GroupRecordV1 {
        let owner = member(1);
        let partition = tp(0);
        GroupRecordV1 {
            group_id: GroupId::new("offsets").unwrap(),
            state_version: GroupStateVersion::new(3).unwrap(),
            active_group_epoch: 2,
            pending_group_epoch: None,
            phase: GroupPhase::Stable,
            members: BTreeMap::from([(
                owner,
                MemberRecordV1 {
                    member_id: owner,
                    instance_id: None,
                    member_epoch: MemberEpoch::new(1).unwrap(),
                    session_timeout_ms: 30_000,
                    subscriptions: vec![partition.topic_id],
                },
            )]),
            active_assignments: BTreeMap::from([(partition, owner)]),
            target_assignments: BTreeMap::from([(partition, owner)]),
            pending_revocations: BTreeMap::new(),
        }
    }

    fn group_fence() -> OffsetGroupFenceV1 {
        OffsetGroupFenceV1 {
            group_id: GroupId::new("offsets").unwrap(),
            group_epoch: GroupEpoch::new(2).unwrap(),
            source_group_state_version: GroupStateVersion::new(2).unwrap(),
            owners: BTreeMap::from([(tp(0), (member(1), MemberEpoch::new(1).unwrap()))]),
        }
    }

    fn authority() -> MemberAuthorityV0 {
        MemberAuthorityV0 {
            group_id: GroupId::new("offsets").unwrap(),
            member_id: member(1),
            group_epoch: GroupEpoch::new(2).unwrap(),
            member_epoch: MemberEpoch::new(1).unwrap(),
        }
    }

    #[test]
    fn group_fence_is_monotonic_idempotent_and_resets_epoch_owner_overrides() {
        let mut state = OffsetStateMachine::new(GroupId::new("offsets").unwrap());
        let first = group_fence();
        assert_eq!(
            state.apply_committed_group_fence(first.clone()).unwrap(),
            OffsetApplyOutcome::Applied
        );
        assert_eq!(
            state.apply_committed_group_fence(first.clone()).unwrap(),
            OffsetApplyOutcome::Idempotent
        );

        let override_fence = OffsetOwnerFenceV1 {
            group_id: first.group_id.clone(),
            topic_partition: tp(0),
            group_epoch: first.group_epoch,
            member_id: member(2),
            member_epoch: MemberEpoch::new(1).unwrap(),
            source_group_state_version: GroupStateVersion::new(3).unwrap(),
        };
        state.apply_committed_owner_fence(override_fence).unwrap();
        assert_eq!(state.effective_owner(tp(0)).unwrap().0, member(2));

        let mut next = first.clone();
        next.group_epoch = GroupEpoch::new(3).unwrap();
        next.source_group_state_version = GroupStateVersion::new(4).unwrap();
        next.owners = BTreeMap::from([(tp(0), (member(1), MemberEpoch::new(1).unwrap()))]);
        state.apply_committed_group_fence(next).unwrap();
        assert!(state.owner_fence(tp(0)).is_none());
        assert_eq!(state.effective_owner(tp(0)).unwrap().0, member(1));

        assert!(matches!(
            state.apply_committed_group_fence(first),
            Err(OffsetStateError::GroupFenceRegression)
        ));
    }

    #[test]
    fn owner_fence_requires_current_epoch_and_later_group_state() {
        let mut state = OffsetStateMachine::new(GroupId::new("offsets").unwrap());
        state.apply_committed_group_fence(group_fence()).unwrap();
        let stale_source = OffsetOwnerFenceV1 {
            group_id: GroupId::new("offsets").unwrap(),
            topic_partition: tp(0),
            group_epoch: GroupEpoch::new(2).unwrap(),
            member_id: member(2),
            member_epoch: MemberEpoch::new(1).unwrap(),
            source_group_state_version: GroupStateVersion::new(2).unwrap(),
        };
        assert!(matches!(
            state.apply_committed_owner_fence(stale_source),
            Err(OffsetStateError::FenceMismatch(_))
        ));
    }

    #[test]
    fn commit_validation_is_atomic_and_checks_group_plus_durable_owner_authority() {
        let group = stable_group();
        let mut state = OffsetStateMachine::new(group.group_id.clone());
        state.apply_committed_group_fence(group_fence()).unwrap();
        let request = [CommitOffsetV0 {
            topic_id: tp(0).topic_id,
            partition: 0,
            next_offset: 10,
            metadata: "first".into(),
        }];
        let values = state
            .prepare_commit(&group, &authority(), &request, 123)
            .unwrap();
        assert_eq!(values.len(), 1);
        assert_eq!(values[0].commit_timestamp_ms, 123);
        state.apply_committed_value(values[0].clone()).unwrap();
        assert_eq!(state.value(tp(0)).unwrap().next_offset, 10);

        let mut wrong_partition = group.clone();
        wrong_partition.active_assignments.clear();
        let before = state.value(tp(0)).cloned();
        assert_eq!(
            state
                .prepare_commit(&wrong_partition, &authority(), &request, 124)
                .unwrap_err()
                .public_error(),
            Some(PublicError::InvalidOffsetCommit)
        );
        assert_eq!(state.value(tp(0)).cloned(), before);
    }

    #[test]
    fn owner_fence_rejects_late_old_owner_and_authorized_offset_rewind_is_allowed() {
        let group = stable_group();
        let mut state = OffsetStateMachine::new(group.group_id.clone());
        state.apply_committed_group_fence(group_fence()).unwrap();
        let first = state
            .prepare_commit(
                &group,
                &authority(),
                &[CommitOffsetV0 {
                    topic_id: tp(0).topic_id,
                    partition: 0,
                    next_offset: 100,
                    metadata: "forward".into(),
                }],
                1,
            )
            .unwrap()
            .pop()
            .unwrap();
        state.apply_committed_value(first).unwrap();

        let fence = OffsetOwnerFenceV1 {
            group_id: group.group_id.clone(),
            topic_partition: tp(0),
            group_epoch: GroupEpoch::new(2).unwrap(),
            member_id: member(2),
            member_epoch: MemberEpoch::new(1).unwrap(),
            source_group_state_version: GroupStateVersion::new(4).unwrap(),
        };
        state.apply_committed_owner_fence(fence).unwrap();
        let late = OffsetValueV1 {
            group_id: group.group_id.clone(),
            topic_partition: tp(0),
            next_offset: 101,
            metadata: String::new(),
            commit_timestamp_ms: 2,
            group_epoch: GroupEpoch::new(2).unwrap(),
            member_id: member(1),
            member_epoch: MemberEpoch::new(1).unwrap(),
        };
        assert_eq!(
            state
                .apply_committed_value(late)
                .unwrap_err()
                .public_error(),
            Some(PublicError::InvalidOffsetCommit)
        );

        let rewound = OffsetValueV1 {
            group_id: group.group_id.clone(),
            topic_partition: tp(0),
            next_offset: 5,
            metadata: "rewind".into(),
            commit_timestamp_ms: 3,
            group_epoch: GroupEpoch::new(2).unwrap(),
            member_id: member(2),
            member_epoch: MemberEpoch::new(1).unwrap(),
        };
        state.apply_committed_value(rewound).unwrap();
        assert_eq!(state.value(tp(0)).unwrap().next_offset, 5);
    }

    #[test]
    fn newer_group_fence_rejects_stale_epoch_values() {
        let mut state = OffsetStateMachine::new(GroupId::new("offsets").unwrap());
        let first = group_fence();
        state.apply_committed_group_fence(first.clone()).unwrap();
        let mut next = first;
        next.group_epoch = GroupEpoch::new(3).unwrap();
        next.source_group_state_version = GroupStateVersion::new(4).unwrap();
        state.apply_committed_group_fence(next).unwrap();
        let stale = OffsetValueV1 {
            group_id: GroupId::new("offsets").unwrap(),
            topic_partition: tp(0),
            next_offset: 1,
            metadata: String::new(),
            commit_timestamp_ms: 1,
            group_epoch: GroupEpoch::new(2).unwrap(),
            member_id: member(1),
            member_epoch: MemberEpoch::new(1).unwrap(),
        };
        assert_eq!(
            state
                .apply_committed_value(stale)
                .unwrap_err()
                .public_error(),
            Some(PublicError::GroupEpochStale)
        );
    }

    #[test]
    fn atomic_batch_rejects_live_state_overflow_before_any_value_is_prepared() {
        let group = stable_group();
        let mut state = OffsetStateMachine::new(group.group_id.clone());
        state.apply_committed_group_fence(group_fence()).unwrap();
        for partition in 0..(MAX_GROUP_PARTITIONS - 1) {
            let topic_partition = TopicPartitionV0 {
                topic_id: TopicId::new([3; 16]),
                partition: u32::try_from(partition).unwrap(),
            };
            state.values.insert(
                topic_partition,
                OffsetValueV1 {
                    group_id: group.group_id.clone(),
                    topic_partition,
                    next_offset: 1,
                    metadata: String::new(),
                    commit_timestamp_ms: 1,
                    group_epoch: GroupEpoch::new(2).unwrap(),
                    member_id: member(1),
                    member_epoch: MemberEpoch::new(1).unwrap(),
                },
            );
        }
        let before = state.values.len();
        let result = state.prepare_commit(
            &group,
            &authority(),
            &[
                CommitOffsetV0 {
                    topic_id: tp(0).topic_id,
                    partition: 0,
                    next_offset: 1,
                    metadata: String::new(),
                },
                CommitOffsetV0 {
                    topic_id: tp(1).topic_id,
                    partition: 1,
                    next_offset: 2,
                    metadata: String::new(),
                },
            ],
            2,
        );
        assert!(matches!(
            result,
            Err(OffsetStateError::InvalidOffsetCommit(
                "offset logical-state bound would be exceeded by atomic batch"
            ))
        ));
        assert_eq!(state.values.len(), before);
    }

    #[derive(Debug, Clone, Copy, PartialEq, Eq)]
    enum FenceModelEvent {
        AdvanceGroup,
        StaleOldEpochValue,
        SwitchOwner,
        OldOwnerSameEpochValue,
        NewOwnerValue,
    }

    #[derive(Debug, Clone)]
    enum AcceptedModelRecord {
        GroupFence(OffsetGroupFenceV1),
        OwnerFence(OffsetOwnerFenceV1),
        Value(OffsetValueV1),
    }

    fn replay_model(history: &[AcceptedModelRecord]) -> OffsetStateMachine {
        let mut replay = OffsetStateMachine::new(GroupId::new("offsets-model").unwrap());
        let initial_fence = OffsetGroupFenceV1 {
            group_id: GroupId::new("offsets-model").unwrap(),
            group_epoch: GroupEpoch::new(2).unwrap(),
            source_group_state_version: GroupStateVersion::new(2).unwrap(),
            owners: BTreeMap::from([(tp(0), (member(1), MemberEpoch::new(1).unwrap()))]),
        };
        replay.apply_committed_group_fence(initial_fence).unwrap();
        let initial = OffsetValueV1 {
            group_id: GroupId::new("offsets-model").unwrap(),
            topic_partition: tp(0),
            next_offset: 10,
            metadata: "initial".into(),
            commit_timestamp_ms: 1,
            group_epoch: GroupEpoch::new(2).unwrap(),
            member_id: member(1),
            member_epoch: MemberEpoch::new(1).unwrap(),
        };
        replay.apply_committed_value(initial).unwrap();
        for record in history {
            match record {
                AcceptedModelRecord::GroupFence(fence) => {
                    replay.apply_committed_group_fence(fence.clone()).unwrap();
                }
                AcceptedModelRecord::OwnerFence(fence) => {
                    replay.apply_committed_owner_fence(fence.clone()).unwrap();
                }
                AcceptedModelRecord::Value(value) => {
                    replay.apply_committed_value(value.clone()).unwrap();
                }
            }
        }
        replay
    }

    fn model_event_record(event: FenceModelEvent) -> AcceptedModelRecord {
        match event {
            FenceModelEvent::AdvanceGroup => AcceptedModelRecord::GroupFence(OffsetGroupFenceV1 {
                group_id: GroupId::new("offsets-model").unwrap(),
                group_epoch: GroupEpoch::new(3).unwrap(),
                source_group_state_version: GroupStateVersion::new(4).unwrap(),
                owners: BTreeMap::from([(tp(0), (member(1), MemberEpoch::new(1).unwrap()))]),
            }),
            FenceModelEvent::SwitchOwner => AcceptedModelRecord::OwnerFence(OffsetOwnerFenceV1 {
                group_id: GroupId::new("offsets-model").unwrap(),
                topic_partition: tp(0),
                group_epoch: GroupEpoch::new(3).unwrap(),
                member_id: member(2),
                member_epoch: MemberEpoch::new(1).unwrap(),
                source_group_state_version: GroupStateVersion::new(5).unwrap(),
            }),
            FenceModelEvent::StaleOldEpochValue => AcceptedModelRecord::Value(OffsetValueV1 {
                group_id: GroupId::new("offsets-model").unwrap(),
                topic_partition: tp(0),
                next_offset: 20,
                metadata: "old-epoch".into(),
                commit_timestamp_ms: 2,
                group_epoch: GroupEpoch::new(2).unwrap(),
                member_id: member(1),
                member_epoch: MemberEpoch::new(1).unwrap(),
            }),
            FenceModelEvent::OldOwnerSameEpochValue => AcceptedModelRecord::Value(OffsetValueV1 {
                group_id: GroupId::new("offsets-model").unwrap(),
                topic_partition: tp(0),
                next_offset: 30,
                metadata: "old-owner".into(),
                commit_timestamp_ms: 3,
                group_epoch: GroupEpoch::new(3).unwrap(),
                member_id: member(1),
                member_epoch: MemberEpoch::new(1).unwrap(),
            }),
            FenceModelEvent::NewOwnerValue => AcceptedModelRecord::Value(OffsetValueV1 {
                group_id: GroupId::new("offsets-model").unwrap(),
                topic_partition: tp(0),
                next_offset: 40,
                metadata: "new-owner".into(),
                commit_timestamp_ms: 4,
                group_epoch: GroupEpoch::new(3).unwrap(),
                member_id: member(2),
                member_epoch: MemberEpoch::new(1).unwrap(),
            }),
        }
    }

    fn event_permutations(
        prefix: &mut Vec<FenceModelEvent>,
        remaining: &mut Vec<FenceModelEvent>,
        output: &mut Vec<Vec<FenceModelEvent>>,
    ) {
        if remaining.is_empty() {
            output.push(prefix.clone());
            return;
        }
        for index in 0..remaining.len() {
            let event = remaining.remove(index);
            prefix.push(event);
            event_permutations(prefix, remaining, output);
            prefix.pop();
            remaining.insert(index, event);
        }
    }

    #[test]
    fn bounded_offset_fence_model_rejects_stale_writers_under_every_event_order() {
        let events = vec![
            FenceModelEvent::AdvanceGroup,
            FenceModelEvent::StaleOldEpochValue,
            FenceModelEvent::SwitchOwner,
            FenceModelEvent::OldOwnerSameEpochValue,
            FenceModelEvent::NewOwnerValue,
        ];
        let mut permutations = Vec::new();
        event_permutations(&mut Vec::new(), &mut events.clone(), &mut permutations);
        assert_eq!(permutations.len(), 120);

        for order in permutations {
            let mut state = replay_model(&[]);
            let mut accepted = Vec::new();
            let mut group_advanced = false;
            let mut owner_switched = false;

            for event in order {
                let record = model_event_record(event);
                let expected_accept = match event {
                    FenceModelEvent::AdvanceGroup => true,
                    FenceModelEvent::StaleOldEpochValue => !group_advanced,
                    FenceModelEvent::SwitchOwner => group_advanced,
                    FenceModelEvent::OldOwnerSameEpochValue => group_advanced && !owner_switched,
                    FenceModelEvent::NewOwnerValue => owner_switched,
                };
                let result = match &record {
                    AcceptedModelRecord::GroupFence(fence) => {
                        state.apply_committed_group_fence(fence.clone())
                    }
                    AcceptedModelRecord::OwnerFence(fence) => {
                        state.apply_committed_owner_fence(fence.clone())
                    }
                    AcceptedModelRecord::Value(value) => state.apply_committed_value(value.clone()),
                };
                assert_eq!(
                    result.is_ok(),
                    expected_accept,
                    "unexpected fence-model outcome for {event:?}"
                );
                if result.is_ok() {
                    match event {
                        FenceModelEvent::AdvanceGroup => group_advanced = true,
                        FenceModelEvent::SwitchOwner => owner_switched = true,
                        _ => {}
                    }
                    accepted.push(record);
                }

                let replay = replay_model(&accepted);
                assert_eq!(state.group_fence(), replay.group_fence());
                assert_eq!(state.owner_fence(tp(0)), replay.owner_fence(tp(0)));
                assert_eq!(state.value(tp(0)), replay.value(tp(0)));
                if group_advanced {
                    assert_eq!(state.group_fence().unwrap().group_epoch.get(), 3);
                }
                if owner_switched {
                    assert_eq!(state.effective_owner(tp(0)).unwrap().0, member(2));
                }
            }
        }
    }
}
