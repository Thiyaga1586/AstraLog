use std::{collections::BTreeMap, fmt};

use astralog_protocol::api::{
    GroupEpoch, GroupStateVersion, MAX_MEMBER_ASSIGNMENT_PARTITIONS, MemberAuthorityV0,
    MemberEpoch, MemberId, TopicPartitionV0,
};

use crate::{
    GroupPhase, GroupRecordV1, GroupStateError, GroupStateMachine, InternalRecordV1,
    OffsetGroupFenceV1, OffsetOwnerFenceV1,
};

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CooperativeError {
    Group(GroupStateError),
    WrongPhase(&'static str),
    MissingPreparedEpoch,
    FenceRegression,
    FenceConflict,
    FenceMismatch(&'static str),
    OutstandingOwnerFence,
    InvalidRevokeAck(&'static str),
    NonCanonicalRevokeAck,
    NoReadyActivation,
    RecoveryHistory(&'static str),
}

impl fmt::Display for CooperativeError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Group(error) => write!(f, "{error}"),
            Self::WrongPhase(message) => write!(f, "invalid cooperative phase: {message}"),
            Self::MissingPreparedEpoch => f.write_str("prepared group epoch is missing"),
            Self::FenceRegression => f.write_str("cooperative fence epoch regressed"),
            Self::FenceConflict => f.write_str("conflicting cooperative fence"),
            Self::FenceMismatch(message) => write!(f, "cooperative fence mismatch: {message}"),
            Self::OutstandingOwnerFence => f.write_str(
                "committed owner fence must activate or be superseded before another release",
            ),
            Self::InvalidRevokeAck(message) => {
                write!(f, "invalid revoke acknowledgement: {message}")
            }
            Self::NonCanonicalRevokeAck => {
                f.write_str("revoke acknowledgement set is not canonical")
            }
            Self::NoReadyActivation => {
                f.write_str("no durably fenced target owner is ready to activate")
            }
            Self::RecoveryHistory(message) => {
                write!(f, "invalid cooperative recovery history: {message}")
            }
        }
    }
}

impl std::error::Error for CooperativeError {}

impl From<GroupStateError> for CooperativeError {
    fn from(value: GroupStateError) -> Self {
        Self::Group(value)
    }
}

pub type CooperativeResult<T> = core::result::Result<T, CooperativeError>;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CooperativeApplyOutcome {
    Applied,
    Idempotent,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct ReleasedOwner {
    group_epoch: GroupEpoch,
    member_id: MemberId,
    member_epoch: MemberEpoch,
}

#[derive(Debug, Clone)]
pub struct CooperativeStateMachine {
    group: GroupStateMachine,
    last_group_fence: Option<OffsetGroupFenceV1>,
    owner_fences: BTreeMap<TopicPartitionV0, OffsetOwnerFenceV1>,
    released_by: BTreeMap<TopicPartitionV0, ReleasedOwner>,
}

impl CooperativeStateMachine {
    pub fn from_group_record(record: GroupRecordV1) -> CooperativeResult<Self> {
        Ok(Self {
            group: GroupStateMachine::from_snapshot(record)?,
            last_group_fence: None,
            owner_fences: BTreeMap::new(),
            released_by: BTreeMap::new(),
        })
    }

    pub fn recover(
        group_records: &[GroupRecordV1],
        group_fences: &[OffsetGroupFenceV1],
        owner_fences: &[OffsetOwnerFenceV1],
    ) -> CooperativeResult<Self> {
        Self::recover_inner(group_records, group_fences, owner_fences, false)
    }

    pub fn recover_workflow_suffix(
        group_records: &[GroupRecordV1],
        group_fences: &[OffsetGroupFenceV1],
        owner_fences: &[OffsetOwnerFenceV1],
    ) -> CooperativeResult<Self> {
        Self::recover_inner(group_records, group_fences, owner_fences, true)
    }

    fn recover_inner(
        group_records: &[GroupRecordV1],
        group_fences: &[OffsetGroupFenceV1],
        owner_fences: &[OffsetOwnerFenceV1],
        workflow_suffix: bool,
    ) -> CooperativeResult<Self> {
        let first = group_records
            .first()
            .ok_or(CooperativeError::RecoveryHistory("group history is empty"))?;
        let mut group = if workflow_suffix {
            if first.phase != GroupPhase::PreparingEpochFence {
                return Err(CooperativeError::RecoveryHistory(
                    "workflow suffix must begin at PREPARING_EPOCH_FENCE",
                ));
            }
            GroupStateMachine::from_snapshot(first.clone())?
        } else {
            GroupStateMachine::new(first.group_id.clone())
        };
        let mut by_version = BTreeMap::new();
        if workflow_suffix {
            by_version.insert(first.state_version.get(), first.clone());
        }
        for record in group_records.iter().skip(usize::from(workflow_suffix)) {
            if record.group_id != first.group_id {
                return Err(CooperativeError::RecoveryHistory(
                    "group history contains multiple group ids",
                ));
            }
            group.apply_committed(record.clone())?;
            by_version.insert(record.state_version.get(), record.clone());
        }

        let mut fences_by_epoch = BTreeMap::<u64, OffsetGroupFenceV1>::new();
        let mut latest_epoch = 0_u64;
        for fence in group_fences {
            InternalRecordV1::OffsetGroupFence(fence.clone())
                .encode()
                .map_err(GroupStateError::from)?;
            if fence.group_id != first.group_id {
                return Err(CooperativeError::FenceMismatch("group id differs"));
            }
            let epoch = fence.group_epoch.get();
            if epoch < latest_epoch {
                return Err(CooperativeError::FenceRegression);
            }
            if let Some(existing) = fences_by_epoch.get(&epoch) {
                if existing != fence {
                    return Err(CooperativeError::FenceConflict);
                }
                continue;
            }
            let source = by_version
                .get(&fence.source_group_state_version.get())
                .ok_or(CooperativeError::RecoveryHistory(
                    "group-fence source version is absent",
                ))?;
            if required_group_fence(source)? != *fence {
                return Err(CooperativeError::FenceMismatch(
                    "group fence does not match its prepared source state",
                ));
            }
            fences_by_epoch.insert(epoch, fence.clone());
            latest_epoch = epoch;
        }

        let mut all_owner_fences = BTreeMap::<(u64, TopicPartitionV0), OffsetOwnerFenceV1>::new();
        for fence in owner_fences {
            InternalRecordV1::OffsetOwnerFence(fence.clone())
                .encode()
                .map_err(GroupStateError::from)?;
            if fence.group_id != first.group_id {
                return Err(CooperativeError::FenceMismatch("group id differs"));
            }
            let source = by_version
                .get(&fence.source_group_state_version.get())
                .ok_or(CooperativeError::RecoveryHistory(
                    "owner-fence source version is absent",
                ))?;
            validate_owner_fence_against_source(source, fence, &fences_by_epoch)?;
            let key = (fence.group_epoch.get(), fence.topic_partition);
            if let Some(existing) = all_owner_fences.get(&key) {
                if existing != fence {
                    return Err(CooperativeError::FenceConflict);
                }
                continue;
            }
            all_owner_fences.insert(key, fence.clone());
        }

        validate_cross_log_order(group_records, &fences_by_epoch, &all_owner_fences)?;

        let current = group.current().ok_or(CooperativeError::RecoveryHistory(
            "group history did not apply",
        ))?;
        let active_epoch = current.active_group_epoch;
        let mut released_by = BTreeMap::new();
        for pair in group_records.windows(2) {
            let previous = &pair[0];
            let next = &pair[1];
            if next.active_group_epoch != active_epoch
                || previous.active_group_epoch != active_epoch
            {
                continue;
            }
            if next.phase == GroupPhase::PreparingEpochFence {
                continue;
            }
            for (partition, old_owner) in &previous.active_assignments {
                if next.active_assignments.contains_key(partition) {
                    continue;
                }
                if previous.pending_revocations.get(partition) != Some(old_owner) {
                    continue;
                }
                let old_epoch = previous
                    .members
                    .get(old_owner)
                    .ok_or(CooperativeError::RecoveryHistory(
                        "released owner is absent from previous membership",
                    ))?
                    .member_epoch;
                released_by.insert(
                    *partition,
                    ReleasedOwner {
                        group_epoch: GroupEpoch::new(active_epoch).map_err(|_| {
                            CooperativeError::RecoveryHistory("released owner epoch is invalid")
                        })?,
                        member_id: *old_owner,
                        member_epoch: old_epoch,
                    },
                );
            }
        }

        let last_group_fence = fences_by_epoch.values().next_back().cloned();
        let mut current_owner_fences = BTreeMap::new();
        for ((epoch, partition), fence) in all_owner_fences {
            if epoch == active_epoch
                && current.target_assignments.get(&partition) == Some(&fence.member_id)
            {
                current_owner_fences.insert(partition, fence);
            }
        }

        Ok(Self {
            group,
            last_group_fence,
            owner_fences: current_owner_fences,
            released_by,
        })
    }

    /// Recovers one already-validated bounded cooperative workflow slice.
    ///
    /// The caller must have replay-validated the complete committed group log
    /// before selecting this slice. The first record is therefore a trusted
    /// `PREPARING_EPOCH_FENCE` snapshot whose state version may be greater than
    /// one; all later records are still replayed canonically from that snapshot.
    /// This lets broker orchestration recover the current handoff without
    /// retaining the group's unbounded lifetime history in memory.
    pub fn recover_validated_workflow(
        group_records: &[GroupRecordV1],
        group_fences: &[OffsetGroupFenceV1],
        owner_fences: &[OffsetOwnerFenceV1],
    ) -> CooperativeResult<Self> {
        let first = group_records
            .first()
            .ok_or(CooperativeError::RecoveryHistory("group history is empty"))?;
        if first.phase != GroupPhase::PreparingEpochFence {
            return Err(CooperativeError::RecoveryHistory(
                "bounded workflow must begin at PREPARING_EPOCH_FENCE",
            ));
        }
        let mut group = GroupStateMachine::from_snapshot(first.clone())?;
        let mut by_version = BTreeMap::from([(first.state_version.get(), first.clone())]);
        for record in group_records.iter().skip(1) {
            if record.group_id != first.group_id {
                return Err(CooperativeError::RecoveryHistory(
                    "group history contains multiple group ids",
                ));
            }
            group.apply_committed(record.clone())?;
            by_version.insert(record.state_version.get(), record.clone());
        }

        let mut fences_by_epoch = BTreeMap::<u64, OffsetGroupFenceV1>::new();
        let mut latest_epoch = 0_u64;
        for fence in group_fences {
            InternalRecordV1::OffsetGroupFence(fence.clone())
                .encode()
                .map_err(GroupStateError::from)?;
            if fence.group_id != first.group_id {
                return Err(CooperativeError::FenceMismatch("group id differs"));
            }
            let epoch = fence.group_epoch.get();
            if epoch < latest_epoch {
                return Err(CooperativeError::FenceRegression);
            }
            if let Some(existing) = fences_by_epoch.get(&epoch) {
                if existing != fence {
                    return Err(CooperativeError::FenceConflict);
                }
                continue;
            }
            let source = by_version
                .get(&fence.source_group_state_version.get())
                .ok_or(CooperativeError::RecoveryHistory(
                    "group-fence source version is absent",
                ))?;
            if required_group_fence(source)? != *fence {
                return Err(CooperativeError::FenceMismatch(
                    "group fence does not match its prepared source state",
                ));
            }
            fences_by_epoch.insert(epoch, fence.clone());
            latest_epoch = epoch;
        }

        let mut all_owner_fences = BTreeMap::<(u64, TopicPartitionV0), OffsetOwnerFenceV1>::new();
        for fence in owner_fences {
            InternalRecordV1::OffsetOwnerFence(fence.clone())
                .encode()
                .map_err(GroupStateError::from)?;
            if fence.group_id != first.group_id {
                return Err(CooperativeError::FenceMismatch("group id differs"));
            }
            let source = by_version
                .get(&fence.source_group_state_version.get())
                .ok_or(CooperativeError::RecoveryHistory(
                    "owner-fence source version is absent",
                ))?;
            validate_owner_fence_against_source(source, fence, &fences_by_epoch)?;
            let key = (fence.group_epoch.get(), fence.topic_partition);
            if let Some(existing) = all_owner_fences.get(&key) {
                if existing != fence {
                    return Err(CooperativeError::FenceConflict);
                }
                continue;
            }
            all_owner_fences.insert(key, fence.clone());
        }

        validate_cross_log_order(group_records, &fences_by_epoch, &all_owner_fences)?;

        let current = group.current().ok_or(CooperativeError::RecoveryHistory(
            "group history did not apply",
        ))?;
        let active_epoch = current.active_group_epoch;
        let mut released_by = BTreeMap::new();
        for pair in group_records.windows(2) {
            let previous = &pair[0];
            let next = &pair[1];
            if next.active_group_epoch != active_epoch
                || previous.active_group_epoch != active_epoch
            {
                continue;
            }
            if next.phase == GroupPhase::PreparingEpochFence {
                continue;
            }
            for (partition, old_owner) in &previous.active_assignments {
                if next.active_assignments.contains_key(partition) {
                    continue;
                }
                if previous.pending_revocations.get(partition) != Some(old_owner) {
                    continue;
                }
                let old_epoch = previous
                    .members
                    .get(old_owner)
                    .ok_or(CooperativeError::RecoveryHistory(
                        "released owner is absent from previous membership",
                    ))?
                    .member_epoch;
                released_by.insert(
                    *partition,
                    ReleasedOwner {
                        group_epoch: GroupEpoch::new(active_epoch).map_err(|_| {
                            CooperativeError::RecoveryHistory("released owner epoch is invalid")
                        })?,
                        member_id: *old_owner,
                        member_epoch: old_epoch,
                    },
                );
            }
        }

        let last_group_fence = fences_by_epoch.values().next_back().cloned();
        let mut current_owner_fences = BTreeMap::new();
        for ((epoch, partition), fence) in all_owner_fences {
            if epoch == active_epoch
                && current.target_assignments.get(&partition) == Some(&fence.member_id)
            {
                current_owner_fences.insert(partition, fence);
            }
        }

        Ok(Self {
            group,
            last_group_fence,
            owner_fences: current_owner_fences,
            released_by,
        })
    }

    pub fn current(&self) -> &GroupRecordV1 {
        self.group
            .current()
            .expect("cooperative state machine always owns a group record")
    }

    pub fn required_group_fence(&self) -> CooperativeResult<OffsetGroupFenceV1> {
        required_group_fence(self.current())
    }

    pub fn apply_committed_group_fence(
        &mut self,
        fence: OffsetGroupFenceV1,
    ) -> CooperativeResult<CooperativeApplyOutcome> {
        if let Some(existing) = &self.last_group_fence {
            if fence.group_epoch.get() < existing.group_epoch.get() {
                return Err(CooperativeError::FenceRegression);
            }
            if fence.group_epoch == existing.group_epoch {
                return if *existing == fence {
                    Ok(CooperativeApplyOutcome::Idempotent)
                } else {
                    Err(CooperativeError::FenceConflict)
                };
            }
        }
        let required = self.required_group_fence()?;
        if fence.group_id != required.group_id || fence.group_epoch != required.group_epoch {
            return Err(CooperativeError::FenceMismatch(
                "group fence does not target current prepared epoch",
            ));
        }
        if fence != required {
            return Err(CooperativeError::FenceMismatch(
                "group fence bytes differ from prepared active-owner snapshot",
            ));
        }
        self.last_group_fence = Some(fence);
        Ok(CooperativeApplyOutcome::Applied)
    }

    pub fn activate_prepared_epoch(&mut self) -> CooperativeResult<GroupRecordV1> {
        let current = self.current().clone();
        let required = required_group_fence(&current)?;
        if self.last_group_fence.as_ref() != Some(&required) {
            return Err(CooperativeError::FenceMismatch(
                "prepared epoch cannot activate before matching group fence commits",
            ));
        }
        let next = activation_candidate(&current)?;
        self.group.apply_committed(next.clone())?;
        self.owner_fences.clear();
        self.released_by.clear();
        Ok(next)
    }

    pub fn revocations_for(&self, member_id: MemberId) -> Vec<TopicPartitionV0> {
        self.current()
            .pending_revocations
            .iter()
            .filter_map(|(partition, owner)| (*owner == member_id).then_some(*partition))
            .collect()
    }

    pub fn revoke_ack(
        &mut self,
        authority: &MemberAuthorityV0,
        revoked: &[TopicPartitionV0],
    ) -> CooperativeResult<CooperativeApplyOutcome> {
        let current = self.current().clone();
        validate_member_authority(&current, authority)?;
        validate_revoked_set(revoked)?;

        let mut to_release = Vec::new();
        for partition in revoked {
            let pending_owner = current.pending_revocations.get(partition).copied();
            let active_owner = current.active_assignments.get(partition).copied();
            if pending_owner == Some(authority.member_id)
                && active_owner == Some(authority.member_id)
            {
                to_release.push(*partition);
                continue;
            }
            if let Some(released) = self.released_by.get(partition)
                && released.group_epoch == authority.group_epoch
                && released.member_id == authority.member_id
                && released.member_epoch == authority.member_epoch
                && pending_owner.is_none()
                && active_owner != Some(authority.member_id)
            {
                continue;
            }
            return Err(CooperativeError::InvalidRevokeAck(
                "partition is not pending revocation from this exact member authority",
            ));
        }

        if to_release.is_empty() {
            return Ok(CooperativeApplyOutcome::Idempotent);
        }
        if current.phase != GroupPhase::Revoking {
            return Err(CooperativeError::WrongPhase("RevokeAck requires REVOKING"));
        }
        if self.has_unactivated_owner_fence() {
            return Err(CooperativeError::OutstandingOwnerFence);
        }

        let mut next = current.clone();
        next.state_version = next_state_version(&current)?;
        for partition in &to_release {
            next.active_assignments.remove(partition);
            next.pending_revocations.remove(partition);
        }
        next.phase = phase_after_progress(&next);
        self.group.apply_committed(next.clone())?;
        for partition in to_release {
            self.released_by.insert(
                partition,
                ReleasedOwner {
                    group_epoch: authority.group_epoch,
                    member_id: authority.member_id,
                    member_epoch: authority.member_epoch,
                },
            );
        }
        Ok(CooperativeApplyOutcome::Applied)
    }

    pub fn required_owner_fences(&self) -> CooperativeResult<Vec<OffsetOwnerFenceV1>> {
        let current = self.current();
        if current.phase == GroupPhase::Stable {
            return Ok(Vec::new());
        }
        if current.phase != GroupPhase::Revoking {
            return Err(CooperativeError::WrongPhase(
                "owner fences require REVOKING",
            ));
        }
        let group_fence = self
            .last_group_fence
            .as_ref()
            .ok_or(CooperativeError::FenceMismatch(
                "active group epoch has no committed group fence",
            ))?;
        if group_fence.group_epoch.get() != current.active_group_epoch {
            return Err(CooperativeError::FenceMismatch(
                "latest group fence does not match active group epoch",
            ));
        }

        let group_epoch = GroupEpoch::new(current.active_group_epoch)
            .map_err(|_| CooperativeError::FenceMismatch("active group epoch is invalid"))?;
        let mut required = Vec::new();
        for (partition, target_member) in &current.target_assignments {
            if current.active_assignments.get(partition) == Some(target_member)
                || current.pending_revocations.contains_key(partition)
            {
                continue;
            }
            let member_epoch = current
                .members
                .get(target_member)
                .ok_or(CooperativeError::FenceMismatch(
                    "target owner is absent from committed membership",
                ))?
                .member_epoch;
            let candidate = OffsetOwnerFenceV1 {
                group_id: current.group_id.clone(),
                topic_partition: *partition,
                group_epoch,
                member_id: *target_member,
                member_epoch,
                source_group_state_version: current.state_version,
            };
            if let Some(existing) = self.owner_fences.get(partition) {
                if existing.member_id != candidate.member_id
                    || existing.member_epoch != candidate.member_epoch
                    || existing.group_epoch != candidate.group_epoch
                {
                    return Err(CooperativeError::FenceConflict);
                }
                continue;
            }
            required.push(candidate);
        }
        Ok(required)
    }

    pub fn apply_committed_owner_fence(
        &mut self,
        fence: OffsetOwnerFenceV1,
    ) -> CooperativeResult<CooperativeApplyOutcome> {
        if let Some(existing) = self.owner_fences.get(&fence.topic_partition) {
            return if *existing == fence {
                Ok(CooperativeApplyOutcome::Idempotent)
            } else {
                Err(CooperativeError::FenceConflict)
            };
        }
        let current = self.current();
        validate_live_owner_fence(current, &fence, self.last_group_fence.as_ref())?;
        self.owner_fences.insert(fence.topic_partition, fence);
        Ok(CooperativeApplyOutcome::Applied)
    }

    pub fn activate_fenced_targets(&mut self) -> CooperativeResult<GroupRecordV1> {
        let current = self.current().clone();
        if current.phase != GroupPhase::Revoking {
            return Err(CooperativeError::WrongPhase(
                "target activation requires REVOKING",
            ));
        }
        let mut ready = Vec::new();
        for (partition, target_member) in &current.target_assignments {
            if current.active_assignments.get(partition) == Some(target_member)
                || current.pending_revocations.contains_key(partition)
            {
                continue;
            }
            let Some(fence) = self.owner_fences.get(partition) else {
                continue;
            };
            let target_epoch = current
                .members
                .get(target_member)
                .ok_or(CooperativeError::FenceMismatch(
                    "target owner is absent from committed membership",
                ))?
                .member_epoch;
            if fence.group_epoch.get() != current.active_group_epoch
                || fence.member_id != *target_member
                || fence.member_epoch != target_epoch
            {
                return Err(CooperativeError::FenceConflict);
            }
            ready.push((*partition, *target_member));
        }
        if ready.is_empty() {
            return Err(CooperativeError::NoReadyActivation);
        }

        let mut next = current.clone();
        next.state_version = next_state_version(&current)?;
        for (partition, target_member) in &ready {
            if next
                .active_assignments
                .insert(*partition, *target_member)
                .is_some()
            {
                return Err(CooperativeError::FenceMismatch(
                    "target activation would overwrite an active owner",
                ));
            }
        }
        next.phase = phase_after_progress(&next);
        self.group.apply_committed(next.clone())?;
        Ok(next)
    }

    pub fn apply_superseding_prepared(
        &mut self,
        record: GroupRecordV1,
    ) -> CooperativeResult<GroupRecordV1> {
        let current = self.current().clone();
        if record.phase != GroupPhase::PreparingEpochFence {
            return Err(CooperativeError::WrongPhase(
                "superseding record must be PREPARING_EPOCH_FENCE",
            ));
        }
        if record.active_group_epoch != current.active_group_epoch {
            return Err(CooperativeError::FenceMismatch(
                "superseding prepare cannot change active group epoch",
            ));
        }
        let previous_ceiling = current
            .pending_group_epoch
            .map(GroupEpoch::get)
            .unwrap_or(current.active_group_epoch);
        let next_pending = record
            .pending_group_epoch
            .ok_or(CooperativeError::MissingPreparedEpoch)?
            .get();
        if next_pending <= previous_ceiling {
            return Err(CooperativeError::FenceRegression);
        }
        for (partition, owner) in &record.active_assignments {
            if current.active_assignments.get(partition) != Some(owner) {
                return Err(CooperativeError::FenceMismatch(
                    "superseding prepare cannot add or transfer active ownership",
                ));
            }
        }
        for (partition, old_owner) in &current.active_assignments {
            if record.active_assignments.contains_key(partition) {
                continue;
            }
            if record.members.contains_key(old_owner) {
                return Err(CooperativeError::FenceMismatch(
                    "superseding prepare may release ownership only by removing its member authority",
                ));
            }
        }
        self.group.apply_committed(record.clone())?;
        self.owner_fences.clear();
        self.released_by.clear();
        Ok(record)
    }

    fn has_unactivated_owner_fence(&self) -> bool {
        let current = self.current();
        self.owner_fences.iter().any(|(partition, fence)| {
            current.active_assignments.get(partition) != Some(&fence.member_id)
        })
    }
}

fn required_group_fence(record: &GroupRecordV1) -> CooperativeResult<OffsetGroupFenceV1> {
    if record.phase != GroupPhase::PreparingEpochFence {
        return Err(CooperativeError::WrongPhase(
            "group fence requires PREPARING_EPOCH_FENCE",
        ));
    }
    let group_epoch = record
        .pending_group_epoch
        .ok_or(CooperativeError::MissingPreparedEpoch)?;
    let mut owners = BTreeMap::new();
    for (partition, member_id) in &record.active_assignments {
        let member_epoch = record
            .members
            .get(member_id)
            .ok_or(CooperativeError::FenceMismatch(
                "active owner is absent from committed membership",
            ))?
            .member_epoch;
        owners.insert(*partition, (*member_id, member_epoch));
    }
    Ok(OffsetGroupFenceV1 {
        group_id: record.group_id.clone(),
        group_epoch,
        source_group_state_version: record.state_version,
        owners,
    })
}

fn activation_candidate(current: &GroupRecordV1) -> CooperativeResult<GroupRecordV1> {
    if current.phase != GroupPhase::PreparingEpochFence {
        return Err(CooperativeError::WrongPhase(
            "epoch activation requires PREPARING_EPOCH_FENCE",
        ));
    }
    let pending = current
        .pending_group_epoch
        .ok_or(CooperativeError::MissingPreparedEpoch)?;
    let mut next = current.clone();
    next.state_version = next_state_version(current)?;
    next.active_group_epoch = pending.get();
    next.pending_group_epoch = None;
    next.pending_revocations = current
        .active_assignments
        .iter()
        .filter_map(|(partition, old_owner)| {
            (current.target_assignments.get(partition) != Some(old_owner))
                .then_some((*partition, *old_owner))
        })
        .collect();
    next.phase = phase_after_progress(&next);
    Ok(next)
}

fn phase_after_progress(record: &GroupRecordV1) -> GroupPhase {
    if record.pending_revocations.is_empty()
        && record.active_assignments == record.target_assignments
    {
        GroupPhase::Stable
    } else {
        GroupPhase::Revoking
    }
}

fn next_state_version(current: &GroupRecordV1) -> CooperativeResult<GroupStateVersion> {
    current.state_version.checked_next().map_err(|_| {
        CooperativeError::Group(GroupStateError::InvalidTransition("state version overflow"))
    })
}

fn validate_member_authority(
    current: &GroupRecordV1,
    authority: &MemberAuthorityV0,
) -> CooperativeResult<()> {
    if authority.group_id != current.group_id {
        return Err(CooperativeError::FenceMismatch("group id differs"));
    }
    if authority.group_epoch.get() != current.active_group_epoch {
        return Err(CooperativeError::Group(GroupStateError::GroupEpochStale {
            expected: authority.group_epoch.get(),
            actual: current.active_group_epoch,
        }));
    }
    let member = current
        .members
        .get(&authority.member_id)
        .ok_or(CooperativeError::Group(GroupStateError::UnknownMember))?;
    if member.member_epoch != authority.member_epoch {
        return Err(CooperativeError::Group(GroupStateError::MemberEpochStale {
            expected: authority.member_epoch.get(),
            actual: member.member_epoch.get(),
        }));
    }
    Ok(())
}

fn validate_revoked_set(revoked: &[TopicPartitionV0]) -> CooperativeResult<()> {
    if revoked.is_empty() || revoked.len() > MAX_MEMBER_ASSIGNMENT_PARTITIONS {
        return Err(CooperativeError::InvalidRevokeAck(
            "revoked count is outside frozen 1..=1024 bound",
        ));
    }
    if revoked.windows(2).any(|pair| pair[0] >= pair[1]) {
        return Err(CooperativeError::NonCanonicalRevokeAck);
    }
    Ok(())
}

fn validate_live_owner_fence(
    current: &GroupRecordV1,
    fence: &OffsetOwnerFenceV1,
    last_group_fence: Option<&OffsetGroupFenceV1>,
) -> CooperativeResult<()> {
    if current.phase != GroupPhase::Revoking {
        return Err(CooperativeError::WrongPhase(
            "owner fence requires REVOKING",
        ));
    }
    if fence.group_id != current.group_id || fence.group_epoch.get() != current.active_group_epoch {
        return Err(CooperativeError::FenceMismatch(
            "owner fence group authority differs",
        ));
    }
    if fence.source_group_state_version != current.state_version {
        return Err(CooperativeError::FenceMismatch(
            "owner fence must cite the exact released group-state version",
        ));
    }
    if current
        .active_assignments
        .contains_key(&fence.topic_partition)
        || current
            .pending_revocations
            .contains_key(&fence.topic_partition)
    {
        return Err(CooperativeError::FenceMismatch(
            "owner fence cannot precede durable release",
        ));
    }
    if current.target_assignments.get(&fence.topic_partition) != Some(&fence.member_id) {
        return Err(CooperativeError::FenceMismatch(
            "owner fence target differs from committed target assignment",
        ));
    }
    let target_epoch = current
        .members
        .get(&fence.member_id)
        .ok_or(CooperativeError::FenceMismatch(
            "owner-fence target is absent from membership",
        ))?
        .member_epoch;
    if fence.member_epoch != target_epoch {
        return Err(CooperativeError::FenceMismatch(
            "owner-fence member epoch differs from committed target authority",
        ));
    }
    if last_group_fence
        .is_none_or(|group_fence| group_fence.group_epoch.get() != current.active_group_epoch)
    {
        return Err(CooperativeError::FenceMismatch(
            "owner fence has no committed group fence for active epoch",
        ));
    }
    Ok(())
}

fn validate_owner_fence_against_source(
    source: &GroupRecordV1,
    fence: &OffsetOwnerFenceV1,
    group_fences: &BTreeMap<u64, OffsetGroupFenceV1>,
) -> CooperativeResult<()> {
    if source.group_id != fence.group_id
        || source.active_group_epoch != fence.group_epoch.get()
        || source.phase != GroupPhase::Revoking
    {
        return Err(CooperativeError::FenceMismatch(
            "owner-fence source group authority differs",
        ));
    }
    if source
        .active_assignments
        .contains_key(&fence.topic_partition)
        || source
            .pending_revocations
            .contains_key(&fence.topic_partition)
    {
        return Err(CooperativeError::FenceMismatch(
            "owner-fence source does not prove release",
        ));
    }
    if source.target_assignments.get(&fence.topic_partition) != Some(&fence.member_id) {
        return Err(CooperativeError::FenceMismatch(
            "owner-fence source target differs",
        ));
    }
    let member_epoch = source
        .members
        .get(&fence.member_id)
        .ok_or(CooperativeError::FenceMismatch(
            "owner-fence source target member is absent",
        ))?
        .member_epoch;
    if member_epoch != fence.member_epoch {
        return Err(CooperativeError::FenceMismatch(
            "owner-fence source member epoch differs",
        ));
    }
    if !group_fences.contains_key(&fence.group_epoch.get()) {
        return Err(CooperativeError::FenceMismatch(
            "owner fence has no group fence for its epoch",
        ));
    }
    Ok(())
}

fn validate_cross_log_order(
    group_records: &[GroupRecordV1],
    group_fences: &BTreeMap<u64, OffsetGroupFenceV1>,
    owner_fences: &BTreeMap<(u64, TopicPartitionV0), OffsetOwnerFenceV1>,
) -> CooperativeResult<()> {
    for pair in group_records.windows(2) {
        let previous = &pair[0];
        let next = &pair[1];
        if next.phase == GroupPhase::PreparingEpochFence {
            validate_preparing_supersession(previous, next)?;
            continue;
        }
        if next.active_group_epoch > previous.active_group_epoch {
            let expected = activation_candidate(previous)?;
            if next != &expected {
                return Err(CooperativeError::RecoveryHistory(
                    "epoch activation is not the canonical post-fence transition",
                ));
            }
            if group_fences.get(&next.active_group_epoch) != Some(&required_group_fence(previous)?)
            {
                return Err(CooperativeError::RecoveryHistory(
                    "group epoch activated before matching group fence",
                ));
            }
            continue;
        }
        if next.active_group_epoch != previous.active_group_epoch {
            return Err(CooperativeError::RecoveryHistory(
                "active group epoch moved backward",
            ));
        }

        for (partition, old_owner) in &previous.active_assignments {
            if next.active_assignments.contains_key(partition) {
                continue;
            }
            if previous.pending_revocations.get(partition) != Some(old_owner) {
                return Err(CooperativeError::RecoveryHistory(
                    "active owner disappeared without pending revocation",
                ));
            }
        }
        for (partition, new_owner) in &next.active_assignments {
            if previous.active_assignments.contains_key(partition) {
                continue;
            }
            let fence = owner_fences
                .get(&(next.active_group_epoch, *partition))
                .ok_or(CooperativeError::RecoveryHistory(
                    "target owner activated without owner fence",
                ))?;
            if fence.member_id != *new_owner
                || fence.source_group_state_version.get() >= next.state_version.get()
            {
                return Err(CooperativeError::RecoveryHistory(
                    "target activation is not ordered after a matching owner fence",
                ));
            }
        }
    }
    Ok(())
}

fn validate_preparing_supersession(
    previous: &GroupRecordV1,
    next: &GroupRecordV1,
) -> CooperativeResult<()> {
    if next.active_group_epoch != previous.active_group_epoch {
        return Err(CooperativeError::RecoveryHistory(
            "prepared supersession changed active group epoch",
        ));
    }
    let previous_ceiling = previous
        .pending_group_epoch
        .map(GroupEpoch::get)
        .unwrap_or(previous.active_group_epoch);
    let next_pending = next
        .pending_group_epoch
        .ok_or(CooperativeError::RecoveryHistory(
            "prepared supersession lacks pending epoch",
        ))?
        .get();
    if next_pending <= previous_ceiling {
        return Err(CooperativeError::RecoveryHistory(
            "prepared supersession did not strictly advance epoch",
        ));
    }
    for (partition, owner) in &next.active_assignments {
        if previous.active_assignments.get(partition) != Some(owner) {
            return Err(CooperativeError::RecoveryHistory(
                "prepared supersession added or transferred active ownership",
            ));
        }
    }
    for (partition, old_owner) in &previous.active_assignments {
        if next.active_assignments.contains_key(partition) {
            continue;
        }
        if next.members.contains_key(old_owner) {
            return Err(CooperativeError::RecoveryHistory(
                "prepared supersession released a still-authoritative member",
            ));
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use std::collections::{BTreeSet, VecDeque};

    use astralog_common::TopicId;
    use astralog_protocol::api::{GroupId, MemberEpoch};

    use super::*;
    use crate::{GroupMutationPlan, MemberRecordV1};

    fn topic(byte: u8) -> TopicId {
        TopicId::new([byte; 16])
    }

    fn tp(partition: u32) -> TopicPartitionV0 {
        TopicPartitionV0 {
            topic_id: topic(1),
            partition,
        }
    }

    fn member(byte: u8) -> MemberId {
        MemberId::new([byte; 16]).unwrap()
    }

    fn member_record(byte: u8) -> MemberRecordV1 {
        MemberRecordV1 {
            member_id: member(byte),
            instance_id: None,
            member_epoch: MemberEpoch::new(1).unwrap(),
            session_timeout_ms: 30_000,
            subscriptions: vec![topic(1)],
        }
    }

    fn stable_two_member_group() -> GroupRecordV1 {
        let a = member(1);
        let b = member(2);
        let members = BTreeMap::from([(a, member_record(1)), (b, member_record(2))]);
        let active = BTreeMap::from([(tp(0), a), (tp(1), a), (tp(2), b), (tp(3), b)]);
        GroupRecordV1 {
            group_id: GroupId::new("cooperative").unwrap(),
            state_version: GroupStateVersion::new(1).unwrap(),
            active_group_epoch: 1,
            pending_group_epoch: None,
            phase: GroupPhase::Stable,
            members,
            active_assignments: active.clone(),
            target_assignments: active,
            pending_revocations: BTreeMap::new(),
        }
    }

    fn prepare_move() -> (GroupRecordV1, GroupRecordV1) {
        let stable = stable_two_member_group();
        let a = member(1);
        let b = member(2);
        let mut machine = GroupStateMachine::from_snapshot(stable.clone()).unwrap();
        let prepared = machine
            .apply(crate::GroupCommand::UpdateSubscription {
                expected_group_epoch: GroupEpoch::new(1).unwrap(),
                member_id: a,
                expected_member_epoch: MemberEpoch::new(1).unwrap(),
                subscriptions: vec![topic(1), topic(2)],
                plan: GroupMutationPlan {
                    target_assignments: BTreeMap::from([
                        (tp(0), a),
                        (tp(1), b),
                        (tp(2), b),
                        (tp(3), a),
                        (tp(4), b),
                    ]),
                },
            })
            .unwrap();
        (stable, prepared)
    }

    fn authority(group: &GroupRecordV1, member_id: MemberId) -> MemberAuthorityV0 {
        MemberAuthorityV0 {
            group_id: group.group_id.clone(),
            member_id,
            group_epoch: GroupEpoch::new(group.active_group_epoch).unwrap(),
            member_epoch: group.members[&member_id].member_epoch,
        }
    }

    #[test]
    fn prepared_group_fence_snapshots_exact_current_owners_before_epoch_activation() {
        let (_, prepared) = prepare_move();
        let mut cooperative = CooperativeStateMachine::from_group_record(prepared.clone()).unwrap();
        let fence = cooperative.required_group_fence().unwrap();
        assert_eq!(fence.group_epoch.get(), 2);
        assert_eq!(fence.source_group_state_version, prepared.state_version);
        assert_eq!(fence.owners.len(), prepared.active_assignments.len());
        for (partition, owner) in &prepared.active_assignments {
            assert_eq!(
                fence.owners.get(partition),
                Some(&(*owner, prepared.members[owner].member_epoch))
            );
        }
        assert!(cooperative.activate_prepared_epoch().is_err());
        assert_eq!(
            cooperative
                .apply_committed_group_fence(fence.clone())
                .unwrap(),
            CooperativeApplyOutcome::Applied
        );
        assert_eq!(
            cooperative.apply_committed_group_fence(fence).unwrap(),
            CooperativeApplyOutcome::Idempotent
        );
    }

    #[test]
    fn epoch_activation_enters_revoking_and_generates_only_moved_revocations() {
        let (_, prepared) = prepare_move();
        let mut cooperative = CooperativeStateMachine::from_group_record(prepared).unwrap();
        let fence = cooperative.required_group_fence().unwrap();
        cooperative
            .apply_committed_group_fence(fence.clone())
            .unwrap();
        let active = cooperative.activate_prepared_epoch().unwrap();
        assert_eq!(
            cooperative.apply_committed_group_fence(fence).unwrap(),
            CooperativeApplyOutcome::Idempotent
        );
        assert_eq!(active.active_group_epoch, 2);
        assert_eq!(active.phase, GroupPhase::Revoking);
        assert_eq!(
            active.pending_revocations,
            BTreeMap::from([(tp(1), member(1)), (tp(3), member(2))])
        );
        assert_eq!(cooperative.revocations_for(member(1)), vec![tp(1)]);
        assert_eq!(cooperative.revocations_for(member(2)), vec![tp(3)]);
        assert_eq!(active.active_assignments[&tp(1)], member(1));
        assert_eq!(active.active_assignments[&tp(3)], member(2));
        assert!(!active.active_assignments.contains_key(&tp(4)));
    }

    #[test]
    fn revoke_ack_requires_exact_authority_and_release_precedes_owner_fence() {
        let (_, prepared) = prepare_move();
        let mut cooperative = CooperativeStateMachine::from_group_record(prepared).unwrap();
        let group_fence = cooperative.required_group_fence().unwrap();
        cooperative
            .apply_committed_group_fence(group_fence)
            .unwrap();
        cooperative.activate_prepared_epoch().unwrap();
        let a = member(1);
        let auth = authority(cooperative.current(), a);

        let before = cooperative.current().clone();
        let premature = OffsetOwnerFenceV1 {
            group_id: before.group_id.clone(),
            topic_partition: tp(1),
            group_epoch: GroupEpoch::new(2).unwrap(),
            member_id: member(2),
            member_epoch: MemberEpoch::new(1).unwrap(),
            source_group_state_version: before.state_version,
        };
        assert!(cooperative.apply_committed_owner_fence(premature).is_err());

        assert_eq!(
            cooperative.revoke_ack(&auth, &[tp(1)]).unwrap(),
            CooperativeApplyOutcome::Applied
        );
        assert!(
            !cooperative
                .current()
                .active_assignments
                .contains_key(&tp(1))
        );
        assert!(
            !cooperative
                .current()
                .pending_revocations
                .contains_key(&tp(1))
        );
        let fences = cooperative.required_owner_fences().unwrap();
        assert!(fences.iter().any(|fence| fence.topic_partition == tp(1)));
    }

    #[test]
    fn target_activation_requires_committed_owner_fence_and_never_overlaps_old_owner() {
        let (_, prepared) = prepare_move();
        let mut cooperative = CooperativeStateMachine::from_group_record(prepared).unwrap();
        cooperative
            .apply_committed_group_fence(cooperative.required_group_fence().unwrap())
            .unwrap();
        cooperative.activate_prepared_epoch().unwrap();
        let a = member(1);
        let auth = authority(cooperative.current(), a);
        cooperative.revoke_ack(&auth, &[tp(1)]).unwrap();
        assert!(cooperative.activate_fenced_targets().is_err());

        let fence = cooperative
            .required_owner_fences()
            .unwrap()
            .into_iter()
            .find(|fence| fence.topic_partition == tp(1))
            .unwrap();
        cooperative
            .apply_committed_owner_fence(fence.clone())
            .unwrap();
        assert_eq!(
            cooperative
                .apply_committed_owner_fence(fence.clone())
                .unwrap(),
            CooperativeApplyOutcome::Idempotent
        );
        let activated = cooperative.activate_fenced_targets().unwrap();
        assert_eq!(activated.active_assignments[&tp(1)], member(2));
        assert_ne!(activated.active_assignments[&tp(1)], a);
        assert_eq!(
            cooperative.apply_committed_owner_fence(fence).unwrap(),
            CooperativeApplyOutcome::Idempotent
        );
    }

    #[test]
    fn duplicate_revoke_ack_is_idempotent_for_same_completed_handoff() {
        let (_, prepared) = prepare_move();
        let mut cooperative = CooperativeStateMachine::from_group_record(prepared).unwrap();
        cooperative
            .apply_committed_group_fence(cooperative.required_group_fence().unwrap())
            .unwrap();
        cooperative.activate_prepared_epoch().unwrap();
        let auth = authority(cooperative.current(), member(1));
        assert_eq!(
            cooperative.revoke_ack(&auth, &[tp(1)]).unwrap(),
            CooperativeApplyOutcome::Applied
        );
        let version = cooperative.current().state_version;
        assert_eq!(
            cooperative.revoke_ack(&auth, &[tp(1)]).unwrap(),
            CooperativeApplyOutcome::Idempotent
        );
        assert_eq!(cooperative.current().state_version, version);
    }

    #[test]
    fn stable_requires_zero_revocations_and_exact_target_equality() {
        let (_, prepared) = prepare_move();
        let mut cooperative = CooperativeStateMachine::from_group_record(prepared).unwrap();
        cooperative
            .apply_committed_group_fence(cooperative.required_group_fence().unwrap())
            .unwrap();
        cooperative.activate_prepared_epoch().unwrap();

        let auth_a = authority(cooperative.current(), member(1));
        cooperative.revoke_ack(&auth_a, &[tp(1)]).unwrap();
        for fence in cooperative.required_owner_fences().unwrap() {
            cooperative.apply_committed_owner_fence(fence).unwrap();
        }
        cooperative.activate_fenced_targets().unwrap();
        assert_eq!(cooperative.current().phase, GroupPhase::Revoking);
        assert!(!cooperative.current().pending_revocations.is_empty());

        let auth_b = authority(cooperative.current(), member(2));
        cooperative.revoke_ack(&auth_b, &[tp(3)]).unwrap();
        for fence in cooperative.required_owner_fences().unwrap() {
            cooperative.apply_committed_owner_fence(fence).unwrap();
        }
        let stable = cooperative.activate_fenced_targets().unwrap();
        assert_eq!(stable.phase, GroupPhase::Stable);
        assert!(stable.pending_revocations.is_empty());
        assert_eq!(stable.active_assignments, stable.target_assignments);
        assert_eq!(
            cooperative.revoke_ack(&auth_a, &[tp(1)]).unwrap(),
            CooperativeApplyOutcome::Idempotent
        );
    }

    #[test]
    fn strictly_newer_prepare_supersedes_inflight_epoch_without_reusing_authority() {
        let (_, prepared) = prepare_move();
        let mut cooperative = CooperativeStateMachine::from_group_record(prepared.clone()).unwrap();
        let mut superseding = prepared.clone();
        superseding.state_version = prepared.state_version.checked_next().unwrap();
        superseding.pending_group_epoch = Some(GroupEpoch::new(3).unwrap());
        superseding.target_assignments = BTreeMap::from([
            (tp(0), member(1)),
            (tp(1), member(1)),
            (tp(2), member(2)),
            (tp(3), member(2)),
        ]);
        cooperative
            .apply_superseding_prepared(superseding.clone())
            .unwrap();
        assert_eq!(cooperative.current().pending_group_epoch.unwrap().get(), 3);

        let mut reused = superseding.clone();
        reused.state_version = superseding.state_version.checked_next().unwrap();
        assert!(matches!(
            cooperative.apply_superseding_prepared(reused),
            Err(CooperativeError::FenceRegression)
        ));

        let mut illicit = superseding;
        illicit.state_version = illicit.state_version.checked_next().unwrap();
        illicit.pending_group_epoch = Some(GroupEpoch::new(4).unwrap());
        illicit.active_assignments.insert(tp(4), member(2));
        assert!(matches!(
            cooperative.apply_superseding_prepared(illicit),
            Err(CooperativeError::FenceMismatch(
                "superseding prepare cannot add or transfer active ownership"
            ))
        ));
    }

    #[test]
    fn crash_recovery_resumes_after_prepare_group_fence_release_and_owner_fence() {
        let (stable, prepared) = prepare_move();
        let mut live = CooperativeStateMachine::from_group_record(prepared.clone()).unwrap();
        let group_fence = live.required_group_fence().unwrap();

        let recovered_prepare =
            CooperativeStateMachine::recover(&[stable.clone(), prepared.clone()], &[], &[])
                .unwrap();
        assert_eq!(
            recovered_prepare.required_group_fence().unwrap(),
            group_fence
        );

        let mut recovered_after_group_fence = CooperativeStateMachine::recover(
            &[stable.clone(), prepared.clone()],
            std::slice::from_ref(&group_fence),
            &[],
        )
        .unwrap();
        let activated_after_crash = recovered_after_group_fence
            .activate_prepared_epoch()
            .unwrap();
        assert_eq!(activated_after_crash.active_group_epoch, 2);

        live.apply_committed_group_fence(group_fence.clone())
            .unwrap();
        let activated = live.activate_prepared_epoch().unwrap();
        let recovered_fenced = CooperativeStateMachine::recover(
            &[stable.clone(), prepared.clone(), activated.clone()],
            std::slice::from_ref(&group_fence),
            &[],
        )
        .unwrap();
        assert_eq!(recovered_fenced.current(), &activated);

        let auth = authority(live.current(), member(1));
        live.revoke_ack(&auth, &[tp(1)]).unwrap();
        let released = live.current().clone();
        let mut recovered_release = CooperativeStateMachine::recover(
            &[
                stable.clone(),
                prepared.clone(),
                activated.clone(),
                released.clone(),
            ],
            std::slice::from_ref(&group_fence),
            &[],
        )
        .unwrap();
        assert_eq!(
            recovered_release.revoke_ack(&auth, &[tp(1)]).unwrap(),
            CooperativeApplyOutcome::Idempotent
        );
        let owner_fence = recovered_release
            .required_owner_fences()
            .unwrap()
            .into_iter()
            .find(|fence| fence.topic_partition == tp(1))
            .unwrap();
        recovered_release
            .apply_committed_owner_fence(owner_fence.clone())
            .unwrap();

        let mut recovered_owner_fence = CooperativeStateMachine::recover(
            &[stable, prepared, activated, released],
            &[group_fence],
            &[owner_fence],
        )
        .unwrap();
        let resumed = recovered_owner_fence.activate_fenced_targets().unwrap();
        assert_eq!(resumed.active_assignments[&tp(1)], member(2));
    }

    #[test]
    fn bounded_workflow_suffix_recovery_starts_from_preparing_snapshot() {
        let (_stable, prepared) = prepare_move();
        let mut live = CooperativeStateMachine::from_group_record(prepared.clone()).unwrap();
        let group_fence = live.required_group_fence().unwrap();
        live.apply_committed_group_fence(group_fence.clone())
            .unwrap();
        let activated = live.activate_prepared_epoch().unwrap();
        let auth = authority(live.current(), member(1));
        live.revoke_ack(&auth, &[tp(1)]).unwrap();
        let released = live.current().clone();

        let recovered = CooperativeStateMachine::recover_workflow_suffix(
            &[prepared.clone(), activated.clone(), released.clone()],
            std::slice::from_ref(&group_fence),
            &[],
        )
        .unwrap();
        assert_eq!(recovered.current(), &released);
        assert!(
            recovered
                .required_owner_fences()
                .unwrap()
                .iter()
                .any(|fence| fence.topic_partition == tp(1))
        );

        assert!(matches!(
            CooperativeStateMachine::recover_workflow_suffix(
                std::slice::from_ref(&activated),
                std::slice::from_ref(&group_fence),
                &[],
            ),
            Err(CooperativeError::RecoveryHistory(
                "workflow suffix must begin at PREPARING_EPOCH_FENCE"
            ))
        ));
    }

    #[test]
    fn first_epoch_bootstrap_withholds_new_ownership_until_group_and_owner_fences_commit() {
        let group_id = GroupId::new("bootstrap").unwrap();
        let target_member = member(1);
        let mut membership = GroupStateMachine::new(group_id);
        let prepared = membership
            .apply(crate::GroupCommand::AdmitMember {
                expected_group_epoch: 0,
                allocated_member_id: target_member,
                instance_id: None,
                session_timeout_ms: 30_000,
                subscriptions: vec![topic(1)],
                plan: GroupMutationPlan {
                    target_assignments: BTreeMap::from([(tp(0), target_member)]),
                },
            })
            .unwrap();
        assert_eq!(prepared.active_group_epoch, 0);
        assert!(prepared.active_assignments.is_empty());

        let mut cooperative = CooperativeStateMachine::from_group_record(prepared).unwrap();
        let group_fence = cooperative.required_group_fence().unwrap();
        assert_eq!(group_fence.group_epoch.get(), 1);
        assert!(group_fence.owners.is_empty());
        cooperative
            .apply_committed_group_fence(group_fence)
            .unwrap();
        let revoking = cooperative.activate_prepared_epoch().unwrap();
        assert_eq!(revoking.active_group_epoch, 1);
        assert_eq!(revoking.phase, GroupPhase::Revoking);
        assert!(revoking.active_assignments.is_empty());

        let owner_fence = cooperative.required_owner_fences().unwrap().pop().unwrap();
        cooperative
            .apply_committed_owner_fence(owner_fence)
            .unwrap();
        let stable = cooperative.activate_fenced_targets().unwrap();
        assert_eq!(stable.phase, GroupPhase::Stable);
        assert_eq!(stable.active_assignments[&tp(0)], target_member);
    }

    #[test]
    fn canonical_revoke_ack_set_rejects_duplicates_reordering_and_wrong_owner() {
        let (_, prepared) = prepare_move();
        let mut cooperative = CooperativeStateMachine::from_group_record(prepared).unwrap();
        cooperative
            .apply_committed_group_fence(cooperative.required_group_fence().unwrap())
            .unwrap();
        cooperative.activate_prepared_epoch().unwrap();
        let auth = authority(cooperative.current(), member(1));
        assert!(matches!(
            cooperative.revoke_ack(&auth, &[tp(1), tp(1)]),
            Err(CooperativeError::NonCanonicalRevokeAck)
        ));
        assert!(matches!(
            cooperative.revoke_ack(&auth, &[tp(3)]),
            Err(CooperativeError::InvalidRevokeAck(_))
        ));

        let mut stale_group = auth.clone();
        stale_group.group_epoch = GroupEpoch::new(1).unwrap();
        assert!(matches!(
            cooperative.revoke_ack(&stale_group, &[tp(1)]),
            Err(CooperativeError::Group(
                GroupStateError::GroupEpochStale { .. }
            ))
        ));

        let mut stale_member = auth;
        stale_member.member_epoch = MemberEpoch::new(2).unwrap();
        assert!(matches!(
            cooperative.revoke_ack(&stale_member, &[tp(1)]),
            Err(CooperativeError::Group(
                GroupStateError::MemberEpochStale { .. }
            ))
        ));
    }

    #[test]
    fn bounded_model_exhaustively_preserves_old_released_new_handoff_order() {
        #[derive(Clone)]
        struct ModelState {
            machine: CooperativeStateMachine,
            old_owner: BTreeMap<TopicPartitionV0, MemberId>,
        }

        let (_, prepared) = prepare_move();
        let old_owner = prepared.active_assignments.clone();
        let mut initial = CooperativeStateMachine::from_group_record(prepared).unwrap();
        initial
            .apply_committed_group_fence(initial.required_group_fence().unwrap())
            .unwrap();
        initial.activate_prepared_epoch().unwrap();

        let mut queue = VecDeque::from([ModelState {
            machine: initial,
            old_owner,
        }]);
        let mut seen = BTreeSet::new();
        let mut reached_stable = false;
        let mut explored = 0_usize;

        while let Some(state) = queue.pop_front() {
            explored += 1;
            assert!(
                explored < 256,
                "bounded cooperative model escaped expected state space"
            );
            let current = state.machine.current();
            let key = (
                current.state_version.get(),
                current.phase as u8,
                current.active_assignments.clone(),
                current.pending_revocations.clone(),
                state
                    .machine
                    .owner_fences
                    .keys()
                    .copied()
                    .collect::<Vec<_>>(),
            );
            if !seen.insert(key) {
                continue;
            }

            for (partition, target) in &current.target_assignments {
                let old = state.old_owner.get(partition).copied();
                let active = current.active_assignments.get(partition).copied();
                if old.is_some_and(|owner| owner != *target) {
                    assert!(active == old || active.is_none() || active == Some(*target));
                }
            }
            if current.phase == GroupPhase::Stable {
                assert!(current.pending_revocations.is_empty());
                assert_eq!(current.active_assignments, current.target_assignments);
                reached_stable = true;
                continue;
            }

            if !state.machine.has_unactivated_owner_fence() {
                for member_id in [member(1), member(2)] {
                    let revocations = state.machine.revocations_for(member_id);
                    for partition in revocations {
                        let mut next = state.clone();
                        let auth = authority(next.machine.current(), member_id);
                        next.machine.revoke_ack(&auth, &[partition]).unwrap();
                        queue.push_back(next);
                    }
                }
                let required = state.machine.required_owner_fences().unwrap();
                if !required.is_empty() {
                    let mut next = state.clone();
                    for fence in required {
                        next.machine.apply_committed_owner_fence(fence).unwrap();
                    }
                    queue.push_back(next);
                }
            } else {
                let mut next = state.clone();
                next.machine.activate_fenced_targets().unwrap();
                queue.push_back(next);
            }
        }

        assert!(reached_stable);
        assert!(explored > 4);
    }
}
