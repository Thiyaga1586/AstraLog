use std::{collections::BTreeMap, fmt};

use astralog_common::TopicId;
use astralog_protocol::api::{
    GroupEpoch, MAX_SESSION_TIMEOUT_MS, MIN_SESSION_TIMEOUT_MS, MemberAuthorityV0, MemberEpoch,
    MemberId, TopicPartitionV0,
};

use crate::{
    AssignorError, GroupCommand, GroupMutationPlan, GroupRecordV1, GroupStateError, MemberRecordV1,
    uniform_sticky_assign,
};

pub const MIN_HEARTBEAT_INTERVAL_MS: u32 = 1_000;
pub const MAX_HEARTBEAT_INTERVAL_MS: u32 = 10_000;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SessionError {
    Group(GroupStateError),
    Assignor(AssignorError),
    InvalidSessionTimeout(u32),
    WrongGroup,
    GroupEpochZero,
    TimeRegression { previous_ms: u64, now_ms: u64 },
    DeadlineOverflow,
    MissingObservation(MemberId),
    ObservationOutOfSync(MemberId),
    StaleTimeoutCandidate,
    StaticMemberRequired,
    SessionContractChangedWithoutMemberEpoch(MemberId),
}

impl fmt::Display for SessionError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Group(error) => write!(f, "{error}"),
            Self::Assignor(error) => write!(f, "{error}"),
            Self::InvalidSessionTimeout(value) => write!(
                f,
                "session timeout {value}ms is outside frozen P6 range {MIN_SESSION_TIMEOUT_MS}..={MAX_SESSION_TIMEOUT_MS}"
            ),
            Self::WrongGroup => f.write_str("session tracker targets a different group"),
            Self::GroupEpochZero => {
                f.write_str("external session authority cannot use group epoch zero")
            }
            Self::TimeRegression {
                previous_ms,
                now_ms,
            } => write!(
                f,
                "injected session time regressed from {previous_ms}ms to {now_ms}ms"
            ),
            Self::DeadlineOverflow => f.write_str("session deadline arithmetic overflow"),
            Self::MissingObservation(member_id) => {
                write!(
                    f,
                    "member {member_id:?} has no leader-local session observation"
                )
            }
            Self::ObservationOutOfSync(member_id) => write!(
                f,
                "member {member_id:?} leader-local session observation is out of sync with committed authority"
            ),
            Self::StaleTimeoutCandidate => {
                f.write_str("session timeout candidate is stale or no longer expired")
            }
            Self::StaticMemberRequired => {
                f.write_str("static replacement requires committed InstanceId authority")
            }
            Self::SessionContractChangedWithoutMemberEpoch(member_id) => write!(
                f,
                "member {member_id:?} session timeout changed without a MemberEpoch transition"
            ),
        }
    }
}

impl std::error::Error for SessionError {}

impl From<GroupStateError> for SessionError {
    fn from(value: GroupStateError) -> Self {
        Self::Group(value)
    }
}

impl From<AssignorError> for SessionError {
    fn from(value: AssignorError) -> Self {
        Self::Assignor(value)
    }
}

pub type SessionResult<T> = core::result::Result<T, SessionError>;

pub fn validate_session_timeout(session_timeout_ms: u32) -> SessionResult<()> {
    if (MIN_SESSION_TIMEOUT_MS..=MAX_SESSION_TIMEOUT_MS).contains(&session_timeout_ms) {
        Ok(())
    } else {
        Err(SessionError::InvalidSessionTimeout(session_timeout_ms))
    }
}

pub fn heartbeat_interval_ms(session_timeout_ms: u32) -> SessionResult<u32> {
    validate_session_timeout(session_timeout_ms)?;
    Ok((session_timeout_ms / 3).clamp(MIN_HEARTBEAT_INTERVAL_MS, MAX_HEARTBEAT_INTERVAL_MS))
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct SessionObservation {
    member_epoch: MemberEpoch,
    session_timeout_ms: u32,
    deadline_ms: u64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SessionTimeoutCandidate {
    pub authority: MemberAuthorityV0,
    pub observed_deadline_ms: u64,
}

#[derive(Debug, Clone)]
pub struct SessionTracker {
    group_id: astralog_protocol::api::GroupId,
    last_now_ms: u64,
    observations: BTreeMap<MemberId, SessionObservation>,
}

impl SessionTracker {
    pub fn new_leader(now_ms: u64, committed: &GroupRecordV1) -> SessionResult<Self> {
        validate_committed_group(committed)?;
        let mut tracker = Self {
            group_id: committed.group_id.clone(),
            last_now_ms: now_ms,
            observations: BTreeMap::new(),
        };
        tracker.reset_leadership(now_ms, committed)?;
        Ok(tracker)
    }

    pub fn reset_leadership(
        &mut self,
        now_ms: u64,
        committed: &GroupRecordV1,
    ) -> SessionResult<()> {
        self.require_group(committed)?;
        self.require_non_regressing_time(now_ms)?;
        validate_committed_group(committed)?;
        let observations = observations_with_full_grace(now_ms, committed)?;
        self.last_now_ms = now_ms;
        self.observations = observations;
        Ok(())
    }

    pub fn reconcile_committed_group(
        &mut self,
        now_ms: u64,
        committed: &GroupRecordV1,
    ) -> SessionResult<()> {
        self.require_group(committed)?;
        self.require_non_regressing_time(now_ms)?;
        validate_committed_group(committed)?;

        let mut next = BTreeMap::new();
        for (&member_id, member) in &committed.members {
            validate_session_timeout(member.session_timeout_ms)?;
            let observation = match self.observations.get(&member_id) {
                Some(existing) if existing.member_epoch == member.member_epoch => {
                    if existing.session_timeout_ms != member.session_timeout_ms {
                        return Err(SessionError::SessionContractChangedWithoutMemberEpoch(
                            member_id,
                        ));
                    }
                    *existing
                }
                _ => SessionObservation {
                    member_epoch: member.member_epoch,
                    session_timeout_ms: member.session_timeout_ms,
                    deadline_ms: deadline(now_ms, member.session_timeout_ms)?,
                },
            };
            next.insert(member_id, observation);
        }
        self.last_now_ms = now_ms;
        self.observations = next;
        Ok(())
    }

    pub fn heartbeat(
        &mut self,
        now_ms: u64,
        committed: &GroupRecordV1,
        authority: &MemberAuthorityV0,
    ) -> SessionResult<()> {
        self.require_group(committed)?;
        self.require_non_regressing_time(now_ms)?;
        let member = validate_member_authority(committed, authority)?;
        let observation = self
            .observations
            .get_mut(&authority.member_id)
            .ok_or(SessionError::MissingObservation(authority.member_id))?;
        if observation.member_epoch != member.member_epoch
            || observation.session_timeout_ms != member.session_timeout_ms
        {
            return Err(SessionError::ObservationOutOfSync(authority.member_id));
        }
        let new_deadline = deadline(now_ms, member.session_timeout_ms)?;
        self.last_now_ms = now_ms;
        observation.deadline_ms = new_deadline;
        Ok(())
    }

    pub fn expired_members(
        &mut self,
        now_ms: u64,
        committed: &GroupRecordV1,
    ) -> SessionResult<Vec<SessionTimeoutCandidate>> {
        self.require_group(committed)?;
        self.require_non_regressing_time(now_ms)?;
        validate_committed_group(committed)?;
        self.last_now_ms = now_ms;

        if committed.active_group_epoch == 0 {
            return Ok(Vec::new());
        }
        let group_epoch = GroupEpoch::new(committed.active_group_epoch)
            .map_err(|_| SessionError::GroupEpochZero)?;
        let mut expired = Vec::new();
        for (&member_id, member) in &committed.members {
            let observation = self
                .observations
                .get(&member_id)
                .ok_or(SessionError::MissingObservation(member_id))?;
            if observation.member_epoch != member.member_epoch
                || observation.session_timeout_ms != member.session_timeout_ms
            {
                return Err(SessionError::ObservationOutOfSync(member_id));
            }
            if now_ms >= observation.deadline_ms {
                expired.push(SessionTimeoutCandidate {
                    authority: MemberAuthorityV0 {
                        group_id: committed.group_id.clone(),
                        member_id,
                        group_epoch,
                        member_epoch: member.member_epoch,
                    },
                    observed_deadline_ms: observation.deadline_ms,
                });
            }
        }
        Ok(expired)
    }

    pub fn prepare_timeout_removal(
        &self,
        committed: &GroupRecordV1,
        candidate: &SessionTimeoutCandidate,
        topic_partition_counts: &BTreeMap<TopicId, u32>,
    ) -> SessionResult<GroupCommand> {
        self.require_group(committed)?;
        validate_member_authority(committed, &candidate.authority)?;
        let observation = self
            .observations
            .get(&candidate.authority.member_id)
            .ok_or(SessionError::MissingObservation(
                candidate.authority.member_id,
            ))?;
        if observation.member_epoch != candidate.authority.member_epoch
            || observation.deadline_ms != candidate.observed_deadline_ms
            || self.last_now_ms < observation.deadline_ms
        {
            return Err(SessionError::StaleTimeoutCandidate);
        }

        let mut remaining = committed.members.clone();
        remaining.remove(&candidate.authority.member_id);
        let target_assignments = target_for_members(
            &remaining,
            &committed.active_assignments,
            topic_partition_counts,
        )?;
        Ok(GroupCommand::Leave {
            expected_group_epoch: candidate.authority.group_epoch,
            member_id: candidate.authority.member_id,
            expected_member_epoch: candidate.authority.member_epoch,
            plan: GroupMutationPlan { target_assignments },
        })
    }

    pub fn deadline_ms(&self, member_id: MemberId) -> Option<u64> {
        self.observations
            .get(&member_id)
            .map(|value| value.deadline_ms)
    }

    pub fn observation_count(&self) -> usize {
        self.observations.len()
    }

    fn require_group(&self, committed: &GroupRecordV1) -> SessionResult<()> {
        if committed.group_id == self.group_id {
            Ok(())
        } else {
            Err(SessionError::WrongGroup)
        }
    }

    fn require_non_regressing_time(&self, now_ms: u64) -> SessionResult<()> {
        if now_ms < self.last_now_ms {
            Err(SessionError::TimeRegression {
                previous_ms: self.last_now_ms,
                now_ms,
            })
        } else {
            Ok(())
        }
    }
}

pub fn prepare_static_replacement(
    committed: &GroupRecordV1,
    member_id: MemberId,
    expected_member_epoch: MemberEpoch,
    session_timeout_ms: u32,
    subscriptions: Vec<TopicId>,
    topic_partition_counts: &BTreeMap<TopicId, u32>,
) -> SessionResult<GroupCommand> {
    validate_committed_group(committed)?;
    validate_session_timeout(session_timeout_ms)?;
    if committed.active_group_epoch == 0 {
        return Err(SessionError::GroupEpochZero);
    }
    let existing = committed
        .members
        .get(&member_id)
        .ok_or(SessionError::Group(GroupStateError::UnknownMember))?;
    if existing.member_epoch != expected_member_epoch {
        return Err(SessionError::Group(GroupStateError::MemberEpochStale {
            expected: expected_member_epoch.get(),
            actual: existing.member_epoch.get(),
        }));
    }
    if existing.instance_id.is_none() {
        return Err(SessionError::StaticMemberRequired);
    }
    let replacement_epoch = existing.member_epoch.checked_next().map_err(|_| {
        SessionError::Group(GroupStateError::InvalidTransition("member epoch overflow"))
    })?;
    let mut members = committed.members.clone();
    members.insert(
        member_id,
        MemberRecordV1 {
            member_id,
            instance_id: existing.instance_id.clone(),
            member_epoch: replacement_epoch,
            session_timeout_ms,
            subscriptions: subscriptions.clone(),
        },
    );
    let target_assignments = target_for_members(
        &members,
        &committed.active_assignments,
        topic_partition_counts,
    )?;
    Ok(GroupCommand::ReplaceStaticMember {
        expected_group_epoch: GroupEpoch::new(committed.active_group_epoch)
            .map_err(|_| SessionError::GroupEpochZero)?,
        member_id,
        expected_member_epoch,
        session_timeout_ms,
        subscriptions,
        plan: GroupMutationPlan { target_assignments },
    })
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SessionWorkClass {
    Control,
    Ordinary,
}

pub fn select_session_work(
    control_pending: usize,
    ordinary_pending: usize,
) -> Option<SessionWorkClass> {
    if control_pending > 0 {
        Some(SessionWorkClass::Control)
    } else if ordinary_pending > 0 {
        Some(SessionWorkClass::Ordinary)
    } else {
        None
    }
}

fn validate_committed_group(committed: &GroupRecordV1) -> SessionResult<()> {
    crate::GroupStateMachine::from_snapshot(committed.clone())?;
    for member in committed.members.values() {
        validate_session_timeout(member.session_timeout_ms)?;
    }
    Ok(())
}

fn validate_member_authority<'a>(
    committed: &'a GroupRecordV1,
    authority: &MemberAuthorityV0,
) -> SessionResult<&'a MemberRecordV1> {
    if authority.group_id != committed.group_id {
        return Err(SessionError::WrongGroup);
    }
    if committed.active_group_epoch == 0 {
        return Err(SessionError::GroupEpochZero);
    }
    if authority.group_epoch.get() != committed.active_group_epoch {
        return Err(SessionError::Group(GroupStateError::GroupEpochStale {
            expected: authority.group_epoch.get(),
            actual: committed.active_group_epoch,
        }));
    }
    let member = committed
        .members
        .get(&authority.member_id)
        .ok_or(SessionError::Group(GroupStateError::UnknownMember))?;
    if member.member_epoch != authority.member_epoch {
        return Err(SessionError::Group(GroupStateError::MemberEpochStale {
            expected: authority.member_epoch.get(),
            actual: member.member_epoch.get(),
        }));
    }
    Ok(member)
}

fn observations_with_full_grace(
    now_ms: u64,
    committed: &GroupRecordV1,
) -> SessionResult<BTreeMap<MemberId, SessionObservation>> {
    committed
        .members
        .iter()
        .map(|(&member_id, member)| {
            validate_session_timeout(member.session_timeout_ms)?;
            Ok((
                member_id,
                SessionObservation {
                    member_epoch: member.member_epoch,
                    session_timeout_ms: member.session_timeout_ms,
                    deadline_ms: deadline(now_ms, member.session_timeout_ms)?,
                },
            ))
        })
        .collect()
}

fn deadline(now_ms: u64, session_timeout_ms: u32) -> SessionResult<u64> {
    now_ms
        .checked_add(u64::from(session_timeout_ms))
        .ok_or(SessionError::DeadlineOverflow)
}

fn target_for_members(
    members: &BTreeMap<MemberId, MemberRecordV1>,
    current_assignments: &BTreeMap<TopicPartitionV0, MemberId>,
    topic_partition_counts: &BTreeMap<TopicId, u32>,
) -> SessionResult<BTreeMap<TopicPartitionV0, MemberId>> {
    if members.is_empty() {
        return Ok(BTreeMap::new());
    }
    let relevant_counts = topic_partition_counts
        .iter()
        .filter_map(|(&topic_id, &partition_count)| {
            members
                .values()
                .any(|member| member.subscriptions.binary_search(&topic_id).is_ok())
                .then_some((topic_id, partition_count))
        })
        .collect::<BTreeMap<_, _>>();
    Ok(uniform_sticky_assign(members, current_assignments, &relevant_counts)?.assignments)
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use astralog_common::TopicId;
    use astralog_protocol::api::{GroupId, GroupStateVersion};

    use super::*;
    use crate::{CooperativeStateMachine, GroupPhase, GroupStateMachine, InternalRecordV1};

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

    fn static_member(byte: u8, timeout: u32, subscriptions: Vec<TopicId>) -> MemberRecordV1 {
        MemberRecordV1 {
            member_id: member(byte),
            instance_id: Some(format!("instance-{byte}")),
            member_epoch: MemberEpoch::new(1).unwrap(),
            session_timeout_ms: timeout,
            subscriptions,
        }
    }

    fn stable_group() -> GroupRecordV1 {
        let a = member(1);
        let b = member(2);
        let members = BTreeMap::from([
            (a, static_member(1, 6_000, vec![topic(1)])),
            (b, static_member(2, 9_000, vec![topic(1)])),
        ]);
        let active = BTreeMap::from([(tp(0), a), (tp(1), a), (tp(2), b), (tp(3), b)]);
        GroupRecordV1 {
            group_id: GroupId::new("sessions").unwrap(),
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

    fn authority(group: &GroupRecordV1, member_id: MemberId) -> MemberAuthorityV0 {
        MemberAuthorityV0 {
            group_id: group.group_id.clone(),
            member_id,
            group_epoch: GroupEpoch::new(group.active_group_epoch).unwrap(),
            member_epoch: group.members[&member_id].member_epoch,
        }
    }

    fn counts() -> BTreeMap<TopicId, u32> {
        BTreeMap::from([(topic(1), 4)])
    }

    #[test]
    fn session_timeout_range_and_heartbeat_recommendation_are_exact() {
        assert!(validate_session_timeout(5_999).is_err());
        assert!(validate_session_timeout(6_000).is_ok());
        assert!(validate_session_timeout(300_000).is_ok());
        assert!(validate_session_timeout(300_001).is_err());
        assert_eq!(heartbeat_interval_ms(6_000).unwrap(), 2_000);
        assert_eq!(heartbeat_interval_ms(30_000).unwrap(), 10_000);
        assert_eq!(heartbeat_interval_ms(300_000).unwrap(), 10_000);
    }

    #[test]
    fn heartbeat_requires_exact_authority_and_mutates_only_volatile_observation() {
        let group = stable_group();
        let durable_before = InternalRecordV1::GroupState(group.clone())
            .encode()
            .unwrap();
        let mut tracker = SessionTracker::new_leader(1_000, &group).unwrap();
        let a = member(1);
        let auth = authority(&group, a);
        tracker.heartbeat(2_000, &group, &auth).unwrap();
        assert_eq!(tracker.deadline_ms(a), Some(8_000));
        assert_eq!(
            InternalRecordV1::GroupState(group.clone())
                .encode()
                .unwrap(),
            durable_before
        );

        let mut stale_group = auth.clone();
        stale_group.group_epoch = GroupEpoch::new(2).unwrap();
        assert!(matches!(
            tracker.heartbeat(2_001, &group, &stale_group),
            Err(SessionError::Group(GroupStateError::GroupEpochStale { .. }))
        ));
        let mut stale_member = auth;
        stale_member.member_epoch = MemberEpoch::new(2).unwrap();
        assert!(matches!(
            tracker.heartbeat(2_001, &group, &stale_member),
            Err(SessionError::Group(
                GroupStateError::MemberEpochStale { .. }
            ))
        ));
        assert!(matches!(
            tracker.heartbeat(1_999, &group, &authority(&group, a)),
            Err(SessionError::TimeRegression { .. })
        ));
    }

    #[test]
    fn leadership_change_discards_observations_and_grants_full_committed_timeout() {
        let group = stable_group();
        let mut tracker = SessionTracker::new_leader(1_000, &group).unwrap();
        tracker
            .heartbeat(2_000, &group, &authority(&group, member(1)))
            .unwrap();
        assert_eq!(tracker.deadline_ms(member(1)), Some(8_000));
        tracker.reset_leadership(7_000, &group).unwrap();
        assert_eq!(tracker.deadline_ms(member(1)), Some(13_000));
        assert_eq!(tracker.deadline_ms(member(2)), Some(16_000));
        assert_eq!(tracker.observation_count(), 2);
    }

    #[test]
    fn expiry_is_only_a_candidate_and_a_late_heartbeat_can_cancel_it_before_commit() {
        let group = stable_group();
        let mut tracker = SessionTracker::new_leader(1_000, &group).unwrap();
        let before = group.clone();
        let expired = tracker.expired_members(7_000, &group).unwrap();
        let candidate = expired
            .iter()
            .find(|candidate| candidate.authority.member_id == member(1))
            .unwrap()
            .clone();
        assert_eq!(group, before);
        tracker
            .heartbeat(7_000, &group, &authority(&group, member(1)))
            .unwrap();
        assert_eq!(tracker.deadline_ms(member(1)), Some(13_000));
        assert!(matches!(
            tracker.prepare_timeout_removal(&group, &candidate, &counts()),
            Err(SessionError::StaleTimeoutCandidate)
        ));
        assert_eq!(group, before);
    }

    #[test]
    fn crash_without_leave_commits_timeout_removal_then_completes_cooperative_rebalance() {
        let group = stable_group();
        let mut tracker = SessionTracker::new_leader(1_000, &group).unwrap();
        let candidate = tracker
            .expired_members(7_000, &group)
            .unwrap()
            .into_iter()
            .find(|candidate| candidate.authority.member_id == member(1))
            .unwrap();
        let command = tracker
            .prepare_timeout_removal(&group, &candidate, &counts())
            .unwrap();
        assert!(group.members.contains_key(&member(1)));

        let mut state = GroupStateMachine::from_snapshot(group.clone()).unwrap();
        let prepared = state.apply(command).unwrap();
        assert!(!prepared.members.contains_key(&member(1)));
        assert_eq!(prepared.pending_group_epoch.unwrap().get(), 2);
        assert_eq!(prepared.active_assignments.get(&tp(2)), Some(&member(2)));
        assert_eq!(prepared.active_assignments.get(&tp(3)), Some(&member(2)));
        assert!(!prepared.active_assignments.contains_key(&tp(0)));
        assert_eq!(prepared.target_assignments.len(), 4);
        assert!(
            prepared
                .target_assignments
                .values()
                .all(|owner| *owner == member(2))
        );

        tracker.reconcile_committed_group(7_000, &prepared).unwrap();
        assert_eq!(tracker.observation_count(), 1);
        let mut cooperative = CooperativeStateMachine::from_group_record(prepared).unwrap();
        cooperative
            .apply_committed_group_fence(cooperative.required_group_fence().unwrap())
            .unwrap();
        cooperative.activate_prepared_epoch().unwrap();
        for fence in cooperative.required_owner_fences().unwrap() {
            cooperative.apply_committed_owner_fence(fence).unwrap();
        }
        let stable = cooperative.activate_fenced_targets().unwrap();
        assert_eq!(stable.phase, GroupPhase::Stable);
        assert!(
            stable
                .active_assignments
                .values()
                .all(|owner| *owner == member(2))
        );
    }

    #[test]
    fn slow_member_before_deadline_remains_authoritative_without_duplicate_owner() {
        let group = stable_group();
        let mut tracker = SessionTracker::new_leader(1_000, &group).unwrap();
        let expired = tracker.expired_members(6_999, &group).unwrap();
        assert!(
            expired
                .iter()
                .all(|candidate| candidate.authority.member_id != member(1))
        );
        assert_eq!(group.active_assignments.get(&tp(0)), Some(&member(1)));
        assert_eq!(group.active_assignments.get(&tp(1)), Some(&member(1)));
        tracker
            .heartbeat(6_999, &group, &authority(&group, member(1)))
            .unwrap();
        assert_eq!(tracker.deadline_ms(member(1)), Some(12_999));
    }

    #[test]
    fn static_rejoin_fences_old_session_and_identical_subscription_moves_nothing() {
        let group = stable_group();
        let a = member(1);
        let old_authority = authority(&group, a);
        let mut tracker = SessionTracker::new_leader(1_000, &group).unwrap();
        let command = prepare_static_replacement(
            &group,
            a,
            MemberEpoch::new(1).unwrap(),
            6_000,
            vec![topic(1)],
            &counts(),
        )
        .unwrap();
        let mut state = GroupStateMachine::from_snapshot(group.clone()).unwrap();
        let prepared = state.apply(command).unwrap();
        assert_eq!(prepared.members[&a].member_epoch.get(), 2);
        assert_eq!(prepared.pending_group_epoch.unwrap().get(), 2);
        assert_eq!(prepared.target_assignments, group.active_assignments);

        tracker.reconcile_committed_group(2_000, &prepared).unwrap();
        assert_eq!(tracker.deadline_ms(a), Some(8_000));
        assert!(matches!(
            tracker.heartbeat(2_001, &prepared, &old_authority),
            Err(SessionError::Group(
                GroupStateError::MemberEpochStale { .. }
            ))
        ));
        let new_authority = MemberAuthorityV0 {
            group_id: prepared.group_id.clone(),
            member_id: a,
            group_epoch: GroupEpoch::new(1).unwrap(),
            member_epoch: MemberEpoch::new(2).unwrap(),
        };
        tracker.heartbeat(2_001, &prepared, &new_authority).unwrap();

        let mut cooperative = CooperativeStateMachine::from_group_record(prepared).unwrap();
        cooperative
            .apply_committed_group_fence(cooperative.required_group_fence().unwrap())
            .unwrap();
        let stable = cooperative.activate_prepared_epoch().unwrap();
        assert_eq!(stable.phase, GroupPhase::Stable);
        assert_eq!(stable.active_assignments, group.active_assignments);
        assert_eq!(stable.active_group_epoch, 2);
    }

    #[test]
    fn static_changed_subscription_uses_normal_next_epoch_cooperative_rebalance() {
        let group = stable_group();
        let a = member(1);
        let command = prepare_static_replacement(
            &group,
            a,
            MemberEpoch::new(1).unwrap(),
            6_000,
            Vec::new(),
            &counts(),
        )
        .unwrap();
        let mut state = GroupStateMachine::from_snapshot(group.clone()).unwrap();
        let prepared = state.apply(command).unwrap();
        assert_eq!(prepared.pending_group_epoch.unwrap().get(), 2);
        assert!(
            prepared
                .target_assignments
                .values()
                .all(|owner| *owner == member(2))
        );
        assert_eq!(prepared.active_assignments.get(&tp(0)), Some(&a));

        let mut cooperative = CooperativeStateMachine::from_group_record(prepared).unwrap();
        cooperative
            .apply_committed_group_fence(cooperative.required_group_fence().unwrap())
            .unwrap();
        cooperative.activate_prepared_epoch().unwrap();
        let auth = MemberAuthorityV0 {
            group_id: group.group_id.clone(),
            member_id: a,
            group_epoch: GroupEpoch::new(2).unwrap(),
            member_epoch: MemberEpoch::new(2).unwrap(),
        };
        let revocations = cooperative.revocations_for(a);
        cooperative.revoke_ack(&auth, &revocations).unwrap();
        for fence in cooperative.required_owner_fences().unwrap() {
            cooperative.apply_committed_owner_fence(fence).unwrap();
        }
        let stable = cooperative.activate_fenced_targets().unwrap();
        assert_eq!(stable.phase, GroupPhase::Stable);
        assert!(
            stable
                .active_assignments
                .values()
                .all(|owner| *owner == member(2))
        );
    }

    #[test]
    fn control_priority_policy_cannot_be_starved_by_join_or_offset_pressure() {
        assert_eq!(select_session_work(0, 0), None);
        assert_eq!(
            select_session_work(0, usize::MAX),
            Some(SessionWorkClass::Ordinary)
        );
        let mut ordinary_backlog = 0_usize;
        let mut handled_controls = 0_usize;
        for cycle in 0..10_000_usize {
            ordinary_backlog = ordinary_backlog.saturating_add(1_000);
            let control_pending = usize::from(cycle % 7 == 0);
            match select_session_work(control_pending, ordinary_backlog) {
                Some(SessionWorkClass::Control) => {
                    assert_eq!(control_pending, 1);
                    handled_controls += 1;
                }
                Some(SessionWorkClass::Ordinary) => {
                    assert_eq!(control_pending, 0);
                    ordinary_backlog -= 1;
                }
                None => panic!("pressure model always has pending work"),
            }
        }
        assert_eq!(handled_controls, 1_429);
    }
}
