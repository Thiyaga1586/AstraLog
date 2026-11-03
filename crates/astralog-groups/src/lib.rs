#![forbid(unsafe_code)]

mod assignor;
mod cooperative;
mod offset_state;
#[cfg(test)]
mod qualification;
mod session;

pub use assignor::{AssignorError, AssignorResult, UniformStickyAssignment, uniform_sticky_assign};
pub use cooperative::{
    CooperativeApplyOutcome, CooperativeError, CooperativeResult, CooperativeStateMachine,
};
pub use offset_state::{
    OffsetApplyOutcome, OffsetStateError, OffsetStateMachine, OffsetStateResult,
};
pub use session::{
    MAX_HEARTBEAT_INTERVAL_MS, MIN_HEARTBEAT_INTERVAL_MS, SessionError, SessionResult,
    SessionTimeoutCandidate, SessionTracker, SessionWorkClass, heartbeat_interval_ms,
    prepare_static_replacement, select_session_work, validate_session_timeout,
};

use std::{
    collections::{BTreeMap, BTreeSet},
    fmt,
};

use astralog_common::TopicId;
use astralog_protocol::{
    PublicError,
    api::{
        GroupEpoch, GroupId, GroupStateVersion, MAX_GROUP_MEMBERS, MAX_GROUP_PARTITIONS,
        MAX_GROUP_RECORD_BYTES, MAX_INSTANCE_ID_LEN, MAX_MEMBER_ASSIGNMENT_PARTITIONS,
        MAX_OFFSET_METADATA_LEN, MAX_SESSION_TIMEOUT_MS, MAX_SUBSCRIPTION_TOPICS,
        MIN_SESSION_TIMEOUT_MS, MemberEpoch, MemberId, TopicPartitionV0,
    },
    partition_for_key,
};

pub const GROUPS_INTERNAL_TOPIC_NAME: &str = "__astralog_groups";
pub const OFFSETS_INTERNAL_TOPIC_NAME: &str = "__astralog_offsets";
pub const P6_INTERNAL_TOPIC_PARTITIONS: u32 = 64;
pub const P6_INTERNAL_TOPIC_REPLICATION_FACTOR: u16 = 3;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum InternalTopicKind {
    Groups,
    Offsets,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct InternalTopicDescriptor {
    pub kind: InternalTopicKind,
    pub name: &'static str,
    pub partition_count: u32,
    pub replication_factor: u16,
}

pub const P6_INTERNAL_TOPICS: [InternalTopicDescriptor; 2] = [
    InternalTopicDescriptor {
        kind: InternalTopicKind::Groups,
        name: GROUPS_INTERNAL_TOPIC_NAME,
        partition_count: P6_INTERNAL_TOPIC_PARTITIONS,
        replication_factor: P6_INTERNAL_TOPIC_REPLICATION_FACTOR,
    },
    InternalTopicDescriptor {
        kind: InternalTopicKind::Offsets,
        name: OFFSETS_INTERNAL_TOPIC_NAME,
        partition_count: P6_INTERNAL_TOPIC_PARTITIONS,
        replication_factor: P6_INTERNAL_TOPIC_REPLICATION_FACTOR,
    },
];

pub fn is_reserved_internal_topic_name(name: &str) -> bool {
    matches!(
        name,
        GROUPS_INTERNAL_TOPIC_NAME | OFFSETS_INTERNAL_TOPIC_NAME
    )
}

pub fn group_shard(group_id: &GroupId) -> Result<u32> {
    partition_for_key(group_id.as_bytes(), P6_INTERNAL_TOPIC_PARTITIONS)
        .map_err(|_| GroupCodecError::Invalid("frozen group shard mapping failed"))
}

const INTERNAL_VERSION_V1: u16 = 1;
const KIND_GROUP_STATE: u16 = 1;
const KIND_OFFSET_GROUP_FENCE: u16 = 2;
const KIND_OFFSET_OWNER_FENCE: u16 = 3;
const KIND_OFFSET_VALUE: u16 = 4;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum GroupCodecError {
    Invalid(&'static str),
    InvalidOwned(String),
    UnexpectedEof,
    LengthOverflow,
    TooLarge { len: usize, max: usize },
    InvalidUtf8,
}

impl fmt::Display for GroupCodecError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Invalid(message) => write!(f, "invalid P6 group record: {message}"),
            Self::InvalidOwned(message) => write!(f, "invalid P6 group record: {message}"),
            Self::UnexpectedEof => f.write_str("unexpected end of P6 group record"),
            Self::LengthOverflow => f.write_str("P6 group record length overflow"),
            Self::TooLarge { len, max } => write!(f, "P6 group record size {len} exceeds {max}"),
            Self::InvalidUtf8 => f.write_str("invalid UTF-8 in P6 group record"),
        }
    }
}

impl std::error::Error for GroupCodecError {}

pub type Result<T> = core::result::Result<T, GroupCodecError>;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum GroupPhase {
    Empty = 0,
    Stable = 1,
    PreparingEpochFence = 2,
    Revoking = 3,
}

impl TryFrom<u8> for GroupPhase {
    type Error = GroupCodecError;

    fn try_from(value: u8) -> Result<Self> {
        match value {
            0 => Ok(Self::Empty),
            1 => Ok(Self::Stable),
            2 => Ok(Self::PreparingEpochFence),
            3 => Ok(Self::Revoking),
            _ => Err(GroupCodecError::Invalid("unknown group phase")),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MemberRecordV1 {
    pub member_id: MemberId,
    pub instance_id: Option<String>,
    pub member_epoch: MemberEpoch,
    pub session_timeout_ms: u32,
    pub subscriptions: Vec<TopicId>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GroupRecordV1 {
    pub group_id: GroupId,
    pub state_version: GroupStateVersion,
    pub active_group_epoch: u64,
    pub pending_group_epoch: Option<GroupEpoch>,
    pub phase: GroupPhase,
    pub members: BTreeMap<MemberId, MemberRecordV1>,
    pub active_assignments: BTreeMap<TopicPartitionV0, MemberId>,
    pub target_assignments: BTreeMap<TopicPartitionV0, MemberId>,
    pub pending_revocations: BTreeMap<TopicPartitionV0, MemberId>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OffsetGroupFenceV1 {
    pub group_id: GroupId,
    pub group_epoch: GroupEpoch,
    pub source_group_state_version: GroupStateVersion,
    pub owners: BTreeMap<TopicPartitionV0, (MemberId, MemberEpoch)>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OffsetOwnerFenceV1 {
    pub group_id: GroupId,
    pub topic_partition: TopicPartitionV0,
    pub group_epoch: GroupEpoch,
    pub member_id: MemberId,
    pub member_epoch: MemberEpoch,
    pub source_group_state_version: GroupStateVersion,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OffsetValueV1 {
    pub group_id: GroupId,
    pub topic_partition: TopicPartitionV0,
    pub next_offset: u64,
    pub metadata: String,
    pub commit_timestamp_ms: i64,
    pub group_epoch: GroupEpoch,
    pub member_id: MemberId,
    pub member_epoch: MemberEpoch,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum InternalRecordV1 {
    GroupState(GroupRecordV1),
    OffsetGroupFence(OffsetGroupFenceV1),
    OffsetOwnerFence(OffsetOwnerFenceV1),
    OffsetValue(OffsetValueV1),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum GroupStateError {
    Codec(GroupCodecError),
    WrongGroup,
    StateVersionGap { expected: u64, actual: u64 },
    StateVersionConflict { version: u64 },
    GroupEpochStale { expected: u64, actual: u64 },
    MemberEpochStale { expected: u64, actual: u64 },
    UnknownMember,
    MemberAlreadyExists,
    InstanceIdConflict,
    InvalidTransition(&'static str),
}

impl fmt::Display for GroupStateError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Codec(error) => write!(f, "{error}"),
            Self::WrongGroup => f.write_str("group-state mutation targets the wrong group"),
            Self::StateVersionGap { expected, actual } => write!(
                f,
                "group-state version gap: expected {expected}, received {actual}"
            ),
            Self::StateVersionConflict { version } => {
                write!(f, "conflicting group-state record at version {version}")
            }
            Self::GroupEpochStale { expected, actual } => write!(
                f,
                "stale group epoch: expected {expected}, current {actual}"
            ),
            Self::MemberEpochStale { expected, actual } => write!(
                f,
                "stale member epoch: expected {expected}, current {actual}"
            ),
            Self::UnknownMember => f.write_str("unknown group member"),
            Self::MemberAlreadyExists => f.write_str("group member already exists"),
            Self::InstanceIdConflict => {
                f.write_str("instance id already belongs to another member")
            }
            Self::InvalidTransition(message) => {
                write!(f, "invalid group-state transition: {message}")
            }
        }
    }
}

impl std::error::Error for GroupStateError {}

impl GroupStateError {
    pub fn public_error(&self) -> Option<PublicError> {
        match self {
            Self::GroupEpochStale { .. } => Some(PublicError::GroupEpochStale),
            Self::MemberEpochStale { .. } | Self::UnknownMember => Some(PublicError::UnknownMember),
            _ => None,
        }
    }
}

impl From<GroupCodecError> for GroupStateError {
    fn from(value: GroupCodecError) -> Self {
        Self::Codec(value)
    }
}

pub type StateResult<T> = core::result::Result<T, GroupStateError>;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GroupMutationPlan {
    pub target_assignments: BTreeMap<TopicPartitionV0, MemberId>,
}

impl GroupMutationPlan {
    pub fn empty() -> Self {
        Self {
            target_assignments: BTreeMap::new(),
        }
    }
}

impl Default for GroupMutationPlan {
    fn default() -> Self {
        Self::empty()
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum GroupCommand {
    AdmitMember {
        expected_group_epoch: u64,
        allocated_member_id: MemberId,
        instance_id: Option<String>,
        session_timeout_ms: u32,
        subscriptions: Vec<TopicId>,
        plan: GroupMutationPlan,
    },
    ReplaceStaticMember {
        expected_group_epoch: GroupEpoch,
        member_id: MemberId,
        expected_member_epoch: MemberEpoch,
        session_timeout_ms: u32,
        subscriptions: Vec<TopicId>,
        plan: GroupMutationPlan,
    },
    UpdateSubscription {
        expected_group_epoch: GroupEpoch,
        member_id: MemberId,
        expected_member_epoch: MemberEpoch,
        subscriptions: Vec<TopicId>,
        plan: GroupMutationPlan,
    },
    Leave {
        expected_group_epoch: GroupEpoch,
        member_id: MemberId,
        expected_member_epoch: MemberEpoch,
        plan: GroupMutationPlan,
    },
    RebalanceAssignments {
        expected_group_epoch: GroupEpoch,
        plan: GroupMutationPlan,
    },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum GroupApplyOutcome {
    Applied,
    Idempotent,
}

#[derive(Debug, Clone)]
pub struct GroupStateMachine {
    group_id: GroupId,
    current: Option<GroupRecordV1>,
    current_bytes: Option<Vec<u8>>,
}

impl GroupStateMachine {
    pub fn new(group_id: GroupId) -> Self {
        Self {
            group_id,
            current: None,
            current_bytes: None,
        }
    }

    pub fn current(&self) -> Option<&GroupRecordV1> {
        self.current.as_ref()
    }

    pub fn from_snapshot(record: GroupRecordV1) -> StateResult<Self> {
        let bytes = InternalRecordV1::GroupState(record.clone()).encode()?;
        validate_state_semantics(&record)?;
        Ok(Self {
            group_id: record.group_id.clone(),
            current: Some(record),
            current_bytes: Some(bytes),
        })
    }

    pub fn apply_committed(&mut self, record: GroupRecordV1) -> StateResult<GroupApplyOutcome> {
        if record.group_id != self.group_id {
            return Err(GroupStateError::WrongGroup);
        }
        let bytes = InternalRecordV1::GroupState(record.clone()).encode()?;
        match &self.current {
            None => {
                if record.state_version.get() != 1 {
                    return Err(GroupStateError::StateVersionGap {
                        expected: 1,
                        actual: record.state_version.get(),
                    });
                }
                validate_state_semantics(&record)?;
            }
            Some(current) if record.state_version == current.state_version => {
                if self.current_bytes.as_deref() == Some(bytes.as_slice()) {
                    return Ok(GroupApplyOutcome::Idempotent);
                }
                return Err(GroupStateError::StateVersionConflict {
                    version: record.state_version.get(),
                });
            }
            Some(current) => {
                let expected = current
                    .state_version
                    .checked_next()
                    .map_err(|_| GroupStateError::InvalidTransition("state version overflow"))?
                    .get();
                if record.state_version.get() != expected {
                    return Err(GroupStateError::StateVersionGap {
                        expected,
                        actual: record.state_version.get(),
                    });
                }
                validate_state_semantics(&record)?;
                validate_replay_transition(current, &record)?;
            }
        }
        self.current = Some(record);
        self.current_bytes = Some(bytes);
        Ok(GroupApplyOutcome::Applied)
    }

    pub fn apply(&mut self, command: GroupCommand) -> StateResult<GroupRecordV1> {
        let candidate = match command {
            GroupCommand::AdmitMember {
                expected_group_epoch,
                allocated_member_id,
                instance_id,
                session_timeout_ms,
                subscriptions,
                plan,
            } => self.admit_member(
                expected_group_epoch,
                allocated_member_id,
                instance_id,
                session_timeout_ms,
                subscriptions,
                plan,
            )?,
            GroupCommand::ReplaceStaticMember {
                expected_group_epoch,
                member_id,
                expected_member_epoch,
                session_timeout_ms,
                subscriptions,
                plan,
            } => self.replace_static_member(
                expected_group_epoch,
                member_id,
                expected_member_epoch,
                session_timeout_ms,
                subscriptions,
                plan,
            )?,
            GroupCommand::UpdateSubscription {
                expected_group_epoch,
                member_id,
                expected_member_epoch,
                subscriptions,
                plan,
            } => self.update_subscription(
                expected_group_epoch,
                member_id,
                expected_member_epoch,
                subscriptions,
                plan,
            )?,
            GroupCommand::Leave {
                expected_group_epoch,
                member_id,
                expected_member_epoch,
                plan,
            } => self.leave(expected_group_epoch, member_id, expected_member_epoch, plan)?,
            GroupCommand::RebalanceAssignments {
                expected_group_epoch,
                plan,
            } => self.rebalance_assignments(expected_group_epoch, plan)?,
        };
        let bytes = InternalRecordV1::GroupState(candidate.clone()).encode()?;
        validate_state_semantics(&candidate)?;
        self.current_bytes = Some(bytes);
        self.current = Some(candidate.clone());
        Ok(candidate)
    }

    fn admit_member(
        &self,
        expected_group_epoch: u64,
        allocated_member_id: MemberId,
        instance_id: Option<String>,
        session_timeout_ms: u32,
        subscriptions: Vec<TopicId>,
        plan: GroupMutationPlan,
    ) -> StateResult<GroupRecordV1> {
        match &self.current {
            None => {
                if expected_group_epoch != 0 {
                    return Err(GroupStateError::GroupEpochStale {
                        expected: expected_group_epoch,
                        actual: 0,
                    });
                }
                let mut members = BTreeMap::new();
                members.insert(
                    allocated_member_id,
                    MemberRecordV1 {
                        member_id: allocated_member_id,
                        instance_id,
                        member_epoch: MemberEpoch::new(1).map_err(|_| {
                            GroupStateError::InvalidTransition("member epoch one is invalid")
                        })?,
                        session_timeout_ms,
                        subscriptions,
                    },
                );
                Ok(GroupRecordV1 {
                    group_id: self.group_id.clone(),
                    state_version: GroupStateVersion::new(1).map_err(|_| {
                        GroupStateError::InvalidTransition("state version one is invalid")
                    })?,
                    active_group_epoch: 0,
                    pending_group_epoch: Some(GroupEpoch::new(1).map_err(|_| {
                        GroupStateError::InvalidTransition("group epoch one is invalid")
                    })?),
                    phase: GroupPhase::PreparingEpochFence,
                    members,
                    active_assignments: BTreeMap::new(),
                    target_assignments: plan.target_assignments,
                    pending_revocations: BTreeMap::new(),
                })
            }
            Some(current) => {
                self.require_group_epoch(current, expected_group_epoch)?;
                if current.active_group_epoch == 0 {
                    return Err(GroupStateError::InvalidTransition(
                        "first group epoch must activate before another admission",
                    ));
                }
                if current.members.contains_key(&allocated_member_id) {
                    return Err(GroupStateError::MemberAlreadyExists);
                }
                if let Some(instance_id) = instance_id.as_deref() {
                    require_instance_available(current, instance_id, None)?;
                }
                let mut next = current.clone();
                next.members.insert(
                    allocated_member_id,
                    MemberRecordV1 {
                        member_id: allocated_member_id,
                        instance_id,
                        member_epoch: MemberEpoch::new(1).map_err(|_| {
                            GroupStateError::InvalidTransition("member epoch one is invalid")
                        })?,
                        session_timeout_ms,
                        subscriptions,
                    },
                );
                prepare_next_epoch(current, &mut next, plan.target_assignments)?;
                Ok(next)
            }
        }
    }

    fn replace_static_member(
        &self,
        expected_group_epoch: GroupEpoch,
        member_id: MemberId,
        expected_member_epoch: MemberEpoch,
        session_timeout_ms: u32,
        subscriptions: Vec<TopicId>,
        plan: GroupMutationPlan,
    ) -> StateResult<GroupRecordV1> {
        let current = self.require_current()?;
        self.require_group_epoch(current, expected_group_epoch.get())?;
        let existing = self.require_member_epoch(current, member_id, expected_member_epoch)?;
        let instance_id =
            existing
                .instance_id
                .clone()
                .ok_or(GroupStateError::InvalidTransition(
                    "static replacement requires an instance id",
                ))?;
        require_instance_available(current, &instance_id, Some(member_id))?;
        let mut next = current.clone();
        let member_epoch = existing
            .member_epoch
            .checked_next()
            .map_err(|_| GroupStateError::InvalidTransition("member epoch overflow"))?;
        next.members.insert(
            member_id,
            MemberRecordV1 {
                member_id,
                instance_id: Some(instance_id),
                member_epoch,
                session_timeout_ms,
                subscriptions,
            },
        );
        prepare_next_epoch(current, &mut next, plan.target_assignments)?;
        Ok(next)
    }

    fn update_subscription(
        &self,
        expected_group_epoch: GroupEpoch,
        member_id: MemberId,
        expected_member_epoch: MemberEpoch,
        subscriptions: Vec<TopicId>,
        plan: GroupMutationPlan,
    ) -> StateResult<GroupRecordV1> {
        let current = self.require_current()?;
        self.require_group_epoch(current, expected_group_epoch.get())?;
        let existing = self.require_member_epoch(current, member_id, expected_member_epoch)?;
        if existing.subscriptions == subscriptions {
            return Err(GroupStateError::InvalidTransition(
                "subscription update must change canonical subscription set",
            ));
        }
        let mut next = current.clone();
        let member = next
            .members
            .get_mut(&member_id)
            .ok_or(GroupStateError::UnknownMember)?;
        member.subscriptions = subscriptions;
        prepare_next_epoch(current, &mut next, plan.target_assignments)?;
        Ok(next)
    }

    fn leave(
        &self,
        expected_group_epoch: GroupEpoch,
        member_id: MemberId,
        expected_member_epoch: MemberEpoch,
        plan: GroupMutationPlan,
    ) -> StateResult<GroupRecordV1> {
        let current = self.require_current()?;
        self.require_group_epoch(current, expected_group_epoch.get())?;
        self.require_member_epoch(current, member_id, expected_member_epoch)?;
        let mut next = current.clone();
        next.members.remove(&member_id);
        next.active_assignments
            .retain(|_, owner| *owner != member_id);
        next.pending_revocations
            .retain(|_, owner| *owner != member_id);
        if plan
            .target_assignments
            .values()
            .any(|owner| *owner == member_id)
        {
            return Err(GroupStateError::InvalidTransition(
                "leave target assignment still references removed member",
            ));
        }
        prepare_next_epoch(current, &mut next, plan.target_assignments)?;
        Ok(next)
    }

    fn rebalance_assignments(
        &self,
        expected_group_epoch: GroupEpoch,
        plan: GroupMutationPlan,
    ) -> StateResult<GroupRecordV1> {
        let current = self.require_current()?;
        self.require_group_epoch(current, expected_group_epoch.get())?;
        if current.phase != GroupPhase::Stable {
            return Err(GroupStateError::InvalidTransition(
                "metadata assignment rebalance requires STABLE group state",
            ));
        }
        if plan.target_assignments == current.active_assignments {
            return Err(GroupStateError::InvalidTransition(
                "metadata assignment rebalance must change the target assignment",
            ));
        }
        let mut next = current.clone();
        prepare_next_epoch(current, &mut next, plan.target_assignments)?;
        Ok(next)
    }

    fn require_current(&self) -> StateResult<&GroupRecordV1> {
        self.current
            .as_ref()
            .ok_or(GroupStateError::InvalidTransition(
                "group does not yet exist",
            ))
    }

    fn require_group_epoch(&self, current: &GroupRecordV1, expected: u64) -> StateResult<()> {
        if current.active_group_epoch != expected {
            return Err(GroupStateError::GroupEpochStale {
                expected,
                actual: current.active_group_epoch,
            });
        }
        Ok(())
    }

    pub fn current_group_epoch(&self) -> u64 {
        self.current
            .as_ref()
            .map(|record| record.active_group_epoch)
            .unwrap_or(0)
    }

    fn require_member_epoch<'a>(
        &self,
        current: &'a GroupRecordV1,
        member_id: MemberId,
        expected: MemberEpoch,
    ) -> StateResult<&'a MemberRecordV1> {
        let member = current
            .members
            .get(&member_id)
            .ok_or(GroupStateError::UnknownMember)?;
        if member.member_epoch != expected {
            return Err(GroupStateError::MemberEpochStale {
                expected: expected.get(),
                actual: member.member_epoch.get(),
            });
        }
        Ok(member)
    }
}

fn prepare_next_epoch(
    current: &GroupRecordV1,
    next: &mut GroupRecordV1,
    target_assignments: BTreeMap<TopicPartitionV0, MemberId>,
) -> StateResult<()> {
    let authority_ceiling = current
        .pending_group_epoch
        .map(GroupEpoch::get)
        .unwrap_or(current.active_group_epoch);
    let pending = authority_ceiling
        .checked_add(1)
        .ok_or(GroupStateError::InvalidTransition("group epoch overflow"))?;
    next.state_version = current
        .state_version
        .checked_next()
        .map_err(|_| GroupStateError::InvalidTransition("state version overflow"))?;
    next.pending_group_epoch = Some(
        GroupEpoch::new(pending)
            .map_err(|_| GroupStateError::InvalidTransition("next group epoch is invalid"))?,
    );
    next.phase = GroupPhase::PreparingEpochFence;
    next.target_assignments = target_assignments;
    next.pending_revocations.clear();
    Ok(())
}

fn require_instance_available(
    current: &GroupRecordV1,
    instance_id: &str,
    same_member: Option<MemberId>,
) -> StateResult<()> {
    if current.members.values().any(|member| {
        member.instance_id.as_deref() == Some(instance_id)
            && same_member.is_none_or(|member_id| member.member_id != member_id)
    }) {
        return Err(GroupStateError::InstanceIdConflict);
    }
    Ok(())
}

fn validate_replay_transition(current: &GroupRecordV1, next: &GroupRecordV1) -> StateResult<()> {
    if next.active_group_epoch < current.active_group_epoch {
        return Err(GroupStateError::InvalidTransition(
            "active group epoch cannot move backward",
        ));
    }
    if next.active_group_epoch > current.active_group_epoch {
        let expected = current
            .pending_group_epoch
            .ok_or(GroupStateError::InvalidTransition(
                "active group epoch advanced without prepared epoch",
            ))?;
        if next.active_group_epoch != expected.get() {
            return Err(GroupStateError::InvalidTransition(
                "active group epoch did not activate the prepared epoch",
            ));
        }
    }
    if let (Some(current_pending), Some(next_pending)) =
        (current.pending_group_epoch, next.pending_group_epoch)
        && next_pending.get() < current_pending.get()
    {
        return Err(GroupStateError::InvalidTransition(
            "pending group epoch cannot move backward",
        ));
    }
    for (member_id, current_member) in &current.members {
        if let Some(next_member) = next.members.get(member_id)
            && next_member.member_epoch < current_member.member_epoch
        {
            return Err(GroupStateError::InvalidTransition(
                "member epoch cannot move backward",
            ));
        }
    }
    Ok(())
}

fn validate_state_semantics(record: &GroupRecordV1) -> StateResult<()> {
    match record.phase {
        GroupPhase::Empty => {
            if !record.active_assignments.is_empty()
                || !record.target_assignments.is_empty()
                || !record.pending_revocations.is_empty()
            {
                return Err(GroupStateError::InvalidTransition(
                    "EMPTY group cannot carry assignment authority",
                ));
            }
        }
        GroupPhase::Stable => {
            if !record.pending_revocations.is_empty() {
                return Err(GroupStateError::InvalidTransition(
                    "STABLE group cannot carry pending revocations",
                ));
            }
            if record.target_assignments != record.active_assignments {
                return Err(GroupStateError::InvalidTransition(
                    "STABLE target assignment must equal active assignment",
                ));
            }
        }
        GroupPhase::PreparingEpochFence => {
            if !record.pending_revocations.is_empty() {
                return Err(GroupStateError::InvalidTransition(
                    "PREPARING group cannot carry pending revocations",
                ));
            }
        }
        GroupPhase::Revoking => {
            let expected_revocations = record
                .active_assignments
                .iter()
                .filter_map(|(partition, active_owner)| {
                    (record.target_assignments.get(partition) != Some(active_owner))
                        .then_some((*partition, *active_owner))
                })
                .collect::<BTreeMap<_, _>>();
            if record.pending_revocations != expected_revocations {
                return Err(GroupStateError::InvalidTransition(
                    "REVOKING pending revocations must exactly match moved active ownership",
                ));
            }
            if record.pending_revocations.is_empty()
                && record.active_assignments == record.target_assignments
            {
                return Err(GroupStateError::InvalidTransition(
                    "REVOKING group with completed target must be STABLE",
                ));
            }
        }
    }
    Ok(())
}

impl InternalRecordV1 {
    pub fn encode(&self) -> Result<Vec<u8>> {
        let (kind, body) = match self {
            Self::GroupState(value) => (KIND_GROUP_STATE, encode_group_record(value)?),
            Self::OffsetGroupFence(value) => {
                (KIND_OFFSET_GROUP_FENCE, encode_offset_group_fence(value)?)
            }
            Self::OffsetOwnerFence(value) => {
                (KIND_OFFSET_OWNER_FENCE, encode_offset_owner_fence(value)?)
            }
            Self::OffsetValue(value) => (KIND_OFFSET_VALUE, encode_offset_value(value)?),
        };
        let total = 8_usize
            .checked_add(body.len())
            .ok_or(GroupCodecError::LengthOverflow)?;
        if total > MAX_GROUP_RECORD_BYTES {
            return Err(GroupCodecError::TooLarge {
                len: total,
                max: MAX_GROUP_RECORD_BYTES,
            });
        }
        let mut writer = LeWriter::with_capacity(total);
        writer.put_u16(INTERNAL_VERSION_V1);
        writer.put_u16(kind);
        writer.put_u32(u32_len(body.len())?);
        writer.put_bytes(&body);
        Ok(writer.finish())
    }

    pub fn decode(bytes: &[u8]) -> Result<Self> {
        if bytes.len() > MAX_GROUP_RECORD_BYTES {
            return Err(GroupCodecError::TooLarge {
                len: bytes.len(),
                max: MAX_GROUP_RECORD_BYTES,
            });
        }
        let mut reader = LeReader::new(bytes);
        if reader.read_u16()? != INTERNAL_VERSION_V1 {
            return Err(GroupCodecError::Invalid(
                "unsupported internal record version",
            ));
        }
        let kind = reader.read_u16()?;
        let body_len =
            usize::try_from(reader.read_u32()?).map_err(|_| GroupCodecError::LengthOverflow)?;
        if body_len != reader.remaining() {
            return Err(GroupCodecError::Invalid("internal body length mismatch"));
        }
        let body = reader.read_exact(body_len)?;
        match kind {
            KIND_GROUP_STATE => decode_group_record(body).map(Self::GroupState),
            KIND_OFFSET_GROUP_FENCE => decode_offset_group_fence(body).map(Self::OffsetGroupFence),
            KIND_OFFSET_OWNER_FENCE => decode_offset_owner_fence(body).map(Self::OffsetOwnerFence),
            KIND_OFFSET_VALUE => decode_offset_value(body).map(Self::OffsetValue),
            _ => Err(GroupCodecError::Invalid("unknown internal record kind")),
        }
    }
}

pub fn encode_group_state_key(group_id: &GroupId) -> Vec<u8> {
    group_id.as_bytes().to_vec()
}

pub fn encode_offset_group_fence_key(group_id: &GroupId) -> Result<Vec<u8>> {
    let mut writer = LeWriter::new();
    writer.put_u8(1);
    writer.put_string16(group_id.as_str(), 255)?;
    Ok(writer.finish())
}

pub fn encode_offset_owner_fence_key(
    group_id: &GroupId,
    topic_partition: TopicPartitionV0,
) -> Result<Vec<u8>> {
    encode_offset_partition_key(2, group_id, topic_partition)
}

pub fn encode_offset_value_key(
    group_id: &GroupId,
    topic_partition: TopicPartitionV0,
) -> Result<Vec<u8>> {
    encode_offset_partition_key(3, group_id, topic_partition)
}

fn encode_offset_partition_key(
    kind: u8,
    group_id: &GroupId,
    topic_partition: TopicPartitionV0,
) -> Result<Vec<u8>> {
    let mut writer = LeWriter::new();
    writer.put_u8(kind);
    writer.put_string16(group_id.as_str(), 255)?;
    writer.put_bytes(topic_partition.topic_id.as_bytes());
    writer.put_u32(topic_partition.partition);
    Ok(writer.finish())
}

fn encode_group_record(value: &GroupRecordV1) -> Result<Vec<u8>> {
    validate_group_record(value)?;
    let mut writer = LeWriter::new();
    writer.put_string16(value.group_id.as_str(), 255)?;
    writer.put_u64(value.state_version.get());
    writer.put_u64(value.active_group_epoch);
    writer.put_optional_u64(value.pending_group_epoch.map(GroupEpoch::get));
    writer.put_u8(value.phase as u8);
    writer.put_u32(u32_len(value.members.len())?);
    for (member_id, member) in &value.members {
        writer.put_uuid(member_id.as_bytes());
        writer.put_optional_string16(member.instance_id.as_deref(), MAX_INSTANCE_ID_LEN)?;
        writer.put_u64(member.member_epoch.get());
        writer.put_u32(member.session_timeout_ms);
        writer.put_u32(u32_len(member.subscriptions.len())?);
        for topic_id in &member.subscriptions {
            writer.put_bytes(topic_id.as_bytes());
        }
    }
    encode_assignment_map(&mut writer, &value.active_assignments)?;
    encode_assignment_map(&mut writer, &value.target_assignments)?;
    encode_assignment_map(&mut writer, &value.pending_revocations)?;
    Ok(writer.finish())
}

fn decode_group_record(bytes: &[u8]) -> Result<GroupRecordV1> {
    let mut reader = LeReader::new(bytes);
    let group_id = read_group_id(&mut reader)?;
    let state_version = read_state_version(&mut reader)?;
    let active_group_epoch = reader.read_u64()?;
    let pending_group_epoch = reader
        .read_optional_u64()?
        .map(read_group_epoch)
        .transpose()?;
    let phase = GroupPhase::try_from(reader.read_u8()?)?;
    let member_count = reader.read_count(MAX_GROUP_MEMBERS, 45)?;
    let mut members = BTreeMap::new();
    let mut previous_member = None;
    for _ in 0..member_count {
        let member_id = read_member_id(&mut reader)?;
        require_strictly_increasing(previous_member, member_id, "members are not canonical")?;
        let instance_id = reader.read_optional_string16(MAX_INSTANCE_ID_LEN)?;
        if let Some(value) = &instance_id {
            validate_text(value, MAX_INSTANCE_ID_LEN, false)?;
        }
        let member_epoch = read_member_epoch(&mut reader)?;
        let session_timeout_ms = reader.read_u32()?;
        let subscription_count = reader.read_count(MAX_SUBSCRIPTION_TOPICS, 16)?;
        let mut subscriptions = Vec::with_capacity(subscription_count);
        let mut previous_topic = None;
        for _ in 0..subscription_count {
            let topic_id = read_topic_id(&mut reader)?;
            require_strictly_increasing(
                previous_topic,
                topic_id,
                "subscriptions are not canonical",
            )?;
            subscriptions.push(topic_id);
            previous_topic = Some(topic_id);
        }
        let member = MemberRecordV1 {
            member_id,
            instance_id,
            member_epoch,
            session_timeout_ms,
            subscriptions,
        };
        members.insert(member_id, member);
        previous_member = Some(member_id);
    }
    let active_assignments = decode_assignment_map(&mut reader)?;
    let target_assignments = decode_assignment_map(&mut reader)?;
    let pending_revocations = decode_assignment_map(&mut reader)?;
    reader.finish()?;
    let value = GroupRecordV1 {
        group_id,
        state_version,
        active_group_epoch,
        pending_group_epoch,
        phase,
        members,
        active_assignments,
        target_assignments,
        pending_revocations,
    };
    validate_group_record(&value)?;
    Ok(value)
}

fn validate_group_record(value: &GroupRecordV1) -> Result<()> {
    if value.members.len() > MAX_GROUP_MEMBERS {
        return Err(GroupCodecError::Invalid("too many group members"));
    }
    match value.phase {
        GroupPhase::PreparingEpochFence => {
            let pending = value.pending_group_epoch.ok_or(GroupCodecError::Invalid(
                "preparing group record lacks pending epoch",
            ))?;
            if value.active_group_epoch == 0 && pending.get() != 1 {
                return Err(GroupCodecError::Invalid(
                    "bootstrap pending group epoch must be exactly one",
                ));
            }
            if value.active_group_epoch != 0 && pending.get() <= value.active_group_epoch {
                return Err(GroupCodecError::Invalid(
                    "pending group epoch must be strictly newer than active epoch",
                ));
            }
        }
        _ => {
            if value.active_group_epoch == 0 {
                return Err(GroupCodecError::Invalid(
                    "active group epoch zero outside prepare",
                ));
            }
            if value.pending_group_epoch.is_some() {
                return Err(GroupCodecError::Invalid(
                    "pending group epoch outside prepare",
                ));
            }
        }
    }
    let mut instance_ids = BTreeSet::new();
    for (member_id, member) in &value.members {
        if *member_id != member.member_id {
            return Err(GroupCodecError::Invalid("member map key/id mismatch"));
        }
        validate_instance(member.instance_id.as_deref())?;
        if let Some(instance_id) = member.instance_id.as_deref()
            && !instance_ids.insert(instance_id)
        {
            return Err(GroupCodecError::Invalid("duplicate group instance id"));
        }
        if !(MIN_SESSION_TIMEOUT_MS..=MAX_SESSION_TIMEOUT_MS).contains(&member.session_timeout_ms) {
            return Err(GroupCodecError::Invalid(
                "member session timeout out of range",
            ));
        }
        if member.subscriptions.len() > MAX_SUBSCRIPTION_TOPICS {
            return Err(GroupCodecError::Invalid(
                "member has too many subscriptions",
            ));
        }
        require_sorted_unique(&member.subscriptions, "subscriptions are not canonical")?;
    }
    validate_assignment_map(&value.members, &value.active_assignments)?;
    validate_assignment_map(&value.members, &value.target_assignments)?;
    validate_assignment_map(&value.members, &value.pending_revocations)?;
    for (partition, old_member) in &value.pending_revocations {
        if value.active_assignments.get(partition) != Some(old_member) {
            return Err(GroupCodecError::Invalid(
                "pending revocation is not current active owner",
            ));
        }
    }
    validate_per_member_bound(
        &value.active_assignments,
        "member active assignment bound exceeded",
    )?;
    validate_per_member_bound(
        &value.target_assignments,
        "member target assignment bound exceeded",
    )?;
    validate_per_member_bound(
        &value.pending_revocations,
        "member pending-revocation bound exceeded",
    )?;
    Ok(())
}

fn validate_per_member_bound(
    assignments: &BTreeMap<TopicPartitionV0, MemberId>,
    message: &'static str,
) -> Result<()> {
    let mut per_member: BTreeMap<MemberId, usize> = BTreeMap::new();
    for member_id in assignments.values() {
        let count = per_member.entry(*member_id).or_default();
        *count = count
            .checked_add(1)
            .ok_or(GroupCodecError::LengthOverflow)?;
        if *count > MAX_MEMBER_ASSIGNMENT_PARTITIONS {
            return Err(GroupCodecError::Invalid(message));
        }
    }
    Ok(())
}

fn validate_assignment_map(
    members: &BTreeMap<MemberId, MemberRecordV1>,
    assignments: &BTreeMap<TopicPartitionV0, MemberId>,
) -> Result<()> {
    if assignments.len() > MAX_GROUP_PARTITIONS {
        return Err(GroupCodecError::Invalid("group partition bound exceeded"));
    }
    if assignments
        .values()
        .any(|member_id| !members.contains_key(member_id))
    {
        return Err(GroupCodecError::Invalid(
            "assignment references unknown member",
        ));
    }
    Ok(())
}

fn encode_assignment_map(
    writer: &mut LeWriter,
    values: &BTreeMap<TopicPartitionV0, MemberId>,
) -> Result<()> {
    if values.len() > MAX_GROUP_PARTITIONS {
        return Err(GroupCodecError::Invalid("group partition bound exceeded"));
    }
    writer.put_u32(u32_len(values.len())?);
    for (partition, member_id) in values {
        writer.put_bytes(partition.topic_id.as_bytes());
        writer.put_u32(partition.partition);
        writer.put_uuid(member_id.as_bytes());
    }
    Ok(())
}

fn decode_assignment_map(
    reader: &mut LeReader<'_>,
) -> Result<BTreeMap<TopicPartitionV0, MemberId>> {
    let count = reader.read_count(MAX_GROUP_PARTITIONS, 36)?;
    let mut values = BTreeMap::new();
    let mut previous = None;
    for _ in 0..count {
        let partition = read_topic_partition(reader)?;
        require_strictly_increasing(previous, partition, "assignment map is not canonical")?;
        let member_id = read_member_id(reader)?;
        values.insert(partition, member_id);
        previous = Some(partition);
    }
    Ok(values)
}

fn encode_offset_group_fence(value: &OffsetGroupFenceV1) -> Result<Vec<u8>> {
    if value.owners.len() > MAX_GROUP_PARTITIONS {
        return Err(GroupCodecError::Invalid(
            "too many offset group-fence owners",
        ));
    }
    let mut writer = LeWriter::new();
    writer.put_string16(value.group_id.as_str(), 255)?;
    writer.put_u64(value.group_epoch.get());
    writer.put_u64(value.source_group_state_version.get());
    writer.put_u32(u32_len(value.owners.len())?);
    for (partition, (member_id, member_epoch)) in &value.owners {
        writer.put_bytes(partition.topic_id.as_bytes());
        writer.put_u32(partition.partition);
        writer.put_uuid(member_id.as_bytes());
        writer.put_u64(member_epoch.get());
    }
    Ok(writer.finish())
}

fn decode_offset_group_fence(bytes: &[u8]) -> Result<OffsetGroupFenceV1> {
    let mut reader = LeReader::new(bytes);
    let group_id = read_group_id(&mut reader)?;
    let group_epoch = read_group_epoch(reader.read_u64()?)?;
    let source_group_state_version = read_state_version(&mut reader)?;
    let count = reader.read_count(MAX_GROUP_PARTITIONS, 44)?;
    let mut owners = BTreeMap::new();
    let mut previous = None;
    for _ in 0..count {
        let partition = read_topic_partition(&mut reader)?;
        require_strictly_increasing(
            previous,
            partition,
            "offset group-fence owners are not canonical",
        )?;
        let member_id = read_member_id(&mut reader)?;
        let member_epoch = read_member_epoch(&mut reader)?;
        owners.insert(partition, (member_id, member_epoch));
        previous = Some(partition);
    }
    reader.finish()?;
    Ok(OffsetGroupFenceV1 {
        group_id,
        group_epoch,
        source_group_state_version,
        owners,
    })
}

fn encode_offset_owner_fence(value: &OffsetOwnerFenceV1) -> Result<Vec<u8>> {
    let mut writer = LeWriter::new();
    writer.put_string16(value.group_id.as_str(), 255)?;
    writer.put_bytes(value.topic_partition.topic_id.as_bytes());
    writer.put_u32(value.topic_partition.partition);
    writer.put_u64(value.group_epoch.get());
    writer.put_uuid(value.member_id.as_bytes());
    writer.put_u64(value.member_epoch.get());
    writer.put_u64(value.source_group_state_version.get());
    Ok(writer.finish())
}

fn decode_offset_owner_fence(bytes: &[u8]) -> Result<OffsetOwnerFenceV1> {
    let mut reader = LeReader::new(bytes);
    let value = OffsetOwnerFenceV1 {
        group_id: read_group_id(&mut reader)?,
        topic_partition: read_topic_partition(&mut reader)?,
        group_epoch: read_group_epoch(reader.read_u64()?)?,
        member_id: read_member_id(&mut reader)?,
        member_epoch: read_member_epoch(&mut reader)?,
        source_group_state_version: read_state_version(&mut reader)?,
    };
    reader.finish()?;
    Ok(value)
}

fn encode_offset_value(value: &OffsetValueV1) -> Result<Vec<u8>> {
    validate_metadata(&value.metadata)?;
    let mut writer = LeWriter::new();
    writer.put_string16(value.group_id.as_str(), 255)?;
    writer.put_bytes(value.topic_partition.topic_id.as_bytes());
    writer.put_u32(value.topic_partition.partition);
    writer.put_u64(value.next_offset);
    writer.put_string16(&value.metadata, MAX_OFFSET_METADATA_LEN)?;
    writer.put_i64(value.commit_timestamp_ms);
    writer.put_u64(value.group_epoch.get());
    writer.put_uuid(value.member_id.as_bytes());
    writer.put_u64(value.member_epoch.get());
    Ok(writer.finish())
}

fn decode_offset_value(bytes: &[u8]) -> Result<OffsetValueV1> {
    let mut reader = LeReader::new(bytes);
    let group_id = read_group_id(&mut reader)?;
    let topic_partition = read_topic_partition(&mut reader)?;
    let next_offset = reader.read_u64()?;
    let metadata = reader.read_string16(MAX_OFFSET_METADATA_LEN)?;
    validate_metadata(&metadata)?;
    let commit_timestamp_ms = reader.read_i64()?;
    let group_epoch = read_group_epoch(reader.read_u64()?)?;
    let member_id = read_member_id(&mut reader)?;
    let member_epoch = read_member_epoch(&mut reader)?;
    reader.finish()?;
    Ok(OffsetValueV1 {
        group_id,
        topic_partition,
        next_offset,
        metadata,
        commit_timestamp_ms,
        group_epoch,
        member_id,
        member_epoch,
    })
}

fn read_group_id(reader: &mut LeReader<'_>) -> Result<GroupId> {
    let value = reader.read_string16(255)?;
    GroupId::new(value).map_err(|error| GroupCodecError::InvalidOwned(error.to_string()))
}

fn read_member_id(reader: &mut LeReader<'_>) -> Result<MemberId> {
    MemberId::new(reader.read_uuid()?)
        .map_err(|error| GroupCodecError::InvalidOwned(error.to_string()))
}

fn read_group_epoch(value: u64) -> Result<GroupEpoch> {
    GroupEpoch::new(value).map_err(|error| GroupCodecError::InvalidOwned(error.to_string()))
}

fn read_member_epoch(reader: &mut LeReader<'_>) -> Result<MemberEpoch> {
    MemberEpoch::new(reader.read_u64()?)
        .map_err(|error| GroupCodecError::InvalidOwned(error.to_string()))
}

fn read_state_version(reader: &mut LeReader<'_>) -> Result<GroupStateVersion> {
    GroupStateVersion::new(reader.read_u64()?)
        .map_err(|error| GroupCodecError::InvalidOwned(error.to_string()))
}

fn read_topic_id(reader: &mut LeReader<'_>) -> Result<TopicId> {
    Ok(TopicId::new(reader.read_uuid()?))
}

fn read_topic_partition(reader: &mut LeReader<'_>) -> Result<TopicPartitionV0> {
    Ok(TopicPartitionV0 {
        topic_id: read_topic_id(reader)?,
        partition: reader.read_u32()?,
    })
}

fn validate_instance(value: Option<&str>) -> Result<()> {
    if let Some(value) = value {
        validate_text(value, MAX_INSTANCE_ID_LEN, false)?;
    }
    Ok(())
}

fn validate_metadata(value: &str) -> Result<()> {
    validate_text(value, MAX_OFFSET_METADATA_LEN, true)
}

fn validate_text(value: &str, max: usize, allow_empty: bool) -> Result<()> {
    if (!allow_empty && value.is_empty()) || value.len() > max {
        return Err(GroupCodecError::Invalid(
            "string length outside frozen P6 bound",
        ));
    }
    if value.as_bytes().contains(&0) {
        return Err(GroupCodecError::Invalid("NUL is forbidden in P6 string"));
    }
    Ok(())
}

fn require_sorted_unique<T: Ord>(values: &[T], message: &'static str) -> Result<()> {
    if values.windows(2).any(|pair| pair[0] >= pair[1]) {
        return Err(GroupCodecError::Invalid(message));
    }
    Ok(())
}

fn require_strictly_increasing<T: Ord + Copy>(
    previous: Option<T>,
    current: T,
    message: &'static str,
) -> Result<()> {
    if previous.is_some_and(|value| value >= current) {
        return Err(GroupCodecError::Invalid(message));
    }
    Ok(())
}

fn u32_len(value: usize) -> Result<u32> {
    u32::try_from(value).map_err(|_| GroupCodecError::LengthOverflow)
}

#[derive(Default)]
struct LeWriter {
    bytes: Vec<u8>,
}

impl LeWriter {
    fn new() -> Self {
        Self::default()
    }

    fn with_capacity(capacity: usize) -> Self {
        Self {
            bytes: Vec::with_capacity(capacity),
        }
    }

    fn put_u8(&mut self, value: u8) {
        self.bytes.push(value);
    }
    fn put_u16(&mut self, value: u16) {
        self.bytes.extend_from_slice(&value.to_le_bytes());
    }
    fn put_u32(&mut self, value: u32) {
        self.bytes.extend_from_slice(&value.to_le_bytes());
    }
    fn put_u64(&mut self, value: u64) {
        self.bytes.extend_from_slice(&value.to_le_bytes());
    }
    fn put_i64(&mut self, value: i64) {
        self.bytes.extend_from_slice(&value.to_le_bytes());
    }
    fn put_bytes(&mut self, value: &[u8]) {
        self.bytes.extend_from_slice(value);
    }
    fn put_uuid(&mut self, value: [u8; 16]) {
        self.put_bytes(&value);
    }

    fn put_string16(&mut self, value: &str, max: usize) -> Result<()> {
        if value.len() > max || value.len() >= usize::from(u16::MAX) {
            return Err(GroupCodecError::Invalid("string exceeds P6 bound"));
        }
        self.put_u16(u16::try_from(value.len()).map_err(|_| GroupCodecError::LengthOverflow)?);
        self.put_bytes(value.as_bytes());
        Ok(())
    }

    fn put_optional_string16(&mut self, value: Option<&str>, max: usize) -> Result<()> {
        match value {
            Some(value) => {
                self.put_u8(1);
                self.put_string16(value, max)?;
            }
            None => self.put_u8(0),
        }
        Ok(())
    }

    fn put_optional_u64(&mut self, value: Option<u64>) {
        match value {
            Some(value) => {
                self.put_u8(1);
                self.put_u64(value);
            }
            None => self.put_u8(0),
        }
    }

    fn finish(self) -> Vec<u8> {
        self.bytes
    }
}

struct LeReader<'a> {
    bytes: &'a [u8],
    pos: usize,
}

impl<'a> LeReader<'a> {
    fn new(bytes: &'a [u8]) -> Self {
        Self { bytes, pos: 0 }
    }
    fn remaining(&self) -> usize {
        self.bytes.len().saturating_sub(self.pos)
    }

    fn read_exact(&mut self, len: usize) -> Result<&'a [u8]> {
        let end = self
            .pos
            .checked_add(len)
            .ok_or(GroupCodecError::LengthOverflow)?;
        if end > self.bytes.len() {
            return Err(GroupCodecError::UnexpectedEof);
        }
        let value = &self.bytes[self.pos..end];
        self.pos = end;
        Ok(value)
    }

    fn read_u8(&mut self) -> Result<u8> {
        Ok(self.read_exact(1)?[0])
    }
    fn read_u16(&mut self) -> Result<u16> {
        Ok(u16::from_le_bytes(
            self.read_exact(2)?
                .try_into()
                .map_err(|_| GroupCodecError::UnexpectedEof)?,
        ))
    }
    fn read_u32(&mut self) -> Result<u32> {
        Ok(u32::from_le_bytes(
            self.read_exact(4)?
                .try_into()
                .map_err(|_| GroupCodecError::UnexpectedEof)?,
        ))
    }
    fn read_u64(&mut self) -> Result<u64> {
        Ok(u64::from_le_bytes(
            self.read_exact(8)?
                .try_into()
                .map_err(|_| GroupCodecError::UnexpectedEof)?,
        ))
    }
    fn read_i64(&mut self) -> Result<i64> {
        Ok(i64::from_le_bytes(
            self.read_exact(8)?
                .try_into()
                .map_err(|_| GroupCodecError::UnexpectedEof)?,
        ))
    }
    fn read_uuid(&mut self) -> Result<[u8; 16]> {
        self.read_exact(16)?
            .try_into()
            .map_err(|_| GroupCodecError::UnexpectedEof)
    }

    fn read_string16(&mut self, max: usize) -> Result<String> {
        let len = usize::from(self.read_u16()?);
        if len > max {
            return Err(GroupCodecError::Invalid("string exceeds P6 bound"));
        }
        let bytes = self.read_exact(len)?;
        let value = core::str::from_utf8(bytes).map_err(|_| GroupCodecError::InvalidUtf8)?;
        Ok(value.to_owned())
    }

    fn read_optional_string16(&mut self, max: usize) -> Result<Option<String>> {
        match self.read_u8()? {
            0 => Ok(None),
            1 => Ok(Some(self.read_string16(max)?)),
            _ => Err(GroupCodecError::Invalid("invalid optional string marker")),
        }
    }

    fn read_optional_u64(&mut self) -> Result<Option<u64>> {
        match self.read_u8()? {
            0 => Ok(None),
            1 => Ok(Some(self.read_u64()?)),
            _ => Err(GroupCodecError::Invalid("invalid optional u64 marker")),
        }
    }

    fn read_count(&mut self, max: usize, min_width: usize) -> Result<usize> {
        let count =
            usize::try_from(self.read_u32()?).map_err(|_| GroupCodecError::LengthOverflow)?;
        if count > max {
            return Err(GroupCodecError::Invalid("count exceeds P6 bound"));
        }
        if min_width > 0 && count > self.remaining() / min_width {
            return Err(GroupCodecError::Invalid(
                "count cannot fit remaining record bytes",
            ));
        }
        Ok(count)
    }

    fn finish(self) -> Result<()> {
        if self.remaining() == 0 {
            Ok(())
        } else {
            Err(GroupCodecError::Invalid("trailing bytes"))
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn topic(byte: u8) -> TopicId {
        TopicId::new([byte; 16])
    }
    fn tp(byte: u8, partition: u32) -> TopicPartitionV0 {
        TopicPartitionV0 {
            topic_id: topic(byte),
            partition,
        }
    }
    fn member(byte: u8) -> MemberId {
        MemberId::new([byte; 16]).unwrap()
    }

    fn hex_bytes(value: &str) -> Vec<u8> {
        assert_eq!(value.len() % 2, 0);
        let (pairs, remainder) = value.as_bytes().as_chunks::<2>();
        assert!(remainder.is_empty());
        pairs
            .iter()
            .map(|pair| {
                let high = (pair[0] as char).to_digit(16).unwrap();
                let low = (pair[1] as char).to_digit(16).unwrap();
                u8::try_from((high << 4) | low).unwrap()
            })
            .collect()
    }

    fn stable_group() -> GroupRecordV1 {
        let member_id = member(7);
        let mut members = BTreeMap::new();
        members.insert(
            member_id,
            MemberRecordV1 {
                member_id,
                instance_id: Some("worker-a".into()),
                member_epoch: MemberEpoch::new(2).unwrap(),
                session_timeout_ms: 30_000,
                subscriptions: vec![topic(1)],
            },
        );
        let mut assignments = BTreeMap::new();
        assignments.insert(tp(1, 0), member_id);
        GroupRecordV1 {
            group_id: GroupId::new("orders").unwrap(),
            state_version: GroupStateVersion::new(4).unwrap(),
            active_group_epoch: 3,
            pending_group_epoch: None,
            phase: GroupPhase::Stable,
            members,
            active_assignments: assignments.clone(),
            target_assignments: assignments,
            pending_revocations: BTreeMap::new(),
        }
    }

    fn state_machine_with_stable_group() -> GroupStateMachine {
        GroupStateMachine::from_snapshot(stable_group()).unwrap()
    }

    #[test]
    fn first_admission_prepares_exact_epoch_one_and_member_epoch_one() {
        let group_id = GroupId::new("first").unwrap();
        let member_id = member(1);
        let mut state = GroupStateMachine::new(group_id.clone());
        let record = state
            .apply(GroupCommand::AdmitMember {
                expected_group_epoch: 0,
                allocated_member_id: member_id,
                instance_id: None,
                session_timeout_ms: 30_000,
                subscriptions: vec![topic(1)],
                plan: GroupMutationPlan::empty(),
            })
            .unwrap();
        assert_eq!(record.group_id, group_id);
        assert_eq!(record.state_version.get(), 1);
        assert_eq!(record.active_group_epoch, 0);
        assert_eq!(record.pending_group_epoch.unwrap().get(), 1);
        assert_eq!(record.phase, GroupPhase::PreparingEpochFence);
        assert_eq!(record.members[&member_id].member_epoch.get(), 1);
        assert!(record.active_assignments.is_empty());
    }

    #[test]
    fn committed_replay_requires_exact_next_version_and_identical_duplicate() {
        let mut group = stable_group();
        group.state_version = GroupStateVersion::new(1).unwrap();
        let mut state = GroupStateMachine::new(group.group_id.clone());
        assert_eq!(
            state.apply_committed(group.clone()).unwrap(),
            GroupApplyOutcome::Applied
        );
        assert_eq!(
            state.apply_committed(group.clone()).unwrap(),
            GroupApplyOutcome::Idempotent
        );

        let mut conflicting = group.clone();
        conflicting.phase = GroupPhase::Empty;
        assert!(matches!(
            state.apply_committed(conflicting),
            Err(GroupStateError::StateVersionConflict { version: 1 })
        ));

        let mut gap = group;
        gap.state_version = GroupStateVersion::new(3).unwrap();
        assert!(matches!(
            state.apply_committed(gap),
            Err(GroupStateError::StateVersionGap {
                expected: 2,
                actual: 3
            })
        ));
    }

    #[test]
    fn dynamic_admission_and_instance_uniqueness_are_atomic() {
        let mut state = state_machine_with_stable_group();
        let original = state.current().unwrap().clone();
        let new_member = member(8);
        let record = state
            .apply(GroupCommand::AdmitMember {
                expected_group_epoch: GroupEpoch::new(3).unwrap().get(),
                allocated_member_id: new_member,
                instance_id: None,
                session_timeout_ms: 30_000,
                subscriptions: vec![topic(1)],
                plan: GroupMutationPlan {
                    target_assignments: original.active_assignments.clone(),
                },
            })
            .unwrap();
        assert_eq!(record.members[&new_member].member_epoch.get(), 1);
        assert_eq!(record.state_version.get(), 5);
        assert_eq!(record.pending_group_epoch.unwrap().get(), 4);

        let before = state.current().unwrap().clone();
        let result = state.apply(GroupCommand::AdmitMember {
            expected_group_epoch: 3,
            allocated_member_id: member(9),
            instance_id: Some("worker-a".into()),
            session_timeout_ms: 30_000,
            subscriptions: vec![topic(1)],
            plan: GroupMutationPlan::empty(),
        });
        assert!(matches!(result, Err(GroupStateError::InstanceIdConflict)));
        assert_eq!(state.current().unwrap(), &before);
    }

    #[test]
    fn static_replacement_increments_member_epoch_and_fences_stale_session() {
        let mut state = state_machine_with_stable_group();
        let member_id = member(7);
        let active = state.current().unwrap().active_assignments.clone();
        let record = state
            .apply(GroupCommand::ReplaceStaticMember {
                expected_group_epoch: GroupEpoch::new(3).unwrap(),
                member_id,
                expected_member_epoch: MemberEpoch::new(2).unwrap(),
                session_timeout_ms: 45_000,
                subscriptions: vec![topic(1)],
                plan: GroupMutationPlan {
                    target_assignments: active,
                },
            })
            .unwrap();
        assert_eq!(record.members[&member_id].member_epoch.get(), 3);
        assert_eq!(record.pending_group_epoch.unwrap().get(), 4);

        let before = state.current().unwrap().clone();
        let stale = state.apply(GroupCommand::ReplaceStaticMember {
            expected_group_epoch: GroupEpoch::new(3).unwrap(),
            member_id,
            expected_member_epoch: MemberEpoch::new(2).unwrap(),
            session_timeout_ms: 45_000,
            subscriptions: vec![topic(1)],
            plan: GroupMutationPlan::empty(),
        });
        assert!(matches!(
            stale,
            Err(GroupStateError::MemberEpochStale { .. })
        ));
        assert_eq!(state.current().unwrap(), &before);
    }

    #[test]
    fn subscription_update_prepares_next_epoch_and_rejects_stale_group_epoch() {
        let mut state = state_machine_with_stable_group();
        let member_id = member(7);
        let current = state.current().unwrap().clone();
        let record = state
            .apply(GroupCommand::UpdateSubscription {
                expected_group_epoch: GroupEpoch::new(3).unwrap(),
                member_id,
                expected_member_epoch: MemberEpoch::new(2).unwrap(),
                subscriptions: vec![topic(1), topic(2)],
                plan: GroupMutationPlan {
                    target_assignments: current.active_assignments.clone(),
                },
            })
            .unwrap();
        assert_eq!(
            record.members[&member_id].subscriptions,
            vec![topic(1), topic(2)]
        );
        assert_eq!(record.pending_group_epoch.unwrap().get(), 4);

        let before = state.current().unwrap().clone();
        let stale = state.apply(GroupCommand::UpdateSubscription {
            expected_group_epoch: GroupEpoch::new(2).unwrap(),
            member_id,
            expected_member_epoch: MemberEpoch::new(2).unwrap(),
            subscriptions: vec![topic(1)],
            plan: GroupMutationPlan::empty(),
        });
        assert!(matches!(
            stale,
            Err(GroupStateError::GroupEpochStale { .. })
        ));
        assert_eq!(state.current().unwrap(), &before);
    }

    #[test]
    fn metadata_assignment_rebalance_preserves_membership_and_prepares_higher_epoch() {
        let mut state = state_machine_with_stable_group();
        let before = state.current().unwrap().clone();
        let member_id = member(7);
        let expanded = BTreeMap::from([(tp(1, 0), member_id), (tp(1, 1), member_id)]);

        let prepared = state
            .apply(GroupCommand::RebalanceAssignments {
                expected_group_epoch: GroupEpoch::new(3).unwrap(),
                plan: GroupMutationPlan {
                    target_assignments: expanded.clone(),
                },
            })
            .unwrap();

        assert_eq!(prepared.members, before.members);
        assert_eq!(prepared.active_assignments, before.active_assignments);
        assert_eq!(prepared.target_assignments, expanded);
        assert_eq!(prepared.active_group_epoch, 3);
        assert_eq!(prepared.pending_group_epoch.unwrap().get(), 4);
        assert_eq!(prepared.phase, GroupPhase::PreparingEpochFence);

        let mut unchanged = GroupStateMachine::from_snapshot(before).unwrap();
        assert!(matches!(
            unchanged.apply(GroupCommand::RebalanceAssignments {
                expected_group_epoch: GroupEpoch::new(3).unwrap(),
                plan: GroupMutationPlan {
                    target_assignments: unchanged.current().unwrap().active_assignments.clone(),
                },
            }),
            Err(GroupStateError::InvalidTransition(
                "metadata assignment rebalance must change the target assignment"
            ))
        ));
    }

    #[test]
    fn durable_leave_removes_member_authority_and_assignments() {
        let mut state = state_machine_with_stable_group();
        let member_id = member(7);
        let record = state
            .apply(GroupCommand::Leave {
                expected_group_epoch: GroupEpoch::new(3).unwrap(),
                member_id,
                expected_member_epoch: MemberEpoch::new(2).unwrap(),
                plan: GroupMutationPlan::empty(),
            })
            .unwrap();
        assert!(!record.members.contains_key(&member_id));
        assert!(record.active_assignments.is_empty());
        assert!(record.target_assignments.is_empty());
        assert_eq!(record.pending_group_epoch.unwrap().get(), 4);
        assert!(matches!(
            state.apply(GroupCommand::Leave {
                expected_group_epoch: GroupEpoch::new(3).unwrap(),
                member_id,
                expected_member_epoch: MemberEpoch::new(2).unwrap(),
                plan: GroupMutationPlan::empty(),
            }),
            Err(GroupStateError::UnknownMember)
        ));
    }

    #[test]
    fn live_state_bounds_fail_before_mutation() {
        let mut state = state_machine_with_stable_group();
        let before = state.current().unwrap().clone();
        let mut target = BTreeMap::new();
        for partition in 0..=MAX_MEMBER_ASSIGNMENT_PARTITIONS {
            target.insert(
                TopicPartitionV0 {
                    topic_id: topic(1),
                    partition: u32::try_from(partition).unwrap(),
                },
                member(7),
            );
        }
        let result = state.apply(GroupCommand::UpdateSubscription {
            expected_group_epoch: GroupEpoch::new(3).unwrap(),
            member_id: member(7),
            expected_member_epoch: MemberEpoch::new(2).unwrap(),
            subscriptions: vec![topic(1), topic(2)],
            plan: GroupMutationPlan {
                target_assignments: target,
            },
        });
        assert!(matches!(result, Err(GroupStateError::Codec(_))));
        assert_eq!(state.current().unwrap(), &before);

        let group_id = GroupId::new("max-members").unwrap();
        let mut members = BTreeMap::new();
        for index in 1..=MAX_GROUP_MEMBERS {
            let mut bytes = [0_u8; 16];
            bytes[..8].copy_from_slice(&u64::try_from(index).unwrap().to_le_bytes());
            let member_id = MemberId::new(bytes).unwrap();
            members.insert(
                member_id,
                MemberRecordV1 {
                    member_id,
                    instance_id: None,
                    member_epoch: MemberEpoch::new(1).unwrap(),
                    session_timeout_ms: 30_000,
                    subscriptions: Vec::new(),
                },
            );
        }
        let full = GroupRecordV1 {
            group_id: group_id.clone(),
            state_version: GroupStateVersion::new(1).unwrap(),
            active_group_epoch: 1,
            pending_group_epoch: None,
            phase: GroupPhase::Stable,
            members,
            active_assignments: BTreeMap::new(),
            target_assignments: BTreeMap::new(),
            pending_revocations: BTreeMap::new(),
        };
        let mut full_state = GroupStateMachine::from_snapshot(full).unwrap();
        let before = full_state.current().unwrap().clone();
        let result = full_state.apply(GroupCommand::AdmitMember {
            expected_group_epoch: 1,
            allocated_member_id: member(255),
            instance_id: None,
            session_timeout_ms: 30_000,
            subscriptions: Vec::new(),
            plan: GroupMutationPlan::empty(),
        });
        assert!(matches!(result, Err(GroupStateError::Codec(_))));
        assert_eq!(full_state.current().unwrap(), &before);
    }

    #[test]
    fn ordered_logical_commands_are_byte_deterministic() {
        fn run(reverse_insert: bool) -> Vec<u8> {
            let mut state = state_machine_with_stable_group();
            let mut target = BTreeMap::new();
            let partitions = if reverse_insert {
                vec![1, 0]
            } else {
                vec![0, 1]
            };
            for partition in partitions {
                target.insert(tp(1, partition), member(7));
            }
            state
                .apply(GroupCommand::UpdateSubscription {
                    expected_group_epoch: GroupEpoch::new(3).unwrap(),
                    member_id: member(7),
                    expected_member_epoch: MemberEpoch::new(2).unwrap(),
                    subscriptions: vec![topic(1), topic(2)],
                    plan: GroupMutationPlan {
                        target_assignments: target,
                    },
                })
                .unwrap();
            InternalRecordV1::GroupState(state.current().unwrap().clone())
                .encode()
                .unwrap()
        }
        assert_eq!(run(false), run(true));
    }

    #[test]
    fn prepared_epoch_can_be_superseded_only_by_a_strictly_higher_epoch() {
        let mut state = state_machine_with_stable_group();
        state
            .apply(GroupCommand::AdmitMember {
                expected_group_epoch: 3,
                allocated_member_id: member(8),
                instance_id: None,
                session_timeout_ms: 30_000,
                subscriptions: vec![topic(1)],
                plan: GroupMutationPlan::empty(),
            })
            .unwrap();
        assert_eq!(
            state.current().unwrap().pending_group_epoch.unwrap().get(),
            4
        );
        state
            .apply(GroupCommand::AdmitMember {
                expected_group_epoch: 3,
                allocated_member_id: member(9),
                instance_id: None,
                session_timeout_ms: 30_000,
                subscriptions: vec![topic(1)],
                plan: GroupMutationPlan::empty(),
            })
            .unwrap();
        assert_eq!(
            state.current().unwrap().pending_group_epoch.unwrap().get(),
            5
        );
    }

    #[test]
    fn replay_rejects_epoch_regression_and_stale_errors_keep_frozen_public_taxonomy() {
        let mut state = state_machine_with_stable_group();
        let mut regressed = stable_group();
        regressed.state_version = GroupStateVersion::new(5).unwrap();
        regressed.active_group_epoch = 2;
        assert!(matches!(
            state.apply_committed(regressed),
            Err(GroupStateError::InvalidTransition(
                "active group epoch cannot move backward"
            ))
        ));

        let stale_group = state
            .apply(GroupCommand::UpdateSubscription {
                expected_group_epoch: GroupEpoch::new(2).unwrap(),
                member_id: member(7),
                expected_member_epoch: MemberEpoch::new(2).unwrap(),
                subscriptions: vec![topic(1), topic(2)],
                plan: GroupMutationPlan::empty(),
            })
            .unwrap_err();
        assert_eq!(
            stale_group.public_error(),
            Some(PublicError::GroupEpochStale)
        );

        let stale_member = state
            .apply(GroupCommand::UpdateSubscription {
                expected_group_epoch: GroupEpoch::new(3).unwrap(),
                member_id: member(7),
                expected_member_epoch: MemberEpoch::new(1).unwrap(),
                subscriptions: vec![topic(1), topic(2)],
                plan: GroupMutationPlan::empty(),
            })
            .unwrap_err();
        assert_eq!(
            stale_member.public_error(),
            Some(PublicError::UnknownMember)
        );
    }

    #[test]
    fn all_internal_record_variants_round_trip_canonically() {
        let group = InternalRecordV1::GroupState(stable_group());
        assert_eq!(
            InternalRecordV1::decode(&group.encode().unwrap()).unwrap(),
            group
        );
        let mut owners = BTreeMap::new();
        owners.insert(tp(1, 0), (member(7), MemberEpoch::new(2).unwrap()));
        let fence = InternalRecordV1::OffsetGroupFence(OffsetGroupFenceV1 {
            group_id: GroupId::new("orders").unwrap(),
            group_epoch: GroupEpoch::new(3).unwrap(),
            source_group_state_version: GroupStateVersion::new(4).unwrap(),
            owners,
        });
        assert_eq!(
            InternalRecordV1::decode(&fence.encode().unwrap()).unwrap(),
            fence
        );
        let owner = InternalRecordV1::OffsetOwnerFence(OffsetOwnerFenceV1 {
            group_id: GroupId::new("orders").unwrap(),
            topic_partition: tp(1, 0),
            group_epoch: GroupEpoch::new(3).unwrap(),
            member_id: member(7),
            member_epoch: MemberEpoch::new(2).unwrap(),
            source_group_state_version: GroupStateVersion::new(5).unwrap(),
        });
        assert_eq!(
            InternalRecordV1::decode(&owner.encode().unwrap()).unwrap(),
            owner
        );
        let offset = InternalRecordV1::OffsetValue(OffsetValueV1 {
            group_id: GroupId::new("orders").unwrap(),
            topic_partition: tp(1, 0),
            next_offset: 42,
            metadata: "m".into(),
            commit_timestamp_ms: 123,
            group_epoch: GroupEpoch::new(3).unwrap(),
            member_id: member(7),
            member_epoch: MemberEpoch::new(2).unwrap(),
        });
        assert_eq!(
            InternalRecordV1::decode(&offset.encode().unwrap()).unwrap(),
            offset
        );
    }

    #[test]
    fn p6_internal_topic_descriptors_are_exact_and_reserved() {
        assert_eq!(P6_INTERNAL_TOPICS.len(), 2);
        assert_eq!(P6_INTERNAL_TOPICS[0].kind, InternalTopicKind::Groups);
        assert_eq!(P6_INTERNAL_TOPICS[0].name, "__astralog_groups");
        assert_eq!(P6_INTERNAL_TOPICS[1].kind, InternalTopicKind::Offsets);
        assert_eq!(P6_INTERNAL_TOPICS[1].name, "__astralog_offsets");
        for descriptor in P6_INTERNAL_TOPICS {
            assert_eq!(descriptor.partition_count, 64);
            assert_eq!(descriptor.replication_factor, 3);
            assert!(is_reserved_internal_topic_name(descriptor.name));
        }
        assert!(!is_reserved_internal_topic_name("orders"));
    }

    #[test]
    fn frozen_group_hash_vectors_are_exact_and_topic_aligned() {
        let vectors = [
            ("g", 22_u32),
            ("orders", 9_u32),
            ("consumer-a", 21_u32),
            ("group-00000000000000000000000000000001", 59_u32),
        ];
        for (value, expected) in vectors {
            let group_id = GroupId::new(value).unwrap();
            let shard = group_shard(&group_id).unwrap();
            assert_eq!(
                shard, expected,
                "update only from frozen Murmur3 compatibility bytes"
            );
            assert!(shard < P6_INTERNAL_TOPIC_PARTITIONS);
            let groups_shard = shard;
            let offsets_shard = group_shard(&group_id).unwrap();
            assert_eq!(groups_shard, offsets_shard);
        }
    }

    #[test]
    fn group_record_rejects_invalid_bootstrap_epoch_and_dangling_assignment() {
        let mut value = stable_group();
        value.active_group_epoch = 0;
        assert!(
            InternalRecordV1::GroupState(value.clone())
                .encode()
                .is_err()
        );
        value.phase = GroupPhase::PreparingEpochFence;
        value.pending_group_epoch = Some(GroupEpoch::new(1).unwrap());
        assert!(InternalRecordV1::GroupState(value.clone()).encode().is_ok());
        value.active_assignments.insert(tp(2, 0), member(9));
        assert!(InternalRecordV1::GroupState(value).encode().is_err());
    }

    #[test]
    fn internal_decoder_rejects_trailing_unknown_and_noncanonical_bytes() {
        let record = InternalRecordV1::GroupState(stable_group());
        let mut bytes = record.encode().unwrap();
        bytes.push(0);
        assert!(InternalRecordV1::decode(&bytes).is_err());
        let mut unknown = record.encode().unwrap();
        unknown[2..4].copy_from_slice(&99_u16.to_le_bytes());
        assert!(InternalRecordV1::decode(&unknown).is_err());

        let mut group = stable_group();
        let member_id = member(8);
        group.members.insert(
            member_id,
            MemberRecordV1 {
                member_id,
                instance_id: None,
                member_epoch: MemberEpoch::new(1).unwrap(),
                session_timeout_ms: 30_000,
                subscriptions: vec![topic(2), topic(1)],
            },
        );
        assert!(InternalRecordV1::GroupState(group).encode().is_err());
    }

    #[test]
    fn internal_keys_match_frozen_identity_layouts() {
        let group = GroupId::new("g").unwrap();
        assert_eq!(encode_group_state_key(&group), b"g");
        assert_eq!(
            encode_offset_group_fence_key(&group).unwrap(),
            vec![1, 1, 0, b'g']
        );
        let key = encode_offset_value_key(&group, tp(1, 0x0102_0304)).unwrap();
        assert_eq!(&key[..4], &[3, 1, 0, b'g']);
        assert_eq!(&key[4..20], &[1; 16]);
        assert_eq!(&key[20..], &0x0102_0304_u32.to_le_bytes());
    }

    #[test]
    fn every_internal_record_kind_has_exact_golden_bytes() {
        let group = InternalRecordV1::GroupState(stable_group());
        assert_eq!(
            group.encode().unwrap(),
            hex_bytes(concat!(
                "01000100ad00000006006f7264657273040000000000000003000000000000000001",
                "0100000007070707070707070707070707070707010800776f726b65722d6102000000",
                "0000000030750000010000000101010101010101010101010101010101000000010101",
                "010101010101010101010101010000000007070707070707070707070707070707010000",
                "000101010101010101010101010101010100000000070707070707070707070707070707",
                "0700000000"
            ))
        );

        let mut owners = BTreeMap::new();
        owners.insert(tp(1, 0), (member(7), MemberEpoch::new(2).unwrap()));
        let group_fence = InternalRecordV1::OffsetGroupFence(OffsetGroupFenceV1 {
            group_id: GroupId::new("orders").unwrap(),
            group_epoch: GroupEpoch::new(3).unwrap(),
            source_group_state_version: GroupStateVersion::new(4).unwrap(),
            owners,
        });
        assert_eq!(
            group_fence.encode().unwrap(),
            hex_bytes(concat!(
                "010002004800000006006f72646572730300000000000000040000000000000001000000",
                "010101010101010101010101010101010000000007070707070707070707070707070707",
                "0200000000000000"
            ))
        );

        let owner_fence = InternalRecordV1::OffsetOwnerFence(OffsetOwnerFenceV1 {
            group_id: GroupId::new("orders").unwrap(),
            topic_partition: tp(1, 0),
            group_epoch: GroupEpoch::new(3).unwrap(),
            member_id: member(7),
            member_epoch: MemberEpoch::new(2).unwrap(),
            source_group_state_version: GroupStateVersion::new(5).unwrap(),
        });
        assert_eq!(
            owner_fence.encode().unwrap(),
            hex_bytes(concat!(
                "010003004400000006006f72646572730101010101010101010101010101010100000000",
                "030000000000000007070707070707070707070707070707020000000000000005000000",
                "00000000"
            ))
        );

        let offset = InternalRecordV1::OffsetValue(OffsetValueV1 {
            group_id: GroupId::new("g").unwrap(),
            topic_partition: tp(1, 2),
            next_offset: 3,
            metadata: "m".into(),
            commit_timestamp_ms: 4,
            group_epoch: GroupEpoch::new(5).unwrap(),
            member_id: member(6),
            member_epoch: MemberEpoch::new(7).unwrap(),
        });
        assert_eq!(
            offset.encode().unwrap(),
            hex_bytes(concat!(
                "010004004a00000001006701010101010101010101010101010101020000000300000000",
                "00000001006d040000000000000005000000000000000606060606060606060606060606",
                "06060700000000000000"
            ))
        );
    }
}
