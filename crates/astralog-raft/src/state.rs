use std::collections::BTreeMap;

use crate::{
    EffectiveConfiguration, LogEntry, LogIndex, NodeId, RaftError, Result, SnapshotId,
    StableConfiguration, Term,
};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct TimingConfig {
    pub heartbeat_interval: u64,
    pub election_timeout_min: u64,
    pub election_timeout_max: u64,
}

impl TimingConfig {
    pub fn new(
        heartbeat_interval: u64,
        election_timeout_min: u64,
        election_timeout_max: u64,
    ) -> Result<Self> {
        if heartbeat_interval == 0 {
            return Err(RaftError::InvalidTiming(
                "heartbeat interval must be non-zero",
            ));
        }
        let minimum = heartbeat_interval
            .checked_mul(5)
            .ok_or(RaftError::InvalidTiming("heartbeat ratio overflow"))?;
        if election_timeout_min < minimum {
            return Err(RaftError::InvalidTiming(
                "election timeout minimum must be at least 5x heartbeat interval",
            ));
        }
        if election_timeout_max <= election_timeout_min {
            return Err(RaftError::InvalidTiming(
                "election timeout maximum must exceed minimum",
            ));
        }
        Ok(Self {
            heartbeat_interval,
            election_timeout_min,
            election_timeout_max,
        })
    }

    pub const fn contains_election_timeout(self, sample: u64) -> bool {
        sample >= self.election_timeout_min && sample <= self.election_timeout_max
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PreVoteDecision {
    Granted,
    Rejected,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum VoteDecision {
    Granted,
    Rejected,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PreVoteRound {
    pub prospective_term: Term,
    pub responses: BTreeMap<NodeId, PreVoteDecision>,
    pub quorum_reached: bool,
}

impl PreVoteRound {
    pub fn granted_voters(&self) -> std::collections::BTreeSet<NodeId> {
        self.responses
            .iter()
            .filter_map(|(node, decision)| (*decision == PreVoteDecision::Granted).then_some(*node))
            .collect()
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ElectionRound {
    pub term: Term,
    pub responses: BTreeMap<NodeId, VoteDecision>,
    pub quorum_reached: bool,
}

impl ElectionRound {
    pub fn granted_voters(&self) -> std::collections::BTreeSet<NodeId> {
        self.responses
            .iter()
            .filter_map(|(node, decision)| (*decision == VoteDecision::Granted).then_some(*node))
            .collect()
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct HardState {
    pub current_term: Term,
    pub voted_for: Option<NodeId>,
    pub durable_commit_index: LogIndex,
}

impl HardState {
    pub const fn initial() -> Self {
        Self {
            current_term: Term::ZERO,
            voted_for: None,
            durable_commit_index: LogIndex::ZERO,
        }
    }

    pub fn validate(
        &self,
        snapshot_index: LogIndex,
        stable_last_log_index: LogIndex,
    ) -> Result<()> {
        if self.durable_commit_index < snapshot_index {
            return Err(RaftError::InvalidHardState(
                "durable commit index is below snapshot boundary",
            ));
        }
        if self.durable_commit_index > stable_last_log_index {
            return Err(RaftError::InvalidHardState(
                "durable commit index exceeds stable history",
            ));
        }
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SnapshotMetadata {
    pub last_included_index: LogIndex,
    pub last_included_term: Term,
    pub configuration: StableConfiguration,
    pub snapshot_id: SnapshotId,
}

impl SnapshotMetadata {
    pub fn new(
        last_included_index: LogIndex,
        last_included_term: Term,
        configuration: StableConfiguration,
        snapshot_id: SnapshotId,
    ) -> Result<Self> {
        if last_included_index == LogIndex::ZERO && last_included_term != Term::ZERO {
            return Err(RaftError::InvalidSnapshot(
                "zero snapshot index requires zero term",
            ));
        }
        if last_included_index != LogIndex::ZERO && last_included_term == Term::ZERO {
            return Err(RaftError::InvalidSnapshot(
                "non-zero snapshot index requires non-zero term",
            ));
        }
        Ok(Self {
            last_included_index,
            last_included_term,
            configuration,
            snapshot_id,
        })
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Role {
    Follower,
    PreCandidate,
    Candidate,
    Leader,
    Learner,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ReplicationMode {
    Probe,
    Replicate,
    Snapshot,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Progress {
    pub match_index: LogIndex,
    pub next_index: LogIndex,
    pub recently_active: bool,
    pub mode: ReplicationMode,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LeadershipTransfer {
    pub target: NodeId,
    pub elapsed: u64,
    pub timeout_now_sent: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RaftLimits {
    pub max_uncommitted_application_bytes: usize,
    pub max_pending_persistence_batches: usize,
    pub max_pending_persistence_bytes: usize,
    pub max_replication_send_bytes: usize,
}

impl RaftLimits {
    pub fn new(max_uncommitted_application_bytes: usize) -> Result<Self> {
        Self::with_persistence_queue(
            max_uncommitted_application_bytes,
            1,
            max_uncommitted_application_bytes,
        )
    }

    pub fn with_persistence_queue(
        max_uncommitted_application_bytes: usize,
        max_pending_persistence_batches: usize,
        max_pending_persistence_bytes: usize,
    ) -> Result<Self> {
        if max_uncommitted_application_bytes == 0 {
            return Err(RaftError::InvalidHardState(
                "max uncommitted application bytes must be non-zero",
            ));
        }
        if max_pending_persistence_batches == 0 {
            return Err(RaftError::InvalidHardState(
                "max pending persistence batches must be non-zero",
            ));
        }
        if max_pending_persistence_bytes == 0 {
            return Err(RaftError::InvalidHardState(
                "max pending persistence bytes must be non-zero",
            ));
        }
        Ok(Self {
            max_uncommitted_application_bytes,
            max_pending_persistence_batches,
            max_pending_persistence_bytes,
            // Historical P3 behavior is preserved unless the embedding runtime
            // explicitly opts into a bounded replication-send window.
            max_replication_send_bytes: usize::MAX,
        })
    }

    pub fn with_replication_send_bytes(
        mut self,
        max_replication_send_bytes: usize,
    ) -> Result<Self> {
        if max_replication_send_bytes == 0 {
            return Err(RaftError::InvalidHardState(
                "max replication send bytes must be non-zero",
            ));
        }
        self.max_replication_send_bytes = max_replication_send_bytes;
        Ok(self)
    }
}

impl Default for RaftLimits {
    fn default() -> Self {
        Self {
            max_uncommitted_application_bytes: 16 * 1024 * 1024,
            max_pending_persistence_batches: 1,
            max_pending_persistence_bytes: 16 * 1024 * 1024,
            max_replication_send_bytes: usize::MAX,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct VolatileState {
    pub role: Role,
    pub leader_id: Option<NodeId>,
    pub commit_index: LogIndex,
    pub last_applied: LogIndex,
    pub election_elapsed: u64,
    pub randomized_election_timeout: u64,
    pub effective_configuration: EffectiveConfiguration,
    pub pending_configuration_index: Option<LogIndex>,
    pub progress: BTreeMap<NodeId, Progress>,
    pub pre_vote: Option<PreVoteRound>,
    pub election: Option<ElectionRound>,
    pub heartbeat_elapsed: u64,
    pub leadership_transfer: Option<LeadershipTransfer>,
}

impl VolatileState {
    pub fn recovered(
        local_node: NodeId,
        hard_state: &HardState,
        snapshot: &SnapshotMetadata,
        effective_configuration: EffectiveConfiguration,
        election_timeout: u64,
    ) -> Self {
        let role = if effective_configuration.contains_voter(local_node) {
            Role::Follower
        } else {
            Role::Learner
        };
        Self {
            role,
            leader_id: None,
            commit_index: hard_state.durable_commit_index,
            last_applied: snapshot.last_included_index,
            election_elapsed: 0,
            randomized_election_timeout: election_timeout,
            effective_configuration,
            pending_configuration_index: None,
            progress: BTreeMap::new(),
            pre_vote: None,
            election: None,
            heartbeat_elapsed: 0,
            leadership_transfer: None,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RecoveredState {
    pub hard_state: HardState,
    pub snapshot: SnapshotMetadata,
    pub log: Vec<LogEntry>,
    pub effective_configuration: EffectiveConfiguration,
    pub pending_configuration_index: Option<LogIndex>,
}

impl RecoveredState {
    pub fn from_persisted(
        hard_state: HardState,
        snapshot: SnapshotMetadata,
        log: Vec<LogEntry>,
    ) -> Result<Self> {
        let (effective_configuration, pending_configuration_index) =
            derive_membership(&snapshot, &log, hard_state.durable_commit_index)?;

        let recovered = Self {
            hard_state,
            snapshot,
            log,
            effective_configuration,
            pending_configuration_index,
        };
        recovered.validate()?;
        Ok(recovered)
    }

    pub fn validate(&self) -> Result<()> {
        let mut expected = self.snapshot.last_included_index;
        for entry in &self.log {
            if entry.term == Term::ZERO {
                return Err(RaftError::InvalidRecoveredLog(
                    "real log entry has term zero",
                ));
            }
            expected = expected.checked_next()?;
            if entry.index != expected {
                return Err(RaftError::InvalidRecoveredLog(
                    "retained log is not contiguous above snapshot",
                ));
            }
            if entry.term > self.hard_state.current_term {
                return Err(RaftError::InvalidRecoveredLog(
                    "log entry term exceeds recovered current term",
                ));
            }
        }
        if self.snapshot.last_included_term > self.hard_state.current_term {
            return Err(RaftError::InvalidHardState(
                "snapshot term exceeds recovered current term",
            ));
        }
        let stable_last = self
            .log
            .last()
            .map_or(self.snapshot.last_included_index, |entry| entry.index);
        self.hard_state
            .validate(self.snapshot.last_included_index, stable_last)?;
        // `voted_for` records the single durable vote cast in `current_term`.
        // A later configuration entry in the same term may remove that node.
        // Recovery must preserve the vote rather than reinterpret eligibility
        // using the newest configuration and accidentally permit a second vote.
        if let Some(pending) = self.pending_configuration_index
            && (pending <= self.snapshot.last_included_index || pending > stable_last)
        {
            return Err(RaftError::InvalidRecoveredLog(
                "pending configuration index is outside retained log",
            ));
        }

        if self
            .log
            .iter()
            .any(|entry| matches!(entry.kind, crate::EntryKind::Configuration(_)))
        {
            let (derived_configuration, derived_pending) = derive_membership(
                &self.snapshot,
                &self.log,
                self.hard_state.durable_commit_index,
            )?;
            if derived_configuration != self.effective_configuration
                || derived_pending != self.pending_configuration_index
            {
                return Err(RaftError::InvalidRecoveredLog(
                    "effective membership does not match snapshot + durable configuration log",
                ));
            }
        }
        Ok(())
    }
}

fn derive_membership(
    snapshot: &SnapshotMetadata,
    log: &[LogEntry],
    durable_commit_index: LogIndex,
) -> Result<(EffectiveConfiguration, Option<LogIndex>)> {
    let mut effective_configuration =
        EffectiveConfiguration::Stable(snapshot.configuration.clone());
    let mut pending_configuration_index = None;

    for entry in log {
        let crate::EntryKind::Configuration(change) = &entry.kind else {
            continue;
        };
        match change {
            crate::ConfigurationChange::EnterJoint { old, new } => {
                match &effective_configuration {
                    EffectiveConfiguration::Stable(current) if current == old => {}
                    _ => {
                        return Err(RaftError::InvalidRecoveredLog(
                            "EnterJoint old configuration does not match recovered stable configuration",
                        ));
                    }
                }
                effective_configuration = EffectiveConfiguration::Joint(
                    crate::JointConfiguration::new(old.clone(), new.clone())?,
                );
                pending_configuration_index = Some(entry.index);
            }
            crate::ConfigurationChange::LeaveJoint { new } => {
                match &effective_configuration {
                    EffectiveConfiguration::Joint(joint) if joint.new_config() == new => {}
                    _ => {
                        return Err(RaftError::InvalidRecoveredLog(
                            "LeaveJoint does not match recovered joint configuration",
                        ));
                    }
                }
                effective_configuration = EffectiveConfiguration::Stable(new.clone());
                pending_configuration_index = Some(entry.index);
            }
        }
    }

    if matches!(effective_configuration, EffectiveConfiguration::Stable(_))
        && pending_configuration_index.is_some_and(|index| index <= durable_commit_index)
    {
        pending_configuration_index = None;
    }

    Ok((effective_configuration, pending_configuration_index))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn node(value: u64) -> NodeId {
        NodeId::new(value).unwrap()
    }

    fn stable() -> StableConfiguration {
        StableConfiguration::new([node(1), node(2), node(3)], []).unwrap()
    }

    fn empty_snapshot() -> SnapshotMetadata {
        SnapshotMetadata::new(LogIndex::ZERO, Term::ZERO, stable(), SnapshotId::ZERO).unwrap()
    }

    #[test]
    fn hard_state_commit_must_be_inside_stable_history() {
        let hard = HardState {
            current_term: Term::new(2),
            voted_for: Some(node(1)),
            durable_commit_index: LogIndex::new(3),
        };
        assert_eq!(
            hard.validate(LogIndex::ZERO, LogIndex::new(2)),
            Err(RaftError::InvalidHardState(
                "durable commit index exceeds stable history"
            ))
        );
    }

    #[test]
    fn recovered_log_must_be_contiguous_and_nonzero_term() {
        let state = RecoveredState {
            hard_state: HardState::initial(),
            snapshot: empty_snapshot(),
            log: vec![LogEntry::new(
                Term::new(1),
                LogIndex::new(2),
                crate::EntryKind::LeaderBarrier,
            )],
            effective_configuration: EffectiveConfiguration::Stable(stable()),
            pending_configuration_index: None,
        };
        assert_eq!(
            state.validate(),
            Err(RaftError::InvalidRecoveredLog(
                "retained log is not contiguous above snapshot"
            ))
        );
    }

    #[test]
    fn recovery_selects_follower_or_learner_from_membership() {
        let hard = HardState::initial();
        let snapshot = empty_snapshot();
        let voters = EffectiveConfiguration::Stable(stable());
        let follower = VolatileState::recovered(node(1), &hard, &snapshot, voters, 10);
        assert_eq!(follower.role, Role::Follower);

        let with_learner =
            EffectiveConfiguration::Stable(StableConfiguration::new([node(1)], [node(4)]).unwrap());
        let learner = VolatileState::recovered(node(4), &hard, &snapshot, with_learner, 10);
        assert_eq!(learner.role, Role::Learner);
    }

    #[test]
    fn timing_contract_enforces_frozen_ratio_and_range() {
        assert_eq!(
            TimingConfig::new(0, 5, 10),
            Err(RaftError::InvalidTiming(
                "heartbeat interval must be non-zero"
            ))
        );
        assert_eq!(
            TimingConfig::new(2, 9, 20),
            Err(RaftError::InvalidTiming(
                "election timeout minimum must be at least 5x heartbeat interval"
            ))
        );
        let timing = TimingConfig::new(2, 10, 20).unwrap();
        assert!(timing.contains_election_timeout(10));
        assert!(timing.contains_election_timeout(20));
        assert!(!timing.contains_election_timeout(9));
    }

    #[test]
    fn recovery_reconstructs_joint_and_final_stable_configuration_from_log() {
        let old = StableConfiguration::new([node(1), node(2), node(3)], [node(4)]).unwrap();
        let new = StableConfiguration::new([node(2), node(3), node(4)], []).unwrap();
        let snapshot =
            SnapshotMetadata::new(LogIndex::ZERO, Term::ZERO, old.clone(), SnapshotId::ZERO)
                .unwrap();
        let enter = LogEntry::new(
            Term::new(1),
            LogIndex::new(1),
            crate::EntryKind::Configuration(crate::ConfigurationChange::EnterJoint {
                old: old.clone(),
                new: new.clone(),
            }),
        );

        let joint = RecoveredState::from_persisted(
            HardState {
                current_term: Term::new(1),
                voted_for: Some(node(1)),
                durable_commit_index: LogIndex::ZERO,
            },
            snapshot.clone(),
            vec![enter.clone()],
        )
        .unwrap();
        assert!(matches!(
            joint.effective_configuration,
            EffectiveConfiguration::Joint(_)
        ));
        assert_eq!(joint.pending_configuration_index, Some(LogIndex::new(1)));

        let leave = LogEntry::new(
            Term::new(1),
            LogIndex::new(2),
            crate::EntryKind::Configuration(crate::ConfigurationChange::LeaveJoint {
                new: new.clone(),
            }),
        );
        let stable = RecoveredState::from_persisted(
            HardState {
                current_term: Term::new(1),
                voted_for: Some(node(1)),
                durable_commit_index: LogIndex::new(2),
            },
            snapshot,
            vec![enter, leave],
        )
        .unwrap();
        assert_eq!(
            stable.effective_configuration,
            EffectiveConfiguration::Stable(new)
        );
        assert_eq!(stable.pending_configuration_index, None);
        assert_eq!(stable.hard_state.voted_for, Some(node(1)));
    }

    #[test]
    fn recovery_rejects_history_from_a_term_newer_than_hard_state() {
        let state = RecoveredState {
            hard_state: HardState::initial(),
            snapshot: empty_snapshot(),
            log: vec![LogEntry::new(
                Term::new(1),
                LogIndex::FIRST,
                crate::EntryKind::LeaderBarrier,
            )],
            effective_configuration: EffectiveConfiguration::Stable(stable()),
            pending_configuration_index: None,
        };
        assert_eq!(
            state.validate(),
            Err(RaftError::InvalidRecoveredLog(
                "log entry term exceeds recovered current term"
            ))
        );
    }
}
