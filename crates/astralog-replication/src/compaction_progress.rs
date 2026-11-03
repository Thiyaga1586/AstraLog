//! P9-G volatile leader-side clean-progress evidence.
//!
//! This is deliberately not cluster authority. Only a quorum-committed
//! AdvanceCompactionSafeOffset application entry can authorize deletion.
use std::collections::{BTreeMap, BTreeSet};

use astralog_common::{PartitionId, StreamOffset, TopicId};
use astralog_raft::{EffectiveConfiguration, NodeId};

use crate::{ReplicationError, Result};

const MAGIC: &[u8; 8] = b"ASTCLNP1";
const VERSION: u16 = 1;
pub const CLEAN_PROGRESS_REPORT_BYTES: usize = 80;

/// Fixed-size and CRC-protected transport sideband. No variable allocation or
/// user payload is permitted in this frame.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct CleanProgressReport {
    pub topic_id: TopicId,
    pub partition_id: PartitionId,
    pub term: u64,
    pub leader_epoch: u64,
    pub source_broker_id: u32,
    pub source_incarnation: u64,
    pub assignment_generation: u64,
    pub clean_compacted_offset: StreamOffset,
}

impl CleanProgressReport {
    pub fn encode(self) -> Result<[u8; CLEAN_PROGRESS_REPORT_BYTES]> {
        self.validate()?;
        let mut out = [0_u8; CLEAN_PROGRESS_REPORT_BYTES];
        out[..8].copy_from_slice(MAGIC);
        out[8..10].copy_from_slice(&VERSION.to_le_bytes());
        out[12..28].copy_from_slice(self.topic_id.as_bytes());
        out[28..32].copy_from_slice(&self.partition_id.get().to_le_bytes());
        out[32..40].copy_from_slice(&self.term.to_le_bytes());
        out[40..48].copy_from_slice(&self.leader_epoch.to_le_bytes());
        out[48..52].copy_from_slice(&self.source_broker_id.to_le_bytes());
        out[52..60].copy_from_slice(&self.source_incarnation.to_le_bytes());
        out[60..68].copy_from_slice(&self.assignment_generation.to_le_bytes());
        out[68..76].copy_from_slice(&self.clean_compacted_offset.get().to_le_bytes());
        let crc = crc32c::crc32c(&out[..76]);
        out[76..80].copy_from_slice(&crc.to_le_bytes());
        Ok(out)
    }

    pub fn decode(bytes: &[u8]) -> Result<Self> {
        if bytes.len() != CLEAN_PROGRESS_REPORT_BYTES
            || &bytes[..8] != MAGIC
            || u16::from_le_bytes([bytes[8], bytes[9]]) != VERSION
            || bytes[10..12] != [0, 0]
        {
            return Err(ReplicationError::Invalid(
                "P9 clean progress header/size invalid",
            ));
        }
        let expected = u32::from_le_bytes(bytes[76..80].try_into().expect("4-byte CRC"));
        let actual = crc32c::crc32c(&bytes[..76]);
        if actual != expected {
            return Err(ReplicationError::ChecksumMismatch {
                context: "P9 clean progress report",
                expected,
                actual,
            });
        }
        let mut topic = [0_u8; 16];
        topic.copy_from_slice(&bytes[12..28]);
        let report = Self {
            topic_id: TopicId::new(topic),
            partition_id: PartitionId::new(u32::from_le_bytes(bytes[28..32].try_into().unwrap())),
            term: u64::from_le_bytes(bytes[32..40].try_into().unwrap()),
            leader_epoch: u64::from_le_bytes(bytes[40..48].try_into().unwrap()),
            source_broker_id: u32::from_le_bytes(bytes[48..52].try_into().unwrap()),
            source_incarnation: u64::from_le_bytes(bytes[52..60].try_into().unwrap()),
            assignment_generation: u64::from_le_bytes(bytes[60..68].try_into().unwrap()),
            clean_compacted_offset: StreamOffset::new(u64::from_le_bytes(
                bytes[68..76].try_into().unwrap(),
            )),
        };
        report.validate()?;
        if report.encode()?.as_slice() != bytes {
            return Err(ReplicationError::Invalid(
                "non-canonical P9 clean progress report",
            ));
        }
        Ok(report)
    }

    fn validate(self) -> Result<()> {
        if self.term == 0
            || self.leader_epoch == 0
            || self.term != self.leader_epoch
            || self.source_incarnation == 0
            || self.assignment_generation == 0
            || self.source_broker_id == u32::MAX
        {
            return Err(ReplicationError::Invalid("invalid P9 clean progress fence"));
        }
        Ok(())
    }
}

/// A committed assignment maps *every* effective eligible Raft voter to its
/// broker identity/incarnation. Joint consensus uses the union, not a quorum.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct VoterIdentity {
    pub broker_id: u32,
    pub incarnation: u64,
}

/// Volatile and resettable upon term, assignment or eligible-voter changes.
/// Missing reports yield None; they are never represented as an optimistic
/// upper bound. A candidate is not a committed safe offset.
#[derive(Debug, Clone)]
pub struct CleanProgressTracker {
    topic_id: TopicId,
    partition_id: PartitionId,
    term: u64,
    assignment_generation: u64,
    eligible: BTreeMap<NodeId, VoterIdentity>,
    reports: BTreeMap<NodeId, StreamOffset>,
    last_report_tick: BTreeMap<NodeId, u64>,
    observation_tick: u64,
}

impl CleanProgressTracker {
    pub fn from_committed_assignment(
        topic_id: TopicId,
        partition_id: PartitionId,
        term: u64,
        assignment_generation: u64,
        effective: &EffectiveConfiguration,
        committed_voter_identities: &BTreeMap<NodeId, VoterIdentity>,
    ) -> Result<Self> {
        if term == 0 || assignment_generation == 0 {
            return Err(ReplicationError::Invalid(
                "invalid P9 progress tracking term/generation",
            ));
        }
        let mut eligible = BTreeMap::new();
        let mut seen_broker_ids = BTreeSet::new();
        for node in effective.voters() {
            let id =
                committed_voter_identities
                    .get(&node)
                    .copied()
                    .ok_or(ReplicationError::Invalid(
                        "missing committed P9 eligible voter identity",
                    ))?;
            if id.incarnation == 0
                || id.broker_id == u32::MAX
                || node.get() != u64::from(id.broker_id) + 1
                || !seen_broker_ids.insert(id.broker_id)
            {
                return Err(ReplicationError::Invalid(
                    "invalid committed P9 voter identity/incarnation",
                ));
            }
            eligible.insert(node, id);
        }
        if eligible.is_empty() {
            return Err(ReplicationError::Invalid("no eligible P9 voters"));
        }
        Ok(Self {
            topic_id,
            partition_id,
            term,
            assignment_generation,
            eligible,
            reports: BTreeMap::new(),
            last_report_tick: BTreeMap::new(),
            observation_tick: 0,
        })
    }

    /// Transport supplies independently authenticated source NodeId,
    /// broker-id and incarnation. Do not trust the report's claimed source.
    pub fn accept(
        &mut self,
        source: NodeId,
        observed_broker_id: u32,
        observed_incarnation: u64,
        report: CleanProgressReport,
        committed_high_watermark: StreamOffset,
    ) -> bool {
        if report.validate().is_err()
            || report.topic_id != self.topic_id
            || report.partition_id != self.partition_id
            || report.term != self.term
            || report.leader_epoch != self.term
            || report.assignment_generation != self.assignment_generation
            || report.source_broker_id != observed_broker_id
            || report.source_incarnation != observed_incarnation
            || report.clean_compacted_offset > committed_high_watermark
            || self.eligible.get(&source)
                != Some(&VoterIdentity {
                    broker_id: observed_broker_id,
                    incarnation: observed_incarnation,
                })
        {
            return false;
        }
        if self
            .reports
            .get(&source)
            .is_some_and(|old| report.clean_compacted_offset < *old)
        {
            // The reporter may have lost verified local progress after a
            // corrupt sidecar or manifest rollback. The earlier higher value
            // can no longer contribute to the all-voter minimum. A lower
            // report never rewinds committed authority; it revokes volatile
            // evidence until a fresh report is accepted.
            self.reports.remove(&source);
            self.last_report_tick.remove(&source);
            return false;
        }
        self.reports.insert(source, report.clean_compacted_offset);
        self.last_report_tick.insert(source, self.observation_tick);
        true
    }

    /// Injected logical broker tick, not wall-clock time. Reports from a
    /// disconnected, paused or congested eligible voter are revoked after
    /// the configured age, so a past valid report cannot indefinitely enable
    /// a later minimum. Rejected/foreign reports never renew freshness.
    pub fn advance_observation_tick(&mut self, max_report_age_ticks: u64) {
        self.observation_tick = self.observation_tick.saturating_add(1);
        if max_report_age_ticks == 0 {
            self.reports.clear();
            self.last_report_tick.clear();
            return;
        }
        self.last_report_tick.retain(|node, last| {
            let current = self.observation_tick.saturating_sub(*last) < max_report_age_ticks;
            if !current {
                self.reports.remove(node);
            }
            current
        });
    }

    pub fn candidate_safe_offset(&self) -> Option<StreamOffset> {
        let mut minimum = None;
        for node in self.eligible.keys() {
            let progress = *self.reports.get(node)?;
            minimum = Some(minimum.map_or(progress, |value: StreamOffset| value.min(progress)));
        }
        minimum
    }

    pub fn is_current_for(
        &self,
        topic_id: TopicId,
        partition_id: PartitionId,
        term: u64,
        assignment_generation: u64,
        effective: &EffectiveConfiguration,
    ) -> bool {
        self.topic_id == topic_id
            && self.partition_id == partition_id
            && self.term == term
            && self.assignment_generation == assignment_generation
            && self
                .eligible
                .keys()
                .copied()
                .collect::<std::collections::BTreeSet<_>>()
                == effective.voters()
    }

    /// P5 incarnation rotation may preserve the *same* Raft voter IDs and
    /// assignment generation. A leader must not reuse progress from the old
    /// process merely because those identifiers still match. The supplied
    /// identity pool may include a committed P5 movement destination, but
    /// only the identities of the current effective P3 voters may count.
    pub fn is_current_for_voter_identities(
        &self,
        topic_id: TopicId,
        partition_id: PartitionId,
        term: u64,
        assignment_generation: u64,
        effective: &EffectiveConfiguration,
        committed_identities: &BTreeMap<NodeId, VoterIdentity>,
    ) -> bool {
        self.is_current_for(
            topic_id,
            partition_id,
            term,
            assignment_generation,
            effective,
        ) && self
            .eligible
            .iter()
            .all(|(node, id)| committed_identities.get(node) == Some(id))
    }

    pub fn report_count(&self) -> usize {
        self.reports.len()
    }

    pub fn required_voter_count(&self) -> usize {
        self.eligible.len()
    }
}

/// A replica behind committed cluster authority must not be promoted until
/// authoritative V2 rebootstrap completes.
pub fn requires_retained_history_rebootstrap(
    local_clean_offset: StreamOffset,
    committed_safe_offset: StreamOffset,
) -> bool {
    local_clean_offset < committed_safe_offset
}

#[cfg(test)]
mod tests {
    use super::*;
    use astralog_raft::{JointConfiguration, StableConfiguration};

    fn node(id: u64) -> NodeId {
        NodeId::new(id).unwrap()
    }

    fn report(broker: u32, incarnation: u64, generation: u64, offset: u64) -> CleanProgressReport {
        CleanProgressReport {
            topic_id: TopicId::new([0x93; 16]),
            partition_id: PartitionId::new(4),
            term: 11,
            leader_epoch: 11,
            source_broker_id: broker,
            source_incarnation: incarnation,
            assignment_generation: generation,
            clean_compacted_offset: StreamOffset::new(offset),
        }
    }

    fn ids() -> BTreeMap<NodeId, VoterIdentity> {
        (1..=4)
            .map(|id| {
                (
                    node(id),
                    VoterIdentity {
                        broker_id: u32::try_from(id - 1).unwrap(),
                        incarnation: 9,
                    },
                )
            })
            .collect()
    }

    #[test]
    fn p9g_bounded_crc_report_detects_corruption_and_noncanonical_fields() {
        let original = report(2, 9, 23, 40);
        let bytes = original.encode().unwrap();
        assert_eq!(bytes.len(), CLEAN_PROGRESS_REPORT_BYTES);
        assert_eq!(CleanProgressReport::decode(&bytes).unwrap(), original);
        let mut corrupt = bytes;
        corrupt[70] ^= 1;
        assert!(CleanProgressReport::decode(&corrupt).is_err());
        let mut reserved = bytes;
        reserved[10] = 1;
        assert!(CleanProgressReport::decode(&reserved).is_err());
        assert!(CleanProgressReport::decode(&bytes[..79]).is_err());
        assert!(report(2, 0, 23, 40).encode().is_err());
    }

    #[test]
    fn p9g_missing_offline_eligible_voter_blocks_and_joint_includes_union() {
        let old = StableConfiguration::new([node(1), node(2), node(3)], []).unwrap();
        let new = StableConfiguration::new([node(2), node(3), node(4)], []).unwrap();
        let joint = EffectiveConfiguration::Joint(JointConfiguration::new(old, new).unwrap());
        let mut tracker = CleanProgressTracker::from_committed_assignment(
            TopicId::new([0x93; 16]),
            PartitionId::new(4),
            11,
            23,
            &joint,
            &ids(),
        )
        .unwrap();
        assert_eq!(tracker.required_voter_count(), 4);
        for (id, value) in [(1, 90), (2, 80), (3, 70)] {
            assert!(tracker.accept(
                node(id),
                (id - 1) as u32,
                9,
                report((id - 1) as u32, 9, 23, value),
                StreamOffset::new(100)
            ));
        }
        assert_eq!(
            tracker.candidate_safe_offset(),
            None,
            "missing joint voter blocks"
        );
        assert!(!tracker.accept(node(4), 3, 8, report(3, 9, 23, 60), StreamOffset::new(100)));
        assert!(!tracker.accept(node(4), 3, 9, report(3, 9, 22, 60), StreamOffset::new(100)));
        assert!(tracker.accept(node(4), 3, 9, report(3, 9, 23, 60), StreamOffset::new(100)));
        assert_eq!(tracker.candidate_safe_offset(), Some(StreamOffset::new(60)));
        assert!(!tracker.accept(node(4), 3, 9, report(3, 9, 23, 59), StreamOffset::new(100)));
        assert_eq!(
            tracker.candidate_safe_offset(),
            None,
            "regression must revoke old evidence"
        );
        assert_eq!(tracker.report_count(), 3);
        assert!(tracker.accept(node(4), 3, 9, report(3, 9, 23, 59), StreamOffset::new(100)));
        assert_eq!(tracker.candidate_safe_offset(), Some(StreamOffset::new(59)));
    }

    #[test]
    fn p9g_joint_old_only_report_expires_while_all_three_survivors_remain_positive() {
        // P5 old-only broker 0 stays an eligible P3 joint voter until a
        // committed exclusion; the other three brokers can still satisfy
        // both Raft majorities. Their fresh, positive sideband reports must
        // never substitute for an expired old-only report.
        let old = StableConfiguration::new([node(1), node(2), node(3)], []).unwrap();
        let new = StableConfiguration::new([node(2), node(3), node(4)], []).unwrap();
        let joint = EffectiveConfiguration::Joint(JointConfiguration::new(old, new).unwrap());
        let mut tracker = CleanProgressTracker::from_committed_assignment(
            TopicId::new([0x93; 16]),
            PartitionId::new(4),
            11,
            23,
            &joint,
            &ids(),
        )
        .unwrap();
        assert_eq!(tracker.required_voter_count(), 4);
        for (broker, clean) in [(0, 90), (1, 80), (2, 70), (3, 60)] {
            assert!(tracker.accept(
                node(u64::from(broker) + 1),
                broker,
                9,
                report(broker, 9, 23, clean),
                StreamOffset::new(100),
            ));
        }
        assert_eq!(tracker.candidate_safe_offset(), Some(StreamOffset::new(60)));

        // Only the three survivors report again. Every renewal is a genuine
        // authenticated current-term/current-incarnation report; expired
        // broker 0 is not replaced by either a P3 majority or a P5 proposal.
        for tick in 1..=10 {
            tracker.advance_observation_tick(10);
            for (broker, clean) in [(1, 80), (2, 70), (3, 60)] {
                assert!(tracker.accept(
                    node(u64::from(broker) + 1),
                    broker,
                    9,
                    report(broker, 9, 23, clean),
                    StreamOffset::new(100),
                ));
            }
            if tick < 10 {
                assert_eq!(tracker.candidate_safe_offset(), Some(StreamOffset::new(60)));
            }
        }
        assert_eq!(tracker.report_count(), 3);
        assert_eq!(
            tracker.candidate_safe_offset(),
            None,
            "old-only voter expiry must block even though every survivor has fresh positive progress"
        );
        assert!(!tracker.accept(node(1), 0, 8, report(0, 9, 23, 90), StreamOffset::new(100)));
        assert_eq!(tracker.candidate_safe_offset(), None);
        assert!(tracker.accept(node(1), 0, 9, report(0, 9, 23, 90), StreamOffset::new(100)));
        assert_eq!(tracker.candidate_safe_offset(), Some(StreamOffset::new(60)));
    }

    #[test]
    fn p9g_committed_exclusion_requires_new_assignment_and_resets_reports() {
        let old = EffectiveConfiguration::Stable(
            StableConfiguration::new([node(1), node(2), node(3)], []).unwrap(),
        );
        let replacement = EffectiveConfiguration::Stable(
            StableConfiguration::new([node(1), node(2)], [node(3)]).unwrap(),
        );
        let mut first = CleanProgressTracker::from_committed_assignment(
            TopicId::new([0x93; 16]),
            PartitionId::new(4),
            11,
            23,
            &old,
            &ids(),
        )
        .unwrap();
        assert!(first.accept(node(1), 0, 9, report(0, 9, 23, 50), StreamOffset::new(80)));
        assert!(first.accept(node(2), 1, 9, report(1, 9, 23, 40), StreamOffset::new(80)));
        assert_eq!(
            first.candidate_safe_offset(),
            None,
            "old offline voter still blocks"
        );
        let mut second = CleanProgressTracker::from_committed_assignment(
            TopicId::new([0x93; 16]),
            PartitionId::new(4),
            11,
            24,
            &replacement,
            &ids(),
        )
        .unwrap();
        assert_eq!(
            second.candidate_safe_offset(),
            None,
            "old reports cannot transfer assignments"
        );
        assert!(
            !second.accept(node(3), 2, 9, report(2, 9, 24, 70), StreamOffset::new(80)),
            "learner ignored"
        );
        assert!(!second.accept(node(1), 0, 9, report(0, 9, 23, 50), StreamOffset::new(80)));
        assert!(second.accept(node(1), 0, 9, report(0, 9, 24, 50), StreamOffset::new(80)));
        assert!(second.accept(node(2), 1, 9, report(1, 9, 24, 40), StreamOffset::new(80)));
        assert_eq!(second.candidate_safe_offset(), Some(StreamOffset::new(40)));
        assert!(requires_retained_history_rebootstrap(
            StreamOffset::new(39),
            StreamOffset::new(40)
        ));
        assert!(!requires_retained_history_rebootstrap(
            StreamOffset::new(40),
            StreamOffset::new(40)
        ));
    }

    #[test]
    fn p9g_offline_voter_report_expires_and_blocks_minimum_without_replacement() {
        let effective = EffectiveConfiguration::Stable(
            StableConfiguration::new([node(1), node(2), node(3)], []).unwrap(),
        );
        let mut tracker = CleanProgressTracker::from_committed_assignment(
            TopicId::new([0x93; 16]),
            PartitionId::new(4),
            11,
            23,
            &effective,
            &ids(),
        )
        .unwrap();
        for broker in 0..3_u32 {
            assert!(tracker.accept(
                node(u64::from(broker) + 1),
                broker,
                9,
                report(broker, 9, 23, 70 + u64::from(broker)),
                StreamOffset::new(100),
            ));
        }
        assert_eq!(tracker.candidate_safe_offset(), Some(StreamOffset::new(70)));
        for _ in 0..4 {
            tracker.advance_observation_tick(5);
            assert!(tracker.accept(node(1), 0, 9, report(0, 9, 23, 70), StreamOffset::new(100)));
            assert!(tracker.accept(node(2), 1, 9, report(1, 9, 23, 71), StreamOffset::new(100)));
        }
        assert_eq!(tracker.candidate_safe_offset(), Some(StreamOffset::new(70)));
        tracker.advance_observation_tick(5);
        assert_eq!(tracker.report_count(), 2);
        assert_eq!(
            tracker.candidate_safe_offset(),
            None,
            "offline voter must revoke safe candidate"
        );
        assert!(!tracker.accept(node(3), 2, 8, report(2, 9, 23, 72), StreamOffset::new(100)));
        assert_eq!(
            tracker.candidate_safe_offset(),
            None,
            "invalid incarnation cannot refresh"
        );
        assert!(tracker.accept(node(3), 2, 9, report(2, 9, 23, 72), StreamOffset::new(100)));
        assert_eq!(tracker.candidate_safe_offset(), Some(StreamOffset::new(70)));
        tracker.advance_observation_tick(0);
        assert_eq!(
            tracker.candidate_safe_offset(),
            None,
            "zero age fails closed"
        );
    }

    #[test]
    fn p9g_unchanged_voters_and_assignment_must_revoke_old_incarnation_reports() {
        let effective = EffectiveConfiguration::Stable(
            StableConfiguration::new([node(1), node(2), node(3)], []).unwrap(),
        );
        let mut committed = ids();
        let mut old = CleanProgressTracker::from_committed_assignment(
            TopicId::new([0x93; 16]),
            PartitionId::new(4),
            11,
            23,
            &effective,
            &committed,
        )
        .unwrap();
        for broker in 0..3_u32 {
            assert!(old.accept(
                node(u64::from(broker) + 1),
                broker,
                9,
                report(broker, 9, 23, 90),
                StreamOffset::new(100),
            ));
        }
        assert_eq!(old.candidate_safe_offset(), Some(StreamOffset::new(90)));
        assert!(old.is_current_for_voter_identities(
            TopicId::new([0x93; 16]),
            PartitionId::new(4),
            11,
            23,
            &effective,
            &committed,
        ));

        // An ordinary broker rejoin changes the committed identity but may
        // keep P3 voters [1,2,3], term 11, and assignment generation 23.
        committed.get_mut(&node(3)).unwrap().incarnation = 10;
        assert!(
            old.is_current_for(
                TopicId::new([0x93; 16]),
                PartitionId::new(4),
                11,
                23,
                &effective,
            ),
            "legacy voter-set comparison alone cannot detect a P5 rejoin"
        );
        assert!(
            !old.is_current_for_voter_identities(
                TopicId::new([0x93; 16]),
                PartitionId::new(4),
                11,
                23,
                &effective,
                &committed,
            ),
            "cached reports from the previous broker incarnation are stale"
        );
        let mut renewed = CleanProgressTracker::from_committed_assignment(
            TopicId::new([0x93; 16]),
            PartitionId::new(4),
            11,
            23,
            &effective,
            &committed,
        )
        .unwrap();
        assert_eq!(renewed.candidate_safe_offset(), None);
        for broker in 0..2_u32 {
            assert!(renewed.accept(
                node(u64::from(broker) + 1),
                broker,
                9,
                report(broker, 9, 23, 75),
                StreamOffset::new(100),
            ));
        }
        assert!(
            !renewed.accept(node(3), 2, 9, report(2, 9, 23, 75), StreamOffset::new(100),),
            "old authenticated transport incarnation cannot substitute for the rejoined voter"
        );
        assert_eq!(renewed.candidate_safe_offset(), None);
        let mut updated = report(2, 10, 23, 75);
        updated.clean_compacted_offset = StreamOffset::new(60);
        assert!(renewed.accept(node(3), 2, 10, updated, StreamOffset::new(100)));
        assert_eq!(renewed.candidate_safe_offset(), Some(StreamOffset::new(60)));
    }
}
