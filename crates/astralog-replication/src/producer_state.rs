use std::collections::VecDeque;

use astralog_common::{
    ProducerAuthority, ProducerEpoch, ProducerId, ProducerSequence, RaftIndex, StreamOffset,
};

pub const MAX_RECENT_COMMITTED_BATCHES: usize = 5;
pub const MAX_PARTITION_PRODUCER_ENTRIES: usize = 4096;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ProducerStateError {
    ProducerFenced,
    OutOfOrderSequence,
    InvalidProducer,
    InvalidBatch(&'static str),
    InvalidCommittedHistory(&'static str),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ProducerBatchIdentityV1 {
    pub authority: ProducerAuthority,
    pub base_sequence: ProducerSequence,
    pub last_sequence: ProducerSequence,
    pub record_count: u32,
    pub canonical_digest: [u8; 32],
}

impl ProducerBatchIdentityV1 {
    pub fn new(
        authority: ProducerAuthority,
        base_sequence: ProducerSequence,
        record_count: u32,
        canonical_digest: [u8; 32],
    ) -> Result<Self, ProducerStateError> {
        if record_count == 0 {
            return Err(ProducerStateError::InvalidBatch(
                "producer batch record count must be non-zero",
            ));
        }
        let Some(last_sequence) = base_sequence.checked_last(record_count) else {
            return Err(ProducerStateError::InvalidBatch(
                "producer batch sequence range overflow",
            ));
        };
        if base_sequence.checked_next_after(record_count).is_none() {
            return Err(ProducerStateError::InvalidBatch(
                "producer batch sequence range leaves no next sequence",
            ));
        }
        Ok(Self {
            authority,
            base_sequence,
            last_sequence,
            record_count,
            canonical_digest,
        })
    }

    pub fn next_sequence(self) -> ProducerSequence {
        self.base_sequence
            .checked_next_after(self.record_count)
            .expect("validated producer batch has next sequence")
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct AppendCandidateV1 {
    pub batch: ProducerBatchIdentityV1,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct CommittedRetryV1 {
    pub base_stream_offset: StreamOffset,
    pub last_stream_offset: StreamOffset,
    pub raft_index: RaftIndex,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ProducerAdmissionV1 {
    Append(AppendCandidateV1),
    Duplicate(CommittedRetryV1),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RecentBatchV1 {
    pub base_sequence: ProducerSequence,
    pub last_sequence: ProducerSequence,
    pub record_count: u32,
    pub base_stream_offset: StreamOffset,
    pub last_stream_offset: StreamOffset,
    pub raft_index: RaftIndex,
    pub canonical_digest: [u8; 32],
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct CommittedBatchV1 {
    pub authority: ProducerAuthority,
    pub base_sequence: ProducerSequence,
    pub last_sequence: ProducerSequence,
    pub record_count: u32,
    pub base_stream_offset: StreamOffset,
    pub last_stream_offset: StreamOffset,
    pub raft_index: RaftIndex,
    pub canonical_digest: [u8; 32],
}

impl CommittedBatchV1 {
    pub fn new(
        authority: ProducerAuthority,
        base_sequence: ProducerSequence,
        record_count: u32,
        base_stream_offset: StreamOffset,
        raft_index: RaftIndex,
        canonical_digest: [u8; 32],
    ) -> Result<Self, ProducerStateError> {
        let identity =
            ProducerBatchIdentityV1::new(authority, base_sequence, record_count, canonical_digest)?;
        let Some(last_stream_offset) = base_stream_offset.checked_add(u64::from(record_count - 1))
        else {
            return Err(ProducerStateError::InvalidBatch(
                "producer batch stream-offset range overflow",
            ));
        };
        Ok(Self {
            authority,
            base_sequence,
            last_sequence: identity.last_sequence,
            record_count,
            base_stream_offset,
            last_stream_offset,
            raft_index,
            canonical_digest,
        })
    }

    fn recent(self) -> RecentBatchV1 {
        RecentBatchV1 {
            base_sequence: self.base_sequence,
            last_sequence: self.last_sequence,
            record_count: self.record_count,
            base_stream_offset: self.base_stream_offset,
            last_stream_offset: self.last_stream_offset,
            raft_index: self.raft_index,
            canonical_digest: self.canonical_digest,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CommitApplyOutcomeV1 {
    Applied,
    AlreadyApplied,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PartitionProducerStateV1 {
    producer_id: ProducerId,
    accepted_epoch: ProducerEpoch,
    next_sequence: ProducerSequence,
    recent_committed_batches: VecDeque<RecentBatchV1>,
}

impl PartitionProducerStateV1 {
    pub fn from_snapshot_parts(
        producer_id: ProducerId,
        accepted_epoch: ProducerEpoch,
        next_sequence: ProducerSequence,
        recent_committed_batches: VecDeque<RecentBatchV1>,
    ) -> Result<Self, ProducerStateError> {
        if recent_committed_batches.is_empty()
            || recent_committed_batches.len() > MAX_RECENT_COMMITTED_BATCHES
        {
            return Err(ProducerStateError::InvalidCommittedHistory(
                "producer snapshot recent-batch count is outside 1..=5",
            ));
        }
        let mut previous: Option<&RecentBatchV1> = None;
        for recent in &recent_committed_batches {
            if recent.record_count == 0
                || recent.base_sequence.checked_last(recent.record_count)
                    != Some(recent.last_sequence)
                || recent
                    .base_stream_offset
                    .checked_add(u64::from(recent.record_count - 1))
                    != Some(recent.last_stream_offset)
            {
                return Err(ProducerStateError::InvalidCommittedHistory(
                    "producer snapshot contains an invalid retained batch range",
                ));
            }
            if let Some(previous) = previous
                && (previous.last_sequence.checked_next_after(1) != Some(recent.base_sequence)
                    || recent.raft_index <= previous.raft_index
                    || recent.base_stream_offset <= previous.last_stream_offset)
            {
                return Err(ProducerStateError::InvalidCommittedHistory(
                    "producer snapshot retained batches are not canonically ordered",
                ));
            }
            previous = Some(recent);
        }
        let last = recent_committed_batches
            .back()
            .expect("non-empty recent producer snapshot window");
        if last.last_sequence.checked_next_after(1) != Some(next_sequence) {
            return Err(ProducerStateError::InvalidCommittedHistory(
                "producer snapshot next sequence does not follow retained history",
            ));
        }
        Ok(Self {
            producer_id,
            accepted_epoch,
            next_sequence,
            recent_committed_batches,
        })
    }

    pub fn prepare_initial(
        authority: ProducerAuthority,
        base_sequence: ProducerSequence,
        record_count: u32,
        canonical_digest: [u8; 32],
    ) -> Result<AppendCandidateV1, ProducerStateError> {
        let batch =
            ProducerBatchIdentityV1::new(authority, base_sequence, record_count, canonical_digest)?;
        if batch.base_sequence != ProducerSequence::ZERO {
            return Err(ProducerStateError::OutOfOrderSequence);
        }
        Ok(AppendCandidateV1 { batch })
    }

    pub fn from_first_committed(batch: CommittedBatchV1) -> Result<Self, ProducerStateError> {
        if batch.base_sequence != ProducerSequence::ZERO {
            return Err(ProducerStateError::InvalidCommittedHistory(
                "first committed producer batch must begin at sequence zero",
            ));
        }
        let mut recent_committed_batches = VecDeque::new();
        recent_committed_batches.push_back(batch.recent());
        Ok(Self {
            producer_id: batch.authority.producer_id,
            accepted_epoch: batch.authority.producer_epoch,
            next_sequence: batch
                .base_sequence
                .checked_next_after(batch.record_count)
                .expect("validated committed batch has next sequence"),
            recent_committed_batches,
        })
    }

    pub const fn producer_id(&self) -> ProducerId {
        self.producer_id
    }

    pub const fn accepted_epoch(&self) -> ProducerEpoch {
        self.accepted_epoch
    }

    pub const fn next_sequence(&self) -> ProducerSequence {
        self.next_sequence
    }

    pub fn recent_committed_batches(&self) -> &VecDeque<RecentBatchV1> {
        &self.recent_committed_batches
    }

    pub fn admit(
        &self,
        authority: ProducerAuthority,
        base_sequence: ProducerSequence,
        record_count: u32,
        canonical_digest: [u8; 32],
    ) -> Result<ProducerAdmissionV1, ProducerStateError> {
        if authority.producer_id != self.producer_id {
            return Err(ProducerStateError::InvalidProducer);
        }
        let batch =
            ProducerBatchIdentityV1::new(authority, base_sequence, record_count, canonical_digest)?;
        if authority.producer_epoch < self.accepted_epoch {
            return Err(ProducerStateError::ProducerFenced);
        }
        if authority.producer_epoch > self.accepted_epoch {
            if batch.base_sequence != ProducerSequence::ZERO {
                return Err(ProducerStateError::OutOfOrderSequence);
            }
            return Ok(ProducerAdmissionV1::Append(AppendCandidateV1 { batch }));
        }

        if batch.base_sequence == self.next_sequence {
            return Ok(ProducerAdmissionV1::Append(AppendCandidateV1 { batch }));
        }
        if batch.base_sequence > self.next_sequence {
            return Err(ProducerStateError::OutOfOrderSequence);
        }

        let Some(recent) = self.recent_committed_batches.iter().find(|recent| {
            recent.base_sequence == batch.base_sequence
                && recent.last_sequence == batch.last_sequence
                && recent.record_count == batch.record_count
        }) else {
            return Err(ProducerStateError::OutOfOrderSequence);
        };
        if recent.canonical_digest != batch.canonical_digest {
            return Err(ProducerStateError::OutOfOrderSequence);
        }
        Ok(ProducerAdmissionV1::Duplicate(CommittedRetryV1 {
            base_stream_offset: recent.base_stream_offset,
            last_stream_offset: recent.last_stream_offset,
            raft_index: recent.raft_index,
        }))
    }

    pub fn apply_committed(
        &mut self,
        batch: CommittedBatchV1,
    ) -> Result<CommitApplyOutcomeV1, ProducerStateError> {
        if batch.authority.producer_id != self.producer_id {
            return Err(ProducerStateError::InvalidCommittedHistory(
                "committed producer id differs from state authority",
            ));
        }
        if batch.authority.producer_epoch < self.accepted_epoch {
            return Err(ProducerStateError::InvalidCommittedHistory(
                "committed producer epoch regressed",
            ));
        }
        if batch.authority.producer_epoch > self.accepted_epoch {
            if batch.base_sequence != ProducerSequence::ZERO {
                return Err(ProducerStateError::InvalidCommittedHistory(
                    "first committed batch of newer producer epoch must begin at sequence zero",
                ));
            }
            self.accepted_epoch = batch.authority.producer_epoch;
            self.next_sequence = batch
                .base_sequence
                .checked_next_after(batch.record_count)
                .expect("validated committed batch has next sequence");
            self.recent_committed_batches.clear();
            self.push_recent(batch.recent());
            return Ok(CommitApplyOutcomeV1::Applied);
        }

        if batch.base_sequence == self.next_sequence {
            self.next_sequence = batch
                .base_sequence
                .checked_next_after(batch.record_count)
                .expect("validated committed batch has next sequence");
            self.push_recent(batch.recent());
            return Ok(CommitApplyOutcomeV1::Applied);
        }

        if batch.base_sequence > self.next_sequence {
            return Err(ProducerStateError::InvalidCommittedHistory(
                "committed producer sequence contains a gap",
            ));
        }

        let Some(recent) = self.recent_committed_batches.iter().find(|recent| {
            recent.base_sequence == batch.base_sequence
                && recent.last_sequence == batch.last_sequence
                && recent.record_count == batch.record_count
        }) else {
            return Err(ProducerStateError::InvalidCommittedHistory(
                "committed producer range predates retained retry window or partially overlaps history",
            ));
        };
        if *recent != batch.recent() {
            return Err(ProducerStateError::InvalidCommittedHistory(
                "committed producer metadata conflicts with retained history",
            ));
        }
        Ok(CommitApplyOutcomeV1::AlreadyApplied)
    }

    fn push_recent(&mut self, batch: RecentBatchV1) {
        self.recent_committed_batches.push_back(batch);
        while self.recent_committed_batches.len() > MAX_RECENT_COMMITTED_BATCHES {
            self.recent_committed_batches.pop_front();
        }
    }
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use super::*;

    fn authority(epoch: u32) -> ProducerAuthority {
        ProducerAuthority::new(
            ProducerId::new(7).unwrap(),
            ProducerEpoch::new(epoch).unwrap(),
        )
    }

    fn digest(value: u8) -> [u8; 32] {
        [value; 32]
    }

    fn committed(
        epoch: u32,
        base_sequence: u32,
        record_count: u32,
        base_offset: u64,
        raft_index: u64,
        digest_value: u8,
    ) -> CommittedBatchV1 {
        CommittedBatchV1::new(
            authority(epoch),
            ProducerSequence::new(base_sequence),
            record_count,
            StreamOffset::new(base_offset),
            RaftIndex::new(raft_index).unwrap(),
            digest(digest_value),
        )
        .unwrap()
    }

    #[test]
    fn first_seen_and_equal_epoch_admission_are_side_effect_free_and_gap_fenced() {
        let initial = PartitionProducerStateV1::prepare_initial(
            authority(1),
            ProducerSequence::ZERO,
            2,
            digest(1),
        )
        .unwrap();
        assert_eq!(initial.batch.last_sequence, ProducerSequence::new(1));
        assert!(matches!(
            PartitionProducerStateV1::prepare_initial(
                authority(1),
                ProducerSequence::new(1),
                1,
                digest(1)
            ),
            Err(ProducerStateError::OutOfOrderSequence)
        ));

        let mut state =
            PartitionProducerStateV1::from_first_committed(committed(1, 0, 2, 10, 1, 1)).unwrap();
        let before = state.clone();
        assert!(matches!(
            state.admit(authority(1), ProducerSequence::new(2), 2, digest(2)),
            Ok(ProducerAdmissionV1::Append(_))
        ));
        assert_eq!(state, before);
        assert!(matches!(
            state.admit(authority(1), ProducerSequence::new(3), 1, digest(3)),
            Err(ProducerStateError::OutOfOrderSequence)
        ));
        assert_eq!(state, before);

        state.apply_committed(committed(1, 2, 2, 20, 2, 2)).unwrap();
        assert_eq!(state.next_sequence(), ProducerSequence::new(4));
    }

    #[test]
    fn exact_retry_returns_original_offsets_and_changed_or_partial_retry_is_rejected() {
        let mut state =
            PartitionProducerStateV1::from_first_committed(committed(1, 0, 2, 100, 1, 1)).unwrap();
        state
            .apply_committed(committed(1, 2, 2, 200, 2, 2))
            .unwrap();
        let before = state.clone();

        assert_eq!(
            state
                .admit(authority(1), ProducerSequence::new(0), 2, digest(1))
                .unwrap(),
            ProducerAdmissionV1::Duplicate(CommittedRetryV1 {
                base_stream_offset: StreamOffset::new(100),
                last_stream_offset: StreamOffset::new(101),
                raft_index: RaftIndex::new(1).unwrap(),
            })
        );
        assert_eq!(state, before);
        assert!(matches!(
            state.admit(authority(1), ProducerSequence::new(0), 2, digest(9)),
            Err(ProducerStateError::OutOfOrderSequence)
        ));
        assert!(matches!(
            state.admit(authority(1), ProducerSequence::new(1), 2, digest(1)),
            Err(ProducerStateError::OutOfOrderSequence)
        ));
        assert_eq!(state, before);
    }

    #[test]
    fn recent_window_keeps_exactly_five_newest_batches_and_old_retry_fails_explicitly() {
        let mut state =
            PartitionProducerStateV1::from_first_committed(committed(1, 0, 1, 10, 1, 1)).unwrap();
        for sequence in 1..=5_u32 {
            state
                .apply_committed(committed(
                    1,
                    sequence,
                    1,
                    10 + u64::from(sequence),
                    u64::from(sequence + 1),
                    u8::try_from(sequence + 1).unwrap(),
                ))
                .unwrap();
        }
        assert_eq!(state.recent_committed_batches().len(), 5);
        assert_eq!(
            state
                .recent_committed_batches()
                .front()
                .unwrap()
                .base_sequence,
            ProducerSequence::new(1)
        );
        assert!(matches!(
            state.admit(authority(1), ProducerSequence::ZERO, 1, digest(1)),
            Err(ProducerStateError::OutOfOrderSequence)
        ));
        assert!(matches!(
            state.admit(authority(1), ProducerSequence::new(1), 1, digest(2)),
            Ok(ProducerAdmissionV1::Duplicate(_))
        ));
    }

    #[test]
    fn higher_epoch_resets_only_on_commit_and_lower_epoch_is_fenced() {
        let mut state =
            PartitionProducerStateV1::from_first_committed(committed(1, 0, 2, 10, 1, 1)).unwrap();
        let before = state.clone();
        assert!(matches!(
            state.admit(authority(2), ProducerSequence::ZERO, 1, digest(2)),
            Ok(ProducerAdmissionV1::Append(_))
        ));
        assert_eq!(state, before);
        assert!(matches!(
            state.admit(authority(2), ProducerSequence::new(1), 1, digest(2)),
            Err(ProducerStateError::OutOfOrderSequence)
        ));

        state.apply_committed(committed(2, 0, 1, 20, 2, 2)).unwrap();
        assert_eq!(state.accepted_epoch(), ProducerEpoch::new(2).unwrap());
        assert_eq!(state.next_sequence(), ProducerSequence::new(1));
        assert_eq!(state.recent_committed_batches().len(), 1);
        assert!(matches!(
            state.admit(authority(1), ProducerSequence::new(2), 1, digest(3)),
            Err(ProducerStateError::ProducerFenced)
        ));
    }

    #[test]
    fn committed_replay_is_idempotent_and_conflicts_fail_closed() {
        let first = committed(1, 0, 2, 10, 1, 1);
        let mut state = PartitionProducerStateV1::from_first_committed(first).unwrap();
        let before = state.clone();
        assert_eq!(
            state.apply_committed(first).unwrap(),
            CommitApplyOutcomeV1::AlreadyApplied
        );
        assert_eq!(state, before);

        let mut conflicting = first;
        conflicting.canonical_digest = digest(9);
        assert!(matches!(
            state.apply_committed(conflicting),
            Err(ProducerStateError::InvalidCommittedHistory(_))
        ));
        assert_eq!(state, before);

        assert!(matches!(
            state.apply_committed(committed(1, 3, 1, 30, 3, 3)),
            Err(ProducerStateError::InvalidCommittedHistory(_))
        ));
        assert_eq!(state, before);
        assert!(matches!(
            state.apply_committed(committed(2, 1, 1, 40, 4, 4)),
            Err(ProducerStateError::InvalidCommittedHistory(_))
        ));
        assert_eq!(state, before);
    }

    #[test]
    fn sequence_and_offset_arithmetic_never_wraps() {
        assert!(matches!(
            ProducerBatchIdentityV1::new(
                authority(1),
                ProducerSequence::new(u32::MAX),
                1,
                digest(1)
            ),
            Err(ProducerStateError::InvalidBatch(_))
        ));
        assert!(matches!(
            CommittedBatchV1::new(
                authority(1),
                ProducerSequence::ZERO,
                2,
                StreamOffset::new(u64::MAX),
                RaftIndex::FIRST,
                digest(1)
            ),
            Err(ProducerStateError::InvalidBatch(_))
        ));
    }

    #[test]
    fn exhaustive_small_state_oracle_preserves_i15_i16_and_i17() {
        for accepted_epoch in 1..=3_u32 {
            for committed_batches in 1..=4_u32 {
                let mut state = PartitionProducerStateV1::from_first_committed(committed(
                    accepted_epoch,
                    0,
                    1,
                    100,
                    1,
                    1,
                ))
                .unwrap();
                for sequence in 1..committed_batches {
                    state
                        .apply_committed(committed(
                            accepted_epoch,
                            sequence,
                            1,
                            100 + u64::from(sequence),
                            u64::from(sequence + 1),
                            u8::try_from(sequence + 1).unwrap(),
                        ))
                        .unwrap();
                }
                let frozen = state.clone();
                let next = state.next_sequence().get();
                for presented_epoch in 1..=4_u32 {
                    for base_sequence in 0..=next.saturating_add(2) {
                        for record_count in 1..=2_u32 {
                            for digest_matches in [false, true] {
                                let digest_value = if digest_matches {
                                    u8::try_from(base_sequence + 1).unwrap_or(1)
                                } else {
                                    250
                                };
                                let result = state.admit(
                                    authority(presented_epoch),
                                    ProducerSequence::new(base_sequence),
                                    record_count,
                                    digest(digest_value),
                                );
                                let expected = if presented_epoch < accepted_epoch {
                                    "fenced"
                                } else if presented_epoch > accepted_epoch {
                                    if base_sequence == 0 { "append" } else { "ooo" }
                                } else if base_sequence == next {
                                    "append"
                                } else if base_sequence > next {
                                    "ooo"
                                } else {
                                    let last = base_sequence.checked_add(record_count - 1);
                                    let exact = last.and_then(|last| {
                                        state.recent_committed_batches().iter().find(|recent| {
                                            recent.base_sequence.get() == base_sequence
                                                && recent.last_sequence.get() == last
                                                && recent.record_count == record_count
                                        })
                                    });
                                    if exact.is_some_and(|recent| {
                                        recent.canonical_digest == digest(digest_value)
                                    }) {
                                        "duplicate"
                                    } else {
                                        "ooo"
                                    }
                                };
                                match (expected, result) {
                                    ("fenced", Err(ProducerStateError::ProducerFenced))
                                    | ("ooo", Err(ProducerStateError::OutOfOrderSequence))
                                    | ("append", Ok(ProducerAdmissionV1::Append(_)))
                                    | ("duplicate", Ok(ProducerAdmissionV1::Duplicate(_))) => {}
                                    (expected, actual) => panic!(
                                        "oracle mismatch epoch={presented_epoch} base={base_sequence} count={record_count} digest_matches={digest_matches}: expected {expected}, got {actual:?}"
                                    ),
                                }
                                assert_eq!(state, frozen, "admission mutated committed state");
                            }
                        }
                    }
                }
            }
        }
    }

    #[test]
    fn p7j_bounded_sequence_state_space_preserves_i15_i16_i17_across_window_edges() {
        for accepted_epoch in 1..=3_u32 {
            for committed_batches in 1..=8_u32 {
                let mut state = PartitionProducerStateV1::from_first_committed(committed(
                    accepted_epoch,
                    0,
                    1,
                    1_000,
                    1,
                    1,
                ))
                .unwrap();
                for sequence in 1..committed_batches {
                    state
                        .apply_committed(committed(
                            accepted_epoch,
                            sequence,
                            1,
                            1_000 + u64::from(sequence),
                            u64::from(sequence + 1),
                            u8::try_from(sequence + 1).unwrap(),
                        ))
                        .unwrap();
                }

                let frozen = state.clone();
                let next = state.next_sequence().get();
                for presented_epoch in 1..=4_u32 {
                    for base_sequence in 0..=next.saturating_add(2) {
                        for record_count in 1..=3_u32 {
                            let digest_value =
                                u8::try_from(base_sequence.saturating_add(1)).unwrap_or(1);
                            let result = state.admit(
                                authority(presented_epoch),
                                ProducerSequence::new(base_sequence),
                                record_count,
                                digest(digest_value),
                            );
                            if presented_epoch < accepted_epoch {
                                assert!(matches!(result, Err(ProducerStateError::ProducerFenced)));
                            } else if presented_epoch > accepted_epoch {
                                if base_sequence == 0 {
                                    assert!(matches!(result, Ok(ProducerAdmissionV1::Append(_))));
                                } else {
                                    assert!(matches!(
                                        result,
                                        Err(ProducerStateError::OutOfOrderSequence)
                                    ));
                                }
                            } else if base_sequence == next {
                                assert!(matches!(result, Ok(ProducerAdmissionV1::Append(_))));
                            } else if base_sequence > next {
                                assert!(matches!(
                                    result,
                                    Err(ProducerStateError::OutOfOrderSequence)
                                ));
                            }
                            assert_eq!(
                                state, frozen,
                                "P7-J admission mutated committed producer state"
                            );
                        }
                    }
                }
                assert!(state.recent_committed_batches().len() <= MAX_RECENT_COMMITTED_BATCHES);
            }
        }
    }

    #[test]
    fn p7j_commit_response_loss_failover_permutations_never_duplicate_committed_range() {
        for commit_before_failover in [false, true] {
            for response_lost in [false, true] {
                let batch = committed(1, 0, 1, 50, 1, 7);
                let mut leader_state = None;
                if commit_before_failover {
                    leader_state =
                        Some(PartitionProducerStateV1::from_first_committed(batch).unwrap());
                }

                let mut successor_state = leader_state.clone();
                let retry = match successor_state.as_mut() {
                    Some(state) => state
                        .admit(authority(1), ProducerSequence::ZERO, 1, digest(7))
                        .unwrap(),
                    None => PartitionProducerStateV1::prepare_initial(
                        authority(1),
                        ProducerSequence::ZERO,
                        1,
                        digest(7),
                    )
                    .map(ProducerAdmissionV1::Append)
                    .unwrap(),
                };

                match (commit_before_failover, retry) {
                    (true, ProducerAdmissionV1::Duplicate(original)) => {
                        assert_eq!(original.base_stream_offset, StreamOffset::new(50));
                        assert_eq!(original.last_stream_offset, StreamOffset::new(50));
                        assert_eq!(original.raft_index, RaftIndex::FIRST);
                    }
                    (false, ProducerAdmissionV1::Append(candidate)) => {
                        let committed_retry = CommittedBatchV1::new(
                            candidate.batch.authority,
                            candidate.batch.base_sequence,
                            candidate.batch.record_count,
                            StreamOffset::new(50),
                            RaftIndex::FIRST,
                            candidate.batch.canonical_digest,
                        )
                        .unwrap();
                        successor_state = Some(
                            PartitionProducerStateV1::from_first_committed(committed_retry)
                                .unwrap(),
                        );
                    }
                    (committed, other) => {
                        panic!(
                            "unexpected P7-J failover disposition committed={committed} response_lost={response_lost}: {other:?}"
                        )
                    }
                }

                let final_state = successor_state.unwrap();
                assert_eq!(final_state.next_sequence(), ProducerSequence::new(1));
                assert_eq!(final_state.recent_committed_batches().len(), 1);
                assert_eq!(
                    final_state.recent_committed_batches()[0].base_stream_offset,
                    StreamOffset::new(50)
                );
                let _ = response_lost;
            }
        }
    }

    #[test]
    fn p7j_higher_epoch_old_epoch_race_model_has_monotonic_committed_authority() {
        for commit_higher_first in [false, true] {
            let mut state =
                PartitionProducerStateV1::from_first_committed(committed(1, 0, 1, 10, 1, 1))
                    .unwrap();
            let epoch_two = authority(2);
            assert!(matches!(
                state.admit(epoch_two, ProducerSequence::ZERO, 1, digest(2)),
                Ok(ProducerAdmissionV1::Append(_))
            ));

            if commit_higher_first {
                state.apply_committed(committed(2, 0, 1, 11, 2, 2)).unwrap();
                assert!(matches!(
                    state.admit(authority(1), ProducerSequence::new(1), 1, digest(3)),
                    Err(ProducerStateError::ProducerFenced)
                ));
                assert_eq!(state.accepted_epoch(), ProducerEpoch::new(2).unwrap());
            } else {
                assert!(matches!(
                    state.admit(authority(1), ProducerSequence::new(1), 1, digest(3)),
                    Ok(ProducerAdmissionV1::Append(_))
                ));
                state.apply_committed(committed(1, 1, 1, 11, 2, 3)).unwrap();
                state.apply_committed(committed(2, 0, 1, 12, 3, 2)).unwrap();
                assert!(matches!(
                    state.admit(authority(1), ProducerSequence::new(2), 1, digest(4)),
                    Err(ProducerStateError::ProducerFenced)
                ));
                assert_eq!(state.accepted_epoch(), ProducerEpoch::new(2).unwrap());
            }
        }
    }

    #[test]
    fn p7j_recent_window_eviction_campaign_dedupes_only_retained_exact_ranges() {
        let mut state =
            PartitionProducerStateV1::from_first_committed(committed(1, 0, 1, 100, 1, 1)).unwrap();
        for sequence in 1..=11_u32 {
            state
                .apply_committed(committed(
                    1,
                    sequence,
                    1,
                    100 + u64::from(sequence),
                    u64::from(sequence + 1),
                    u8::try_from(sequence + 1).unwrap(),
                ))
                .unwrap();
        }
        assert_eq!(
            state.recent_committed_batches().len(),
            MAX_RECENT_COMMITTED_BATCHES
        );
        for sequence in 0..=11_u32 {
            let result = state.admit(
                authority(1),
                ProducerSequence::new(sequence),
                1,
                digest(u8::try_from(sequence + 1).unwrap()),
            );
            if sequence >= 7 {
                assert!(
                    matches!(result, Ok(ProducerAdmissionV1::Duplicate(_))),
                    "retained sequence {sequence} did not dedupe"
                );
            } else {
                assert!(
                    matches!(result, Err(ProducerStateError::OutOfOrderSequence)),
                    "evicted sequence {sequence} did not fail explicitly"
                );
            }
        }
    }

    #[test]
    fn p7j_sequence_arithmetic_boundary_campaign_never_wraps() {
        let near_max = [
            (u32::MAX - 3, 1_u32, true),
            (u32::MAX - 3, 2_u32, true),
            (u32::MAX - 3, 3_u32, true),
            (u32::MAX - 3, 4_u32, false),
            (u32::MAX - 2, 2_u32, true),
            (u32::MAX - 2, 3_u32, false),
            (u32::MAX - 1, 1_u32, true),
            (u32::MAX - 1, 2_u32, false),
            (u32::MAX, 1_u32, false),
        ];
        for (base, count, valid) in near_max {
            assert_eq!(
                ProducerBatchIdentityV1::new(
                    authority(1),
                    ProducerSequence::new(base),
                    count,
                    digest(1)
                )
                .is_ok(),
                valid,
                "unexpected sequence-boundary result base={base} count={count}"
            );
        }

        for count in 1..=4_u32 {
            let base_offset = u64::MAX - u64::from(count.saturating_sub(1));
            assert!(
                CommittedBatchV1::new(
                    authority(1),
                    ProducerSequence::ZERO,
                    count,
                    StreamOffset::new(base_offset),
                    RaftIndex::FIRST,
                    digest(1)
                )
                .is_ok(),
                "exact u64 offset boundary should be accepted for count={count}"
            );
            if base_offset < u64::MAX {
                assert!(
                    CommittedBatchV1::new(
                        authority(1),
                        ProducerSequence::ZERO,
                        count,
                        StreamOffset::new(base_offset + 1),
                        RaftIndex::FIRST,
                        digest(1)
                    )
                    .is_err(),
                    "offset overflow should be rejected for count={count}"
                );
            }
        }
    }

    #[derive(Debug, Clone, Copy)]
    struct P7jSeededRng(u64);

    type P7jCampaignSummary = BTreeMap<(u64, u32), (u32, u32, usize)>;

    impl P7jSeededRng {
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
    }

    fn run_p7j_seeded_multi_producer_campaign(seed: u64) -> (Vec<u8>, P7jCampaignSummary) {
        let mut rng = P7jSeededRng::new(seed);
        let mut states = BTreeMap::<(u64, u32), PartitionProducerStateV1>::new();
        let mut next_offsets = [0_u64; 3];
        let mut next_raft = [1_u64; 3];
        let mut trace = Vec::new();

        for step in 0..192_u32 {
            let producer_number = 1 + (rng.next_u64() % 4);
            let partition = u32::try_from(rng.next_u64() % 3).unwrap();
            let partition_index = usize::try_from(partition).unwrap();
            let key = (producer_number, partition);
            let producer_id = ProducerId::new(producer_number).unwrap();
            let digest_value = u8::try_from(1 + (rng.next_u64() % 250)).unwrap();

            if let std::collections::btree_map::Entry::Vacant(entry) = states.entry(key) {
                let batch = CommittedBatchV1::new(
                    ProducerAuthority::new(producer_id, ProducerEpoch::new(1).unwrap()),
                    ProducerSequence::ZERO,
                    1,
                    StreamOffset::new(next_offsets[partition_index]),
                    RaftIndex::new(next_raft[partition_index]).unwrap(),
                    digest(digest_value),
                )
                .unwrap();
                entry.insert(PartitionProducerStateV1::from_first_committed(batch).unwrap());
                next_offsets[partition_index] += 1;
                next_raft[partition_index] += 1;
                trace.extend_from_slice(
                    format!("C:{step}:{producer_number}:{partition}:1:0:{digest_value};")
                        .as_bytes(),
                );
                continue;
            }

            let action = rng.next_u64() % 4;
            let state = states.get_mut(&key).unwrap();
            match action {
                0 => {
                    let epoch = state.accepted_epoch();
                    let base_sequence = state.next_sequence();
                    let batch = CommittedBatchV1::new(
                        ProducerAuthority::new(producer_id, epoch),
                        base_sequence,
                        1,
                        StreamOffset::new(next_offsets[partition_index]),
                        RaftIndex::new(next_raft[partition_index]).unwrap(),
                        digest(digest_value),
                    )
                    .unwrap();
                    assert_eq!(
                        state.apply_committed(batch).unwrap(),
                        CommitApplyOutcomeV1::Applied
                    );
                    next_offsets[partition_index] += 1;
                    next_raft[partition_index] += 1;
                    trace.extend_from_slice(
                        format!(
                            "C:{step}:{producer_number}:{partition}:{}:{}:{digest_value};",
                            epoch.get(),
                            base_sequence.get()
                        )
                        .as_bytes(),
                    );
                }
                1 => {
                    let higher = state.accepted_epoch().checked_next().unwrap();
                    let batch = CommittedBatchV1::new(
                        ProducerAuthority::new(producer_id, higher),
                        ProducerSequence::ZERO,
                        1,
                        StreamOffset::new(next_offsets[partition_index]),
                        RaftIndex::new(next_raft[partition_index]).unwrap(),
                        digest(digest_value),
                    )
                    .unwrap();
                    assert_eq!(
                        state.apply_committed(batch).unwrap(),
                        CommitApplyOutcomeV1::Applied
                    );
                    next_offsets[partition_index] += 1;
                    next_raft[partition_index] += 1;
                    trace.extend_from_slice(
                        format!(
                            "E:{step}:{producer_number}:{partition}:{}:{digest_value};",
                            higher.get()
                        )
                        .as_bytes(),
                    );
                }
                2 => {
                    let recent_index = usize::try_from(rng.next_u64()).unwrap()
                        % state.recent_committed_batches().len();
                    let recent = state.recent_committed_batches()[recent_index];
                    let authority = ProducerAuthority::new(producer_id, state.accepted_epoch());
                    let retry = state
                        .admit(
                            authority,
                            recent.base_sequence,
                            recent.record_count,
                            recent.canonical_digest,
                        )
                        .unwrap();
                    assert!(matches!(retry, ProducerAdmissionV1::Duplicate(_)));
                    trace.extend_from_slice(
                        format!(
                            "D:{step}:{producer_number}:{partition}:{};",
                            recent.base_sequence.get()
                        )
                        .as_bytes(),
                    );
                }
                _ => {
                    if state.accepted_epoch().get() > 1 {
                        let stale = ProducerAuthority::new(
                            producer_id,
                            ProducerEpoch::new(state.accepted_epoch().get() - 1).unwrap(),
                        );
                        assert!(matches!(
                            state.admit(stale, state.next_sequence(), 1, digest(digest_value)),
                            Err(ProducerStateError::ProducerFenced)
                        ));
                        trace.extend_from_slice(
                            format!("F:{step}:{producer_number}:{partition};").as_bytes(),
                        );
                    } else {
                        let gap = ProducerSequence::new(state.next_sequence().get() + 1);
                        assert!(matches!(
                            state.admit(
                                ProducerAuthority::new(producer_id, state.accepted_epoch()),
                                gap,
                                1,
                                digest(digest_value)
                            ),
                            Err(ProducerStateError::OutOfOrderSequence)
                        ));
                        trace.extend_from_slice(
                            format!("G:{step}:{producer_number}:{partition};").as_bytes(),
                        );
                    }
                }
            }
        }

        let summary = states
            .into_iter()
            .map(|(key, state)| {
                (
                    key,
                    (
                        state.accepted_epoch().get(),
                        state.next_sequence().get(),
                        state.recent_committed_batches().len(),
                    ),
                )
            })
            .collect();
        (trace, summary)
    }

    #[test]
    fn p7j_seeded_multi_producer_multi_partition_campaign_is_byte_repeatable() {
        let seed = 0x5eed_a57a_10c0_0007;
        let left = run_p7j_seeded_multi_producer_campaign(seed);
        let right = run_p7j_seeded_multi_producer_campaign(seed);
        assert_eq!(
            left, right,
            "identical P7-J seed must reproduce exact trace/state"
        );

        let different = run_p7j_seeded_multi_producer_campaign(seed ^ 0x9e37_79b9);
        assert_ne!(
            left.0, different.0,
            "different P7-J seed unexpectedly produced the same byte trace"
        );
        assert!(!left.1.is_empty());
        assert!(
            left.1
                .values()
                .all(|(_, _, recent)| *recent <= MAX_RECENT_COMMITTED_BATCHES)
        );
    }
}
