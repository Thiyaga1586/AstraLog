#![forbid(unsafe_code)]

use core::fmt;

#[derive(Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct TopicId([u8; 16]);

impl TopicId {
    pub const fn new(bytes: [u8; 16]) -> Self {
        Self(bytes)
    }

    pub const fn as_bytes(&self) -> &[u8; 16] {
        &self.0
    }
}

impl fmt::Debug for TopicId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        for (i, byte) in self.0.iter().enumerate() {
            if matches!(i, 4 | 6 | 8 | 10) {
                f.write_str("-")?;
            }
            write!(f, "{byte:02x}")?;
        }
        Ok(())
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct PartitionId(u32);

impl PartitionId {
    pub const fn new(value: u32) -> Self {
        Self(value)
    }

    pub const fn get(self) -> u32 {
        self.0
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct StreamOffset(u64);

impl StreamOffset {
    pub const fn new(value: u64) -> Self {
        Self(value)
    }

    pub const fn get(self) -> u64 {
        self.0
    }

    pub fn checked_add(self, amount: u64) -> Option<Self> {
        self.0.checked_add(amount).map(Self)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct SegmentBaseOffset(StreamOffset);

impl SegmentBaseOffset {
    pub const fn new(value: u64) -> Self {
        Self(StreamOffset::new(value))
    }

    pub const fn get(self) -> u64 {
        self.0.get()
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct RaftIndex(u64);

impl RaftIndex {
    pub const FIRST: Self = Self(1);

    pub const fn new(value: u64) -> Option<Self> {
        if value == 0 { None } else { Some(Self(value)) }
    }

    pub const fn get(self) -> u64 {
        self.0
    }

    pub fn checked_next(self) -> Option<Self> {
        self.0.checked_add(1).and_then(Self::new)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct RaftTerm(u64);

impl RaftTerm {
    pub const fn new(value: u64) -> Self {
        Self(value)
    }

    pub const fn get(self) -> u64 {
        self.0
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct ProducerId(u64);

impl ProducerId {
    pub const fn new(value: u64) -> Option<Self> {
        if value == 0 { None } else { Some(Self(value)) }
    }

    pub const fn get(self) -> u64 {
        self.0
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct ProducerEpoch(u32);

impl ProducerEpoch {
    pub const fn new(value: u32) -> Option<Self> {
        if value == 0 { None } else { Some(Self(value)) }
    }

    pub const fn get(self) -> u32 {
        self.0
    }

    pub fn checked_next(self) -> Option<Self> {
        self.0.checked_add(1).and_then(Self::new)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct ProducerSequence(u32);

impl ProducerSequence {
    pub const ZERO: Self = Self(0);

    pub const fn new(value: u32) -> Self {
        Self(value)
    }

    pub const fn get(self) -> u32 {
        self.0
    }

    pub fn checked_last(self, record_count: u32) -> Option<Self> {
        if record_count == 0 {
            return None;
        }
        self.0.checked_add(record_count - 1).map(Self)
    }

    pub fn checked_next_after(self, record_count: u32) -> Option<Self> {
        self.checked_last(record_count)?.0.checked_add(1).map(Self)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct ProducerAuthority {
    pub producer_id: ProducerId,
    pub producer_epoch: ProducerEpoch,
}

impl ProducerAuthority {
    pub const fn new(producer_id: ProducerId, producer_epoch: ProducerEpoch) -> Self {
        Self {
            producer_id,
            producer_epoch,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn raft_index_zero_is_reserved() {
        assert_eq!(RaftIndex::new(0), None);
        assert_eq!(RaftIndex::new(1), Some(RaftIndex::FIRST));
    }

    #[test]
    fn checked_offset_arithmetic_does_not_wrap() {
        assert_eq!(StreamOffset::new(u64::MAX).checked_add(1), None,);
    }

    #[test]
    fn producer_identity_and_sequence_arithmetic_are_checked() {
        assert_eq!(ProducerId::new(0), None);
        assert_eq!(ProducerEpoch::new(0), None);
        assert_eq!(ProducerEpoch::new(u32::MAX).unwrap().checked_next(), None);
        assert_eq!(ProducerSequence::new(7).checked_last(3).unwrap().get(), 9);
        assert_eq!(
            ProducerSequence::new(7)
                .checked_next_after(3)
                .unwrap()
                .get(),
            10
        );
        assert_eq!(ProducerSequence::new(u32::MAX).checked_next_after(1), None);
        assert_eq!(ProducerSequence::ZERO.checked_last(0), None);
    }
}
