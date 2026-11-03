use core::fmt;

use crate::{RaftError, Result};

#[derive(Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct NodeId(u64);

impl NodeId {
    pub const fn new(value: u64) -> Result<Self> {
        if value == 0 {
            Err(RaftError::ZeroNodeId)
        } else {
            Ok(Self(value))
        }
    }

    pub const fn get(self) -> u64 {
        self.0
    }
}

impl fmt::Debug for NodeId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "NodeId({})", self.0)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord, Default)]
pub struct Term(u64);

impl Term {
    pub const ZERO: Self = Self(0);

    pub const fn new(value: u64) -> Self {
        Self(value)
    }

    pub const fn get(self) -> u64 {
        self.0
    }

    pub fn checked_next(self) -> Result<Self> {
        self.0
            .checked_add(1)
            .map(Self)
            .ok_or(RaftError::TermOverflow)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord, Default)]
pub struct LogIndex(u64);

impl LogIndex {
    pub const ZERO: Self = Self(0);
    pub const FIRST: Self = Self(1);

    pub const fn new(value: u64) -> Self {
        Self(value)
    }

    pub const fn get(self) -> u64 {
        self.0
    }

    pub fn checked_next(self) -> Result<Self> {
        self.0
            .checked_add(1)
            .map(Self)
            .ok_or(RaftError::LogIndexOverflow)
    }

    pub fn checked_add(self, amount: u64) -> Result<Self> {
        self.0
            .checked_add(amount)
            .map(Self)
            .ok_or(RaftError::LogIndexOverflow)
    }
}

macro_rules! opaque_u64 {
    ($name:ident) => {
        #[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
        pub struct $name(u64);

        impl $name {
            pub const fn new(value: u64) -> Self {
                Self(value)
            }

            pub const fn get(self) -> u64 {
                self.0
            }
        }
    };
}

opaque_u64!(PersistenceToken);
opaque_u64!(ProposalId);

#[derive(Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct SnapshotId([u8; 16]);

impl SnapshotId {
    pub const ZERO: Self = Self([0; 16]);

    pub const fn new(bytes: [u8; 16]) -> Self {
        Self(bytes)
    }

    pub const fn as_bytes(&self) -> &[u8; 16] {
        &self.0
    }
}

impl fmt::Debug for SnapshotId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        for byte in self.0 {
            write!(f, "{byte:02x}")?;
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn node_zero_is_reserved() {
        assert_eq!(NodeId::new(0), Err(RaftError::ZeroNodeId));
        assert_eq!(NodeId::new(7).unwrap().get(), 7);
    }

    #[test]
    fn term_and_index_arithmetic_never_wraps() {
        assert_eq!(
            Term::new(u64::MAX).checked_next(),
            Err(RaftError::TermOverflow)
        );
        assert_eq!(
            LogIndex::new(u64::MAX).checked_next(),
            Err(RaftError::LogIndexOverflow)
        );
        assert_eq!(
            LogIndex::new(u64::MAX - 1).checked_add(2),
            Err(RaftError::LogIndexOverflow)
        );
    }
}
