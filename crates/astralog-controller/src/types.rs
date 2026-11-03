use crate::{ControllerError, Result};

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct ControllerNodeId(u64);

impl ControllerNodeId {
    pub fn new(value: u64) -> Result<Self> {
        if value == 0 {
            return Err(ControllerError::Invalid(
                "controller node id zero is reserved",
            ));
        }
        Ok(Self(value))
    }

    pub const fn get(self) -> u64 {
        self.0
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct AssignmentGeneration(u64);

impl AssignmentGeneration {
    pub fn new(value: u64) -> Result<Self> {
        if value == 0 {
            return Err(ControllerError::Invalid(
                "assignment generation zero is reserved",
            ));
        }
        Ok(Self(value))
    }

    pub const fn get(self) -> u64 {
        self.0
    }

    pub fn checked_next(self) -> Result<Self> {
        Self::new(
            self.0
                .checked_add(1)
                .ok_or(ControllerError::Invalid("assignment generation overflow"))?,
        )
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct MovementId([u8; 16]);

impl MovementId {
    pub fn new(bytes: [u8; 16]) -> Result<Self> {
        if bytes == [0; 16] {
            return Err(ControllerError::Invalid("movement id zero is reserved"));
        }
        Ok(Self(bytes))
    }

    pub const fn as_bytes(&self) -> &[u8; 16] {
        &self.0
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn checked_controller_types_reject_reserved_zero_and_overflow() {
        assert!(ControllerNodeId::new(0).is_err());
        assert!(AssignmentGeneration::new(0).is_err());
        assert!(MovementId::new([0; 16]).is_err());
        assert!(
            AssignmentGeneration::new(u64::MAX)
                .unwrap()
                .checked_next()
                .is_err()
        );
    }
}
