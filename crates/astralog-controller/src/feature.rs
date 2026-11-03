use crate::{ControllerError, Result};

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct FeatureRange {
    min: u16,
    max: u16,
}

impl FeatureRange {
    pub fn new(min: u16, max: u16) -> Result<Self> {
        if min == 0 || max == 0 || min > max {
            return Err(ControllerError::Invalid(
                "feature range must satisfy 0 < min <= max",
            ));
        }
        Ok(Self { min, max })
    }

    pub const fn min(self) -> u16 {
        self.min
    }

    pub const fn max(self) -> u16 {
        self.max
    }

    pub const fn supports(self, level: u16) -> bool {
        level >= self.min && level <= self.max
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct FeatureGate {
    required_level: u16,
}

impl FeatureGate {
    pub fn new(required_level: u16) -> Result<Self> {
        if required_level == 0 {
            return Err(ControllerError::Invalid(
                "feature gate level zero is reserved",
            ));
        }
        Ok(Self { required_level })
    }

    pub const fn required_level(self) -> u16 {
        self.required_level
    }

    pub const fn enabled(self, active_feature_level: u16) -> bool {
        active_feature_level >= self.required_level
    }

    pub fn require(self, active_feature_level: u16) -> Result<()> {
        if !self.enabled(active_feature_level) {
            return Err(ControllerError::Invalid(
                "irreversible behavior is not enabled by active cluster feature level",
            ));
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn feature_ranges_and_gates_fail_closed() {
        assert!(FeatureRange::new(0, 1).is_err());
        assert!(FeatureRange::new(2, 1).is_err());
        let range = FeatureRange::new(1, 3).unwrap();
        assert!(range.supports(1));
        assert!(range.supports(3));
        assert!(!range.supports(4));

        assert!(FeatureGate::new(0).is_err());
        let gate = FeatureGate::new(2).unwrap();
        assert!(!gate.enabled(1));
        assert!(gate.require(1).is_err());
        assert!(gate.enabled(2));
        gate.require(2).unwrap();
    }
}
