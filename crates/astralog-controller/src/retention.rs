use astralog_common::TopicId;

use crate::{ControllerError, Result};

pub const P9_RETENTION_FEATURE_LEVEL: u16 = 5;
pub const DEFAULT_TOMBSTONE_GRACE_MS: u64 = 86_400_000;

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
#[repr(u8)]
pub enum CleanupPolicyV1 {
    Delete = 0,
    Compact = 1,
    CompactDelete = 2,
}

impl TryFrom<u8> for CleanupPolicyV1 {
    type Error = ControllerError;

    fn try_from(value: u8) -> Result<Self> {
        match value {
            0 => Ok(Self::Delete),
            1 => Ok(Self::Compact),
            2 => Ok(Self::CompactDelete),
            _ => Err(ControllerError::Invalid("unknown P9 cleanup policy")),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
#[repr(u8)]
pub enum SubscriptionProtectionV1 {
    Disabled = 0,
    ReservedForProtectedFloor = 1,
}

impl TryFrom<u8> for SubscriptionProtectionV1 {
    type Error = ControllerError;

    fn try_from(value: u8) -> Result<Self> {
        match value {
            0 => Ok(Self::Disabled),
            1 => Ok(Self::ReservedForProtectedFloor),
            _ => Err(ControllerError::Invalid(
                "unknown P9 subscription protection policy",
            )),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct TopicRetentionPolicyV1 {
    pub topic_id: TopicId,
    pub cleanup_policy: CleanupPolicyV1,
    pub retention_ms: Option<u64>,
    pub retention_bytes: Option<u64>,
    pub tombstone_grace_ms: u64,
    pub subscription_protection: SubscriptionProtectionV1,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct TopicRetentionSpecV1 {
    pub cleanup_policy: CleanupPolicyV1,
    pub retention_ms: Option<u64>,
    pub retention_bytes: Option<u64>,
    pub tombstone_grace_ms: u64,
    pub subscription_protection: SubscriptionProtectionV1,
}

impl TopicRetentionSpecV1 {
    pub const fn historical_default() -> Self {
        Self {
            cleanup_policy: CleanupPolicyV1::Delete,
            retention_ms: None,
            retention_bytes: None,
            tombstone_grace_ms: DEFAULT_TOMBSTONE_GRACE_MS,
            subscription_protection: SubscriptionProtectionV1::Disabled,
        }
    }

    pub fn into_policy(self, topic_id: TopicId) -> Result<TopicRetentionPolicyV1> {
        TopicRetentionPolicyV1::new(
            topic_id,
            self.cleanup_policy,
            self.retention_ms,
            self.retention_bytes,
            self.tombstone_grace_ms,
            self.subscription_protection,
        )
    }

    pub fn validate(self) -> Result<()> {
        self.into_policy(TopicId::new([1; 16])).map(|_| ())
    }
}

impl TopicRetentionPolicyV1 {
    pub fn new(
        topic_id: TopicId,
        cleanup_policy: CleanupPolicyV1,
        retention_ms: Option<u64>,
        retention_bytes: Option<u64>,
        tombstone_grace_ms: u64,
        subscription_protection: SubscriptionProtectionV1,
    ) -> Result<Self> {
        let policy = Self {
            topic_id,
            cleanup_policy,
            retention_ms,
            retention_bytes,
            tombstone_grace_ms,
            subscription_protection,
        };
        policy.validate()?;
        Ok(policy)
    }

    pub const fn default_for(topic_id: TopicId) -> Self {
        Self {
            topic_id,
            cleanup_policy: CleanupPolicyV1::Delete,
            retention_ms: None,
            retention_bytes: None,
            tombstone_grace_ms: DEFAULT_TOMBSTONE_GRACE_MS,
            subscription_protection: SubscriptionProtectionV1::Disabled,
        }
    }

    pub fn validate(self) -> Result<()> {
        if *self.topic_id.as_bytes() == [0; 16] {
            return Err(ControllerError::Invalid("topic id zero is reserved"));
        }
        if self.tombstone_grace_ms == 0 {
            return Err(ControllerError::Invalid(
                "P9 tombstone grace must be positive",
            ));
        }
        if self.cleanup_policy == CleanupPolicyV1::Compact
            && (self.retention_ms.is_some() || self.retention_bytes.is_some())
        {
            return Err(ControllerError::Invalid(
                "P9 compact-only policy cannot carry delete-retention limits",
            ));
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn topic(value: u8) -> TopicId {
        TopicId::new([value; 16])
    }

    #[test]
    fn p9_cleanup_policy_is_closed_and_defaults_are_infinite_delete_retention() {
        assert_eq!(
            CleanupPolicyV1::try_from(0).unwrap(),
            CleanupPolicyV1::Delete
        );
        assert_eq!(
            CleanupPolicyV1::try_from(1).unwrap(),
            CleanupPolicyV1::Compact
        );
        assert_eq!(
            CleanupPolicyV1::try_from(2).unwrap(),
            CleanupPolicyV1::CompactDelete
        );
        assert!(CleanupPolicyV1::try_from(3).is_err());

        let default = TopicRetentionPolicyV1::default_for(topic(1));
        default.validate().unwrap();
        assert_eq!(default.cleanup_policy, CleanupPolicyV1::Delete);
        assert_eq!(default.retention_ms, None);
        assert_eq!(default.retention_bytes, None);
        assert_eq!(default.tombstone_grace_ms, 86_400_000);
        assert_eq!(
            default.subscription_protection,
            SubscriptionProtectionV1::Disabled
        );
    }

    #[test]
    fn p9_retention_policy_validation_matches_frozen_cleanup_matrix() {
        TopicRetentionPolicyV1::new(
            topic(2),
            CleanupPolicyV1::Delete,
            Some(0),
            Some(0),
            1,
            SubscriptionProtectionV1::Disabled,
        )
        .unwrap();
        TopicRetentionPolicyV1::new(
            topic(3),
            CleanupPolicyV1::CompactDelete,
            Some(1000),
            None,
            10,
            SubscriptionProtectionV1::Disabled,
        )
        .unwrap();
        assert!(
            TopicRetentionPolicyV1::new(
                topic(4),
                CleanupPolicyV1::Compact,
                Some(1),
                None,
                10,
                SubscriptionProtectionV1::Disabled,
            )
            .is_err()
        );
        assert!(
            TopicRetentionPolicyV1::new(
                topic(5),
                CleanupPolicyV1::Compact,
                None,
                None,
                0,
                SubscriptionProtectionV1::Disabled,
            )
            .is_err()
        );
        assert!(
            SubscriptionProtectionV1::try_from(2).is_err(),
            "subscription protection is a closed internal enum"
        );
    }
}
