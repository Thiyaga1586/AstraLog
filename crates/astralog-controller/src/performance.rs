use astralog_common::TopicId;
use astralog_protocol::api::{MAX_RECORD_VALUE_LEN, MAX_RECORDS_PER_PARTITION};

use crate::{ControllerError, Result};

pub const P8_PERFORMANCE_FEATURE_LEVEL: u16 = 4;
pub const MAX_TOPIC_RECORD_BYTES: u32 = MAX_RECORD_VALUE_LEN as u32;
pub const MAX_TOPIC_BATCH_RECORDS: u32 = MAX_RECORDS_PER_PARTITION as u32;
pub const DEFAULT_TOPIC_BATCH_UNCOMPRESSED_BYTES: u32 = 16 * 1024 * 1024 - 96;

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
#[repr(u8)]
pub enum CompressionPolicyV1 {
    None = 0,
    Lz4 = 1,
    Zstd = 2,
}

impl TryFrom<u8> for CompressionPolicyV1 {
    type Error = ControllerError;

    fn try_from(value: u8) -> Result<Self> {
        match value {
            0 => Ok(Self::None),
            1 => Ok(Self::Lz4),
            2 => Ok(Self::Zstd),
            _ => Err(ControllerError::Invalid("unknown P8 compression policy")),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct TopicPerformancePolicyV1 {
    pub topic_id: TopicId,
    pub compression: CompressionPolicyV1,
    pub max_record_bytes: u32,
    pub max_batch_records: u32,
    pub max_batch_uncompressed_bytes: u32,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct TopicPerformanceSpecV1 {
    pub compression: CompressionPolicyV1,
    pub max_record_bytes: u32,
    pub max_batch_records: u32,
    pub max_batch_uncompressed_bytes: u32,
}

impl TopicPerformanceSpecV1 {
    pub fn default_v1() -> Self {
        Self {
            compression: CompressionPolicyV1::None,
            max_record_bytes: MAX_TOPIC_RECORD_BYTES,
            max_batch_records: MAX_TOPIC_BATCH_RECORDS,
            max_batch_uncompressed_bytes: DEFAULT_TOPIC_BATCH_UNCOMPRESSED_BYTES,
        }
    }

    pub fn into_policy(self, topic_id: TopicId) -> Result<TopicPerformancePolicyV1> {
        TopicPerformancePolicyV1::new(
            topic_id,
            self.compression,
            self.max_record_bytes,
            self.max_batch_records,
            self.max_batch_uncompressed_bytes,
        )
    }
}

impl TopicPerformancePolicyV1 {
    pub fn new(
        topic_id: TopicId,
        compression: CompressionPolicyV1,
        max_record_bytes: u32,
        max_batch_records: u32,
        max_batch_uncompressed_bytes: u32,
    ) -> Result<Self> {
        let policy = Self {
            topic_id,
            compression,
            max_record_bytes,
            max_batch_records,
            max_batch_uncompressed_bytes,
        };
        policy.validate()?;
        Ok(policy)
    }

    pub fn default_for(topic_id: TopicId) -> Self {
        Self {
            topic_id,
            compression: CompressionPolicyV1::None,
            max_record_bytes: MAX_TOPIC_RECORD_BYTES,
            max_batch_records: MAX_TOPIC_BATCH_RECORDS,
            max_batch_uncompressed_bytes: DEFAULT_TOPIC_BATCH_UNCOMPRESSED_BYTES,
        }
    }

    pub fn validate(self) -> Result<()> {
        if *self.topic_id.as_bytes() == [0; 16] {
            return Err(ControllerError::Invalid("topic id zero is reserved"));
        }
        if self.max_record_bytes == 0 || self.max_record_bytes > MAX_TOPIC_RECORD_BYTES {
            return Err(ControllerError::Invalid(
                "topic max_record_bytes exceeds frozen P8/public bound",
            ));
        }
        if self.max_batch_records == 0 || self.max_batch_records > MAX_TOPIC_BATCH_RECORDS {
            return Err(ControllerError::Invalid(
                "topic max_batch_records exceeds frozen P8/protocol bound",
            ));
        }
        if self.max_batch_uncompressed_bytes == 0
            || self.max_batch_uncompressed_bytes > DEFAULT_TOPIC_BATCH_UNCOMPRESSED_BYTES
            || self.max_record_bytes > self.max_batch_uncompressed_bytes
        {
            return Err(ControllerError::Invalid(
                "topic max_batch_uncompressed_bytes exceeds frozen P8 decompression bound",
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
    fn compression_policy_is_closed_and_topic_policy_bounds_are_checked() {
        assert_eq!(
            CompressionPolicyV1::try_from(0).unwrap(),
            CompressionPolicyV1::None
        );
        assert_eq!(
            CompressionPolicyV1::try_from(1).unwrap(),
            CompressionPolicyV1::Lz4
        );
        assert_eq!(
            CompressionPolicyV1::try_from(2).unwrap(),
            CompressionPolicyV1::Zstd
        );
        assert!(CompressionPolicyV1::try_from(3).is_err());

        let default = TopicPerformancePolicyV1::default_for(topic(1));
        default.validate().unwrap();
        assert_eq!(default.max_record_bytes, 8 * 1024 * 1024);
        assert_eq!(default.max_batch_records, 10_000);

        assert!(
            TopicPerformancePolicyV1::new(topic(2), CompressionPolicyV1::Lz4, 0, 1, 1).is_err()
        );
        assert!(
            TopicPerformancePolicyV1::new(
                topic(2),
                CompressionPolicyV1::Lz4,
                MAX_TOPIC_RECORD_BYTES + 1,
                1,
                DEFAULT_TOPIC_BATCH_UNCOMPRESSED_BYTES
            )
            .is_err()
        );
        assert!(
            TopicPerformancePolicyV1::new(
                topic(2),
                CompressionPolicyV1::Lz4,
                1,
                MAX_TOPIC_BATCH_RECORDS + 1,
                DEFAULT_TOPIC_BATCH_UNCOMPRESSED_BYTES
            )
            .is_err()
        );
        assert!(
            TopicPerformancePolicyV1::new(
                topic(2),
                CompressionPolicyV1::Lz4,
                1,
                1,
                DEFAULT_TOPIC_BATCH_UNCOMPRESSED_BYTES + 1
            )
            .is_err()
        );
    }
}
