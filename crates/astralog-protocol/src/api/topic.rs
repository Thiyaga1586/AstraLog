use astralog_common::TopicId;

use crate::{
    ProtocolError, Reader, Result, TaggedField, Writer,
    api::{
        BodyCodec, MAX_RECORD_VALUE_LEN, MAX_RECORDS_PER_PARTITION, MAX_TOPIC_NAME_LEN,
        finish_body, put_top_level_tags,
    },
};

pub const MAX_TOPIC_BATCH_UNCOMPRESSED_BYTES_V1: u32 = 16 * 1024 * 1024 - 96;
pub const DEFAULT_TOMBSTONE_GRACE_MS_V1: u64 = 86_400_000;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum TopicCompressionV1 {
    None = 0,
    Lz4 = 1,
    Zstd = 2,
}

impl TryFrom<u8> for TopicCompressionV1 {
    type Error = ProtocolError;

    fn try_from(value: u8) -> Result<Self> {
        match value {
            0 => Ok(Self::None),
            1 => Ok(Self::Lz4),
            2 => Ok(Self::Zstd),
            _ => Err(ProtocolError::InvalidFrame(
                "unknown P8 topic compression policy",
            )),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct TopicPerformanceConfigV1 {
    pub compression: TopicCompressionV1,
    pub max_record_bytes: u32,
    pub max_batch_records: u32,
    pub max_batch_uncompressed_bytes: u32,
}

impl TopicPerformanceConfigV1 {
    pub fn validate(self) -> Result<()> {
        if self.max_record_bytes == 0 || self.max_record_bytes > MAX_RECORD_VALUE_LEN as u32 {
            return Err(ProtocolError::InvalidFrame(
                "topic max_record_bytes exceeds V1 bound",
            ));
        }
        if self.max_batch_records == 0 || self.max_batch_records > MAX_RECORDS_PER_PARTITION as u32
        {
            return Err(ProtocolError::InvalidFrame(
                "topic max_batch_records exceeds V1 bound",
            ));
        }
        if self.max_batch_uncompressed_bytes == 0
            || self.max_batch_uncompressed_bytes > MAX_TOPIC_BATCH_UNCOMPRESSED_BYTES_V1
            || self.max_record_bytes > self.max_batch_uncompressed_bytes
        {
            return Err(ProtocolError::InvalidFrame(
                "topic max_batch_uncompressed_bytes exceeds V1 bound",
            ));
        }
        Ok(())
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum TopicCleanupPolicyV1 {
    Delete = 0,
    Compact = 1,
    CompactDelete = 2,
}

impl TryFrom<u8> for TopicCleanupPolicyV1 {
    type Error = ProtocolError;

    fn try_from(value: u8) -> Result<Self> {
        match value {
            0 => Ok(Self::Delete),
            1 => Ok(Self::Compact),
            2 => Ok(Self::CompactDelete),
            _ => Err(ProtocolError::InvalidFrame("unknown P9 cleanup policy")),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct TopicRetentionConfigV1 {
    pub cleanup_policy: TopicCleanupPolicyV1,
    pub retention_ms: Option<u64>,
    pub retention_bytes: Option<u64>,
    pub tombstone_grace_ms: u64,
}

impl TopicRetentionConfigV1 {
    pub const fn historical_default() -> Self {
        Self {
            cleanup_policy: TopicCleanupPolicyV1::Delete,
            retention_ms: None,
            retention_bytes: None,
            tombstone_grace_ms: DEFAULT_TOMBSTONE_GRACE_MS_V1,
        }
    }

    pub fn validate(self) -> Result<()> {
        if self.tombstone_grace_ms == 0 {
            return Err(ProtocolError::InvalidFrame(
                "P9 tombstone grace must be positive",
            ));
        }
        if self.cleanup_policy == TopicCleanupPolicyV1::Compact
            && (self.retention_ms.is_some() || self.retention_bytes.is_some())
        {
            return Err(ProtocolError::InvalidFrame(
                "P9 compact-only policy cannot carry delete-retention limits",
            ));
        }
        Ok(())
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct TopicRetentionPatchV1 {
    pub cleanup_policy: Option<TopicCleanupPolicyV1>,
    pub retention_ms: Option<Option<u64>>,
    pub retention_bytes: Option<Option<u64>>,
    pub tombstone_grace_ms: Option<u64>,
}

impl TopicRetentionPatchV1 {
    pub const fn is_empty(self) -> bool {
        self.cleanup_policy.is_none()
            && self.retention_ms.is_none()
            && self.retention_bytes.is_none()
            && self.tombstone_grace_ms.is_none()
    }

    pub fn apply_to(self, current: TopicRetentionConfigV1) -> Result<TopicRetentionConfigV1> {
        if self.is_empty() {
            return Err(ProtocolError::InvalidFrame(
                "AlterTopicConfig retention patch is empty",
            ));
        }
        let next = TopicRetentionConfigV1 {
            cleanup_policy: self.cleanup_policy.unwrap_or(current.cleanup_policy),
            retention_ms: self.retention_ms.unwrap_or(current.retention_ms),
            retention_bytes: self.retention_bytes.unwrap_or(current.retention_bytes),
            tombstone_grace_ms: self
                .tombstone_grace_ms
                .unwrap_or(current.tombstone_grace_ms),
        };
        next.validate()?;
        Ok(next)
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CreateTopicRequestV0 {
    pub topic_name: String,
    pub partition_count: u32,
    pub replication_factor: u16,
    pub tags: Vec<TaggedField>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CreateTopicResponseV0 {
    pub topic_id: TopicId,
    pub metadata_epoch: u64,
    pub tags: Vec<TaggedField>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CreateTopicRequestV1 {
    pub topic_name: String,
    pub partition_count: u32,
    pub replication_factor: u16,
    pub performance: TopicPerformanceConfigV1,
    pub tags: Vec<TaggedField>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CreateTopicResponseV1 {
    pub topic_id: TopicId,
    pub metadata_epoch: u64,
    pub performance: TopicPerformanceConfigV1,
    pub tags: Vec<TaggedField>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CreateTopicRequestV2 {
    pub topic_name: String,
    pub partition_count: u32,
    pub replication_factor: u16,
    pub performance: TopicPerformanceConfigV1,
    pub retention: TopicRetentionConfigV1,
    pub tags: Vec<TaggedField>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CreateTopicResponseV2 {
    pub topic_id: TopicId,
    pub metadata_epoch: u64,
    pub performance: TopicPerformanceConfigV1,
    pub retention: TopicRetentionConfigV1,
    pub tags: Vec<TaggedField>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DeleteTopicRequestV0 {
    pub topic_name: String,
    pub tags: Vec<TaggedField>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DeleteTopicResponseV0 {
    pub deleted_topic_id: TopicId,
    pub metadata_epoch: u64,
    pub tags: Vec<TaggedField>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DescribeTopicRequestV0 {
    pub topic_name: String,
    pub tags: Vec<TaggedField>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DescribeTopicResponseV0 {
    pub topic_name: String,
    pub topic_id: TopicId,
    pub partition_count: u32,
    pub replication_factor: u16,
    pub metadata_epoch: u64,
    pub tags: Vec<TaggedField>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DescribeTopicRequestV1 {
    pub topic_name: String,
    pub tags: Vec<TaggedField>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DescribeTopicResponseV1 {
    pub topic_name: String,
    pub topic_id: TopicId,
    pub partition_count: u32,
    pub replication_factor: u16,
    pub metadata_epoch: u64,
    pub performance: TopicPerformanceConfigV1,
    pub tags: Vec<TaggedField>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DescribeTopicRequestV2 {
    pub topic_name: String,
    pub tags: Vec<TaggedField>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DescribeTopicResponseV2 {
    pub topic_name: String,
    pub topic_id: TopicId,
    pub partition_count: u32,
    pub replication_factor: u16,
    pub metadata_epoch: u64,
    pub performance: TopicPerformanceConfigV1,
    pub retention: TopicRetentionConfigV1,
    pub tags: Vec<TaggedField>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AlterTopicConfigRequestV0 {
    pub topic_name: String,
    pub retention: TopicRetentionPatchV1,
    pub tags: Vec<TaggedField>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AlterTopicConfigResponseV0 {
    pub topic_id: TopicId,
    pub metadata_epoch: u64,
    pub retention: TopicRetentionConfigV1,
    pub tags: Vec<TaggedField>,
}

pub fn validate_topic_name(name: &str) -> Result<()> {
    let bytes = name.as_bytes();
    if bytes.is_empty() || bytes.len() > MAX_TOPIC_NAME_LEN {
        return Err(ProtocolError::InvalidFrame("invalid topic name length"));
    }
    if name == "." || name == ".." {
        return Err(ProtocolError::InvalidFrame("reserved topic name"));
    }
    if name.starts_with("__astralog_") {
        return Err(ProtocolError::InvalidFrame(
            "internal topic prefix is reserved",
        ));
    }
    if !bytes
        .iter()
        .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'.' | b'_' | b'-'))
    {
        return Err(ProtocolError::InvalidFrame("invalid topic name character"));
    }
    Ok(())
}

impl BodyCodec for CreateTopicRequestV0 {
    fn encode_body(&self) -> Result<Vec<u8>> {
        validate_topic_name(&self.topic_name)?;
        let mut writer = Writer::new();
        writer.put_string16(&self.topic_name, MAX_TOPIC_NAME_LEN, "topic_name")?;
        writer.put_u32(self.partition_count);
        writer.put_u16(self.replication_factor);
        put_top_level_tags(&mut writer, &self.tags)?;
        Ok(writer.into_inner())
    }

    fn decode_body(bytes: &[u8]) -> Result<Self> {
        let mut reader = Reader::new(bytes);
        let topic_name = reader.read_string16(MAX_TOPIC_NAME_LEN, "topic_name")?;
        validate_topic_name(&topic_name)?;
        let partition_count = reader.read_u32()?;
        let replication_factor = reader.read_u16()?;
        let tags = finish_body(&mut reader)?;
        Ok(Self {
            topic_name,
            partition_count,
            replication_factor,
            tags,
        })
    }
}

impl BodyCodec for CreateTopicRequestV1 {
    fn encode_body(&self) -> Result<Vec<u8>> {
        validate_topic_name(&self.topic_name)?;
        self.performance.validate()?;
        let mut writer = Writer::new();
        writer.put_string16(&self.topic_name, MAX_TOPIC_NAME_LEN, "topic_name")?;
        writer.put_u32(self.partition_count);
        writer.put_u16(self.replication_factor);
        put_performance(&mut writer, self.performance);
        put_top_level_tags(&mut writer, &self.tags)?;
        Ok(writer.into_inner())
    }

    fn decode_body(bytes: &[u8]) -> Result<Self> {
        let mut reader = Reader::new(bytes);
        let topic_name = reader.read_string16(MAX_TOPIC_NAME_LEN, "topic_name")?;
        validate_topic_name(&topic_name)?;
        let partition_count = reader.read_u32()?;
        let replication_factor = reader.read_u16()?;
        let performance = read_performance(&mut reader)?;
        let tags = finish_body(&mut reader)?;
        Ok(Self {
            topic_name,
            partition_count,
            replication_factor,
            performance,
            tags,
        })
    }
}

impl BodyCodec for CreateTopicRequestV2 {
    fn encode_body(&self) -> Result<Vec<u8>> {
        validate_topic_name(&self.topic_name)?;
        self.performance.validate()?;
        self.retention.validate()?;
        let mut writer = Writer::new();
        writer.put_string16(&self.topic_name, MAX_TOPIC_NAME_LEN, "topic_name")?;
        writer.put_u32(self.partition_count);
        writer.put_u16(self.replication_factor);
        put_performance(&mut writer, self.performance);
        put_retention(&mut writer, self.retention);
        put_top_level_tags(&mut writer, &self.tags)?;
        Ok(writer.into_inner())
    }

    fn decode_body(bytes: &[u8]) -> Result<Self> {
        let mut reader = Reader::new(bytes);
        let topic_name = reader.read_string16(MAX_TOPIC_NAME_LEN, "topic_name")?;
        validate_topic_name(&topic_name)?;
        let partition_count = reader.read_u32()?;
        let replication_factor = reader.read_u16()?;
        let performance = read_performance(&mut reader)?;
        let retention = read_retention(&mut reader)?;
        let tags = finish_body(&mut reader)?;
        Ok(Self {
            topic_name,
            partition_count,
            replication_factor,
            performance,
            retention,
            tags,
        })
    }
}

macro_rules! impl_named_request {
    ($ty:ty) => {
        impl BodyCodec for $ty {
            fn encode_body(&self) -> Result<Vec<u8>> {
                validate_topic_name(&self.topic_name)?;
                let mut writer = Writer::new();
                writer.put_string16(&self.topic_name, MAX_TOPIC_NAME_LEN, "topic_name")?;
                put_top_level_tags(&mut writer, &self.tags)?;
                Ok(writer.into_inner())
            }

            fn decode_body(bytes: &[u8]) -> Result<Self> {
                let mut reader = Reader::new(bytes);
                let topic_name = reader.read_string16(MAX_TOPIC_NAME_LEN, "topic_name")?;
                validate_topic_name(&topic_name)?;
                let tags = finish_body(&mut reader)?;
                Ok(Self { topic_name, tags })
            }
        }
    };
}

impl_named_request!(DeleteTopicRequestV0);
impl_named_request!(DescribeTopicRequestV0);
impl_named_request!(DescribeTopicRequestV1);
impl_named_request!(DescribeTopicRequestV2);

impl BodyCodec for CreateTopicResponseV0 {
    fn encode_body(&self) -> Result<Vec<u8>> {
        let mut writer = Writer::new();
        writer.put_topic_id(self.topic_id);
        writer.put_u64(self.metadata_epoch);
        put_top_level_tags(&mut writer, &self.tags)?;
        Ok(writer.into_inner())
    }

    fn decode_body(bytes: &[u8]) -> Result<Self> {
        let mut reader = Reader::new(bytes);
        let topic_id = reader.read_topic_id()?;
        let metadata_epoch = reader.read_u64()?;
        let tags = finish_body(&mut reader)?;
        Ok(Self {
            topic_id,
            metadata_epoch,
            tags,
        })
    }
}

impl BodyCodec for CreateTopicResponseV1 {
    fn encode_body(&self) -> Result<Vec<u8>> {
        self.performance.validate()?;
        let mut writer = Writer::new();
        writer.put_topic_id(self.topic_id);
        writer.put_u64(self.metadata_epoch);
        put_performance(&mut writer, self.performance);
        put_top_level_tags(&mut writer, &self.tags)?;
        Ok(writer.into_inner())
    }

    fn decode_body(bytes: &[u8]) -> Result<Self> {
        let mut reader = Reader::new(bytes);
        let topic_id = reader.read_topic_id()?;
        let metadata_epoch = reader.read_u64()?;
        let performance = read_performance(&mut reader)?;
        let tags = finish_body(&mut reader)?;
        Ok(Self {
            topic_id,
            metadata_epoch,
            performance,
            tags,
        })
    }
}

impl BodyCodec for CreateTopicResponseV2 {
    fn encode_body(&self) -> Result<Vec<u8>> {
        self.performance.validate()?;
        self.retention.validate()?;
        let mut writer = Writer::new();
        writer.put_topic_id(self.topic_id);
        writer.put_u64(self.metadata_epoch);
        put_performance(&mut writer, self.performance);
        put_retention(&mut writer, self.retention);
        put_top_level_tags(&mut writer, &self.tags)?;
        Ok(writer.into_inner())
    }

    fn decode_body(bytes: &[u8]) -> Result<Self> {
        let mut reader = Reader::new(bytes);
        let topic_id = reader.read_topic_id()?;
        let metadata_epoch = reader.read_u64()?;
        let performance = read_performance(&mut reader)?;
        let retention = read_retention(&mut reader)?;
        let tags = finish_body(&mut reader)?;
        Ok(Self {
            topic_id,
            metadata_epoch,
            performance,
            retention,
            tags,
        })
    }
}

impl BodyCodec for DeleteTopicResponseV0 {
    fn encode_body(&self) -> Result<Vec<u8>> {
        let mut writer = Writer::new();
        writer.put_topic_id(self.deleted_topic_id);
        writer.put_u64(self.metadata_epoch);
        put_top_level_tags(&mut writer, &self.tags)?;
        Ok(writer.into_inner())
    }

    fn decode_body(bytes: &[u8]) -> Result<Self> {
        let mut reader = Reader::new(bytes);
        let deleted_topic_id = reader.read_topic_id()?;
        let metadata_epoch = reader.read_u64()?;
        let tags = finish_body(&mut reader)?;
        Ok(Self {
            deleted_topic_id,
            metadata_epoch,
            tags,
        })
    }
}

impl BodyCodec for DescribeTopicResponseV0 {
    fn encode_body(&self) -> Result<Vec<u8>> {
        validate_topic_name(&self.topic_name)?;
        let mut writer = Writer::new();
        writer.put_string16(&self.topic_name, MAX_TOPIC_NAME_LEN, "topic_name")?;
        writer.put_topic_id(self.topic_id);
        writer.put_u32(self.partition_count);
        writer.put_u16(self.replication_factor);
        writer.put_u64(self.metadata_epoch);
        put_top_level_tags(&mut writer, &self.tags)?;
        Ok(writer.into_inner())
    }

    fn decode_body(bytes: &[u8]) -> Result<Self> {
        let mut reader = Reader::new(bytes);
        let topic_name = reader.read_string16(MAX_TOPIC_NAME_LEN, "topic_name")?;
        validate_topic_name(&topic_name)?;
        let topic_id = reader.read_topic_id()?;
        let partition_count = reader.read_u32()?;
        let replication_factor = reader.read_u16()?;
        let metadata_epoch = reader.read_u64()?;
        let tags = finish_body(&mut reader)?;
        Ok(Self {
            topic_name,
            topic_id,
            partition_count,
            replication_factor,
            metadata_epoch,
            tags,
        })
    }
}

impl BodyCodec for DescribeTopicResponseV1 {
    fn encode_body(&self) -> Result<Vec<u8>> {
        validate_topic_name(&self.topic_name)?;
        self.performance.validate()?;
        let mut writer = Writer::new();
        writer.put_string16(&self.topic_name, MAX_TOPIC_NAME_LEN, "topic_name")?;
        writer.put_topic_id(self.topic_id);
        writer.put_u32(self.partition_count);
        writer.put_u16(self.replication_factor);
        writer.put_u64(self.metadata_epoch);
        put_performance(&mut writer, self.performance);
        put_top_level_tags(&mut writer, &self.tags)?;
        Ok(writer.into_inner())
    }

    fn decode_body(bytes: &[u8]) -> Result<Self> {
        let mut reader = Reader::new(bytes);
        let topic_name = reader.read_string16(MAX_TOPIC_NAME_LEN, "topic_name")?;
        validate_topic_name(&topic_name)?;
        let topic_id = reader.read_topic_id()?;
        let partition_count = reader.read_u32()?;
        let replication_factor = reader.read_u16()?;
        let metadata_epoch = reader.read_u64()?;
        let performance = read_performance(&mut reader)?;
        let tags = finish_body(&mut reader)?;
        Ok(Self {
            topic_name,
            topic_id,
            partition_count,
            replication_factor,
            metadata_epoch,
            performance,
            tags,
        })
    }
}

impl BodyCodec for DescribeTopicResponseV2 {
    fn encode_body(&self) -> Result<Vec<u8>> {
        validate_topic_name(&self.topic_name)?;
        self.performance.validate()?;
        self.retention.validate()?;
        let mut writer = Writer::new();
        writer.put_string16(&self.topic_name, MAX_TOPIC_NAME_LEN, "topic_name")?;
        writer.put_topic_id(self.topic_id);
        writer.put_u32(self.partition_count);
        writer.put_u16(self.replication_factor);
        writer.put_u64(self.metadata_epoch);
        put_performance(&mut writer, self.performance);
        put_retention(&mut writer, self.retention);
        put_top_level_tags(&mut writer, &self.tags)?;
        Ok(writer.into_inner())
    }

    fn decode_body(bytes: &[u8]) -> Result<Self> {
        let mut reader = Reader::new(bytes);
        let topic_name = reader.read_string16(MAX_TOPIC_NAME_LEN, "topic_name")?;
        validate_topic_name(&topic_name)?;
        let topic_id = reader.read_topic_id()?;
        let partition_count = reader.read_u32()?;
        let replication_factor = reader.read_u16()?;
        let metadata_epoch = reader.read_u64()?;
        let performance = read_performance(&mut reader)?;
        let retention = read_retention(&mut reader)?;
        let tags = finish_body(&mut reader)?;
        Ok(Self {
            topic_name,
            topic_id,
            partition_count,
            replication_factor,
            metadata_epoch,
            performance,
            retention,
            tags,
        })
    }
}

impl BodyCodec for AlterTopicConfigRequestV0 {
    fn encode_body(&self) -> Result<Vec<u8>> {
        validate_topic_name(&self.topic_name)?;
        if self.retention.is_empty() {
            return Err(ProtocolError::InvalidFrame(
                "AlterTopicConfig retention patch is empty",
            ));
        }
        let mut writer = Writer::new();
        writer.put_string16(&self.topic_name, MAX_TOPIC_NAME_LEN, "topic_name")?;
        put_retention_patch(&mut writer, self.retention)?;
        put_top_level_tags(&mut writer, &self.tags)?;
        Ok(writer.into_inner())
    }

    fn decode_body(bytes: &[u8]) -> Result<Self> {
        let mut reader = Reader::new(bytes);
        let topic_name = reader.read_string16(MAX_TOPIC_NAME_LEN, "topic_name")?;
        validate_topic_name(&topic_name)?;
        let retention = read_retention_patch(&mut reader)?;
        if retention.is_empty() {
            return Err(ProtocolError::InvalidFrame(
                "AlterTopicConfig retention patch is empty",
            ));
        }
        let tags = finish_body(&mut reader)?;
        Ok(Self {
            topic_name,
            retention,
            tags,
        })
    }
}

impl BodyCodec for AlterTopicConfigResponseV0 {
    fn encode_body(&self) -> Result<Vec<u8>> {
        self.retention.validate()?;
        let mut writer = Writer::new();
        writer.put_topic_id(self.topic_id);
        writer.put_u64(self.metadata_epoch);
        put_retention(&mut writer, self.retention);
        put_top_level_tags(&mut writer, &self.tags)?;
        Ok(writer.into_inner())
    }

    fn decode_body(bytes: &[u8]) -> Result<Self> {
        let mut reader = Reader::new(bytes);
        let topic_id = reader.read_topic_id()?;
        let metadata_epoch = reader.read_u64()?;
        let retention = read_retention(&mut reader)?;
        let tags = finish_body(&mut reader)?;
        Ok(Self {
            topic_id,
            metadata_epoch,
            retention,
            tags,
        })
    }
}

fn put_performance(writer: &mut Writer, performance: TopicPerformanceConfigV1) {
    writer.put_u8(performance.compression as u8);
    writer.put_u8(0);
    writer.put_u16(0);
    writer.put_u32(performance.max_record_bytes);
    writer.put_u32(performance.max_batch_records);
    writer.put_u32(performance.max_batch_uncompressed_bytes);
}

fn read_performance(reader: &mut Reader<'_>) -> Result<TopicPerformanceConfigV1> {
    let compression = TopicCompressionV1::try_from(reader.read_u8()?)?;
    if reader.read_u8()? != 0 || reader.read_u16()? != 0 {
        return Err(ProtocolError::InvalidFrame(
            "topic performance reserved field is non-zero",
        ));
    }
    let performance = TopicPerformanceConfigV1 {
        compression,
        max_record_bytes: reader.read_u32()?,
        max_batch_records: reader.read_u32()?,
        max_batch_uncompressed_bytes: reader.read_u32()?,
    };
    performance.validate()?;
    Ok(performance)
}

fn put_retention(writer: &mut Writer, retention: TopicRetentionConfigV1) {
    writer.put_u8(retention.cleanup_policy as u8);
    let mut flags = 0_u8;
    if retention.retention_ms.is_some() {
        flags |= 1;
    }
    if retention.retention_bytes.is_some() {
        flags |= 2;
    }
    writer.put_u8(flags);
    writer.put_u16(0);
    writer.put_u64(retention.retention_ms.unwrap_or(0));
    writer.put_u64(retention.retention_bytes.unwrap_or(0));
    writer.put_u64(retention.tombstone_grace_ms);
}

fn read_retention(reader: &mut Reader<'_>) -> Result<TopicRetentionConfigV1> {
    let cleanup_policy = TopicCleanupPolicyV1::try_from(reader.read_u8()?)?;
    let flags = reader.read_u8()?;
    if flags & !0b11 != 0 || reader.read_u16()? != 0 {
        return Err(ProtocolError::InvalidFrame(
            "topic retention flags/reserved field is invalid",
        ));
    }
    let retention_ms_raw = reader.read_u64()?;
    let retention_bytes_raw = reader.read_u64()?;
    let tombstone_grace_ms = reader.read_u64()?;
    if flags & 1 == 0 && retention_ms_raw != 0 {
        return Err(ProtocolError::InvalidFrame(
            "absent retention_ms must encode zero",
        ));
    }
    if flags & 2 == 0 && retention_bytes_raw != 0 {
        return Err(ProtocolError::InvalidFrame(
            "absent retention_bytes must encode zero",
        ));
    }
    let retention = TopicRetentionConfigV1 {
        cleanup_policy,
        retention_ms: (flags & 1 != 0).then_some(retention_ms_raw),
        retention_bytes: (flags & 2 != 0).then_some(retention_bytes_raw),
        tombstone_grace_ms,
    };
    retention.validate()?;
    Ok(retention)
}

fn put_retention_patch(writer: &mut Writer, patch: TopicRetentionPatchV1) -> Result<()> {
    if patch.is_empty() {
        return Err(ProtocolError::InvalidFrame(
            "AlterTopicConfig retention patch is empty",
        ));
    }
    let mut present = 0_u16;
    let mut clear = 0_u16;
    if patch.cleanup_policy.is_some() {
        present |= 1;
    }
    if let Some(value) = patch.retention_ms {
        present |= 2;
        if value.is_none() {
            clear |= 2;
        }
    }
    if let Some(value) = patch.retention_bytes {
        present |= 4;
        if value.is_none() {
            clear |= 4;
        }
    }
    if patch.tombstone_grace_ms.is_some() {
        present |= 8;
    }
    if patch.tombstone_grace_ms == Some(0) {
        return Err(ProtocolError::InvalidFrame(
            "P9 tombstone grace must be positive",
        ));
    }
    writer.put_u16(present);
    writer.put_u16(clear);
    writer.put_u8(patch.cleanup_policy.unwrap_or(TopicCleanupPolicyV1::Delete) as u8);
    writer.put_u8(0);
    writer.put_u16(0);
    writer.put_u64(patch.retention_ms.flatten().unwrap_or(0));
    writer.put_u64(patch.retention_bytes.flatten().unwrap_or(0));
    writer.put_u64(patch.tombstone_grace_ms.unwrap_or(0));
    Ok(())
}

fn read_retention_patch(reader: &mut Reader<'_>) -> Result<TopicRetentionPatchV1> {
    let present = reader.read_u16()?;
    let clear = reader.read_u16()?;
    if present & !0b1111 != 0 || clear & !0b0110 != 0 || clear & !present != 0 {
        return Err(ProtocolError::InvalidFrame(
            "AlterTopicConfig retention patch flags are invalid",
        ));
    }
    let cleanup_raw = reader.read_u8()?;
    if reader.read_u8()? != 0 || reader.read_u16()? != 0 {
        return Err(ProtocolError::InvalidFrame(
            "AlterTopicConfig retention reserved field is non-zero",
        ));
    }
    let retention_ms_raw = reader.read_u64()?;
    let retention_bytes_raw = reader.read_u64()?;
    let grace_raw = reader.read_u64()?;
    if present & 1 == 0 && cleanup_raw != 0 {
        return Err(ProtocolError::InvalidFrame(
            "absent cleanup policy must encode zero",
        ));
    }
    if (present & 2 == 0 || clear & 2 != 0) && retention_ms_raw != 0 {
        return Err(ProtocolError::InvalidFrame(
            "absent/cleared retention_ms must encode zero",
        ));
    }
    if (present & 4 == 0 || clear & 4 != 0) && retention_bytes_raw != 0 {
        return Err(ProtocolError::InvalidFrame(
            "absent/cleared retention_bytes must encode zero",
        ));
    }
    if present & 8 == 0 && grace_raw != 0 {
        return Err(ProtocolError::InvalidFrame(
            "absent tombstone grace must encode zero",
        ));
    }
    if present & 8 != 0 && grace_raw == 0 {
        return Err(ProtocolError::InvalidFrame(
            "P9 tombstone grace must be positive",
        ));
    }
    Ok(TopicRetentionPatchV1 {
        cleanup_policy: (present & 1 != 0)
            .then(|| TopicCleanupPolicyV1::try_from(cleanup_raw))
            .transpose()?,
        retention_ms: (present & 2 != 0).then_some(if clear & 2 != 0 {
            None
        } else {
            Some(retention_ms_raw)
        }),
        retention_bytes: (present & 4 != 0).then_some(if clear & 4 != 0 {
            None
        } else {
            Some(retention_bytes_raw)
        }),
        tombstone_grace_ms: (present & 8 != 0).then_some(grace_raw),
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn create_topic_round_trip_and_name_validation() {
        let request = CreateTopicRequestV0 {
            topic_name: "orders.v1".into(),
            partition_count: 3,
            replication_factor: 1,
            tags: vec![],
        };
        assert_eq!(
            CreateTopicRequestV0::decode_body(&request.encode_body().unwrap()).unwrap(),
            request
        );
        assert!(validate_topic_name("__astralog_internal").is_err());
        assert!(validate_topic_name("bad/name").is_err());
    }

    #[test]
    fn p8_topic_v1_round_trip_and_bounds_are_canonical() {
        let performance = TopicPerformanceConfigV1 {
            compression: TopicCompressionV1::Zstd,
            max_record_bytes: 1024 * 1024,
            max_batch_records: 256,
            max_batch_uncompressed_bytes: 4 * 1024 * 1024,
        };
        let create = CreateTopicRequestV1 {
            topic_name: "compressed.orders".into(),
            partition_count: 3,
            replication_factor: 3,
            performance,
            tags: vec![],
        };
        assert_eq!(
            CreateTopicRequestV1::decode_body(&create.encode_body().unwrap()).unwrap(),
            create
        );
        let describe = DescribeTopicResponseV1 {
            topic_name: "compressed.orders".into(),
            topic_id: TopicId::new([9; 16]),
            partition_count: 3,
            replication_factor: 3,
            metadata_epoch: 44,
            performance,
            tags: vec![],
        };
        assert_eq!(
            DescribeTopicResponseV1::decode_body(&describe.encode_body().unwrap()).unwrap(),
            describe
        );
        let invalid = TopicPerformanceConfigV1 {
            max_record_bytes: MAX_RECORD_VALUE_LEN as u32 + 1,
            ..performance
        };
        assert!(invalid.validate().is_err());
    }

    #[test]
    fn p9_topic_v2_and_alter_retention_round_trip_canonically() {
        let performance = TopicPerformanceConfigV1 {
            compression: TopicCompressionV1::Lz4,
            max_record_bytes: 1024,
            max_batch_records: 32,
            max_batch_uncompressed_bytes: 64 * 1024,
        };
        let retention = TopicRetentionConfigV1 {
            cleanup_policy: TopicCleanupPolicyV1::CompactDelete,
            retention_ms: Some(60_000),
            retention_bytes: Some(1_000_000),
            tombstone_grace_ms: 30_000,
        };
        let create = CreateTopicRequestV2 {
            topic_name: "retained.orders".into(),
            partition_count: 3,
            replication_factor: 3,
            performance,
            retention,
            tags: vec![],
        };
        assert_eq!(
            CreateTopicRequestV2::decode_body(&create.encode_body().unwrap()).unwrap(),
            create
        );
        let describe = DescribeTopicResponseV2 {
            topic_name: create.topic_name.clone(),
            topic_id: TopicId::new([7; 16]),
            partition_count: 3,
            replication_factor: 3,
            metadata_epoch: 55,
            performance,
            retention,
            tags: vec![],
        };
        assert_eq!(
            DescribeTopicResponseV2::decode_body(&describe.encode_body().unwrap()).unwrap(),
            describe
        );
        let alter = AlterTopicConfigRequestV0 {
            topic_name: create.topic_name,
            retention: TopicRetentionPatchV1 {
                cleanup_policy: Some(TopicCleanupPolicyV1::Delete),
                retention_ms: Some(None),
                retention_bytes: Some(Some(2048)),
                tombstone_grace_ms: Some(45_000),
            },
            tags: vec![],
        };
        assert_eq!(
            AlterTopicConfigRequestV0::decode_body(&alter.encode_body().unwrap()).unwrap(),
            alter
        );
    }

    #[test]
    fn p9_retention_codecs_reject_empty_noncanonical_and_invalid_policy_shapes() {
        let empty = AlterTopicConfigRequestV0 {
            topic_name: "orders".into(),
            retention: TopicRetentionPatchV1 {
                cleanup_policy: None,
                retention_ms: None,
                retention_bytes: None,
                tombstone_grace_ms: None,
            },
            tags: vec![],
        };
        assert!(empty.encode_body().is_err());

        let invalid_compact = TopicRetentionConfigV1 {
            cleanup_policy: TopicCleanupPolicyV1::Compact,
            retention_ms: Some(1),
            retention_bytes: None,
            tombstone_grace_ms: 1,
        };
        assert!(invalid_compact.validate().is_err());

        let valid = AlterTopicConfigRequestV0 {
            topic_name: "orders".into(),
            retention: TopicRetentionPatchV1 {
                cleanup_policy: None,
                retention_ms: Some(None),
                retention_bytes: None,
                tombstone_grace_ms: None,
            },
            tags: vec![],
        };
        let mut bytes = valid.encode_body().unwrap();
        let flags = 2 + "orders".len();
        bytes[flags..flags + 2].copy_from_slice(&0_u16.to_be_bytes());
        assert!(AlterTopicConfigRequestV0::decode_body(&bytes).is_err());

        let mut trailing = valid.encode_body().unwrap();
        trailing.push(0);
        assert!(AlterTopicConfigRequestV0::decode_body(&trailing).is_err());
    }
}
