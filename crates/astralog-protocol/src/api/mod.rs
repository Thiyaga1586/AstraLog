mod api_versions;
mod cluster;
mod fetch;
mod groups;
mod init_producer;
mod list_offsets;
mod metadata;
mod produce;
mod topic;

pub use api_versions::{
    ApiVersionRange, ApiVersionsRequestV0, ApiVersionsResponseV0, FeatureRange,
};
pub use cluster::{
    DescribeClusterRequestV0, DescribeClusterResponseV0, ReassignPartitionResultV0,
    ReassignPartitionV0, ReassignPartitionsRequestV0, ReassignPartitionsResponseV0,
};
pub use fetch::{
    FetchPartitionRequestV0, FetchPartitionResponseV0, FetchRecordV0, FetchRequestV0,
    FetchResponseV0, IsolationLevel,
};
pub use groups::{
    CommitOffsetV0, CommitOffsetsRequestV0, CommitOffsetsResponseV0, CoordinatorHintV0,
    FetchOffsetResultV0, FetchOffsetValueV0, FetchOffsetsRequestV0, FetchOffsetsResponseV0,
    GroupEpoch, GroupId, GroupStateVersion, HeartbeatRequestV0, HeartbeatResponseV0,
    JoinSubscribeRequestV0, JoinSubscribeResponseV0, LeaveRequestV0, LeaveResponseV0,
    MAX_GROUP_ID_LEN, MAX_GROUP_MEMBERS, MAX_GROUP_PARTITIONS, MAX_GROUP_RECORD_BYTES,
    MAX_INSTANCE_ID_LEN, MAX_MEMBER_ASSIGNMENT_PARTITIONS, MAX_OFFSET_METADATA_LEN,
    MAX_OFFSETS_PER_REQUEST, MAX_SESSION_TIMEOUT_MS, MAX_SUBSCRIPTION_TOPICS,
    MIN_SESSION_TIMEOUT_MS, MemberAuthorityV0, MemberEpoch, MemberId, RevokeAckRequestV0,
    RevokeAckResponseV0, TopicPartitionV0,
};
pub use init_producer::{InitProducerRequestV0, InitProducerResponseV0};
pub use list_offsets::{
    ListOffsetsQueryV0, ListOffsetsRequestV0, ListOffsetsResponseV0, ListOffsetsResultV0,
};
pub use metadata::{
    BrokerMetadataV0, MetadataPartitionV0, MetadataRequestV0, MetadataResponseV0, MetadataTopicV0,
};
pub use produce::{
    AckMode, ProducePartitionRequestV0, ProducePartitionRequestV1, ProducePartitionResponseV0,
    ProducePartitionResponseV1, ProduceRequestV0, ProduceRequestV1, ProduceResponseV0,
    ProduceResponseV1, WireHeaderV0, WireRecordV0, canonical_record_set_digest,
};
pub use topic::{
    AlterTopicConfigRequestV0, AlterTopicConfigResponseV0, CreateTopicRequestV0,
    CreateTopicRequestV1, CreateTopicRequestV2, CreateTopicResponseV0, CreateTopicResponseV1,
    CreateTopicResponseV2, DEFAULT_TOMBSTONE_GRACE_MS_V1, DeleteTopicRequestV0,
    DeleteTopicResponseV0, DescribeTopicRequestV0, DescribeTopicRequestV1, DescribeTopicRequestV2,
    DescribeTopicResponseV0, DescribeTopicResponseV1, DescribeTopicResponseV2,
    MAX_TOPIC_BATCH_UNCOMPRESSED_BYTES_V1, TopicCleanupPolicyV1, TopicCompressionV1,
    TopicPerformanceConfigV1, TopicRetentionConfigV1, TopicRetentionPatchV1, validate_topic_name,
};

use crate::{ProtocolError, Reader, Result, TaggedField, Writer};

pub const MAX_TOPIC_NAME_LEN: usize = 249;
pub const MAX_CLIENT_SOFTWARE_NAME_LEN: usize = 128;
pub const MAX_CLIENT_SOFTWARE_VERSION_LEN: usize = 128;
pub const MAX_SERVER_SOFTWARE_NAME_LEN: usize = 128;
pub const MAX_SERVER_SOFTWARE_VERSION_LEN: usize = 128;
pub const MAX_HOST_LEN: usize = 255;
pub const MAX_TOPICS_PER_REQUEST: usize = 1_024;
pub const MAX_PARTITIONS_PER_REQUEST: usize = 1_024;
pub const MAX_RECORDS_PER_PARTITION: usize = 10_000;
pub const MAX_RECORD_HEADERS: usize = 64;
pub const MAX_RECORD_HEADER_KEY_LEN: usize = 255;
pub const MAX_RECORD_KEY_LEN: usize = 1024 * 1024;
pub const MAX_RECORD_VALUE_LEN: usize = 8 * 1024 * 1024;

pub trait BodyCodec: Sized {
    fn encode_body(&self) -> Result<Vec<u8>>;
    fn decode_body(bytes: &[u8]) -> Result<Self>;
}

pub(crate) fn put_top_level_tags(writer: &mut Writer, tags: &[TaggedField]) -> Result<()> {
    writer.put_tagged_fields(tags)
}

pub(crate) fn finish_body(reader: &mut Reader<'_>) -> Result<Vec<TaggedField>> {
    let tags = reader.read_tagged_fields()?;
    if !reader.is_empty() {
        return Err(ProtocolError::InvalidFrame(
            "trailing bytes after top-level tagged fields",
        ));
    }
    Ok(tags)
}

pub(crate) fn read_bounded_count(
    reader: &mut Reader<'_>,
    max: usize,
    minimum_element_width: usize,
    field: &'static str,
) -> Result<usize> {
    let count = usize::try_from(reader.read_u32()?).map_err(|_| ProtocolError::LengthOverflow)?;
    if count > max {
        return Err(ProtocolError::FieldTooLarge {
            field,
            len: count,
            max,
        });
    }
    if minimum_element_width > 0 && count > reader.remaining() / minimum_element_width {
        return Err(ProtocolError::InvalidFrameOwned(format!(
            "{field} count cannot fit remaining body bytes"
        )));
    }
    Ok(count)
}
