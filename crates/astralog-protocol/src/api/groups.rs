use std::collections::BTreeSet;

use astralog_common::TopicId;

use crate::{
    ProtocolError, Reader, Result, TaggedField, UNKNOWN_BROKER_ID, UNKNOWN_EPOCH, Writer,
    api::{BodyCodec, finish_body, put_top_level_tags, read_bounded_count},
};

pub const MAX_GROUP_ID_LEN: usize = 255;
pub const MAX_INSTANCE_ID_LEN: usize = 255;
pub const MAX_GROUP_MEMBERS: usize = 1_024;
pub const MAX_SUBSCRIPTION_TOPICS: usize = 256;
pub const MAX_GROUP_PARTITIONS: usize = 16_384;
pub const MAX_MEMBER_ASSIGNMENT_PARTITIONS: usize = 1_024;
pub const MAX_OFFSETS_PER_REQUEST: usize = 1_024;
pub const MAX_OFFSET_METADATA_LEN: usize = 4_096;
pub const MAX_GROUP_RECORD_BYTES: usize = 4 * 1024 * 1024;
pub const MIN_SESSION_TIMEOUT_MS: u32 = 6_000;
pub const MAX_SESSION_TIMEOUT_MS: u32 = 300_000;

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct GroupId(String);

impl GroupId {
    pub fn new(value: impl Into<String>) -> Result<Self> {
        let value = value.into();
        validate_text_id(&value, MAX_GROUP_ID_LEN, "group_id")?;
        Ok(Self(value))
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }

    pub fn as_bytes(&self) -> &[u8] {
        self.0.as_bytes()
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct MemberId([u8; 16]);

impl MemberId {
    pub fn new(bytes: [u8; 16]) -> Result<Self> {
        if bytes == [0; 16] {
            return Err(ProtocolError::InvalidFrame("member id zero is reserved"));
        }
        Ok(Self(bytes))
    }

    pub const fn as_bytes(self) -> [u8; 16] {
        self.0
    }
}

macro_rules! checked_epoch {
    ($name:ident, $zero_message:literal) => {
        #[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
        pub struct $name(u64);

        impl $name {
            pub fn new(value: u64) -> Result<Self> {
                if value == 0 {
                    return Err(ProtocolError::InvalidFrame($zero_message));
                }
                Ok(Self(value))
            }

            pub const fn get(self) -> u64 {
                self.0
            }

            pub fn checked_next(self) -> Result<Self> {
                self.0
                    .checked_add(1)
                    .ok_or(ProtocolError::LengthOverflow)
                    .and_then(Self::new)
            }
        }
    };
}

checked_epoch!(GroupEpoch, "group epoch zero is reserved");
checked_epoch!(MemberEpoch, "member epoch zero is reserved");
checked_epoch!(GroupStateVersion, "group state version zero is reserved");

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct TopicPartitionV0 {
    pub topic_id: TopicId,
    pub partition: u32,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MemberAuthorityV0 {
    pub group_id: GroupId,
    pub member_id: MemberId,
    pub group_epoch: GroupEpoch,
    pub member_epoch: MemberEpoch,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct CoordinatorHintV0 {
    pub coordinator_broker_id: u32,
    pub coordinator_epoch: u64,
    pub current_group_epoch: u64,
    pub current_member_epoch: u64,
}

impl CoordinatorHintV0 {
    pub const UNKNOWN: Self = Self {
        coordinator_broker_id: UNKNOWN_BROKER_ID,
        coordinator_epoch: UNKNOWN_EPOCH,
        current_group_epoch: 0,
        current_member_epoch: 0,
    };
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct JoinSubscribeRequestV0 {
    pub group_id: GroupId,
    pub member_id: [u8; 16],
    pub instance_id: Option<String>,
    pub expected_group_epoch: u64,
    pub expected_member_epoch: u64,
    pub session_timeout_ms: u32,
    pub subscriptions: Vec<TopicId>,
    pub tags: Vec<TaggedField>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct JoinSubscribeResponseV0 {
    pub hint: CoordinatorHintV0,
    pub member_id: [u8; 16],
    pub group_phase: u8,
    pub heartbeat_interval_ms: u32,
    pub active_assignments: Vec<TopicPartitionV0>,
    pub revocations: Vec<TopicPartitionV0>,
    pub tags: Vec<TaggedField>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct HeartbeatRequestV0 {
    pub authority: MemberAuthorityV0,
    pub tags: Vec<TaggedField>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct HeartbeatResponseV0 {
    pub hint: CoordinatorHintV0,
    pub group_phase: u8,
    pub heartbeat_interval_ms: u32,
    pub active_assignments: Vec<TopicPartitionV0>,
    pub revocations: Vec<TopicPartitionV0>,
    pub tags: Vec<TaggedField>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RevokeAckRequestV0 {
    pub authority: MemberAuthorityV0,
    pub revoked: Vec<TopicPartitionV0>,
    pub tags: Vec<TaggedField>,
}

pub type RevokeAckResponseV0 = HeartbeatResponseV0;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LeaveRequestV0 {
    pub authority: MemberAuthorityV0,
    pub tags: Vec<TaggedField>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LeaveResponseV0 {
    pub hint: CoordinatorHintV0,
    pub tags: Vec<TaggedField>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CommitOffsetV0 {
    pub topic_id: TopicId,
    pub partition: u32,
    pub next_offset: u64,
    pub metadata: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CommitOffsetsRequestV0 {
    pub authority: MemberAuthorityV0,
    pub offsets: Vec<CommitOffsetV0>,
    pub tags: Vec<TaggedField>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CommitOffsetsResponseV0 {
    pub hint: CoordinatorHintV0,
    pub offsets_commit_index: u64,
    pub tags: Vec<TaggedField>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FetchOffsetsRequestV0 {
    pub group_id: GroupId,
    pub partitions: Vec<TopicPartitionV0>,
    pub tags: Vec<TaggedField>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FetchOffsetValueV0 {
    pub next_offset: u64,
    pub commit_timestamp_ms: i64,
    pub group_epoch: GroupEpoch,
    pub member_id: MemberId,
    pub member_epoch: MemberEpoch,
    pub metadata: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FetchOffsetResultV0 {
    pub topic_id: TopicId,
    pub partition: u32,
    pub value: Option<FetchOffsetValueV0>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FetchOffsetsResponseV0 {
    pub hint: CoordinatorHintV0,
    pub results: Vec<FetchOffsetResultV0>,
    pub tags: Vec<TaggedField>,
}

impl BodyCodec for JoinSubscribeRequestV0 {
    fn encode_body(&self) -> Result<Vec<u8>> {
        validate_join(self)?;
        let mut writer = Writer::new();
        put_group_id(&mut writer, &self.group_id)?;
        writer.put_uuid(&self.member_id);
        writer.put_bool(self.instance_id.is_some());
        if let Some(instance_id) = &self.instance_id {
            validate_text_id(instance_id, MAX_INSTANCE_ID_LEN, "instance_id")?;
            writer.put_string16(instance_id, MAX_INSTANCE_ID_LEN, "instance_id")?;
        }
        writer.put_u64(self.expected_group_epoch);
        writer.put_u64(self.expected_member_epoch);
        writer.put_u32(self.session_timeout_ms);
        put_topic_ids(&mut writer, &self.subscriptions)?;
        put_top_level_tags(&mut writer, &self.tags)?;
        Ok(writer.into_inner())
    }

    fn decode_body(bytes: &[u8]) -> Result<Self> {
        let mut reader = Reader::new(bytes);
        let group_id = read_group_id(&mut reader)?;
        let member_id = reader.read_uuid()?;
        let instance_id = if reader.read_bool()? {
            Some(read_text_id(
                &mut reader,
                MAX_INSTANCE_ID_LEN,
                "instance_id",
            )?)
        } else {
            None
        };
        let expected_group_epoch = reader.read_u64()?;
        let expected_member_epoch = reader.read_u64()?;
        let session_timeout_ms = reader.read_u32()?;
        let subscription_count =
            read_bounded_count(&mut reader, MAX_SUBSCRIPTION_TOPICS, 16, "subscriptions")?;
        let mut subscriptions = Vec::with_capacity(subscription_count);
        for _ in 0..subscription_count {
            subscriptions.push(reader.read_topic_id()?);
        }
        validate_topic_ids(&subscriptions)?;
        let tags = finish_body(&mut reader)?;
        let value = Self {
            group_id,
            member_id,
            instance_id,
            expected_group_epoch,
            expected_member_epoch,
            session_timeout_ms,
            subscriptions,
            tags,
        };
        validate_join(&value)?;
        Ok(value)
    }
}

impl BodyCodec for JoinSubscribeResponseV0 {
    fn encode_body(&self) -> Result<Vec<u8>> {
        validate_member_state(
            self.group_phase,
            self.heartbeat_interval_ms,
            &self.active_assignments,
            &self.revocations,
        )?;
        let mut writer = Writer::new();
        put_hint(&mut writer, self.hint);
        writer.put_uuid(&self.member_id);
        writer.put_u8(self.group_phase);
        writer.put_u32(self.heartbeat_interval_ms);
        put_topic_partitions(
            &mut writer,
            &self.active_assignments,
            MAX_MEMBER_ASSIGNMENT_PARTITIONS,
            "active_assignments",
            false,
        )?;
        put_topic_partitions(
            &mut writer,
            &self.revocations,
            MAX_MEMBER_ASSIGNMENT_PARTITIONS,
            "revocations",
            false,
        )?;
        put_top_level_tags(&mut writer, &self.tags)?;
        Ok(writer.into_inner())
    }

    fn decode_body(bytes: &[u8]) -> Result<Self> {
        let mut reader = Reader::new(bytes);
        let hint = read_hint(&mut reader)?;
        let member_id = reader.read_uuid()?;
        let group_phase = reader.read_u8()?;
        let heartbeat_interval_ms = reader.read_u32()?;
        let active_assignments = read_topic_partitions(
            &mut reader,
            MAX_MEMBER_ASSIGNMENT_PARTITIONS,
            "active_assignments",
            false,
        )?;
        let revocations = read_topic_partitions(
            &mut reader,
            MAX_MEMBER_ASSIGNMENT_PARTITIONS,
            "revocations",
            false,
        )?;
        let tags = finish_body(&mut reader)?;
        validate_member_state(
            group_phase,
            heartbeat_interval_ms,
            &active_assignments,
            &revocations,
        )?;
        Ok(Self {
            hint,
            member_id,
            group_phase,
            heartbeat_interval_ms,
            active_assignments,
            revocations,
            tags,
        })
    }
}

impl BodyCodec for HeartbeatRequestV0 {
    fn encode_body(&self) -> Result<Vec<u8>> {
        let mut writer = Writer::new();
        put_authority(&mut writer, &self.authority)?;
        put_top_level_tags(&mut writer, &self.tags)?;
        Ok(writer.into_inner())
    }

    fn decode_body(bytes: &[u8]) -> Result<Self> {
        let mut reader = Reader::new(bytes);
        let authority = read_authority(&mut reader)?;
        let tags = finish_body(&mut reader)?;
        Ok(Self { authority, tags })
    }
}

impl BodyCodec for HeartbeatResponseV0 {
    fn encode_body(&self) -> Result<Vec<u8>> {
        validate_member_state(
            self.group_phase,
            self.heartbeat_interval_ms,
            &self.active_assignments,
            &self.revocations,
        )?;
        let mut writer = Writer::new();
        put_hint(&mut writer, self.hint);
        writer.put_u8(self.group_phase);
        writer.put_u32(self.heartbeat_interval_ms);
        put_topic_partitions(
            &mut writer,
            &self.active_assignments,
            MAX_MEMBER_ASSIGNMENT_PARTITIONS,
            "active_assignments",
            false,
        )?;
        put_topic_partitions(
            &mut writer,
            &self.revocations,
            MAX_MEMBER_ASSIGNMENT_PARTITIONS,
            "revocations",
            false,
        )?;
        put_top_level_tags(&mut writer, &self.tags)?;
        Ok(writer.into_inner())
    }

    fn decode_body(bytes: &[u8]) -> Result<Self> {
        let mut reader = Reader::new(bytes);
        let hint = read_hint(&mut reader)?;
        let group_phase = reader.read_u8()?;
        let heartbeat_interval_ms = reader.read_u32()?;
        let active_assignments = read_topic_partitions(
            &mut reader,
            MAX_MEMBER_ASSIGNMENT_PARTITIONS,
            "active_assignments",
            false,
        )?;
        let revocations = read_topic_partitions(
            &mut reader,
            MAX_MEMBER_ASSIGNMENT_PARTITIONS,
            "revocations",
            false,
        )?;
        let tags = finish_body(&mut reader)?;
        validate_member_state(
            group_phase,
            heartbeat_interval_ms,
            &active_assignments,
            &revocations,
        )?;
        Ok(Self {
            hint,
            group_phase,
            heartbeat_interval_ms,
            active_assignments,
            revocations,
            tags,
        })
    }
}

impl BodyCodec for RevokeAckRequestV0 {
    fn encode_body(&self) -> Result<Vec<u8>> {
        validate_topic_partitions(
            &self.revoked,
            MAX_MEMBER_ASSIGNMENT_PARTITIONS,
            "revoked",
            true,
        )?;
        let mut writer = Writer::new();
        put_authority(&mut writer, &self.authority)?;
        put_topic_partitions(
            &mut writer,
            &self.revoked,
            MAX_MEMBER_ASSIGNMENT_PARTITIONS,
            "revoked",
            true,
        )?;
        put_top_level_tags(&mut writer, &self.tags)?;
        Ok(writer.into_inner())
    }

    fn decode_body(bytes: &[u8]) -> Result<Self> {
        let mut reader = Reader::new(bytes);
        let authority = read_authority(&mut reader)?;
        let revoked = read_topic_partitions(
            &mut reader,
            MAX_MEMBER_ASSIGNMENT_PARTITIONS,
            "revoked",
            true,
        )?;
        let tags = finish_body(&mut reader)?;
        Ok(Self {
            authority,
            revoked,
            tags,
        })
    }
}

impl BodyCodec for LeaveRequestV0 {
    fn encode_body(&self) -> Result<Vec<u8>> {
        let mut writer = Writer::new();
        put_authority(&mut writer, &self.authority)?;
        put_top_level_tags(&mut writer, &self.tags)?;
        Ok(writer.into_inner())
    }

    fn decode_body(bytes: &[u8]) -> Result<Self> {
        let mut reader = Reader::new(bytes);
        let authority = read_authority(&mut reader)?;
        let tags = finish_body(&mut reader)?;
        Ok(Self { authority, tags })
    }
}

impl BodyCodec for LeaveResponseV0 {
    fn encode_body(&self) -> Result<Vec<u8>> {
        let mut writer = Writer::new();
        put_hint(&mut writer, self.hint);
        put_top_level_tags(&mut writer, &self.tags)?;
        Ok(writer.into_inner())
    }

    fn decode_body(bytes: &[u8]) -> Result<Self> {
        let mut reader = Reader::new(bytes);
        let hint = read_hint(&mut reader)?;
        let tags = finish_body(&mut reader)?;
        Ok(Self { hint, tags })
    }
}

impl BodyCodec for CommitOffsetsRequestV0 {
    fn encode_body(&self) -> Result<Vec<u8>> {
        validate_commit_offsets(&self.offsets)?;
        let mut writer = Writer::new();
        put_authority(&mut writer, &self.authority)?;
        writer
            .put_u32(u32::try_from(self.offsets.len()).map_err(|_| ProtocolError::LengthOverflow)?);
        for offset in &self.offsets {
            writer.put_topic_id(offset.topic_id);
            writer.put_u32(offset.partition);
            writer.put_u64(offset.next_offset);
            validate_offset_metadata(&offset.metadata)?;
            writer.put_string16(&offset.metadata, MAX_OFFSET_METADATA_LEN, "offset_metadata")?;
        }
        put_top_level_tags(&mut writer, &self.tags)?;
        Ok(writer.into_inner())
    }

    fn decode_body(bytes: &[u8]) -> Result<Self> {
        let mut reader = Reader::new(bytes);
        let authority = read_authority(&mut reader)?;
        let count = read_bounded_count(&mut reader, MAX_OFFSETS_PER_REQUEST, 30, "offsets")?;
        if count == 0 {
            return Err(ProtocolError::InvalidFrame("offset commit batch is empty"));
        }
        let mut offsets = Vec::with_capacity(count);
        for _ in 0..count {
            let topic_id = reader.read_topic_id()?;
            let partition = reader.read_u32()?;
            let next_offset = reader.read_u64()?;
            let metadata = reader.read_string16(MAX_OFFSET_METADATA_LEN, "offset_metadata")?;
            validate_offset_metadata(&metadata)?;
            offsets.push(CommitOffsetV0 {
                topic_id,
                partition,
                next_offset,
                metadata,
            });
        }
        validate_commit_offsets(&offsets)?;
        let tags = finish_body(&mut reader)?;
        Ok(Self {
            authority,
            offsets,
            tags,
        })
    }
}

impl BodyCodec for CommitOffsetsResponseV0 {
    fn encode_body(&self) -> Result<Vec<u8>> {
        let mut writer = Writer::new();
        put_hint(&mut writer, self.hint);
        writer.put_u64(self.offsets_commit_index);
        put_top_level_tags(&mut writer, &self.tags)?;
        Ok(writer.into_inner())
    }

    fn decode_body(bytes: &[u8]) -> Result<Self> {
        let mut reader = Reader::new(bytes);
        let hint = read_hint(&mut reader)?;
        let offsets_commit_index = reader.read_u64()?;
        let tags = finish_body(&mut reader)?;
        Ok(Self {
            hint,
            offsets_commit_index,
            tags,
        })
    }
}

impl BodyCodec for FetchOffsetsRequestV0 {
    fn encode_body(&self) -> Result<Vec<u8>> {
        validate_topic_partitions(
            &self.partitions,
            MAX_OFFSETS_PER_REQUEST,
            "fetch_partitions",
            true,
        )?;
        let mut writer = Writer::new();
        put_group_id(&mut writer, &self.group_id)?;
        put_topic_partitions(
            &mut writer,
            &self.partitions,
            MAX_OFFSETS_PER_REQUEST,
            "fetch_partitions",
            true,
        )?;
        put_top_level_tags(&mut writer, &self.tags)?;
        Ok(writer.into_inner())
    }

    fn decode_body(bytes: &[u8]) -> Result<Self> {
        let mut reader = Reader::new(bytes);
        let group_id = read_group_id(&mut reader)?;
        let partitions = read_topic_partitions(
            &mut reader,
            MAX_OFFSETS_PER_REQUEST,
            "fetch_partitions",
            true,
        )?;
        let tags = finish_body(&mut reader)?;
        Ok(Self {
            group_id,
            partitions,
            tags,
        })
    }
}

impl BodyCodec for FetchOffsetsResponseV0 {
    fn encode_body(&self) -> Result<Vec<u8>> {
        validate_fetch_results(&self.results)?;
        let mut writer = Writer::new();
        put_hint(&mut writer, self.hint);
        writer
            .put_u32(u32::try_from(self.results.len()).map_err(|_| ProtocolError::LengthOverflow)?);
        for result in &self.results {
            writer.put_topic_id(result.topic_id);
            writer.put_u32(result.partition);
            writer.put_bool(result.value.is_some());
            if let Some(value) = &result.value {
                writer.put_u64(value.next_offset);
                writer.put_i64(value.commit_timestamp_ms);
                writer.put_u64(value.group_epoch.get());
                writer.put_uuid(&value.member_id.as_bytes());
                writer.put_u64(value.member_epoch.get());
                validate_offset_metadata(&value.metadata)?;
                writer.put_string16(&value.metadata, MAX_OFFSET_METADATA_LEN, "offset_metadata")?;
            }
        }
        put_top_level_tags(&mut writer, &self.tags)?;
        Ok(writer.into_inner())
    }

    fn decode_body(bytes: &[u8]) -> Result<Self> {
        let mut reader = Reader::new(bytes);
        let hint = read_hint(&mut reader)?;
        let count = read_bounded_count(&mut reader, MAX_OFFSETS_PER_REQUEST, 21, "fetch_results")?;
        let mut results = Vec::with_capacity(count);
        for _ in 0..count {
            let topic_id = reader.read_topic_id()?;
            let partition = reader.read_u32()?;
            let value = if reader.read_bool()? {
                let next_offset = reader.read_u64()?;
                let commit_timestamp_ms = reader.read_i64()?;
                let group_epoch = GroupEpoch::new(reader.read_u64()?)?;
                let member_id = MemberId::new(reader.read_uuid()?)?;
                let member_epoch = MemberEpoch::new(reader.read_u64()?)?;
                let metadata = reader.read_string16(MAX_OFFSET_METADATA_LEN, "offset_metadata")?;
                validate_offset_metadata(&metadata)?;
                Some(FetchOffsetValueV0 {
                    next_offset,
                    commit_timestamp_ms,
                    group_epoch,
                    member_id,
                    member_epoch,
                    metadata,
                })
            } else {
                None
            };
            results.push(FetchOffsetResultV0 {
                topic_id,
                partition,
                value,
            });
        }
        validate_fetch_results(&results)?;
        let tags = finish_body(&mut reader)?;
        Ok(Self {
            hint,
            results,
            tags,
        })
    }
}

fn validate_text_id(value: &str, max: usize, field: &'static str) -> Result<()> {
    let len = value.len();
    if len == 0 || len > max {
        return Err(ProtocolError::FieldTooLarge { field, len, max });
    }
    if value.as_bytes().contains(&0) {
        return Err(ProtocolError::InvalidFrame(
            "NUL is forbidden in group identifier",
        ));
    }
    Ok(())
}

fn read_text_id(reader: &mut Reader<'_>, max: usize, field: &'static str) -> Result<String> {
    let value = reader.read_string16(max, field)?;
    validate_text_id(&value, max, field)?;
    Ok(value)
}

fn put_group_id(writer: &mut Writer, value: &GroupId) -> Result<()> {
    writer.put_string16(value.as_str(), MAX_GROUP_ID_LEN, "group_id")
}

fn read_group_id(reader: &mut Reader<'_>) -> Result<GroupId> {
    GroupId::new(read_text_id(reader, MAX_GROUP_ID_LEN, "group_id")?)
}

fn validate_instance_id(value: &Option<String>) -> Result<()> {
    if let Some(value) = value {
        validate_text_id(value, MAX_INSTANCE_ID_LEN, "instance_id")?;
    }
    Ok(())
}

fn validate_join(value: &JoinSubscribeRequestV0) -> Result<()> {
    validate_instance_id(&value.instance_id)?;
    if !(MIN_SESSION_TIMEOUT_MS..=MAX_SESSION_TIMEOUT_MS).contains(&value.session_timeout_ms) {
        return Err(ProtocolError::InvalidFrame(
            "session timeout outside P6 V0 range",
        ));
    }
    let is_new = value.member_id == [0; 16];
    if is_new {
        if value.expected_member_epoch != 0 {
            return Err(ProtocolError::InvalidFrame(
                "new member must use expected member epoch zero",
            ));
        }
    } else {
        let _ = MemberId::new(value.member_id)?;
        let _ = MemberEpoch::new(value.expected_member_epoch)?;
    }
    if value.expected_group_epoch != 0 {
        let _ = GroupEpoch::new(value.expected_group_epoch)?;
    }
    if value.subscriptions.len() > MAX_SUBSCRIPTION_TOPICS {
        return Err(ProtocolError::FieldTooLarge {
            field: "subscriptions",
            len: value.subscriptions.len(),
            max: MAX_SUBSCRIPTION_TOPICS,
        });
    }
    validate_topic_ids(&value.subscriptions)
}

fn validate_topic_ids(values: &[TopicId]) -> Result<()> {
    let mut previous: Option<TopicId> = None;
    for current in values {
        if previous.is_some_and(|value| value >= *current) {
            return Err(ProtocolError::InvalidFrame(
                "topic ids must be strictly ascending and unique",
            ));
        }
        previous = Some(*current);
    }
    Ok(())
}

fn put_topic_ids(writer: &mut Writer, values: &[TopicId]) -> Result<()> {
    validate_topic_ids(values)?;
    writer.put_u32(u32::try_from(values.len()).map_err(|_| ProtocolError::LengthOverflow)?);
    for value in values {
        writer.put_topic_id(*value);
    }
    Ok(())
}

fn put_authority(writer: &mut Writer, authority: &MemberAuthorityV0) -> Result<()> {
    put_group_id(writer, &authority.group_id)?;
    writer.put_uuid(&authority.member_id.as_bytes());
    writer.put_u64(authority.group_epoch.get());
    writer.put_u64(authority.member_epoch.get());
    Ok(())
}

fn read_authority(reader: &mut Reader<'_>) -> Result<MemberAuthorityV0> {
    Ok(MemberAuthorityV0 {
        group_id: read_group_id(reader)?,
        member_id: MemberId::new(reader.read_uuid()?)?,
        group_epoch: GroupEpoch::new(reader.read_u64()?)?,
        member_epoch: MemberEpoch::new(reader.read_u64()?)?,
    })
}

fn put_hint(writer: &mut Writer, hint: CoordinatorHintV0) {
    writer.put_u32(hint.coordinator_broker_id);
    writer.put_u64(hint.coordinator_epoch);
    writer.put_u64(hint.current_group_epoch);
    writer.put_u64(hint.current_member_epoch);
}

fn read_hint(reader: &mut Reader<'_>) -> Result<CoordinatorHintV0> {
    Ok(CoordinatorHintV0 {
        coordinator_broker_id: reader.read_u32()?,
        coordinator_epoch: reader.read_u64()?,
        current_group_epoch: reader.read_u64()?,
        current_member_epoch: reader.read_u64()?,
    })
}

fn validate_group_phase(value: u8) -> Result<()> {
    if value > 3 {
        return Err(ProtocolError::InvalidFrame("unknown P6 group phase"));
    }
    Ok(())
}

fn validate_heartbeat_interval(value: u32) -> Result<()> {
    if value != 0 && !(1_000..=10_000).contains(&value) {
        return Err(ProtocolError::InvalidFrame(
            "heartbeat interval outside P6 V0 range",
        ));
    }
    Ok(())
}

fn validate_member_state(
    phase: u8,
    heartbeat_interval_ms: u32,
    active: &[TopicPartitionV0],
    revocations: &[TopicPartitionV0],
) -> Result<()> {
    validate_group_phase(phase)?;
    validate_heartbeat_interval(heartbeat_interval_ms)?;
    validate_topic_partitions(
        active,
        MAX_MEMBER_ASSIGNMENT_PARTITIONS,
        "active_assignments",
        false,
    )?;
    validate_topic_partitions(
        revocations,
        MAX_MEMBER_ASSIGNMENT_PARTITIONS,
        "revocations",
        false,
    )
}

fn validate_topic_partitions(
    values: &[TopicPartitionV0],
    max: usize,
    field: &'static str,
    require_nonempty: bool,
) -> Result<()> {
    if require_nonempty && values.is_empty() {
        return Err(ProtocolError::InvalidFrame("partition set is empty"));
    }
    if values.len() > max {
        return Err(ProtocolError::FieldTooLarge {
            field,
            len: values.len(),
            max,
        });
    }
    let mut previous = None;
    for current in values {
        if previous.is_some_and(|value: TopicPartitionV0| value >= *current) {
            return Err(ProtocolError::InvalidFrame(
                "topic partitions must be strictly ascending and unique",
            ));
        }
        previous = Some(*current);
    }
    Ok(())
}

fn put_topic_partitions(
    writer: &mut Writer,
    values: &[TopicPartitionV0],
    max: usize,
    field: &'static str,
    require_nonempty: bool,
) -> Result<()> {
    validate_topic_partitions(values, max, field, require_nonempty)?;
    writer.put_u32(u32::try_from(values.len()).map_err(|_| ProtocolError::LengthOverflow)?);
    for value in values {
        writer.put_topic_id(value.topic_id);
        writer.put_u32(value.partition);
    }
    Ok(())
}

fn read_topic_partitions(
    reader: &mut Reader<'_>,
    max: usize,
    field: &'static str,
    require_nonempty: bool,
) -> Result<Vec<TopicPartitionV0>> {
    let count = read_bounded_count(reader, max, 20, field)?;
    if require_nonempty && count == 0 {
        return Err(ProtocolError::InvalidFrame("partition set is empty"));
    }
    let mut values = Vec::with_capacity(count);
    for _ in 0..count {
        values.push(TopicPartitionV0 {
            topic_id: reader.read_topic_id()?,
            partition: reader.read_u32()?,
        });
    }
    validate_topic_partitions(&values, max, field, require_nonempty)?;
    Ok(values)
}

fn validate_offset_metadata(value: &str) -> Result<()> {
    if value.len() > MAX_OFFSET_METADATA_LEN {
        return Err(ProtocolError::FieldTooLarge {
            field: "offset_metadata",
            len: value.len(),
            max: MAX_OFFSET_METADATA_LEN,
        });
    }
    Ok(())
}

fn validate_commit_offsets(values: &[CommitOffsetV0]) -> Result<()> {
    if values.is_empty() {
        return Err(ProtocolError::InvalidFrame("offset commit batch is empty"));
    }
    if values.len() > MAX_OFFSETS_PER_REQUEST {
        return Err(ProtocolError::FieldTooLarge {
            field: "offsets",
            len: values.len(),
            max: MAX_OFFSETS_PER_REQUEST,
        });
    }
    let mut previous: Option<TopicPartitionV0> = None;
    for value in values {
        validate_offset_metadata(&value.metadata)?;
        let current = TopicPartitionV0 {
            topic_id: value.topic_id,
            partition: value.partition,
        };
        if previous.is_some_and(|prior| prior >= current) {
            return Err(ProtocolError::InvalidFrame(
                "offset entries must be strictly ascending and unique",
            ));
        }
        previous = Some(current);
    }
    Ok(())
}

fn validate_fetch_results(values: &[FetchOffsetResultV0]) -> Result<()> {
    if values.len() > MAX_OFFSETS_PER_REQUEST {
        return Err(ProtocolError::FieldTooLarge {
            field: "fetch_results",
            len: values.len(),
            max: MAX_OFFSETS_PER_REQUEST,
        });
    }
    let mut seen = BTreeSet::new();
    let mut previous = None;
    for value in values {
        let current = TopicPartitionV0 {
            topic_id: value.topic_id,
            partition: value.partition,
        };
        if previous.is_some_and(|prior| prior >= current) || !seen.insert(current) {
            return Err(ProtocolError::InvalidFrame(
                "fetch results must be strictly ascending and unique",
            ));
        }
        if let Some(offset) = &value.value {
            validate_offset_metadata(&offset.metadata)?;
        }
        previous = Some(current);
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn topic(byte: u8) -> TopicId {
        TopicId::new([byte; 16])
    }

    fn authority() -> MemberAuthorityV0 {
        MemberAuthorityV0 {
            group_id: GroupId::new("orders").unwrap(),
            member_id: MemberId::new([7; 16]).unwrap(),
            group_epoch: GroupEpoch::new(3).unwrap(),
            member_epoch: MemberEpoch::new(2).unwrap(),
        }
    }

    #[test]
    fn checked_group_types_reject_reserved_or_noncanonical_values() {
        assert!(GroupId::new("").is_err());
        assert!(GroupId::new("bad group").is_err());
        assert!(GroupId::new("a".repeat(256)).is_err());
        assert!(MemberId::new([0; 16]).is_err());
        assert!(GroupEpoch::new(0).is_err());
        assert!(MemberEpoch::new(0).is_err());
        assert!(GroupStateVersion::new(0).is_err());
        assert!(GroupEpoch::new(u64::MAX).unwrap().checked_next().is_err());
    }

    #[test]
    fn all_p6_public_bodies_round_trip_canonically() {
        let tp = TopicPartitionV0 {
            topic_id: topic(1),
            partition: 2,
        };
        let join = JoinSubscribeRequestV0 {
            group_id: GroupId::new("orders").unwrap(),
            member_id: [0; 16],
            instance_id: Some("worker-a".into()),
            expected_group_epoch: 0,
            expected_member_epoch: 0,
            session_timeout_ms: 30_000,
            subscriptions: vec![topic(1), topic(2)],
            tags: vec![],
        };
        assert_eq!(
            JoinSubscribeRequestV0::decode_body(&join.encode_body().unwrap()).unwrap(),
            join
        );
        let join_response = JoinSubscribeResponseV0 {
            hint: CoordinatorHintV0 {
                coordinator_broker_id: 1,
                coordinator_epoch: 9,
                current_group_epoch: 3,
                current_member_epoch: 2,
            },
            member_id: [7; 16],
            group_phase: 3,
            heartbeat_interval_ms: 10_000,
            active_assignments: vec![tp],
            revocations: vec![tp],
            tags: vec![],
        };
        assert_eq!(
            JoinSubscribeResponseV0::decode_body(&join_response.encode_body().unwrap()).unwrap(),
            join_response
        );
        let heartbeat = HeartbeatRequestV0 {
            authority: authority(),
            tags: vec![],
        };
        assert_eq!(
            HeartbeatRequestV0::decode_body(&heartbeat.encode_body().unwrap()).unwrap(),
            heartbeat
        );
        let heartbeat_response = HeartbeatResponseV0 {
            hint: CoordinatorHintV0::UNKNOWN,
            group_phase: 1,
            heartbeat_interval_ms: 1_000,
            active_assignments: vec![tp],
            revocations: vec![],
            tags: vec![],
        };
        assert_eq!(
            HeartbeatResponseV0::decode_body(&heartbeat_response.encode_body().unwrap()).unwrap(),
            heartbeat_response
        );
        let revoke = RevokeAckRequestV0 {
            authority: authority(),
            revoked: vec![tp],
            tags: vec![],
        };
        assert_eq!(
            RevokeAckRequestV0::decode_body(&revoke.encode_body().unwrap()).unwrap(),
            revoke
        );
        let leave = LeaveRequestV0 {
            authority: authority(),
            tags: vec![],
        };
        assert_eq!(
            LeaveRequestV0::decode_body(&leave.encode_body().unwrap()).unwrap(),
            leave
        );
        let leave_response = LeaveResponseV0 {
            hint: CoordinatorHintV0::UNKNOWN,
            tags: vec![],
        };
        assert_eq!(
            LeaveResponseV0::decode_body(&leave_response.encode_body().unwrap()).unwrap(),
            leave_response
        );
        let commit = CommitOffsetsRequestV0 {
            authority: authority(),
            offsets: vec![CommitOffsetV0 {
                topic_id: topic(1),
                partition: 2,
                next_offset: 42,
                metadata: "m".into(),
            }],
            tags: vec![],
        };
        assert_eq!(
            CommitOffsetsRequestV0::decode_body(&commit.encode_body().unwrap()).unwrap(),
            commit
        );
        let commit_response = CommitOffsetsResponseV0 {
            hint: CoordinatorHintV0::UNKNOWN,
            offsets_commit_index: 77,
            tags: vec![],
        };
        assert_eq!(
            CommitOffsetsResponseV0::decode_body(&commit_response.encode_body().unwrap()).unwrap(),
            commit_response
        );
        let fetch = FetchOffsetsRequestV0 {
            group_id: GroupId::new("orders").unwrap(),
            partitions: vec![tp],
            tags: vec![],
        };
        assert_eq!(
            FetchOffsetsRequestV0::decode_body(&fetch.encode_body().unwrap()).unwrap(),
            fetch
        );
        let fetch_response = FetchOffsetsResponseV0 {
            hint: CoordinatorHintV0::UNKNOWN,
            results: vec![FetchOffsetResultV0 {
                topic_id: topic(1),
                partition: 2,
                value: Some(FetchOffsetValueV0 {
                    next_offset: 42,
                    commit_timestamp_ms: 123,
                    group_epoch: GroupEpoch::new(3).unwrap(),
                    member_id: MemberId::new([7; 16]).unwrap(),
                    member_epoch: MemberEpoch::new(2).unwrap(),
                    metadata: "m".into(),
                }),
            }],
            tags: vec![],
        };
        assert_eq!(
            FetchOffsetsResponseV0::decode_body(&fetch_response.encode_body().unwrap()).unwrap(),
            fetch_response
        );
    }

    #[test]
    fn p6_public_decoders_reject_noncanonical_and_oversized_shapes() {
        let mut duplicate = FetchOffsetsRequestV0 {
            group_id: GroupId::new("g").unwrap(),
            partitions: vec![
                TopicPartitionV0 {
                    topic_id: topic(1),
                    partition: 0,
                },
                TopicPartitionV0 {
                    topic_id: topic(1),
                    partition: 0,
                },
            ],
            tags: vec![],
        };
        assert!(duplicate.encode_body().is_err());
        duplicate.partitions.truncate(1);
        let mut bytes = duplicate.encode_body().unwrap();
        bytes.push(0);
        assert!(FetchOffsetsRequestV0::decode_body(&bytes).is_err());
        let bad_join = JoinSubscribeRequestV0 {
            group_id: GroupId::new("g").unwrap(),
            member_id: [0; 16],
            instance_id: None,
            expected_group_epoch: 0,
            expected_member_epoch: 1,
            session_timeout_ms: 30_000,
            subscriptions: vec![],
            tags: vec![],
        };
        assert!(bad_join.encode_body().is_err());
        let oversized = CommitOffsetsRequestV0 {
            authority: authority(),
            offsets: (0..=MAX_OFFSETS_PER_REQUEST)
                .map(|partition| CommitOffsetV0 {
                    topic_id: topic(1),
                    partition: u32::try_from(partition).unwrap(),
                    next_offset: 0,
                    metadata: String::new(),
                })
                .collect(),
            tags: vec![],
        };
        assert!(oversized.encode_body().is_err());
    }
}
