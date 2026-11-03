use astralog_common::{PartitionId, RaftIndex, RaftTerm, SegmentBaseOffset, StreamOffset, TopicId};

use crate::{
    Result, StorageError,
    compression::{compress_data_payload, decompress_data_payload},
    limits::StorageLimits,
};

pub const FORMAT_VERSION: u16 = 1;
pub const SEGMENT_HEADER_LEN: usize = 72;
pub const PARTITION_META_LEN: usize = 64;
pub const FRAME_HEADER_LEN: usize = 96;
pub const FRAME_CRC_LEN: usize = 4;
pub const MIN_FRAME_LEN: usize = FRAME_HEADER_LEN + FRAME_CRC_LEN;

const SEGMENT_MAGIC: &[u8; 8] = b"ASTRALOG";
const PARTITION_META_MAGIC: &[u8; 8] = b"ASTMETA1";
const FRAME_MAGIC: &[u8; 4] = b"ALF1";
const IDEMPOTENT_ATTRIBUTE: u32 = 1;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum FrameType {
    Data = 1,
    Control = 2,
}

impl TryFrom<u8> for FrameType {
    type Error = StorageError;

    fn try_from(value: u8) -> Result<Self> {
        match value {
            1 => Ok(Self::Data),
            2 => Ok(Self::Control),
            _ => Err(StorageError::InvalidFormatOwned(format!(
                "unknown frame type {value}",
            ))),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum Compression {
    None = 0,
    Lz4 = 1,
    Zstd = 2,
}

impl TryFrom<u8> for Compression {
    type Error = StorageError;

    fn try_from(value: u8) -> Result<Self> {
        match value {
            0 => Ok(Self::None),
            1 => Ok(Self::Lz4),
            2 => Ok(Self::Zstd),
            _ => Err(StorageError::InvalidFormatOwned(format!(
                "unknown compression codec {value}",
            ))),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ProducerIdentity {
    pub producer_id: u64,
    pub producer_epoch: u32,
    pub base_sequence: u32,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct UserHeader {
    pub name: Vec<u8>,
    pub value: Option<Vec<u8>>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DataRecord {
    pub timestamp_ms: i64,
    pub key: Option<Vec<u8>>,
    pub value: Option<Vec<u8>>,
    pub headers: Vec<UserHeader>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RecordBatch {
    pub raft_term: RaftTerm,
    pub raft_index: RaftIndex,
    pub base_stream_offset: StreamOffset,
    pub producer: Option<ProducerIdentity>,
    pub compression: Compression,
    pub records: Vec<DataRecord>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ControlEntry {
    pub raft_term: RaftTerm,
    pub raft_index: RaftIndex,
    pub control_kind: u16,
    pub control_version: u16,
    pub body: Vec<u8>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Frame {
    Data(RecordBatch),
    Control(ControlEntry),
}

impl Frame {
    pub fn raft_index(&self) -> RaftIndex {
        match self {
            Self::Data(batch) => batch.raft_index,
            Self::Control(entry) => entry.raft_index,
        }
    }

    pub fn raft_term(&self) -> RaftTerm {
        match self {
            Self::Data(batch) => batch.raft_term,
            Self::Control(entry) => entry.raft_term,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SegmentHeader {
    pub topic_id: TopicId,
    pub partition_id: PartitionId,
    pub generation: u32,
    pub base_stream_offset: SegmentBaseOffset,
    pub base_raft_index: RaftIndex,
    pub created_at_ms: i64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PartitionMeta {
    pub topic_id: TopicId,
    pub partition_id: PartitionId,
    pub committed_raft_index: u64,
    pub metadata_epoch: u64,
}

pub fn encode_segment_header(header: &SegmentHeader) -> [u8; SEGMENT_HEADER_LEN] {
    let mut out = [0_u8; SEGMENT_HEADER_LEN];
    out[0..8].copy_from_slice(SEGMENT_MAGIC);
    out[8..10].copy_from_slice(&FORMAT_VERSION.to_le_bytes());
    out[10..12].copy_from_slice(&(SEGMENT_HEADER_LEN as u16).to_le_bytes());
    out[12..16].copy_from_slice(&0_u32.to_le_bytes());
    out[16..32].copy_from_slice(header.topic_id.as_bytes());
    out[32..36].copy_from_slice(&header.partition_id.get().to_le_bytes());
    out[36..40].copy_from_slice(&header.generation.to_le_bytes());
    out[40..48].copy_from_slice(&header.base_stream_offset.get().to_le_bytes());
    out[48..56].copy_from_slice(&header.base_raft_index.get().to_le_bytes());
    out[56..64].copy_from_slice(&header.created_at_ms.to_le_bytes());
    out[64..68].copy_from_slice(&0_u32.to_le_bytes());
    let crc = crc32c::crc32c(&out[..68]);
    out[68..72].copy_from_slice(&crc.to_le_bytes());
    out
}

pub fn decode_segment_header(bytes: &[u8]) -> Result<SegmentHeader> {
    if bytes.len() != SEGMENT_HEADER_LEN {
        return Err(StorageError::InvalidFormat(
            "segment header must be exactly 72 bytes",
        ));
    }
    if &bytes[0..8] != SEGMENT_MAGIC {
        return Err(StorageError::InvalidFormat("segment magic mismatch"));
    }
    let version = read_u16(bytes, 8)?;
    if version != FORMAT_VERSION {
        return Err(StorageError::UnsupportedVersion(version));
    }
    if read_u16(bytes, 10)? as usize != SEGMENT_HEADER_LEN {
        return Err(StorageError::InvalidFormat(
            "segment header length mismatch",
        ));
    }
    if read_u32(bytes, 12)? != 0 || read_u32(bytes, 64)? != 0 {
        return Err(StorageError::InvalidFormat(
            "segment reserved field is nonzero",
        ));
    }
    let expected = read_u32(bytes, 68)?;
    let actual = crc32c::crc32c(&bytes[..68]);
    if expected != actual {
        return Err(StorageError::ChecksumMismatch {
            context: "segment header",
            expected,
            actual,
        });
    }
    let mut topic = [0_u8; 16];
    topic.copy_from_slice(&bytes[16..32]);
    let base_raft_raw = read_u64(bytes, 48)?;
    let base_raft_index = RaftIndex::new(base_raft_raw).ok_or(StorageError::InvalidFormat(
        "segment base Raft index cannot be zero",
    ))?;

    Ok(SegmentHeader {
        topic_id: TopicId::new(topic),
        partition_id: PartitionId::new(read_u32(bytes, 32)?),
        generation: read_u32(bytes, 36)?,
        base_stream_offset: SegmentBaseOffset::new(read_u64(bytes, 40)?),
        base_raft_index,
        created_at_ms: read_i64(bytes, 56)?,
    })
}

pub fn encode_partition_meta(meta: &PartitionMeta) -> [u8; PARTITION_META_LEN] {
    let mut out = [0_u8; PARTITION_META_LEN];
    out[0..8].copy_from_slice(PARTITION_META_MAGIC);
    out[8..10].copy_from_slice(&FORMAT_VERSION.to_le_bytes());
    out[10..12].copy_from_slice(&(PARTITION_META_LEN as u16).to_le_bytes());
    out[12..16].copy_from_slice(&0_u32.to_le_bytes());
    out[16..32].copy_from_slice(meta.topic_id.as_bytes());
    out[32..36].copy_from_slice(&meta.partition_id.get().to_le_bytes());
    out[36..40].copy_from_slice(&0_u32.to_le_bytes());
    out[40..48].copy_from_slice(&meta.committed_raft_index.to_le_bytes());
    out[48..56].copy_from_slice(&meta.metadata_epoch.to_le_bytes());
    out[56..60].copy_from_slice(&0_u32.to_le_bytes());
    let crc = crc32c::crc32c(&out[..60]);
    out[60..64].copy_from_slice(&crc.to_le_bytes());
    out
}

pub fn decode_partition_meta(bytes: &[u8]) -> Result<PartitionMeta> {
    if bytes.len() != PARTITION_META_LEN {
        return Err(StorageError::InvalidFormat(
            "partition metadata must be exactly 64 bytes",
        ));
    }
    if &bytes[0..8] != PARTITION_META_MAGIC {
        return Err(StorageError::InvalidFormat(
            "partition metadata magic mismatch",
        ));
    }
    let version = read_u16(bytes, 8)?;
    if version != FORMAT_VERSION {
        return Err(StorageError::UnsupportedVersion(version));
    }
    if read_u16(bytes, 10)? as usize != PARTITION_META_LEN {
        return Err(StorageError::InvalidFormat(
            "partition metadata length mismatch",
        ));
    }
    if read_u32(bytes, 12)? != 0 || read_u32(bytes, 36)? != 0 || read_u32(bytes, 56)? != 0 {
        return Err(StorageError::InvalidFormat(
            "partition metadata reserved field is nonzero",
        ));
    }
    let expected = read_u32(bytes, 60)?;
    let actual = crc32c::crc32c(&bytes[..60]);
    if expected != actual {
        return Err(StorageError::ChecksumMismatch {
            context: "partition metadata",
            expected,
            actual,
        });
    }
    let mut topic = [0_u8; 16];
    topic.copy_from_slice(&bytes[16..32]);
    Ok(PartitionMeta {
        topic_id: TopicId::new(topic),
        partition_id: PartitionId::new(read_u32(bytes, 32)?),
        committed_raft_index: read_u64(bytes, 40)?,
        metadata_epoch: read_u64(bytes, 48)?,
    })
}

pub fn encode_frame(frame: &Frame, limits: StorageLimits) -> Result<Vec<u8>> {
    match frame {
        Frame::Data(batch) => encode_data_frame(batch, limits),
        Frame::Control(entry) => encode_control_frame(entry, limits),
    }
}

fn encode_data_frame(batch: &RecordBatch, limits: StorageLimits) -> Result<Vec<u8>> {
    if batch.records.is_empty() {
        return Err(StorageError::InvalidFormat("data batch cannot be empty"));
    }
    let record_count =
        u32::try_from(batch.records.len()).map_err(|_| StorageError::LimitExceeded {
            what: "record count",
            actual: batch.records.len(),
            max: u32::MAX as usize,
        })?;
    if let Some(producer) = &batch.producer {
        validate_producer_identity(producer, record_count)?;
    }

    let min_timestamp = batch
        .records
        .iter()
        .map(|record| record.timestamp_ms)
        .min()
        .ok_or(StorageError::InvalidFormat("data batch cannot be empty"))?;
    let max_timestamp = batch
        .records
        .iter()
        .map(|record| record.timestamp_ms)
        .max()
        .ok_or(StorageError::InvalidFormat("data batch cannot be empty"))?;

    let mut payload = Vec::new();
    for (ordinal, record) in batch.records.iter().enumerate() {
        encode_record(&mut payload, ordinal, min_timestamp, record, limits)?;
    }
    let uncompressed_payload_len = payload.len();
    if uncompressed_payload_len > limits.max_uncompressed_payload_bytes {
        return Err(StorageError::LimitExceeded {
            what: "uncompressed payload bytes",
            actual: uncompressed_payload_len,
            max: limits.max_uncompressed_payload_bytes,
        });
    }
    let (stored_compression, stored_payload) = compress_data_payload(batch.compression, &payload)?;
    let payload_len = stored_payload.len();
    let total_len = FRAME_HEADER_LEN
        .checked_add(payload_len)
        .and_then(|value| value.checked_add(FRAME_CRC_LEN))
        .ok_or(StorageError::ArithmeticOverflow("frame length"))?;
    if total_len > limits.max_frame_bytes {
        return Err(StorageError::LimitExceeded {
            what: "frame bytes",
            actual: total_len,
            max: limits.max_frame_bytes,
        });
    }
    let total_len_u32 = u32::try_from(total_len)
        .map_err(|_| StorageError::ArithmeticOverflow("u32 frame length"))?;
    let payload_len_u32 = u32::try_from(payload_len)
        .map_err(|_| StorageError::ArithmeticOverflow("u32 payload length"))?;
    let uncompressed_payload_len_u32 = u32::try_from(uncompressed_payload_len)
        .map_err(|_| StorageError::ArithmeticOverflow("u32 uncompressed payload length"))?;

    let mut out = Vec::with_capacity(total_len);
    out.extend_from_slice(&total_len_u32.to_le_bytes());
    out.extend_from_slice(FRAME_MAGIC);
    out.extend_from_slice(&FORMAT_VERSION.to_le_bytes());
    out.push(FrameType::Data as u8);
    out.push(stored_compression as u8);
    let attributes = if batch.producer.is_some() {
        IDEMPOTENT_ATTRIBUTE
    } else {
        0
    };
    out.extend_from_slice(&attributes.to_le_bytes());
    out.extend_from_slice(&batch.raft_term.get().to_le_bytes());
    out.extend_from_slice(&batch.raft_index.get().to_le_bytes());
    out.extend_from_slice(&batch.base_stream_offset.get().to_le_bytes());
    out.extend_from_slice(&record_count.to_le_bytes());
    out.extend_from_slice(&0_u32.to_le_bytes());
    out.extend_from_slice(&min_timestamp.to_le_bytes());
    out.extend_from_slice(&max_timestamp.to_le_bytes());
    if let Some(producer) = &batch.producer {
        out.extend_from_slice(&producer.producer_id.to_le_bytes());
        out.extend_from_slice(&producer.producer_epoch.to_le_bytes());
        out.extend_from_slice(&producer.base_sequence.to_le_bytes());
    } else {
        out.extend_from_slice(&0_u64.to_le_bytes());
        out.extend_from_slice(&0_u32.to_le_bytes());
        out.extend_from_slice(&0_u32.to_le_bytes());
    }
    out.extend_from_slice(&payload_len_u32.to_le_bytes());
    out.extend_from_slice(&uncompressed_payload_len_u32.to_le_bytes());
    out.extend_from_slice(&1_u16.to_le_bytes());
    out.extend_from_slice(&(FRAME_HEADER_LEN as u16).to_le_bytes());
    out.extend_from_slice(&0_u32.to_le_bytes());
    debug_assert_eq!(out.len(), FRAME_HEADER_LEN);
    out.extend_from_slice(&stored_payload);
    let crc = crc32c::crc32c(&out);
    out.extend_from_slice(&crc.to_le_bytes());
    Ok(out)
}

fn encode_record(
    payload: &mut Vec<u8>,
    ordinal: usize,
    min_timestamp: i64,
    record: &DataRecord,
    limits: StorageLimits,
) -> Result<()> {
    if record.headers.len() > limits.max_header_count {
        return Err(StorageError::LimitExceeded {
            what: "record header count",
            actual: record.headers.len(),
            max: limits.max_header_count,
        });
    }
    if let Some(key) = &record.key
        && key.len() > limits.max_key_bytes
    {
        return Err(StorageError::LimitExceeded {
            what: "record key bytes",
            actual: key.len(),
            max: limits.max_key_bytes,
        });
    }

    let mut body = Vec::new();
    let ordinal = u32::try_from(ordinal)
        .map_err(|_| StorageError::ArithmeticOverflow("record offset delta"))?;
    body.extend_from_slice(&ordinal.to_le_bytes());
    let delta = record
        .timestamp_ms
        .checked_sub(min_timestamp)
        .ok_or(StorageError::ArithmeticOverflow("record timestamp delta"))?;
    body.extend_from_slice(&delta.to_le_bytes());
    encode_nullable_len(&mut body, record.key.as_deref(), "record key")?;
    encode_nullable_len(&mut body, record.value.as_deref(), "record value")?;
    let header_count = u16::try_from(record.headers.len())
        .map_err(|_| StorageError::ArithmeticOverflow("record header count"))?;
    body.extend_from_slice(&header_count.to_le_bytes());
    body.extend_from_slice(&0_u16.to_le_bytes());
    if let Some(key) = &record.key {
        body.extend_from_slice(key);
    }
    if let Some(value) = &record.value {
        body.extend_from_slice(value);
    }
    for header in &record.headers {
        if header.name.len() > limits.max_header_name_bytes {
            return Err(StorageError::LimitExceeded {
                what: "header name bytes",
                actual: header.name.len(),
                max: limits.max_header_name_bytes,
            });
        }
        let name_len = u16::try_from(header.name.len())
            .map_err(|_| StorageError::ArithmeticOverflow("header name length"))?;
        body.extend_from_slice(&name_len.to_le_bytes());
        encode_nullable_len(&mut body, header.value.as_deref(), "header value")?;
        body.extend_from_slice(&header.name);
        if let Some(value) = &header.value {
            body.extend_from_slice(value);
        }
    }
    if body.len() + 4 > limits.max_record_bytes {
        return Err(StorageError::LimitExceeded {
            what: "record bytes",
            actual: body.len() + 4,
            max: limits.max_record_bytes,
        });
    }
    let record_len =
        u32::try_from(body.len()).map_err(|_| StorageError::ArithmeticOverflow("record length"))?;
    payload.extend_from_slice(&record_len.to_le_bytes());
    payload.extend_from_slice(&body);
    Ok(())
}

fn encode_nullable_len(out: &mut Vec<u8>, bytes: Option<&[u8]>, what: &'static str) -> Result<()> {
    match bytes {
        Some(bytes) => {
            let len =
                i32::try_from(bytes.len()).map_err(|_| StorageError::ArithmeticOverflow(what))?;
            out.extend_from_slice(&len.to_le_bytes());
        }
        None => out.extend_from_slice(&(-1_i32).to_le_bytes()),
    }
    Ok(())
}

fn encode_control_frame(entry: &ControlEntry, limits: StorageLimits) -> Result<Vec<u8>> {
    if entry.control_kind == 0 {
        return Err(StorageError::InvalidFormat("control kind cannot be zero"));
    }
    let body_len = u32::try_from(entry.body.len())
        .map_err(|_| StorageError::ArithmeticOverflow("control body length"))?;
    let payload_len = 8_usize
        .checked_add(entry.body.len())
        .ok_or(StorageError::ArithmeticOverflow("control payload length"))?;
    let total_len = FRAME_HEADER_LEN
        .checked_add(payload_len)
        .and_then(|value| value.checked_add(FRAME_CRC_LEN))
        .ok_or(StorageError::ArithmeticOverflow("control frame length"))?;
    if total_len > limits.max_frame_bytes {
        return Err(StorageError::LimitExceeded {
            what: "frame bytes",
            actual: total_len,
            max: limits.max_frame_bytes,
        });
    }
    let total_len_u32 = u32::try_from(total_len)
        .map_err(|_| StorageError::ArithmeticOverflow("u32 frame length"))?;
    let payload_len_u32 = u32::try_from(payload_len)
        .map_err(|_| StorageError::ArithmeticOverflow("u32 payload length"))?;

    let mut out = Vec::with_capacity(total_len);
    out.extend_from_slice(&total_len_u32.to_le_bytes());
    out.extend_from_slice(FRAME_MAGIC);
    out.extend_from_slice(&FORMAT_VERSION.to_le_bytes());
    out.push(FrameType::Control as u8);
    out.push(Compression::None as u8);
    out.extend_from_slice(&0_u32.to_le_bytes());
    out.extend_from_slice(&entry.raft_term.get().to_le_bytes());
    out.extend_from_slice(&entry.raft_index.get().to_le_bytes());
    out.extend_from_slice(&u64::MAX.to_le_bytes());
    out.extend_from_slice(&0_u32.to_le_bytes());
    out.extend_from_slice(&0_u32.to_le_bytes());
    out.extend_from_slice(&0_i64.to_le_bytes());
    out.extend_from_slice(&0_i64.to_le_bytes());
    out.extend_from_slice(&0_u64.to_le_bytes());
    out.extend_from_slice(&0_u32.to_le_bytes());
    out.extend_from_slice(&0_u32.to_le_bytes());
    out.extend_from_slice(&payload_len_u32.to_le_bytes());
    out.extend_from_slice(&payload_len_u32.to_le_bytes());
    out.extend_from_slice(&2_u16.to_le_bytes());
    out.extend_from_slice(&(FRAME_HEADER_LEN as u16).to_le_bytes());
    out.extend_from_slice(&0_u32.to_le_bytes());
    debug_assert_eq!(out.len(), FRAME_HEADER_LEN);
    out.extend_from_slice(&entry.control_kind.to_le_bytes());
    out.extend_from_slice(&entry.control_version.to_le_bytes());
    out.extend_from_slice(&body_len.to_le_bytes());
    out.extend_from_slice(&entry.body);
    let crc = crc32c::crc32c(&out);
    out.extend_from_slice(&crc.to_le_bytes());
    Ok(out)
}

pub fn decode_frame(bytes: &[u8], limits: StorageLimits) -> Result<Frame> {
    if bytes.len() < MIN_FRAME_LEN {
        return Err(StorageError::InvalidFormat("frame shorter than minimum"));
    }
    if bytes.len() > limits.max_frame_bytes {
        return Err(StorageError::LimitExceeded {
            what: "frame bytes",
            actual: bytes.len(),
            max: limits.max_frame_bytes,
        });
    }
    let total_len = read_u32(bytes, 0)? as usize;
    if total_len != bytes.len() {
        return Err(StorageError::InvalidFormat("total frame length mismatch"));
    }
    if &bytes[4..8] != FRAME_MAGIC {
        return Err(StorageError::InvalidFormat("frame magic mismatch"));
    }
    let version = read_u16(bytes, 8)?;
    if version != FORMAT_VERSION {
        return Err(StorageError::UnsupportedVersion(version));
    }
    if read_u16(bytes, 90)? as usize != FRAME_HEADER_LEN {
        return Err(StorageError::InvalidFormat("frame header length mismatch"));
    }
    if read_u32(bytes, 44)? != 0 || read_u32(bytes, 92)? != 0 {
        return Err(StorageError::InvalidFormat(
            "frame reserved field is nonzero",
        ));
    }
    let stored_payload_len = read_u32(bytes, 80)? as usize;
    let expected_payload_len = bytes
        .len()
        .checked_sub(MIN_FRAME_LEN)
        .ok_or(StorageError::ArithmeticOverflow("frame payload length"))?;
    if stored_payload_len != expected_payload_len {
        return Err(StorageError::InvalidFormat(
            "stored payload length mismatch",
        ));
    }
    let uncompressed_len = read_u32(bytes, 84)? as usize;
    if uncompressed_len > limits.max_uncompressed_payload_bytes {
        return Err(StorageError::LimitExceeded {
            what: "uncompressed payload bytes",
            actual: uncompressed_len,
            max: limits.max_uncompressed_payload_bytes,
        });
    }
    let expected_crc = read_u32(bytes, bytes.len() - 4)?;
    let actual_crc = crc32c::crc32c(&bytes[..bytes.len() - 4]);
    if expected_crc != actual_crc {
        return Err(StorageError::ChecksumMismatch {
            context: "frame",
            expected: expected_crc,
            actual: actual_crc,
        });
    }
    let compression = Compression::try_from(bytes[11])?;
    let frame_type = FrameType::try_from(bytes[10])?;
    let attributes = read_u32(bytes, 12)?;
    if attributes & !IDEMPOTENT_ATTRIBUTE != 0 {
        return Err(StorageError::InvalidFormat(
            "unknown V1 frame attribute bit",
        ));
    }
    let raft_term = RaftTerm::new(read_u64(bytes, 16)?);
    let raft_index = RaftIndex::new(read_u64(bytes, 24)?).ok_or(StorageError::InvalidFormat(
        "frame Raft index cannot be zero",
    ))?;
    let payload_schema = read_u16(bytes, 88)?;
    let stored_payload = &bytes[FRAME_HEADER_LEN..bytes.len() - FRAME_CRC_LEN];
    if frame_type == FrameType::Control && compression != Compression::None {
        return Err(StorageError::InvalidFormat(
            "control frames must use compression=none",
        ));
    }
    let decompressed_payload;
    let payload = if compression == Compression::None {
        if uncompressed_len != stored_payload_len {
            return Err(StorageError::InvalidFormat(
                "uncompressed length must equal stored length for compression=none",
            ));
        }
        stored_payload
    } else {
        decompressed_payload =
            decompress_data_payload(compression, stored_payload, uncompressed_len)?;
        decompressed_payload.as_slice()
    };

    match frame_type {
        FrameType::Data => decode_data_frame(
            bytes,
            payload,
            payload_schema,
            attributes,
            compression,
            raft_term,
            raft_index,
            limits,
        ),
        FrameType::Control => decode_control_frame(
            bytes,
            payload,
            payload_schema,
            attributes,
            raft_term,
            raft_index,
        ),
    }
}

#[allow(clippy::too_many_arguments)]
fn decode_data_frame(
    header: &[u8],
    payload: &[u8],
    payload_schema: u16,
    attributes: u32,
    compression: Compression,
    raft_term: RaftTerm,
    raft_index: RaftIndex,
    limits: StorageLimits,
) -> Result<Frame> {
    if payload_schema != 1 {
        return Err(StorageError::InvalidFormat(
            "data frame payload schema must be 1",
        ));
    }
    let record_count_u32 = read_u32(header, 40)?;
    let record_count = record_count_u32 as usize;
    if record_count == 0 {
        return Err(StorageError::InvalidFormat(
            "data frame record count cannot be zero",
        ));
    }
    let base_offset_raw = read_u64(header, 32)?;
    if base_offset_raw == u64::MAX {
        return Err(StorageError::InvalidFormat(
            "data frame cannot use control stream-offset sentinel",
        ));
    }
    let min_timestamp = read_i64(header, 48)?;
    let max_timestamp = read_i64(header, 56)?;
    if min_timestamp > max_timestamp {
        return Err(StorageError::InvalidFormat(
            "data frame timestamp range is inverted",
        ));
    }
    let producer = if attributes & IDEMPOTENT_ATTRIBUTE != 0 {
        let producer = ProducerIdentity {
            producer_id: read_u64(header, 64)?,
            producer_epoch: read_u32(header, 72)?,
            base_sequence: read_u32(header, 76)?,
        };
        validate_producer_identity(&producer, record_count_u32)?;
        Some(producer)
    } else {
        if read_u64(header, 64)? != 0 || read_u32(header, 72)? != 0 || read_u32(header, 76)? != 0 {
            return Err(StorageError::InvalidFormat(
                "producer fields must be zero when idempotent flag is clear",
            ));
        }
        None
    };

    let mut cursor = SliceCursor::new(payload);
    let mut records = Vec::with_capacity(record_count.min(1024));
    for ordinal in 0..record_count {
        let record_len = cursor.read_u32()? as usize;
        if record_len + 4 > limits.max_record_bytes {
            return Err(StorageError::LimitExceeded {
                what: "record bytes",
                actual: record_len + 4,
                max: limits.max_record_bytes,
            });
        }
        let record_bytes = cursor.take(record_len)?;
        records.push(decode_record(
            record_bytes,
            ordinal,
            min_timestamp,
            max_timestamp,
            limits,
        )?);
    }
    if !cursor.is_empty() {
        return Err(StorageError::InvalidFormat(
            "data payload contains trailing bytes after record count",
        ));
    }

    Ok(Frame::Data(RecordBatch {
        raft_term,
        raft_index,
        base_stream_offset: StreamOffset::new(base_offset_raw),
        producer,
        compression,
        records,
    }))
}

fn validate_producer_identity(producer: &ProducerIdentity, record_count: u32) -> Result<()> {
    if producer.producer_id == 0 {
        return Err(StorageError::InvalidFormat(
            "idempotent producer id zero is reserved",
        ));
    }
    if producer.producer_epoch == 0 {
        return Err(StorageError::InvalidFormat(
            "idempotent producer epoch zero is reserved",
        ));
    }
    if producer.base_sequence.checked_add(record_count).is_none() {
        return Err(StorageError::InvalidFormat(
            "idempotent producer sequence range cannot wrap",
        ));
    }
    Ok(())
}

fn decode_record(
    bytes: &[u8],
    ordinal: usize,
    min_timestamp: i64,
    max_timestamp: i64,
    limits: StorageLimits,
) -> Result<DataRecord> {
    let mut cursor = SliceCursor::new(bytes);
    let offset_delta = cursor.read_u32()? as usize;
    if offset_delta != ordinal {
        return Err(StorageError::InvalidFormatOwned(format!(
            "record offset delta {offset_delta} does not match ordinal {ordinal}",
        )));
    }
    let timestamp_delta = cursor.read_i64()?;
    let timestamp_ms = min_timestamp
        .checked_add(timestamp_delta)
        .ok_or(StorageError::ArithmeticOverflow("record timestamp"))?;
    if timestamp_ms < min_timestamp || timestamp_ms > max_timestamp {
        return Err(StorageError::InvalidFormat(
            "record timestamp outside frame timestamp range",
        ));
    }
    let key_len = cursor.read_i32()?;
    let value_len = cursor.read_i32()?;
    validate_nullable_len(key_len, "record key")?;
    validate_nullable_len(value_len, "record value")?;
    if key_len > limits.max_key_bytes as i32 {
        return Err(StorageError::LimitExceeded {
            what: "record key bytes",
            actual: key_len as usize,
            max: limits.max_key_bytes,
        });
    }
    let header_count = cursor.read_u16()? as usize;
    if header_count > limits.max_header_count {
        return Err(StorageError::LimitExceeded {
            what: "record header count",
            actual: header_count,
            max: limits.max_header_count,
        });
    }
    if cursor.read_u16()? != 0 {
        return Err(StorageError::InvalidFormat("record attributes are nonzero"));
    }
    let key = read_nullable_bytes(&mut cursor, key_len)?;
    let value = read_nullable_bytes(&mut cursor, value_len)?;
    let mut headers = Vec::with_capacity(header_count);
    for _ in 0..header_count {
        let name_len = cursor.read_u16()? as usize;
        if name_len > limits.max_header_name_bytes {
            return Err(StorageError::LimitExceeded {
                what: "header name bytes",
                actual: name_len,
                max: limits.max_header_name_bytes,
            });
        }
        let header_value_len = cursor.read_i32()?;
        validate_nullable_len(header_value_len, "header value")?;
        let name = cursor.take(name_len)?.to_vec();
        let header_value = read_nullable_bytes(&mut cursor, header_value_len)?;
        headers.push(UserHeader {
            name,
            value: header_value,
        });
    }
    if !cursor.is_empty() {
        return Err(StorageError::InvalidFormat(
            "record contains trailing bytes",
        ));
    }
    Ok(DataRecord {
        timestamp_ms,
        key,
        value,
        headers,
    })
}

fn decode_control_frame(
    header: &[u8],
    payload: &[u8],
    payload_schema: u16,
    attributes: u32,
    raft_term: RaftTerm,
    raft_index: RaftIndex,
) -> Result<Frame> {
    if payload_schema != 2 {
        return Err(StorageError::InvalidFormat(
            "control frame payload schema must be 2",
        ));
    }
    if attributes != 0 {
        return Err(StorageError::InvalidFormat(
            "control frame attributes must be zero",
        ));
    }
    if read_u64(header, 32)? != u64::MAX
        || read_u32(header, 40)? != 0
        || read_i64(header, 48)? != 0
        || read_i64(header, 56)? != 0
        || read_u64(header, 64)? != 0
        || read_u32(header, 72)? != 0
        || read_u32(header, 76)? != 0
    {
        return Err(StorageError::InvalidFormat(
            "control frame contains invalid data-only fields",
        ));
    }
    let mut cursor = SliceCursor::new(payload);
    let control_kind = cursor.read_u16()?;
    if control_kind == 0 {
        return Err(StorageError::InvalidFormat("control kind cannot be zero"));
    }
    let control_version = cursor.read_u16()?;
    let body_len = cursor.read_u32()? as usize;
    let body = cursor.take(body_len)?.to_vec();
    if !cursor.is_empty() {
        return Err(StorageError::InvalidFormat(
            "control payload has trailing bytes",
        ));
    }
    Ok(Frame::Control(ControlEntry {
        raft_term,
        raft_index,
        control_kind,
        control_version,
        body,
    }))
}

fn validate_nullable_len(len: i32, what: &'static str) -> Result<()> {
    if len < -1 {
        return Err(StorageError::InvalidFormatOwned(format!(
            "{what} length cannot be {len}",
        )));
    }
    Ok(())
}

fn read_nullable_bytes(cursor: &mut SliceCursor<'_>, len: i32) -> Result<Option<Vec<u8>>> {
    if len == -1 {
        Ok(None)
    } else {
        Ok(Some(cursor.take(len as usize)?.to_vec()))
    }
}

fn read_u16(bytes: &[u8], offset: usize) -> Result<u16> {
    let data = bytes
        .get(offset..offset + 2)
        .ok_or(StorageError::InvalidFormat("unexpected end of bytes"))?;
    Ok(u16::from_le_bytes([data[0], data[1]]))
}

fn read_u32(bytes: &[u8], offset: usize) -> Result<u32> {
    let data = bytes
        .get(offset..offset + 4)
        .ok_or(StorageError::InvalidFormat("unexpected end of bytes"))?;
    Ok(u32::from_le_bytes([data[0], data[1], data[2], data[3]]))
}

fn read_u64(bytes: &[u8], offset: usize) -> Result<u64> {
    let data = bytes
        .get(offset..offset + 8)
        .ok_or(StorageError::InvalidFormat("unexpected end of bytes"))?;
    Ok(u64::from_le_bytes([
        data[0], data[1], data[2], data[3], data[4], data[5], data[6], data[7],
    ]))
}

fn read_i64(bytes: &[u8], offset: usize) -> Result<i64> {
    let data = bytes
        .get(offset..offset + 8)
        .ok_or(StorageError::InvalidFormat("unexpected end of bytes"))?;
    Ok(i64::from_le_bytes([
        data[0], data[1], data[2], data[3], data[4], data[5], data[6], data[7],
    ]))
}

struct SliceCursor<'a> {
    bytes: &'a [u8],
    pos: usize,
}

impl<'a> SliceCursor<'a> {
    fn new(bytes: &'a [u8]) -> Self {
        Self { bytes, pos: 0 }
    }

    fn take(&mut self, len: usize) -> Result<&'a [u8]> {
        let end = self
            .pos
            .checked_add(len)
            .ok_or(StorageError::ArithmeticOverflow("slice cursor"))?;
        let out = self
            .bytes
            .get(self.pos..end)
            .ok_or(StorageError::InvalidFormat("unexpected end of payload"))?;
        self.pos = end;
        Ok(out)
    }

    fn read_u16(&mut self) -> Result<u16> {
        let bytes = self.take(2)?;
        Ok(u16::from_le_bytes([bytes[0], bytes[1]]))
    }

    fn read_u32(&mut self) -> Result<u32> {
        let bytes = self.take(4)?;
        Ok(u32::from_le_bytes([bytes[0], bytes[1], bytes[2], bytes[3]]))
    }

    fn read_i32(&mut self) -> Result<i32> {
        let bytes = self.take(4)?;
        Ok(i32::from_le_bytes([bytes[0], bytes[1], bytes[2], bytes[3]]))
    }

    fn read_i64(&mut self) -> Result<i64> {
        let bytes = self.take(8)?;
        Ok(i64::from_le_bytes([
            bytes[0], bytes[1], bytes[2], bytes[3], bytes[4], bytes[5], bytes[6], bytes[7],
        ]))
    }

    fn is_empty(&self) -> bool {
        self.pos == self.bytes.len()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_support::decode_hex;

    fn rewrite_crc(bytes: &mut [u8]) {
        let crc_offset = bytes.len() - FRAME_CRC_LEN;
        let crc = crc32c::crc32c(&bytes[..crc_offset]);
        bytes[crc_offset..].copy_from_slice(&crc.to_le_bytes());
    }

    fn p8_compressible_frame(compression: Compression) -> Frame {
        Frame::Data(RecordBatch {
            raft_term: RaftTerm::new(7),
            raft_index: RaftIndex::new(19).unwrap(),
            base_stream_offset: StreamOffset::new(300),
            producer: Some(ProducerIdentity {
                producer_id: 9001,
                producer_epoch: 4,
                base_sequence: 33,
            }),
            compression,
            records: vec![
                DataRecord {
                    timestamp_ms: 1_700_000_100_000,
                    key: Some(b"repeat-key".to_vec()),
                    value: Some(vec![b'A'; 768]),
                    headers: vec![UserHeader {
                        name: b"codec".to_vec(),
                        value: Some(b"p8".to_vec()),
                    }],
                },
                DataRecord {
                    timestamp_ms: 1_700_000_100_005,
                    key: Some(b"repeat-key".to_vec()),
                    value: Some(vec![b'A'; 768]),
                    headers: vec![],
                },
            ],
        })
    }

    fn topic() -> TopicId {
        TopicId::new([
            0x00, 0x11, 0x22, 0x33, 0x44, 0x55, 0x66, 0x77, 0x88, 0x99, 0xaa, 0xbb, 0xcc, 0xdd,
            0xee, 0xff,
        ])
    }

    #[test]
    fn segment_header_round_trip() {
        let header = SegmentHeader {
            topic_id: topic(),
            partition_id: PartitionId::new(7),
            generation: 3,
            base_stream_offset: SegmentBaseOffset::new(42),
            base_raft_index: RaftIndex::new(11).unwrap(),
            created_at_ms: 1_700_000_000_000,
        };
        let encoded = encode_segment_header(&header);
        let golden = decode_hex(include_str!("../tests/golden/segment_header_v1.hex"));
        assert_eq!(encoded.as_slice(), golden.as_slice());
        assert_eq!(decode_segment_header(&encoded).unwrap(), header);
    }

    #[test]
    fn partition_meta_round_trip() {
        let meta = PartitionMeta {
            topic_id: topic(),
            partition_id: PartitionId::new(2),
            committed_raft_index: 9,
            metadata_epoch: 4,
        };
        let encoded = encode_partition_meta(&meta);
        let golden = decode_hex(include_str!(
            "../tests/golden/partition_meta_committed_v1.hex"
        ));
        assert_eq!(encoded.as_slice(), golden.as_slice());
        assert_eq!(decode_partition_meta(&encoded).unwrap(), meta);
    }

    #[test]
    fn empty_partition_meta_matches_golden_v1() {
        let meta = PartitionMeta {
            topic_id: topic(),
            partition_id: PartitionId::new(2),
            committed_raft_index: 0,
            metadata_epoch: 0,
        };
        let encoded = encode_partition_meta(&meta);
        let golden = decode_hex(include_str!("../tests/golden/partition_meta_empty_v1.hex"));
        assert_eq!(encoded.as_slice(), golden.as_slice());
        assert_eq!(decode_partition_meta(&golden).unwrap(), meta);
    }

    #[test]
    fn data_frame_round_trip_preserves_null_and_empty() {
        let frame = Frame::Data(RecordBatch {
            raft_term: RaftTerm::new(3),
            raft_index: RaftIndex::new(8).unwrap(),
            base_stream_offset: StreamOffset::new(100),
            producer: Some(ProducerIdentity {
                producer_id: 123,
                producer_epoch: 2,
                base_sequence: 17,
            }),
            compression: Compression::None,
            records: vec![
                DataRecord {
                    timestamp_ms: 1000,
                    key: None,
                    value: Some(Vec::new()),
                    headers: vec![UserHeader {
                        name: b"trace".to_vec(),
                        value: Some(b"abc".to_vec()),
                    }],
                },
                DataRecord {
                    timestamp_ms: 1010,
                    key: Some(b"k".to_vec()),
                    value: None,
                    headers: Vec::new(),
                },
            ],
        });
        let encoded = encode_frame(&frame, StorageLimits::default()).unwrap();
        let golden = decode_hex(include_str!("../tests/golden/data_idempotent_multi_v1.hex"));
        assert_eq!(encoded, golden);
        let decoded = decode_frame(&encoded, StorageLimits::default()).unwrap();
        assert_eq!(decoded, frame);
    }

    #[test]
    fn non_idempotent_single_record_matches_golden_v1() {
        let frame = Frame::Data(RecordBatch {
            raft_term: RaftTerm::new(1),
            raft_index: RaftIndex::FIRST,
            base_stream_offset: StreamOffset::new(0),
            producer: None,
            compression: Compression::None,
            records: vec![DataRecord {
                timestamp_ms: 1_700_000_000_123,
                key: Some(b"k".to_vec()),
                value: Some(b"hello".to_vec()),
                headers: Vec::new(),
            }],
        });
        let golden = decode_hex(include_str!("../tests/golden/data_single_v1.hex"));
        assert_eq!(
            encode_frame(&frame, StorageLimits::default()).unwrap(),
            golden,
        );
        assert_eq!(
            decode_frame(&golden, StorageLimits::default()).unwrap(),
            frame,
        );
    }

    #[test]
    fn control_frame_round_trip() {
        let frame = Frame::Control(ControlEntry {
            raft_term: RaftTerm::new(4),
            raft_index: RaftIndex::new(12).unwrap(),
            control_kind: 7,
            control_version: 1,
            body: b"opaque-control-body".to_vec(),
        });
        let encoded = encode_frame(&frame, StorageLimits::default()).unwrap();
        let golden = decode_hex(include_str!("../tests/golden/control_v1.hex"));
        assert_eq!(encoded, golden);
        assert_eq!(
            decode_frame(&encoded, StorageLimits::default()).unwrap(),
            frame,
        );
    }

    #[test]
    fn p8_lz4_and_zstd_data_frames_round_trip_with_physical_length_metadata() {
        for codec in [Compression::Lz4, Compression::Zstd] {
            let frame = p8_compressible_frame(codec);
            let encoded = encode_frame(&frame, StorageLimits::default()).unwrap();
            let golden = match codec {
                Compression::Lz4 => decode_hex(include_str!("../tests/golden/data_p8_lz4_v1.hex")),
                Compression::Zstd => {
                    decode_hex(include_str!("../tests/golden/data_p8_zstd_v1.hex"))
                }
                Compression::None => unreachable!(),
            };
            assert_eq!(encoded, golden);
            assert_eq!(Compression::try_from(encoded[11]).unwrap(), codec);
            let stored_len = read_u32(&encoded, 80).unwrap() as usize;
            let uncompressed_len = read_u32(&encoded, 84).unwrap() as usize;
            assert_eq!(stored_len, encoded.len() - MIN_FRAME_LEN);
            assert!(stored_len < uncompressed_len);
            assert_eq!(
                decode_frame(&encoded, StorageLimits::default()).unwrap(),
                frame
            );
        }
    }

    #[test]
    fn p8_frame_decode_checks_ceiling_crc_and_codec_before_logical_records() {
        let encoded = encode_frame(
            &p8_compressible_frame(Compression::Zstd),
            StorageLimits::default(),
        )
        .unwrap();

        let mut crc_damaged = encoded.clone();
        crc_damaged[FRAME_HEADER_LEN] ^= 0x01;
        assert!(matches!(
            decode_frame(&crc_damaged, StorageLimits::default()),
            Err(StorageError::ChecksumMismatch {
                context: "frame",
                ..
            })
        ));

        let limits = StorageLimits::default();
        let mut over_limit = encoded.clone();
        over_limit[84..88].copy_from_slice(
            &u32::try_from(limits.max_uncompressed_payload_bytes + 1)
                .unwrap()
                .to_le_bytes(),
        );
        assert!(matches!(
            decode_frame(&over_limit, limits),
            Err(StorageError::LimitExceeded {
                what: "uncompressed payload bytes",
                ..
            })
        ));

        let mut unknown = encoded.clone();
        unknown[11] = 3;
        rewrite_crc(&mut unknown);
        assert!(matches!(
            decode_frame(&unknown, StorageLimits::default()),
            Err(StorageError::InvalidFormatOwned(message))
                if message == "unknown compression codec 3"
        ));
    }

    #[test]
    fn p8_frame_decode_rejects_exact_length_and_trailing_codec_violations() {
        for codec in [Compression::Lz4, Compression::Zstd] {
            let encoded =
                encode_frame(&p8_compressible_frame(codec), StorageLimits::default()).unwrap();
            let declared = read_u32(&encoded, 84).unwrap();

            let mut short = encoded.clone();
            short[84..88].copy_from_slice(&(declared - 1).to_le_bytes());
            rewrite_crc(&mut short);
            assert!(decode_frame(&short, StorageLimits::default()).is_err());

            let mut long = encoded.clone();
            long[84..88].copy_from_slice(&(declared + 1).to_le_bytes());
            rewrite_crc(&mut long);
            assert!(decode_frame(&long, StorageLimits::default()).is_err());

            let crc_offset = encoded.len() - FRAME_CRC_LEN;
            let mut trailing = encoded[..crc_offset].to_vec();
            trailing.push(0);
            trailing.extend_from_slice(&0_u32.to_le_bytes());
            let total = u32::try_from(trailing.len()).unwrap();
            trailing[0..4].copy_from_slice(&total.to_le_bytes());
            let stored = read_u32(&trailing, 80).unwrap() + 1;
            trailing[80..84].copy_from_slice(&stored.to_le_bytes());
            rewrite_crc(&mut trailing);
            assert!(decode_frame(&trailing, StorageLimits::default()).is_err());
        }
    }

    #[test]
    fn p8_control_frames_remain_none_and_requested_no_gain_falls_back_to_none() {
        let control = Frame::Control(ControlEntry {
            raft_term: RaftTerm::new(2),
            raft_index: RaftIndex::new(3).unwrap(),
            control_kind: 4,
            control_version: 1,
            body: b"control-body".to_vec(),
        });
        let encoded = encode_frame(&control, StorageLimits::default()).unwrap();
        assert_eq!(encoded[11], Compression::None as u8);

        let mut forged = encoded.clone();
        forged[11] = Compression::Lz4 as u8;
        rewrite_crc(&mut forged);
        assert!(matches!(
            decode_frame(&forged, StorageLimits::default()),
            Err(StorageError::InvalidFormat(
                "control frames must use compression=none"
            ))
        ));

        for codec in [Compression::Lz4, Compression::Zstd] {
            assert_eq!(
                compress_data_payload(codec, &[0x7f]).unwrap(),
                (Compression::None, vec![0x7f]),
            );
        }
    }

    #[test]
    fn frame_crc_detects_bit_flip() {
        let frame = Frame::Control(ControlEntry {
            raft_term: RaftTerm::new(1),
            raft_index: RaftIndex::FIRST,
            control_kind: 1,
            control_version: 1,
            body: b"abc".to_vec(),
        });
        let mut encoded = encode_frame(&frame, StorageLimits::default()).unwrap();
        encoded[FRAME_HEADER_LEN + 1] ^= 0x40;
        assert!(matches!(
            decode_frame(&encoded, StorageLimits::default()),
            Err(StorageError::ChecksumMismatch {
                context: "frame",
                ..
            })
        ));
    }

    #[test]
    fn idempotent_producer_metadata_is_checked_and_crc_covered() {
        let frame = Frame::Data(RecordBatch {
            raft_term: RaftTerm::new(3),
            raft_index: RaftIndex::new(8).unwrap(),
            base_stream_offset: StreamOffset::new(100),
            producer: Some(ProducerIdentity {
                producer_id: 123,
                producer_epoch: 2,
                base_sequence: 17,
            }),
            compression: Compression::None,
            records: vec![DataRecord {
                timestamp_ms: 1000,
                key: None,
                value: Some(vec![1]),
                headers: vec![],
            }],
        });
        let encoded = encode_frame(&frame, StorageLimits::default()).unwrap();

        let mut crc_damaged = encoded.clone();
        crc_damaged[64] ^= 0x01;
        assert!(matches!(
            decode_frame(&crc_damaged, StorageLimits::default()),
            Err(StorageError::ChecksumMismatch {
                context: "frame",
                ..
            })
        ));

        for (range, replacement, expected) in [
            (
                64..72,
                0_u64.to_le_bytes().to_vec(),
                "idempotent producer id zero is reserved",
            ),
            (
                72..76,
                0_u32.to_le_bytes().to_vec(),
                "idempotent producer epoch zero is reserved",
            ),
            (
                76..80,
                u32::MAX.to_le_bytes().to_vec(),
                "idempotent producer sequence range cannot wrap",
            ),
        ] {
            let mut malformed = encoded.clone();
            malformed[range].copy_from_slice(&replacement);
            let crc_offset = malformed.len() - FRAME_CRC_LEN;
            let crc = crc32c::crc32c(&malformed[..crc_offset]);
            malformed[crc_offset..].copy_from_slice(&crc.to_le_bytes());
            assert!(matches!(
                decode_frame(&malformed, StorageLimits::default()),
                Err(StorageError::InvalidFormat(message)) if message == expected
            ));
        }

        let mut zero_id = match frame {
            Frame::Data(batch) => batch,
            Frame::Control(_) => unreachable!(),
        };
        zero_id.producer.as_mut().unwrap().producer_id = 0;
        assert!(matches!(
            encode_frame(&Frame::Data(zero_id), StorageLimits::default()),
            Err(StorageError::InvalidFormat(
                "idempotent producer id zero is reserved"
            ))
        ));
    }

    #[test]
    fn frame_length_is_checked_before_payload_use() {
        let frame = Frame::Control(ControlEntry {
            raft_term: RaftTerm::new(1),
            raft_index: RaftIndex::FIRST,
            control_kind: 1,
            control_version: 1,
            body: Vec::new(),
        });
        let mut encoded = encode_frame(&frame, StorageLimits::default()).unwrap();
        encoded[0..4].copy_from_slice(&u32::MAX.to_le_bytes());
        assert!(matches!(
            decode_frame(&encoded, StorageLimits::default()),
            Err(StorageError::InvalidFormat("total frame length mismatch"))
        ));
    }

    #[test]
    fn every_single_bit_corruption_is_detected() {
        let frame = Frame::Control(ControlEntry {
            raft_term: RaftTerm::new(2),
            raft_index: RaftIndex::FIRST,
            control_kind: 9,
            control_version: 1,
            body: b"crc-must-cover-the-whole-frame".to_vec(),
        });
        let encoded = encode_frame(&frame, StorageLimits::default()).unwrap();
        for byte_index in 0..encoded.len() {
            for bit in 0..8 {
                let mut damaged = encoded.clone();
                damaged[byte_index] ^= 1_u8 << bit;
                assert!(
                    decode_frame(&damaged, StorageLimits::default()).is_err(),
                    "single-bit corruption escaped detection at byte {byte_index}, bit {bit}",
                );
            }
        }
    }

    #[test]
    fn deterministic_decoder_fuzz_never_panics() {
        let mut state = 0x9e37_79b9_7f4a_7c15_u64;
        for case in 0..10_000_usize {
            state ^= state << 13;
            state ^= state >> 7;
            state ^= state << 17;
            let len = (state as usize) % 513;
            let mut bytes = vec![0_u8; len];
            for byte in &mut bytes {
                state ^= state << 13;
                state ^= state >> 7;
                state ^= state << 17;
                *byte = state as u8;
            }
            let outcome = std::panic::catch_unwind(|| {
                let _ = decode_frame(&bytes, StorageLimits::default());
            });
            assert!(
                outcome.is_ok(),
                "decoder panicked on deterministic fuzz case {case}"
            );
        }
    }
}
