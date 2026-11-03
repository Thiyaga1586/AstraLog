use std::{
    fs::File,
    io::{Read, Seek, SeekFrom},
    path::Path,
};

use astralog_common::{PartitionId, RaftIndex, RaftTerm, SegmentBaseOffset, StreamOffset, TopicId};

use crate::{
    Compression, DataRecord, Result, SegmentHeader, StorageError, StorageLimits, UserHeader,
    compression::{compress_data_payload, decompress_data_payload},
};

pub const FORMAT_VERSION_V2: u16 = 2;
pub const RETAINED_HISTORY_FLAG: u32 = 1;
pub const SEGMENT_HEADER_LEN_V2: usize = 72;
pub const FRAME_HEADER_LEN_V2: usize = 96;
pub const FRAME_CRC_LEN_V2: usize = 4;
pub const MIN_FRAME_LEN_V2: usize = FRAME_HEADER_LEN_V2 + FRAME_CRC_LEN_V2;

const SEGMENT_MAGIC: &[u8; 8] = b"ASTRALOG";
const FRAME_MAGIC_V2: &[u8; 4] = b"ALF2";
const SPARSE_RECORD_SCHEMA_V2: u16 = 3;
const RETAINED_CONTROL_SCHEMA_V2: u16 = 2;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum StorageFormatVersion {
    V1,
    V2,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RetainedSegmentHeaderV2 {
    pub topic_id: TopicId,
    pub partition_id: PartitionId,
    pub generation: u32,
    pub base_stream_offset: SegmentBaseOffset,
    pub base_raft_index: RaftIndex,
    pub created_at_ms: i64,
}

impl RetainedSegmentHeaderV2 {
    pub fn identity_header(&self) -> SegmentHeader {
        SegmentHeader {
            topic_id: self.topic_id,
            partition_id: self.partition_id,
            generation: self.generation,
            base_stream_offset: self.base_stream_offset,
            base_raft_index: self.base_raft_index,
            created_at_ms: self.created_at_ms,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SparseRetainedRecordV2 {
    pub offset: StreamOffset,
    pub record: DataRecord,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SparseRetainedBatchV2 {
    pub raft_term: RaftTerm,
    pub raft_index: RaftIndex,
    pub compression: Compression,
    pub records: Vec<SparseRetainedRecordV2>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u16)]
pub enum RetainedControlKindV2 {
    AdvanceLogStartOffset = 1,
    ProducerStateCheckpoint = 2,
    AdvanceCompactionSafeOffset = 3,
}

impl TryFrom<u16> for RetainedControlKindV2 {
    type Error = StorageError;

    fn try_from(value: u16) -> Result<Self> {
        match value {
            1 => Ok(Self::AdvanceLogStartOffset),
            2 => Ok(Self::ProducerStateCheckpoint),
            3 => Ok(Self::AdvanceCompactionSafeOffset),
            _ => Err(StorageError::InvalidFormatOwned(format!(
                "unknown P9 retained control kind {value}",
            ))),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RetainedControlFrameV2 {
    pub raft_term: RaftTerm,
    pub raft_index: RaftIndex,
    pub kind: RetainedControlKindV2,
    pub body: Vec<u8>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RetainedFrameV2 {
    SparseData(SparseRetainedBatchV2),
    Control(RetainedControlFrameV2),
}

impl RetainedFrameV2 {
    pub fn raft_index(&self) -> RaftIndex {
        match self {
            Self::SparseData(batch) => batch.raft_index,
            Self::Control(control) => control.raft_index,
        }
    }

    pub fn raft_term(&self) -> RaftTerm {
        match self {
            Self::SparseData(batch) => batch.raft_term,
            Self::Control(control) => control.raft_term,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RetainedScannedFrameV2 {
    pub position: u64,
    pub encoded_len: u32,
    pub frame: RetainedFrameV2,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RetainedSegmentScanV2 {
    pub header: RetainedSegmentHeaderV2,
    pub frames: Vec<RetainedScannedFrameV2>,
}

pub fn segment_format_version(bytes: &[u8]) -> Result<StorageFormatVersion> {
    if bytes.len() < 10 {
        return Err(StorageError::InvalidFormat(
            "segment header is too short for format dispatch",
        ));
    }
    if &bytes[0..8] != SEGMENT_MAGIC {
        return Err(StorageError::InvalidFormat("segment magic mismatch"));
    }
    match u16::from_le_bytes([bytes[8], bytes[9]]) {
        1 => Ok(StorageFormatVersion::V1),
        2 => Ok(StorageFormatVersion::V2),
        other => Err(StorageError::UnsupportedVersion(other)),
    }
}

pub fn read_segment_format(path: &Path) -> Result<StorageFormatVersion> {
    let mut file = File::open(path)?;
    let mut prefix = [0_u8; 10];
    file.read_exact(&mut prefix)?;
    segment_format_version(&prefix)
}

pub fn encode_retained_segment_header_v2(
    header: &RetainedSegmentHeaderV2,
) -> [u8; SEGMENT_HEADER_LEN_V2] {
    let mut out = [0_u8; SEGMENT_HEADER_LEN_V2];
    out[0..8].copy_from_slice(SEGMENT_MAGIC);
    out[8..10].copy_from_slice(&FORMAT_VERSION_V2.to_le_bytes());
    out[10..12].copy_from_slice(&(SEGMENT_HEADER_LEN_V2 as u16).to_le_bytes());
    out[12..16].copy_from_slice(&RETAINED_HISTORY_FLAG.to_le_bytes());
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

pub fn decode_retained_segment_header_v2(bytes: &[u8]) -> Result<RetainedSegmentHeaderV2> {
    if bytes.len() != SEGMENT_HEADER_LEN_V2 {
        return Err(StorageError::InvalidFormat(
            "V2 segment header must be exactly 72 bytes",
        ));
    }
    if segment_format_version(bytes)? != StorageFormatVersion::V2 {
        return Err(StorageError::UnsupportedVersion(1));
    }
    if read_u16(bytes, 10)? as usize != SEGMENT_HEADER_LEN_V2 {
        return Err(StorageError::InvalidFormat(
            "V2 segment header length mismatch",
        ));
    }
    let flags = read_u32(bytes, 12)?;
    if flags != RETAINED_HISTORY_FLAG {
        return Err(StorageError::InvalidFormat(
            "V2 segment requires exactly RETAINED_HISTORY flag",
        ));
    }
    if read_u32(bytes, 64)? != 0 {
        return Err(StorageError::InvalidFormat(
            "V2 segment reserved field is nonzero",
        ));
    }
    let expected = read_u32(bytes, 68)?;
    let actual = crc32c::crc32c(&bytes[..68]);
    if expected != actual {
        return Err(StorageError::ChecksumMismatch {
            context: "V2 segment header",
            expected,
            actual,
        });
    }
    let base_raft_index = RaftIndex::new(read_u64(bytes, 48)?).ok_or(
        StorageError::InvalidFormat("V2 segment base Raft index cannot be zero"),
    )?;
    let mut topic = [0_u8; 16];
    topic.copy_from_slice(&bytes[16..32]);
    Ok(RetainedSegmentHeaderV2 {
        topic_id: TopicId::new(topic),
        partition_id: PartitionId::new(read_u32(bytes, 32)?),
        generation: read_u32(bytes, 36)?,
        base_stream_offset: SegmentBaseOffset::new(read_u64(bytes, 40)?),
        base_raft_index,
        created_at_ms: read_i64(bytes, 56)?,
    })
}

pub fn read_retained_segment_header_v2(path: &Path) -> Result<RetainedSegmentHeaderV2> {
    let mut file = File::open(path)?;
    let mut bytes = [0_u8; SEGMENT_HEADER_LEN_V2];
    file.read_exact(&mut bytes)?;
    decode_retained_segment_header_v2(&bytes)
}

pub fn encode_retained_frame_v2(frame: &RetainedFrameV2, limits: StorageLimits) -> Result<Vec<u8>> {
    match frame {
        RetainedFrameV2::SparseData(batch) => encode_sparse_data_frame(batch, limits),
        RetainedFrameV2::Control(control) => encode_retained_control_frame(control, limits),
    }
}

fn encode_sparse_data_frame(
    batch: &SparseRetainedBatchV2,
    limits: StorageLimits,
) -> Result<Vec<u8>> {
    if batch.raft_term.get() == 0 {
        return Err(StorageError::InvalidFormat(
            "V2 frame Raft term cannot be zero",
        ));
    }
    if batch.records.is_empty() {
        return Err(StorageError::InvalidFormat(
            "V2 sparse data batch cannot be empty",
        ));
    }
    let record_count =
        u32::try_from(batch.records.len()).map_err(|_| StorageError::LimitExceeded {
            what: "V2 sparse record count",
            actual: batch.records.len(),
            max: u32::MAX as usize,
        })?;
    validate_sparse_offsets(&batch.records)?;
    let base_offset = batch.records[0].offset;
    let min_timestamp = batch
        .records
        .iter()
        .map(|record| record.record.timestamp_ms)
        .min()
        .ok_or(StorageError::InvalidFormat("V2 sparse batch is empty"))?;
    let max_timestamp = batch
        .records
        .iter()
        .map(|record| record.record.timestamp_ms)
        .max()
        .ok_or(StorageError::InvalidFormat("V2 sparse batch is empty"))?;

    let mut raw_payload = Vec::new();
    for retained in &batch.records {
        let delta = retained.offset.get().checked_sub(base_offset.get()).ok_or(
            StorageError::InvalidFormat("V2 sparse offset precedes frame base"),
        )?;
        encode_sparse_record(
            &mut raw_payload,
            delta,
            min_timestamp,
            &retained.record,
            limits,
        )?;
    }
    if raw_payload.len() > limits.max_uncompressed_payload_bytes {
        return Err(StorageError::LimitExceeded {
            what: "V2 uncompressed payload bytes",
            actual: raw_payload.len(),
            max: limits.max_uncompressed_payload_bytes,
        });
    }
    let (compression, stored_payload) = compress_data_payload(batch.compression, &raw_payload)?;
    encode_frame_envelope_v2(
        3,
        compression,
        batch.raft_term,
        batch.raft_index,
        base_offset.get(),
        record_count,
        min_timestamp,
        max_timestamp,
        SPARSE_RECORD_SCHEMA_V2,
        &stored_payload,
        raw_payload.len(),
        limits,
    )
}

fn encode_sparse_record(
    payload: &mut Vec<u8>,
    offset_delta: u64,
    min_timestamp: i64,
    record: &DataRecord,
    limits: StorageLimits,
) -> Result<()> {
    if record.headers.len() > limits.max_header_count {
        return Err(StorageError::LimitExceeded {
            what: "V2 record header count",
            actual: record.headers.len(),
            max: limits.max_header_count,
        });
    }
    if let Some(key) = &record.key
        && key.len() > limits.max_key_bytes
    {
        return Err(StorageError::LimitExceeded {
            what: "V2 record key bytes",
            actual: key.len(),
            max: limits.max_key_bytes,
        });
    }
    let mut body = Vec::new();
    body.extend_from_slice(&offset_delta.to_le_bytes());
    let timestamp_delta =
        record
            .timestamp_ms
            .checked_sub(min_timestamp)
            .ok_or(StorageError::ArithmeticOverflow(
                "V2 record timestamp delta",
            ))?;
    body.extend_from_slice(&timestamp_delta.to_le_bytes());
    encode_nullable_len(&mut body, record.key.as_deref(), "V2 record key")?;
    encode_nullable_len(&mut body, record.value.as_deref(), "V2 record value")?;
    let header_count = u16::try_from(record.headers.len())
        .map_err(|_| StorageError::ArithmeticOverflow("V2 record header count"))?;
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
                what: "V2 header name bytes",
                actual: header.name.len(),
                max: limits.max_header_name_bytes,
            });
        }
        let name_len = u16::try_from(header.name.len())
            .map_err(|_| StorageError::ArithmeticOverflow("V2 header name length"))?;
        body.extend_from_slice(&name_len.to_le_bytes());
        encode_nullable_len(&mut body, header.value.as_deref(), "V2 header value")?;
        body.extend_from_slice(&header.name);
        if let Some(value) = &header.value {
            body.extend_from_slice(value);
        }
    }
    if body.len() + 4 > limits.max_record_bytes {
        return Err(StorageError::LimitExceeded {
            what: "V2 record bytes",
            actual: body.len() + 4,
            max: limits.max_record_bytes,
        });
    }
    let record_len = u32::try_from(body.len())
        .map_err(|_| StorageError::ArithmeticOverflow("V2 record length"))?;
    payload.extend_from_slice(&record_len.to_le_bytes());
    payload.extend_from_slice(&body);
    Ok(())
}

fn encode_retained_control_frame(
    control: &RetainedControlFrameV2,
    limits: StorageLimits,
) -> Result<Vec<u8>> {
    if control.raft_term.get() == 0 {
        return Err(StorageError::InvalidFormat(
            "V2 frame Raft term cannot be zero",
        ));
    }
    let body_len = u32::try_from(control.body.len())
        .map_err(|_| StorageError::ArithmeticOverflow("V2 retained control body length"))?;
    let mut payload = Vec::with_capacity(8 + control.body.len());
    payload.extend_from_slice(&(control.kind as u16).to_le_bytes());
    payload.extend_from_slice(&1_u16.to_le_bytes());
    payload.extend_from_slice(&body_len.to_le_bytes());
    payload.extend_from_slice(&control.body);
    encode_frame_envelope_v2(
        2,
        Compression::None,
        control.raft_term,
        control.raft_index,
        u64::MAX,
        0,
        0,
        0,
        RETAINED_CONTROL_SCHEMA_V2,
        &payload,
        payload.len(),
        limits,
    )
}

#[allow(clippy::too_many_arguments)]
fn encode_frame_envelope_v2(
    frame_type: u8,
    compression: Compression,
    raft_term: RaftTerm,
    raft_index: RaftIndex,
    base_stream_offset: u64,
    record_count: u32,
    min_timestamp_ms: i64,
    max_timestamp_ms: i64,
    payload_schema: u16,
    stored_payload: &[u8],
    uncompressed_len: usize,
    limits: StorageLimits,
) -> Result<Vec<u8>> {
    let total_len = FRAME_HEADER_LEN_V2
        .checked_add(stored_payload.len())
        .and_then(|value| value.checked_add(FRAME_CRC_LEN_V2))
        .ok_or(StorageError::ArithmeticOverflow("V2 frame length"))?;
    if total_len > limits.max_frame_bytes {
        return Err(StorageError::LimitExceeded {
            what: "V2 frame bytes",
            actual: total_len,
            max: limits.max_frame_bytes,
        });
    }
    let total_len_u32 = u32::try_from(total_len)
        .map_err(|_| StorageError::ArithmeticOverflow("V2 u32 frame length"))?;
    let stored_len_u32 = u32::try_from(stored_payload.len())
        .map_err(|_| StorageError::ArithmeticOverflow("V2 stored payload length"))?;
    let uncompressed_len_u32 = u32::try_from(uncompressed_len)
        .map_err(|_| StorageError::ArithmeticOverflow("V2 uncompressed payload length"))?;

    let mut out = Vec::with_capacity(total_len);
    out.extend_from_slice(&total_len_u32.to_le_bytes());
    out.extend_from_slice(FRAME_MAGIC_V2);
    out.extend_from_slice(&FORMAT_VERSION_V2.to_le_bytes());
    out.push(frame_type);
    out.push(compression as u8);
    out.extend_from_slice(&0_u32.to_le_bytes());
    out.extend_from_slice(&raft_term.get().to_le_bytes());
    out.extend_from_slice(&raft_index.get().to_le_bytes());
    out.extend_from_slice(&base_stream_offset.to_le_bytes());
    out.extend_from_slice(&record_count.to_le_bytes());
    out.extend_from_slice(&0_u32.to_le_bytes());
    out.extend_from_slice(&min_timestamp_ms.to_le_bytes());
    out.extend_from_slice(&max_timestamp_ms.to_le_bytes());
    out.extend_from_slice(&0_u64.to_le_bytes());
    out.extend_from_slice(&0_u32.to_le_bytes());
    out.extend_from_slice(&0_u32.to_le_bytes());
    out.extend_from_slice(&stored_len_u32.to_le_bytes());
    out.extend_from_slice(&uncompressed_len_u32.to_le_bytes());
    out.extend_from_slice(&payload_schema.to_le_bytes());
    out.extend_from_slice(&(FRAME_HEADER_LEN_V2 as u16).to_le_bytes());
    out.extend_from_slice(&0_u32.to_le_bytes());
    debug_assert_eq!(out.len(), FRAME_HEADER_LEN_V2);
    out.extend_from_slice(stored_payload);
    let crc = crc32c::crc32c(&out);
    out.extend_from_slice(&crc.to_le_bytes());
    Ok(out)
}

pub fn decode_retained_frame_v2(bytes: &[u8], limits: StorageLimits) -> Result<RetainedFrameV2> {
    if bytes.len() < MIN_FRAME_LEN_V2 {
        return Err(StorageError::InvalidFormat("V2 frame shorter than minimum"));
    }
    if bytes.len() > limits.max_frame_bytes {
        return Err(StorageError::LimitExceeded {
            what: "V2 frame bytes",
            actual: bytes.len(),
            max: limits.max_frame_bytes,
        });
    }
    if read_u32(bytes, 0)? as usize != bytes.len() {
        return Err(StorageError::InvalidFormat(
            "V2 total frame length mismatch",
        ));
    }
    if &bytes[4..8] != FRAME_MAGIC_V2 {
        return Err(StorageError::InvalidFormat("V2 frame magic mismatch"));
    }
    if read_u16(bytes, 8)? != FORMAT_VERSION_V2 {
        return Err(StorageError::UnsupportedVersion(read_u16(bytes, 8)?));
    }
    if read_u16(bytes, 90)? as usize != FRAME_HEADER_LEN_V2 {
        return Err(StorageError::InvalidFormat(
            "V2 frame header length mismatch",
        ));
    }
    if read_u32(bytes, 12)? != 0 || read_u32(bytes, 44)? != 0 || read_u32(bytes, 92)? != 0 {
        return Err(StorageError::InvalidFormat(
            "V2 frame attributes/reserved field is nonzero",
        ));
    }
    if read_u64(bytes, 64)? != 0 || read_u32(bytes, 72)? != 0 || read_u32(bytes, 76)? != 0 {
        return Err(StorageError::InvalidFormat(
            "V2 producer header fields must be zero",
        ));
    }
    let stored_len = read_u32(bytes, 80)? as usize;
    let actual_payload_len = bytes
        .len()
        .checked_sub(MIN_FRAME_LEN_V2)
        .ok_or(StorageError::ArithmeticOverflow("V2 payload length"))?;
    if stored_len != actual_payload_len {
        return Err(StorageError::InvalidFormat(
            "V2 stored payload length mismatch",
        ));
    }
    let uncompressed_len = read_u32(bytes, 84)? as usize;
    if uncompressed_len > limits.max_uncompressed_payload_bytes {
        return Err(StorageError::LimitExceeded {
            what: "V2 uncompressed payload bytes",
            actual: uncompressed_len,
            max: limits.max_uncompressed_payload_bytes,
        });
    }
    let expected_crc = read_u32(bytes, bytes.len() - 4)?;
    let actual_crc = crc32c::crc32c(&bytes[..bytes.len() - 4]);
    if expected_crc != actual_crc {
        return Err(StorageError::ChecksumMismatch {
            context: "V2 frame",
            expected: expected_crc,
            actual: actual_crc,
        });
    }
    let raft_term = RaftTerm::new(read_u64(bytes, 16)?);
    if raft_term.get() == 0 {
        return Err(StorageError::InvalidFormat(
            "V2 frame Raft term cannot be zero",
        ));
    }
    let raft_index = RaftIndex::new(read_u64(bytes, 24)?).ok_or(StorageError::InvalidFormat(
        "V2 frame Raft index cannot be zero",
    ))?;
    let compression = Compression::try_from(bytes[11])?;
    let stored_payload = &bytes[FRAME_HEADER_LEN_V2..bytes.len() - FRAME_CRC_LEN_V2];
    let decompressed;
    let payload = if compression == Compression::None {
        if stored_len != uncompressed_len {
            return Err(StorageError::InvalidFormat(
                "V2 none-compressed payload length mismatch",
            ));
        }
        stored_payload
    } else {
        decompressed = decompress_data_payload(compression, stored_payload, uncompressed_len)?;
        decompressed.as_slice()
    };

    match bytes[10] {
        3 => decode_sparse_data_frame(bytes, payload, compression, raft_term, raft_index, limits),
        2 => decode_retained_control(bytes, payload, compression, raft_term, raft_index),
        other => Err(StorageError::InvalidFormatOwned(format!(
            "unknown V2 frame type {other}",
        ))),
    }
}

fn decode_sparse_data_frame(
    header: &[u8],
    payload: &[u8],
    compression: Compression,
    raft_term: RaftTerm,
    raft_index: RaftIndex,
    limits: StorageLimits,
) -> Result<RetainedFrameV2> {
    if read_u16(header, 88)? != SPARSE_RECORD_SCHEMA_V2 {
        return Err(StorageError::InvalidFormat(
            "V2 sparse data payload schema must be 3",
        ));
    }
    let record_count = read_u32(header, 40)? as usize;
    if record_count == 0 {
        return Err(StorageError::InvalidFormat(
            "V2 sparse data record count cannot be zero",
        ));
    }
    let base_raw = read_u64(header, 32)?;
    if base_raw == u64::MAX {
        return Err(StorageError::InvalidFormat(
            "V2 sparse data cannot use control offset sentinel",
        ));
    }
    let base = StreamOffset::new(base_raw);
    let min_timestamp = read_i64(header, 48)?;
    let max_timestamp = read_i64(header, 56)?;
    if min_timestamp > max_timestamp {
        return Err(StorageError::InvalidFormat(
            "V2 sparse timestamp range is inverted",
        ));
    }
    let mut cursor = SliceCursor::new(payload);
    let mut records = Vec::with_capacity(record_count.min(1024));
    let mut previous_delta = None;
    for ordinal in 0..record_count {
        let record_len = cursor.read_u32()? as usize;
        if record_len + 4 > limits.max_record_bytes {
            return Err(StorageError::LimitExceeded {
                what: "V2 record bytes",
                actual: record_len + 4,
                max: limits.max_record_bytes,
            });
        }
        let record_bytes = cursor.take(record_len)?;
        let (delta, record) =
            decode_sparse_record(record_bytes, min_timestamp, max_timestamp, limits)?;
        if ordinal == 0 && delta != 0 {
            return Err(StorageError::InvalidFormat(
                "first V2 sparse offset delta must be zero",
            ));
        }
        if previous_delta.is_some_and(|previous| delta <= previous) {
            return Err(StorageError::InvalidFormat(
                "V2 sparse offset deltas must be strictly increasing",
            ));
        }
        let offset = base
            .checked_add(delta)
            .ok_or(StorageError::ArithmeticOverflow("V2 sparse stream offset"))?;
        records.push(SparseRetainedRecordV2 { offset, record });
        previous_delta = Some(delta);
    }
    if !cursor.is_empty() {
        return Err(StorageError::InvalidFormat(
            "V2 sparse payload contains trailing bytes",
        ));
    }
    validate_sparse_offsets(&records)?;
    Ok(RetainedFrameV2::SparseData(SparseRetainedBatchV2 {
        raft_term,
        raft_index,
        compression,
        records,
    }))
}

fn decode_sparse_record(
    bytes: &[u8],
    min_timestamp: i64,
    max_timestamp: i64,
    limits: StorageLimits,
) -> Result<(u64, DataRecord)> {
    let mut cursor = SliceCursor::new(bytes);
    let offset_delta = cursor.read_u64()?;
    let timestamp_delta = cursor.read_i64()?;
    let timestamp_ms = min_timestamp
        .checked_add(timestamp_delta)
        .ok_or(StorageError::ArithmeticOverflow("V2 record timestamp"))?;
    if timestamp_ms < min_timestamp || timestamp_ms > max_timestamp {
        return Err(StorageError::InvalidFormat(
            "V2 record timestamp outside frame range",
        ));
    }
    let key_len = cursor.read_i32()?;
    let value_len = cursor.read_i32()?;
    validate_nullable_len(key_len, "V2 record key")?;
    validate_nullable_len(value_len, "V2 record value")?;
    if key_len > limits.max_key_bytes as i32 {
        return Err(StorageError::LimitExceeded {
            what: "V2 record key bytes",
            actual: key_len as usize,
            max: limits.max_key_bytes,
        });
    }
    let header_count = cursor.read_u16()? as usize;
    if header_count > limits.max_header_count {
        return Err(StorageError::LimitExceeded {
            what: "V2 record header count",
            actual: header_count,
            max: limits.max_header_count,
        });
    }
    if cursor.read_u16()? != 0 {
        return Err(StorageError::InvalidFormat(
            "V2 record attributes must be zero",
        ));
    }
    let key = read_nullable_bytes(&mut cursor, key_len)?;
    let value = read_nullable_bytes(&mut cursor, value_len)?;
    let mut headers = Vec::with_capacity(header_count);
    for _ in 0..header_count {
        let name_len = cursor.read_u16()? as usize;
        if name_len > limits.max_header_name_bytes {
            return Err(StorageError::LimitExceeded {
                what: "V2 header name bytes",
                actual: name_len,
                max: limits.max_header_name_bytes,
            });
        }
        let value_len = cursor.read_i32()?;
        validate_nullable_len(value_len, "V2 header value")?;
        let name = cursor.take(name_len)?.to_vec();
        let value = read_nullable_bytes(&mut cursor, value_len)?;
        headers.push(UserHeader { name, value });
    }
    if !cursor.is_empty() {
        return Err(StorageError::InvalidFormat(
            "V2 record contains trailing bytes",
        ));
    }
    Ok((
        offset_delta,
        DataRecord {
            timestamp_ms,
            key,
            value,
            headers,
        },
    ))
}

fn decode_retained_control(
    header: &[u8],
    payload: &[u8],
    compression: Compression,
    raft_term: RaftTerm,
    raft_index: RaftIndex,
) -> Result<RetainedFrameV2> {
    if compression != Compression::None {
        return Err(StorageError::InvalidFormat(
            "V2 retained control must use compression=none",
        ));
    }
    if read_u16(header, 88)? != RETAINED_CONTROL_SCHEMA_V2
        || read_u64(header, 32)? != u64::MAX
        || read_u32(header, 40)? != 0
        || read_i64(header, 48)? != 0
        || read_i64(header, 56)? != 0
    {
        return Err(StorageError::InvalidFormat(
            "V2 retained control has invalid data-only fields",
        ));
    }
    let mut cursor = SliceCursor::new(payload);
    let kind = RetainedControlKindV2::try_from(cursor.read_u16()?)?;
    let version = cursor.read_u16()?;
    if version != 1 {
        return Err(StorageError::InvalidFormatOwned(format!(
            "unsupported P9 retained control version {version}",
        )));
    }
    let body_len = cursor.read_u32()? as usize;
    let body = cursor.take(body_len)?.to_vec();
    if !cursor.is_empty() {
        return Err(StorageError::InvalidFormat(
            "V2 retained control payload has trailing bytes",
        ));
    }
    Ok(RetainedFrameV2::Control(RetainedControlFrameV2 {
        raft_term,
        raft_index,
        kind,
        body,
    }))
}

pub fn scan_retained_segment_v2(
    path: &Path,
    limits: StorageLimits,
) -> Result<RetainedSegmentScanV2> {
    let header = read_retained_segment_header_v2(path)?;
    let mut file = File::open(path)?;
    let file_len = file.metadata()?.len();
    let mut position = SEGMENT_HEADER_LEN_V2 as u64;
    let mut frames = Vec::new();
    let mut previous_raft = None;
    let mut previous_stream = None;
    while position < file_len {
        let remaining = file_len - position;
        if remaining < FRAME_HEADER_LEN_V2 as u64 {
            return Err(StorageError::InvalidFormat(
                "immutable V2 retained segment has truncated frame header",
            ));
        }
        file.seek(SeekFrom::Start(position))?;
        let mut fixed = [0_u8; FRAME_HEADER_LEN_V2];
        file.read_exact(&mut fixed)?;
        let total_len = u32::from_le_bytes(fixed[0..4].try_into().expect("4 bytes")) as usize;
        if total_len < MIN_FRAME_LEN_V2 || total_len > limits.max_frame_bytes {
            return Err(StorageError::InvalidFormat(
                "immutable V2 retained frame length is invalid",
            ));
        }
        if remaining < total_len as u64 {
            return Err(StorageError::InvalidFormat(
                "immutable V2 retained frame is truncated",
            ));
        }
        let mut encoded = vec![0_u8; total_len];
        encoded[..FRAME_HEADER_LEN_V2].copy_from_slice(&fixed);
        file.read_exact(&mut encoded[FRAME_HEADER_LEN_V2..])?;
        let frame = decode_retained_frame_v2(&encoded, limits)?;
        let raft = frame.raft_index().get();
        if previous_raft.is_some_and(|previous| raft <= previous) {
            return Err(StorageError::InvalidFormat(
                "V2 retained frame Raft indexes must be strictly increasing",
            ));
        }
        if let RetainedFrameV2::SparseData(batch) = &frame {
            for retained in &batch.records {
                let offset = retained.offset.get();
                if previous_stream.is_some_and(|previous| offset <= previous) {
                    return Err(StorageError::InvalidFormat(
                        "V2 retained stream offsets must be strictly increasing",
                    ));
                }
                previous_stream = Some(offset);
            }
        }
        frames.push(RetainedScannedFrameV2 {
            position,
            encoded_len: total_len as u32,
            frame,
        });
        previous_raft = Some(raft);
        position = position
            .checked_add(total_len as u64)
            .ok_or(StorageError::ArithmeticOverflow("V2 scan position"))?;
    }
    Ok(RetainedSegmentScanV2 { header, frames })
}

pub fn read_retained_frame_at_v2(
    path: &Path,
    position: u64,
    limits: StorageLimits,
) -> Result<RetainedScannedFrameV2> {
    if position < SEGMENT_HEADER_LEN_V2 as u64 {
        return Err(StorageError::InvalidFormat(
            "V2 frame position overlaps segment header",
        ));
    }
    let mut file = File::open(path)?;
    let file_len = file.metadata()?.len();
    if position >= file_len || file_len - position < FRAME_HEADER_LEN_V2 as u64 {
        return Err(StorageError::InvalidFormat(
            "V2 frame position/header is outside segment",
        ));
    }
    file.seek(SeekFrom::Start(position))?;
    let mut fixed = [0_u8; FRAME_HEADER_LEN_V2];
    file.read_exact(&mut fixed)?;
    let total_len = u32::from_le_bytes(fixed[0..4].try_into().expect("4 bytes")) as usize;
    if total_len < MIN_FRAME_LEN_V2 || total_len > limits.max_frame_bytes {
        return Err(StorageError::InvalidFormat("V2 frame length is invalid"));
    }
    if file_len - position < total_len as u64 {
        return Err(StorageError::InvalidFormat("V2 frame is truncated"));
    }
    let mut encoded = vec![0_u8; total_len];
    encoded[..FRAME_HEADER_LEN_V2].copy_from_slice(&fixed);
    file.read_exact(&mut encoded[FRAME_HEADER_LEN_V2..])?;
    Ok(RetainedScannedFrameV2 {
        position,
        encoded_len: total_len as u32,
        frame: decode_retained_frame_v2(&encoded, limits)?,
    })
}

fn validate_sparse_offsets(records: &[SparseRetainedRecordV2]) -> Result<()> {
    let mut previous = None;
    for record in records {
        let offset = record.offset.get();
        if previous.is_some_and(|prior| offset <= prior) {
            return Err(StorageError::InvalidFormat(
                "V2 sparse offsets must be strictly increasing and unique",
            ));
        }
        previous = Some(offset);
    }
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
        .ok_or(StorageError::InvalidFormat("unexpected end of V2 bytes"))?;
    Ok(u16::from_le_bytes([data[0], data[1]]))
}

fn read_u32(bytes: &[u8], offset: usize) -> Result<u32> {
    let data = bytes
        .get(offset..offset + 4)
        .ok_or(StorageError::InvalidFormat("unexpected end of V2 bytes"))?;
    Ok(u32::from_le_bytes([data[0], data[1], data[2], data[3]]))
}

fn read_u64(bytes: &[u8], offset: usize) -> Result<u64> {
    let data = bytes
        .get(offset..offset + 8)
        .ok_or(StorageError::InvalidFormat("unexpected end of V2 bytes"))?;
    Ok(u64::from_le_bytes(data.try_into().expect("8 bytes")))
}

fn read_i64(bytes: &[u8], offset: usize) -> Result<i64> {
    let data = bytes
        .get(offset..offset + 8)
        .ok_or(StorageError::InvalidFormat("unexpected end of V2 bytes"))?;
    Ok(i64::from_le_bytes(data.try_into().expect("8 bytes")))
}

struct SliceCursor<'a> {
    bytes: &'a [u8],
    position: usize,
}

impl<'a> SliceCursor<'a> {
    const fn new(bytes: &'a [u8]) -> Self {
        Self { bytes, position: 0 }
    }

    fn take(&mut self, len: usize) -> Result<&'a [u8]> {
        let end = self
            .position
            .checked_add(len)
            .ok_or(StorageError::ArithmeticOverflow("V2 payload cursor"))?;
        let bytes = self
            .bytes
            .get(self.position..end)
            .ok_or(StorageError::InvalidFormat("V2 payload is truncated"))?;
        self.position = end;
        Ok(bytes)
    }

    fn read_u16(&mut self) -> Result<u16> {
        Ok(u16::from_le_bytes(
            self.take(2)?.try_into().expect("2 bytes"),
        ))
    }

    fn read_u32(&mut self) -> Result<u32> {
        Ok(u32::from_le_bytes(
            self.take(4)?.try_into().expect("4 bytes"),
        ))
    }

    fn read_u64(&mut self) -> Result<u64> {
        Ok(u64::from_le_bytes(
            self.take(8)?.try_into().expect("8 bytes"),
        ))
    }

    fn read_i32(&mut self) -> Result<i32> {
        Ok(i32::from_le_bytes(
            self.take(4)?.try_into().expect("4 bytes"),
        ))
    }

    fn read_i64(&mut self) -> Result<i64> {
        Ok(i64::from_le_bytes(
            self.take(8)?.try_into().expect("8 bytes"),
        ))
    }

    fn is_empty(&self) -> bool {
        self.position == self.bytes.len()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_support::decode_hex;

    fn header() -> RetainedSegmentHeaderV2 {
        RetainedSegmentHeaderV2 {
            topic_id: TopicId::new([0x92; 16]),
            partition_id: PartitionId::new(3),
            generation: 7,
            base_stream_offset: SegmentBaseOffset::new(10),
            base_raft_index: RaftIndex::new(8).unwrap(),
            created_at_ms: 1234,
        }
    }

    fn sparse(compression: Compression) -> RetainedFrameV2 {
        RetainedFrameV2::SparseData(SparseRetainedBatchV2 {
            raft_term: RaftTerm::new(4),
            raft_index: RaftIndex::new(9).unwrap(),
            compression,
            records: vec![
                SparseRetainedRecordV2 {
                    offset: StreamOffset::new(11),
                    record: DataRecord {
                        timestamp_ms: 100,
                        key: Some(b"k1".to_vec()),
                        value: Some(vec![b'A'; 256]),
                        headers: vec![],
                    },
                },
                SparseRetainedRecordV2 {
                    offset: StreamOffset::new(14),
                    record: DataRecord {
                        timestamp_ms: 105,
                        key: Some(b"k2".to_vec()),
                        value: Some(vec![b'B'; 256]),
                        headers: vec![],
                    },
                },
            ],
        })
    }

    #[test]
    fn p9c_v2_segment_header_round_trip_crc_flags_and_dispatch() {
        let header = header();
        let bytes = encode_retained_segment_header_v2(&header);
        assert_eq!(
            segment_format_version(&bytes).unwrap(),
            StorageFormatVersion::V2
        );
        assert_eq!(decode_retained_segment_header_v2(&bytes).unwrap(), header);

        let mut bad_flags = bytes;
        bad_flags[12..16].copy_from_slice(&0_u32.to_le_bytes());
        let crc = crc32c::crc32c(&bad_flags[..68]);
        bad_flags[68..72].copy_from_slice(&crc.to_le_bytes());
        assert!(decode_retained_segment_header_v2(&bad_flags).is_err());

        let mut unknown = bytes;
        unknown[12..16].copy_from_slice(&3_u32.to_le_bytes());
        let crc = crc32c::crc32c(&unknown[..68]);
        unknown[68..72].copy_from_slice(&crc.to_le_bytes());
        assert!(decode_retained_segment_header_v2(&unknown).is_err());
    }

    #[test]
    fn p9c_sparse_v2_none_lz4_zstd_round_trip_preserves_holes() {
        for compression in [Compression::None, Compression::Lz4, Compression::Zstd] {
            let frame = sparse(compression);
            let encoded = encode_retained_frame_v2(&frame, StorageLimits::default()).unwrap();
            let decoded = decode_retained_frame_v2(&encoded, StorageLimits::default()).unwrap();
            let RetainedFrameV2::SparseData(decoded) = decoded else {
                panic!("expected sparse V2 data");
            };
            assert_eq!(decoded.records[0].offset.get(), 11);
            assert_eq!(decoded.records[1].offset.get(), 14);
            assert_eq!(
                decoded.records[0].record.value.as_ref().unwrap(),
                &vec![b'A'; 256]
            );
            assert_eq!(
                decoded.records[1].record.value.as_ref().unwrap(),
                &vec![b'B'; 256]
            );
        }
    }

    #[test]
    fn p9c_sparse_v2_rejects_duplicate_or_nonincreasing_offsets_and_producer_fields() {
        let RetainedFrameV2::SparseData(mut batch) = sparse(Compression::None) else {
            unreachable!();
        };
        batch.records[1].offset = batch.records[0].offset;
        assert!(
            encode_retained_frame_v2(
                &RetainedFrameV2::SparseData(batch),
                StorageLimits::default(),
            )
            .is_err()
        );

        let mut encoded =
            encode_retained_frame_v2(&sparse(Compression::None), StorageLimits::default()).unwrap();
        encoded[64..72].copy_from_slice(&1_u64.to_le_bytes());
        let crc = crc32c::crc32c(&encoded[..encoded.len() - 4]);
        let end = encoded.len();
        encoded[end - 4..].copy_from_slice(&crc.to_le_bytes());
        assert!(decode_retained_frame_v2(&encoded, StorageLimits::default()).is_err());
    }

    #[test]
    fn p9c_v2_corruption_truncation_schema_and_offset_overflow_fail_closed() {
        let encoded =
            encode_retained_frame_v2(&sparse(Compression::None), StorageLimits::default()).unwrap();
        for end in 0..encoded.len() {
            assert!(
                decode_retained_frame_v2(&encoded[..end], StorageLimits::default()).is_err(),
                "every V2 frame truncation boundary must fail: {end}",
            );
        }

        let mut crc_corrupt = encoded.clone();
        crc_corrupt[FRAME_HEADER_LEN_V2 + 8] ^= 0x40;
        assert!(matches!(
            decode_retained_frame_v2(&crc_corrupt, StorageLimits::default()),
            Err(StorageError::ChecksumMismatch {
                context: "V2 frame",
                ..
            })
        ));

        let repair_crc = |bytes: &mut Vec<u8>| {
            let crc_offset = bytes.len() - 4;
            let crc = crc32c::crc32c(&bytes[..crc_offset]);
            bytes[crc_offset..].copy_from_slice(&crc.to_le_bytes());
        };

        let mut first_delta_nonzero = encoded.clone();
        first_delta_nonzero[FRAME_HEADER_LEN_V2 + 4..FRAME_HEADER_LEN_V2 + 12]
            .copy_from_slice(&1_u64.to_le_bytes());
        repair_crc(&mut first_delta_nonzero);
        assert!(decode_retained_frame_v2(&first_delta_nonzero, StorageLimits::default()).is_err());

        let mut overflow = encoded.clone();
        overflow[32..40].copy_from_slice(&(u64::MAX - 1).to_le_bytes());
        repair_crc(&mut overflow);
        assert!(decode_retained_frame_v2(&overflow, StorageLimits::default()).is_err());

        let mut unknown_compression = encoded.clone();
        unknown_compression[11] = 3;
        repair_crc(&mut unknown_compression);
        assert!(decode_retained_frame_v2(&unknown_compression, StorageLimits::default()).is_err());

        let mut unknown_schema = encoded;
        unknown_schema[88..90].copy_from_slice(&99_u16.to_le_bytes());
        repair_crc(&mut unknown_schema);
        assert!(decode_retained_frame_v2(&unknown_schema, StorageLimits::default()).is_err());
    }

    #[test]
    fn p9c_retained_control_accepts_only_frozen_kind_and_version() {
        let frame = RetainedFrameV2::Control(RetainedControlFrameV2 {
            raft_term: RaftTerm::new(2),
            raft_index: RaftIndex::new(12).unwrap(),
            kind: RetainedControlKindV2::ProducerStateCheckpoint,
            body: b"checkpoint".to_vec(),
        });
        let encoded = encode_retained_frame_v2(&frame, StorageLimits::default()).unwrap();
        assert_eq!(
            decode_retained_frame_v2(&encoded, StorageLimits::default()).unwrap(),
            frame
        );

        let mut unknown_kind = encoded.clone();
        unknown_kind[96..98].copy_from_slice(&99_u16.to_le_bytes());
        let crc = crc32c::crc32c(&unknown_kind[..unknown_kind.len() - 4]);
        let end = unknown_kind.len();
        unknown_kind[end - 4..].copy_from_slice(&crc.to_le_bytes());
        assert!(decode_retained_frame_v2(&unknown_kind, StorageLimits::default()).is_err());

        let mut unknown_version = encoded;
        unknown_version[98..100].copy_from_slice(&2_u16.to_le_bytes());
        let crc = crc32c::crc32c(&unknown_version[..unknown_version.len() - 4]);
        let end = unknown_version.len();
        unknown_version[end - 4..].copy_from_slice(&crc.to_le_bytes());
        assert!(decode_retained_frame_v2(&unknown_version, StorageLimits::default()).is_err());
    }

    #[test]
    fn p9c_v2_golden_vectors_are_immutable() {
        let header = header();
        assert_eq!(
            encode_retained_segment_header_v2(&header),
            decode_hex(include_str!("../tests/golden/segment_header_v2.hex")).as_slice(),
        );

        for (compression, golden) in [
            (
                Compression::None,
                include_str!("../tests/golden/sparse_none_v2.hex"),
            ),
            (
                Compression::Lz4,
                include_str!("../tests/golden/sparse_lz4_v2.hex"),
            ),
            (
                Compression::Zstd,
                include_str!("../tests/golden/sparse_zstd_v2.hex"),
            ),
        ] {
            assert_eq!(
                encode_retained_frame_v2(&sparse(compression), StorageLimits::default()).unwrap(),
                decode_hex(golden),
            );
        }

        let control = RetainedFrameV2::Control(RetainedControlFrameV2 {
            raft_term: RaftTerm::new(2),
            raft_index: RaftIndex::new(12).unwrap(),
            kind: RetainedControlKindV2::ProducerStateCheckpoint,
            body: b"checkpoint".to_vec(),
        });
        assert_eq!(
            encode_retained_frame_v2(&control, StorageLimits::default()).unwrap(),
            decode_hex(include_str!("../tests/golden/retained_control_v2.hex")),
        );
    }
}
