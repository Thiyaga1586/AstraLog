use astralog_common::{
    ProducerAuthority, ProducerEpoch, ProducerId, ProducerSequence, StreamOffset,
};
use astralog_storage::{DataRecord, FRAME_CRC_LEN, FRAME_HEADER_LEN, StorageLimits, UserHeader};

use crate::{
    MAX_PARTITION_PRODUCER_ENTRIES, MAX_PRODUCER_SNAPSHOT_BYTES, ProducerSnapshotV1,
    ReplicationError, Result,
    producer_snapshot::{decode_producer_snapshot_with_limit, encode_producer_snapshot_with_limit},
};

const COMMAND_VERSION: u16 = 1;
const COMMAND_APPEND_BATCH: u16 = 1;
const COMMAND_VERSION_V2: u16 = 2;
const COMMAND_APPEND_IDEMPOTENT_BATCH: u16 = 1;
const COMMAND_VERSION_V3: u16 = 3;
const COMMAND_ADVANCE_LOG_START_OFFSET: u16 = 1;
const COMMAND_PRODUCER_STATE_CHECKPOINT: u16 = 2;
const COMMAND_ADVANCE_COMPACTION_SAFE_OFFSET: u16 = 3;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct CommandLimits {
    pub max_command_bytes: usize,
    pub max_records: usize,
    pub storage: StorageLimits,
}

impl Default for CommandLimits {
    fn default() -> Self {
        Self {
            max_command_bytes: 16 * 1024 * 1024,
            max_records: 10_000,
            storage: StorageLimits::default(),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ApplicationCommand {
    pub base_stream_offset: StreamOffset,
    pub records: Vec<DataRecord>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ApplicationCommandV2 {
    pub base_stream_offset: StreamOffset,
    pub authority: ProducerAuthority,
    pub base_sequence: ProducerSequence,
    pub records: Vec<DataRecord>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ApplicationMaintenanceCommandV1 {
    pub new_log_start_offset: StreamOffset,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ApplicationProducerStateCheckpointCommandV1 {
    pub checkpoint: ProducerSnapshotV1,
}

/// Cluster authority is gained only once this maintenance entry commits in
/// the ordinary partition Raft application history.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ApplicationCompactionSafeCommandV1 {
    pub new_compaction_safe_offset: StreamOffset,
}

pub fn encode_application_compaction_safe_command_v1(
    command: ApplicationCompactionSafeCommandV1,
) -> Vec<u8> {
    let mut out = Vec::with_capacity(12);
    put_u16(&mut out, COMMAND_VERSION_V3);
    put_u16(&mut out, COMMAND_ADVANCE_COMPACTION_SAFE_OFFSET);
    put_u64(&mut out, command.new_compaction_safe_offset.get());
    out
}

pub fn decode_application_compaction_safe_command_v1(
    bytes: &[u8],
) -> Result<ApplicationCompactionSafeCommandV1> {
    if bytes.len() != 12 {
        return Err(ReplicationError::Invalid(
            "P9 compaction-safe command must be exactly twelve bytes",
        ));
    }
    let mut reader = Reader::new(bytes);
    if reader.u16()? != COMMAND_VERSION_V3
        || reader.u16()? != COMMAND_ADVANCE_COMPACTION_SAFE_OFFSET
    {
        return Err(ReplicationError::Invalid(
            "unknown P9 compaction-safe maintenance command",
        ));
    }
    let command = ApplicationCompactionSafeCommandV1 {
        new_compaction_safe_offset: StreamOffset::new(reader.u64()?),
    };
    if !reader.is_empty() || encode_application_compaction_safe_command_v1(command) != bytes {
        return Err(ReplicationError::Invalid(
            "noncanonical P9 compaction-safe maintenance command",
        ));
    }
    Ok(command)
}

pub fn encode_application_maintenance_command_v1(
    command: ApplicationMaintenanceCommandV1,
) -> Vec<u8> {
    let mut out = Vec::with_capacity(12);
    put_u16(&mut out, COMMAND_VERSION_V3);
    put_u16(&mut out, COMMAND_ADVANCE_LOG_START_OFFSET);
    put_u64(&mut out, command.new_log_start_offset.get());
    out
}

pub fn decode_application_maintenance_command_v1(
    bytes: &[u8],
) -> Result<ApplicationMaintenanceCommandV1> {
    if bytes.len() != 12 {
        return Err(ReplicationError::Invalid(
            "P9 maintenance command must be exactly twelve bytes",
        ));
    }
    let mut reader = Reader::new(bytes);
    if reader.u16()? != COMMAND_VERSION_V3 {
        return Err(ReplicationError::Invalid(
            "unsupported P9 maintenance command version",
        ));
    }
    if reader.u16()? != COMMAND_ADVANCE_LOG_START_OFFSET {
        return Err(ReplicationError::Invalid(
            "unknown P9 maintenance command kind",
        ));
    }
    let command = ApplicationMaintenanceCommandV1 {
        new_log_start_offset: StreamOffset::new(reader.u64()?),
    };
    if !reader.is_empty() {
        return Err(ReplicationError::Invalid(
            "trailing bytes in P9 maintenance command",
        ));
    }
    if encode_application_maintenance_command_v1(command) != bytes {
        return Err(ReplicationError::Invalid(
            "P9 maintenance command is not canonical",
        ));
    }
    Ok(command)
}

pub fn encode_application_producer_state_checkpoint_command_v1(
    command: &ApplicationProducerStateCheckpointCommandV1,
    limits: CommandLimits,
) -> Result<Vec<u8>> {
    if command.checkpoint.states.len() > MAX_PARTITION_PRODUCER_ENTRIES {
        return Err(ReplicationError::LimitExceeded {
            what: "P9 producer checkpoint entries",
            actual: command.checkpoint.states.len(),
            max: MAX_PARTITION_PRODUCER_ENTRIES,
        });
    }
    let command_body_limit =
        limits
            .max_command_bytes
            .checked_sub(4)
            .ok_or(ReplicationError::Invalid(
                "application command byte ceiling is too small for P9 maintenance header",
            ))?;
    let storage_body_limit = limits
        .storage
        .max_frame_bytes
        .checked_sub(FRAME_HEADER_LEN + FRAME_CRC_LEN)
        .ok_or(ReplicationError::Invalid(
            "Storage frame ceiling is too small for P9 producer checkpoint control",
        ))?;
    let body_limit = command_body_limit
        .min(MAX_PRODUCER_SNAPSHOT_BYTES)
        .min(storage_body_limit);
    let body = encode_producer_snapshot_with_limit(&command.checkpoint, body_limit)?;
    let mut out = Vec::with_capacity(body.len().saturating_add(4));
    put_u16(&mut out, COMMAND_VERSION_V3);
    put_u16(&mut out, COMMAND_PRODUCER_STATE_CHECKPOINT);
    out.extend_from_slice(&body);
    if out.len() > limits.max_command_bytes {
        return Err(ReplicationError::LimitExceeded {
            what: "application command bytes",
            actual: out.len(),
            max: limits.max_command_bytes,
        });
    }
    Ok(out)
}

pub fn decode_application_producer_state_checkpoint_command_v1(
    bytes: &[u8],
    limits: CommandLimits,
) -> Result<ApplicationProducerStateCheckpointCommandV1> {
    if bytes.len() > limits.max_command_bytes {
        return Err(ReplicationError::LimitExceeded {
            what: "application command bytes",
            actual: bytes.len(),
            max: limits.max_command_bytes,
        });
    }
    if bytes.len() < 4 {
        return Err(ReplicationError::Invalid(
            "P9 producer checkpoint command is truncated",
        ));
    }
    let mut reader = Reader::new(bytes);
    if reader.u16()? != COMMAND_VERSION_V3 {
        return Err(ReplicationError::Invalid(
            "unsupported P9 maintenance command version",
        ));
    }
    if reader.u16()? != COMMAND_PRODUCER_STATE_CHECKPOINT {
        return Err(ReplicationError::Invalid(
            "unknown P9 producer checkpoint maintenance kind",
        ));
    }
    let command_body_limit =
        limits
            .max_command_bytes
            .checked_sub(4)
            .ok_or(ReplicationError::Invalid(
                "application command byte ceiling is too small for P9 maintenance header",
            ))?;
    let storage_body_limit = limits
        .storage
        .max_frame_bytes
        .checked_sub(FRAME_HEADER_LEN + FRAME_CRC_LEN)
        .ok_or(ReplicationError::Invalid(
            "Storage frame ceiling is too small for P9 producer checkpoint control",
        ))?;
    let body_limit = command_body_limit
        .min(MAX_PRODUCER_SNAPSHOT_BYTES)
        .min(storage_body_limit);
    let checkpoint = decode_producer_snapshot_with_limit(&bytes[4..], body_limit)?;
    if checkpoint.states.len() > MAX_PARTITION_PRODUCER_ENTRIES {
        return Err(ReplicationError::LimitExceeded {
            what: "P9 producer checkpoint entries",
            actual: checkpoint.states.len(),
            max: MAX_PARTITION_PRODUCER_ENTRIES,
        });
    }
    let command = ApplicationProducerStateCheckpointCommandV1 { checkpoint };
    if encode_application_producer_state_checkpoint_command_v1(&command, limits)? != bytes {
        return Err(ReplicationError::Invalid(
            "P9 producer checkpoint command is not canonical",
        ));
    }
    Ok(command)
}

pub fn encode_application_command_v2(
    command: &ApplicationCommandV2,
    limits: CommandLimits,
) -> Result<Vec<u8>> {
    if command.records.is_empty() || command.records.len() > limits.max_records {
        return Err(ReplicationError::Invalid(
            "idempotent application command requires a bounded non-zero record count",
        ));
    }
    let record_count = u32::try_from(command.records.len())
        .map_err(|_| ReplicationError::Invalid("record count overflow"))?;
    if command
        .base_sequence
        .checked_next_after(record_count)
        .is_none()
    {
        return Err(ReplicationError::Invalid(
            "idempotent application sequence range cannot wrap",
        ));
    }
    if command.records.iter().any(|record| record.timestamp_ms < 0) {
        return Err(ReplicationError::Invalid(
            "idempotent application command requires explicit non-negative timestamps",
        ));
    }

    let mut out = Vec::new();
    put_u16(&mut out, COMMAND_VERSION_V2);
    put_u16(&mut out, COMMAND_APPEND_IDEMPOTENT_BATCH);
    put_u64(&mut out, command.base_stream_offset.get());
    put_u64(&mut out, command.authority.producer_id.get());
    put_u32(&mut out, command.authority.producer_epoch.get());
    put_u32(&mut out, command.base_sequence.get());
    put_u32(&mut out, record_count);
    encode_records(&mut out, &command.records, limits)?;
    if out.len() > limits.max_command_bytes {
        return Err(ReplicationError::LimitExceeded {
            what: "application command bytes",
            actual: out.len(),
            max: limits.max_command_bytes,
        });
    }
    Ok(out)
}

pub fn decode_application_command_v2(
    bytes: &[u8],
    limits: CommandLimits,
) -> Result<ApplicationCommandV2> {
    if bytes.len() > limits.max_command_bytes {
        return Err(ReplicationError::LimitExceeded {
            what: "application command bytes",
            actual: bytes.len(),
            max: limits.max_command_bytes,
        });
    }
    let mut reader = Reader::new(bytes);
    if reader.u16()? != COMMAND_VERSION_V2 {
        return Err(ReplicationError::Invalid(
            "unsupported idempotent application command version",
        ));
    }
    if reader.u16()? != COMMAND_APPEND_IDEMPOTENT_BATCH {
        return Err(ReplicationError::Invalid(
            "unknown idempotent application command kind",
        ));
    }
    let base_stream_offset = StreamOffset::new(reader.u64()?);
    let producer_id = ProducerId::new(reader.u64()?)
        .ok_or(ReplicationError::Invalid("producer id zero is reserved"))?;
    let producer_epoch = ProducerEpoch::new(reader.u32()?)
        .ok_or(ReplicationError::Invalid("producer epoch zero is reserved"))?;
    let base_sequence = ProducerSequence::new(reader.u32()?);
    let count = usize::try_from(reader.u32()?)
        .map_err(|_| ReplicationError::Invalid("record count overflow"))?;
    if count == 0 || count > limits.max_records {
        return Err(ReplicationError::Invalid(
            "invalid idempotent application record count",
        ));
    }
    let count_u32 =
        u32::try_from(count).map_err(|_| ReplicationError::Invalid("record count overflow"))?;
    if base_sequence.checked_next_after(count_u32).is_none() {
        return Err(ReplicationError::Invalid(
            "idempotent application sequence range cannot wrap",
        ));
    }
    let records = decode_records(&mut reader, count, limits)?;
    if !reader.is_empty() {
        return Err(ReplicationError::Invalid(
            "trailing bytes in idempotent application command",
        ));
    }
    let command = ApplicationCommandV2 {
        base_stream_offset,
        authority: ProducerAuthority::new(producer_id, producer_epoch),
        base_sequence,
        records,
    };
    if encode_application_command_v2(&command, limits)? != bytes {
        return Err(ReplicationError::Invalid(
            "idempotent application command is not canonical",
        ));
    }
    Ok(command)
}

pub fn encode_application_command(
    command: &ApplicationCommand,
    limits: CommandLimits,
) -> Result<Vec<u8>> {
    if command.records.is_empty() {
        return Err(ReplicationError::Invalid(
            "application command requires at least one record",
        ));
    }
    if command.records.len() > limits.max_records {
        return Err(ReplicationError::LimitExceeded {
            what: "record count",
            actual: command.records.len(),
            max: limits.max_records,
        });
    }

    let mut out = Vec::new();
    put_u16(&mut out, COMMAND_VERSION);
    put_u16(&mut out, COMMAND_APPEND_BATCH);
    put_u64(&mut out, command.base_stream_offset.get());
    put_u32(
        &mut out,
        u32::try_from(command.records.len())
            .map_err(|_| ReplicationError::Invalid("record count overflow"))?,
    );

    encode_records(&mut out, &command.records, limits)?;
    Ok(out)
}

pub fn decode_application_command(
    bytes: &[u8],
    limits: CommandLimits,
) -> Result<ApplicationCommand> {
    if bytes.len() > limits.max_command_bytes {
        return Err(ReplicationError::LimitExceeded {
            what: "application command bytes",
            actual: bytes.len(),
            max: limits.max_command_bytes,
        });
    }
    let mut reader = Reader::new(bytes);
    if reader.u16()? != COMMAND_VERSION {
        return Err(ReplicationError::Invalid(
            "unsupported application command version",
        ));
    }
    if reader.u16()? != COMMAND_APPEND_BATCH {
        return Err(ReplicationError::Invalid(
            "unknown application command kind",
        ));
    }
    let base_stream_offset = StreamOffset::new(reader.u64()?);
    let count = usize::try_from(reader.u32()?)
        .map_err(|_| ReplicationError::Invalid("record count overflow"))?;
    if count == 0 || count > limits.max_records {
        return Err(ReplicationError::Invalid(
            "invalid application record count",
        ));
    }

    let records = decode_records(&mut reader, count, limits)?;
    if !reader.is_empty() {
        return Err(ReplicationError::Invalid(
            "trailing bytes in application command",
        ));
    }
    let command = ApplicationCommand {
        base_stream_offset,
        records,
    };
    let canonical = encode_application_command(&command, limits)?;
    if canonical != bytes {
        return Err(ReplicationError::Invalid(
            "application command is not canonical",
        ));
    }
    Ok(command)
}

fn encode_records(out: &mut Vec<u8>, records: &[DataRecord], limits: CommandLimits) -> Result<()> {
    for record in records {
        validate_record(record, limits)?;
        put_i64(out, record.timestamp_ms);
        put_nullable_len(out, record.key.as_deref())?;
        put_nullable_len(out, record.value.as_deref())?;
        put_u16(
            out,
            u16::try_from(record.headers.len())
                .map_err(|_| ReplicationError::Invalid("header count overflow"))?,
        );
        for header in &record.headers {
            put_u16(
                out,
                u16::try_from(header.name.len())
                    .map_err(|_| ReplicationError::Invalid("header name length overflow"))?,
            );
            put_nullable_len(out, header.value.as_deref())?;
            out.extend_from_slice(&header.name);
            if let Some(value) = &header.value {
                out.extend_from_slice(value);
            }
        }
        if let Some(key) = &record.key {
            out.extend_from_slice(key);
        }
        if let Some(value) = &record.value {
            out.extend_from_slice(value);
        }
        if out.len() > limits.max_command_bytes {
            return Err(ReplicationError::LimitExceeded {
                what: "application command bytes",
                actual: out.len(),
                max: limits.max_command_bytes,
            });
        }
    }
    Ok(())
}

fn decode_records(
    reader: &mut Reader<'_>,
    count: usize,
    limits: CommandLimits,
) -> Result<Vec<DataRecord>> {
    let mut records = Vec::with_capacity(count);
    for _ in 0..count {
        let timestamp_ms = reader.i64()?;
        let key_len = reader.nullable_len(limits.storage.max_key_bytes, "record key")?;
        let value_len = reader.nullable_len(limits.storage.max_record_bytes, "record value")?;
        let header_count = usize::from(reader.u16()?);
        if header_count > limits.storage.max_header_count {
            return Err(ReplicationError::LimitExceeded {
                what: "record header count",
                actual: header_count,
                max: limits.storage.max_header_count,
            });
        }
        let mut headers = Vec::with_capacity(header_count);
        for _ in 0..header_count {
            let name_len = usize::from(reader.u16()?);
            if name_len > limits.storage.max_header_name_bytes {
                return Err(ReplicationError::LimitExceeded {
                    what: "header name bytes",
                    actual: name_len,
                    max: limits.storage.max_header_name_bytes,
                });
            }
            let value_len = reader.nullable_len(limits.storage.max_record_bytes, "header value")?;
            let name = reader.bytes(name_len)?.to_vec();
            let value = value_len
                .map(|len| reader.bytes(len).map(ToOwned::to_owned))
                .transpose()?;
            headers.push(UserHeader { name, value });
        }
        let key = key_len
            .map(|len| reader.bytes(len).map(ToOwned::to_owned))
            .transpose()?;
        let value = value_len
            .map(|len| reader.bytes(len).map(ToOwned::to_owned))
            .transpose()?;
        let record = DataRecord {
            timestamp_ms,
            key,
            value,
            headers,
        };
        validate_record(&record, limits)?;
        records.push(record);
    }
    Ok(records)
}

fn validate_record(record: &DataRecord, limits: CommandLimits) -> Result<()> {
    if record
        .key
        .as_ref()
        .is_some_and(|key| key.len() > limits.storage.max_key_bytes)
    {
        return Err(ReplicationError::LimitExceeded {
            what: "record key bytes",
            actual: record.key.as_ref().map_or(0, Vec::len),
            max: limits.storage.max_key_bytes,
        });
    }
    if record.headers.len() > limits.storage.max_header_count {
        return Err(ReplicationError::LimitExceeded {
            what: "record header count",
            actual: record.headers.len(),
            max: limits.storage.max_header_count,
        });
    }
    let mut estimated = 32_usize
        .saturating_add(record.key.as_ref().map_or(0, Vec::len))
        .saturating_add(record.value.as_ref().map_or(0, Vec::len));
    for header in &record.headers {
        if header.name.len() > limits.storage.max_header_name_bytes {
            return Err(ReplicationError::LimitExceeded {
                what: "header name bytes",
                actual: header.name.len(),
                max: limits.storage.max_header_name_bytes,
            });
        }
        estimated = estimated
            .saturating_add(8)
            .saturating_add(header.name.len())
            .saturating_add(header.value.as_ref().map_or(0, Vec::len));
    }
    if estimated > limits.storage.max_record_bytes {
        return Err(ReplicationError::LimitExceeded {
            what: "record bytes",
            actual: estimated,
            max: limits.storage.max_record_bytes,
        });
    }
    Ok(())
}

fn put_nullable_len(out: &mut Vec<u8>, value: Option<&[u8]>) -> Result<()> {
    match value {
        None => out.extend_from_slice(&(-1_i32).to_le_bytes()),
        Some(bytes) => {
            let len = i32::try_from(bytes.len())
                .map_err(|_| ReplicationError::Invalid("nullable length overflow"))?;
            out.extend_from_slice(&len.to_le_bytes());
        }
    }
    Ok(())
}

fn put_u16(out: &mut Vec<u8>, value: u16) {
    out.extend_from_slice(&value.to_le_bytes());
}

fn put_u32(out: &mut Vec<u8>, value: u32) {
    out.extend_from_slice(&value.to_le_bytes());
}

fn put_u64(out: &mut Vec<u8>, value: u64) {
    out.extend_from_slice(&value.to_le_bytes());
}

fn put_i64(out: &mut Vec<u8>, value: i64) {
    out.extend_from_slice(&value.to_le_bytes());
}

struct Reader<'a> {
    bytes: &'a [u8],
    position: usize,
}

impl<'a> Reader<'a> {
    fn new(bytes: &'a [u8]) -> Self {
        Self { bytes, position: 0 }
    }

    fn bytes(&mut self, len: usize) -> Result<&'a [u8]> {
        let end = self
            .position
            .checked_add(len)
            .ok_or(ReplicationError::Invalid("reader length overflow"))?;
        let value = self
            .bytes
            .get(self.position..end)
            .ok_or(ReplicationError::Invalid("truncated application command"))?;
        self.position = end;
        Ok(value)
    }

    fn u16(&mut self) -> Result<u16> {
        Ok(u16::from_le_bytes(
            self.bytes(2)?.try_into().expect("2-byte slice"),
        ))
    }

    fn u32(&mut self) -> Result<u32> {
        Ok(u32::from_le_bytes(
            self.bytes(4)?.try_into().expect("4-byte slice"),
        ))
    }

    fn u64(&mut self) -> Result<u64> {
        Ok(u64::from_le_bytes(
            self.bytes(8)?.try_into().expect("8-byte slice"),
        ))
    }

    fn i64(&mut self) -> Result<i64> {
        Ok(i64::from_le_bytes(
            self.bytes(8)?.try_into().expect("8-byte slice"),
        ))
    }

    fn nullable_len(&mut self, max: usize, what: &'static str) -> Result<Option<usize>> {
        let raw = i32::from_le_bytes(self.bytes(4)?.try_into().expect("4-byte slice"));
        if raw == -1 {
            return Ok(None);
        }
        if raw < 0 {
            return Err(ReplicationError::Invalid(
                "invalid negative nullable length",
            ));
        }
        let len = usize::try_from(raw)
            .map_err(|_| ReplicationError::Invalid("nullable length overflow"))?;
        if len > max {
            return Err(ReplicationError::LimitExceeded {
                what,
                actual: len,
                max,
            });
        }
        Ok(Some(len))
    }

    fn is_empty(&self) -> bool {
        self.position == self.bytes.len()
    }
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use astralog_common::{PartitionId, RaftIndex, TopicId};

    use super::*;
    use crate::{CommittedBatchV1, PartitionProducerStateV1};

    #[test]
    fn p9g_compaction_safe_command_roundtrip_strict_length_and_version() {
        let command = ApplicationCompactionSafeCommandV1 {
            new_compaction_safe_offset: StreamOffset::new(0x0102_0304_0506_0708),
        };
        let encoded = encode_application_compaction_safe_command_v1(command);
        assert_eq!(encoded.len(), 12);
        assert_eq!(&encoded[..4], &[3, 0, 3, 0]);
        assert_eq!(
            decode_application_compaction_safe_command_v1(&encoded).unwrap(),
            command
        );
        assert!(decode_application_compaction_safe_command_v1(&encoded[..11]).is_err());
        let mut unknown = encoded.clone();
        unknown[2] = 4;
        assert!(decode_application_compaction_safe_command_v1(&unknown).is_err());
    }

    fn decode_hex(input: &str) -> Vec<u8> {
        let (pairs, remainder) = input.as_bytes().as_chunks::<2>();
        assert!(remainder.is_empty());
        pairs
            .iter()
            .map(|pair| u8::from_str_radix(core::str::from_utf8(pair).unwrap(), 16).unwrap())
            .collect()
    }

    #[test]
    fn application_command_round_trip_is_canonical() {
        let command = ApplicationCommand {
            base_stream_offset: StreamOffset::new(42),
            records: vec![DataRecord {
                timestamp_ms: 100,
                key: Some(b"key".to_vec()),
                value: Some(b"value".to_vec()),
                headers: vec![UserHeader {
                    name: b"kind".to_vec(),
                    value: Some(b"event".to_vec()),
                }],
            }],
        };
        let bytes = encode_application_command(&command, CommandLimits::default()).unwrap();
        assert_eq!(
            decode_application_command(&bytes, CommandLimits::default()).unwrap(),
            command
        );
    }

    fn checkpoint_state(producer: u64, offset: u64, raft: u64) -> PartitionProducerStateV1 {
        let authority = ProducerAuthority::new(
            ProducerId::new(producer).unwrap(),
            ProducerEpoch::new(1).unwrap(),
        );
        PartitionProducerStateV1::from_first_committed(
            CommittedBatchV1::new(
                authority,
                ProducerSequence::ZERO,
                1,
                StreamOffset::new(offset),
                RaftIndex::new(raft).unwrap(),
                [u8::try_from(producer & 0xff).unwrap(); 32],
            )
            .unwrap(),
        )
        .unwrap()
    }

    #[test]
    fn p9f_producer_checkpoint_command_is_canonical_bounded_and_preserves_p7_retry_state() {
        let mut states = BTreeMap::new();
        states.insert(ProducerId::new(7).unwrap(), checkpoint_state(7, 41, 9));
        let checkpoint =
            ProducerSnapshotV1::new(TopicId::new([0x9f; 16]), PartitionId::new(3), 11, states)
                .unwrap();
        let command = ApplicationProducerStateCheckpointCommandV1 { checkpoint };
        let bytes = encode_application_producer_state_checkpoint_command_v1(
            &command,
            CommandLimits::default(),
        )
        .unwrap();
        assert_eq!(&bytes[..4], &[3, 0, 2, 0]);
        assert_eq!(
            decode_application_producer_state_checkpoint_command_v1(
                &bytes,
                CommandLimits::default(),
            )
            .unwrap(),
            command
        );
        let state = &command.checkpoint.states[&ProducerId::new(7).unwrap()];
        let recent = state.recent_committed_batches().back().unwrap();
        assert_eq!(recent.base_stream_offset, StreamOffset::new(41));
        assert_eq!(recent.last_stream_offset, StreamOffset::new(41));
        assert_eq!(recent.raft_index, RaftIndex::new(9).unwrap());
        assert_eq!(recent.canonical_digest, [7; 32]);

        let mut at_limit = BTreeMap::new();
        for producer in 1..=u64::try_from(MAX_PARTITION_PRODUCER_ENTRIES).unwrap() {
            at_limit.insert(
                ProducerId::new(producer).unwrap(),
                checkpoint_state(producer, producer - 1, 1),
            );
        }
        let at_limit = ApplicationProducerStateCheckpointCommandV1 {
            checkpoint: ProducerSnapshotV1::new(
                TopicId::new([0x9f; 16]),
                PartitionId::new(3),
                11,
                at_limit.clone(),
            )
            .unwrap(),
        };
        let at_limit_bytes = encode_application_producer_state_checkpoint_command_v1(
            &at_limit,
            CommandLimits::default(),
        )
        .unwrap();
        assert_eq!(
            decode_application_producer_state_checkpoint_command_v1(
                &at_limit_bytes,
                CommandLimits::default(),
            )
            .unwrap()
            .checkpoint
            .states
            .len(),
            MAX_PARTITION_PRODUCER_ENTRIES
        );

        let mut too_many = at_limit.checkpoint.states;
        let one_over = u64::try_from(MAX_PARTITION_PRODUCER_ENTRIES + 1).unwrap();
        too_many.insert(
            ProducerId::new(one_over).unwrap(),
            checkpoint_state(one_over, one_over - 1, 1),
        );
        let too_many = ApplicationProducerStateCheckpointCommandV1 {
            checkpoint: ProducerSnapshotV1::new(
                TopicId::new([0x9f; 16]),
                PartitionId::new(3),
                11,
                too_many,
            )
            .unwrap(),
        };
        assert!(matches!(
            encode_application_producer_state_checkpoint_command_v1(
                &too_many,
                CommandLimits::default()
            ),
            Err(ReplicationError::LimitExceeded {
                what: "P9 producer checkpoint entries",
                ..
            })
        ));

        let tiny_frame = CommandLimits {
            storage: StorageLimits {
                max_frame_bytes: 128,
                ..StorageLimits::default()
            },
            ..CommandLimits::default()
        };
        assert!(matches!(
            encode_application_producer_state_checkpoint_command_v1(&command, tiny_frame),
            Err(ReplicationError::LimitExceeded { .. })
        ));
    }

    #[test]
    fn application_command_rejects_trailing_and_oversized_data() {
        let command = ApplicationCommand {
            base_stream_offset: StreamOffset::new(0),
            records: vec![DataRecord {
                timestamp_ms: 1,
                key: None,
                value: Some(vec![7; 32]),
                headers: Vec::new(),
            }],
        };
        let mut bytes = encode_application_command(&command, CommandLimits::default()).unwrap();
        bytes.push(0);
        assert!(decode_application_command(&bytes, CommandLimits::default()).is_err());

        let limits = CommandLimits {
            max_command_bytes: 8,
            ..CommandLimits::default()
        };
        assert!(encode_application_command(&command, limits).is_err());
    }

    #[test]
    fn idempotent_application_command_v2_round_trip_is_canonical_and_version_separated() {
        let command = ApplicationCommandV2 {
            base_stream_offset: StreamOffset::new(42),
            authority: ProducerAuthority::new(
                ProducerId::new(9).unwrap(),
                ProducerEpoch::new(2).unwrap(),
            ),
            base_sequence: ProducerSequence::new(7),
            records: vec![DataRecord {
                timestamp_ms: 100,
                key: Some(b"key".to_vec()),
                value: Some(b"value".to_vec()),
                headers: vec![UserHeader {
                    name: b"kind".to_vec(),
                    value: Some(b"event".to_vec()),
                }],
            }],
        };
        let bytes = encode_application_command_v2(&command, CommandLimits::default()).unwrap();
        assert_eq!(
            bytes,
            decode_hex(
                "020001002a0000000000000009000000000000000200000007000000010000006400000000000000030000000500000001000400050000006b696e646576656e746b657976616c7565"
            )
        );
        assert_eq!(
            decode_application_command_v2(&bytes, CommandLimits::default()).unwrap(),
            command
        );
        assert!(decode_application_command(&bytes, CommandLimits::default()).is_err());

        let legacy = ApplicationCommand {
            base_stream_offset: StreamOffset::new(1),
            records: command.records,
        };
        let legacy_bytes = encode_application_command(&legacy, CommandLimits::default()).unwrap();
        assert!(decode_application_command_v2(&legacy_bytes, CommandLimits::default()).is_err());
    }

    #[test]
    fn idempotent_application_command_v2_rejects_malformed_identity_trailing_and_wrap() {
        let command = ApplicationCommandV2 {
            base_stream_offset: StreamOffset::new(0),
            authority: ProducerAuthority::new(
                ProducerId::new(1).unwrap(),
                ProducerEpoch::new(1).unwrap(),
            ),
            base_sequence: ProducerSequence::ZERO,
            records: vec![DataRecord {
                timestamp_ms: 1,
                key: None,
                value: Some(vec![1]),
                headers: vec![],
            }],
        };
        let mut bytes = encode_application_command_v2(&command, CommandLimits::default()).unwrap();
        bytes[12..20].fill(0);
        assert!(decode_application_command_v2(&bytes, CommandLimits::default()).is_err());

        let mut trailing =
            encode_application_command_v2(&command, CommandLimits::default()).unwrap();
        trailing.push(0);
        assert!(decode_application_command_v2(&trailing, CommandLimits::default()).is_err());

        let wrap = ApplicationCommandV2 {
            base_sequence: ProducerSequence::new(u32::MAX),
            ..command
        };
        assert!(encode_application_command_v2(&wrap, CommandLimits::default()).is_err());

        let hidden_timestamp = ApplicationCommandV2 {
            base_sequence: ProducerSequence::ZERO,
            records: vec![DataRecord {
                timestamp_ms: -1,
                key: None,
                value: Some(vec![1]),
                headers: vec![],
            }],
            ..wrap
        };
        assert!(
            encode_application_command_v2(&hidden_timestamp, CommandLimits::default()).is_err()
        );
    }

    #[test]
    fn p9d_maintenance_v3_is_exact_canonical_and_version_separated() {
        let command = ApplicationMaintenanceCommandV1 {
            new_log_start_offset: StreamOffset::new(0x0102_0304_0506_0708),
        };
        let bytes = encode_application_maintenance_command_v1(command);
        assert_eq!(bytes, decode_hex("030001000807060504030201"));
        assert_eq!(
            decode_application_maintenance_command_v1(&bytes).unwrap(),
            command
        );
        assert!(decode_application_command(&bytes, CommandLimits::default()).is_err());
        assert!(decode_application_command_v2(&bytes, CommandLimits::default()).is_err());

        let mut unknown_kind = bytes.clone();
        unknown_kind[2..4].copy_from_slice(&2_u16.to_le_bytes());
        assert!(decode_application_maintenance_command_v1(&unknown_kind).is_err());
        let mut trailing = bytes;
        trailing.push(0);
        assert!(decode_application_maintenance_command_v1(&trailing).is_err());
    }
}
