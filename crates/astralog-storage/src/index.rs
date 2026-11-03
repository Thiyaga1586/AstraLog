use std::{fs::File, io::Read, path::Path};

#[cfg(test)]
use std::path::PathBuf;

use crate::{
    Frame, Result, RetainedFrameV2, RetainedScannedFrameV2, RetainedSegmentHeaderV2, ScannedFrame,
    SegmentFileId, SegmentHeader, StorageError,
    io_util::{atomic_write, sync_directory},
};

const INDEX_HEADER_LEN: usize = 40;
const INDEX_ENTRY_LEN: usize = 16;
const FORMAT_VERSION: u16 = 1;

const OFFSET_MAGIC: &[u8; 8] = b"ASTOIDX1";
const TIME_MAGIC: &[u8; 8] = b"ASTTIDX1";
const RAFT_MAGIC: &[u8; 8] = b"ASTRIDX1";

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u16)]
pub enum IndexKind {
    Offset = 1,
    Time = 2,
    Raft = 3,
}

impl IndexKind {
    fn magic(self) -> &'static [u8; 8] {
        match self {
            Self::Offset => OFFSET_MAGIC,
            Self::Time => TIME_MAGIC,
            Self::Raft => RAFT_MAGIC,
        }
    }

    fn extension(self) -> &'static str {
        match self {
            Self::Offset => "index",
            Self::Time => "timeindex",
            Self::Raft => "raftindex",
        }
    }
}

impl TryFrom<u16> for IndexKind {
    type Error = StorageError;

    fn try_from(value: u16) -> Result<Self> {
        match value {
            1 => Ok(Self::Offset),
            2 => Ok(Self::Time),
            3 => Ok(Self::Raft),
            _ => Err(StorageError::InvalidFormatOwned(format!(
                "unknown index kind {value}",
            ))),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct IndexEntry {
    pub key: u64,
    pub value: u64,
}

pub fn rebuild_segment_indexes(
    log_path: &Path,
    header: &SegmentHeader,
    frames: &[ScannedFrame],
    index_interval_bytes: u64,
) -> Result<()> {
    if index_interval_bytes == 0 {
        return Err(StorageError::InvalidFormat(
            "index interval must be greater than zero",
        ));
    }
    let offset_entries = build_offset_entries(frames, index_interval_bytes);
    let time_entries = build_time_entries(frames, index_interval_bytes);
    let raft_entries = build_raft_entries(frames, index_interval_bytes);
    let directory = log_path
        .parent()
        .ok_or(StorageError::InvalidFormat("segment path has no parent"))?;
    let id = SegmentFileId::from_header(header);
    write_index(
        &directory.join(id.index_file_name(IndexKind::Offset.extension())),
        header,
        IndexKind::Offset,
        &offset_entries,
    )?;
    write_index(
        &directory.join(id.index_file_name(IndexKind::Time.extension())),
        header,
        IndexKind::Time,
        &time_entries,
    )?;
    write_index(
        &directory.join(id.index_file_name(IndexKind::Raft.extension())),
        header,
        IndexKind::Raft,
        &raft_entries,
    )?;
    sync_directory(directory)?;
    Ok(())
}

pub fn rebuild_retained_segment_indexes_v2(
    log_path: &Path,
    header: &RetainedSegmentHeaderV2,
    frames: &[RetainedScannedFrameV2],
    index_interval_bytes: u64,
) -> Result<()> {
    if index_interval_bytes == 0 {
        return Err(StorageError::InvalidFormat(
            "index interval must be greater than zero",
        ));
    }
    let identity = header.identity_header();
    let mut offset_entries = Vec::new();
    let mut time_entries = Vec::new();
    let mut raft_entries = Vec::new();
    let mut last_offset_position = None;
    let mut last_time_position = None;
    let mut last_raft_position = None;
    let mut max_timestamp_seen = i64::MIN;

    for scanned in frames {
        if should_index(last_raft_position, scanned.position, index_interval_bytes) {
            raft_entries.push(IndexEntry {
                key: scanned.frame.raft_index().get(),
                value: scanned.position,
            });
            last_raft_position = Some(scanned.position);
        }
        let RetainedFrameV2::SparseData(batch) = &scanned.frame else {
            continue;
        };
        let first = batch.records.first().ok_or(StorageError::InvalidFormat(
            "V2 sparse batch cannot be empty",
        ))?;
        if should_index(last_offset_position, scanned.position, index_interval_bytes) {
            offset_entries.push(IndexEntry {
                key: first.offset.get(),
                value: scanned.position,
            });
            last_offset_position = Some(scanned.position);
        }
        let batch_max = batch
            .records
            .iter()
            .map(|record| record.record.timestamp_ms)
            .max()
            .ok_or(StorageError::InvalidFormat(
                "V2 sparse batch cannot be empty",
            ))?;
        max_timestamp_seen = max_timestamp_seen.max(batch_max);
        if should_index(last_time_position, scanned.position, index_interval_bytes) {
            time_entries.push(IndexEntry {
                key: u64::from_le_bytes(max_timestamp_seen.to_le_bytes()),
                value: first.offset.get(),
            });
            last_time_position = Some(scanned.position);
        }
    }

    let directory = log_path
        .parent()
        .ok_or(StorageError::InvalidFormat("segment path has no parent"))?;
    let id = SegmentFileId::from_header(&identity);
    write_index(
        &directory.join(id.index_file_name(IndexKind::Offset.extension())),
        &identity,
        IndexKind::Offset,
        &offset_entries,
    )?;
    write_index(
        &directory.join(id.index_file_name(IndexKind::Time.extension())),
        &identity,
        IndexKind::Time,
        &time_entries,
    )?;
    write_index(
        &directory.join(id.index_file_name(IndexKind::Raft.extension())),
        &identity,
        IndexKind::Raft,
        &raft_entries,
    )?;
    sync_directory(directory)?;
    Ok(())
}

pub fn validate_index_file(
    path: &Path,
    expected_header: &SegmentHeader,
    expected_kind: IndexKind,
    log_len: u64,
) -> Result<Vec<IndexEntry>> {
    let mut file = File::open(path)?;
    let mut bytes = Vec::new();
    file.read_to_end(&mut bytes)?;
    if bytes.len() < INDEX_HEADER_LEN
        || !(bytes.len() - INDEX_HEADER_LEN).is_multiple_of(INDEX_ENTRY_LEN)
    {
        return Err(StorageError::InvalidFormat("invalid index file length"));
    }
    validate_index_header(&bytes[..INDEX_HEADER_LEN], expected_header, expected_kind)?;
    let mut entries = Vec::new();
    let mut previous_key = None;
    let (chunks, remainder) = bytes[INDEX_HEADER_LEN..].as_chunks::<INDEX_ENTRY_LEN>();
    debug_assert!(remainder.is_empty());
    for chunk in chunks {
        let key = u64::from_le_bytes(chunk[0..8].try_into().expect("8-byte slice"));
        let value = u64::from_le_bytes(chunk[8..16].try_into().expect("8-byte slice"));
        match expected_kind {
            IndexKind::Offset | IndexKind::Raft => {
                if previous_key.is_some_and(|previous| key < previous) {
                    return Err(StorageError::InvalidFormat("index keys are not monotonic"));
                }
                if value < crate::format::SEGMENT_HEADER_LEN as u64 || value >= log_len {
                    return Err(StorageError::InvalidFormat(
                        "index points outside segment frame region",
                    ));
                }
            }
            IndexKind::Time => {
                let timestamp = i64::from_le_bytes(key.to_le_bytes());
                if previous_key
                    .is_some_and(|previous| timestamp < i64::from_le_bytes(previous.to_le_bytes()))
                {
                    return Err(StorageError::InvalidFormat(
                        "time-index timestamps are not monotonic",
                    ));
                }
            }
        }
        entries.push(IndexEntry { key, value });
        previous_key = Some(key);
    }
    Ok(entries)
}

fn build_offset_entries(frames: &[ScannedFrame], interval: u64) -> Vec<IndexEntry> {
    let mut entries = Vec::new();
    let mut last_position = None;
    for scanned in frames {
        let Frame::Data(batch) = &scanned.frame else {
            continue;
        };
        if should_index(last_position, scanned.position, interval) {
            entries.push(IndexEntry {
                key: batch.base_stream_offset.get(),
                value: scanned.position,
            });
            last_position = Some(scanned.position);
        }
    }
    entries
}

fn build_time_entries(frames: &[ScannedFrame], interval: u64) -> Vec<IndexEntry> {
    let mut entries = Vec::new();
    let mut last_position = None;
    let mut max_timestamp_seen = i64::MIN;
    for scanned in frames {
        let Frame::Data(batch) = &scanned.frame else {
            continue;
        };
        if should_index(last_position, scanned.position, interval) {
            let Some(batch_max_timestamp) =
                batch.records.iter().map(|record| record.timestamp_ms).max()
            else {
                continue;
            };
            max_timestamp_seen = max_timestamp_seen.max(batch_max_timestamp);
            entries.push(IndexEntry {
                key: u64::from_le_bytes(max_timestamp_seen.to_le_bytes()),
                value: batch.base_stream_offset.get(),
            });
            last_position = Some(scanned.position);
        }
    }
    entries
}

fn build_raft_entries(frames: &[ScannedFrame], interval: u64) -> Vec<IndexEntry> {
    let mut entries = Vec::new();
    let mut last_position = None;
    for scanned in frames {
        if should_index(last_position, scanned.position, interval) {
            entries.push(IndexEntry {
                key: scanned.frame.raft_index().get(),
                value: scanned.position,
            });
            last_position = Some(scanned.position);
        }
    }
    entries
}

fn should_index(last_position: Option<u64>, position: u64, interval: u64) -> bool {
    last_position.is_none_or(|last| position.saturating_sub(last) >= interval)
}

fn write_index(
    path: &Path,
    segment: &SegmentHeader,
    kind: IndexKind,
    entries: &[IndexEntry],
) -> Result<()> {
    let mut bytes = encode_index_header(segment, kind).to_vec();
    for entry in entries {
        bytes.extend_from_slice(&entry.key.to_le_bytes());
        bytes.extend_from_slice(&entry.value.to_le_bytes());
    }
    atomic_write(path, &bytes)
}

fn encode_index_header(segment: &SegmentHeader, kind: IndexKind) -> [u8; INDEX_HEADER_LEN] {
    let mut bytes = [0_u8; INDEX_HEADER_LEN];
    bytes[0..8].copy_from_slice(kind.magic());
    bytes[8..10].copy_from_slice(&FORMAT_VERSION.to_le_bytes());
    bytes[10..12].copy_from_slice(&(INDEX_HEADER_LEN as u16).to_le_bytes());
    bytes[12..14].copy_from_slice(&(kind as u16).to_le_bytes());
    bytes[14..16].copy_from_slice(&(INDEX_ENTRY_LEN as u16).to_le_bytes());
    bytes[16..24].copy_from_slice(&segment.base_stream_offset.get().to_le_bytes());
    bytes[24..32].copy_from_slice(&segment.base_raft_index.get().to_le_bytes());
    bytes[32..36].copy_from_slice(&segment.generation.to_le_bytes());
    let crc = crc32c::crc32c(&bytes[..36]);
    bytes[36..40].copy_from_slice(&crc.to_le_bytes());
    bytes
}

fn validate_index_header(
    bytes: &[u8],
    segment: &SegmentHeader,
    expected_kind: IndexKind,
) -> Result<()> {
    if bytes.len() != INDEX_HEADER_LEN {
        return Err(StorageError::InvalidFormat("index header length mismatch"));
    }
    if &bytes[0..8] != expected_kind.magic() {
        return Err(StorageError::InvalidFormat("index magic mismatch"));
    }
    if u16::from_le_bytes(bytes[8..10].try_into().expect("2-byte slice")) != FORMAT_VERSION {
        return Err(StorageError::InvalidFormat("index version mismatch"));
    }
    if u16::from_le_bytes(bytes[10..12].try_into().expect("2-byte slice"))
        != INDEX_HEADER_LEN as u16
        || u16::from_le_bytes(bytes[14..16].try_into().expect("2-byte slice"))
            != INDEX_ENTRY_LEN as u16
    {
        return Err(StorageError::InvalidFormat("index layout mismatch"));
    }
    let kind = IndexKind::try_from(u16::from_le_bytes(
        bytes[12..14].try_into().expect("2-byte slice"),
    ))?;
    if kind != expected_kind {
        return Err(StorageError::InvalidFormat("index kind mismatch"));
    }
    let base_stream = u64::from_le_bytes(bytes[16..24].try_into().expect("8-byte slice"));
    let base_raft = u64::from_le_bytes(bytes[24..32].try_into().expect("8-byte slice"));
    let generation = u32::from_le_bytes(bytes[32..36].try_into().expect("4-byte slice"));
    if base_stream != segment.base_stream_offset.get()
        || base_raft != segment.base_raft_index.get()
        || generation != segment.generation
    {
        return Err(StorageError::InvalidFormat(
            "index identity does not match segment",
        ));
    }
    let expected_crc = u32::from_le_bytes(bytes[36..40].try_into().expect("4-byte slice"));
    let actual_crc = crc32c::crc32c(&bytes[..36]);
    if expected_crc != actual_crc {
        return Err(StorageError::ChecksumMismatch {
            context: "index header",
            expected: expected_crc,
            actual: actual_crc,
        });
    }
    Ok(())
}

#[cfg(test)]
pub(crate) fn index_paths(log_path: &Path, header: &SegmentHeader) -> Result<[PathBuf; 3]> {
    let directory = log_path
        .parent()
        .ok_or(StorageError::InvalidFormat("segment path has no parent"))?;
    let id = SegmentFileId::from_header(header);
    Ok([
        directory.join(id.index_file_name(IndexKind::Offset.extension())),
        directory.join(id.index_file_name(IndexKind::Time.extension())),
        directory.join(id.index_file_name(IndexKind::Raft.extension())),
    ])
}

#[cfg(test)]
mod tests {
    use std::{
        fs,
        time::{SystemTime, UNIX_EPOCH},
    };

    use astralog_common::{
        PartitionId, RaftIndex, RaftTerm, SegmentBaseOffset, StreamOffset, TopicId,
    };

    use super::*;
    use crate::test_support::decode_hex;
    use crate::{
        Compression, DataRecord, Frame, RecordBatch, SegmentWriter, StorageLimits, scan_segment,
    };

    fn test_directory(label: &str) -> PathBuf {
        let nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let path = std::env::temp_dir().join(format!(
            "astralog-index-{label}-{}-{nanos}",
            std::process::id(),
        ));
        fs::create_dir_all(&path).unwrap();
        path
    }

    #[test]
    fn indexes_rebuild_from_log_only() {
        let directory = test_directory("rebuild");
        let header = SegmentHeader {
            topic_id: TopicId::new([4; 16]),
            partition_id: PartitionId::new(1),
            generation: 0,
            base_stream_offset: SegmentBaseOffset::new(0),
            base_raft_index: RaftIndex::FIRST,
            created_at_ms: 0,
        };
        let mut writer = SegmentWriter::create(&directory, header.clone()).unwrap();
        for i in 0..3_u64 {
            writer
                .append(
                    &Frame::Data(RecordBatch {
                        raft_term: RaftTerm::new(1),
                        raft_index: RaftIndex::new(i + 1).unwrap(),
                        base_stream_offset: StreamOffset::new(i),
                        producer: None,
                        compression: Compression::None,
                        records: vec![DataRecord {
                            timestamp_ms: i as i64,
                            key: Some(vec![i as u8]),
                            value: Some(vec![1, 2, 3]),
                            headers: Vec::new(),
                        }],
                    }),
                    StorageLimits::default(),
                )
                .unwrap();
        }
        writer.flush_data().unwrap();
        let scan = scan_segment(writer.path(), StorageLimits::default()).unwrap();
        rebuild_segment_indexes(writer.path(), &header, &scan.frames, 1).unwrap();
        let log_len = fs::metadata(writer.path()).unwrap().len();
        let paths = index_paths(writer.path(), &header).unwrap();
        assert!(
            !validate_index_file(&paths[0], &header, IndexKind::Offset, log_len)
                .unwrap()
                .is_empty()
        );
        assert!(
            !validate_index_file(&paths[1], &header, IndexKind::Time, log_len)
                .unwrap()
                .is_empty()
        );
        assert!(
            !validate_index_file(&paths[2], &header, IndexKind::Raft, log_len)
                .unwrap()
                .is_empty()
        );
        fs::remove_dir_all(directory).unwrap();
    }

    #[test]
    fn p8_indexes_rebuild_from_mixed_compressed_frames_using_logical_metadata() {
        let directory = test_directory("p8-compressed-rebuild");
        let header = SegmentHeader {
            topic_id: TopicId::new([0x83; 16]),
            partition_id: PartitionId::new(2),
            generation: 0,
            base_stream_offset: SegmentBaseOffset::new(0),
            base_raft_index: RaftIndex::FIRST,
            created_at_ms: 0,
        };
        let mut writer = SegmentWriter::create(&directory, header.clone()).unwrap();
        for (ordinal, codec) in [Compression::None, Compression::Lz4, Compression::Zstd]
            .into_iter()
            .enumerate()
        {
            writer
                .append(
                    &Frame::Data(RecordBatch {
                        raft_term: RaftTerm::new(1),
                        raft_index: RaftIndex::new(u64::try_from(ordinal + 1).unwrap()).unwrap(),
                        base_stream_offset: StreamOffset::new(u64::try_from(ordinal).unwrap()),
                        producer: None,
                        compression: codec,
                        records: vec![DataRecord {
                            timestamp_ms: 10 + i64::try_from(ordinal).unwrap(),
                            key: Some(vec![u8::try_from(ordinal).unwrap()]),
                            value: Some(vec![b'X' + u8::try_from(ordinal).unwrap(); 2048]),
                            headers: Vec::new(),
                        }],
                    }),
                    StorageLimits::default(),
                )
                .unwrap();
        }
        writer.flush_data().unwrap();
        let scan = scan_segment(writer.path(), StorageLimits::default()).unwrap();
        assert!(scan.failure.is_none());
        assert_eq!(scan.frames.len(), 3);
        rebuild_segment_indexes(writer.path(), &header, &scan.frames, 1).unwrap();

        let log_len = fs::metadata(writer.path()).unwrap().len();
        let paths = index_paths(writer.path(), &header).unwrap();
        let offset = validate_index_file(&paths[0], &header, IndexKind::Offset, log_len).unwrap();
        let time = validate_index_file(&paths[1], &header, IndexKind::Time, log_len).unwrap();
        let raft = validate_index_file(&paths[2], &header, IndexKind::Raft, log_len).unwrap();
        assert_eq!(
            offset.iter().map(|entry| entry.key).collect::<Vec<_>>(),
            vec![0, 1, 2]
        );
        assert_eq!(
            time.iter()
                .map(|entry| i64::from_le_bytes(entry.key.to_le_bytes()))
                .collect::<Vec<_>>(),
            vec![10, 11, 12]
        );
        assert_eq!(
            raft.iter().map(|entry| entry.key).collect::<Vec<_>>(),
            vec![1, 2, 3]
        );
        fs::remove_dir_all(directory).unwrap();
    }

    #[test]
    fn index_v1_headers_and_entries_match_goldens() {
        let header = SegmentHeader {
            topic_id: TopicId::new([0; 16]),
            partition_id: PartitionId::new(0),
            generation: 3,
            base_stream_offset: SegmentBaseOffset::new(42),
            base_raft_index: RaftIndex::new(11).unwrap(),
            created_at_ms: 0,
        };

        let cases = [
            (
                IndexKind::Offset,
                IndexEntry { key: 42, value: 72 },
                include_str!("../tests/golden/offset_index_v1.hex"),
            ),
            (
                IndexKind::Time,
                IndexEntry {
                    key: u64::from_le_bytes(1_700_000_000_000_i64.to_le_bytes()),
                    value: 42,
                },
                include_str!("../tests/golden/time_index_v1.hex"),
            ),
            (
                IndexKind::Raft,
                IndexEntry { key: 11, value: 72 },
                include_str!("../tests/golden/raft_index_v1.hex"),
            ),
        ];

        for (kind, entry, fixture) in cases {
            let mut encoded = encode_index_header(&header, kind).to_vec();
            encoded.extend_from_slice(&entry.key.to_le_bytes());
            encoded.extend_from_slice(&entry.value.to_le_bytes());
            assert_eq!(encoded, decode_hex(fixture));
        }
    }
}
