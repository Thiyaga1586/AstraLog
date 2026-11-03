use std::{
    fs::{File, OpenOptions},
    io::{Read, Seek, SeekFrom, Write},
    path::{Path, PathBuf},
};

use astralog_common::{RaftIndex, SegmentBaseOffset};

use crate::{
    Frame, Result, SegmentHeader, StorageError, StorageLimits, decode_frame, decode_segment_header,
    encode_frame, encode_segment_header,
    format::{FRAME_HEADER_LEN, MIN_FRAME_LEN, SEGMENT_HEADER_LEN},
    io_util::create_new_durable,
};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct SegmentFileId {
    pub base_stream_offset: SegmentBaseOffset,
    pub base_raft_index: RaftIndex,
    pub generation: u32,
}

impl SegmentFileId {
    pub fn from_header(header: &SegmentHeader) -> Self {
        Self {
            base_stream_offset: header.base_stream_offset,
            base_raft_index: header.base_raft_index,
            generation: header.generation,
        }
    }

    pub fn stem(self) -> String {
        format!(
            "{:020}-{:020}-g{:08}",
            self.base_stream_offset.get(),
            self.base_raft_index.get(),
            self.generation,
        )
    }

    pub fn log_file_name(self) -> String {
        format!("{}.log", self.stem())
    }

    pub fn index_file_name(self, extension: &str) -> String {
        format!("{}.{}", self.stem(), extension)
    }

    pub fn parse_log_file_name(name: &str) -> Result<Self> {
        let stem = name
            .strip_suffix(".log")
            .ok_or(StorageError::InvalidFormat("segment file must end in .log"))?;
        let mut parts = stem.split('-');
        let stream = parts
            .next()
            .ok_or(StorageError::InvalidFormat("missing base stream offset"))?;
        let raft = parts
            .next()
            .ok_or(StorageError::InvalidFormat("missing base Raft index"))?;
        let generation = parts
            .next()
            .ok_or(StorageError::InvalidFormat("missing generation"))?;
        if parts.next().is_some() || stream.len() != 20 || raft.len() != 20 {
            return Err(StorageError::InvalidFormat(
                "invalid segment file name shape",
            ));
        }
        let generation = generation
            .strip_prefix('g')
            .ok_or(StorageError::InvalidFormat(
                "segment generation must start with g",
            ))?;
        if generation.len() != 8 {
            return Err(StorageError::InvalidFormat(
                "segment generation must contain 8 digits",
            ));
        }
        let stream = parse_decimal_u64(stream, "base stream offset")?;
        let raft = parse_decimal_u64(raft, "base Raft index")?;
        let generation = generation
            .parse::<u32>()
            .map_err(|_| StorageError::InvalidFormat("invalid segment generation"))?;
        let base_raft_index = RaftIndex::new(raft).ok_or(StorageError::InvalidFormat(
            "base Raft index cannot be zero",
        ))?;
        Ok(Self {
            base_stream_offset: SegmentBaseOffset::new(stream),
            base_raft_index,
            generation,
        })
    }
}

fn parse_decimal_u64(value: &str, what: &'static str) -> Result<u64> {
    if !value.as_bytes().iter().all(u8::is_ascii_digit) {
        return Err(StorageError::InvalidFormatOwned(format!(
            "{what} is not decimal",
        )));
    }
    value
        .parse::<u64>()
        .map_err(|_| StorageError::InvalidFormatOwned(format!("invalid {what}")))
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ScannedFrame {
    pub position: u64,
    pub encoded_len: u32,
    pub frame: Frame,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ScanFailureKind {
    IncompleteTail,
    InvalidFrame(String),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ScanFailure {
    pub position: u64,
    pub kind: ScanFailureKind,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SegmentScan {
    pub header: SegmentHeader,
    pub frames: Vec<ScannedFrame>,
    pub valid_end: u64,
    pub failure: Option<ScanFailure>,
}

pub struct SegmentWriter {
    path: PathBuf,
    file: File,
    header: SegmentHeader,
    position: u64,
    poisoned: bool,
    #[cfg(test)]
    test_fault: Option<TestFault>,
}

#[cfg(test)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum TestFault {
    Append,
    PartialAppend,
    Flush,
}

impl SegmentWriter {
    pub fn create(directory: &Path, header: SegmentHeader) -> Result<Self> {
        let id = SegmentFileId::from_header(&header);
        let path = directory.join(id.log_file_name());
        let bytes = encode_segment_header(&header);
        let file = create_new_durable(&path, &bytes)?;
        Ok(Self {
            path,
            file,
            header,
            position: SEGMENT_HEADER_LEN as u64,
            poisoned: false,
            #[cfg(test)]
            test_fault: None,
        })
    }

    pub(crate) fn open_existing(path: &Path) -> Result<Self> {
        let header = read_segment_header(path)?;
        validate_header_matches_name(path, &header)?;
        let file = OpenOptions::new().read(true).write(true).open(path)?;
        let position = file.metadata()?.len();
        Ok(Self {
            path: path.to_path_buf(),
            file,
            header,
            position,
            poisoned: false,
            #[cfg(test)]
            test_fault: None,
        })
    }

    pub fn append(&mut self, frame: &Frame, limits: StorageLimits) -> Result<u64> {
        let encoded = encode_frame(frame, limits)?;
        self.append_encoded(&encoded)
    }

    pub(crate) fn append_encoded(&mut self, encoded: &[u8]) -> Result<u64> {
        if self.poisoned {
            return Err(StorageError::Corruption {
                reason: "segment writer is poisoned after an unrecoverable append failure"
                    .to_owned(),
            });
        }
        let position = self.position;
        self.file.seek(SeekFrom::Start(position))?;
        if let Err(write_error) = self.write_encoded_bytes(encoded) {
            if let Err(rollback_error) = self.rollback_failed_append(position) {
                self.poisoned = true;
                return Err(StorageError::Corruption {
                    reason: format!(
                        "append failed ({write_error}) and rollback to byte {position} also failed ({rollback_error})",
                    ),
                });
            }
            return Err(write_error.into());
        }
        self.position = position
            .checked_add(encoded.len() as u64)
            .ok_or(StorageError::ArithmeticOverflow("segment position"))?;
        Ok(position)
    }

    pub fn flush_data(&self) -> Result<()> {
        if self.poisoned {
            return Err(StorageError::Corruption {
                reason: "cannot flush a poisoned segment writer".to_owned(),
            });
        }
        #[cfg(test)]
        if self.test_fault == Some(TestFault::Flush) {
            return Err(std::io::Error::from_raw_os_error(5).into());
        }
        self.file.sync_data()?;
        Ok(())
    }

    pub fn flush_all(&self) -> Result<()> {
        if self.poisoned {
            return Err(StorageError::Corruption {
                reason: "cannot flush a poisoned segment writer".to_owned(),
            });
        }
        self.file.sync_all()?;
        Ok(())
    }

    pub(crate) fn truncate_to(&mut self, position: u64) -> Result<()> {
        if position < SEGMENT_HEADER_LEN as u64 || position > self.position {
            return Err(StorageError::InvalidFormat(
                "segment truncation position is outside current file",
            ));
        }
        self.file.set_len(position)?;
        self.file.sync_data()?;
        self.position = position;
        Ok(())
    }

    pub fn should_roll(
        &self,
        next_frame_len: usize,
        max_segment_bytes: u64,
        max_segment_age_ms: Option<i64>,
        now_ms: i64,
    ) -> bool {
        let exceeds_size = self
            .position
            .checked_add(next_frame_len as u64)
            .is_none_or(|size| size > max_segment_bytes);
        let exceeds_age = max_segment_age_ms.is_some_and(|max_age| {
            now_ms
                .checked_sub(self.header.created_at_ms)
                .is_some_and(|age| age >= max_age)
        });
        exceeds_size || exceeds_age
    }

    pub fn position(&self) -> u64 {
        self.position
    }

    pub fn path(&self) -> &Path {
        &self.path
    }

    pub fn header(&self) -> &SegmentHeader {
        &self.header
    }

    fn write_encoded_bytes(&mut self, encoded: &[u8]) -> std::io::Result<()> {
        #[cfg(test)]
        match self.test_fault {
            Some(TestFault::Append) => {
                return Err(std::io::Error::from_raw_os_error(28));
            }
            Some(TestFault::PartialAppend) => {
                let prefix_len = encoded.len().max(2) / 2;
                self.file.write_all(&encoded[..prefix_len])?;
                return Err(std::io::Error::from_raw_os_error(28));
            }
            Some(TestFault::Flush) | None => {}
        }
        self.file.write_all(encoded)
    }

    fn rollback_failed_append(&mut self, position: u64) -> std::io::Result<()> {
        self.file.set_len(position)?;
        self.file.seek(SeekFrom::Start(position))?;
        self.file.sync_data()
    }
}

pub fn read_segment_header(path: &Path) -> Result<SegmentHeader> {
    let mut file = File::open(path)?;
    let mut bytes = [0_u8; SEGMENT_HEADER_LEN];
    file.read_exact(&mut bytes)?;
    let header = decode_segment_header(&bytes)?;
    validate_header_matches_name(path, &header)?;
    Ok(header)
}

pub fn scan_segment(path: &Path, limits: StorageLimits) -> Result<SegmentScan> {
    let header = read_segment_header(path)?;
    let mut file = File::open(path)?;
    let file_len = file.metadata()?.len();
    let mut position = SEGMENT_HEADER_LEN as u64;
    let mut frames = Vec::new();
    let mut failure = None;

    while position < file_len {
        let remaining = file_len - position;
        if remaining < FRAME_HEADER_LEN as u64 {
            failure = Some(ScanFailure {
                position,
                kind: ScanFailureKind::IncompleteTail,
            });
            break;
        }

        file.seek(SeekFrom::Start(position))?;
        let mut header_bytes = [0_u8; FRAME_HEADER_LEN];
        file.read_exact(&mut header_bytes)?;
        let total_len = u32::from_le_bytes([
            header_bytes[0],
            header_bytes[1],
            header_bytes[2],
            header_bytes[3],
        ]) as usize;

        if total_len < MIN_FRAME_LEN {
            failure = Some(ScanFailure {
                position,
                kind: ScanFailureKind::InvalidFrame(format!(
                    "frame length {total_len} is below minimum {MIN_FRAME_LEN}",
                )),
            });
            break;
        }
        if total_len > limits.max_frame_bytes {
            failure = Some(ScanFailure {
                position,
                kind: ScanFailureKind::InvalidFrame(format!(
                    "frame length {total_len} exceeds configured maximum {}",
                    limits.max_frame_bytes,
                )),
            });
            break;
        }
        if remaining < total_len as u64 {
            failure = Some(ScanFailure {
                position,
                kind: ScanFailureKind::IncompleteTail,
            });
            break;
        }

        let mut encoded = vec![0_u8; total_len];
        encoded[..FRAME_HEADER_LEN].copy_from_slice(&header_bytes);
        file.read_exact(&mut encoded[FRAME_HEADER_LEN..])?;
        match decode_frame(&encoded, limits) {
            Ok(frame) => {
                frames.push(ScannedFrame {
                    position,
                    encoded_len: total_len as u32,
                    frame,
                });
                position = position
                    .checked_add(total_len as u64)
                    .ok_or(StorageError::ArithmeticOverflow("segment scan position"))?;
            }
            Err(err) => {
                failure = Some(ScanFailure {
                    position,
                    kind: ScanFailureKind::InvalidFrame(err.to_string()),
                });
                break;
            }
        }
    }

    Ok(SegmentScan {
        header,
        frames,
        valid_end: position,
        failure,
    })
}

pub fn read_scanned_frame_at(
    path: &Path,
    position: u64,
    limits: StorageLimits,
) -> Result<ScannedFrame> {
    if position < SEGMENT_HEADER_LEN as u64 {
        return Err(StorageError::InvalidFormat(
            "frame position overlaps segment header",
        ));
    }
    let mut file = File::open(path)?;
    let file_len = file.metadata()?.len();
    if position >= file_len {
        return Err(StorageError::InvalidFormat(
            "frame position is outside segment",
        ));
    }
    let remaining = file_len - position;
    if remaining < FRAME_HEADER_LEN as u64 {
        return Err(StorageError::InvalidFormat(
            "frame header is truncated at requested position",
        ));
    }
    file.seek(SeekFrom::Start(position))?;
    let mut header_bytes = [0_u8; FRAME_HEADER_LEN];
    file.read_exact(&mut header_bytes)?;
    let total_len = u32::from_le_bytes([
        header_bytes[0],
        header_bytes[1],
        header_bytes[2],
        header_bytes[3],
    ]) as usize;
    if total_len < MIN_FRAME_LEN {
        return Err(StorageError::InvalidFormat("frame length is below minimum"));
    }
    if total_len > limits.max_frame_bytes {
        return Err(StorageError::LimitExceeded {
            what: "frame bytes",
            actual: total_len,
            max: limits.max_frame_bytes,
        });
    }
    if remaining < total_len as u64 {
        return Err(StorageError::InvalidFormat(
            "frame is truncated at requested position",
        ));
    }
    let mut encoded = vec![0_u8; total_len];
    encoded[..FRAME_HEADER_LEN].copy_from_slice(&header_bytes);
    file.read_exact(&mut encoded[FRAME_HEADER_LEN..])?;
    let frame = decode_frame(&encoded, limits)?;
    Ok(ScannedFrame {
        position,
        encoded_len: total_len as u32,
        frame,
    })
}

pub fn read_frame_at(path: &Path, position: u64, limits: StorageLimits) -> Result<Frame> {
    Ok(read_scanned_frame_at(path, position, limits)?.frame)
}

fn validate_header_matches_name(path: &Path, header: &SegmentHeader) -> Result<()> {
    let name =
        path.file_name()
            .and_then(|name| name.to_str())
            .ok_or(StorageError::InvalidFormat(
                "segment file name is not valid UTF-8",
            ))?;
    let id = SegmentFileId::parse_log_file_name(name)?;
    if id != SegmentFileId::from_header(header) {
        return Err(StorageError::InvalidFormat(
            "segment file name does not match segment header identity",
        ));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use std::fs;

    use astralog_common::{PartitionId, RaftTerm, TopicId};

    use super::*;
    use crate::{ControlEntry, Frame, test_support::test_directory};

    fn header() -> SegmentHeader {
        SegmentHeader {
            topic_id: TopicId::new([0x11; 16]),
            partition_id: PartitionId::new(3),
            generation: 0,
            base_stream_offset: SegmentBaseOffset::new(0),
            base_raft_index: RaftIndex::FIRST,
            created_at_ms: 100,
        }
    }

    #[test]
    fn segment_id_round_trip() {
        let id = SegmentFileId::from_header(&header());
        assert_eq!(
            SegmentFileId::parse_log_file_name(&id.log_file_name()).unwrap(),
            id,
        );
    }

    #[test]
    fn write_then_scan_segment() {
        let dir = test_directory("segment", "scan");
        let mut writer = SegmentWriter::create(&dir, header()).unwrap();
        let frame = Frame::Control(ControlEntry {
            raft_term: RaftTerm::new(1),
            raft_index: RaftIndex::FIRST,
            control_kind: 1,
            control_version: 1,
            body: b"state".to_vec(),
        });
        writer.append(&frame, StorageLimits::default()).unwrap();
        writer.flush_data().unwrap();

        let scan = scan_segment(writer.path(), StorageLimits::default()).unwrap();
        assert_eq!(scan.frames.len(), 1);
        assert_eq!(scan.frames[0].frame, frame);
        assert!(scan.failure.is_none());
        assert_eq!(
            read_frame_at(
                writer.path(),
                scan.frames[0].position,
                StorageLimits::default(),
            )
            .unwrap(),
            frame,
        );
        fs::remove_dir_all(dir).unwrap();
    }

    #[test]
    fn truncated_frame_is_classified_as_incomplete_tail() {
        let dir = test_directory("segment", "truncated");
        let mut writer = SegmentWriter::create(&dir, header()).unwrap();
        let frame = Frame::Control(ControlEntry {
            raft_term: RaftTerm::new(1),
            raft_index: RaftIndex::FIRST,
            control_kind: 1,
            control_version: 1,
            body: vec![7; 128],
        });
        writer.append(&frame, StorageLimits::default()).unwrap();
        writer.flush_data().unwrap();
        let short = writer.position() - 5;
        writer.truncate_to(short).unwrap();

        let scan = scan_segment(writer.path(), StorageLimits::default()).unwrap();
        assert!(matches!(
            scan.failure,
            Some(ScanFailure {
                kind: ScanFailureKind::IncompleteTail,
                ..
            })
        ));
        fs::remove_dir_all(dir).unwrap();
    }

    #[test]
    fn every_frame_truncation_boundary_is_detected() {
        let frame = Frame::Control(ControlEntry {
            raft_term: RaftTerm::new(1),
            raft_index: RaftIndex::FIRST,
            control_kind: 1,
            control_version: 1,
            body: vec![3; 64],
        });
        let encoded = crate::encode_frame(&frame, StorageLimits::default()).unwrap();

        for cut in 1..encoded.len() {
            let dir = test_directory("segment", &format!("cut-{cut}"));
            let segment_header = header();
            let id = SegmentFileId::from_header(&segment_header);
            let path = dir.join(id.log_file_name());
            let mut bytes = crate::encode_segment_header(&segment_header).to_vec();
            bytes.extend_from_slice(&encoded[..cut]);
            fs::write(&path, bytes).unwrap();

            let scan = scan_segment(&path, StorageLimits::default()).unwrap();
            assert!(
                scan.failure.is_some(),
                "cut {cut} unexpectedly produced a complete frame",
            );
            assert_eq!(scan.valid_end, SEGMENT_HEADER_LEN as u64);
            fs::remove_dir_all(dir).unwrap();
        }
    }

    #[test]
    fn append_io_failure_is_returned_without_advancing_position() {
        let dir = test_directory("segment", "append-fault");
        let mut writer = SegmentWriter::create(&dir, header()).unwrap();
        let start = writer.position();
        writer.test_fault = Some(TestFault::Append);
        let result = writer.append_encoded(&[0_u8; MIN_FRAME_LEN]);
        assert!(matches!(result, Err(StorageError::Io(_))));
        assert_eq!(writer.position(), start);
        fs::remove_dir_all(dir).unwrap();
    }

    #[test]
    fn partial_append_failure_rolls_back_file_before_retry() {
        let dir = test_directory("segment", "partial-append-fault");
        let mut writer = SegmentWriter::create(&dir, header()).unwrap();
        let frame = Frame::Control(ControlEntry {
            raft_term: RaftTerm::new(1),
            raft_index: RaftIndex::FIRST,
            control_kind: 1,
            control_version: 1,
            body: vec![0x5a; 128],
        });
        let encoded = crate::encode_frame(&frame, StorageLimits::default()).unwrap();
        let start = writer.position();

        writer.test_fault = Some(TestFault::PartialAppend);
        assert!(matches!(
            writer.append_encoded(&encoded),
            Err(StorageError::Io(_))
        ));
        assert_eq!(writer.position(), start);
        assert_eq!(fs::metadata(writer.path()).unwrap().len(), start);

        writer.test_fault = None;
        writer.append_encoded(&encoded).unwrap();
        writer.flush_data().unwrap();
        let scan = scan_segment(writer.path(), StorageLimits::default()).unwrap();
        assert!(scan.failure.is_none());
        assert_eq!(scan.frames.len(), 1);
        assert_eq!(scan.frames[0].frame, frame);
        fs::remove_dir_all(dir).unwrap();
    }

    #[test]
    fn flush_failure_is_never_reported_as_success() {
        let dir = test_directory("segment", "flush-fault");
        let mut writer = SegmentWriter::create(&dir, header()).unwrap();
        writer.test_fault = Some(TestFault::Flush);
        assert!(matches!(writer.flush_data(), Err(StorageError::Io(_))));
        fs::remove_dir_all(dir).unwrap();
    }
}
