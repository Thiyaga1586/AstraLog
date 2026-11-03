use astralog_common::{PartitionId, TopicId};
use astralog_raft::{LogIndex, SnapshotMetadata};
use astralog_storage::{Frame, StorageLimits, decode_frame, encode_frame};

use crate::{
    ReplicationError, Result,
    codec::{decode_snapshot_metadata, encode_snapshot_metadata},
};

const MAGIC: &[u8; 8] = b"ASTRBTP1";
const VERSION: u16 = 1;
const HEADER_LEN: usize = 48;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RebootstrapLimits {
    pub max_package_bytes: usize,
    pub max_frames: usize,
    pub storage: StorageLimits,
}

impl Default for RebootstrapLimits {
    fn default() -> Self {
        Self {
            max_package_bytes: 512 * 1024 * 1024,
            max_frames: 1_000_000,
            storage: StorageLimits::default(),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RebootstrapPackage {
    pub topic_id: TopicId,
    pub partition_id: PartitionId,
    pub snapshot: SnapshotMetadata,
    pub frames: Vec<Frame>,
}

impl RebootstrapPackage {
    pub fn validate(&self, limits: RebootstrapLimits) -> Result<()> {
        if self.snapshot.last_included_index == LogIndex::ZERO {
            return Err(ReplicationError::Invalid(
                "rebootstrap package requires a non-empty snapshot boundary",
            ));
        }
        if self.frames.is_empty() || self.frames.len() > limits.max_frames {
            return Err(ReplicationError::Invalid(
                "rebootstrap frame count is outside configured bounds",
            ));
        }
        let mut expected = 1_u64;
        for frame in &self.frames {
            if frame.raft_index().get() != expected {
                return Err(ReplicationError::Invalid(
                    "rebootstrap frames are not a contiguous Raft prefix",
                ));
            }
            if frame.raft_term().get() == 0 {
                return Err(ReplicationError::Invalid("rebootstrap frame has term zero"));
            }
            expected = expected
                .checked_add(1)
                .ok_or(ReplicationError::Invalid("rebootstrap Raft index overflow"))?;
        }
        let boundary = self.frames.last().ok_or(ReplicationError::Invalid(
            "rebootstrap package has no boundary frame",
        ))?;
        if boundary.raft_index().get() != self.snapshot.last_included_index.get()
            || boundary.raft_term().get() != self.snapshot.last_included_term.get()
        {
            return Err(ReplicationError::Invalid(
                "rebootstrap boundary frame does not match snapshot metadata",
            ));
        }
        Ok(())
    }
}

pub fn encode_rebootstrap_package(
    package: &RebootstrapPackage,
    limits: RebootstrapLimits,
) -> Result<Vec<u8>> {
    package.validate(limits)?;
    let snapshot = encode_snapshot_metadata(&package.snapshot)?;
    let snapshot_len = u32::try_from(snapshot.len())
        .map_err(|_| ReplicationError::Invalid("snapshot metadata length overflow"))?;
    let frame_count = u32::try_from(package.frames.len())
        .map_err(|_| ReplicationError::Invalid("rebootstrap frame count overflow"))?;
    let mut bytes = Vec::new();
    bytes.extend_from_slice(MAGIC);
    bytes.extend_from_slice(&VERSION.to_le_bytes());
    bytes.extend_from_slice(&(HEADER_LEN as u16).to_le_bytes());
    bytes.extend_from_slice(&0_u32.to_le_bytes());
    bytes.extend_from_slice(package.topic_id.as_bytes());
    bytes.extend_from_slice(&package.partition_id.get().to_le_bytes());
    bytes.extend_from_slice(&snapshot_len.to_le_bytes());
    bytes.extend_from_slice(&frame_count.to_le_bytes());
    bytes.extend_from_slice(&0_u32.to_le_bytes());
    debug_assert_eq!(bytes.len(), HEADER_LEN);
    bytes.extend_from_slice(&snapshot);
    for frame in &package.frames {
        let encoded = encode_frame(frame, limits.storage)?;
        let len = u32::try_from(encoded.len())
            .map_err(|_| ReplicationError::Invalid("rebootstrap frame length overflow"))?;
        bytes.extend_from_slice(&len.to_le_bytes());
        bytes.extend_from_slice(&encoded);
        if bytes.len().saturating_add(4) > limits.max_package_bytes {
            return Err(ReplicationError::LimitExceeded {
                what: "rebootstrap package bytes",
                actual: bytes.len().saturating_add(4),
                max: limits.max_package_bytes,
            });
        }
    }
    let crc = crc32c::crc32c(&bytes);
    bytes.extend_from_slice(&crc.to_le_bytes());
    Ok(bytes)
}

pub fn decode_rebootstrap_package(
    bytes: &[u8],
    limits: RebootstrapLimits,
) -> Result<RebootstrapPackage> {
    if bytes.len() < HEADER_LEN + 4 || bytes.len() > limits.max_package_bytes {
        return Err(ReplicationError::Invalid(
            "rebootstrap package length is outside configured bounds",
        ));
    }
    if &bytes[..8] != MAGIC
        || read_u16(bytes, 8)? != VERSION
        || usize::from(read_u16(bytes, 10)?) != HEADER_LEN
        || read_u32(bytes, 12)? != 0
        || read_u32(bytes, 44)? != 0
    {
        return Err(ReplicationError::Invalid(
            "rebootstrap package header mismatch",
        ));
    }
    let expected_crc = read_u32(bytes, bytes.len() - 4)?;
    let actual_crc = crc32c::crc32c(&bytes[..bytes.len() - 4]);
    if expected_crc != actual_crc {
        return Err(ReplicationError::ChecksumMismatch {
            context: "rebootstrap package",
            expected: expected_crc,
            actual: actual_crc,
        });
    }
    let mut topic = [0_u8; 16];
    topic.copy_from_slice(&bytes[16..32]);
    let partition_id = PartitionId::new(read_u32(bytes, 32)?);
    let snapshot_len = usize::try_from(read_u32(bytes, 36)?)
        .map_err(|_| ReplicationError::Invalid("snapshot metadata length overflow"))?;
    let frame_count = usize::try_from(read_u32(bytes, 40)?)
        .map_err(|_| ReplicationError::Invalid("rebootstrap frame count overflow"))?;
    if frame_count == 0 || frame_count > limits.max_frames {
        return Err(ReplicationError::Invalid("invalid rebootstrap frame count"));
    }
    let snapshot_end = HEADER_LEN
        .checked_add(snapshot_len)
        .ok_or(ReplicationError::Invalid(
            "snapshot metadata length overflow",
        ))?;
    if snapshot_end > bytes.len() - 4 {
        return Err(ReplicationError::Invalid(
            "truncated rebootstrap snapshot metadata",
        ));
    }
    let snapshot = decode_snapshot_metadata(&bytes[HEADER_LEN..snapshot_end])?;
    let mut position = snapshot_end;
    let mut frames = Vec::with_capacity(frame_count);
    for _ in 0..frame_count {
        let len = usize::try_from(read_u32(bytes, position)?)
            .map_err(|_| ReplicationError::Invalid("rebootstrap frame length overflow"))?;
        position = position
            .checked_add(4)
            .ok_or(ReplicationError::Invalid("rebootstrap position overflow"))?;
        let end = position.checked_add(len).ok_or(ReplicationError::Invalid(
            "rebootstrap frame length overflow",
        ))?;
        if end > bytes.len() - 4 {
            return Err(ReplicationError::Invalid("truncated rebootstrap frame"));
        }
        frames.push(decode_frame(&bytes[position..end], limits.storage)?);
        position = end;
    }
    if position != bytes.len() - 4 {
        return Err(ReplicationError::Invalid(
            "trailing bytes in rebootstrap package",
        ));
    }
    let package = RebootstrapPackage {
        topic_id: TopicId::new(topic),
        partition_id,
        snapshot,
        frames,
    };
    package.validate(limits)?;
    if encode_rebootstrap_package(&package, limits)? != bytes {
        return Err(ReplicationError::Invalid(
            "rebootstrap package is not canonical",
        ));
    }
    Ok(package)
}

fn read_u16(bytes: &[u8], offset: usize) -> Result<u16> {
    Ok(u16::from_le_bytes(
        bytes
            .get(offset..offset + 2)
            .ok_or(ReplicationError::Invalid("truncated rebootstrap package"))?
            .try_into()
            .expect("2-byte slice"),
    ))
}

fn read_u32(bytes: &[u8], offset: usize) -> Result<u32> {
    Ok(u32::from_le_bytes(
        bytes
            .get(offset..offset + 4)
            .ok_or(ReplicationError::Invalid("truncated rebootstrap package"))?
            .try_into()
            .expect("4-byte slice"),
    ))
}

#[cfg(test)]
mod tests {
    use astralog_common::{RaftIndex, RaftTerm, StreamOffset};
    use astralog_raft::{NodeId, SnapshotId, StableConfiguration, Term};
    use astralog_storage::{Compression, DataRecord, RecordBatch};

    use super::*;

    fn package() -> RebootstrapPackage {
        let config = StableConfiguration::new(
            [
                NodeId::new(1).unwrap(),
                NodeId::new(2).unwrap(),
                NodeId::new(3).unwrap(),
            ],
            [],
        )
        .unwrap();
        RebootstrapPackage {
            topic_id: TopicId::new([8; 16]),
            partition_id: PartitionId::new(0),
            snapshot: SnapshotMetadata::new(
                LogIndex::new(2),
                Term::new(1),
                config,
                SnapshotId::new([9; 16]),
            )
            .unwrap(),
            frames: (1..=2_u64)
                .map(|index| {
                    Frame::Data(RecordBatch {
                        raft_term: RaftTerm::new(1),
                        raft_index: RaftIndex::new(index).unwrap(),
                        base_stream_offset: StreamOffset::new(index - 1),
                        producer: None,
                        compression: Compression::None,
                        records: vec![DataRecord {
                            timestamp_ms: i64::try_from(index).unwrap(),
                            key: None,
                            value: Some(vec![u8::try_from(index).unwrap()]),
                            headers: Vec::new(),
                        }],
                    })
                })
                .collect(),
        }
    }

    #[test]
    fn package_round_trip_and_crc_are_canonical() {
        let package = package();
        let bytes = encode_rebootstrap_package(&package, RebootstrapLimits::default()).unwrap();
        assert_eq!(
            decode_rebootstrap_package(&bytes, RebootstrapLimits::default()).unwrap(),
            package
        );
        let mut corrupted = bytes;
        corrupted[50] ^= 1;
        assert!(matches!(
            decode_rebootstrap_package(&corrupted, RebootstrapLimits::default()),
            Err(ReplicationError::ChecksumMismatch { .. })
        ));
    }

    #[test]
    fn package_rejects_noncontiguous_or_wrong_boundary() {
        let mut package = package();
        if let Frame::Data(batch) = &mut package.frames[1] {
            batch.raft_index = RaftIndex::new(3).unwrap();
        }
        assert!(package.validate(RebootstrapLimits::default()).is_err());
    }
}
