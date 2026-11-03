//! P9-G retained-history rebootstrap V2 package codec.
//!
//! This is only the bounded, strongly hashed wire/staging representation.
//! It deliberately does NOT grant InstallSnapshot authorization or publish
//! files: P3 authorization plus full on-disk format/producer/maintenance
//! validation must qualify before any caller can install this package.
//! The frozen P4 RebootstrapPackage V1 codec remains byte-for-byte unchanged.
use astralog_common::{PartitionId, StreamOffset, TopicId};
use astralog_raft::{LogIndex, SnapshotMetadata};
use astralog_storage::{SegmentManifest, decode_manifest, encode_manifest};
use sha2::{Digest, Sha256};

use crate::{
    ProducerSnapshotV1, ReplicationError, Result,
    codec::{decode_snapshot_metadata, encode_snapshot_metadata},
    decode_producer_snapshot, encode_producer_snapshot,
};

const MAGIC: &[u8; 8] = b"ASTRBTP2";
const VERSION: u16 = 2;
const HEADER_LEN: usize = 88;
const ENTRY_HEADER_LEN: usize = 48;
const FOOTER_LEN: usize = 4;
const MUTABLE_ACTIVE: u16 = 1;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RebootstrapLimitsV2 {
    pub max_package_bytes: usize,
    pub max_files: usize,
    pub max_file_bytes: usize,
    pub max_snapshot_bytes: usize,
    pub max_manifest_bytes: usize,
    pub max_checkpoint_bytes: usize,
}

impl Default for RebootstrapLimitsV2 {
    fn default() -> Self {
        Self {
            max_package_bytes: 512 * 1024 * 1024,
            max_files: 128,
            max_file_bytes: 32 * 1024 * 1024,
            max_snapshot_bytes: 64 * 1024,
            max_manifest_bytes: 64 * 1024,
            max_checkpoint_bytes: crate::MAX_PRODUCER_SNAPSHOT_BYTES,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RebootstrapFileV2 {
    pub file_name: String,
    pub bytes: Vec<u8>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RebootstrapPackageV2 {
    pub topic_id: TopicId,
    pub partition_id: PartitionId,
    pub snapshot: SnapshotMetadata,
    pub manifest: SegmentManifest,
    pub files: Vec<RebootstrapFileV2>,
    pub committed_log_start_offset: StreamOffset,
    pub committed_compaction_safe_offset: StreamOffset,
    pub next_stream_offset: StreamOffset,
    pub producer_checkpoint: Option<ProducerSnapshotV1>,
}

impl RebootstrapPackageV2 {
    fn validate_shape(
        &self,
        limits: RebootstrapLimitsV2,
        encoded_snapshot_len: usize,
        encoded_manifest_len: usize,
        encoded_checkpoint_len: usize,
    ) -> Result<()> {
        if limits.max_package_bytes <= HEADER_LEN + FOOTER_LEN
            || limits.max_files == 0
            || limits.max_file_bytes == 0
            || limits.max_manifest_bytes == 0
            || limits.max_snapshot_bytes == 0
            || self.snapshot.last_included_index == LogIndex::ZERO
            || self.manifest.generation == 0
            || self.files.is_empty()
            || self.files.len() > limits.max_files
            || self.files.len() != self.manifest.entries.len()
            || encoded_snapshot_len > limits.max_snapshot_bytes
            || encoded_manifest_len > limits.max_manifest_bytes
            || encoded_checkpoint_len > limits.max_checkpoint_bytes
            || self.committed_log_start_offset > self.next_stream_offset
            || self.committed_compaction_safe_offset > self.next_stream_offset
        {
            return Err(ReplicationError::Invalid(
                "P9-G V2 rebootstrap package shape exceeds limits or violates bounds",
            ));
        }
        let mut previous = None;
        for (index, (file, manifest_entry)) in
            self.files.iter().zip(&self.manifest.entries).enumerate()
        {
            let is_last = index + 1 == self.files.len();
            if file.file_name != manifest_entry.file_name
                || file.file_name.len() > 255
                || file.bytes.is_empty()
                || file.bytes.len() > limits.max_file_bytes
                || manifest_entry.is_mutable_active() != is_last
                || manifest_entry
                    .expected_size
                    .is_some_and(|expected| expected != file.bytes.len() as u64)
                || previous
                    .as_ref()
                    .is_some_and(|old: &String| old == &file.file_name)
            {
                return Err(ReplicationError::Invalid(
                    "P9-G V2 retained file order/size/manifest entry mismatch",
                ));
            }
            previous = Some(file.file_name.clone());
        }
        if let Some(checkpoint) = &self.producer_checkpoint
            && (checkpoint.topic_id != self.topic_id
                || checkpoint.partition_id != self.partition_id
                || checkpoint.committed_raft_index > self.snapshot.last_included_index.get())
        {
            return Err(ReplicationError::Invalid(
                "P9-G V2 producer checkpoint identity exceeds P3 snapshot boundary",
            ));
        }
        Ok(())
    }
}

pub fn encode_rebootstrap_package_v2(
    package: &RebootstrapPackageV2,
    limits: RebootstrapLimitsV2,
) -> Result<Vec<u8>> {
    let snapshot = encode_snapshot_metadata(&package.snapshot)?;
    let manifest = encode_manifest(&package.manifest)?;
    let checkpoint = package
        .producer_checkpoint
        .as_ref()
        .map(encode_producer_snapshot)
        .transpose()?
        .unwrap_or_default();
    package.validate_shape(limits, snapshot.len(), manifest.len(), checkpoint.len())?;
    let fixed = HEADER_LEN
        .checked_add(snapshot.len())
        .and_then(|n| n.checked_add(manifest.len()))
        .and_then(|n| n.checked_add(checkpoint.len()))
        .and_then(|n| n.checked_add(FOOTER_LEN))
        .ok_or(ReplicationError::Invalid("P9-G V2 length overflow"))?;
    let total = package.files.iter().try_fold(fixed, |acc, file| {
        acc.checked_add(ENTRY_HEADER_LEN)
            .and_then(|n| n.checked_add(file.file_name.len()))
            .and_then(|n| n.checked_add(file.bytes.len()))
            .ok_or(ReplicationError::Invalid("P9-G V2 file size overflow"))
    })?;
    if total > limits.max_package_bytes {
        return Err(ReplicationError::LimitExceeded {
            what: "P9-G V2 package bytes",
            actual: total,
            max: limits.max_package_bytes,
        });
    }
    let mut out = Vec::with_capacity(total);
    out.extend_from_slice(MAGIC);
    out.extend_from_slice(&VERSION.to_le_bytes());
    out.extend_from_slice(&(HEADER_LEN as u16).to_le_bytes());
    out.extend_from_slice(&0_u32.to_le_bytes());
    out.extend_from_slice(package.topic_id.as_bytes());
    out.extend_from_slice(&package.partition_id.get().to_le_bytes());
    out.extend_from_slice(&checked_u32(snapshot.len())?.to_le_bytes());
    out.extend_from_slice(&checked_u32(manifest.len())?.to_le_bytes());
    out.extend_from_slice(&checked_u32(package.files.len())?.to_le_bytes());
    out.extend_from_slice(&checked_u32(checkpoint.len())?.to_le_bytes());
    out.extend_from_slice(&package.next_stream_offset.get().to_le_bytes());
    out.extend_from_slice(&package.committed_log_start_offset.get().to_le_bytes());
    out.extend_from_slice(&package.committed_compaction_safe_offset.get().to_le_bytes());
    out.extend_from_slice(
        &u64::try_from(total)
            .map_err(|_| ReplicationError::Invalid("P9-G V2 length u64 overflow"))?
            .to_le_bytes(),
    );
    out.extend_from_slice(&0_u32.to_le_bytes());
    debug_assert_eq!(out.len(), HEADER_LEN);
    out.extend_from_slice(&snapshot);
    out.extend_from_slice(&manifest);
    out.extend_from_slice(&checkpoint);
    for (file, entry) in package.files.iter().zip(&package.manifest.entries) {
        let name = file.file_name.as_bytes();
        out.extend_from_slice(&(name.len() as u16).to_le_bytes());
        out.extend_from_slice(
            &(if entry.is_mutable_active() {
                MUTABLE_ACTIVE
            } else {
                0
            })
            .to_le_bytes(),
        );
        out.extend_from_slice(&(file.bytes.len() as u64).to_le_bytes());
        out.extend_from_slice(&Sha256::digest(&file.bytes));
        out.extend_from_slice(&crc32c::crc32c(&file.bytes).to_le_bytes());
        out.extend_from_slice(name);
        out.extend_from_slice(&file.bytes);
    }
    debug_assert_eq!(out.len() + FOOTER_LEN, total);
    let crc = crc32c::crc32c(&out);
    out.extend_from_slice(&crc.to_le_bytes());
    Ok(out)
}

pub fn decode_rebootstrap_package_v2(
    bytes: &[u8],
    limits: RebootstrapLimitsV2,
) -> Result<RebootstrapPackageV2> {
    if bytes.len() < HEADER_LEN + FOOTER_LEN
        || bytes.len() > limits.max_package_bytes
        || &bytes[..8] != MAGIC
        || read_u16(bytes, 8)? != VERSION
        || usize::from(read_u16(bytes, 10)?) != HEADER_LEN
        || read_u32(bytes, 12)? != 0
        || read_u32(bytes, 84)? != 0
        || read_u64(bytes, 76)? != bytes.len() as u64
    {
        return Err(ReplicationError::Invalid(
            "invalid P9-G V2 package header/length",
        ));
    }
    let expected_crc = read_u32(bytes, bytes.len() - FOOTER_LEN)?;
    let actual_crc = crc32c::crc32c(&bytes[..bytes.len() - FOOTER_LEN]);
    if expected_crc != actual_crc {
        return Err(ReplicationError::ChecksumMismatch {
            context: "P9-G V2 package",
            expected: expected_crc,
            actual: actual_crc,
        });
    }
    let snapshot_len = read_u32(bytes, 36)? as usize;
    let manifest_len = read_u32(bytes, 40)? as usize;
    let file_count = read_u32(bytes, 44)? as usize;
    let checkpoint_len = read_u32(bytes, 48)? as usize;
    if file_count == 0
        || file_count > limits.max_files
        || snapshot_len > limits.max_snapshot_bytes
        || manifest_len > limits.max_manifest_bytes
        || checkpoint_len > limits.max_checkpoint_bytes
    {
        return Err(ReplicationError::Invalid(
            "P9-G V2 file count/metadata size exceeds limit",
        ));
    }
    let payload_end = bytes.len() - FOOTER_LEN;
    let mut cursor = HEADER_LEN;
    let snap_bytes = take(bytes, &mut cursor, snapshot_len, payload_end)?;
    let snapshot = decode_snapshot_metadata(snap_bytes)?;
    if encode_snapshot_metadata(&snapshot)? != snap_bytes {
        return Err(ReplicationError::Invalid("noncanonical P9-G V2 snapshot"));
    }
    let manifest_bytes = take(bytes, &mut cursor, manifest_len, payload_end)?;
    let manifest = decode_manifest(manifest_bytes)?;
    if encode_manifest(&manifest)? != manifest_bytes {
        return Err(ReplicationError::Invalid("noncanonical P9-G V2 manifest"));
    }
    let checkpoint_bytes = take(bytes, &mut cursor, checkpoint_len, payload_end)?;
    let checkpoint = if checkpoint_bytes.is_empty() {
        None
    } else {
        let recovered = decode_producer_snapshot(checkpoint_bytes)?;
        if encode_producer_snapshot(&recovered)? != checkpoint_bytes {
            return Err(ReplicationError::Invalid(
                "noncanonical P9-G V2 producer checkpoint",
            ));
        }
        Some(recovered)
    };
    if manifest.entries.len() != file_count {
        return Err(ReplicationError::Invalid(
            "P9-G V2 manifest/file count mismatch",
        ));
    }
    let mut files = Vec::with_capacity(file_count);
    for (index, entry) in manifest.entries.iter().enumerate() {
        let name_len = read_u16(bytes, cursor)? as usize;
        let flags = read_u16(bytes, cursor + 2)?;
        let len = read_u64(bytes, cursor + 4)?;
        let len = usize::try_from(len)
            .map_err(|_| ReplicationError::Invalid("P9-G V2 file usize overflow"))?;
        if name_len == 0
            || name_len > 255
            || len == 0
            || len > limits.max_file_bytes
            || flags
                != if index + 1 == file_count {
                    MUTABLE_ACTIVE
                } else {
                    0
                }
            || entry.is_mutable_active() != (index + 1 == file_count)
            || entry
                .expected_size
                .is_some_and(|expected| expected != len as u64)
        {
            return Err(ReplicationError::Invalid(
                "P9-G V2 file header/manifest mismatch",
            ));
        }
        let header = take(bytes, &mut cursor, ENTRY_HEADER_LEN, payload_end)?;
        let name_bytes = take(bytes, &mut cursor, name_len, payload_end)?;
        if name_bytes != entry.file_name.as_bytes() {
            return Err(ReplicationError::Invalid(
                "P9-G V2 file name differs from manifest",
            ));
        }
        let payload = take(bytes, &mut cursor, len, payload_end)?;
        let crc = u32::from_le_bytes(header[44..48].try_into().unwrap());
        if crc32c::crc32c(payload) != crc || Sha256::digest(payload).as_slice() != &header[12..44] {
            return Err(ReplicationError::Invalid(
                "P9-G V2 retained file SHA-256/CRC32C mismatch",
            ));
        }
        files.push(RebootstrapFileV2 {
            file_name: entry.file_name.clone(),
            bytes: payload.to_vec(),
        });
    }
    if cursor != payload_end {
        return Err(ReplicationError::Invalid("P9-G V2 trailing payload"));
    }
    let mut topic_id = [0_u8; 16];
    topic_id.copy_from_slice(&bytes[16..32]);
    let package = RebootstrapPackageV2 {
        topic_id: TopicId::new(topic_id),
        partition_id: PartitionId::new(read_u32(bytes, 32)?),
        snapshot,
        manifest,
        files,
        committed_log_start_offset: StreamOffset::new(read_u64(bytes, 60)?),
        committed_compaction_safe_offset: StreamOffset::new(read_u64(bytes, 68)?),
        next_stream_offset: StreamOffset::new(read_u64(bytes, 52)?),
        producer_checkpoint: checkpoint,
    };
    package.validate_shape(limits, snapshot_len, manifest_len, checkpoint_len)?;
    Ok(package)
}

fn checked_u32(value: usize) -> Result<u32> {
    u32::try_from(value).map_err(|_| ReplicationError::Invalid("P9-G V2 count u32 overflow"))
}

fn take<'a>(bytes: &'a [u8], cursor: &mut usize, len: usize, upper: usize) -> Result<&'a [u8]> {
    let end = cursor
        .checked_add(len)
        .ok_or(ReplicationError::Invalid("P9-G V2 cursor overflow"))?;
    if end > upper {
        return Err(ReplicationError::Invalid("P9-G V2 truncated payload"));
    }
    let result = &bytes[*cursor..end];
    *cursor = end;
    Ok(result)
}

fn read_u16(bytes: &[u8], index: usize) -> Result<u16> {
    Ok(u16::from_le_bytes(
        bytes
            .get(index..index.saturating_add(2))
            .ok_or(ReplicationError::Invalid("truncated P9-G V2 u16"))?
            .try_into()
            .expect("2 bytes"),
    ))
}
fn read_u32(bytes: &[u8], index: usize) -> Result<u32> {
    Ok(u32::from_le_bytes(
        bytes
            .get(index..index.saturating_add(4))
            .ok_or(ReplicationError::Invalid("truncated P9-G V2 u32"))?
            .try_into()
            .expect("4 bytes"),
    ))
}
fn read_u64(bytes: &[u8], index: usize) -> Result<u64> {
    Ok(u64::from_le_bytes(
        bytes
            .get(index..index.saturating_add(8))
            .ok_or(ReplicationError::Invalid("truncated P9-G V2 u64"))?
            .try_into()
            .expect("8 bytes"),
    ))
}

#[cfg(test)]
mod tests {
    use std::collections::{BTreeMap, VecDeque};

    use super::*;
    use astralog_common::{ProducerEpoch, ProducerId, ProducerSequence, RaftIndex};
    use astralog_raft::{NodeId, SnapshotId, StableConfiguration, Term};
    use astralog_storage::ManifestEntry;

    use crate::{PartitionProducerStateV1, RecentBatchV1};

    fn package() -> RebootstrapPackageV2 {
        let topic_id = TopicId::new([0x87; 16]);
        let snapshot = SnapshotMetadata::new(
            LogIndex::new(3),
            Term::new(2),
            StableConfiguration::new([NodeId::new(1).unwrap()], []).unwrap(),
            SnapshotId::new([0x44; 16]),
        )
        .unwrap();
        RebootstrapPackageV2 {
            topic_id,
            partition_id: PartitionId::new(5),
            snapshot,
            manifest: SegmentManifest {
                generation: 7,
                entries: vec![
                    ManifestEntry::immutable("000000001-retained.log", 11),
                    ManifestEntry::mutable_active("000000004-live.log"),
                ],
            },
            files: vec![
                RebootstrapFileV2 {
                    file_name: "000000001-retained.log".into(),
                    bytes: b"retained-v2".to_vec(),
                },
                RebootstrapFileV2 {
                    file_name: "000000004-live.log".into(),
                    bytes: b"mutable-v1".to_vec(),
                },
            ],
            committed_log_start_offset: StreamOffset::new(2),
            committed_compaction_safe_offset: StreamOffset::new(3),
            next_stream_offset: StreamOffset::new(5),
            producer_checkpoint: None,
        }
    }

    fn package_with_producer_checkpoint() -> RebootstrapPackageV2 {
        let mut package = package();
        let producer_id = ProducerId::new(17).unwrap();
        let state = PartitionProducerStateV1::from_snapshot_parts(
            producer_id,
            ProducerEpoch::new(3).unwrap(),
            ProducerSequence::new(1),
            VecDeque::from([RecentBatchV1 {
                base_sequence: ProducerSequence::ZERO,
                last_sequence: ProducerSequence::ZERO,
                record_count: 1,
                base_stream_offset: StreamOffset::new(4),
                last_stream_offset: StreamOffset::new(4),
                raft_index: RaftIndex::new(3).unwrap(),
                canonical_digest: [0x5a; 32],
            }]),
        )
        .unwrap();
        package.producer_checkpoint = Some(
            ProducerSnapshotV1::new(
                package.topic_id,
                package.partition_id,
                package.snapshot.last_included_index.get(),
                BTreeMap::from([(producer_id, state)]),
            )
            .unwrap(),
        );
        package
    }

    fn rewrite_outer_crc(bytes: &mut [u8]) {
        let crc_offset = bytes.len() - FOOTER_LEN;
        let crc = crc32c::crc32c(&bytes[..crc_offset]);
        bytes[crc_offset..].copy_from_slice(&crc.to_le_bytes());
    }

    #[test]
    fn p9g_v2_codec_roundtrip_is_versioned_bounded_and_v1_unmodified() {
        let source = package();
        let limits = RebootstrapLimitsV2::default();
        let bytes = encode_rebootstrap_package_v2(&source, limits).unwrap();
        assert_eq!(&bytes[..8], MAGIC);
        assert_eq!(read_u16(&bytes, 8).unwrap(), 2);
        assert_eq!(
            decode_rebootstrap_package_v2(&bytes, limits).unwrap(),
            source
        );
        assert!(
            crate::decode_rebootstrap_package(&bytes, crate::RebootstrapLimits::default()).is_err(),
            "frozen P4 V1 decoder rejects V2 distinct magic"
        );
        let reduced = RebootstrapLimitsV2 {
            max_files: 1,
            ..limits
        };
        assert!(decode_rebootstrap_package_v2(&bytes, reduced).is_err());
        assert!(encode_rebootstrap_package_v2(&source, reduced).is_err());
        let tiny = RebootstrapLimitsV2 {
            max_package_bytes: bytes.len() - 1,
            ..limits
        };
        assert!(decode_rebootstrap_package_v2(&bytes, tiny).is_err());
        assert!(encode_rebootstrap_package_v2(&source, tiny).is_err());
        assert!(decode_rebootstrap_package_v2(&bytes[..bytes.len() - 1], limits).is_err());
    }

    #[test]
    fn p9g_v2_codec_rejects_sha_crc_manifest_and_identity_tamper() {
        let original = package();
        let limits = RebootstrapLimitsV2::default();
        let bytes = encode_rebootstrap_package_v2(&original, limits).unwrap();
        let mut corrupt = bytes.clone();
        let corrupt_index = corrupt.len() - FOOTER_LEN - 1;
        corrupt[corrupt_index] ^= 1;
        assert!(decode_rebootstrap_package_v2(&corrupt, limits).is_err());
        let payload_crc = crc32c::crc32c(&corrupt[..corrupt.len() - FOOTER_LEN]);
        let last = corrupt.len() - FOOTER_LEN;
        corrupt[last..].copy_from_slice(&payload_crc.to_le_bytes());
        assert!(
            decode_rebootstrap_package_v2(&corrupt, limits).is_err(),
            "forged outer CRC cannot bypass per-file SHA-256"
        );
        let mut duplicate = original.clone();
        duplicate.files[1].file_name = duplicate.files[0].file_name.clone();
        assert!(encode_rebootstrap_package_v2(&duplicate, limits).is_err());
        let mut invalid = original.clone();
        invalid.manifest.entries[0].expected_size = Some(17);
        assert!(encode_rebootstrap_package_v2(&invalid, limits).is_err());
        let mut boundary = original.clone();
        boundary.committed_compaction_safe_offset = StreamOffset::new(6);
        assert!(encode_rebootstrap_package_v2(&boundary, limits).is_err());
        let mut empty = original;
        empty.files.clear();
        assert!(encode_rebootstrap_package_v2(&empty, limits).is_err());
    }

    #[test]
    fn p9g_v2_producer_checkpoint_roundtrip_is_bounded_and_identity_bound() {
        let source = package_with_producer_checkpoint();
        let limits = RebootstrapLimitsV2::default();
        let bytes = encode_rebootstrap_package_v2(&source, limits).unwrap();
        assert_eq!(
            decode_rebootstrap_package_v2(&bytes, limits).unwrap(),
            source
        );

        let checkpoint_len = read_u32(&bytes, 48).unwrap() as usize;
        assert!(checkpoint_len > 0);
        let checkpoint_too_small = RebootstrapLimitsV2 {
            max_checkpoint_bytes: checkpoint_len - 1,
            ..limits
        };
        assert!(encode_rebootstrap_package_v2(&source, checkpoint_too_small).is_err());
        assert!(decode_rebootstrap_package_v2(&bytes, checkpoint_too_small).is_err());

        let mut wrong_topic = source.clone();
        wrong_topic.producer_checkpoint.as_mut().unwrap().topic_id = TopicId::new([0x13; 16]);
        assert!(encode_rebootstrap_package_v2(&wrong_topic, limits).is_err());

        let mut wrong_partition = source.clone();
        wrong_partition
            .producer_checkpoint
            .as_mut()
            .unwrap()
            .partition_id = PartitionId::new(6);
        assert!(encode_rebootstrap_package_v2(&wrong_partition, limits).is_err());

        let mut ahead = source;
        ahead
            .producer_checkpoint
            .as_mut()
            .unwrap()
            .committed_raft_index = 4;
        assert!(encode_rebootstrap_package_v2(&ahead, limits).is_err());
    }

    #[test]
    fn p9g_v2_nested_manifest_and_producer_crc_cannot_be_hidden_by_outer_crc() {
        let source = package_with_producer_checkpoint();
        let limits = RebootstrapLimitsV2::default();
        let bytes = encode_rebootstrap_package_v2(&source, limits).unwrap();
        let snapshot_len = read_u32(&bytes, 36).unwrap() as usize;
        let manifest_len = read_u32(&bytes, 40).unwrap() as usize;
        let checkpoint_len = read_u32(&bytes, 48).unwrap() as usize;
        let manifest_start = HEADER_LEN + snapshot_len;
        let checkpoint_start = manifest_start + manifest_len;
        let checkpoint_end = checkpoint_start + checkpoint_len;

        let mut manifest_corrupt = bytes.clone();
        manifest_corrupt[manifest_start + 16] ^= 0x01;
        rewrite_outer_crc(&mut manifest_corrupt);
        assert!(decode_rebootstrap_package_v2(&manifest_corrupt, limits).is_err());

        let mut checkpoint_corrupt = bytes.clone();
        checkpoint_corrupt[checkpoint_start + 16] ^= 0x01;
        rewrite_outer_crc(&mut checkpoint_corrupt);
        assert!(decode_rebootstrap_package_v2(&checkpoint_corrupt, limits).is_err());

        // Recompute both nested and outer CRCs after changing the checkpoint
        // identity. The producer snapshot is internally valid, but it no
        // longer belongs to this V2 package and must still fail closed.
        let mut forged_checkpoint_identity = bytes;
        forged_checkpoint_identity[checkpoint_start + 16] ^= 0x01;
        let checkpoint_crc_offset = checkpoint_end - 4;
        let checkpoint_crc =
            crc32c::crc32c(&forged_checkpoint_identity[checkpoint_start..checkpoint_crc_offset]);
        forged_checkpoint_identity[checkpoint_crc_offset..checkpoint_end]
            .copy_from_slice(&checkpoint_crc.to_le_bytes());
        rewrite_outer_crc(&mut forged_checkpoint_identity);
        assert!(decode_rebootstrap_package_v2(&forged_checkpoint_identity, limits).is_err());
    }
}
