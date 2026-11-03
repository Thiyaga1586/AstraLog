use std::{
    collections::{BTreeMap, VecDeque},
    fs::{self, OpenOptions},
    io::Write,
    path::{Path, PathBuf},
};

#[cfg(unix)]
use std::fs::File;

use astralog_common::{
    PartitionId, ProducerEpoch, ProducerId, ProducerSequence, RaftIndex, StreamOffset, TopicId,
};

use crate::{
    MAX_RECENT_COMMITTED_BATCHES, PartitionProducerStateV1, RecentBatchV1, ReplicationError, Result,
};

pub const MAX_PRODUCER_SNAPSHOT_BYTES: usize = 64 * 1024 * 1024;
const PRODUCER_SNAPSHOT_MAGIC: &[u8; 8] = b"ASTRPSN1";
const PRODUCER_SNAPSHOT_VERSION: u16 = 1;
const PRODUCER_SNAPSHOT_HEADER_LEN: usize = 64;
const PRODUCER_ENTRY_HEADER_LEN: usize = 24;
const RECENT_BATCH_LEN: usize = 72;
const MIN_PRODUCER_ENTRY_LEN: usize = PRODUCER_ENTRY_HEADER_LEN + RECENT_BATCH_LEN;
const CRC_LEN: usize = 4;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ProducerSnapshotV1 {
    pub topic_id: TopicId,
    pub partition_id: PartitionId,
    pub committed_raft_index: u64,
    pub states: BTreeMap<ProducerId, PartitionProducerStateV1>,
}

impl ProducerSnapshotV1 {
    pub fn new(
        topic_id: TopicId,
        partition_id: PartitionId,
        committed_raft_index: u64,
        states: BTreeMap<ProducerId, PartitionProducerStateV1>,
    ) -> Result<Self> {
        if committed_raft_index == 0 && !states.is_empty() {
            return Err(ReplicationError::Invalid(
                "producer snapshot with state requires a non-zero committed Raft boundary",
            ));
        }
        for (producer_id, state) in &states {
            if *producer_id != state.producer_id() {
                return Err(ReplicationError::Invalid(
                    "producer snapshot map key differs from state producer id",
                ));
            }
            if state
                .recent_committed_batches()
                .iter()
                .any(|batch| batch.raft_index.get() > committed_raft_index)
            {
                return Err(ReplicationError::Invalid(
                    "producer snapshot contains retry state beyond its committed boundary",
                ));
            }
        }
        Ok(Self {
            topic_id,
            partition_id,
            committed_raft_index,
            states,
        })
    }
}

#[derive(Debug, Clone)]
pub struct ProducerSnapshotStore {
    path: PathBuf,
}

impl ProducerSnapshotStore {
    pub fn new(directory: &Path) -> Self {
        Self {
            path: directory.join("producer.snapshot"),
        }
    }

    pub fn load(&self) -> Result<Option<ProducerSnapshotV1>> {
        let metadata = match fs::metadata(&self.path) {
            Ok(metadata) => metadata,
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(None),
            Err(error) => return Err(error.into()),
        };
        let encoded_len = usize::try_from(metadata.len()).map_err(|_| {
            ReplicationError::Invalid("producer snapshot file length does not fit address space")
        })?;
        if encoded_len > MAX_PRODUCER_SNAPSHOT_BYTES {
            return Err(ReplicationError::LimitExceeded {
                what: "producer snapshot bytes",
                actual: encoded_len,
                max: MAX_PRODUCER_SNAPSHOT_BYTES,
            });
        }
        let bytes = fs::read(&self.path)?;
        Ok(Some(decode_producer_snapshot(&bytes)?))
    }

    pub fn persist(&self, snapshot: &ProducerSnapshotV1) -> Result<()> {
        let bytes = encode_producer_snapshot(snapshot)?;
        write_snapshot_atomic(&self.path, &bytes, None)
    }

    pub fn discard(&self) -> Result<()> {
        match fs::remove_file(&self.path) {
            Ok(()) => sync_parent(&self.path),
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(()),
            Err(error) => Err(error.into()),
        }
    }

    pub fn path(&self) -> &Path {
        &self.path
    }
}

pub fn encode_producer_snapshot(snapshot: &ProducerSnapshotV1) -> Result<Vec<u8>> {
    encode_producer_snapshot_with_limit(snapshot, MAX_PRODUCER_SNAPSHOT_BYTES)
}

pub(crate) fn encode_producer_snapshot_with_limit(
    snapshot: &ProducerSnapshotV1,
    max_bytes: usize,
) -> Result<Vec<u8>> {
    ProducerSnapshotV1::new(
        snapshot.topic_id,
        snapshot.partition_id,
        snapshot.committed_raft_index,
        snapshot.states.clone(),
    )?;
    let producer_count = u32::try_from(snapshot.states.len())
        .map_err(|_| ReplicationError::Invalid("producer snapshot count overflow"))?;
    let mut entries_len = 0_usize;
    for state in snapshot.states.values() {
        let recent_bytes = state
            .recent_committed_batches()
            .len()
            .checked_mul(RECENT_BATCH_LEN)
            .ok_or(ReplicationError::Invalid(
                "producer snapshot entry length overflow",
            ))?;
        entries_len = entries_len
            .checked_add(PRODUCER_ENTRY_HEADER_LEN)
            .and_then(|value| value.checked_add(recent_bytes))
            .ok_or(ReplicationError::Invalid(
                "producer snapshot length overflow",
            ))?;
    }
    let total_len = PRODUCER_SNAPSHOT_HEADER_LEN
        .checked_add(entries_len)
        .and_then(|value| value.checked_add(CRC_LEN))
        .ok_or(ReplicationError::Invalid(
            "producer snapshot length overflow",
        ))?;
    if total_len > max_bytes {
        return Err(ReplicationError::LimitExceeded {
            what: "producer snapshot bytes",
            actual: total_len,
            max: max_bytes,
        });
    }
    let entries_len_u32 = u32::try_from(entries_len)
        .map_err(|_| ReplicationError::Invalid("producer snapshot entries length overflow"))?;
    let mut bytes = Vec::with_capacity(total_len);
    bytes.extend_from_slice(PRODUCER_SNAPSHOT_MAGIC);
    bytes.extend_from_slice(&PRODUCER_SNAPSHOT_VERSION.to_le_bytes());
    bytes.extend_from_slice(&(PRODUCER_SNAPSHOT_HEADER_LEN as u16).to_le_bytes());
    bytes.extend_from_slice(&0_u32.to_le_bytes());
    bytes.extend_from_slice(snapshot.topic_id.as_bytes());
    bytes.extend_from_slice(&snapshot.partition_id.get().to_le_bytes());
    bytes.extend_from_slice(&0_u32.to_le_bytes());
    bytes.extend_from_slice(&snapshot.committed_raft_index.to_le_bytes());
    bytes.extend_from_slice(&producer_count.to_le_bytes());
    bytes.extend_from_slice(&entries_len_u32.to_le_bytes());
    bytes.extend_from_slice(&0_u64.to_le_bytes());

    for (producer_id, state) in &snapshot.states {
        bytes.extend_from_slice(&producer_id.get().to_le_bytes());
        bytes.extend_from_slice(&state.accepted_epoch().get().to_le_bytes());
        bytes.extend_from_slice(&state.next_sequence().get().to_le_bytes());
        bytes.push(
            u8::try_from(state.recent_committed_batches().len())
                .expect("producer retry window is bounded to five entries"),
        );
        bytes.extend_from_slice(&[0_u8; 7]);
        for recent in state.recent_committed_batches() {
            encode_recent_batch(&mut bytes, recent);
        }
    }
    debug_assert_eq!(bytes.len(), total_len - CRC_LEN);
    let crc = crc32c::crc32c(&bytes);
    bytes.extend_from_slice(&crc.to_le_bytes());
    Ok(bytes)
}

pub fn decode_producer_snapshot(bytes: &[u8]) -> Result<ProducerSnapshotV1> {
    decode_producer_snapshot_with_limit(bytes, MAX_PRODUCER_SNAPSHOT_BYTES)
}

pub(crate) fn decode_producer_snapshot_with_limit(
    bytes: &[u8],
    max_bytes: usize,
) -> Result<ProducerSnapshotV1> {
    if bytes.len() > max_bytes {
        return Err(ReplicationError::LimitExceeded {
            what: "producer snapshot bytes",
            actual: bytes.len(),
            max: max_bytes,
        });
    }
    if bytes.len() < PRODUCER_SNAPSHOT_HEADER_LEN + CRC_LEN {
        return Err(ReplicationError::Invalid("producer snapshot is truncated"));
    }
    if &bytes[0..8] != PRODUCER_SNAPSHOT_MAGIC
        || read_u16(bytes, 8)? != PRODUCER_SNAPSHOT_VERSION
        || usize::from(read_u16(bytes, 10)?) != PRODUCER_SNAPSHOT_HEADER_LEN
        || read_u32(bytes, 12)? != 0
        || read_u32(bytes, 36)? != 0
        || read_u64(bytes, 56)? != 0
    {
        return Err(ReplicationError::Invalid(
            "producer snapshot header mismatch",
        ));
    }
    let expected_crc = read_u32(bytes, bytes.len() - CRC_LEN)?;
    let actual_crc = crc32c::crc32c(&bytes[..bytes.len() - CRC_LEN]);
    if expected_crc != actual_crc {
        return Err(ReplicationError::ChecksumMismatch {
            context: "producer snapshot",
            expected: expected_crc,
            actual: actual_crc,
        });
    }
    let entries_len = usize::try_from(read_u32(bytes, 52)?)
        .map_err(|_| ReplicationError::Invalid("producer snapshot entries length overflow"))?;
    if PRODUCER_SNAPSHOT_HEADER_LEN
        .checked_add(entries_len)
        .and_then(|value| value.checked_add(CRC_LEN))
        != Some(bytes.len())
    {
        return Err(ReplicationError::Invalid(
            "producer snapshot length mismatch",
        ));
    }
    let producer_count = usize::try_from(read_u32(bytes, 48)?)
        .map_err(|_| ReplicationError::Invalid("producer snapshot count overflow"))?;
    if producer_count > entries_len / MIN_PRODUCER_ENTRY_LEN {
        return Err(ReplicationError::Invalid(
            "producer snapshot count cannot fit encoded entry bytes",
        ));
    }
    let mut topic = [0_u8; 16];
    topic.copy_from_slice(&bytes[16..32]);
    let topic_id = TopicId::new(topic);
    let partition_id = PartitionId::new(read_u32(bytes, 32)?);
    let committed_raft_index = read_u64(bytes, 40)?;
    let entries_end = PRODUCER_SNAPSHOT_HEADER_LEN + entries_len;
    let mut cursor = PRODUCER_SNAPSHOT_HEADER_LEN;
    let mut states = BTreeMap::new();
    let mut previous_producer = None;
    for _ in 0..producer_count {
        if cursor
            .checked_add(PRODUCER_ENTRY_HEADER_LEN)
            .is_none_or(|end| end > entries_end)
        {
            return Err(ReplicationError::Invalid(
                "producer snapshot entry is truncated",
            ));
        }
        let producer_id = ProducerId::new(read_u64(bytes, cursor)?).ok_or(
            ReplicationError::Invalid("producer snapshot contains zero ProducerId"),
        )?;
        if previous_producer.is_some_and(|previous| producer_id <= previous) {
            return Err(ReplicationError::Invalid(
                "producer snapshot entries are not strictly sorted",
            ));
        }
        previous_producer = Some(producer_id);
        let accepted_epoch = ProducerEpoch::new(read_u32(bytes, cursor + 8)?).ok_or(
            ReplicationError::Invalid("producer snapshot contains zero ProducerEpoch"),
        )?;
        let next_sequence = ProducerSequence::new(read_u32(bytes, cursor + 12)?);
        let recent_count = usize::from(bytes[cursor + 16]);
        if recent_count == 0 || recent_count > MAX_RECENT_COMMITTED_BATCHES {
            return Err(ReplicationError::Invalid(
                "producer snapshot recent-batch count is outside 1..=5",
            ));
        }
        if bytes[cursor + 17..cursor + 24]
            .iter()
            .any(|byte| *byte != 0)
        {
            return Err(ReplicationError::Invalid(
                "producer snapshot entry reserved bytes are non-zero",
            ));
        }
        cursor += PRODUCER_ENTRY_HEADER_LEN;
        let recent_bytes =
            recent_count
                .checked_mul(RECENT_BATCH_LEN)
                .ok_or(ReplicationError::Invalid(
                    "producer snapshot recent length overflow",
                ))?;
        if cursor
            .checked_add(recent_bytes)
            .is_none_or(|end| end > entries_end)
        {
            return Err(ReplicationError::Invalid(
                "producer snapshot recent-batch bytes are truncated",
            ));
        }
        let mut recent = VecDeque::new();
        for _ in 0..recent_count {
            let batch = decode_recent_batch(bytes, cursor)?;
            if batch.raft_index.get() > committed_raft_index {
                return Err(ReplicationError::Invalid(
                    "producer snapshot retry metadata exceeds snapshot boundary",
                ));
            }
            recent.push_back(batch);
            cursor += RECENT_BATCH_LEN;
        }
        let state = PartitionProducerStateV1::from_snapshot_parts(
            producer_id,
            accepted_epoch,
            next_sequence,
            recent,
        )
        .map_err(|error| {
            ReplicationError::InvalidOwned(format!(
                "invalid producer snapshot state for {}: {error:?}",
                producer_id.get()
            ))
        })?;
        states.insert(producer_id, state);
    }
    if cursor != entries_end {
        return Err(ReplicationError::Invalid(
            "producer snapshot contains trailing entry bytes",
        ));
    }
    ProducerSnapshotV1::new(topic_id, partition_id, committed_raft_index, states)
}

fn encode_recent_batch(bytes: &mut Vec<u8>, recent: &RecentBatchV1) {
    bytes.extend_from_slice(&recent.base_sequence.get().to_le_bytes());
    bytes.extend_from_slice(&recent.last_sequence.get().to_le_bytes());
    bytes.extend_from_slice(&recent.record_count.to_le_bytes());
    bytes.extend_from_slice(&0_u32.to_le_bytes());
    bytes.extend_from_slice(&recent.base_stream_offset.get().to_le_bytes());
    bytes.extend_from_slice(&recent.last_stream_offset.get().to_le_bytes());
    bytes.extend_from_slice(&recent.raft_index.get().to_le_bytes());
    bytes.extend_from_slice(&recent.canonical_digest);
}

fn decode_recent_batch(bytes: &[u8], offset: usize) -> Result<RecentBatchV1> {
    if read_u32(bytes, offset + 12)? != 0 {
        return Err(ReplicationError::Invalid(
            "producer snapshot recent-batch reserved bytes are non-zero",
        ));
    }
    let raft_index = RaftIndex::new(read_u64(bytes, offset + 32)?).ok_or(
        ReplicationError::Invalid("producer snapshot recent batch has zero Raft index"),
    )?;
    let mut digest = [0_u8; 32];
    digest.copy_from_slice(bytes.get(offset + 40..offset + 72).ok_or(
        ReplicationError::Invalid("producer snapshot digest is truncated"),
    )?);
    Ok(RecentBatchV1 {
        base_sequence: ProducerSequence::new(read_u32(bytes, offset)?),
        last_sequence: ProducerSequence::new(read_u32(bytes, offset + 4)?),
        record_count: read_u32(bytes, offset + 8)?,
        base_stream_offset: StreamOffset::new(read_u64(bytes, offset + 16)?),
        last_stream_offset: StreamOffset::new(read_u64(bytes, offset + 24)?),
        raft_index,
        canonical_digest: digest,
    })
}

fn read_u16(bytes: &[u8], offset: usize) -> Result<u16> {
    Ok(u16::from_le_bytes(
        bytes
            .get(offset..offset + 2)
            .ok_or(ReplicationError::Invalid("truncated producer snapshot"))?
            .try_into()
            .expect("2-byte slice"),
    ))
}

fn read_u32(bytes: &[u8], offset: usize) -> Result<u32> {
    Ok(u32::from_le_bytes(
        bytes
            .get(offset..offset + 4)
            .ok_or(ReplicationError::Invalid("truncated producer snapshot"))?
            .try_into()
            .expect("4-byte slice"),
    ))
}

fn read_u64(bytes: &[u8], offset: usize) -> Result<u64> {
    Ok(u64::from_le_bytes(
        bytes
            .get(offset..offset + 8)
            .ok_or(ReplicationError::Invalid("truncated producer snapshot"))?
            .try_into()
            .expect("8-byte slice"),
    ))
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum SnapshotWriteStop {
    TempWritten,
    TempSynced,
    Renamed,
}

fn write_snapshot_atomic(path: &Path, bytes: &[u8], stop: Option<SnapshotWriteStop>) -> Result<()> {
    let parent = path.parent().ok_or(ReplicationError::Invalid(
        "producer snapshot path has no parent",
    ))?;
    fs::create_dir_all(parent)?;
    let temp = path.with_file_name("producer.snapshot.tmp");
    match fs::remove_file(&temp) {
        Ok(()) => {}
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
        Err(error) => return Err(error.into()),
    }
    {
        let mut file = OpenOptions::new()
            .create_new(true)
            .write(true)
            .open(&temp)?;
        file.write_all(bytes)?;
        if stop == Some(SnapshotWriteStop::TempWritten) {
            return Err(ReplicationError::Invalid(
                "injected producer snapshot interruption",
            ));
        }
        file.sync_all()?;
    }
    if stop == Some(SnapshotWriteStop::TempSynced) {
        return Err(ReplicationError::Invalid(
            "injected producer snapshot interruption",
        ));
    }
    fs::rename(&temp, path)?;
    if stop == Some(SnapshotWriteStop::Renamed) {
        return Err(ReplicationError::Invalid(
            "injected producer snapshot interruption",
        ));
    }
    sync_parent(path)
}

fn sync_parent(path: &Path) -> Result<()> {
    let parent = path.parent().ok_or(ReplicationError::Invalid(
        "producer snapshot path has no parent",
    ))?;
    #[cfg(unix)]
    File::open(parent)?.sync_all()?;
    #[cfg(not(unix))]
    let _ = parent;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use astralog_common::ProducerAuthority;

    fn recent(sequence: u32, offset: u64, raft: u64, digest: u8) -> RecentBatchV1 {
        RecentBatchV1 {
            base_sequence: ProducerSequence::new(sequence),
            last_sequence: ProducerSequence::new(sequence),
            record_count: 1,
            base_stream_offset: StreamOffset::new(offset),
            last_stream_offset: StreamOffset::new(offset),
            raft_index: RaftIndex::new(raft).unwrap(),
            canonical_digest: [digest; 32],
        }
    }

    fn state(id: u64, epoch: u32, base: u32, raft: u64) -> PartitionProducerStateV1 {
        let producer_id = ProducerId::new(id).unwrap();
        let accepted_epoch = ProducerEpoch::new(epoch).unwrap();
        let recent = VecDeque::from([recent(base, u64::from(base), raft, id as u8)]);
        PartitionProducerStateV1::from_snapshot_parts(
            producer_id,
            accepted_epoch,
            ProducerSequence::new(base + 1),
            recent,
        )
        .unwrap()
    }

    fn snapshot(boundary: u64) -> ProducerSnapshotV1 {
        ProducerSnapshotV1::new(
            TopicId::new([7; 16]),
            PartitionId::new(3),
            boundary,
            BTreeMap::from([
                (ProducerId::new(1).unwrap(), state(1, 2, 4, boundary - 1)),
                (ProducerId::new(2).unwrap(), state(2, 1, 8, boundary)),
            ]),
        )
        .unwrap()
    }

    fn temp_dir(label: &str) -> PathBuf {
        let path = std::env::temp_dir().join(format!(
            "astralog-producer-snapshot-{label}-{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        fs::create_dir_all(&path).unwrap();
        path
    }

    #[test]
    fn producer_snapshot_round_trip_is_canonical_bounded_and_crc_protected() {
        let value = snapshot(12);
        let bytes = encode_producer_snapshot(&value).unwrap();
        assert_eq!(decode_producer_snapshot(&bytes).unwrap(), value);
        assert_eq!(
            encode_producer_snapshot(&decode_producer_snapshot(&bytes).unwrap()).unwrap(),
            bytes
        );

        let encoded_len = bytes.len();
        assert!(matches!(
            encode_producer_snapshot_with_limit(&value, encoded_len - 1),
            Err(ReplicationError::LimitExceeded { .. })
        ));
        assert!(matches!(
            decode_producer_snapshot_with_limit(&bytes, encoded_len - 1),
            Err(ReplicationError::LimitExceeded { .. })
        ));
        let mut corrupt = bytes.clone();
        corrupt[20] ^= 1;
        assert!(matches!(
            decode_producer_snapshot(&corrupt),
            Err(ReplicationError::ChecksumMismatch { .. })
        ));
    }

    #[test]
    fn producer_snapshot_store_rejects_oversized_file_before_read_allocation() {
        let dir = temp_dir("oversized-file");
        let store = ProducerSnapshotStore::new(&dir);
        let file = OpenOptions::new()
            .create_new(true)
            .write(true)
            .open(store.path())
            .unwrap();
        file.set_len(u64::try_from(MAX_PRODUCER_SNAPSHOT_BYTES + 1).unwrap())
            .unwrap();
        drop(file);
        assert!(matches!(
            store.load(),
            Err(ReplicationError::LimitExceeded {
                what: "producer snapshot bytes",
                actual,
                max: MAX_PRODUCER_SNAPSHOT_BYTES,
            }) if actual == MAX_PRODUCER_SNAPSHOT_BYTES + 1
        ));
        fs::remove_dir_all(dir).unwrap();
    }

    #[test]
    fn producer_snapshot_rejects_noncanonical_order_and_invalid_counts_with_valid_crc() {
        let value = snapshot(12);
        let mut bytes = encode_producer_snapshot(&value).unwrap();
        let entry_len = PRODUCER_ENTRY_HEADER_LEN + RECENT_BATCH_LEN;
        let first =
            bytes[PRODUCER_SNAPSHOT_HEADER_LEN..PRODUCER_SNAPSHOT_HEADER_LEN + entry_len].to_vec();
        let second = bytes[PRODUCER_SNAPSHOT_HEADER_LEN + entry_len
            ..PRODUCER_SNAPSHOT_HEADER_LEN + 2 * entry_len]
            .to_vec();
        bytes[PRODUCER_SNAPSHOT_HEADER_LEN..PRODUCER_SNAPSHOT_HEADER_LEN + entry_len]
            .copy_from_slice(&second);
        bytes[PRODUCER_SNAPSHOT_HEADER_LEN + entry_len
            ..PRODUCER_SNAPSHOT_HEADER_LEN + 2 * entry_len]
            .copy_from_slice(&first);
        let crc = crc32c::crc32c(&bytes[..bytes.len() - CRC_LEN]);
        let crc_offset = bytes.len() - CRC_LEN;
        bytes[crc_offset..].copy_from_slice(&crc.to_le_bytes());
        assert!(matches!(
            decode_producer_snapshot(&bytes),
            Err(ReplicationError::Invalid(
                "producer snapshot entries are not strictly sorted"
            ))
        ));

        let mut count_corrupt = encode_producer_snapshot(&value).unwrap();
        count_corrupt[48..52].copy_from_slice(&u32::MAX.to_le_bytes());
        let crc = crc32c::crc32c(&count_corrupt[..count_corrupt.len() - CRC_LEN]);
        let crc_offset = count_corrupt.len() - CRC_LEN;
        count_corrupt[crc_offset..].copy_from_slice(&crc.to_le_bytes());
        assert!(decode_producer_snapshot(&count_corrupt).is_err());
    }

    #[test]
    fn p7j_malformed_producer_snapshot_campaign_fails_closed_with_valid_crc() {
        let canonical = encode_producer_snapshot(&snapshot(12)).unwrap();
        let first_entry = PRODUCER_SNAPSHOT_HEADER_LEN;

        let mut mutations = Vec::new();

        let mut version = canonical.clone();
        version[8..10].copy_from_slice(&2_u16.to_le_bytes());
        mutations.push(version);

        let mut header_reserved = canonical.clone();
        header_reserved[12..16].copy_from_slice(&1_u32.to_le_bytes());
        mutations.push(header_reserved);

        let mut zero_id = canonical.clone();
        zero_id[first_entry..first_entry + 8].copy_from_slice(&0_u64.to_le_bytes());
        mutations.push(zero_id);

        let mut zero_epoch = canonical.clone();
        zero_epoch[first_entry + 8..first_entry + 12].copy_from_slice(&0_u32.to_le_bytes());
        mutations.push(zero_epoch);

        let mut zero_recent = canonical.clone();
        zero_recent[first_entry + 16] = 0;
        mutations.push(zero_recent);

        let mut entry_reserved = canonical.clone();
        entry_reserved[first_entry + 17] = 1;
        mutations.push(entry_reserved);

        let mut recent_reserved = canonical.clone();
        let recent = first_entry + PRODUCER_ENTRY_HEADER_LEN;
        recent_reserved[recent + 12..recent + 16].copy_from_slice(&1_u32.to_le_bytes());
        mutations.push(recent_reserved);

        let mut zero_raft = canonical.clone();
        zero_raft[recent + 32..recent + 40].copy_from_slice(&0_u64.to_le_bytes());
        mutations.push(zero_raft);

        for mut bytes in mutations {
            let crc_offset = bytes.len() - CRC_LEN;
            let crc = crc32c::crc32c(&bytes[..crc_offset]);
            bytes[crc_offset..].copy_from_slice(&crc.to_le_bytes());
            assert!(
                decode_producer_snapshot(&bytes).is_err(),
                "P7-J malformed producer snapshot was accepted"
            );
        }
    }

    #[test]
    fn producer_snapshot_atomic_write_interruption_leaves_old_or_complete_new_snapshot() {
        let dir = temp_dir("atomic");
        let store = ProducerSnapshotStore::new(&dir);
        let old = snapshot(12);
        store.persist(&old).unwrap();
        let mut new = snapshot(20);
        new.states
            .insert(ProducerId::new(3).unwrap(), state(3, 1, 9, 20));
        let new_bytes = encode_producer_snapshot(&new).unwrap();

        for stop in [
            SnapshotWriteStop::TempWritten,
            SnapshotWriteStop::TempSynced,
        ] {
            assert!(write_snapshot_atomic(store.path(), &new_bytes, Some(stop)).is_err());
            assert_eq!(store.load().unwrap(), Some(old.clone()));
        }
        assert!(
            write_snapshot_atomic(store.path(), &new_bytes, Some(SnapshotWriteStop::Renamed))
                .is_err()
        );
        assert_eq!(store.load().unwrap(), Some(new));
        fs::remove_dir_all(dir).unwrap();
    }

    #[test]
    fn producer_snapshot_state_constructor_rejects_broken_retry_history() {
        let producer_id = ProducerId::new(1).unwrap();
        let epoch = ProducerEpoch::new(1).unwrap();
        let broken = VecDeque::from([recent(0, 0, 1, 1), recent(2, 2, 2, 2)]);
        assert!(
            PartitionProducerStateV1::from_snapshot_parts(
                producer_id,
                epoch,
                ProducerSequence::new(3),
                broken
            )
            .is_err()
        );
        let _ = ProducerAuthority::new(producer_id, epoch);
    }
}
