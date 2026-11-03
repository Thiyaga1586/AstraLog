use std::{
    fs::{self, OpenOptions},
    io::Write,
    path::{Path, PathBuf},
};

#[cfg(unix)]
use std::fs::File;

use astralog_raft::{HardState, LogIndex, NodeId, SnapshotMetadata, Term};

use crate::{
    ReplicationError, Result,
    codec::{decode_snapshot_metadata, encode_snapshot_metadata},
};

const HARD_MAGIC: &[u8; 8] = b"ASTRHST1";
const HARD_VERSION: u16 = 1;
const HARD_LEN: usize = 48;

const SNAP_MAGIC: &[u8; 8] = b"ASTRSNP1";
const SNAP_VERSION: u16 = 1;
const SNAP_HEADER_LEN: usize = 16;

const COMMIT_MAGIC: &[u8; 8] = b"ASTRCMT1";
const COMMIT_VERSION: u16 = 1;
const COMMIT_LEN: usize = 40;

#[derive(Debug, Clone)]
pub struct HardStateStore {
    path: PathBuf,
}

impl HardStateStore {
    pub fn new(directory: &Path) -> Self {
        Self {
            path: directory.join("raft.hardstate.v1"),
        }
    }

    pub fn load(&self) -> Result<Option<HardState>> {
        match fs::read(&self.path) {
            Ok(bytes) => Ok(Some(decode_hard_state(&bytes)?)),
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(None),
            Err(error) => Err(error.into()),
        }
    }

    pub fn persist(&self, state: &HardState) -> Result<()> {
        atomic_write(&self.path, &encode_hard_state(state))
    }

    pub fn path(&self) -> &Path {
        &self.path
    }
}

#[derive(Debug, Clone)]
pub struct SnapshotStore {
    path: PathBuf,
}

impl SnapshotStore {
    pub fn new(directory: &Path) -> Self {
        Self {
            path: directory.join("raft.snapshot.v1"),
        }
    }

    pub fn load(&self) -> Result<Option<SnapshotMetadata>> {
        match fs::read(&self.path) {
            Ok(bytes) => Ok(Some(decode_snapshot_file(&bytes)?)),
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(None),
            Err(error) => Err(error.into()),
        }
    }

    pub fn persist(&self, metadata: &SnapshotMetadata) -> Result<()> {
        atomic_write(&self.path, &encode_snapshot_file(metadata)?)
    }

    pub fn path(&self) -> &Path {
        &self.path
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct CommitIntent {
    pub previous: LogIndex,
    pub target: LogIndex,
}

#[derive(Debug, Clone)]
pub(crate) struct CommitIntentStore {
    path: PathBuf,
}

impl CommitIntentStore {
    pub(crate) fn new(directory: &Path) -> Self {
        Self {
            path: directory.join("raft.commit-intent.v1"),
        }
    }

    pub(crate) fn load(&self) -> Result<Option<CommitIntent>> {
        match fs::read(&self.path) {
            Ok(bytes) => Ok(Some(decode_commit_intent(&bytes)?)),
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(None),
            Err(error) => Err(error.into()),
        }
    }

    pub(crate) fn persist(&self, intent: CommitIntent) -> Result<()> {
        atomic_write(&self.path, &encode_commit_intent(intent))
    }

    pub(crate) fn clear(&self) -> Result<()> {
        match fs::remove_file(&self.path) {
            Ok(()) => sync_parent(&self.path),
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(()),
            Err(error) => Err(error.into()),
        }
    }
}

fn encode_hard_state(state: &HardState) -> [u8; HARD_LEN] {
    let mut bytes = [0_u8; HARD_LEN];
    bytes[0..8].copy_from_slice(HARD_MAGIC);
    bytes[8..10].copy_from_slice(&HARD_VERSION.to_le_bytes());
    bytes[10..12].copy_from_slice(&(HARD_LEN as u16).to_le_bytes());
    bytes[12..16].copy_from_slice(&0_u32.to_le_bytes());
    bytes[16..24].copy_from_slice(&state.current_term.get().to_le_bytes());
    bytes[24..32].copy_from_slice(&state.voted_for.map_or(0, NodeId::get).to_le_bytes());
    bytes[32..40].copy_from_slice(&state.durable_commit_index.get().to_le_bytes());
    bytes[40..44].copy_from_slice(&0_u32.to_le_bytes());
    let crc = crc32c::crc32c(&bytes[..44]);
    bytes[44..48].copy_from_slice(&crc.to_le_bytes());
    bytes
}

fn decode_hard_state(bytes: &[u8]) -> Result<HardState> {
    if bytes.len() != HARD_LEN {
        return Err(ReplicationError::Invalid(
            "Raft hard-state sidecar length mismatch",
        ));
    }
    if &bytes[0..8] != HARD_MAGIC
        || read_u16(bytes, 8)? != HARD_VERSION
        || usize::from(read_u16(bytes, 10)?) != HARD_LEN
        || read_u32(bytes, 12)? != 0
        || read_u32(bytes, 40)? != 0
    {
        return Err(ReplicationError::Invalid(
            "Raft hard-state sidecar header mismatch",
        ));
    }
    verify_crc(bytes, 44, "Raft hard-state sidecar")?;
    let vote = read_u64(bytes, 24)?;
    Ok(HardState {
        current_term: Term::new(read_u64(bytes, 16)?),
        voted_for: if vote == 0 {
            None
        } else {
            Some(NodeId::new(vote)?)
        },
        durable_commit_index: LogIndex::new(read_u64(bytes, 32)?),
    })
}

fn encode_snapshot_file(metadata: &SnapshotMetadata) -> Result<Vec<u8>> {
    let body = encode_snapshot_metadata(metadata)?;
    let body_len = u32::try_from(body.len())
        .map_err(|_| ReplicationError::Invalid("snapshot sidecar body length overflow"))?;
    let mut bytes = Vec::with_capacity(SNAP_HEADER_LEN + body.len() + 4);
    bytes.extend_from_slice(SNAP_MAGIC);
    bytes.extend_from_slice(&SNAP_VERSION.to_le_bytes());
    bytes.extend_from_slice(&(SNAP_HEADER_LEN as u16).to_le_bytes());
    bytes.extend_from_slice(&body_len.to_le_bytes());
    bytes.extend_from_slice(&body);
    let crc = crc32c::crc32c(&bytes);
    bytes.extend_from_slice(&crc.to_le_bytes());
    Ok(bytes)
}

fn decode_snapshot_file(bytes: &[u8]) -> Result<SnapshotMetadata> {
    if bytes.len() < SNAP_HEADER_LEN + 4 {
        return Err(ReplicationError::Invalid("snapshot sidecar is truncated"));
    }
    if &bytes[0..8] != SNAP_MAGIC
        || read_u16(bytes, 8)? != SNAP_VERSION
        || usize::from(read_u16(bytes, 10)?) != SNAP_HEADER_LEN
    {
        return Err(ReplicationError::Invalid(
            "snapshot sidecar header mismatch",
        ));
    }
    let body_len = usize::try_from(read_u32(bytes, 12)?)
        .map_err(|_| ReplicationError::Invalid("snapshot sidecar body length overflow"))?;
    if SNAP_HEADER_LEN
        .checked_add(body_len)
        .and_then(|value| value.checked_add(4))
        != Some(bytes.len())
    {
        return Err(ReplicationError::Invalid(
            "snapshot sidecar length mismatch",
        ));
    }
    verify_crc(bytes, bytes.len() - 4, "snapshot sidecar")?;
    decode_snapshot_metadata(&bytes[SNAP_HEADER_LEN..SNAP_HEADER_LEN + body_len])
}

fn encode_commit_intent(intent: CommitIntent) -> [u8; COMMIT_LEN] {
    let mut bytes = [0_u8; COMMIT_LEN];
    bytes[0..8].copy_from_slice(COMMIT_MAGIC);
    bytes[8..10].copy_from_slice(&COMMIT_VERSION.to_le_bytes());
    bytes[10..12].copy_from_slice(&(COMMIT_LEN as u16).to_le_bytes());
    bytes[12..16].copy_from_slice(&0_u32.to_le_bytes());
    bytes[16..24].copy_from_slice(&intent.previous.get().to_le_bytes());
    bytes[24..32].copy_from_slice(&intent.target.get().to_le_bytes());
    bytes[32..36].copy_from_slice(&0_u32.to_le_bytes());
    let crc = crc32c::crc32c(&bytes[..36]);
    bytes[36..40].copy_from_slice(&crc.to_le_bytes());
    bytes
}

fn decode_commit_intent(bytes: &[u8]) -> Result<CommitIntent> {
    if bytes.len() != COMMIT_LEN
        || &bytes[0..8] != COMMIT_MAGIC
        || read_u16(bytes, 8)? != COMMIT_VERSION
        || usize::from(read_u16(bytes, 10)?) != COMMIT_LEN
        || read_u32(bytes, 12)? != 0
        || read_u32(bytes, 32)? != 0
    {
        return Err(ReplicationError::Invalid("commit intent header mismatch"));
    }
    verify_crc(bytes, 36, "commit intent")?;
    let previous = LogIndex::new(read_u64(bytes, 16)?);
    let target = LogIndex::new(read_u64(bytes, 24)?);
    if target <= previous {
        return Err(ReplicationError::Invalid(
            "commit intent target must advance the committed index",
        ));
    }
    Ok(CommitIntent { previous, target })
}

fn verify_crc(bytes: &[u8], crc_offset: usize, context: &'static str) -> Result<()> {
    let expected = read_u32(bytes, crc_offset)?;
    let actual = crc32c::crc32c(&bytes[..crc_offset]);
    if expected != actual {
        return Err(ReplicationError::ChecksumMismatch {
            context,
            expected,
            actual,
        });
    }
    Ok(())
}

fn read_u16(bytes: &[u8], offset: usize) -> Result<u16> {
    Ok(u16::from_le_bytes(
        bytes
            .get(offset..offset + 2)
            .ok_or(ReplicationError::Invalid("truncated sidecar"))?
            .try_into()
            .expect("2-byte slice"),
    ))
}

fn read_u32(bytes: &[u8], offset: usize) -> Result<u32> {
    Ok(u32::from_le_bytes(
        bytes
            .get(offset..offset + 4)
            .ok_or(ReplicationError::Invalid("truncated sidecar"))?
            .try_into()
            .expect("4-byte slice"),
    ))
}

fn read_u64(bytes: &[u8], offset: usize) -> Result<u64> {
    Ok(u64::from_le_bytes(
        bytes
            .get(offset..offset + 8)
            .ok_or(ReplicationError::Invalid("truncated sidecar"))?
            .try_into()
            .expect("8-byte slice"),
    ))
}

pub(crate) fn atomic_write(path: &Path, bytes: &[u8]) -> Result<()> {
    let parent = path
        .parent()
        .ok_or(ReplicationError::Invalid("sidecar path has no parent"))?;
    fs::create_dir_all(parent)?;
    let file_name = path
        .file_name()
        .and_then(|name| name.to_str())
        .ok_or(ReplicationError::Invalid("sidecar file name is not UTF-8"))?;
    let temp = path.with_file_name(format!("{file_name}.tmp"));
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
        file.sync_all()?;
    }
    fs::rename(&temp, path)?;
    sync_parent(path)
}

fn sync_parent(path: &Path) -> Result<()> {
    let parent = path
        .parent()
        .ok_or(ReplicationError::Invalid("sidecar path has no parent"))?;
    #[cfg(unix)]
    File::open(parent)?.sync_all()?;
    #[cfg(not(unix))]
    let _ = parent;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use astralog_raft::{SnapshotId, StableConfiguration};

    fn temp_dir(label: &str) -> PathBuf {
        let path = std::env::temp_dir().join(format!(
            "astralog-replication-{label}-{}-{}",
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
    fn hard_state_round_trip_and_crc_failure() {
        let dir = temp_dir("hardstate");
        let store = HardStateStore::new(&dir);
        let state = HardState {
            current_term: Term::new(9),
            voted_for: Some(NodeId::new(3).unwrap()),
            durable_commit_index: LogIndex::new(7),
        };
        store.persist(&state).unwrap();
        assert_eq!(store.load().unwrap(), Some(state));
        let mut bytes = fs::read(store.path()).unwrap();
        bytes[20] ^= 1;
        fs::write(store.path(), bytes).unwrap();
        assert!(matches!(
            store.load(),
            Err(ReplicationError::ChecksumMismatch { .. })
        ));
        fs::remove_dir_all(dir).unwrap();
    }

    #[test]
    fn snapshot_round_trip() {
        let dir = temp_dir("snapshot");
        let store = SnapshotStore::new(&dir);
        let config =
            StableConfiguration::new([NodeId::new(1).unwrap(), NodeId::new(2).unwrap()], [])
                .unwrap();
        let metadata = SnapshotMetadata::new(
            LogIndex::new(5),
            Term::new(2),
            config,
            SnapshotId::new([4; 16]),
        )
        .unwrap();
        store.persist(&metadata).unwrap();
        assert_eq!(store.load().unwrap(), Some(metadata));
        fs::remove_dir_all(dir).unwrap();
    }
}
