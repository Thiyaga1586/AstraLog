//! Local P9-G clean progress is derived cache, never a replicated authority.
//!
//! Stale/missing/corrupt generation evidence conservatively falls back to zero.
use std::{
    fs,
    io::{ErrorKind, Read},
    path::{Path, PathBuf},
};

use astralog_common::{PartitionId, StreamOffset, TopicId};

use crate::{Result, StorageError, io_util::atomic_write};

const MAGIC: &[u8; 8] = b"ASTCLNS1";
const VERSION: u16 = 1;
const SIDECAR_LEN: usize = 52;
const NAME: &str = "clean.progress.v1";

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct LocalCleanProgress {
    pub topic_id: TopicId,
    pub partition_id: PartitionId,
    pub manifest_generation: u64,
    pub clean_compacted_offset: StreamOffset,
}

impl LocalCleanProgress {
    fn encode(self) -> [u8; SIDECAR_LEN] {
        let mut bytes = [0_u8; SIDECAR_LEN];
        bytes[..8].copy_from_slice(MAGIC);
        bytes[8..10].copy_from_slice(&VERSION.to_le_bytes());
        bytes[12..28].copy_from_slice(self.topic_id.as_bytes());
        bytes[28..32].copy_from_slice(&self.partition_id.get().to_le_bytes());
        bytes[32..40].copy_from_slice(&self.manifest_generation.to_le_bytes());
        bytes[40..48].copy_from_slice(&self.clean_compacted_offset.get().to_le_bytes());
        let crc = crc32c::crc32c(&bytes[..48]);
        bytes[48..52].copy_from_slice(&crc.to_le_bytes());
        bytes
    }

    fn decode(bytes: &[u8]) -> Result<Self> {
        if bytes.len() != SIDECAR_LEN
            || &bytes[..8] != MAGIC
            || bytes[8..10] != VERSION.to_le_bytes()
            || bytes[10..12] != [0, 0]
        {
            return Err(StorageError::InvalidFormat(
                "P9 local clean progress sidecar header invalid",
            ));
        }
        let expected = u32::from_le_bytes(bytes[48..52].try_into().unwrap());
        let actual = crc32c::crc32c(&bytes[..48]);
        if actual != expected {
            return Err(StorageError::ChecksumMismatch {
                context: "P9 local clean progress",
                expected,
                actual,
            });
        }
        let mut topic = [0_u8; 16];
        topic.copy_from_slice(&bytes[12..28]);
        let result = Self {
            topic_id: TopicId::new(topic),
            partition_id: PartitionId::new(u32::from_le_bytes(bytes[28..32].try_into().unwrap())),
            manifest_generation: u64::from_le_bytes(bytes[32..40].try_into().unwrap()),
            clean_compacted_offset: StreamOffset::new(u64::from_le_bytes(
                bytes[40..48].try_into().unwrap(),
            )),
        };
        if result.manifest_generation == 0 || result.encode().as_slice() != bytes {
            return Err(StorageError::InvalidFormat(
                "P9 local clean progress sidecar noncanonical",
            ));
        }
        Ok(result)
    }
}

#[derive(Debug, Clone)]
pub struct LocalCleanProgressStore {
    path: PathBuf,
    topic_id: TopicId,
    partition_id: PartitionId,
}

impl LocalCleanProgressStore {
    pub fn new(directory: &Path, topic_id: TopicId, partition_id: PartitionId) -> Self {
        Self {
            path: directory.join(NAME),
            topic_id,
            partition_id,
        }
    }

    /// The only acceptable evidence is for the exact *currently authoritative*
    /// manifest generation. Any failure is conservative; absence is not proof
    /// that a cleaning pass succeeded.
    pub fn load_conservative(&self, current_manifest_generation: Option<u64>) -> StreamOffset {
        let Some(generation) = current_manifest_generation else {
            return StreamOffset::new(0);
        };
        // Never use fs::read for an untrusted sidecar: a corrupt file may be
        // arbitrarily large. Validate exact metadata size before fixed-read.
        let Ok(mut file) = fs::File::open(&self.path) else {
            return StreamOffset::new(0);
        };
        if !file
            .metadata()
            .is_ok_and(|metadata| metadata.len() == SIDECAR_LEN as u64)
        {
            return StreamOffset::new(0);
        }
        let mut bytes = [0_u8; SIDECAR_LEN];
        if file.read_exact(&mut bytes).is_err() {
            return StreamOffset::new(0);
        }
        let mut trailing = [0_u8; 1];
        if file.read(&mut trailing).ok() != Some(0) {
            return StreamOffset::new(0);
        }
        LocalCleanProgress::decode(&bytes)
            .ok()
            .filter(|saved| {
                saved.topic_id == self.topic_id
                    && saved.partition_id == self.partition_id
                    && saved.manifest_generation == generation
            })
            .map_or(StreamOffset::new(0), |saved| saved.clean_compacted_offset)
    }

    /// Called only after the partition cleaner has verified that **every**
    /// keyed record below the proposed exclusive bound has been fully cleaned
    /// in the exact authoritative generation. Merely rewriting one selected
    /// source segment is not such a proof. This sidecar never authorizes
    /// tombstone deletion, which requires committed cluster safe authority.
    pub(crate) fn persist_verified(
        &self,
        manifest_generation: u64,
        candidate: StreamOffset,
        committed_high_watermark: StreamOffset,
    ) -> Result<()> {
        if manifest_generation == 0 || candidate > committed_high_watermark {
            return Err(StorageError::InvalidFormat(
                "P9 local clean progress requires a valid manifest and committed bound",
            ));
        }
        let previous = self.load_conservative(Some(manifest_generation));
        if candidate < previous {
            return Err(StorageError::InvalidFormat(
                "P9 local clean progress may not regress within a manifest generation",
            ));
        }
        atomic_write(
            &self.path,
            &LocalCleanProgress {
                topic_id: self.topic_id,
                partition_id: self.partition_id,
                manifest_generation,
                clean_compacted_offset: candidate,
            }
            .encode(),
        )
    }

    pub fn discard(&self) -> Result<()> {
        match fs::remove_file(&self.path) {
            Ok(()) => Ok(()),
            Err(error) if error.kind() == ErrorKind::NotFound => Ok(()),
            Err(error) => Err(error.into()),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn p9g_sidecar_crc_generation_identity_and_monotonicity_fail_closed() {
        let root = std::env::temp_dir().join(format!(
            "astralog-p9g-clean-sidecar-{}-{:?}",
            std::process::id(),
            std::thread::current().id(),
        ));
        fs::create_dir_all(&root).unwrap();
        let topic = TopicId::new([0x95; 16]);
        let store = LocalCleanProgressStore::new(&root, topic, PartitionId::new(7));
        assert_eq!(store.load_conservative(Some(1)), StreamOffset::new(0));
        store
            .persist_verified(1, StreamOffset::new(9), StreamOffset::new(9))
            .unwrap();
        assert_eq!(store.load_conservative(Some(1)), StreamOffset::new(9));
        assert_eq!(store.load_conservative(Some(2)), StreamOffset::new(0));
        assert!(
            store
                .persist_verified(1, StreamOffset::new(8), StreamOffset::new(9))
                .is_err()
        );
        assert!(
            store
                .persist_verified(1, StreamOffset::new(10), StreamOffset::new(9))
                .is_err()
        );
        let foreign =
            LocalCleanProgressStore::new(&root, TopicId::new([0x96; 16]), PartitionId::new(7));
        assert_eq!(foreign.load_conservative(Some(1)), StreamOffset::new(0));
        let mut damaged = fs::read(&store.path).unwrap();
        damaged[40] ^= 1;
        fs::write(&store.path, damaged).unwrap();
        assert_eq!(store.load_conservative(Some(1)), StreamOffset::new(0));
        fs::write(&store.path, vec![0x55; 1_000_000]).unwrap();
        assert_eq!(store.load_conservative(Some(1)), StreamOffset::new(0));
        store.discard().unwrap();
        assert_eq!(store.load_conservative(Some(1)), StreamOffset::new(0));
        fs::remove_dir_all(root).unwrap();
    }
}
