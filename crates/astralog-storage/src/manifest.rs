use std::{
    fs::{self, OpenOptions},
    io::Write,
    path::{Path, PathBuf},
};

use astralog_common::{PartitionId, TopicId};

use crate::{
    Result, SegmentFileId, StorageError, StorageFormatVersion,
    io_util::{atomic_write, sync_directory},
    read_retained_segment_header_v2, read_segment_format, read_segment_header,
};

const MANIFEST_MAGIC: &[u8; 8] = b"ASTMANI1";
const MANIFEST_VERSION: u16 = 1;
const MANIFEST_HEADER_LEN: usize = 32;
const MANIFEST_CRC_LEN: usize = 4;
const MUTABLE_ACTIVE: u16 = 1;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ManifestEntry {
    pub file_name: String,
    pub expected_size: Option<u64>,
}

impl ManifestEntry {
    pub fn immutable(file_name: impl Into<String>, expected_size: u64) -> Self {
        Self {
            file_name: file_name.into(),
            expected_size: Some(expected_size),
        }
    }

    pub fn mutable_active(file_name: impl Into<String>) -> Self {
        Self {
            file_name: file_name.into(),
            expected_size: None,
        }
    }

    pub fn is_mutable_active(&self) -> bool {
        self.expected_size.is_none()
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SegmentManifest {
    pub generation: u64,
    pub entries: Vec<ManifestEntry>,
}

pub fn encode_manifest(manifest: &SegmentManifest) -> Result<Vec<u8>> {
    validate_manifest_shape(manifest)?;
    let mut body = Vec::new();
    for entry in &manifest.entries {
        validate_file_name(&entry.file_name)?;
        let name = entry.file_name.as_bytes();
        let name_len = u16::try_from(name.len())
            .map_err(|_| StorageError::ArithmeticOverflow("manifest filename length"))?;
        body.extend_from_slice(&name_len.to_le_bytes());
        let (flags, expected_size) = match entry.expected_size {
            Some(size) => (0_u16, size),
            None => (MUTABLE_ACTIVE, 0),
        };
        body.extend_from_slice(&flags.to_le_bytes());
        body.extend_from_slice(&expected_size.to_le_bytes());
        body.extend_from_slice(name);
    }

    let body_len = u32::try_from(body.len())
        .map_err(|_| StorageError::ArithmeticOverflow("manifest body length"))?;
    let entry_count = u32::try_from(manifest.entries.len())
        .map_err(|_| StorageError::ArithmeticOverflow("manifest entry count"))?;
    let capacity = MANIFEST_HEADER_LEN
        .checked_add(body.len())
        .and_then(|value| value.checked_add(MANIFEST_CRC_LEN))
        .ok_or(StorageError::ArithmeticOverflow("manifest total length"))?;
    let mut out = Vec::with_capacity(capacity);
    out.extend_from_slice(MANIFEST_MAGIC);
    out.extend_from_slice(&MANIFEST_VERSION.to_le_bytes());
    out.extend_from_slice(&(MANIFEST_HEADER_LEN as u16).to_le_bytes());
    out.extend_from_slice(&manifest.generation.to_le_bytes());
    out.extend_from_slice(&entry_count.to_le_bytes());
    out.extend_from_slice(&body_len.to_le_bytes());
    out.extend_from_slice(&0_u32.to_le_bytes());
    debug_assert_eq!(out.len(), MANIFEST_HEADER_LEN);
    out.extend_from_slice(&body);
    let crc = crc32c::crc32c(&out);
    out.extend_from_slice(&crc.to_le_bytes());
    Ok(out)
}

pub fn decode_manifest(bytes: &[u8]) -> Result<SegmentManifest> {
    if bytes.len() < MANIFEST_HEADER_LEN + MANIFEST_CRC_LEN {
        return Err(StorageError::InvalidFormat("manifest is too short"));
    }
    if &bytes[0..8] != MANIFEST_MAGIC {
        return Err(StorageError::InvalidFormat("manifest magic mismatch"));
    }
    let version = u16::from_le_bytes(
        bytes[8..10]
            .try_into()
            .map_err(|_| StorageError::InvalidFormat("manifest version bytes missing"))?,
    );
    if version != MANIFEST_VERSION {
        return Err(StorageError::UnsupportedVersion(version));
    }
    let header_len = u16::from_le_bytes(
        bytes[10..12]
            .try_into()
            .map_err(|_| StorageError::InvalidFormat("manifest header length missing"))?,
    ) as usize;
    if header_len != MANIFEST_HEADER_LEN {
        return Err(StorageError::InvalidFormat(
            "manifest header length mismatch",
        ));
    }
    let generation = u64::from_le_bytes(
        bytes[12..20]
            .try_into()
            .map_err(|_| StorageError::InvalidFormat("manifest generation missing"))?,
    );
    let entry_count = u32::from_le_bytes(
        bytes[20..24]
            .try_into()
            .map_err(|_| StorageError::InvalidFormat("manifest entry count missing"))?,
    ) as usize;
    let body_len = u32::from_le_bytes(
        bytes[24..28]
            .try_into()
            .map_err(|_| StorageError::InvalidFormat("manifest body length missing"))?,
    ) as usize;
    if u32::from_le_bytes(
        bytes[28..32]
            .try_into()
            .map_err(|_| StorageError::InvalidFormat("manifest reserved bytes missing"))?,
    ) != 0
    {
        return Err(StorageError::InvalidFormat(
            "manifest reserved field is nonzero",
        ));
    }
    let expected_total = MANIFEST_HEADER_LEN
        .checked_add(body_len)
        .and_then(|value| value.checked_add(MANIFEST_CRC_LEN))
        .ok_or(StorageError::ArithmeticOverflow("manifest total length"))?;
    if expected_total != bytes.len() {
        return Err(StorageError::InvalidFormat("manifest body length mismatch"));
    }
    let crc_offset = bytes.len() - MANIFEST_CRC_LEN;
    let expected_crc = u32::from_le_bytes(
        bytes[crc_offset..]
            .try_into()
            .map_err(|_| StorageError::InvalidFormat("manifest CRC missing"))?,
    );
    let actual_crc = crc32c::crc32c(&bytes[..crc_offset]);
    if expected_crc != actual_crc {
        return Err(StorageError::ChecksumMismatch {
            context: "segment manifest",
            expected: expected_crc,
            actual: actual_crc,
        });
    }

    let body = &bytes[MANIFEST_HEADER_LEN..crc_offset];
    let mut cursor = 0_usize;
    let mut entries = Vec::with_capacity(entry_count.min(1024));
    for _ in 0..entry_count {
        let name_len = read_body_u16(body, &mut cursor)? as usize;
        let flags = read_body_u16(body, &mut cursor)?;
        if flags & !MUTABLE_ACTIVE != 0 {
            return Err(StorageError::InvalidFormat("unknown manifest entry flag"));
        }
        let expected_size = read_body_u64(body, &mut cursor)?;
        let end = cursor
            .checked_add(name_len)
            .ok_or(StorageError::ArithmeticOverflow("manifest filename end"))?;
        let name_bytes = body.get(cursor..end).ok_or(StorageError::InvalidFormat(
            "manifest filename exceeds body",
        ))?;
        let file_name = std::str::from_utf8(name_bytes)
            .map_err(|_| StorageError::InvalidFormat("manifest filename is not UTF-8"))?
            .to_owned();
        validate_file_name(&file_name)?;
        cursor = end;
        let expected_size = if flags & MUTABLE_ACTIVE != 0 {
            if expected_size != 0 {
                return Err(StorageError::InvalidFormat(
                    "mutable manifest entry expected_size must be zero",
                ));
            }
            None
        } else {
            Some(expected_size)
        };
        entries.push(ManifestEntry {
            file_name,
            expected_size,
        });
    }
    if cursor != body.len() {
        return Err(StorageError::InvalidFormat(
            "manifest body contains trailing bytes",
        ));
    }
    let manifest = SegmentManifest {
        generation,
        entries,
    };
    validate_manifest_shape(&manifest)?;
    Ok(manifest)
}

pub fn load_manifest(directory: &Path) -> Result<Option<SegmentManifest>> {
    let path = directory.join("segments.manifest");
    match fs::read(path) {
        Ok(bytes) => Ok(Some(decode_manifest(&bytes)?)),
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => Ok(None),
        Err(err) => Err(err.into()),
    }
}

pub fn validate_manifest_files(directory: &Path, manifest: &SegmentManifest) -> Result<()> {
    validate_manifest_shape(manifest)?;
    for entry in &manifest.entries {
        validate_file_name(&entry.file_name)?;
        let path = directory.join(&entry.file_name);
        let metadata = fs::metadata(&path).map_err(|err| {
            StorageError::InvalidFormatOwned(format!(
                "manifest references unavailable file {}: {err}",
                path.display(),
            ))
        })?;
        if !metadata.is_file() {
            return Err(StorageError::InvalidFormatOwned(format!(
                "manifest entry is not a file: {}",
                path.display(),
            )));
        }
        if let Some(expected_size) = entry.expected_size
            && metadata.len() != expected_size
        {
            return Err(StorageError::InvalidFormatOwned(format!(
                "manifest size mismatch for {}: expected {}, got {}",
                path.display(),
                expected_size,
                metadata.len(),
            )));
        }
    }
    Ok(())
}

/// Validates a manifest as authoritative live `PartitionLog` inventory.
///
/// P1's generic manifest primitive intentionally allowed zero mutable entries
/// because it was not yet wired into a live partition. P9-C tightens the live
/// inventory contract without changing the frozen manifest bytes: a live
/// partition has exactly one mutable entry, it is the last entry, it is V1,
/// and every immutable V1/V2 entry has exact size and partition identity.
pub fn validate_partition_manifest_files(
    directory: &Path,
    manifest: &SegmentManifest,
    topic_id: TopicId,
    partition_id: PartitionId,
) -> Result<Vec<PathBuf>> {
    validate_manifest_files(directory, manifest)?;
    let mutable = manifest
        .entries
        .iter()
        .enumerate()
        .filter(|(_, entry)| entry.is_mutable_active())
        .collect::<Vec<_>>();
    if mutable.len() != 1 {
        return Err(StorageError::InvalidFormat(
            "live partition manifest requires exactly one mutable active segment",
        ));
    }
    let (mutable_index, _) = mutable[0];
    if mutable_index + 1 != manifest.entries.len() {
        return Err(StorageError::InvalidFormat(
            "live partition manifest mutable active segment must be last",
        ));
    }

    let mut paths = Vec::with_capacity(manifest.entries.len());
    let mut previous_id = None;
    for (index, entry) in manifest.entries.iter().enumerate() {
        let id = SegmentFileId::parse_log_file_name(&entry.file_name)?;
        if previous_id.is_some_and(|previous| id <= previous) {
            return Err(StorageError::InvalidFormat(
                "live partition manifest segment identities must be strictly ordered",
            ));
        }
        let path = directory.join(&entry.file_name);
        match read_segment_format(&path)? {
            StorageFormatVersion::V1 => {
                let header = read_segment_header(&path)?;
                if header.topic_id != topic_id || header.partition_id != partition_id {
                    return Err(StorageError::InvalidFormat(
                        "manifest V1 segment identity does not match partition",
                    ));
                }
                if SegmentFileId::from_header(&header) != id {
                    return Err(StorageError::InvalidFormat(
                        "manifest V1 filename does not match segment header identity",
                    ));
                }
            }
            StorageFormatVersion::V2 => {
                if index == mutable_index || entry.is_mutable_active() {
                    return Err(StorageError::InvalidFormat(
                        "V2 retained-history segment cannot be mutable active",
                    ));
                }
                let header = read_retained_segment_header_v2(&path)?;
                if header.topic_id != topic_id || header.partition_id != partition_id {
                    return Err(StorageError::InvalidFormat(
                        "manifest V2 segment identity does not match partition",
                    ));
                }
                if SegmentFileId::from_header(&header.identity_header()) != id {
                    return Err(StorageError::InvalidFormat(
                        "manifest V2 filename does not match segment header identity",
                    ));
                }
            }
        }
        previous_id = Some(id);
        paths.push(path);
    }

    if read_segment_format(&paths[mutable_index])? != StorageFormatVersion::V1 {
        return Err(StorageError::InvalidFormat(
            "live partition mutable active segment must be V1",
        ));
    }
    Ok(paths)
}

pub struct GenerationTransaction {
    directory: PathBuf,
    generation: u64,
    base_generation: Option<u64>,
    entries: Vec<ManifestEntry>,
    staged: Vec<(PathBuf, PathBuf)>,
}

impl GenerationTransaction {
    pub fn new(directory: &Path, generation: u64) -> Result<Self> {
        fs::create_dir_all(directory)?;
        let base_generation = load_manifest(directory)?.map(|manifest| manifest.generation);
        let expected_generation = match base_generation {
            Some(base) => base
                .checked_add(1)
                .ok_or(StorageError::ArithmeticOverflow("manifest generation"))?,
            None => 1,
        };
        if generation != expected_generation {
            return Err(StorageError::InvalidFormatOwned(format!(
                "manifest generation must advance exactly once: expected {expected_generation}, got {generation}",
            )));
        }
        Ok(Self {
            directory: directory.to_path_buf(),
            generation,
            base_generation,
            entries: Vec::new(),
            staged: Vec::new(),
        })
    }

    pub fn retain(&mut self, entry: ManifestEntry) -> Result<()> {
        validate_file_name(&entry.file_name)?;
        if self
            .entries
            .iter()
            .any(|existing| existing.file_name == entry.file_name)
        {
            return Err(StorageError::InvalidFormat(
                "manifest contains duplicate filename",
            ));
        }
        self.entries.push(entry);
        Ok(())
    }

    pub fn stage_file(
        &mut self,
        file_name: impl Into<String>,
        bytes: &[u8],
        mutable_active: bool,
    ) -> Result<()> {
        let file_name = file_name.into();
        validate_file_name(&file_name)?;
        if self
            .entries
            .iter()
            .any(|existing| existing.file_name == file_name)
        {
            return Err(StorageError::InvalidFormat(
                "generation stages duplicate filename",
            ));
        }
        let final_path = self.directory.join(&file_name);
        if final_path.exists() {
            return Err(StorageError::InvalidFormat(
                "generation final file already exists",
            ));
        }
        let ordinal = self.staged.len();
        let temp_name = format!(
            ".{file_name}.manifest-g{:020}-{ordinal:04}.tmp",
            self.generation,
        );
        let temp_path = self.directory.join(temp_name);
        let mut file = OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(&temp_path)?;
        file.write_all(bytes)?;
        file.sync_all()?;
        let entry = if mutable_active {
            ManifestEntry::mutable_active(file_name)
        } else {
            ManifestEntry::immutable(file_name, bytes.len() as u64)
        };
        self.entries.push(entry);
        self.staged.push((temp_path, final_path));
        Ok(())
    }

    pub fn commit(mut self) -> Result<SegmentManifest> {
        self.ensure_base_generation_unchanged()?;
        self.finalize_staged_files()?;
        self.ensure_base_generation_unchanged()?;
        let manifest = SegmentManifest {
            generation: self.generation,
            entries: self.entries,
        };
        let bytes = encode_manifest(&manifest)?;
        validate_manifest_files(&self.directory, &manifest)?;
        atomic_write(&self.directory.join("segments.manifest"), &bytes)?;
        validate_manifest_files(&self.directory, &manifest)?;
        Ok(manifest)
    }

    fn finalize_staged_files(&mut self) -> Result<()> {
        for (temp, final_path) in &self.staged {
            fs::rename(temp, final_path)?;
        }
        sync_directory(&self.directory)?;
        self.staged.clear();
        Ok(())
    }

    fn ensure_base_generation_unchanged(&self) -> Result<()> {
        let current = load_manifest(&self.directory)?.map(|manifest| manifest.generation);
        if current != self.base_generation {
            return Err(StorageError::InvalidFormatOwned(format!(
                "stale generation transaction: expected base {:?}, found {:?}",
                self.base_generation, current,
            )));
        }
        Ok(())
    }
}

fn validate_manifest_shape(manifest: &SegmentManifest) -> Result<()> {
    let mutable_count = manifest
        .entries
        .iter()
        .filter(|entry| entry.is_mutable_active())
        .count();
    if mutable_count > 1 {
        return Err(StorageError::InvalidFormat(
            "manifest contains more than one mutable active segment",
        ));
    }
    let mut names = std::collections::BTreeSet::new();
    for entry in &manifest.entries {
        validate_file_name(&entry.file_name)?;
        if !names.insert(&entry.file_name) {
            return Err(StorageError::InvalidFormat(
                "manifest contains duplicate filename",
            ));
        }
    }
    Ok(())
}

fn validate_file_name(name: &str) -> Result<()> {
    if name.is_empty()
        || name == "."
        || name == ".."
        || name.contains('/')
        || name.contains('\\')
        || name.as_bytes().iter().any(|byte| !byte.is_ascii_graphic())
    {
        return Err(StorageError::InvalidFormat(
            "manifest filename must be a safe ASCII basename",
        ));
    }
    Ok(())
}

fn read_body_u16(body: &[u8], cursor: &mut usize) -> Result<u16> {
    let end = cursor
        .checked_add(2)
        .ok_or(StorageError::ArithmeticOverflow("manifest cursor"))?;
    let bytes = body
        .get(*cursor..end)
        .ok_or(StorageError::InvalidFormat("manifest entry is truncated"))?;
    *cursor = end;
    Ok(u16::from_le_bytes([bytes[0], bytes[1]]))
}

fn read_body_u64(body: &[u8], cursor: &mut usize) -> Result<u64> {
    let end = cursor
        .checked_add(8)
        .ok_or(StorageError::ArithmeticOverflow("manifest cursor"))?;
    let bytes = body
        .get(*cursor..end)
        .ok_or(StorageError::InvalidFormat("manifest entry is truncated"))?;
    *cursor = end;
    Ok(u64::from_le_bytes([
        bytes[0], bytes[1], bytes[2], bytes[3], bytes[4], bytes[5], bytes[6], bytes[7],
    ]))
}

#[cfg(test)]
mod tests {
    use std::time::{SystemTime, UNIX_EPOCH};

    use super::*;
    use crate::test_support::decode_hex;

    fn test_directory(label: &str) -> PathBuf {
        let nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let path = std::env::temp_dir().join(format!(
            "astralog-manifest-{label}-{}-{nanos}",
            std::process::id(),
        ));
        fs::create_dir_all(&path).unwrap();
        path
    }

    #[test]
    fn manifest_round_trip() {
        let manifest = SegmentManifest {
            generation: 8,
            entries: vec![
                ManifestEntry::immutable("a.log", 123),
                ManifestEntry::mutable_active("b.log"),
            ],
        };
        let encoded = encode_manifest(&manifest).unwrap();
        let golden = decode_hex(include_str!("../tests/golden/manifest_v1.hex"));
        assert_eq!(encoded, golden);
        assert_eq!(decode_manifest(&encoded).unwrap(), manifest);
    }

    #[test]
    fn p9c_mixed_v1_v2_manifest_golden_is_immutable() {
        let manifest = SegmentManifest {
            generation: 7,
            entries: vec![
                ManifestEntry::immutable(
                    "00000000000000000000-00000000000000000001-g00000000.log",
                    123,
                ),
                ManifestEntry::immutable(
                    "00000000000000000003-00000000000000000002-g00000001.log",
                    456,
                ),
                ManifestEntry::mutable_active(
                    "00000000000000000010-00000000000000000005-g00000000.log",
                ),
            ],
        };
        let encoded = encode_manifest(&manifest).unwrap();
        assert_eq!(
            encoded,
            decode_hex(include_str!("../tests/golden/manifest_p9_mixed_v1.hex")),
        );
        assert_eq!(decode_manifest(&encoded).unwrap(), manifest);
    }

    #[test]
    fn manifest_rejects_path_traversal() {
        let manifest = SegmentManifest {
            generation: 1,
            entries: vec![ManifestEntry::immutable("../escape.log", 1)],
        };
        assert!(encode_manifest(&manifest).is_err());
    }

    #[test]
    fn manifest_crc_detects_corruption() {
        let manifest = SegmentManifest {
            generation: 1,
            entries: vec![ManifestEntry::immutable("a.log", 1)],
        };
        let mut encoded = encode_manifest(&manifest).unwrap();
        encoded[20] ^= 0x80;
        assert!(matches!(
            decode_manifest(&encoded),
            Err(StorageError::ChecksumMismatch {
                context: "segment manifest",
                ..
            })
        ));
    }

    #[test]
    fn generation_commit_publishes_only_after_files_are_durable_and_named() {
        let directory = test_directory("commit");
        let mut transaction = GenerationTransaction::new(&directory, 1).unwrap();
        transaction
            .stage_file("closed.log", b"immutable", false)
            .unwrap();
        transaction
            .stage_file("active.log", b"active-header", true)
            .unwrap();

        assert!(load_manifest(&directory).unwrap().is_none());
        let manifest = transaction.commit().unwrap();
        assert_eq!(load_manifest(&directory).unwrap(), Some(manifest.clone()));
        validate_manifest_files(&directory, &manifest).unwrap();
        fs::remove_dir_all(directory).unwrap();
    }

    #[test]
    fn files_renamed_before_manifest_switch_are_not_authoritative() {
        let directory = test_directory("pre-manifest");

        let mut old = GenerationTransaction::new(&directory, 1).unwrap();
        old.stage_file("old.log", b"old", true).unwrap();
        let old_manifest = old.commit().unwrap();

        let mut next = GenerationTransaction::new(&directory, 2).unwrap();
        next.retain(ManifestEntry::immutable("old.log", 3)).unwrap();
        next.stage_file("new.log", b"new", true).unwrap();
        next.finalize_staged_files().unwrap();

        assert_eq!(
            load_manifest(&directory).unwrap(),
            Some(old_manifest),
            "renamed but unmanifested files must not become authoritative",
        );
        fs::remove_dir_all(directory).unwrap();
    }

    #[test]
    fn staged_temp_file_does_not_change_authoritative_manifest() {
        let directory = test_directory("staged-only");
        let mut old = GenerationTransaction::new(&directory, 1).unwrap();
        old.stage_file("old.log", b"old", true).unwrap();
        let old_manifest = old.commit().unwrap();

        let mut next = GenerationTransaction::new(&directory, 2).unwrap();
        next.stage_file("new.log", b"new", true).unwrap();
        assert_eq!(load_manifest(&directory).unwrap(), Some(old_manifest));
        fs::remove_dir_all(directory).unwrap();
    }

    #[test]
    fn prepared_manifest_temp_does_not_switch_generation() {
        let directory = test_directory("manifest-temp");
        let mut old = GenerationTransaction::new(&directory, 1).unwrap();
        old.stage_file("old.log", b"old", true).unwrap();
        let old_manifest = old.commit().unwrap();

        fs::write(directory.join("new.log"), b"new").unwrap();
        let next_manifest = SegmentManifest {
            generation: 2,
            entries: vec![ManifestEntry::mutable_active("new.log")],
        };
        fs::write(
            directory.join("segments.manifest.tmp"),
            encode_manifest(&next_manifest).unwrap(),
        )
        .unwrap();

        assert_eq!(load_manifest(&directory).unwrap(), Some(old_manifest));
        fs::remove_dir_all(directory).unwrap();
    }

    #[test]
    fn invalid_retained_file_is_rejected_before_manifest_switch() {
        let directory = test_directory("pre-switch-validation");
        let mut old = GenerationTransaction::new(&directory, 1).unwrap();
        old.stage_file("old.log", b"old", true).unwrap();
        let old_manifest = old.commit().unwrap();

        let mut next = GenerationTransaction::new(&directory, 2).unwrap();
        next.retain(ManifestEntry::immutable("old.log", 999))
            .unwrap();
        assert!(next.commit().is_err());
        assert_eq!(load_manifest(&directory).unwrap(), Some(old_manifest));
        fs::remove_dir_all(directory).unwrap();
    }

    #[test]
    fn manifest_file_size_mismatch_is_detected_for_immutable_entries() {
        let directory = test_directory("size");
        fs::write(directory.join("closed.log"), b"abc").unwrap();
        let manifest = SegmentManifest {
            generation: 1,
            entries: vec![ManifestEntry::immutable("closed.log", 4)],
        };
        assert!(validate_manifest_files(&directory, &manifest).is_err());
        fs::remove_dir_all(directory).unwrap();
    }

    #[test]
    fn generation_numbers_cannot_skip_or_move_backwards() {
        let directory = test_directory("generation-sequence");
        assert!(GenerationTransaction::new(&directory, 2).is_err());

        let mut first = GenerationTransaction::new(&directory, 1).unwrap();
        first.stage_file("one.log", b"one", true).unwrap();
        first.commit().unwrap();

        assert!(GenerationTransaction::new(&directory, 1).is_err());
        assert!(GenerationTransaction::new(&directory, 3).is_err());
        assert!(GenerationTransaction::new(&directory, 2).is_ok());
        fs::remove_dir_all(directory).unwrap();
    }

    #[test]
    fn stale_generation_transaction_cannot_replace_newer_manifest() {
        let directory = test_directory("stale-transaction");
        let mut first = GenerationTransaction::new(&directory, 1).unwrap();
        first.stage_file("one.log", b"one", true).unwrap();
        first.commit().unwrap();

        let mut winner = GenerationTransaction::new(&directory, 2).unwrap();
        winner
            .retain(ManifestEntry::immutable("one.log", 3))
            .unwrap();
        winner.stage_file("winner.log", b"winner", true).unwrap();

        let mut stale = GenerationTransaction::new(&directory, 2).unwrap();
        stale
            .retain(ManifestEntry::immutable("one.log", 3))
            .unwrap();
        stale.stage_file("stale.log", b"stale", true).unwrap();

        let winner_manifest = winner.commit().unwrap();
        assert!(stale.commit().is_err());
        assert_eq!(load_manifest(&directory).unwrap(), Some(winner_manifest));
        fs::remove_dir_all(directory).unwrap();
    }
}
