use std::{
    fs::{self, OpenOptions},
    io::Write,
    path::{Path, PathBuf},
};

#[cfg(unix)]
use std::fs::File;

use astralog_raft::{NodeId, StableConfiguration};

use crate::{ControllerError, ControllerNodeId, Result};

const BOOTSTRAP_MAGIC: &[u8; 8] = b"ASTCBT01";
const BOOTSTRAP_VERSION: u16 = 1;
const BOOTSTRAP_HEADER_LEN: u16 = 32;
const BOOTSTRAP_FILE_NAME: &str = "controller.bootstrap.v1";
const BOOTSTRAP_TEMP_PREFIX: &str = ".controller.bootstrap.v1.tmp.";

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BootstrapInstallOutcome {
    Created,
    AlreadyInitialized,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ControllerBootstrapManifest {
    cluster_id: [u8; 16],
    voters: Vec<ControllerNodeId>,
}

impl ControllerBootstrapManifest {
    pub fn new(cluster_id: [u8; 16], configuration: &StableConfiguration) -> Result<Self> {
        validate_static_configuration(configuration)?;
        if cluster_id == [0; 16] {
            return Err(ControllerError::Invalid(
                "controller bootstrap cluster id zero is reserved",
            ));
        }
        let voters = configuration
            .voters()
            .map(|node| ControllerNodeId::new(node.get()))
            .collect::<Result<Vec<_>>>()?;
        Ok(Self { cluster_id, voters })
    }

    pub const fn cluster_id(&self) -> &[u8; 16] {
        &self.cluster_id
    }

    pub fn voters(&self) -> &[ControllerNodeId] {
        &self.voters
    }

    pub fn static_configuration(&self) -> Result<StableConfiguration> {
        Ok(StableConfiguration::new(
            self.voters
                .iter()
                .map(|node| NodeId::new(node.get()))
                .collect::<core::result::Result<Vec<_>, _>>()?,
            [],
        )?)
    }
}

pub fn initialize_controller_bootstrap(
    directory: &Path,
    cluster_id: [u8; 16],
    configuration: &StableConfiguration,
) -> Result<BootstrapInstallOutcome> {
    let expected = ControllerBootstrapManifest::new(cluster_id, configuration)?;
    fs::create_dir_all(directory)?;
    let path = directory.join(BOOTSTRAP_FILE_NAME);

    if path.exists() {
        validate_existing_manifest(&path, &expected)?;
        return Ok(BootstrapInstallOutcome::AlreadyInitialized);
    }

    reject_preexisting_authority_without_manifest(directory)?;
    let bytes = encode_manifest(&expected)?;
    let temp = create_unique_staging_file(directory, &bytes)?;
    let publish = fs::hard_link(&temp, &path);
    match publish {
        Ok(()) => {
            sync_directory(directory)?;
            remove_staging_file(&temp)?;
            validate_existing_manifest(&path, &expected)?;
            Ok(BootstrapInstallOutcome::Created)
        }
        Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists => {
            remove_staging_file(&temp)?;
            validate_existing_manifest(&path, &expected)?;
            Ok(BootstrapInstallOutcome::AlreadyInitialized)
        }
        Err(error) => {
            let _ = fs::remove_file(&temp);
            Err(error.into())
        }
    }
}

pub fn load_controller_bootstrap(directory: &Path) -> Result<ControllerBootstrapManifest> {
    let path = directory.join(BOOTSTRAP_FILE_NAME);
    let bytes = fs::read(&path).map_err(|error| {
        if error.kind() == std::io::ErrorKind::NotFound {
            ControllerError::Invalid(
                "controller bootstrap manifest is missing; initialize cluster identity before serving",
            )
        } else {
            error.into()
        }
    })?;
    decode_manifest(&bytes)
}

pub fn validate_controller_bootstrap(
    directory: &Path,
    cluster_id: [u8; 16],
    configuration: &StableConfiguration,
) -> Result<ControllerBootstrapManifest> {
    let expected = ControllerBootstrapManifest::new(cluster_id, configuration)?;
    let actual = load_controller_bootstrap(directory)?;
    if actual.cluster_id != expected.cluster_id {
        return Err(ControllerError::Invalid(
            "controller bootstrap cluster id differs from configured cluster",
        ));
    }
    if actual.voters != expected.voters {
        return Err(ControllerError::Invalid(
            "controller bootstrap voter set differs from configured static voters",
        ));
    }
    Ok(actual)
}

fn validate_existing_manifest(path: &Path, expected: &ControllerBootstrapManifest) -> Result<()> {
    let actual = decode_manifest(&fs::read(path)?)?;
    if &actual != expected {
        return Err(ControllerError::Invalid(
            "controller bootstrap manifest already exists with different cluster/voter identity",
        ));
    }
    Ok(())
}

fn reject_preexisting_authority_without_manifest(directory: &Path) -> Result<()> {
    for entry in fs::read_dir(directory)? {
        let entry = entry?;
        let name = entry.file_name();
        let Some(name) = name.to_str() else {
            return Err(ControllerError::Invalid(
                "controller directory contains non-UTF8 entry before bootstrap",
            ));
        };
        if name.starts_with(BOOTSTRAP_TEMP_PREFIX) {
            continue;
        }
        return Err(ControllerError::Invalid(
            "controller persistence exists without bootstrap manifest",
        ));
    }
    Ok(())
}

fn create_unique_staging_file(directory: &Path, bytes: &[u8]) -> Result<PathBuf> {
    for attempt in 0_u32..64 {
        let path = directory.join(format!(
            "{BOOTSTRAP_TEMP_PREFIX}{}.{}",
            std::process::id(),
            attempt
        ));
        match OpenOptions::new().create_new(true).write(true).open(&path) {
            Ok(mut file) => {
                file.write_all(bytes)?;
                file.sync_all()?;
                return Ok(path);
            }
            Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists => continue,
            Err(error) => return Err(error.into()),
        }
    }
    Err(ControllerError::Busy(
        "controller bootstrap staging namespace is exhausted",
    ))
}

fn remove_staging_file(path: &Path) -> Result<()> {
    match fs::remove_file(path) {
        Ok(()) => {
            let parent = path.parent().ok_or(ControllerError::Invalid(
                "controller bootstrap staging path has no parent",
            ))?;
            sync_directory(parent)
        }
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(()),
        Err(error) => Err(error.into()),
    }
}

fn encode_manifest(manifest: &ControllerBootstrapManifest) -> Result<Vec<u8>> {
    if !matches!(manifest.voters.len(), 3 | 5) {
        return Err(ControllerError::Invalid(
            "controller bootstrap requires exactly 3 or 5 voters",
        ));
    }
    let mut out = Vec::with_capacity(36 + manifest.voters.len() * 8);
    out.extend_from_slice(BOOTSTRAP_MAGIC);
    out.extend_from_slice(&BOOTSTRAP_VERSION.to_le_bytes());
    out.extend_from_slice(&BOOTSTRAP_HEADER_LEN.to_le_bytes());
    out.extend_from_slice(&manifest.cluster_id);
    out.extend_from_slice(
        &u16::try_from(manifest.voters.len())
            .map_err(|_| ControllerError::Invalid("controller voter count overflow"))?
            .to_le_bytes(),
    );
    out.extend_from_slice(&0_u16.to_le_bytes());
    for voter in &manifest.voters {
        out.extend_from_slice(&voter.get().to_le_bytes());
    }
    let crc = crc32c::crc32c(&out);
    out.extend_from_slice(&crc.to_le_bytes());
    Ok(out)
}

fn decode_manifest(bytes: &[u8]) -> Result<ControllerBootstrapManifest> {
    if bytes.len() < 36 || bytes.get(..8) != Some(BOOTSTRAP_MAGIC.as_slice()) {
        return Err(ControllerError::Invalid(
            "invalid controller bootstrap manifest header",
        ));
    }
    let version = read_u16(bytes, 8)?;
    let header_len = read_u16(bytes, 10)?;
    if version != BOOTSTRAP_VERSION || header_len != BOOTSTRAP_HEADER_LEN {
        return Err(ControllerError::Invalid(
            "unsupported controller bootstrap manifest version/header",
        ));
    }
    let mut cluster_id = [0_u8; 16];
    cluster_id.copy_from_slice(
        bytes
            .get(12..28)
            .ok_or(ControllerError::Invalid("bootstrap manifest is truncated"))?,
    );
    let voter_count = usize::from(read_u16(bytes, 28)?);
    if !matches!(voter_count, 3 | 5) || read_u16(bytes, 30)? != 0 {
        return Err(ControllerError::Invalid(
            "invalid controller bootstrap voter count/reserved field",
        ));
    }
    let expected_len = 32_usize
        .checked_add(voter_count.saturating_mul(8))
        .and_then(|value| value.checked_add(4))
        .ok_or(ControllerError::Invalid(
            "controller bootstrap manifest length overflow",
        ))?;
    if bytes.len() != expected_len {
        return Err(ControllerError::Invalid(
            "controller bootstrap manifest length mismatch",
        ));
    }
    let crc_offset = expected_len - 4;
    let expected_crc = read_u32(bytes, crc_offset)?;
    let actual_crc = crc32c::crc32c(&bytes[..crc_offset]);
    if expected_crc != actual_crc {
        return Err(ControllerError::ChecksumMismatch {
            context: "controller bootstrap manifest",
            expected: expected_crc,
            actual: actual_crc,
        });
    }
    let mut voters = Vec::with_capacity(voter_count);
    let mut previous = 0_u64;
    for ordinal in 0..voter_count {
        let offset = 32 + ordinal * 8;
        let raw = read_u64(bytes, offset)?;
        if raw <= previous {
            return Err(ControllerError::Invalid(
                "controller bootstrap voters are not canonical ascending/distinct",
            ));
        }
        previous = raw;
        voters.push(ControllerNodeId::new(raw)?);
    }
    let manifest = ControllerBootstrapManifest { cluster_id, voters };
    if manifest.cluster_id == [0; 16] || encode_manifest(&manifest)? != bytes {
        return Err(ControllerError::Invalid(
            "controller bootstrap manifest is not canonical",
        ));
    }
    Ok(manifest)
}

fn validate_static_configuration(configuration: &StableConfiguration) -> Result<()> {
    if !matches!(configuration.voter_count(), 3 | 5) || configuration.learners().next().is_some() {
        return Err(ControllerError::Invalid(
            "P5 bootstrap requires exactly 3 or 5 static controller voters and no learners",
        ));
    }
    Ok(())
}

fn read_u16(bytes: &[u8], offset: usize) -> Result<u16> {
    Ok(u16::from_le_bytes(
        bytes
            .get(offset..offset + 2)
            .ok_or(ControllerError::Invalid("bootstrap manifest is truncated"))?
            .try_into()
            .expect("two-byte slice"),
    ))
}

fn read_u32(bytes: &[u8], offset: usize) -> Result<u32> {
    Ok(u32::from_le_bytes(
        bytes
            .get(offset..offset + 4)
            .ok_or(ControllerError::Invalid("bootstrap manifest is truncated"))?
            .try_into()
            .expect("four-byte slice"),
    ))
}

fn read_u64(bytes: &[u8], offset: usize) -> Result<u64> {
    Ok(u64::from_le_bytes(
        bytes
            .get(offset..offset + 8)
            .ok_or(ControllerError::Invalid("bootstrap manifest is truncated"))?
            .try_into()
            .expect("eight-byte slice"),
    ))
}

fn sync_directory(directory: &Path) -> Result<()> {
    #[cfg(unix)]
    File::open(directory)?.sync_all()?;
    #[cfg(not(unix))]
    let _ = directory;
    Ok(())
}

#[cfg(test)]
mod tests {
    use std::sync::{Arc, Barrier};
    use std::time::{SystemTime, UNIX_EPOCH};

    use super::*;

    fn node(value: u64) -> NodeId {
        NodeId::new(value).unwrap()
    }

    fn voters() -> StableConfiguration {
        StableConfiguration::new([node(1), node(2), node(3)], []).unwrap()
    }

    fn temp_dir(label: &str) -> PathBuf {
        let nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let path = std::env::temp_dir().join(format!(
            "astralog-p5e-{label}-{}-{nanos}",
            std::process::id()
        ));
        fs::create_dir_all(&path).unwrap();
        path
    }

    #[test]
    fn manifest_is_canonical_crc_protected_and_round_trips() {
        let manifest = ControllerBootstrapManifest::new([7; 16], &voters()).unwrap();
        let bytes = encode_manifest(&manifest).unwrap();
        assert_eq!(decode_manifest(&bytes).unwrap(), manifest);
        let mut corrupt = bytes;
        corrupt[20] ^= 0x80;
        assert!(decode_manifest(&corrupt).is_err());
    }

    #[test]
    fn first_bootstrap_is_create_once_and_same_identity_is_idempotent() {
        let dir = temp_dir("create-once");
        assert_eq!(
            initialize_controller_bootstrap(&dir, [5; 16], &voters()).unwrap(),
            BootstrapInstallOutcome::Created
        );
        let before = fs::read(dir.join(BOOTSTRAP_FILE_NAME)).unwrap();
        assert_eq!(
            initialize_controller_bootstrap(&dir, [5; 16], &voters()).unwrap(),
            BootstrapInstallOutcome::AlreadyInitialized
        );
        assert_eq!(fs::read(dir.join(BOOTSTRAP_FILE_NAME)).unwrap(), before);
        fs::remove_dir_all(dir).unwrap();
    }

    #[test]
    fn conflicting_cluster_or_voter_set_cannot_replace_bootstrap_identity() {
        let dir = temp_dir("conflict");
        initialize_controller_bootstrap(&dir, [5; 16], &voters()).unwrap();
        assert!(initialize_controller_bootstrap(&dir, [6; 16], &voters()).is_err());
        let other = StableConfiguration::new([node(1), node(2), node(4)], []).unwrap();
        assert!(initialize_controller_bootstrap(&dir, [5; 16], &other).is_err());
        fs::remove_dir_all(dir).unwrap();
    }

    #[test]
    fn persistence_without_manifest_is_not_retroactively_adopted() {
        let dir = temp_dir("legacy-reject");
        fs::write(dir.join("controller.raft.log.v1"), b"legacy").unwrap();
        assert!(initialize_controller_bootstrap(&dir, [5; 16], &voters()).is_err());
        fs::remove_dir_all(dir).unwrap();
    }

    #[test]
    fn concurrent_conflicting_bootstrap_has_exactly_one_authoritative_identity() {
        let dir = temp_dir("concurrent");
        let barrier = Arc::new(Barrier::new(2));
        let mut workers = Vec::new();
        for cluster in [[5; 16], [6; 16]] {
            let path = dir.clone();
            let start = barrier.clone();
            workers.push(std::thread::spawn(move || {
                start.wait();
                initialize_controller_bootstrap(&path, cluster, &voters())
            }));
        }
        let results = workers
            .into_iter()
            .map(|worker| worker.join().unwrap())
            .collect::<Vec<_>>();
        assert_eq!(results.iter().filter(|result| result.is_ok()).count(), 1);
        let manifest = load_controller_bootstrap(&dir).unwrap();
        assert!(matches!(manifest.cluster_id(), id if id == &[5; 16] || id == &[6; 16]));
        assert_eq!(manifest.static_configuration().unwrap(), voters());
        fs::remove_dir_all(dir).unwrap();
    }
}
