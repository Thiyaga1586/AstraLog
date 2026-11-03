use std::{
    fs::{self, OpenOptions},
    io::Write,
    path::{Path, PathBuf},
};

#[cfg(unix)]
use std::fs::File;

use astralog_raft::{
    EntryKind, HardState, LogEntry, LogIndex, NodeId, PersistenceBatch, PersistenceOp,
    RecoveredState, SnapshotId, SnapshotMetadata, StableConfiguration, Term,
};

use crate::{
    CommandLimits, ControllerError, ControllerLogCommand, ControllerSnapshot, ControllerState,
    Result, SnapshotLimits, decode_controller_log_command, decode_controller_state_snapshot,
    encode_controller_state_snapshot,
};

const LOG_MAGIC: &[u8; 8] = b"ASTCTLG1";
const LOG_VERSION: u16 = 1;
const LOG_HEADER_LEN: usize = 16;
const LOG_KIND_APPLICATION: u8 = 1;
const LOG_KIND_LEADER_BARRIER: u8 = 2;

const HARD_MAGIC: &[u8; 8] = b"ASTCHS01";
const HARD_VERSION: u16 = 1;
const HARD_LEN: usize = 48;

const COMMIT_MAGIC: &[u8; 8] = b"ASTCCM01";
const COMMIT_VERSION: u16 = 1;
const COMMIT_LEN: usize = 32;

const COMMIT_INTENT_MAGIC: &[u8; 8] = b"ASTCCI01";
const COMMIT_INTENT_VERSION: u16 = 1;
const COMMIT_INTENT_LEN: usize = 40;

const SNAP_BUNDLE_MAGIC: &[u8; 8] = b"ASTCSNP1";
const SNAP_BUNDLE_VERSION: u16 = 1;
const SNAP_BUNDLE_HEADER_LEN: usize = 20;

const SNAP_INTENT_MAGIC: &[u8; 8] = b"ASTCSI01";
const SNAP_INTENT_VERSION: u16 = 1;
const SNAP_INTENT_LEN: usize = 72;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ControllerPersistenceConfig {
    pub command_limits: CommandLimits,
    pub snapshot_limits: SnapshotLimits,
    pub max_log_entries: usize,
    pub max_log_bytes: usize,
}

impl Default for ControllerPersistenceConfig {
    fn default() -> Self {
        Self {
            command_limits: CommandLimits::default(),
            snapshot_limits: SnapshotLimits::default(),
            max_log_entries: 1_000_000,
            max_log_bytes: 256 * 1024 * 1024,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ControllerSnapshotPackage {
    pub metadata: SnapshotMetadata,
    pub snapshot: ControllerSnapshot,
}

pub fn encode_controller_snapshot_package(
    package: &ControllerSnapshotPackage,
    limits: SnapshotLimits,
) -> Result<Vec<u8>> {
    encode_snapshot_bundle(package, limits)
}

pub fn decode_controller_snapshot_package(
    bytes: &[u8],
    limits: SnapshotLimits,
) -> Result<ControllerSnapshotPackage> {
    decode_snapshot_bundle(bytes, limits)
}

pub struct ControllerPersistence {
    directory: PathBuf,
    config: ControllerPersistenceConfig,
    static_configuration: StableConfiguration,
    hard_state: HardState,
    committed_index: LogIndex,
    snapshot_package: ControllerSnapshotPackage,
    log: Vec<LogEntry>,
}

impl ControllerPersistence {
    pub fn open(
        directory: &Path,
        static_configuration: StableConfiguration,
        initial_state: ControllerState,
        config: ControllerPersistenceConfig,
    ) -> Result<(Self, RecoveredState, ControllerState)> {
        validate_static_configuration(&static_configuration)?;
        fs::create_dir_all(directory)?;

        let log_path = directory.join("controller.raft.log.v1");
        let hard_path = directory.join("controller.raft.hardstate.v1");
        let commit_path = directory.join("controller.raft.commit.v1");
        let snapshot_path = directory.join("controller.snapshot.v1");

        let log = match fs::read(&log_path) {
            Ok(bytes) => decode_log(&bytes, config)?,
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
                atomic_write(&log_path, &encode_log(&[], config)?)?;
                Vec::new()
            }
            Err(error) => return Err(error.into()),
        };

        let hard_state = match fs::read(&hard_path) {
            Ok(bytes) => decode_hard_state(&bytes)?,
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
                if !log.is_empty() {
                    return Err(ControllerError::Invalid(
                        "controller log exists without hard-state sidecar",
                    ));
                }
                let state = HardState::initial();
                atomic_write(&hard_path, &encode_hard_state(&state))?;
                state
            }
            Err(error) => return Err(error.into()),
        };

        let committed_index = match fs::read(&commit_path) {
            Ok(bytes) => decode_commit_marker(&bytes)?,
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
                if hard_state.durable_commit_index != LogIndex::ZERO {
                    return Err(ControllerError::Invalid(
                        "controller hard state has commit without commit sidecar",
                    ));
                }
                atomic_write(&commit_path, &encode_commit_marker(LogIndex::ZERO))?;
                LogIndex::ZERO
            }
            Err(error) => return Err(error.into()),
        };

        let snapshot_package = match fs::read(&snapshot_path) {
            Ok(bytes) => decode_snapshot_bundle(&bytes, config.snapshot_limits)?,
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
                if !log.is_empty()
                    || hard_state != HardState::initial()
                    || committed_index != LogIndex::ZERO
                {
                    return Err(ControllerError::Invalid(
                        "non-empty controller persistence is missing snapshot sidecar",
                    ));
                }
                let metadata = SnapshotMetadata::new(
                    LogIndex::ZERO,
                    Term::ZERO,
                    static_configuration.clone(),
                    SnapshotId::ZERO,
                )?;
                let package = ControllerSnapshotPackage {
                    metadata,
                    snapshot: ControllerSnapshot::new(initial_state.clone()),
                };
                atomic_write(
                    &snapshot_path,
                    &encode_snapshot_bundle(&package, config.snapshot_limits)?,
                )?;
                package
            }
            Err(error) => return Err(error.into()),
        };

        let mut persistence = Self {
            directory: directory.to_path_buf(),
            config,
            static_configuration,
            hard_state,
            committed_index,
            snapshot_package,
            log,
        };
        persistence.recover_snapshot_intent()?;
        persistence.recover_commit_intent()?;
        persistence.reload_authoritative_files()?;
        persistence.validate_persisted_state(initial_state.cluster_id())?;

        let recovered = persistence.recovered_state()?;
        let controller_state = persistence.recover_controller_state()?;
        Ok((persistence, recovered, controller_state))
    }

    pub fn execute(
        &mut self,
        batch: &PersistenceBatch,
        snapshot: Option<&ControllerSnapshotPackage>,
    ) -> Result<()> {
        let mut snapshot_consumed = false;
        for operation in &batch.operations {
            match operation {
                PersistenceOp::UpdateHardState(state) => self.update_hard_state(state)?,
                PersistenceOp::TruncateSuffix { from } => self.truncate_suffix(*from)?,
                PersistenceOp::Append(entries) => self.append_entries(entries)?,
                PersistenceOp::AdvanceCommit { to } => self.advance_commit(*to)?,
                PersistenceOp::InstallSnapshot(metadata) => {
                    let package = snapshot.ok_or(ControllerError::Invalid(
                        "InstallSnapshot requires controller snapshot package",
                    ))?;
                    if snapshot_consumed {
                        return Err(ControllerError::Invalid(
                            "persistence batch contains multiple snapshot installs",
                        ));
                    }
                    if package.metadata != *metadata {
                        return Err(ControllerError::Invalid(
                            "controller snapshot package metadata mismatch",
                        ));
                    }
                    self.install_snapshot(package)?;
                    snapshot_consumed = true;
                }
                PersistenceOp::DiscardPrefix { through } => self.discard_prefix(*through)?,
            }
        }
        if snapshot.is_some() && !snapshot_consumed {
            return Err(ControllerError::Invalid(
                "snapshot package supplied without InstallSnapshot operation",
            ));
        }
        self.validate_persisted_state(self.snapshot_package.snapshot.state.cluster_id())
    }

    pub fn recovered_state(&self) -> Result<RecoveredState> {
        Ok(RecoveredState::from_persisted(
            self.hard_state.clone(),
            self.snapshot_package.metadata.clone(),
            self.log.clone(),
        )?)
    }

    pub fn recover_controller_state(&self) -> Result<ControllerState> {
        let mut state = self.snapshot_package.snapshot.state.clone();
        if state.metadata_epoch() > self.snapshot_package.metadata.last_included_index.get() {
            return Err(ControllerError::Invalid(
                "controller snapshot metadata epoch exceeds Raft snapshot boundary",
            ));
        }
        for entry in &self.log {
            if entry.index > self.hard_state.durable_commit_index {
                break;
            }
            match &entry.kind {
                EntryKind::Application(payload) => {
                    match decode_controller_log_command(payload, self.config.command_limits)? {
                        ControllerLogCommand::V1(command) => {
                            state.apply(entry.index.get(), &command)?;
                        }
                        ControllerLogCommand::ProducerV2(command) => {
                            state.apply_producer_session(entry.index.get(), command)?;
                        }
                        ControllerLogCommand::PerformanceV3(command) => {
                            state.apply_topic_performance(entry.index.get(), &command)?;
                        }
                        ControllerLogCommand::RetentionV4(command) => {
                            state.apply_topic_retention(entry.index.get(), &command)?;
                        }
                    }
                }
                EntryKind::LeaderBarrier => {}
                EntryKind::Configuration(_) => {
                    return Err(ControllerError::Invalid(
                        "controller log contains dynamic membership entry outside P5 scope",
                    ));
                }
            }
        }
        Ok(state)
    }

    pub fn build_snapshot_package(
        &self,
        state: &ControllerState,
        metadata: &SnapshotMetadata,
    ) -> Result<ControllerSnapshotPackage> {
        if metadata.last_included_index == LogIndex::ZERO
            || metadata.last_included_index > self.hard_state.durable_commit_index
            || state.metadata_epoch() > metadata.last_included_index.get()
            || state.cluster_id() != self.snapshot_package.snapshot.state.cluster_id()
            || metadata.configuration != self.static_configuration
            || self.term_at(metadata.last_included_index) != Some(metadata.last_included_term)
        {
            return Err(ControllerError::Invalid(
                "controller snapshot build does not match committed durable history",
            ));
        }
        let package = ControllerSnapshotPackage {
            metadata: metadata.clone(),
            snapshot: ControllerSnapshot::new(state.clone()),
        };
        encode_snapshot_bundle(&package, self.config.snapshot_limits)?;
        Ok(package)
    }

    pub const fn hard_state(&self) -> &HardState {
        &self.hard_state
    }

    pub const fn committed_index(&self) -> LogIndex {
        self.committed_index
    }

    pub fn snapshot_package(&self) -> &ControllerSnapshotPackage {
        &self.snapshot_package
    }

    pub fn log(&self) -> &[LogEntry] {
        &self.log
    }

    pub fn directory(&self) -> &Path {
        &self.directory
    }

    fn reload_authoritative_files(&mut self) -> Result<()> {
        self.log = decode_log(&fs::read(self.log_path())?, self.config)?;
        self.hard_state = decode_hard_state(&fs::read(self.hard_path())?)?;
        self.committed_index = decode_commit_marker(&fs::read(self.commit_path())?)?;
        self.snapshot_package = decode_snapshot_bundle(
            &fs::read(self.snapshot_path())?,
            self.config.snapshot_limits,
        )?;
        Ok(())
    }

    fn validate_persisted_state(&self, expected_cluster_id: &[u8; 16]) -> Result<()> {
        if self.snapshot_package.snapshot.state.cluster_id() != expected_cluster_id {
            return Err(ControllerError::Invalid(
                "controller snapshot belongs to another cluster",
            ));
        }
        if self.snapshot_package.metadata.configuration != self.static_configuration {
            return Err(ControllerError::Invalid(
                "controller snapshot voter configuration differs from static bootstrap",
            ));
        }
        if self.snapshot_package.snapshot.state.metadata_epoch()
            > self.snapshot_package.metadata.last_included_index.get()
        {
            return Err(ControllerError::Invalid(
                "controller snapshot state is ahead of Raft snapshot boundary",
            ));
        }
        if self.hard_state.durable_commit_index != self.committed_index {
            return Err(ControllerError::Invalid(
                "controller hard-state/commit-marker mismatch without recoverable intent",
            ));
        }
        let recovered = self.recovered_state()?;
        recovered.validate()?;
        self.recover_controller_state()?;
        Ok(())
    }

    fn update_hard_state(&mut self, state: &HardState) -> Result<()> {
        if state.durable_commit_index != self.committed_index {
            return Err(ControllerError::Invalid(
                "hard-state update may not change commit outside AdvanceCommit",
            ));
        }
        if state.current_term < self.hard_state.current_term {
            return Err(ControllerError::Invalid(
                "controller hard-state term cannot move backward",
            ));
        }
        atomic_write(&self.hard_path(), &encode_hard_state(state))?;
        self.hard_state = state.clone();
        Ok(())
    }

    fn append_entries(&mut self, entries: &[LogEntry]) -> Result<()> {
        if entries.is_empty() {
            return Ok(());
        }
        let mut next = self
            .log
            .last()
            .map_or(
                self.snapshot_package.metadata.last_included_index,
                |entry| entry.index,
            )
            .checked_next()?;
        let mut staged = self.log.clone();
        for entry in entries {
            if entry.index != next || entry.term == Term::ZERO {
                return Err(ControllerError::Invalid(
                    "controller Raft append is not contiguous/non-zero-term",
                ));
            }
            match &entry.kind {
                EntryKind::Application(payload) => {
                    decode_controller_log_command(payload, self.config.command_limits)?;
                }
                EntryKind::LeaderBarrier => {}
                EntryKind::Configuration(_) => {
                    return Err(ControllerError::Invalid(
                        "dynamic controller membership is outside P5 scope",
                    ));
                }
            }
            staged.push(entry.clone());
            next = next.checked_next()?;
        }
        self.persist_log(&staged)?;
        self.log = staged;
        Ok(())
    }

    fn truncate_suffix(&mut self, from: LogIndex) -> Result<()> {
        if from <= self.committed_index
            || from <= self.snapshot_package.metadata.last_included_index
        {
            return Err(ControllerError::Invalid(
                "controller suffix truncation would cross committed/snapshot boundary",
            ));
        }
        let staged = self
            .log
            .iter()
            .filter(|entry| entry.index < from)
            .cloned()
            .collect::<Vec<_>>();
        self.persist_log(&staged)?;
        self.log = staged;
        Ok(())
    }

    fn advance_commit(&mut self, target: LogIndex) -> Result<()> {
        let previous = self.committed_index;
        if target < previous {
            return Err(ControllerError::Invalid(
                "controller durable commit index cannot move backward",
            ));
        }
        if target == previous {
            return Ok(());
        }
        if target > self.stable_last_index() {
            return Err(ControllerError::Invalid(
                "controller commit target exceeds durable history",
            ));
        }
        let intent = CommitIntent { previous, target };
        atomic_write(&self.commit_intent_path(), &encode_commit_intent(intent))?;
        atomic_write(&self.commit_path(), &encode_commit_marker(target))?;
        let mut hard = self.hard_state.clone();
        hard.durable_commit_index = target;
        atomic_write(&self.hard_path(), &encode_hard_state(&hard))?;
        remove_file_sync(&self.commit_intent_path())?;
        self.committed_index = target;
        self.hard_state = hard;
        Ok(())
    }

    fn install_snapshot(&mut self, package: &ControllerSnapshotPackage) -> Result<()> {
        validate_snapshot_package(
            package,
            &self.static_configuration,
            self.snapshot_package.snapshot.state.cluster_id(),
            self.config.snapshot_limits,
        )?;
        if package.metadata.last_included_index
            <= self.snapshot_package.metadata.last_included_index
        {
            return Err(ControllerError::Invalid(
                "controller snapshot install must advance snapshot boundary",
            ));
        }

        let retain_suffix = self.term_at(package.metadata.last_included_index)
            == Some(package.metadata.last_included_term);
        let previous_commit = self.committed_index;
        let target_commit = previous_commit.max(package.metadata.last_included_index);
        let intent = SnapshotIntent {
            previous_commit,
            target_commit,
            snapshot_index: package.metadata.last_included_index,
            snapshot_id: package.metadata.snapshot_id,
            retain_suffix,
        };
        atomic_write(
            &self.snapshot_intent_path(),
            &encode_snapshot_intent(intent),
        )?;
        atomic_write(
            &self.snapshot_path(),
            &encode_snapshot_bundle(package, self.config.snapshot_limits)?,
        )?;

        let retained = if retain_suffix {
            self.log
                .iter()
                .filter(|entry| entry.index > package.metadata.last_included_index)
                .cloned()
                .collect::<Vec<_>>()
        } else {
            Vec::new()
        };
        self.persist_log(&retained)?;

        if target_commit != previous_commit {
            atomic_write(&self.commit_path(), &encode_commit_marker(target_commit))?;
            let mut hard = self.hard_state.clone();
            hard.durable_commit_index = target_commit;
            atomic_write(&self.hard_path(), &encode_hard_state(&hard))?;
            self.hard_state = hard;
            self.committed_index = target_commit;
        }
        remove_file_sync(&self.snapshot_intent_path())?;
        self.snapshot_package = package.clone();
        self.log = retained;
        Ok(())
    }

    fn discard_prefix(&mut self, through: LogIndex) -> Result<()> {
        if through > self.snapshot_package.metadata.last_included_index {
            return Err(ControllerError::Invalid(
                "controller log compaction exceeds durable snapshot boundary",
            ));
        }
        let retained = self
            .log
            .iter()
            .filter(|entry| entry.index > through)
            .cloned()
            .collect::<Vec<_>>();
        if retained != self.log {
            self.persist_log(&retained)?;
            self.log = retained;
        }
        Ok(())
    }

    fn recover_commit_intent(&mut self) -> Result<()> {
        let path = self.commit_intent_path();
        let Some(intent) = read_optional(&path)?
            .map(|bytes| decode_commit_intent(&bytes))
            .transpose()?
        else {
            return Ok(());
        };
        let hard = decode_hard_state(&fs::read(self.hard_path())?)?;
        let marker = decode_commit_marker(&fs::read(self.commit_path())?)?;
        if !matches!(hard.durable_commit_index, value if value == intent.previous || value == intent.target)
            || !matches!(marker, value if value == intent.previous || value == intent.target)
            || intent.target > self.stable_last_index()
        {
            return Err(ControllerError::Invalid(
                "controller commit intent does not explain durable state",
            ));
        }
        atomic_write(&self.commit_path(), &encode_commit_marker(intent.target))?;
        let mut completed = hard;
        completed.durable_commit_index = intent.target;
        atomic_write(&self.hard_path(), &encode_hard_state(&completed))?;
        remove_file_sync(&path)
    }

    fn recover_snapshot_intent(&mut self) -> Result<()> {
        let path = self.snapshot_intent_path();
        let Some(intent) = read_optional(&path)?
            .map(|bytes| decode_snapshot_intent(&bytes))
            .transpose()?
        else {
            return Ok(());
        };
        let hard = decode_hard_state(&fs::read(self.hard_path())?)?;
        let marker = decode_commit_marker(&fs::read(self.commit_path())?)?;
        let installed = decode_snapshot_bundle(
            &fs::read(self.snapshot_path())?,
            self.config.snapshot_limits,
        )?;
        let package_matches = installed.metadata.last_included_index == intent.snapshot_index
            && installed.metadata.snapshot_id == intent.snapshot_id;

        if !package_matches {
            if hard.durable_commit_index != intent.previous_commit
                || marker != intent.previous_commit
            {
                return Err(ControllerError::Invalid(
                    "snapshot intent lacks published snapshot but commit state advanced",
                ));
            }
            return remove_file_sync(&path);
        }

        if !matches!(hard.durable_commit_index, value if value == intent.previous_commit || value == intent.target_commit)
            || !matches!(marker, value if value == intent.previous_commit || value == intent.target_commit)
        {
            return Err(ControllerError::Invalid(
                "snapshot intent does not explain durable commit state",
            ));
        }

        let current_log = decode_log(&fs::read(self.log_path())?, self.config)?;
        let retained = if intent.retain_suffix {
            current_log
                .into_iter()
                .filter(|entry| entry.index > intent.snapshot_index)
                .collect::<Vec<_>>()
        } else {
            Vec::new()
        };
        self.persist_log(&retained)?;
        atomic_write(
            &self.commit_path(),
            &encode_commit_marker(intent.target_commit),
        )?;
        let mut completed = hard;
        completed.durable_commit_index = intent.target_commit;
        atomic_write(&self.hard_path(), &encode_hard_state(&completed))?;
        remove_file_sync(&path)
    }

    fn persist_log(&self, entries: &[LogEntry]) -> Result<()> {
        atomic_write(&self.log_path(), &encode_log(entries, self.config)?)
    }

    fn stable_last_index(&self) -> LogIndex {
        self.log.last().map_or(
            self.snapshot_package.metadata.last_included_index,
            |entry| entry.index,
        )
    }

    fn term_at(&self, index: LogIndex) -> Option<Term> {
        if index == self.snapshot_package.metadata.last_included_index {
            return Some(self.snapshot_package.metadata.last_included_term);
        }
        self.log
            .iter()
            .find(|entry| entry.index == index)
            .map(|entry| entry.term)
    }

    fn log_path(&self) -> PathBuf {
        self.directory.join("controller.raft.log.v1")
    }

    fn hard_path(&self) -> PathBuf {
        self.directory.join("controller.raft.hardstate.v1")
    }

    fn commit_path(&self) -> PathBuf {
        self.directory.join("controller.raft.commit.v1")
    }

    fn commit_intent_path(&self) -> PathBuf {
        self.directory.join("controller.raft.commit-intent.v1")
    }

    fn snapshot_path(&self) -> PathBuf {
        self.directory.join("controller.snapshot.v1")
    }

    fn snapshot_intent_path(&self) -> PathBuf {
        self.directory.join("controller.snapshot-intent.v1")
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct CommitIntent {
    previous: LogIndex,
    target: LogIndex,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct SnapshotIntent {
    previous_commit: LogIndex,
    target_commit: LogIndex,
    snapshot_index: LogIndex,
    snapshot_id: SnapshotId,
    retain_suffix: bool,
}

fn validate_static_configuration(configuration: &StableConfiguration) -> Result<()> {
    if !matches!(configuration.voter_count(), 3 | 5) || configuration.learners().next().is_some() {
        return Err(ControllerError::Invalid(
            "P5 controller configuration requires exactly 3 or 5 static voters and no learners",
        ));
    }
    Ok(())
}

fn validate_snapshot_package(
    package: &ControllerSnapshotPackage,
    expected_configuration: &StableConfiguration,
    expected_cluster_id: &[u8; 16],
    limits: SnapshotLimits,
) -> Result<()> {
    if package.metadata.last_included_index == LogIndex::ZERO
        || package.metadata.last_included_term == Term::ZERO
        || package.metadata.configuration != *expected_configuration
        || package.snapshot.state.cluster_id() != expected_cluster_id
        || package.snapshot.state.metadata_epoch() > package.metadata.last_included_index.get()
    {
        return Err(ControllerError::Invalid(
            "invalid controller snapshot package identity/boundary",
        ));
    }
    encode_snapshot_bundle(package, limits)?;
    Ok(())
}

fn encode_log(entries: &[LogEntry], config: ControllerPersistenceConfig) -> Result<Vec<u8>> {
    if entries.len() > config.max_log_entries {
        return Err(ControllerError::LimitExceeded {
            what: "controller Raft log entry count",
            actual: entries.len(),
            max: config.max_log_entries,
        });
    }
    let mut out = Vec::new();
    out.extend_from_slice(LOG_MAGIC);
    out.extend_from_slice(&LOG_VERSION.to_le_bytes());
    out.extend_from_slice(&(LOG_HEADER_LEN as u16).to_le_bytes());
    out.extend_from_slice(
        &u32::try_from(entries.len())
            .map_err(|_| ControllerError::Invalid("controller log entry count overflow"))?
            .to_le_bytes(),
    );
    for entry in entries {
        if entry.term == Term::ZERO || entry.index == LogIndex::ZERO {
            return Err(ControllerError::Invalid(
                "controller log cannot encode term/index zero entry",
            ));
        }
        let (kind, payload): (u8, &[u8]) = match &entry.kind {
            EntryKind::Application(payload) => {
                decode_controller_log_command(payload, config.command_limits)?;
                (LOG_KIND_APPLICATION, payload)
            }
            EntryKind::LeaderBarrier => (LOG_KIND_LEADER_BARRIER, &[]),
            EntryKind::Configuration(_) => {
                return Err(ControllerError::Invalid(
                    "controller log cannot encode dynamic membership in P5",
                ));
            }
        };
        let payload_len = u32::try_from(payload.len())
            .map_err(|_| ControllerError::Invalid("controller log payload length overflow"))?;
        let record_len = 28_usize
            .checked_add(payload.len())
            .ok_or(ControllerError::Invalid(
                "controller log record length overflow",
            ))?;
        out.extend_from_slice(
            &u32::try_from(record_len)
                .map_err(|_| ControllerError::Invalid("controller log record length overflow"))?
                .to_le_bytes(),
        );
        let crc_start = out.len();
        out.extend_from_slice(&entry.term.get().to_le_bytes());
        out.extend_from_slice(&entry.index.get().to_le_bytes());
        out.push(kind);
        out.extend_from_slice(&[0; 3]);
        out.extend_from_slice(&payload_len.to_le_bytes());
        out.extend_from_slice(payload);
        let crc = crc32c::crc32c(&out[crc_start..]);
        out.extend_from_slice(&crc.to_le_bytes());
        if out.len().saturating_add(4) > config.max_log_bytes {
            return Err(ControllerError::LimitExceeded {
                what: "controller Raft log bytes",
                actual: out.len().saturating_add(4),
                max: config.max_log_bytes,
            });
        }
    }
    let crc = crc32c::crc32c(&out);
    out.extend_from_slice(&crc.to_le_bytes());
    Ok(out)
}

fn decode_log(bytes: &[u8], config: ControllerPersistenceConfig) -> Result<Vec<LogEntry>> {
    if bytes.len() > config.max_log_bytes {
        return Err(ControllerError::LimitExceeded {
            what: "controller Raft log bytes",
            actual: bytes.len(),
            max: config.max_log_bytes,
        });
    }
    if bytes.len() < LOG_HEADER_LEN + 4
        || &bytes[0..8] != LOG_MAGIC
        || read_u16(bytes, 8)? != LOG_VERSION
        || usize::from(read_u16(bytes, 10)?) != LOG_HEADER_LEN
    {
        return Err(ControllerError::Invalid(
            "controller Raft log header mismatch",
        ));
    }
    verify_crc(bytes, bytes.len() - 4, "controller Raft log")?;
    let count = usize::try_from(read_u32(bytes, 12)?)
        .map_err(|_| ControllerError::Invalid("controller log count overflow"))?;
    if count > config.max_log_entries {
        return Err(ControllerError::LimitExceeded {
            what: "controller Raft log entry count",
            actual: count,
            max: config.max_log_entries,
        });
    }
    let mut position = LOG_HEADER_LEN;
    let end = bytes.len() - 4;
    let mut entries = Vec::with_capacity(count);
    for _ in 0..count {
        let record_len = usize::try_from(read_u32(bytes, position)?)
            .map_err(|_| ControllerError::Invalid("controller log record length overflow"))?;
        if record_len < 28 {
            return Err(ControllerError::Invalid(
                "controller log record is too short",
            ));
        }
        position += 4;
        let record_end = position
            .checked_add(record_len)
            .ok_or(ControllerError::Invalid(
                "controller log record length overflow",
            ))?;
        if record_end > end {
            return Err(ControllerError::Invalid(
                "controller log record is truncated",
            ));
        }
        verify_crc(
            &bytes[position..record_end],
            record_len - 4,
            "controller log record",
        )?;
        let term = Term::new(read_u64(bytes, position)?);
        let index = LogIndex::new(read_u64(bytes, position + 8)?);
        let kind = bytes[position + 16];
        if bytes[position + 17..position + 20] != [0, 0, 0] {
            return Err(ControllerError::Invalid(
                "controller log record reserved bytes are non-zero",
            ));
        }
        let payload_len = usize::try_from(read_u32(bytes, position + 20)?)
            .map_err(|_| ControllerError::Invalid("controller log payload length overflow"))?;
        if 28_usize.checked_add(payload_len) != Some(record_len) {
            return Err(ControllerError::Invalid(
                "controller log payload length mismatch",
            ));
        }
        let payload = &bytes[position + 24..position + 24 + payload_len];
        let entry_kind = match kind {
            LOG_KIND_APPLICATION => {
                decode_controller_log_command(payload, config.command_limits)?;
                EntryKind::Application(payload.to_vec())
            }
            LOG_KIND_LEADER_BARRIER if payload.is_empty() => EntryKind::LeaderBarrier,
            _ => {
                return Err(ControllerError::Invalid(
                    "unknown/invalid controller log entry kind",
                ));
            }
        };
        entries.push(LogEntry::new(term, index, entry_kind));
        position = record_end;
    }
    if position != end {
        return Err(ControllerError::Invalid(
            "controller log contains trailing bytes",
        ));
    }
    if entries
        .windows(2)
        .any(|window| window[0].index.checked_next().ok() != Some(window[1].index))
    {
        return Err(ControllerError::Invalid(
            "controller log indexes are not contiguous",
        ));
    }
    Ok(entries)
}

fn encode_hard_state(state: &HardState) -> [u8; HARD_LEN] {
    let mut bytes = [0_u8; HARD_LEN];
    bytes[0..8].copy_from_slice(HARD_MAGIC);
    bytes[8..10].copy_from_slice(&HARD_VERSION.to_le_bytes());
    bytes[10..12].copy_from_slice(&(HARD_LEN as u16).to_le_bytes());
    bytes[16..24].copy_from_slice(&state.current_term.get().to_le_bytes());
    bytes[24..32].copy_from_slice(&state.voted_for.map_or(0, NodeId::get).to_le_bytes());
    bytes[32..40].copy_from_slice(&state.durable_commit_index.get().to_le_bytes());
    let crc = crc32c::crc32c(&bytes[..44]);
    bytes[44..48].copy_from_slice(&crc.to_le_bytes());
    bytes
}

fn decode_hard_state(bytes: &[u8]) -> Result<HardState> {
    if bytes.len() != HARD_LEN
        || &bytes[0..8] != HARD_MAGIC
        || read_u16(bytes, 8)? != HARD_VERSION
        || usize::from(read_u16(bytes, 10)?) != HARD_LEN
        || read_u32(bytes, 12)? != 0
        || read_u32(bytes, 40)? != 0
    {
        return Err(ControllerError::Invalid(
            "controller hard-state sidecar header mismatch",
        ));
    }
    verify_crc(bytes, 44, "controller hard-state sidecar")?;
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

fn encode_commit_marker(index: LogIndex) -> [u8; COMMIT_LEN] {
    let mut bytes = [0_u8; COMMIT_LEN];
    bytes[0..8].copy_from_slice(COMMIT_MAGIC);
    bytes[8..10].copy_from_slice(&COMMIT_VERSION.to_le_bytes());
    bytes[10..12].copy_from_slice(&(COMMIT_LEN as u16).to_le_bytes());
    bytes[16..24].copy_from_slice(&index.get().to_le_bytes());
    let crc = crc32c::crc32c(&bytes[..28]);
    bytes[28..32].copy_from_slice(&crc.to_le_bytes());
    bytes
}

fn decode_commit_marker(bytes: &[u8]) -> Result<LogIndex> {
    if bytes.len() != COMMIT_LEN
        || &bytes[0..8] != COMMIT_MAGIC
        || read_u16(bytes, 8)? != COMMIT_VERSION
        || usize::from(read_u16(bytes, 10)?) != COMMIT_LEN
        || read_u32(bytes, 12)? != 0
        || read_u32(bytes, 24)? != 0
    {
        return Err(ControllerError::Invalid(
            "controller commit sidecar header mismatch",
        ));
    }
    verify_crc(bytes, 28, "controller commit sidecar")?;
    Ok(LogIndex::new(read_u64(bytes, 16)?))
}

fn encode_commit_intent(intent: CommitIntent) -> [u8; COMMIT_INTENT_LEN] {
    let mut bytes = [0_u8; COMMIT_INTENT_LEN];
    bytes[0..8].copy_from_slice(COMMIT_INTENT_MAGIC);
    bytes[8..10].copy_from_slice(&COMMIT_INTENT_VERSION.to_le_bytes());
    bytes[10..12].copy_from_slice(&(COMMIT_INTENT_LEN as u16).to_le_bytes());
    bytes[16..24].copy_from_slice(&intent.previous.get().to_le_bytes());
    bytes[24..32].copy_from_slice(&intent.target.get().to_le_bytes());
    let crc = crc32c::crc32c(&bytes[..36]);
    bytes[36..40].copy_from_slice(&crc.to_le_bytes());
    bytes
}

fn decode_commit_intent(bytes: &[u8]) -> Result<CommitIntent> {
    if bytes.len() != COMMIT_INTENT_LEN
        || &bytes[0..8] != COMMIT_INTENT_MAGIC
        || read_u16(bytes, 8)? != COMMIT_INTENT_VERSION
        || usize::from(read_u16(bytes, 10)?) != COMMIT_INTENT_LEN
        || read_u32(bytes, 12)? != 0
        || read_u32(bytes, 32)? != 0
    {
        return Err(ControllerError::Invalid(
            "controller commit-intent header mismatch",
        ));
    }
    verify_crc(bytes, 36, "controller commit intent")?;
    let previous = LogIndex::new(read_u64(bytes, 16)?);
    let target = LogIndex::new(read_u64(bytes, 24)?);
    if target <= previous {
        return Err(ControllerError::Invalid(
            "controller commit intent target must advance",
        ));
    }
    Ok(CommitIntent { previous, target })
}

fn encode_snapshot_intent(intent: SnapshotIntent) -> [u8; SNAP_INTENT_LEN] {
    let mut bytes = [0_u8; SNAP_INTENT_LEN];
    bytes[0..8].copy_from_slice(SNAP_INTENT_MAGIC);
    bytes[8..10].copy_from_slice(&SNAP_INTENT_VERSION.to_le_bytes());
    bytes[10..12].copy_from_slice(&(SNAP_INTENT_LEN as u16).to_le_bytes());
    bytes[16..24].copy_from_slice(&intent.previous_commit.get().to_le_bytes());
    bytes[24..32].copy_from_slice(&intent.target_commit.get().to_le_bytes());
    bytes[32..40].copy_from_slice(&intent.snapshot_index.get().to_le_bytes());
    bytes[40..56].copy_from_slice(intent.snapshot_id.as_bytes());
    bytes[56] = u8::from(intent.retain_suffix);
    let crc = crc32c::crc32c(&bytes[..68]);
    bytes[68..72].copy_from_slice(&crc.to_le_bytes());
    bytes
}

fn decode_snapshot_intent(bytes: &[u8]) -> Result<SnapshotIntent> {
    if bytes.len() != SNAP_INTENT_LEN
        || &bytes[0..8] != SNAP_INTENT_MAGIC
        || read_u16(bytes, 8)? != SNAP_INTENT_VERSION
        || usize::from(read_u16(bytes, 10)?) != SNAP_INTENT_LEN
        || read_u32(bytes, 12)? != 0
        || bytes[57..68] != [0; 11]
    {
        return Err(ControllerError::Invalid(
            "controller snapshot-intent header mismatch",
        ));
    }
    verify_crc(bytes, 68, "controller snapshot intent")?;
    let retain_suffix = match bytes[56] {
        0 => false,
        1 => true,
        _ => {
            return Err(ControllerError::Invalid(
                "controller snapshot intent retain flag is invalid",
            ));
        }
    };
    let snapshot_id = SnapshotId::new(bytes[40..56].try_into().expect("snapshot id slice"));
    let previous_commit = LogIndex::new(read_u64(bytes, 16)?);
    let target_commit = LogIndex::new(read_u64(bytes, 24)?);
    let snapshot_index = LogIndex::new(read_u64(bytes, 32)?);
    if target_commit < previous_commit || target_commit < snapshot_index {
        return Err(ControllerError::Invalid(
            "controller snapshot intent commit bounds are invalid",
        ));
    }
    Ok(SnapshotIntent {
        previous_commit,
        target_commit,
        snapshot_index,
        snapshot_id,
        retain_suffix,
    })
}

fn encode_snapshot_bundle(
    package: &ControllerSnapshotPackage,
    limits: SnapshotLimits,
) -> Result<Vec<u8>> {
    let metadata = encode_snapshot_metadata(&package.metadata)?;
    let state = encode_controller_state_snapshot(&package.snapshot.state, limits)?;
    let mut out = Vec::with_capacity(
        SNAP_BUNDLE_HEADER_LEN
            .saturating_add(metadata.len())
            .saturating_add(state.len())
            .saturating_add(4),
    );
    out.extend_from_slice(SNAP_BUNDLE_MAGIC);
    out.extend_from_slice(&SNAP_BUNDLE_VERSION.to_le_bytes());
    out.extend_from_slice(&(SNAP_BUNDLE_HEADER_LEN as u16).to_le_bytes());
    out.extend_from_slice(
        &u32::try_from(metadata.len())
            .map_err(|_| ControllerError::Invalid("snapshot metadata length overflow"))?
            .to_le_bytes(),
    );
    out.extend_from_slice(
        &u32::try_from(state.len())
            .map_err(|_| ControllerError::Invalid("controller snapshot length overflow"))?
            .to_le_bytes(),
    );
    out.extend_from_slice(&metadata);
    out.extend_from_slice(&state);
    let crc = crc32c::crc32c(&out);
    out.extend_from_slice(&crc.to_le_bytes());
    Ok(out)
}

fn decode_snapshot_bundle(
    bytes: &[u8],
    limits: SnapshotLimits,
) -> Result<ControllerSnapshotPackage> {
    if bytes.len() < SNAP_BUNDLE_HEADER_LEN + 4
        || &bytes[0..8] != SNAP_BUNDLE_MAGIC
        || read_u16(bytes, 8)? != SNAP_BUNDLE_VERSION
        || usize::from(read_u16(bytes, 10)?) != SNAP_BUNDLE_HEADER_LEN
    {
        return Err(ControllerError::Invalid(
            "controller snapshot bundle header mismatch",
        ));
    }
    verify_crc(bytes, bytes.len() - 4, "controller snapshot bundle")?;
    let metadata_len = usize::try_from(read_u32(bytes, 12)?)
        .map_err(|_| ControllerError::Invalid("snapshot metadata length overflow"))?;
    let state_len = usize::try_from(read_u32(bytes, 16)?)
        .map_err(|_| ControllerError::Invalid("controller snapshot length overflow"))?;
    let metadata_end = SNAP_BUNDLE_HEADER_LEN
        .checked_add(metadata_len)
        .ok_or(ControllerError::Invalid("snapshot bundle length overflow"))?;
    let state_end = metadata_end
        .checked_add(state_len)
        .ok_or(ControllerError::Invalid("snapshot bundle length overflow"))?;
    if state_end.checked_add(4) != Some(bytes.len()) {
        return Err(ControllerError::Invalid(
            "controller snapshot bundle length mismatch",
        ));
    }
    let metadata = decode_snapshot_metadata(&bytes[SNAP_BUNDLE_HEADER_LEN..metadata_end])?;
    let snapshot = decode_controller_state_snapshot(&bytes[metadata_end..state_end], limits)?;
    Ok(ControllerSnapshotPackage { metadata, snapshot })
}

fn encode_snapshot_metadata(metadata: &SnapshotMetadata) -> Result<Vec<u8>> {
    let voters = metadata.configuration.voters().collect::<Vec<_>>();
    let learners = metadata.configuration.learners().collect::<Vec<_>>();
    let mut out = Vec::new();
    out.extend_from_slice(&metadata.last_included_index.get().to_le_bytes());
    out.extend_from_slice(&metadata.last_included_term.get().to_le_bytes());
    out.extend_from_slice(metadata.snapshot_id.as_bytes());
    out.extend_from_slice(
        &u32::try_from(voters.len())
            .map_err(|_| ControllerError::Invalid("snapshot voter count overflow"))?
            .to_le_bytes(),
    );
    for voter in voters {
        out.extend_from_slice(&voter.get().to_le_bytes());
    }
    out.extend_from_slice(
        &u32::try_from(learners.len())
            .map_err(|_| ControllerError::Invalid("snapshot learner count overflow"))?
            .to_le_bytes(),
    );
    for learner in learners {
        out.extend_from_slice(&learner.get().to_le_bytes());
    }
    Ok(out)
}

fn decode_snapshot_metadata(bytes: &[u8]) -> Result<SnapshotMetadata> {
    if bytes.len() < 40 {
        return Err(ControllerError::Invalid(
            "controller Raft snapshot metadata is truncated",
        ));
    }
    let index = LogIndex::new(read_u64(bytes, 0)?);
    let term = Term::new(read_u64(bytes, 8)?);
    let snapshot_id = SnapshotId::new(bytes[16..32].try_into().expect("snapshot id slice"));
    let voter_count = usize::try_from(read_u32(bytes, 32)?)
        .map_err(|_| ControllerError::Invalid("snapshot voter count overflow"))?;
    if !matches!(voter_count, 3 | 5) {
        return Err(ControllerError::Invalid(
            "controller snapshot voter count must be 3 or 5",
        ));
    }
    let mut position = 36;
    let mut voters = Vec::with_capacity(voter_count);
    for _ in 0..voter_count {
        voters.push(NodeId::new(read_u64(bytes, position)?)?);
        position += 8;
    }
    let learner_count = usize::try_from(read_u32(bytes, position)?)
        .map_err(|_| ControllerError::Invalid("snapshot learner count overflow"))?;
    position += 4;
    if learner_count != 0 {
        return Err(ControllerError::Invalid(
            "P5 controller snapshot cannot contain learners",
        ));
    }
    if position != bytes.len() {
        return Err(ControllerError::Invalid(
            "controller snapshot metadata contains trailing bytes",
        ));
    }
    SnapshotMetadata::new(
        index,
        term,
        StableConfiguration::new(voters, [])?,
        snapshot_id,
    )
    .map_err(Into::into)
}

fn verify_crc(bytes: &[u8], crc_offset: usize, context: &'static str) -> Result<()> {
    let expected = read_u32(bytes, crc_offset)?;
    let actual = crc32c::crc32c(&bytes[..crc_offset]);
    if expected != actual {
        return Err(ControllerError::ChecksumMismatch {
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
            .ok_or(ControllerError::Invalid("persistence file is truncated"))?
            .try_into()
            .expect("two-byte slice"),
    ))
}

fn read_u32(bytes: &[u8], offset: usize) -> Result<u32> {
    Ok(u32::from_le_bytes(
        bytes
            .get(offset..offset + 4)
            .ok_or(ControllerError::Invalid("persistence file is truncated"))?
            .try_into()
            .expect("four-byte slice"),
    ))
}

fn read_u64(bytes: &[u8], offset: usize) -> Result<u64> {
    Ok(u64::from_le_bytes(
        bytes
            .get(offset..offset + 8)
            .ok_or(ControllerError::Invalid("persistence file is truncated"))?
            .try_into()
            .expect("eight-byte slice"),
    ))
}

fn read_optional(path: &Path) -> Result<Option<Vec<u8>>> {
    match fs::read(path) {
        Ok(bytes) => Ok(Some(bytes)),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(None),
        Err(error) => Err(error.into()),
    }
}

fn atomic_write(path: &Path, bytes: &[u8]) -> Result<()> {
    let parent = path
        .parent()
        .ok_or(ControllerError::Invalid("persistence file has no parent"))?;
    fs::create_dir_all(parent)?;
    let name =
        path.file_name()
            .and_then(|value| value.to_str())
            .ok_or(ControllerError::Invalid(
                "persistence filename is not UTF-8",
            ))?;
    let temp = parent.join(format!(".{name}.tmp"));
    {
        let mut file = OpenOptions::new()
            .create(true)
            .truncate(true)
            .write(true)
            .open(&temp)?;
        file.write_all(bytes)?;
        file.sync_all()?;
    }
    fs::rename(&temp, path)?;
    sync_directory(parent)
}

fn remove_file_sync(path: &Path) -> Result<()> {
    match fs::remove_file(path) {
        Ok(()) => {
            let parent = path
                .parent()
                .ok_or(ControllerError::Invalid("persistence file has no parent"))?;
            sync_directory(parent)
        }
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(()),
        Err(error) => Err(error.into()),
    }
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
    use std::time::{SystemTime, UNIX_EPOCH};

    use astralog_raft::{PersistenceToken, SnapshotId};

    use super::*;
    use crate::{BrokerLifecycle, ControllerCommand};

    fn node(value: u64) -> NodeId {
        NodeId::new(value).unwrap()
    }

    fn voters() -> StableConfiguration {
        StableConfiguration::new([node(1), node(2), node(3)], []).unwrap()
    }

    fn initial_state() -> ControllerState {
        ControllerState::new([0x51; 16], 1, 1_000).unwrap()
    }

    fn temp_dir(label: &str) -> PathBuf {
        let nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let path = std::env::temp_dir().join(format!(
            "astralog-p5c-{label}-{}-{nanos}",
            std::process::id()
        ));
        fs::create_dir_all(&path).unwrap();
        path
    }

    fn register(id: u32) -> ControllerCommand {
        ControllerCommand::RegisterBroker {
            broker_id: id,
            node_uuid: [u8::try_from(id).unwrap(); 16],
            initial_incarnation: 1,
            client_host: format!("broker-{id}"),
            client_port: 9000 + u16::try_from(id).unwrap(),
            internal_host: format!("internal-{id}"),
            internal_port: 10000 + u16::try_from(id).unwrap(),
            rack: None,
            zone: None,
            min_feature: 1,
            max_feature: 3,
        }
    }

    fn app(term: u64, index: u64, command: ControllerCommand) -> LogEntry {
        LogEntry::new(
            Term::new(term),
            LogIndex::new(index),
            EntryKind::Application(
                crate::encode_command(&command, CommandLimits::default()).unwrap(),
            ),
        )
    }

    fn app_v2(term: u64, index: u64, command: crate::ProducerSessionCommandV2) -> LogEntry {
        LogEntry::new(
            Term::new(term),
            LogIndex::new(index),
            EntryKind::Application(crate::encode_producer_session_command_v2(command).unwrap()),
        )
    }

    fn barrier(term: u64, index: u64) -> LogEntry {
        LogEntry::new(
            Term::new(term),
            LogIndex::new(index),
            EntryKind::LeaderBarrier,
        )
    }

    fn open(dir: &Path) -> (ControllerPersistence, RecoveredState, ControllerState) {
        ControllerPersistence::open(
            dir,
            voters(),
            initial_state(),
            ControllerPersistenceConfig::default(),
        )
        .unwrap()
    }

    #[test]
    fn hard_state_log_commit_and_controller_state_recover_deterministically() {
        let dir = temp_dir("recover");
        let (mut persistence, _, _) = open(&dir);
        persistence
            .execute(
                &PersistenceBatch::new(
                    PersistenceToken::new(1),
                    vec![PersistenceOp::UpdateHardState(HardState {
                        current_term: Term::new(1),
                        voted_for: Some(node(1)),
                        durable_commit_index: LogIndex::ZERO,
                    })],
                ),
                None,
            )
            .unwrap();
        persistence
            .execute(
                &PersistenceBatch::new(
                    PersistenceToken::new(2),
                    vec![PersistenceOp::Append(vec![
                        barrier(1, 1),
                        app(1, 2, register(1)),
                    ])],
                ),
                None,
            )
            .unwrap();
        persistence
            .execute(
                &PersistenceBatch::new(
                    PersistenceToken::new(3),
                    vec![PersistenceOp::AdvanceCommit {
                        to: LogIndex::new(2),
                    }],
                ),
                None,
            )
            .unwrap();
        drop(persistence);

        let (_, recovered, state) = open(&dir);
        assert_eq!(recovered.hard_state.current_term, Term::new(1));
        assert_eq!(recovered.hard_state.durable_commit_index, LogIndex::new(2));
        assert_eq!(state.metadata_epoch(), 2);
        assert_eq!(state.brokers()[&1].lifecycle, BrokerLifecycle::Joining);
        fs::remove_dir_all(dir).unwrap();
    }

    #[test]
    fn p7_mixed_v1_v2_committed_log_replays_sessions_without_reusing_allocator_ids() {
        let dir = temp_dir("p7-mixed-replay");
        let initial =
            ControllerState::new([0x51; 16], crate::P7_PRODUCER_FEATURE_LEVEL, 1_000).unwrap();
        let (mut persistence, _, _) = ControllerPersistence::open(
            &dir,
            voters(),
            initial.clone(),
            ControllerPersistenceConfig::default(),
        )
        .unwrap();
        persistence
            .execute(
                &PersistenceBatch::new(
                    PersistenceToken::new(1),
                    vec![PersistenceOp::UpdateHardState(HardState {
                        current_term: Term::new(1),
                        voted_for: Some(node(1)),
                        durable_commit_index: LogIndex::ZERO,
                    })],
                ),
                None,
            )
            .unwrap();
        persistence
            .execute(
                &PersistenceBatch::new(
                    PersistenceToken::new(2),
                    vec![PersistenceOp::Append(vec![
                        barrier(1, 1),
                        app(
                            1,
                            2,
                            ControllerCommand::AllocateProducerIds {
                                allocation_count: 1,
                            },
                        ),
                        app_v2(
                            1,
                            3,
                            crate::ProducerSessionCommandV2::InitializeSession {
                                producer_id: astralog_common::ProducerId::new(1_001).unwrap(),
                                producer_epoch: astralog_common::ProducerEpoch::new(1).unwrap(),
                            },
                        ),
                    ])],
                ),
                None,
            )
            .unwrap();
        persistence
            .execute(
                &PersistenceBatch::new(
                    PersistenceToken::new(3),
                    vec![PersistenceOp::AdvanceCommit {
                        to: LogIndex::new(3),
                    }],
                ),
                None,
            )
            .unwrap();
        drop(persistence);

        let (_, _, recovered) = ControllerPersistence::open(
            &dir,
            voters(),
            initial,
            ControllerPersistenceConfig::default(),
        )
        .unwrap();
        assert_eq!(recovered.metadata_epoch(), 3);
        assert_eq!(recovered.next_unallocated_producer_id(), 1_002);
        assert_eq!(
            recovered.producer_authority(astralog_common::ProducerId::new(1_001).unwrap()),
            Some(astralog_common::ProducerAuthority::new(
                astralog_common::ProducerId::new(1_001).unwrap(),
                astralog_common::ProducerEpoch::new(1).unwrap(),
            ))
        );
        fs::remove_dir_all(dir).unwrap();
    }

    #[test]
    fn p7_v2_snapshot_restart_restores_sessions_allocator_and_metadata_epoch() {
        let dir = temp_dir("p7-v2-snapshot-restart");
        let initial =
            ControllerState::new([0x51; 16], crate::P7_PRODUCER_FEATURE_LEVEL, 1_000).unwrap();
        let (mut persistence, _, _) = ControllerPersistence::open(
            &dir,
            voters(),
            initial.clone(),
            ControllerPersistenceConfig::default(),
        )
        .unwrap();
        let producer_id = astralog_common::ProducerId::new(1_000).unwrap();
        let producer_epoch = astralog_common::ProducerEpoch::new(1).unwrap();
        persistence
            .execute(
                &PersistenceBatch::new(
                    PersistenceToken::new(1),
                    vec![
                        PersistenceOp::UpdateHardState(HardState {
                            current_term: Term::new(1),
                            voted_for: Some(node(1)),
                            durable_commit_index: LogIndex::ZERO,
                        }),
                        PersistenceOp::Append(vec![
                            barrier(1, 1),
                            app_v2(
                                1,
                                2,
                                crate::ProducerSessionCommandV2::InitializeSession {
                                    producer_id,
                                    producer_epoch,
                                },
                            ),
                        ]),
                        PersistenceOp::AdvanceCommit {
                            to: LogIndex::new(2),
                        },
                    ],
                ),
                None,
            )
            .unwrap();
        let state = persistence.recover_controller_state().unwrap();
        assert_eq!(state.metadata_epoch(), 2);
        assert_eq!(state.next_unallocated_producer_id(), 1_001);

        let metadata = SnapshotMetadata::new(
            LogIndex::new(2),
            Term::new(1),
            voters(),
            SnapshotId::new([0x72; 16]),
        )
        .unwrap();
        let package = persistence
            .build_snapshot_package(&state, &metadata)
            .unwrap();
        persistence
            .execute(
                &PersistenceBatch::new(
                    PersistenceToken::new(2),
                    vec![
                        PersistenceOp::InstallSnapshot(metadata),
                        PersistenceOp::DiscardPrefix {
                            through: LogIndex::new(2),
                        },
                    ],
                ),
                Some(&package),
            )
            .unwrap();
        assert!(persistence.log().is_empty());
        drop(persistence);

        let (_, recovered_raft, recovered_state) = ControllerPersistence::open(
            &dir,
            voters(),
            initial,
            ControllerPersistenceConfig::default(),
        )
        .unwrap();
        assert_eq!(
            recovered_raft.snapshot.last_included_index,
            LogIndex::new(2)
        );
        assert_eq!(recovered_state.metadata_epoch(), 2);
        assert_eq!(recovered_state.next_unallocated_producer_id(), 1_001);
        assert_eq!(
            recovered_state.producer_authority(producer_id),
            Some(astralog_common::ProducerAuthority::new(
                producer_id,
                producer_epoch,
            ))
        );
        fs::remove_dir_all(dir).unwrap();
    }

    #[test]
    fn commit_intent_repairs_cross_file_crash_boundary() {
        let dir = temp_dir("commit-intent");
        let (mut persistence, _, _) = open(&dir);
        persistence
            .execute(
                &PersistenceBatch::new(
                    PersistenceToken::new(1),
                    vec![
                        PersistenceOp::UpdateHardState(HardState {
                            current_term: Term::new(1),
                            voted_for: Some(node(1)),
                            durable_commit_index: LogIndex::ZERO,
                        }),
                        PersistenceOp::Append(vec![barrier(1, 1)]),
                    ],
                ),
                None,
            )
            .unwrap();
        let intent = CommitIntent {
            previous: LogIndex::ZERO,
            target: LogIndex::new(1),
        };
        atomic_write(
            &persistence.commit_intent_path(),
            &encode_commit_intent(intent),
        )
        .unwrap();
        atomic_write(
            &persistence.commit_path(),
            &encode_commit_marker(LogIndex::new(1)),
        )
        .unwrap();
        drop(persistence);

        let (_, recovered, _) = open(&dir);
        assert_eq!(recovered.hard_state.durable_commit_index, LogIndex::new(1));
        assert!(!dir.join("controller.raft.commit-intent.v1").exists());
        fs::remove_dir_all(dir).unwrap();
    }

    #[test]
    fn unexplained_commit_mismatch_fails_loudly() {
        let dir = temp_dir("mismatch");
        let (persistence, _, _) = open(&dir);
        atomic_write(
            &persistence.commit_path(),
            &encode_commit_marker(LogIndex::new(1)),
        )
        .unwrap();
        drop(persistence);
        assert!(
            ControllerPersistence::open(
                &dir,
                voters(),
                initial_state(),
                ControllerPersistenceConfig::default()
            )
            .is_err()
        );
        fs::remove_dir_all(dir).unwrap();
    }

    #[test]
    fn local_snapshot_build_compacts_prefix_and_replays_suffix() {
        let dir = temp_dir("local-snapshot");
        let (mut persistence, _, mut state) = open(&dir);
        let hard = HardState {
            current_term: Term::new(2),
            voted_for: Some(node(1)),
            durable_commit_index: LogIndex::ZERO,
        };
        persistence
            .execute(
                &PersistenceBatch::new(
                    PersistenceToken::new(1),
                    vec![
                        PersistenceOp::UpdateHardState(hard),
                        PersistenceOp::Append(vec![
                            barrier(1, 1),
                            app(1, 2, register(1)),
                            barrier(2, 3),
                            app(2, 4, register(2)),
                            barrier(2, 5),
                        ]),
                        PersistenceOp::AdvanceCommit {
                            to: LogIndex::new(5),
                        },
                    ],
                ),
                None,
            )
            .unwrap();
        state.apply(2, &register(1)).unwrap();
        let metadata = SnapshotMetadata::new(
            LogIndex::new(3),
            Term::new(2),
            voters(),
            SnapshotId::new([3; 16]),
        )
        .unwrap();
        let package = persistence
            .build_snapshot_package(&state, &metadata)
            .unwrap();
        persistence
            .execute(
                &PersistenceBatch::new(
                    PersistenceToken::new(2),
                    vec![
                        PersistenceOp::InstallSnapshot(metadata),
                        PersistenceOp::DiscardPrefix {
                            through: LogIndex::new(3),
                        },
                    ],
                ),
                Some(&package),
            )
            .unwrap();
        assert_eq!(persistence.log()[0].index, LogIndex::new(4));
        drop(persistence);

        let (_, recovered, recovered_state) = open(&dir);
        assert_eq!(recovered.snapshot.last_included_index, LogIndex::new(3));
        assert_eq!(recovered.log.len(), 2);
        assert_eq!(recovered_state.metadata_epoch(), 4);
        assert!(recovered_state.brokers().contains_key(&2));
        fs::remove_dir_all(dir).unwrap();
    }

    #[test]
    fn follower_install_snapshot_rebootstraps_and_continues_at_snapshot_plus_one() {
        let leader_dir = temp_dir("leader-package");
        let (mut leader, _, mut leader_state) = open(&leader_dir);
        leader
            .execute(
                &PersistenceBatch::new(
                    PersistenceToken::new(1),
                    vec![
                        PersistenceOp::UpdateHardState(HardState {
                            current_term: Term::new(2),
                            voted_for: Some(node(1)),
                            durable_commit_index: LogIndex::ZERO,
                        }),
                        PersistenceOp::Append(vec![
                            barrier(1, 1),
                            app(1, 2, register(1)),
                            barrier(2, 3),
                            app(2, 4, register(2)),
                            barrier(2, 5),
                        ]),
                        PersistenceOp::AdvanceCommit {
                            to: LogIndex::new(5),
                        },
                    ],
                ),
                None,
            )
            .unwrap();
        leader_state.apply(2, &register(1)).unwrap();
        leader_state.apply(4, &register(2)).unwrap();
        let metadata = SnapshotMetadata::new(
            LogIndex::new(5),
            Term::new(2),
            voters(),
            SnapshotId::new([5; 16]),
        )
        .unwrap();
        let package = leader
            .build_snapshot_package(&leader_state, &metadata)
            .unwrap();

        let follower_dir = temp_dir("follower-install");
        let (mut follower, _, _) = open(&follower_dir);
        follower
            .execute(
                &PersistenceBatch::new(
                    PersistenceToken::new(2),
                    vec![
                        PersistenceOp::UpdateHardState(HardState {
                            current_term: Term::new(2),
                            voted_for: None,
                            durable_commit_index: LogIndex::ZERO,
                        }),
                        PersistenceOp::InstallSnapshot(metadata.clone()),
                        PersistenceOp::DiscardPrefix {
                            through: LogIndex::new(5),
                        },
                        PersistenceOp::AdvanceCommit {
                            to: LogIndex::new(5),
                        },
                    ],
                ),
                Some(&package),
            )
            .unwrap();
        follower
            .execute(
                &PersistenceBatch::new(
                    PersistenceToken::new(3),
                    vec![PersistenceOp::Append(vec![app(2, 6, register(3))])],
                ),
                None,
            )
            .unwrap();
        assert_eq!(follower.log()[0].index, LogIndex::new(6));
        fs::remove_dir_all(leader_dir).unwrap();
        fs::remove_dir_all(follower_dir).unwrap();
    }

    #[test]
    fn snapshot_intent_completes_published_snapshot_after_crash() {
        let leader_dir = temp_dir("snap-intent-leader");
        let (mut leader, _, mut state) = open(&leader_dir);
        leader
            .execute(
                &PersistenceBatch::new(
                    PersistenceToken::new(1),
                    vec![
                        PersistenceOp::UpdateHardState(HardState {
                            current_term: Term::new(1),
                            voted_for: Some(node(1)),
                            durable_commit_index: LogIndex::ZERO,
                        }),
                        PersistenceOp::Append(vec![app(1, 1, register(1))]),
                        PersistenceOp::AdvanceCommit {
                            to: LogIndex::new(1),
                        },
                    ],
                ),
                None,
            )
            .unwrap();
        state.apply(1, &register(1)).unwrap();
        let metadata = SnapshotMetadata::new(
            LogIndex::new(1),
            Term::new(1),
            voters(),
            SnapshotId::new([9; 16]),
        )
        .unwrap();
        let package = leader.build_snapshot_package(&state, &metadata).unwrap();

        let follower_dir = temp_dir("snap-intent-follower");
        let (mut follower, _, _) = open(&follower_dir);
        follower
            .execute(
                &PersistenceBatch::new(
                    PersistenceToken::new(7),
                    vec![PersistenceOp::UpdateHardState(HardState {
                        current_term: Term::new(1),
                        voted_for: None,
                        durable_commit_index: LogIndex::ZERO,
                    })],
                ),
                None,
            )
            .unwrap();
        let intent = SnapshotIntent {
            previous_commit: LogIndex::ZERO,
            target_commit: LogIndex::new(1),
            snapshot_index: LogIndex::new(1),
            snapshot_id: metadata.snapshot_id,
            retain_suffix: false,
        };
        atomic_write(
            &follower.snapshot_intent_path(),
            &encode_snapshot_intent(intent),
        )
        .unwrap();
        atomic_write(
            &follower.snapshot_path(),
            &encode_snapshot_bundle(&package, SnapshotLimits::default()).unwrap(),
        )
        .unwrap();
        drop(follower);

        let (_, recovered, recovered_state) = open(&follower_dir);
        assert_eq!(recovered.snapshot.last_included_index, LogIndex::new(1));
        assert_eq!(recovered.hard_state.durable_commit_index, LogIndex::new(1));
        assert_eq!(recovered_state.metadata_epoch(), 1);
        fs::remove_dir_all(leader_dir).unwrap();
        fs::remove_dir_all(follower_dir).unwrap();
    }

    #[test]
    fn snapshot_intent_before_publication_rolls_back_without_advancing_commit() {
        let dir = temp_dir("snap-intent-prepublish");
        let (persistence, _, _) = open(&dir);
        let intent = SnapshotIntent {
            previous_commit: LogIndex::ZERO,
            target_commit: LogIndex::new(3),
            snapshot_index: LogIndex::new(3),
            snapshot_id: SnapshotId::new([0x31; 16]),
            retain_suffix: false,
        };
        atomic_write(
            &persistence.snapshot_intent_path(),
            &encode_snapshot_intent(intent),
        )
        .unwrap();
        drop(persistence);

        let (_, recovered, state) = open(&dir);
        assert_eq!(recovered.snapshot.last_included_index, LogIndex::ZERO);
        assert_eq!(recovered.hard_state.durable_commit_index, LogIndex::ZERO);
        assert_eq!(state.metadata_epoch(), 0);
        assert!(!dir.join("controller.snapshot-intent.v1").exists());
        fs::remove_dir_all(dir).unwrap();
    }

    #[test]
    fn follower_snapshot_install_discards_conflicting_uncommitted_suffix() {
        let leader_dir = temp_dir("conflict-leader");
        let (mut leader, _, mut leader_state) = open(&leader_dir);
        leader
            .execute(
                &PersistenceBatch::new(
                    PersistenceToken::new(1),
                    vec![
                        PersistenceOp::UpdateHardState(HardState {
                            current_term: Term::new(2),
                            voted_for: Some(node(1)),
                            durable_commit_index: LogIndex::ZERO,
                        }),
                        PersistenceOp::Append(vec![
                            app(1, 1, register(1)),
                            barrier(2, 2),
                            app(2, 3, register(2)),
                        ]),
                        PersistenceOp::AdvanceCommit {
                            to: LogIndex::new(3),
                        },
                    ],
                ),
                None,
            )
            .unwrap();
        leader_state.apply(1, &register(1)).unwrap();
        leader_state.apply(3, &register(2)).unwrap();
        let metadata = SnapshotMetadata::new(
            LogIndex::new(3),
            Term::new(2),
            voters(),
            SnapshotId::new([0x32; 16]),
        )
        .unwrap();
        let package = leader
            .build_snapshot_package(&leader_state, &metadata)
            .unwrap();

        let follower_dir = temp_dir("conflict-follower");
        let (mut follower, _, _) = open(&follower_dir);
        follower
            .execute(
                &PersistenceBatch::new(
                    PersistenceToken::new(2),
                    vec![
                        PersistenceOp::UpdateHardState(HardState {
                            current_term: Term::new(2),
                            voted_for: None,
                            durable_commit_index: LogIndex::ZERO,
                        }),
                        PersistenceOp::Append(vec![
                            barrier(1, 1),
                            barrier(1, 2),
                            barrier(1, 3),
                            barrier(1, 4),
                        ]),
                    ],
                ),
                None,
            )
            .unwrap();
        follower
            .execute(
                &PersistenceBatch::new(
                    PersistenceToken::new(3),
                    vec![
                        PersistenceOp::InstallSnapshot(metadata.clone()),
                        PersistenceOp::TruncateSuffix {
                            from: LogIndex::new(4),
                        },
                        PersistenceOp::DiscardPrefix {
                            through: LogIndex::new(3),
                        },
                        PersistenceOp::AdvanceCommit {
                            to: LogIndex::new(3),
                        },
                    ],
                ),
                Some(&package),
            )
            .unwrap();
        assert!(follower.log().is_empty());
        assert_eq!(follower.snapshot_package().metadata, metadata);
        fs::remove_dir_all(leader_dir).unwrap();
        fs::remove_dir_all(follower_dir).unwrap();
    }

    #[test]
    fn committed_suffix_truncation_is_rejected() {
        let dir = temp_dir("truncate-commit");
        let (mut persistence, _, _) = open(&dir);
        persistence
            .execute(
                &PersistenceBatch::new(
                    PersistenceToken::new(1),
                    vec![
                        PersistenceOp::UpdateHardState(HardState {
                            current_term: Term::new(1),
                            voted_for: Some(node(1)),
                            durable_commit_index: LogIndex::ZERO,
                        }),
                        PersistenceOp::Append(vec![barrier(1, 1), barrier(1, 2)]),
                        PersistenceOp::AdvanceCommit {
                            to: LogIndex::new(1),
                        },
                    ],
                ),
                None,
            )
            .unwrap();
        assert!(
            persistence
                .execute(
                    &PersistenceBatch::new(
                        PersistenceToken::new(2),
                        vec![PersistenceOp::TruncateSuffix {
                            from: LogIndex::new(1),
                        }],
                    ),
                    None,
                )
                .is_err()
        );
        fs::remove_dir_all(dir).unwrap();
    }

    #[test]
    fn foreign_cluster_or_controller_configuration_fails_recovery() {
        let dir = temp_dir("foreign-recovery");
        let (persistence, _, _) = open(&dir);
        drop(persistence);

        let foreign_state = ControllerState::new([0x99; 16], 1, 1_000).unwrap();
        assert!(
            ControllerPersistence::open(
                &dir,
                voters(),
                foreign_state,
                ControllerPersistenceConfig::default(),
            )
            .is_err()
        );

        let other_voters = StableConfiguration::new([node(4), node(5), node(6)], []).unwrap();
        assert!(
            ControllerPersistence::open(
                &dir,
                other_voters,
                initial_state(),
                ControllerPersistenceConfig::default(),
            )
            .is_err()
        );
        fs::remove_dir_all(dir).unwrap();
    }

    #[test]
    fn corruption_in_hard_log_or_snapshot_is_loud() {
        for file in [
            "controller.raft.hardstate.v1",
            "controller.raft.log.v1",
            "controller.snapshot.v1",
        ] {
            let dir = temp_dir("corrupt");
            let (persistence, _, _) = open(&dir);
            drop(persistence);
            let path = dir.join(file);
            let mut bytes = fs::read(&path).unwrap();
            let index = bytes.len() / 2;
            bytes[index] ^= 0x80;
            fs::write(&path, bytes).unwrap();
            assert!(
                ControllerPersistence::open(
                    &dir,
                    voters(),
                    initial_state(),
                    ControllerPersistenceConfig::default()
                )
                .is_err()
            );
            fs::remove_dir_all(dir).unwrap();
        }
    }

    #[test]
    fn compaction_never_crosses_snapshot_boundary_or_loses_suffix() {
        let dir = temp_dir("compact-guard");
        let (mut persistence, _, _) = open(&dir);
        assert!(
            persistence
                .execute(
                    &PersistenceBatch::new(
                        PersistenceToken::new(1),
                        vec![PersistenceOp::DiscardPrefix {
                            through: LogIndex::new(1),
                        }],
                    ),
                    None,
                )
                .is_err()
        );
        fs::remove_dir_all(dir).unwrap();
    }
}
