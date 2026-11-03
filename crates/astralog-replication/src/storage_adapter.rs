use std::{
    collections::BTreeMap,
    fs::{self, OpenOptions},
    io::Write,
    path::{Path, PathBuf},
    sync::RwLock,
};

#[cfg(unix)]
use std::fs::File;

use astralog_common::{
    PartitionId, ProducerAuthority, ProducerEpoch, ProducerId, ProducerSequence, RaftIndex,
    RaftTerm, StreamOffset, TopicId,
};
use astralog_raft::{
    EntryKind, HardState, LogEntry, LogIndex, PersistenceBatch, PersistenceOp, RecoveredState,
    SnapshotId, SnapshotMetadata, StableConfiguration, Term,
};
use astralog_storage::{
    CompactionLimits, CompactionOutcome, CompactionPlan, CompactionPlanSpec, Compression,
    ControlEntry, DeleteRetentionPlan, DeleteRetentionSpec, Frame, LocalCleanProgressStore,
    P9_CONTROL_ADVANCE_COMPACTION_SAFE_OFFSET, P9_CONTROL_ADVANCE_LOG_START_OFFSET,
    P9_CONTROL_PRODUCER_STATE_CHECKPOINT, P9_MAINTENANCE_CONTROL_VERSION, PartitionLog,
    PartitionLogConfig, PartitionMeta, ProducerCheckpointFence, ProducerIdentity, RecordBatch,
    TombstoneRemovalGate, decode_compaction_safe_offset_body, decode_log_start_offset_body,
    encode_compaction_safe_offset_body, encode_log_start_offset_body, encode_manifest,
    encode_partition_meta, load_manifest, validate_partition_manifest_files,
};

use crate::{
    ApplicationCommand, ApplicationCommandV2, ApplicationCompactionSafeCommandV1,
    ApplicationMaintenanceCommandV1, ApplicationProducerStateCheckpointCommandV1, CommandLimits,
    CommittedBatchV1, PartitionProducerStateV1, ProducerSnapshotStore, ProducerSnapshotV1,
    RebootstrapFileV2, RebootstrapLimits, RebootstrapLimitsV2, RebootstrapPackage,
    RebootstrapPackageV2, ReplicationError, Result,
    codec::{decode_configuration_change, encode_configuration_change},
    command::{
        decode_application_command, decode_application_command_v2,
        decode_application_compaction_safe_command_v1, decode_application_maintenance_command_v1,
        decode_application_producer_state_checkpoint_command_v1, encode_application_command,
        encode_application_command_v2, encode_application_compaction_safe_command_v1,
        encode_application_maintenance_command_v1,
        encode_application_producer_state_checkpoint_command_v1,
    },
    decode_producer_snapshot, encode_producer_snapshot,
    producer_digest::canonical_storage_record_set_digest,
    sidecar::{CommitIntent, CommitIntentStore, HardStateStore, SnapshotStore, atomic_write},
};

pub const P4_CONTROL_LEADER_BARRIER: u16 = 0x0101;
pub const P4_CONTROL_CONFIGURATION: u16 = 0x0102;
const P4_CONTROL_VERSION: u16 = 1;
const PRODUCER_RECOVERY_PAGE_FRAMES: usize = 4;

pub struct PartitionPersistence {
    directory: PathBuf,
    topic_id: TopicId,
    partition_id: PartitionId,
    storage_config: PartitionLogConfig,
    initial_configuration: StableConfiguration,
    log: Option<PartitionLog>,
    hard_state: HardState,
    snapshot: SnapshotMetadata,
    hard_store: HardStateStore,
    snapshot_store: SnapshotStore,
    producer_snapshot_store: ProducerSnapshotStore,
    commit_intent_store: CommitIntentStore,
    command_limits: CommandLimits,
    data_compression: Compression,
    #[cfg(test)]
    fail_next_execute: Option<&'static str>,
    #[cfg(test)]
    fail_next_group_sync: Option<&'static str>,
}

impl PartitionPersistence {
    pub fn open(
        directory: &Path,
        topic_id: TopicId,
        partition_id: PartitionId,
        now_ms: i64,
        storage_config: PartitionLogConfig,
        initial_configuration: StableConfiguration,
        command_limits: CommandLimits,
    ) -> Result<(Self, RecoveredState)> {
        recover_interrupted_rebootstrap(directory)?;
        let opened = Self::open_inner(
            directory,
            topic_id,
            partition_id,
            now_ms,
            storage_config,
            initial_configuration.clone(),
            command_limits,
        );
        match opened {
            Ok(result) => {
                finish_rebootstrap_recovery(directory)?;
                Ok(result)
            }
            Err(_error) if rebootstrap_backup_path(directory).exists() => {
                restore_rebootstrap_backup(directory)?;
                Self::open_inner(
                    directory,
                    topic_id,
                    partition_id,
                    now_ms,
                    storage_config,
                    initial_configuration,
                    command_limits,
                )
            }
            Err(error) => Err(error),
        }
    }

    fn open_inner(
        directory: &Path,
        topic_id: TopicId,
        partition_id: PartitionId,
        now_ms: i64,
        storage_config: PartitionLogConfig,
        initial_configuration: StableConfiguration,
        command_limits: CommandLimits,
    ) -> Result<(Self, RecoveredState)> {
        let mut log = PartitionLog::open_or_create(
            directory,
            topic_id,
            partition_id,
            now_ms,
            storage_config,
        )?;
        // P1 intentionally forgets whether an uncommitted tail had been fsynced
        // before restart. P4, however, reconstructs every valid recovered frame
        // into Raft's stable log. Re-fsync the recovered active tail before that
        // reconstruction so any surviving-but-uncommitted entries become
        // durably established in this process before Raft may advance commit to
        // them without re-appending the same bytes.
        log.flush_data()?;
        let hard_store = HardStateStore::new(directory);
        let snapshot_store = SnapshotStore::new(directory);
        let producer_snapshot_store = ProducerSnapshotStore::new(directory);
        let commit_intent_store = CommitIntentStore::new(directory);

        let mut adapter = Self {
            directory: directory.to_path_buf(),
            topic_id,
            partition_id,
            storage_config,
            initial_configuration: initial_configuration.clone(),
            log: Some(log),
            hard_state: HardState::initial(),
            snapshot: SnapshotMetadata::new(
                LogIndex::ZERO,
                Term::ZERO,
                initial_configuration,
                SnapshotId::ZERO,
            )?,
            hard_store,
            snapshot_store,
            producer_snapshot_store,
            commit_intent_store,
            command_limits,
            data_compression: Compression::None,
            #[cfg(test)]
            fail_next_execute: None,
            #[cfg(test)]
            fail_next_group_sync: None,
        };

        adapter.recover_commit_intent()?;
        adapter.load_or_initialize_sidecars()?;
        let recovered = adapter.recovered_state()?;
        Ok((adapter, recovered))
    }

    pub fn execute(&mut self, batch: &PersistenceBatch, now_ms: i64) -> Result<()> {
        self.execute_with_rebootstrap(batch, now_ms, None)
    }

    pub fn stage_append_batch(&mut self, batch: &PersistenceBatch, now_ms: i64) -> Result<()> {
        #[cfg(test)]
        if let Some(message) = self.fail_next_execute.take() {
            return Err(ReplicationError::InvalidOwned(format!(
                "injected persistence failure: {message}"
            )));
        }
        let [PersistenceOp::Append(entries)] = batch.operations.as_slice() else {
            return Err(ReplicationError::Invalid(
                "group-commit staging accepts append-only persistence batches",
            ));
        };
        if entries.is_empty() {
            return Err(ReplicationError::Invalid(
                "group-commit staging requires a non-empty append",
            ));
        }
        self.append_entries_unflushed(entries, now_ms)
    }

    pub fn flush_staged_appends(&mut self) -> Result<()> {
        #[cfg(test)]
        if let Some(message) = self.fail_next_group_sync.take() {
            return Err(ReplicationError::InvalidOwned(format!(
                "injected group-sync failure: {message}"
            )));
        }
        self.log_mut().flush_data()?;
        Ok(())
    }

    pub fn execute_with_rebootstrap(
        &mut self,
        batch: &PersistenceBatch,
        now_ms: i64,
        rebootstrap: Option<&RebootstrapPackage>,
    ) -> Result<()> {
        self.execute_with_rebootstrap_versioned(batch, now_ms, rebootstrap, None)
    }

    /// Only a P3 InstallSnapshot persistence effect may consume either
    /// generation. The caller must bind the package to that exact P3 request;
    /// this function independently checks the equality before any file swap.
    pub fn execute_with_rebootstrap_versioned(
        &mut self,
        batch: &PersistenceBatch,
        now_ms: i64,
        rebootstrap: Option<&RebootstrapPackage>,
        rebootstrap_v2: Option<&RebootstrapPackageV2>,
    ) -> Result<()> {
        if rebootstrap.is_some() && rebootstrap_v2.is_some() {
            return Err(ReplicationError::Invalid(
                "P9-G cannot mix P4 V1 and P9 V2 rebootstrap generations",
            ));
        }
        #[cfg(test)]
        if let Some(message) = self.fail_next_execute.take() {
            return Err(ReplicationError::InvalidOwned(format!(
                "injected persistence failure: {message}"
            )));
        }
        for operation in &batch.operations {
            match operation {
                PersistenceOp::UpdateHardState(state) => self.update_hard_state(state)?,
                PersistenceOp::TruncateSuffix { from } => {
                    self.log_mut().truncate_raft_suffix(from.get())?;
                }
                PersistenceOp::Append(entries) => self.append_entries(entries, now_ms)?,
                PersistenceOp::AdvanceCommit { to } => self.advance_commit(*to)?,
                PersistenceOp::InstallSnapshot(metadata) => {
                    if let Some(package) = rebootstrap_v2 {
                        if package.snapshot != *metadata {
                            return Err(ReplicationError::Invalid(
                                "P9-G V2 package differs from authorized P3 InstallSnapshot",
                            ));
                        }
                        self.install_rebootstrap_package_v2(package, now_ms)?;
                    } else if let Some(package) = rebootstrap {
                        if package.snapshot != *metadata {
                            return Err(ReplicationError::Invalid(
                                "rebootstrap package snapshot does not match P3 InstallSnapshot",
                            ));
                        }
                        self.install_rebootstrap_package(package, now_ms)?;
                    } else {
                        self.install_snapshot(metadata)?;
                    }
                }
                PersistenceOp::DiscardPrefix { through } => {
                    if *through > self.snapshot.last_included_index {
                        return Err(ReplicationError::Invalid(
                            "DiscardPrefix exceeds persisted snapshot boundary",
                        ));
                    }
                    // P4 deliberately retains P1 prefix bytes. P9 owns physical
                    // retention/compaction. Keeping the prefix is safety-neutral.
                }
            }
        }
        Ok(())
    }

    /// Staging, validation and publication reuse the already-qualified P4
    /// crash-intent/backup directory protocol. The V2 source inventory is
    /// validated independently because compacted retained history can lack
    /// the old 1..snapshot contiguous prefix.
    fn install_rebootstrap_package_v2(
        &mut self,
        package: &RebootstrapPackageV2,
        now_ms: i64,
    ) -> Result<()> {
        let limits = RebootstrapLimitsV2::default();
        let encoded = crate::encode_rebootstrap_package_v2(package, limits)?;
        if crate::decode_rebootstrap_package_v2(&encoded, limits)? != *package {
            return Err(ReplicationError::Invalid(
                "P9-G V2 source package fails canonical byte validation",
            ));
        }
        if package.topic_id != self.topic_id
            || package.partition_id != self.partition_id
            || package.snapshot.last_included_index <= self.hard_state.durable_commit_index
        {
            return Err(ReplicationError::Invalid(
                "P9-G V2 package targets wrong partition or obsolete committed snapshot",
            ));
        }
        let staging = rebootstrap_staging_path(&self.directory);
        let backup = rebootstrap_backup_path(&self.directory);
        let intent = rebootstrap_intent_path(&self.directory);
        if backup.exists() || intent.exists() {
            return Err(ReplicationError::Invalid(
                "P9-G another rebootstrap publication already in progress",
            ));
        }
        if staging.exists() {
            fs::remove_dir_all(&staging)?;
        }
        fs::create_dir_all(&staging)?;
        for file in &package.files {
            let mut writer = OpenOptions::new()
                .create_new(true)
                .write(true)
                .open(staging.join(&file.file_name))?;
            writer.write_all(&file.bytes)?;
            writer.sync_all()?;
        }
        atomic_write(
            &staging.join("segments.manifest"),
            &encode_manifest(&package.manifest)?,
        )?;
        // V2 immutable history is already committed. Storage must see the
        // exact committed boundary *before* recovery scans the sparse files;
        // an implicit freshly created commit=0 meta would reject every V2
        // retained control as apparently uncommitted.
        atomic_write(
            &staging.join("partition.meta"),
            &encode_partition_meta(&PartitionMeta {
                topic_id: self.topic_id,
                partition_id: self.partition_id,
                committed_raft_index: package.snapshot.last_included_index.get(),
                metadata_epoch: 1,
            }),
        )?;
        let mut staged_log = PartitionLog::open_or_create(
            &staging,
            self.topic_id,
            self.partition_id,
            now_ms,
            self.storage_config,
        )?;
        if staged_log.manifest_generation() != Some(package.manifest.generation)
            || staged_log.next_raft_index().get()
                != package.snapshot.last_included_index.get().saturating_add(1)
            || staged_log.next_stream_offset() != package.next_stream_offset
        {
            return Err(ReplicationError::Invalid(
                "P9-G V2 staged Raft/stream boundaries do not match P3 snapshot",
            ));
        }
        staged_log.flush_data()?;
        staged_log.mark_committed(package.snapshot.last_included_index.get())?;
        if staged_log.committed_log_start_offset()? != package.committed_log_start_offset
            || staged_log.committed_compaction_safe_offset()?
                != package.committed_compaction_safe_offset
        {
            return Err(ReplicationError::Invalid(
                "P9-G V2 staged retained maintenance authority differs from package",
            ));
        }
        staged_log.validate_and_publish_rebootstrap_clean_progress(
            package.snapshot.last_included_index.get(),
            CompactionLimits::default(),
        )?;
        drop(staged_log);
        let mut staged_hard = self.hard_state.clone();
        staged_hard.durable_commit_index = package.snapshot.last_included_index;
        HardStateStore::new(&staging).persist(&staged_hard)?;
        SnapshotStore::new(&staging).persist(&package.snapshot)?;
        if let Some(checkpoint) = &package.producer_checkpoint {
            ProducerSnapshotStore::new(&staging).persist(checkpoint)?;
        }

        // Reopening the fully staged generation exercises exact manifest,
        // Storage V1/V2 indexes/CRCs, P3 sidecars, and the authoritative
        // committed Raft state *before* the old generation is renamed.
        let (validated, recovered) = Self::open_inner(
            &staging,
            self.topic_id,
            self.partition_id,
            now_ms,
            self.storage_config,
            self.initial_configuration.clone(),
            self.command_limits,
        )?;
        if recovered.hard_state.durable_commit_index != package.snapshot.last_included_index
            || recovered.snapshot != package.snapshot
            || validated.log().committed_compaction_safe_offset()?
                != package.committed_compaction_safe_offset
        {
            return Err(ReplicationError::Invalid(
                "P9-G V2 staged reopened authority does not match package",
            ));
        }
        // P7 state cannot be inferred from holes in compacted history. P9-F
        // requires any omitted producer-bearing source to have an earlier
        // committed producer checkpoint. If such a checkpoint exists,
        // independently recover the canonical state and check any optional
        // acceleration snapshot. Without one, a sparse package may contain
        // only non-idempotent history: no P7 replay is required or possible.
        let control = validated
            .log()
            .latest_committed_producer_checkpoint_control()?;
        let producer_frames = validated.log().read_frames()?.iter().any(|frame| {
            matches!(frame, Frame::Data(batch)
                if batch.producer.is_some()
                   && batch.raft_index.get() <= package.snapshot.last_included_index.get())
        });
        if control.is_none() && (producer_frames || package.producer_checkpoint.is_some()) {
            return Err(ReplicationError::Invalid(
                "P9-G V2 cannot omit producer recovery without a committed P9-F checkpoint",
            ));
        }
        if control.is_some() {
            let restored_producers = validated.recover_committed_producer_states()?;
            if let Some(checkpoint) = &package.producer_checkpoint
                && checkpoint.states != restored_producers
            {
                return Err(ReplicationError::Invalid(
                    "P9-G V2 producer snapshot differs from committed retained recovery",
                ));
            }
        }
        drop(validated);

        // The intent is the linearization bridge for interrupted directory
        // swaps. Restart either restores old authority or validates the new
        // generation before retiring the backup.
        atomic_write(&intent, b"ASTRSWP1")?;
        #[cfg(test)]
        p9g_v2_pause_for_external_process_kill("intent_published");
        let parent = self
            .directory
            .parent()
            .ok_or(ReplicationError::Invalid(
                "P9-G V2 partition directory has no swap parent",
            ))?
            .to_path_buf();
        drop(self.log.take());
        fs::rename(&self.directory, &backup)?;
        sync_directory_path(&parent)?;
        #[cfg(test)]
        p9g_v2_pause_for_external_process_kill("old_moved_to_backup");
        fs::rename(&staging, &self.directory)?;
        sync_directory_path(&parent)?;
        #[cfg(test)]
        p9g_v2_pause_for_external_process_kill("new_generation_published");

        let selected_compression = self.data_compression;
        let (mut replacement, _) = Self::open_inner(
            &self.directory,
            self.topic_id,
            self.partition_id,
            now_ms,
            self.storage_config,
            self.initial_configuration.clone(),
            self.command_limits,
        )?;
        replacement.data_compression = selected_compression;
        *self = replacement;
        fs::remove_dir_all(&backup)?;
        remove_file_if_exists(&intent)?;
        sync_directory_path(&parent)?;
        Ok(())
    }

    pub fn recovered_state(&self) -> Result<RecoveredState> {
        if self.hard_state.durable_commit_index.get() != self.log().committed_raft_index() {
            return Err(ReplicationError::Invalid(
                "hard-state commit does not match P1 partition committed index",
            ));
        }
        let mut entries = Vec::new();
        for frame in self.log().read_frames()? {
            let entry = frame_to_log_entry(frame, self.command_limits)?;
            if entry.index > self.snapshot.last_included_index {
                entries.push(entry);
            }
        }
        Ok(RecoveredState::from_persisted(
            self.hard_state.clone(),
            self.snapshot.clone(),
            entries,
        )?)
    }

    pub fn log(&self) -> &PartitionLog {
        self.log
            .as_ref()
            .expect("partition persistence always owns an open log outside atomic rebootstrap swap")
    }

    pub fn log_mut(&mut self) -> &mut PartitionLog {
        self.log
            .as_mut()
            .expect("partition persistence always owns an open log outside atomic rebootstrap swap")
    }

    pub fn set_data_compression(&mut self, compression: Compression) {
        self.data_compression = compression;
    }

    pub fn hard_state(&self) -> &HardState {
        &self.hard_state
    }

    pub fn snapshot(&self) -> &SnapshotMetadata {
        &self.snapshot
    }

    pub fn next_stream_offset(&self) -> StreamOffset {
        self.log().next_stream_offset()
    }

    pub fn physical_data_sync_count(&self) -> u64 {
        self.log().physical_sync_count()
    }

    pub fn committed_stream_high_watermark(&self) -> Result<StreamOffset> {
        Ok(self.log().committed_stream_high_watermark()?)
    }

    pub fn committed_log_start_offset(&self) -> Result<StreamOffset> {
        Ok(self.log().committed_log_start_offset()?)
    }

    pub fn committed_compaction_safe_offset(&self) -> Result<StreamOffset> {
        Ok(self.log().committed_compaction_safe_offset()?)
    }

    pub fn local_clean_compacted_offset(&self) -> StreamOffset {
        LocalCleanProgressStore::new(&self.directory, self.topic_id, self.partition_id)
            .load_conservative(self.log().manifest_generation())
    }

    /// A derived sidecar from a previous manifest is not authority. This
    /// bounded revalidation may refresh local evidence after a harmless roll;
    /// if the retained generation cannot prove the committed safe prefix,
    /// the caller must keep the replica non-electable until V2 rebootstrap.
    pub fn revalidate_clean_progress_at_persisted_snapshot(&self) -> Result<StreamOffset> {
        Ok(self.log().validate_and_publish_rebootstrap_clean_progress(
            self.snapshot.last_included_index.get(),
            CompactionLimits::default(),
        )?)
    }

    pub fn manifest_generation(&self) -> Option<u64> {
        self.log().manifest_generation()
    }

    /// A closed and durable P3 snapshot boundary is mandatory for exporting
    /// retained history. This deliberately fails closed when an active V1
    /// uncommitted or post-snapshot suffix exists; it never represents a
    /// partial generation as a complete snapshot package.
    pub fn build_rebootstrap_package_v2(
        &self,
        metadata: &SnapshotMetadata,
        limits: RebootstrapLimitsV2,
    ) -> Result<RebootstrapPackageV2> {
        if metadata != &self.snapshot
            || metadata.last_included_index == LogIndex::ZERO
            || metadata.last_included_index.get() != self.log().committed_raft_index()
            || self.log().next_raft_index().get()
                != metadata.last_included_index.get().saturating_add(1)
            || self.log().durable_raft_index() < metadata.last_included_index.get()
        {
            return Err(ReplicationError::Invalid(
                "P9-G V2 source requires exact durable committed P3 snapshot boundary",
            ));
        }
        let manifest = load_manifest(&self.directory)?.ok_or(ReplicationError::Invalid(
            "P9-G V2 source requires authoritative retained-history manifest",
        ))?;
        if Some(manifest.generation) != self.log().manifest_generation()
            || manifest.entries.is_empty()
            || manifest.entries.len() > limits.max_files
        {
            return Err(ReplicationError::Invalid(
                "P9-G V2 manifest generation/file count changed",
            ));
        }
        let paths = validate_partition_manifest_files(
            &self.directory,
            &manifest,
            self.topic_id,
            self.partition_id,
        )?;
        let mut files = Vec::with_capacity(paths.len());
        let mut total = 0_usize;
        for (path, entry) in paths.iter().zip(&manifest.entries) {
            let metadata = fs::metadata(path)?;
            if metadata.len() == 0 || metadata.len() > limits.max_file_bytes as u64 {
                return Err(ReplicationError::Invalid(
                    "P9-G V2 retained file exceeds per-file budget",
                ));
            }
            let bytes = fs::read(path)?;
            if bytes.len() as u64 != metadata.len() {
                return Err(ReplicationError::Invalid(
                    "P9-G V2 source file changed during package copy",
                ));
            }
            total = total
                .checked_add(bytes.len())
                .ok_or(ReplicationError::Invalid(
                    "P9-G V2 source total bytes overflow",
                ))?;
            if total > limits.max_package_bytes {
                return Err(ReplicationError::Invalid(
                    "P9-G V2 source exceeds total package budget",
                ));
            }
            files.push(RebootstrapFileV2 {
                file_name: entry.file_name.clone(),
                bytes,
            });
        }
        // Recheck the authoritative generation after copying. The actor
        // serializes local append/roll/cleaning, but this also guards a
        // filesystem substitution between the first and final reads.
        if load_manifest(&self.directory)?.as_ref() != Some(&manifest) {
            return Err(ReplicationError::Invalid(
                "P9-G V2 manifest changed during package export",
            ));
        }
        let package = RebootstrapPackageV2 {
            topic_id: self.topic_id,
            partition_id: self.partition_id,
            snapshot: metadata.clone(),
            manifest,
            files,
            committed_log_start_offset: self.log().committed_log_start_offset()?,
            committed_compaction_safe_offset: self.log().committed_compaction_safe_offset()?,
            next_stream_offset: self.log().next_stream_offset(),
            producer_checkpoint: None, // P9 committed control is the authority, not P7 sidecar
        };
        let encoded = crate::encode_rebootstrap_package_v2(&package, limits)?;
        // Require the same canonical layout and SHA-256/CRC checks expected
        // at the receiving end, before the package leaves this partition.
        crate::decode_rebootstrap_package_v2(&encoded, limits)?;
        Ok(package)
    }

    pub fn plan_delete_retention(
        &mut self,
        now_ms: i64,
        retention_ms: Option<u64>,
        retention_bytes: Option<u64>,
        protected_log_start_ceiling: Option<StreamOffset>,
    ) -> Result<DeleteRetentionPlan> {
        let snapshot_floor_raft_index = self.snapshot.last_included_index.get();
        Ok(self.log_mut().plan_delete_retention(DeleteRetentionSpec {
            now_ms,
            retention_ms,
            retention_bytes,
            protected_log_start_ceiling,
            snapshot_floor_raft_index,
        })?)
    }

    pub fn publish_delete_retention_generation(
        &mut self,
        expected_manifest_generation: u64,
    ) -> Result<usize> {
        let snapshot_floor_raft_index = self.snapshot.last_included_index.get();
        Ok(self.log_mut().publish_delete_retention_generation(
            expected_manifest_generation,
            snapshot_floor_raft_index,
        )?)
    }

    pub fn plan_keyed_compaction(
        &mut self,
        policy_epoch: u64,
        limits: CompactionLimits,
    ) -> Result<CompactionPlan> {
        self.plan_keyed_compaction_with_tombstone_gate(policy_epoch, limits, None)
    }

    pub fn plan_keyed_compaction_with_tombstone_gate(
        &mut self,
        policy_epoch: u64,
        limits: CompactionLimits,
        tombstone_removal: Option<TombstoneRemovalGate>,
    ) -> Result<CompactionPlan> {
        let snapshot_floor_raft_index = self.snapshot.last_included_index.get();
        let producer_checkpoint = self.latest_producer_checkpoint_fence()?;
        Ok(self.log_mut().plan_keyed_compaction(CompactionPlanSpec {
            policy_epoch,
            snapshot_floor_raft_index,
            producer_checkpoint,
            tombstone_removal,
            limits,
        })?)
    }

    pub fn execute_keyed_compaction(
        &mut self,
        plan: &CompactionPlan,
        current_policy_epoch: u64,
    ) -> Result<CompactionOutcome> {
        let snapshot_floor_raft_index = self.snapshot.last_included_index.get();
        Ok(self.log_mut().execute_keyed_compaction(
            plan,
            current_policy_epoch,
            snapshot_floor_raft_index,
        )?)
    }

    pub fn execute_keyed_compaction_with_publication_guard(
        &mut self,
        plan: &CompactionPlan,
        current_policy_epoch: u64,
        publication_metadata_epoch: &RwLock<u64>,
    ) -> Result<CompactionOutcome> {
        let snapshot_floor_raft_index = self.snapshot.last_included_index.get();
        Ok(self
            .log_mut()
            .execute_keyed_compaction_with_publication_guard(
                plan,
                current_policy_epoch,
                snapshot_floor_raft_index,
                publication_metadata_epoch,
            )?)
    }

    /// Rebuilds the authoritative P7 producer state strictly from committed
    /// Storage V1 frames. Uncommitted idempotent tails are deliberately ignored.
    ///
    /// This is the frozen P7-E/P9 handoff surface: later physical retention or
    /// producer snapshots may accelerate reconstruction, but they may not change
    /// the logical state represented here.
    pub fn recover_committed_producer_states(
        &self,
    ) -> Result<BTreeMap<ProducerId, PartitionProducerStateV1>> {
        self.recover_producer_states_with_snapshot()
    }

    pub fn persist_producer_snapshot(&self) -> Result<()> {
        let states = self.recover_producer_states_with_snapshot()?;
        let snapshot = ProducerSnapshotV1::new(
            self.topic_id,
            self.partition_id,
            self.log().committed_raft_index(),
            states,
        )?;
        self.producer_snapshot_store.persist(&snapshot)
    }

    pub fn discard_producer_snapshot(&self) -> Result<()> {
        self.producer_snapshot_store.discard()
    }

    pub fn producer_checkpoint_candidate(&self) -> Result<Option<ProducerSnapshotV1>> {
        let states = self.recover_producer_states_with_snapshot()?;
        if states.is_empty() {
            return Ok(None);
        }
        if states.len() > crate::MAX_PARTITION_PRODUCER_ENTRIES {
            return Err(ReplicationError::LimitExceeded {
                what: "P9 producer checkpoint entries",
                actual: states.len(),
                max: crate::MAX_PARTITION_PRODUCER_ENTRIES,
            });
        }
        let latest_producer_raft = states
            .values()
            .flat_map(|state| state.recent_committed_batches().iter())
            .map(|batch| batch.raft_index.get())
            .max()
            .ok_or(ReplicationError::Invalid(
                "non-empty producer state has no retained retry history",
            ))?;
        if self
            .latest_producer_checkpoint_fence()?
            .is_some_and(|checkpoint| checkpoint.covered_raft_index >= latest_producer_raft)
        {
            return Ok(None);
        }
        Ok(Some(ProducerSnapshotV1::new(
            self.topic_id,
            self.partition_id,
            self.log().committed_raft_index(),
            states,
        )?))
    }

    fn recover_producer_states_with_snapshot(
        &self,
    ) -> Result<BTreeMap<ProducerId, PartitionProducerStateV1>> {
        let committed = self.log().committed_raft_index();
        if let Some((checkpoint, _control_raft_index)) = self.latest_producer_checkpoint()? {
            return self.replay_committed_producer_suffix(
                checkpoint.states,
                checkpoint.committed_raft_index,
                committed,
                true,
            );
        }
        let loaded = self.producer_snapshot_store.load();
        let (states, boundary) = match loaded {
            Ok(Some(snapshot))
                if snapshot.topic_id == self.topic_id
                    && snapshot.partition_id == self.partition_id
                    && snapshot.committed_raft_index <= committed =>
            {
                (snapshot.states, snapshot.committed_raft_index)
            }
            Ok(Some(_)) => {
                self.producer_snapshot_store.discard()?;
                (BTreeMap::new(), 0)
            }
            Ok(None) => (BTreeMap::new(), 0),
            Err(error) if is_rebuildable_producer_snapshot_error(&error) => {
                self.producer_snapshot_store.discard()?;
                (BTreeMap::new(), 0)
            }
            Err(error) => return Err(error),
        };
        self.replay_committed_producer_suffix(states, boundary, committed, false)
    }

    #[cfg(test)]
    fn recover_producer_states_full_scan(
        &self,
    ) -> Result<BTreeMap<ProducerId, PartitionProducerStateV1>> {
        self.replay_committed_producer_suffix(
            BTreeMap::new(),
            0,
            self.log().committed_raft_index(),
            false,
        )
    }

    fn replay_committed_producer_suffix(
        &self,
        mut states: BTreeMap<ProducerId, PartitionProducerStateV1>,
        boundary: u64,
        committed: u64,
        allow_sparse_retained_prefix: bool,
    ) -> Result<BTreeMap<ProducerId, PartitionProducerStateV1>> {
        if boundary > committed {
            return Err(ReplicationError::Invalid(
                "producer recovery boundary exceeds committed Raft index",
            ));
        }
        let mut next = boundary.checked_add(1).ok_or(ReplicationError::Invalid(
            "producer recovery boundary overflow",
        ))?;
        while next <= committed {
            let start = RaftIndex::new(next).ok_or(ReplicationError::Invalid(
                "producer recovery page cannot begin at Raft index zero",
            ))?;
            let page = self
                .log()
                .read_frames_from_raft(start, PRODUCER_RECOVERY_PAGE_FRAMES)?;
            if page.is_empty() {
                if allow_sparse_retained_prefix {
                    return Ok(states);
                }
                return Err(ReplicationError::Invalid(
                    "committed producer recovery ended before committed Raft boundary",
                ));
            }
            let mut advanced = false;
            for frame in page {
                let raft_index = frame.raft_index().get();
                if raft_index > committed {
                    return Ok(states);
                }
                if raft_index < next {
                    return Err(ReplicationError::Invalid(
                        "bounded producer recovery page regressed Raft index",
                    ));
                }
                apply_committed_producer_frame(&mut states, frame, self.command_limits)?;
                next = raft_index.checked_add(1).ok_or(ReplicationError::Invalid(
                    "producer recovery Raft index overflow",
                ))?;
                advanced = true;
                if next > committed {
                    break;
                }
            }
            if !advanced {
                return Err(ReplicationError::Invalid(
                    "bounded producer recovery page made no progress",
                ));
            }
        }
        Ok(states)
    }

    fn latest_producer_checkpoint(&self) -> Result<Option<(ProducerSnapshotV1, u64)>> {
        let Some(control) = self.log().latest_committed_producer_checkpoint_control()? else {
            return Ok(None);
        };
        let committed = self.log().committed_raft_index();
        let control_raft_index = control.raft_index.get();
        if control_raft_index > committed {
            return Err(ReplicationError::Invalid(
                "P9 producer checkpoint control exceeds committed boundary",
            ));
        }
        let command_bytes = control
            .body
            .len()
            .checked_add(4)
            .ok_or(ReplicationError::Invalid(
                "P9 producer checkpoint command length overflow",
            ))?;
        if command_bytes > self.command_limits.max_command_bytes {
            return Err(ReplicationError::LimitExceeded {
                what: "P9 producer checkpoint command bytes",
                actual: command_bytes,
                max: self.command_limits.max_command_bytes,
            });
        }
        let checkpoint = decode_producer_snapshot(&control.body)?;
        if checkpoint.topic_id != self.topic_id || checkpoint.partition_id != self.partition_id {
            return Err(ReplicationError::Invalid(
                "P9 producer checkpoint partition identity mismatch",
            ));
        }
        if checkpoint.states.len() > crate::MAX_PARTITION_PRODUCER_ENTRIES {
            return Err(ReplicationError::LimitExceeded {
                what: "P9 producer checkpoint entries",
                actual: checkpoint.states.len(),
                max: crate::MAX_PARTITION_PRODUCER_ENTRIES,
            });
        }
        if checkpoint.committed_raft_index >= control_raft_index {
            return Err(ReplicationError::Invalid(
                "P9 producer checkpoint covered boundary must precede its committed control",
            ));
        }
        Ok(Some((checkpoint, control_raft_index)))
    }

    pub fn latest_producer_checkpoint_fence(&self) -> Result<Option<ProducerCheckpointFence>> {
        Ok(self
            .latest_producer_checkpoint()?
            .map(|(checkpoint, control_raft_index)| ProducerCheckpointFence {
                control_raft_index,
                covered_raft_index: checkpoint.committed_raft_index,
            }))
    }

    pub fn directory(&self) -> &Path {
        &self.directory
    }

    #[cfg(test)]
    pub(crate) fn producer_snapshot_path(&self) -> &Path {
        self.producer_snapshot_store.path()
    }

    #[cfg(test)]
    pub(crate) fn fail_next_execute(&mut self, message: &'static str) {
        self.fail_next_execute = Some(message);
    }

    #[cfg(test)]
    pub(crate) fn fail_next_group_sync(&mut self, message: &'static str) {
        self.fail_next_group_sync = Some(message);
    }

    pub fn build_rebootstrap_package(
        &self,
        metadata: &SnapshotMetadata,
    ) -> Result<RebootstrapPackage> {
        if metadata.last_included_index == LogIndex::ZERO
            || metadata.last_included_index > LogIndex::new(self.log().durable_raft_index())
        {
            return Err(ReplicationError::Invalid(
                "rebootstrap snapshot boundary is not durably available",
            ));
        }
        let frames = self
            .log()
            .read_frames()?
            .into_iter()
            .take_while(|frame| frame.raft_index().get() <= metadata.last_included_index.get())
            .collect::<Vec<_>>();
        let package = RebootstrapPackage {
            topic_id: self.topic_id,
            partition_id: self.partition_id,
            snapshot: metadata.clone(),
            frames,
        };
        package.validate(RebootstrapLimits::default())?;
        Ok(package)
    }

    fn load_or_initialize_sidecars(&mut self) -> Result<()> {
        self.hard_state = match self.hard_store.load()? {
            Some(state) => state,
            None => {
                if self.log().next_raft_index() != RaftIndex::FIRST
                    || self.log().committed_raft_index() != 0
                {
                    return Err(ReplicationError::Invalid(
                        "non-empty P1 partition is missing P4 Raft hard state",
                    ));
                }
                let state = HardState::initial();
                self.hard_store.persist(&state)?;
                state
            }
        };
        if self.hard_state.durable_commit_index.get() != self.log().committed_raft_index() {
            return Err(ReplicationError::Invalid(
                "hard-state/P1 committed index mismatch without recoverable intent",
            ));
        }

        self.snapshot = match self.snapshot_store.load()? {
            Some(snapshot) => snapshot,
            None => {
                self.snapshot_store.persist(&self.snapshot)?;
                self.snapshot.clone()
            }
        };
        Ok(())
    }

    fn recover_commit_intent(&mut self) -> Result<()> {
        let Some(intent) = self.commit_intent_store.load()? else {
            return Ok(());
        };
        let hard = self.hard_store.load()?.ok_or(ReplicationError::Invalid(
            "commit intent exists without hard-state sidecar",
        ))?;
        let log_commit = LogIndex::new(self.log().committed_raft_index());
        let hard_commit = hard.durable_commit_index;
        if !matches!(log_commit, value if value == intent.previous || value == intent.target)
            || !matches!(hard_commit, value if value == intent.previous || value == intent.target)
        {
            return Err(ReplicationError::Invalid(
                "commit intent does not explain durable commit mismatch",
            ));
        }
        if self.log().durable_raft_index() < intent.target.get() {
            return Err(ReplicationError::Invalid(
                "commit intent target exceeds locally durable P1 log",
            ));
        }
        if log_commit != intent.target {
            self.log_mut().mark_committed(intent.target.get())?;
        }
        let mut completed = hard;
        if completed.durable_commit_index != intent.target {
            completed.durable_commit_index = intent.target;
            self.hard_store.persist(&completed)?;
        }
        self.commit_intent_store.clear()?;
        Ok(())
    }

    fn update_hard_state(&mut self, state: &HardState) -> Result<()> {
        if state.durable_commit_index.get() != self.log().committed_raft_index() {
            return Err(ReplicationError::Invalid(
                "hard-state update may not change commit outside AdvanceCommit",
            ));
        }
        self.hard_store.persist(state)?;
        self.hard_state = state.clone();
        Ok(())
    }

    fn append_entries(&mut self, entries: &[LogEntry], now_ms: i64) -> Result<()> {
        self.append_entries_unflushed(entries, now_ms)?;
        self.log_mut().flush_data()?;
        Ok(())
    }

    fn append_entries_unflushed(&mut self, entries: &[LogEntry], now_ms: i64) -> Result<()> {
        if entries.is_empty() {
            return Ok(());
        }
        for entry in entries {
            let frame = log_entry_to_frame(entry, self.command_limits, self.data_compression)?;
            self.log_mut().append(&frame, now_ms)?;
        }
        Ok(())
    }

    fn advance_commit(&mut self, target: LogIndex) -> Result<()> {
        let previous = self.hard_state.durable_commit_index;
        if target < previous {
            return Err(ReplicationError::Invalid(
                "commit index cannot move backward",
            ));
        }
        if target == previous {
            return Ok(());
        }
        if self.log().committed_raft_index() != previous.get() {
            return Err(ReplicationError::Invalid(
                "P1 commit diverged before AdvanceCommit",
            ));
        }
        let intent = CommitIntent { previous, target };
        self.commit_intent_store.persist(intent)?;
        self.log_mut().mark_committed(target.get())?;
        let mut hard = self.hard_state.clone();
        hard.durable_commit_index = target;
        self.hard_store.persist(&hard)?;
        self.hard_state = hard;
        self.commit_intent_store.clear()?;
        Ok(())
    }

    fn install_snapshot(&mut self, metadata: &SnapshotMetadata) -> Result<()> {
        if metadata.last_included_index > LogIndex::new(self.log().durable_raft_index()) {
            return Err(ReplicationError::Invalid(
                "snapshot metadata requires P1 rebootstrap before persistence completion",
            ));
        }
        self.snapshot_store.persist(metadata)?;
        self.snapshot = metadata.clone();
        Ok(())
    }

    fn install_rebootstrap_package(
        &mut self,
        package: &RebootstrapPackage,
        now_ms: i64,
    ) -> Result<()> {
        package.validate(RebootstrapLimits::default())?;
        if package.topic_id != self.topic_id || package.partition_id != self.partition_id {
            return Err(ReplicationError::Invalid(
                "rebootstrap package targets another partition",
            ));
        }
        if package.snapshot.last_included_index <= self.hard_state.durable_commit_index {
            return Err(ReplicationError::Invalid(
                "rebootstrap package does not advance the durable snapshot/commit boundary",
            ));
        }

        let staging = rebootstrap_staging_path(&self.directory);
        let backup = rebootstrap_backup_path(&self.directory);
        let intent = rebootstrap_intent_path(&self.directory);
        if intent.exists() || backup.exists() {
            return Err(ReplicationError::Invalid(
                "another rebootstrap publication is already in progress",
            ));
        }
        if staging.exists() {
            fs::remove_dir_all(&staging)?;
        }
        fs::create_dir_all(&staging)?;

        let mut staged_log = PartitionLog::open_or_create(
            &staging,
            self.topic_id,
            self.partition_id,
            now_ms,
            self.storage_config,
        )?;
        for frame in &package.frames {
            staged_log.append(frame, now_ms)?;
        }
        staged_log.flush_data()?;
        staged_log.mark_committed(package.snapshot.last_included_index.get())?;
        drop(staged_log);

        let mut staged_hard = self.hard_state.clone();
        staged_hard.durable_commit_index = package.snapshot.last_included_index;
        HardStateStore::new(&staging).persist(&staged_hard)?;
        SnapshotStore::new(&staging).persist(&package.snapshot)?;

        // Full validation occurs before the publication intent is durable.
        let (validated, _) = Self::open_inner(
            &staging,
            self.topic_id,
            self.partition_id,
            now_ms,
            self.storage_config,
            self.initial_configuration.clone(),
            self.command_limits,
        )?;
        drop(validated);

        atomic_write(&intent, b"ASTRSWP1")?;
        let parent = self
            .directory
            .parent()
            .ok_or(ReplicationError::Invalid(
                "partition directory has no parent for rebootstrap swap",
            ))?
            .to_path_buf();

        // Close all handles into the currently authoritative partition before
        // directory rename. The actor becomes fatal on any publication error;
        // restart recovery will choose/restore a valid generation.
        drop(self.log.take());
        fs::rename(&self.directory, &backup)?;
        sync_directory_path(&parent)?;
        fs::rename(&staging, &self.directory)?;
        sync_directory_path(&parent)?;

        let selected_compression = self.data_compression;
        let (mut replacement, _) = Self::open_inner(
            &self.directory,
            self.topic_id,
            self.partition_id,
            now_ms,
            self.storage_config,
            self.initial_configuration.clone(),
            self.command_limits,
        )?;
        replacement.data_compression = selected_compression;
        *self = replacement;

        fs::remove_dir_all(&backup)?;
        remove_file_if_exists(&intent)?;
        sync_directory_path(&parent)?;
        Ok(())
    }
}

fn rebootstrap_staging_path(directory: &Path) -> PathBuf {
    sibling_path(directory, "p4-rebootstrap-staging")
}

fn rebootstrap_backup_path(directory: &Path) -> PathBuf {
    sibling_path(directory, "p4-rebootstrap-backup")
}

fn rebootstrap_intent_path(directory: &Path) -> PathBuf {
    sibling_path(directory, "p4-rebootstrap-intent")
}

fn sibling_path(directory: &Path, suffix: &str) -> PathBuf {
    let parent = directory.parent().unwrap_or_else(|| Path::new("."));
    let name = directory
        .file_name()
        .and_then(|value| value.to_str())
        .unwrap_or("partition");
    parent.join(format!(".{name}.{suffix}"))
}

fn recover_interrupted_rebootstrap(directory: &Path) -> Result<()> {
    let staging = rebootstrap_staging_path(directory);
    let backup = rebootstrap_backup_path(directory);
    let intent = rebootstrap_intent_path(directory);
    let parent = directory.parent().ok_or(ReplicationError::Invalid(
        "partition directory has no parent for rebootstrap recovery",
    ))?;

    if !intent.exists() {
        if backup.exists() {
            return Err(ReplicationError::Invalid(
                "rebootstrap backup exists without publication intent",
            ));
        }
        if staging.exists() {
            fs::remove_dir_all(&staging)?;
            sync_directory_path(parent)?;
        }
        return Ok(());
    }

    match (directory.exists(), staging.exists(), backup.exists()) {
        // Intent was durable but the old generation was never moved. Preserve old.
        (true, true, false) => {
            fs::remove_dir_all(&staging)?;
            remove_file_if_exists(&intent)?;
            sync_directory_path(parent)?;
        }
        // Old generation was moved but new publication did not complete. Roll back.
        (false, true, true) | (false, false, true) => {
            fs::rename(&backup, directory)?;
            if staging.exists() {
                fs::remove_dir_all(&staging)?;
            }
            remove_file_if_exists(&intent)?;
            sync_directory_path(parent)?;
        }
        // New target is published and backup remains. Normal open validates the
        // new target; finish_rebootstrap_recovery removes backup only after that.
        (true, false, true) => {}
        // Backup was already removed; publication completed except intent cleanup.
        (true, false, false) => {
            remove_file_if_exists(&intent)?;
            sync_directory_path(parent)?;
        }
        _ => {
            return Err(ReplicationError::Invalid(
                "ambiguous rebootstrap publication state",
            ));
        }
    }
    Ok(())
}

fn finish_rebootstrap_recovery(directory: &Path) -> Result<()> {
    let backup = rebootstrap_backup_path(directory);
    let staging = rebootstrap_staging_path(directory);
    let intent = rebootstrap_intent_path(directory);
    if !intent.exists() {
        return Ok(());
    }
    let parent = directory.parent().ok_or(ReplicationError::Invalid(
        "partition directory has no parent for rebootstrap recovery",
    ))?;
    if backup.exists() {
        fs::remove_dir_all(&backup)?;
    }
    if staging.exists() {
        fs::remove_dir_all(&staging)?;
    }
    remove_file_if_exists(&intent)?;
    sync_directory_path(parent)
}

fn restore_rebootstrap_backup(directory: &Path) -> Result<()> {
    let backup = rebootstrap_backup_path(directory);
    let staging = rebootstrap_staging_path(directory);
    let intent = rebootstrap_intent_path(directory);
    let parent = directory.parent().ok_or(ReplicationError::Invalid(
        "partition directory has no parent for rebootstrap rollback",
    ))?;
    if directory.exists() {
        fs::remove_dir_all(directory)?;
    }
    fs::rename(&backup, directory)?;
    if staging.exists() {
        fs::remove_dir_all(&staging)?;
    }
    remove_file_if_exists(&intent)?;
    sync_directory_path(parent)
}

fn remove_file_if_exists(path: &Path) -> Result<()> {
    match fs::remove_file(path) {
        Ok(()) => Ok(()),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(()),
        Err(error) => Err(error.into()),
    }
}

fn sync_directory_path(directory: &Path) -> Result<()> {
    #[cfg(unix)]
    File::open(directory)?.sync_all()?;
    #[cfg(not(unix))]
    let _ = directory;
    Ok(())
}

fn log_entry_to_frame(
    entry: &LogEntry,
    command_limits: CommandLimits,
    data_compression: Compression,
) -> Result<Frame> {
    let raft_index = RaftIndex::new(entry.index.get())
        .ok_or(ReplicationError::Invalid("P1 cannot store Raft index zero"))?;
    let raft_term = RaftTerm::new(entry.term.get());
    if entry.term == Term::ZERO {
        return Err(ReplicationError::Invalid(
            "P1 cannot store a term-zero Raft entry",
        ));
    }
    match &entry.kind {
        EntryKind::Application(payload) => match application_payload_version(payload)? {
            1 => {
                let command = decode_application_command(payload, command_limits)?;
                Ok(Frame::Data(RecordBatch {
                    raft_term,
                    raft_index,
                    base_stream_offset: command.base_stream_offset,
                    producer: None,
                    compression: data_compression,
                    records: command.records,
                }))
            }
            2 => {
                let command = decode_application_command_v2(payload, command_limits)?;
                Ok(Frame::Data(RecordBatch {
                    raft_term,
                    raft_index,
                    base_stream_offset: command.base_stream_offset,
                    producer: Some(ProducerIdentity {
                        producer_id: command.authority.producer_id.get(),
                        producer_epoch: command.authority.producer_epoch.get(),
                        base_sequence: command.base_sequence.get(),
                    }),
                    compression: data_compression,
                    records: command.records,
                }))
            }
            3 => match application_maintenance_kind(payload)? {
                1 => {
                    let command = decode_application_maintenance_command_v1(payload)?;
                    Ok(Frame::Control(ControlEntry {
                        raft_term,
                        raft_index,
                        control_kind: P9_CONTROL_ADVANCE_LOG_START_OFFSET,
                        control_version: P9_MAINTENANCE_CONTROL_VERSION,
                        body: encode_log_start_offset_body(command.new_log_start_offset).to_vec(),
                    }))
                }
                2 => {
                    let command = decode_application_producer_state_checkpoint_command_v1(
                        payload,
                        command_limits,
                    )?;
                    if command.checkpoint.committed_raft_index >= raft_index.get() {
                        return Err(ReplicationError::Invalid(
                            "P9 producer checkpoint covered boundary must precede its Raft entry",
                        ));
                    }
                    Ok(Frame::Control(ControlEntry {
                        raft_term,
                        raft_index,
                        control_kind: P9_CONTROL_PRODUCER_STATE_CHECKPOINT,
                        control_version: P9_MAINTENANCE_CONTROL_VERSION,
                        body: encode_producer_snapshot(&command.checkpoint)?,
                    }))
                }
                3 => {
                    let command = decode_application_compaction_safe_command_v1(payload)?;
                    Ok(Frame::Control(ControlEntry {
                        raft_term,
                        raft_index,
                        control_kind: P9_CONTROL_ADVANCE_COMPACTION_SAFE_OFFSET,
                        control_version: P9_MAINTENANCE_CONTROL_VERSION,
                        body: encode_compaction_safe_offset_body(
                            command.new_compaction_safe_offset,
                        )
                        .to_vec(),
                    }))
                }
                _ => Err(ReplicationError::Invalid(
                    "unknown P9 maintenance command kind",
                )),
            },
            version => Err(ReplicationError::InvalidOwned(format!(
                "unsupported application command version {version}",
            ))),
        },
        EntryKind::LeaderBarrier => Ok(Frame::Control(ControlEntry {
            raft_term,
            raft_index,
            control_kind: P4_CONTROL_LEADER_BARRIER,
            control_version: P4_CONTROL_VERSION,
            body: Vec::new(),
        })),
        EntryKind::Configuration(change) => Ok(Frame::Control(ControlEntry {
            raft_term,
            raft_index,
            control_kind: P4_CONTROL_CONFIGURATION,
            control_version: P4_CONTROL_VERSION,
            body: encode_configuration_change(change)?,
        })),
    }
}

fn frame_to_log_entry(frame: Frame, command_limits: CommandLimits) -> Result<LogEntry> {
    let term = Term::new(frame.raft_term().get());
    let index = LogIndex::new(frame.raft_index().get());
    if term == Term::ZERO {
        return Err(ReplicationError::Invalid(
            "term-zero frame cannot be recovered into P3 log",
        ));
    }
    let kind = match frame {
        Frame::Data(batch) => {
            if let Some(producer) = batch.producer {
                let command = ApplicationCommandV2 {
                    base_stream_offset: batch.base_stream_offset,
                    authority: checked_storage_authority(&producer)?,
                    base_sequence: ProducerSequence::new(producer.base_sequence),
                    records: batch.records,
                };
                EntryKind::Application(encode_application_command_v2(&command, command_limits)?)
            } else {
                let command = ApplicationCommand {
                    base_stream_offset: batch.base_stream_offset,
                    records: batch.records,
                };
                EntryKind::Application(encode_application_command(&command, command_limits)?)
            }
        }
        Frame::Control(control) => {
            if control.control_kind == P9_CONTROL_ADVANCE_LOG_START_OFFSET {
                if control.control_version != P9_MAINTENANCE_CONTROL_VERSION {
                    return Err(ReplicationError::Invalid(
                        "unsupported P9 maintenance control entry version",
                    ));
                }
                let command = ApplicationMaintenanceCommandV1 {
                    new_log_start_offset: decode_log_start_offset_body(&control.body)?,
                };
                return Ok(LogEntry::new(
                    term,
                    index,
                    EntryKind::Application(encode_application_maintenance_command_v1(command)),
                ));
            }
            if control.control_kind == P9_CONTROL_PRODUCER_STATE_CHECKPOINT {
                if control.control_version != P9_MAINTENANCE_CONTROL_VERSION {
                    return Err(ReplicationError::Invalid(
                        "unsupported P9 producer checkpoint control version",
                    ));
                }
                let checkpoint = decode_producer_snapshot(&control.body)?;
                if checkpoint.committed_raft_index >= index.get() {
                    return Err(ReplicationError::Invalid(
                        "P9 producer checkpoint covered boundary must precede its control entry",
                    ));
                }
                let command = ApplicationProducerStateCheckpointCommandV1 { checkpoint };
                return Ok(LogEntry::new(
                    term,
                    index,
                    EntryKind::Application(
                        encode_application_producer_state_checkpoint_command_v1(
                            &command,
                            command_limits,
                        )?,
                    ),
                ));
            }
            if control.control_kind == P9_CONTROL_ADVANCE_COMPACTION_SAFE_OFFSET {
                if control.control_version != P9_MAINTENANCE_CONTROL_VERSION {
                    return Err(ReplicationError::Invalid(
                        "unsupported P9 compaction-safe control version",
                    ));
                }
                let command = ApplicationCompactionSafeCommandV1 {
                    new_compaction_safe_offset: decode_compaction_safe_offset_body(&control.body)?,
                };
                return Ok(LogEntry::new(
                    term,
                    index,
                    EntryKind::Application(encode_application_compaction_safe_command_v1(command)),
                ));
            }
            if control.control_version != P4_CONTROL_VERSION {
                return Err(ReplicationError::Invalid(
                    "unsupported P4 control entry version",
                ));
            }
            match control.control_kind {
                P4_CONTROL_LEADER_BARRIER => {
                    if !control.body.is_empty() {
                        return Err(ReplicationError::Invalid(
                            "leader barrier control body must be empty",
                        ));
                    }
                    EntryKind::LeaderBarrier
                }
                P4_CONTROL_CONFIGURATION => {
                    EntryKind::Configuration(decode_configuration_change(&control.body)?)
                }
                _ => {
                    return Err(ReplicationError::InvalidOwned(format!(
                        "unknown P4 control kind {}",
                        control.control_kind
                    )));
                }
            }
        }
    };
    Ok(LogEntry::new(term, index, kind))
}

fn application_payload_version(payload: &[u8]) -> Result<u16> {
    let prefix = payload.get(..2).ok_or(ReplicationError::Invalid(
        "application command version is truncated",
    ))?;
    Ok(u16::from_le_bytes([prefix[0], prefix[1]]))
}

fn application_maintenance_kind(payload: &[u8]) -> Result<u16> {
    let bytes = payload.get(2..4).ok_or(ReplicationError::Invalid(
        "P9 maintenance command kind is truncated",
    ))?;
    Ok(u16::from_le_bytes([bytes[0], bytes[1]]))
}

fn checked_storage_authority(producer: &ProducerIdentity) -> Result<ProducerAuthority> {
    let producer_id = ProducerId::new(producer.producer_id).ok_or(ReplicationError::Invalid(
        "stored producer id zero is reserved",
    ))?;
    let producer_epoch = ProducerEpoch::new(producer.producer_epoch).ok_or(
        ReplicationError::Invalid("stored producer epoch zero is reserved"),
    )?;
    Ok(ProducerAuthority::new(producer_id, producer_epoch))
}

fn producer_state_recovery_error(error: crate::ProducerStateError) -> ReplicationError {
    ReplicationError::InvalidOwned(format!("invalid committed producer history: {error:?}",))
}

fn apply_committed_producer_frame(
    states: &mut BTreeMap<ProducerId, PartitionProducerStateV1>,
    frame: Frame,
    command_limits: CommandLimits,
) -> Result<()> {
    let Frame::Data(batch) = frame else {
        return Ok(());
    };
    let Some(producer) = &batch.producer else {
        return Ok(());
    };
    if batch.records.len() > command_limits.max_records {
        return Err(ReplicationError::LimitExceeded {
            what: "committed producer record count",
            actual: batch.records.len(),
            max: command_limits.max_records,
        });
    }
    let authority = checked_storage_authority(producer)?;
    let record_count = u32::try_from(batch.records.len())
        .map_err(|_| ReplicationError::Invalid("producer record count overflow"))?;
    let committed = CommittedBatchV1::new(
        authority,
        ProducerSequence::new(producer.base_sequence),
        record_count,
        batch.base_stream_offset,
        batch.raft_index,
        canonical_storage_record_set_digest(&batch.records)?,
    )
    .map_err(producer_state_recovery_error)?;
    match states.get_mut(&authority.producer_id) {
        Some(state) => {
            state
                .apply_committed(committed)
                .map_err(producer_state_recovery_error)?;
        }
        None => {
            states.insert(
                authority.producer_id,
                PartitionProducerStateV1::from_first_committed(committed)
                    .map_err(producer_state_recovery_error)?,
            );
        }
    }
    Ok(())
}

fn is_rebuildable_producer_snapshot_error(error: &ReplicationError) -> bool {
    matches!(
        error,
        ReplicationError::Invalid(_)
            | ReplicationError::InvalidOwned(_)
            | ReplicationError::LimitExceeded { .. }
            | ReplicationError::ChecksumMismatch { .. }
    )
}

// The integration-style unit test launches a separate copy of its test binary
// and TERMINATES it from the parent after this exact on-disk publication
// barrier. This hook is not compiled into production builds. It adds no
// failure behavior or environment-variable control to serving binaries.
#[cfg(test)]
fn p9g_v2_pause_for_external_process_kill(stage: &'static str) {
    if std::env::var("ASTRALOG_P9G_V2_PROCESS_KILL_STAGE")
        .ok()
        .as_deref()
        != Some(stage)
    {
        return;
    }
    let marker = std::env::var_os("ASTRALOG_P9G_V2_PROCESS_KILL_MARKER")
        .expect("P9-G child must supply an isolated readiness marker");
    let mut ready = OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(marker)
        .expect("P9-G child must persist its crash-barrier marker");
    ready
        .write_all(stage.as_bytes())
        .expect("P9-G child must write crash-barrier marker");
    ready
        .sync_all()
        .expect("P9-G child must sync crash-barrier marker");
    // The parent has a 30s deadline to Child::kill this process while its
    // real filesystem publication state is in flight. If the parent dies,
    // do not leave an orphaned test process parked indefinitely: fail the
    // child after a longer independent 45s watchdog instead. The watchdog
    // is never success evidence for the parent test.
    std::thread::sleep(std::time::Duration::from_secs(45));
    panic!("P9-G external process-kill parent never terminated its paused test child");
}

#[cfg(test)]
mod tests {
    use std::{
        fs,
        time::{SystemTime, UNIX_EPOCH},
    };

    use astralog_common::SegmentBaseOffset;
    use astralog_raft::{ConfigurationChange, NodeId, PersistenceToken};
    use astralog_storage::{
        DataRecord, GenerationTransaction, ManifestEntry, RetainedFrameV2, RetainedSegmentHeaderV2,
        SegmentFileId, SparseRetainedBatchV2, SparseRetainedRecordV2, StorageLimits, UserHeader,
        encode_retained_frame_v2, encode_retained_segment_header_v2,
    };

    use super::*;
    use crate::{decode_rebootstrap_package, encode_rebootstrap_package};

    fn node(value: u64) -> NodeId {
        NodeId::new(value).unwrap()
    }

    fn temp_dir(label: &str) -> PathBuf {
        let nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let path = std::env::temp_dir().join(format!(
            "astralog-p4-adapter-{label}-{}-{nanos}",
            std::process::id()
        ));
        fs::create_dir_all(&path).unwrap();
        path
    }

    fn config() -> StableConfiguration {
        StableConfiguration::new([node(1), node(2), node(3)], []).unwrap()
    }

    fn open(dir: &Path) -> (PartitionPersistence, RecoveredState) {
        PartitionPersistence::open(
            dir,
            TopicId::new([0x44; 16]),
            PartitionId::new(0),
            0,
            PartitionLogConfig::default(),
            config(),
            CommandLimits::default(),
        )
        .unwrap()
    }

    fn application(index: u64, offset: u64, value: &[u8]) -> LogEntry {
        let command = ApplicationCommand {
            base_stream_offset: StreamOffset::new(offset),
            records: vec![DataRecord {
                timestamp_ms: 1,
                key: Some(b"k".to_vec()),
                value: Some(value.to_vec()),
                headers: vec![UserHeader {
                    name: b"h".to_vec(),
                    value: None,
                }],
            }],
        };
        LogEntry::new(
            Term::new(1),
            LogIndex::new(index),
            EntryKind::Application(
                encode_application_command(&command, CommandLimits::default()).unwrap(),
            ),
        )
    }

    fn producer_authority(epoch: u32) -> ProducerAuthority {
        ProducerAuthority::new(
            ProducerId::new(7).unwrap(),
            ProducerEpoch::new(epoch).unwrap(),
        )
    }

    fn idempotent_application(
        index: u64,
        offset: u64,
        epoch: u32,
        base_sequence: u32,
        value: &[u8],
    ) -> LogEntry {
        let command = ApplicationCommandV2 {
            base_stream_offset: StreamOffset::new(offset),
            authority: producer_authority(epoch),
            base_sequence: ProducerSequence::new(base_sequence),
            records: vec![DataRecord {
                timestamp_ms: 10 + i64::from(base_sequence),
                key: Some(b"k".to_vec()),
                value: Some(value.to_vec()),
                headers: vec![UserHeader {
                    name: b"h".to_vec(),
                    value: Some(vec![epoch as u8]),
                }],
            }],
        };
        LogEntry::new(
            Term::new(1),
            LogIndex::new(index),
            EntryKind::Application(
                encode_application_command_v2(&command, CommandLimits::default()).unwrap(),
            ),
        )
    }

    fn idempotent_application_for_producer(index: u64, offset: u64, producer_id: u64) -> LogEntry {
        let authority = ProducerAuthority::new(
            ProducerId::new(producer_id).unwrap(),
            ProducerEpoch::new(1).unwrap(),
        );
        let command = ApplicationCommandV2 {
            base_stream_offset: StreamOffset::new(offset),
            authority,
            base_sequence: ProducerSequence::ZERO,
            records: vec![DataRecord {
                timestamp_ms: i64::try_from(producer_id).unwrap(),
                key: Some(b"p9f-shared-key".to_vec()),
                value: Some(vec![u8::try_from(producer_id & 0xff).unwrap()]),
                headers: Vec::new(),
            }],
        };
        LogEntry::new(
            Term::new(1),
            LogIndex::new(index),
            EntryKind::Application(
                encode_application_command_v2(&command, CommandLimits::default()).unwrap(),
            ),
        )
    }

    #[test]
    fn p9f_many_producer_checkpoint_at_p7_bound_unlocks_compaction_and_recovers_all_states() {
        let dir = temp_dir("p9f-many-producers");
        let topic_id = TopicId::new([0xf8; 16]);
        let storage_config = PartitionLogConfig {
            max_segment_bytes: 8 * 1024 * 1024,
            max_segment_age_ms: None,
            index_interval_bytes: 1,
            limits: StorageLimits::default(),
        };
        let (mut adapter, _) = PartitionPersistence::open(
            &dir,
            topic_id,
            PartitionId::new(0),
            0,
            storage_config,
            config(),
            CommandLimits::default(),
        )
        .unwrap();

        let producer_count = u64::try_from(crate::MAX_PARTITION_PRODUCER_ENTRIES).unwrap();
        // One real producer-bearing source frame is enough to exercise the
        // cleaner fence. The checkpoint itself carries the full 4,096-entry
        // canonical P7 map, so this test hits the actual P9-F snapshot/cache
        // resource bound without O(n^2) historical replay cost.
        adapter
            .execute(
                &PersistenceBatch::new(
                    PersistenceToken::new(1),
                    vec![
                        PersistenceOp::UpdateHardState(HardState {
                            current_term: Term::new(1),
                            voted_for: Some(node(1)),
                            durable_commit_index: LogIndex::ZERO,
                        }),
                        PersistenceOp::Append(vec![idempotent_application_for_producer(1, 0, 1)]),
                        PersistenceOp::AdvanceCommit {
                            to: LogIndex::FIRST,
                        },
                    ],
                ),
                1,
            )
            .unwrap();
        // Freeze the producer-bearing source as one immutable candidate.
        adapter.log_mut().roll(2).unwrap();

        let winner_index = 2;
        let winner_offset = 1;
        let winner = ApplicationCommand {
            base_stream_offset: StreamOffset::new(winner_offset),
            records: vec![DataRecord {
                timestamp_ms: 50_000,
                key: Some(b"p9f-shared-key".to_vec()),
                value: Some(b"newest-winner".to_vec()),
                headers: Vec::new(),
            }],
        };
        adapter
            .execute(
                &PersistenceBatch::new(
                    PersistenceToken::new(2),
                    vec![
                        PersistenceOp::Append(vec![LogEntry::new(
                            Term::new(1),
                            LogIndex::new(winner_index),
                            EntryKind::Application(
                                encode_application_command(&winner, CommandLimits::default())
                                    .unwrap(),
                            ),
                        )]),
                        PersistenceOp::AdvanceCommit {
                            to: LogIndex::new(winner_index),
                        },
                    ],
                ),
                2,
            )
            .unwrap();

        let actual_source_record = DataRecord {
            timestamp_ms: 1,
            key: Some(b"p9f-shared-key".to_vec()),
            value: Some(vec![1]),
            headers: Vec::new(),
        };
        let source_digest = canonical_storage_record_set_digest(&[actual_source_record]).unwrap();
        let mut states = BTreeMap::new();
        for producer_id in 1..=producer_count {
            let authority = ProducerAuthority::new(
                ProducerId::new(producer_id).unwrap(),
                ProducerEpoch::new(1).unwrap(),
            );
            let digest = if producer_id == 1 {
                source_digest
            } else {
                [u8::try_from(producer_id & 0xff).unwrap(); 32]
            };
            let committed = CommittedBatchV1::new(
                authority,
                ProducerSequence::ZERO,
                1,
                StreamOffset::new(0),
                RaftIndex::new(1).unwrap(),
                digest,
            )
            .unwrap();
            states.insert(
                authority.producer_id,
                PartitionProducerStateV1::from_first_committed(committed).unwrap(),
            );
        }
        let checkpoint = ProducerSnapshotV1::new(topic_id, PartitionId::new(0), 2, states).unwrap();
        assert_eq!(
            checkpoint.states.len(),
            crate::MAX_PARTITION_PRODUCER_ENTRIES
        );
        ProducerSnapshotStore::new(&dir)
            .persist(&checkpoint)
            .unwrap();
        let checkpoint_command = encode_application_producer_state_checkpoint_command_v1(
            &ApplicationProducerStateCheckpointCommandV1 { checkpoint },
            CommandLimits::default(),
        )
        .unwrap();
        let checkpoint_index = 3;
        adapter
            .execute(
                &PersistenceBatch::new(
                    PersistenceToken::new(3),
                    vec![
                        PersistenceOp::Append(vec![LogEntry::new(
                            Term::new(1),
                            LogIndex::new(checkpoint_index),
                            EntryKind::Application(checkpoint_command),
                        )]),
                        PersistenceOp::AdvanceCommit {
                            to: LogIndex::new(checkpoint_index),
                        },
                    ],
                ),
                3,
            )
            .unwrap();
        let snapshot = SnapshotMetadata::new(
            LogIndex::new(checkpoint_index),
            Term::new(1),
            config(),
            SnapshotId::new([0xf8; 16]),
        )
        .unwrap();
        adapter
            .execute(
                &PersistenceBatch::new(
                    PersistenceToken::new(4),
                    vec![PersistenceOp::InstallSnapshot(snapshot)],
                ),
                4,
            )
            .unwrap();

        let plan = adapter
            .plan_keyed_compaction(88, CompactionLimits::default())
            .unwrap();
        assert_eq!(plan.source_file_names.len(), 1);
        let outcome = adapter.execute_keyed_compaction(&plan, 88).unwrap();
        assert!(outcome.replaced_segments > 0);
        adapter.discard_producer_snapshot().unwrap();
        assert!(!adapter.producer_snapshot_path().exists());

        let states = adapter.recover_committed_producer_states().unwrap();
        assert_eq!(states.len(), crate::MAX_PARTITION_PRODUCER_ENTRIES);
        for producer_id in [1_u64, producer_count / 2, producer_count] {
            let state = &states[&ProducerId::new(producer_id).unwrap()];
            assert_eq!(state.accepted_epoch(), ProducerEpoch::new(1).unwrap());
            assert_eq!(state.next_sequence(), ProducerSequence::new(1));
            let retry = state.recent_committed_batches().back().unwrap();
            assert_eq!(retry.base_stream_offset, StreamOffset::new(0));
            assert_eq!(retry.last_stream_offset, StreamOffset::new(0));
            assert_eq!(retry.raft_index, RaftIndex::new(1).unwrap());
        }
        assert!(
            adapter
                .log()
                .read_frames()
                .unwrap()
                .iter()
                .all(|frame| { !matches!(frame, Frame::Data(batch) if batch.producer.is_some()) })
        );
        drop(adapter);

        let (reopened, _) = PartitionPersistence::open(
            &dir,
            topic_id,
            PartitionId::new(0),
            4,
            storage_config,
            config(),
            CommandLimits::default(),
        )
        .unwrap();
        assert_eq!(
            reopened.recover_committed_producer_states().unwrap().len(),
            crate::MAX_PARTITION_PRODUCER_ENTRIES
        );
        fs::remove_dir_all(dir).unwrap();
    }

    #[test]
    fn p7_mixed_v1_v2_persistence_recovers_exact_commands_offsets_and_producer_state() {
        let dir = temp_dir("p7-mixed-v1-v2");
        let entries = vec![
            application(1, 0, b"legacy-a"),
            idempotent_application(2, 1, 1, 0, b"p7-a"),
            application(3, 2, b"legacy-b"),
            idempotent_application(4, 3, 1, 1, b"p7-b"),
        ];
        {
            let (mut adapter, _) = open(&dir);
            adapter
                .execute(
                    &PersistenceBatch::new(
                        PersistenceToken::new(1),
                        vec![
                            PersistenceOp::UpdateHardState(HardState {
                                current_term: Term::new(1),
                                voted_for: Some(node(1)),
                                durable_commit_index: LogIndex::ZERO,
                            }),
                            PersistenceOp::Append(entries.clone()),
                            PersistenceOp::AdvanceCommit {
                                to: LogIndex::new(4),
                            },
                        ],
                    ),
                    0,
                )
                .unwrap();

            let frames = adapter.log().read_frames().unwrap();
            assert_eq!(frames.len(), 4);
            for (ordinal, frame) in frames.iter().enumerate() {
                let Frame::Data(batch) = frame else {
                    panic!("mixed application history must remain data frames");
                };
                assert_eq!(batch.base_stream_offset.get(), ordinal as u64);
                if ordinal == 1 || ordinal == 3 {
                    let producer = batch.producer.as_ref().unwrap();
                    assert_eq!(producer.producer_id, 7);
                    assert_eq!(producer.producer_epoch, 1);
                    assert_eq!(producer.base_sequence, (ordinal / 2) as u32);
                } else {
                    assert!(batch.producer.is_none());
                }
            }
            let records = adapter
                .log()
                .read_records_from(StreamOffset::new(0), 16)
                .unwrap();
            assert_eq!(
                records
                    .iter()
                    .map(|record| record.offset.get())
                    .collect::<Vec<_>>(),
                vec![0, 1, 2, 3]
            );
        }

        let (reopened, recovered) = open(&dir);
        assert_eq!(recovered.log, entries);
        let states = reopened.recover_committed_producer_states().unwrap();
        let state = states.get(&ProducerId::new(7).unwrap()).unwrap();
        assert_eq!(state.accepted_epoch(), ProducerEpoch::new(1).unwrap());
        assert_eq!(state.next_sequence(), ProducerSequence::new(2));
        assert_eq!(state.recent_committed_batches().len(), 2);
        fs::remove_dir_all(dir).unwrap();
    }

    #[test]
    fn p7_producer_snapshot_plus_suffix_missing_corrupt_and_ahead_all_match_full_replay() {
        let dir = temp_dir("p7-producer-snapshot-recovery");
        let (mut adapter, _) = open(&dir);
        let first = (1..=8_u64)
            .map(|index| {
                idempotent_application(
                    index,
                    index - 1,
                    1,
                    u32::try_from(index - 1).unwrap(),
                    &[index as u8],
                )
            })
            .collect::<Vec<_>>();
        adapter
            .execute(
                &PersistenceBatch::new(
                    PersistenceToken::new(1),
                    vec![
                        PersistenceOp::UpdateHardState(HardState {
                            current_term: Term::new(1),
                            voted_for: None,
                            durable_commit_index: LogIndex::ZERO,
                        }),
                        PersistenceOp::Append(first[..2].to_vec()),
                        PersistenceOp::AdvanceCommit {
                            to: LogIndex::new(2),
                        },
                    ],
                ),
                0,
            )
            .unwrap();
        adapter.persist_producer_snapshot().unwrap();
        assert!(adapter.producer_snapshot_path().is_file());

        adapter
            .execute(
                &PersistenceBatch::new(
                    PersistenceToken::new(2),
                    vec![
                        PersistenceOp::Append(first[2..].to_vec()),
                        PersistenceOp::AdvanceCommit {
                            to: LogIndex::new(8),
                        },
                    ],
                ),
                1,
            )
            .unwrap();
        let oracle = adapter.recover_producer_states_full_scan().unwrap();
        let accelerated = adapter.recover_committed_producer_states().unwrap();
        assert_eq!(accelerated, oracle);
        let state = accelerated.get(&ProducerId::new(7).unwrap()).unwrap();
        assert_eq!(state.next_sequence(), ProducerSequence::new(8));
        assert_eq!(state.recent_committed_batches().len(), 5);
        assert_eq!(
            state
                .recent_committed_batches()
                .back()
                .unwrap()
                .base_stream_offset,
            StreamOffset::new(7)
        );

        fs::remove_file(adapter.producer_snapshot_path()).unwrap();
        assert_eq!(adapter.recover_committed_producer_states().unwrap(), oracle);

        fs::write(adapter.producer_snapshot_path(), b"corrupt-derived-state").unwrap();
        assert_eq!(adapter.recover_committed_producer_states().unwrap(), oracle);
        assert!(!adapter.producer_snapshot_path().exists());

        let wrong_identity = ProducerSnapshotV1::new(
            TopicId::new([8; 16]),
            PartitionId::new(0),
            8,
            BTreeMap::new(),
        )
        .unwrap();
        ProducerSnapshotStore::new(&dir)
            .persist(&wrong_identity)
            .unwrap();
        assert_eq!(adapter.recover_committed_producer_states().unwrap(), oracle);
        assert!(!adapter.producer_snapshot_path().exists());

        let ahead = ProducerSnapshotV1::new(
            TopicId::new([0x44; 16]),
            PartitionId::new(0),
            99,
            BTreeMap::new(),
        )
        .unwrap();
        ProducerSnapshotStore::new(&dir).persist(&ahead).unwrap();
        assert_eq!(adapter.recover_committed_producer_states().unwrap(), oracle);
        assert!(!adapter.producer_snapshot_path().exists());
        fs::remove_dir_all(dir).unwrap();
    }

    #[test]
    fn p7_uncommitted_tail_is_ignored_and_conflict_truncation_replaces_tentative_metadata() {
        let dir = temp_dir("p7-uncommitted-truncate");
        let (mut adapter, _) = open(&dir);
        adapter
            .execute(
                &PersistenceBatch::new(
                    PersistenceToken::new(1),
                    vec![
                        PersistenceOp::UpdateHardState(HardState {
                            current_term: Term::new(1),
                            voted_for: None,
                            durable_commit_index: LogIndex::ZERO,
                        }),
                        PersistenceOp::Append(vec![
                            idempotent_application(1, 0, 1, 0, b"committed"),
                            idempotent_application(2, 1, 1, 1, b"tentative"),
                        ]),
                        PersistenceOp::AdvanceCommit {
                            to: LogIndex::FIRST,
                        },
                    ],
                ),
                0,
            )
            .unwrap();

        let states = adapter.recover_committed_producer_states().unwrap();
        let state = states.get(&ProducerId::new(7).unwrap()).unwrap();
        assert_eq!(state.next_sequence(), ProducerSequence::new(1));
        assert_eq!(state.recent_committed_batches().len(), 1);

        adapter
            .execute(
                &PersistenceBatch::new(
                    PersistenceToken::new(2),
                    vec![
                        PersistenceOp::TruncateSuffix {
                            from: LogIndex::new(2),
                        },
                        PersistenceOp::Append(vec![idempotent_application(
                            2,
                            1,
                            1,
                            1,
                            b"replacement",
                        )]),
                        PersistenceOp::AdvanceCommit {
                            to: LogIndex::new(2),
                        },
                    ],
                ),
                1,
            )
            .unwrap();
        let states = adapter.recover_committed_producer_states().unwrap();
        let state = states.get(&ProducerId::new(7).unwrap()).unwrap();
        assert_eq!(state.next_sequence(), ProducerSequence::new(2));
        let latest = state.recent_committed_batches().back().unwrap();
        assert_eq!(latest.base_stream_offset, StreamOffset::new(1));
        let replacement = idempotent_application(2, 1, 1, 1, b"replacement");
        let EntryKind::Application(payload) = replacement.kind else {
            unreachable!();
        };
        let command = decode_application_command_v2(&payload, CommandLimits::default()).unwrap();
        assert_eq!(
            latest.canonical_digest,
            canonical_storage_record_set_digest(&command.records).unwrap()
        );
        fs::remove_dir_all(dir).unwrap();
    }

    #[test]
    fn p7_replica_restart_restores_committed_producer_state_exactly() {
        let dir = temp_dir("p7-restart");
        {
            let (mut adapter, _) = open(&dir);
            adapter
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
                                idempotent_application(1, 0, 1, 0, b"a"),
                                idempotent_application(2, 1, 1, 1, b"b"),
                                idempotent_application(3, 2, 2, 0, b"new-epoch"),
                            ]),
                            PersistenceOp::AdvanceCommit {
                                to: LogIndex::new(3),
                            },
                        ],
                    ),
                    0,
                )
                .unwrap();
        }

        let (reopened, recovered) = open(&dir);
        assert_eq!(recovered.hard_state.durable_commit_index, LogIndex::new(3));
        let states = reopened.recover_committed_producer_states().unwrap();
        let state = states.get(&ProducerId::new(7).unwrap()).unwrap();
        assert_eq!(state.accepted_epoch(), ProducerEpoch::new(2).unwrap());
        assert_eq!(state.next_sequence(), ProducerSequence::new(1));
        assert_eq!(state.recent_committed_batches().len(), 1);
        assert_eq!(
            state
                .recent_committed_batches()
                .front()
                .unwrap()
                .base_stream_offset,
            StreamOffset::new(2)
        );
        fs::remove_dir_all(dir).unwrap();
    }

    #[test]
    fn p7_follower_materialization_is_byte_equivalent_for_idempotent_metadata() {
        let left_dir = temp_dir("p7-follower-left");
        let right_dir = temp_dir("p7-follower-right");
        let entry = idempotent_application(1, 0, 1, 0, b"same");

        for dir in [&left_dir, &right_dir] {
            let (mut adapter, _) = open(dir);
            adapter
                .execute(
                    &PersistenceBatch::new(
                        PersistenceToken::new(1),
                        vec![
                            PersistenceOp::UpdateHardState(HardState {
                                current_term: Term::new(1),
                                voted_for: None,
                                durable_commit_index: LogIndex::ZERO,
                            }),
                            PersistenceOp::Append(vec![entry.clone()]),
                            PersistenceOp::AdvanceCommit {
                                to: LogIndex::FIRST,
                            },
                        ],
                    ),
                    0,
                )
                .unwrap();
        }

        let (left, _) = open(&left_dir);
        let (right, _) = open(&right_dir);
        assert_eq!(
            left.log().read_frames().unwrap(),
            right.log().read_frames().unwrap()
        );
        assert_eq!(
            left.recover_committed_producer_states().unwrap(),
            right.recover_committed_producer_states().unwrap()
        );
        fs::remove_dir_all(left_dir).unwrap();
        fs::remove_dir_all(right_dir).unwrap();
    }

    #[test]
    fn p7_rebootstrap_preserves_committed_producer_history_and_suffix_continues() {
        let source_dir = temp_dir("p7-rebootstrap-source");
        let (mut source, _) = open(&source_dir);
        source
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
                            LogEntry::new(Term::new(1), LogIndex::FIRST, EntryKind::LeaderBarrier),
                            idempotent_application(2, 0, 1, 0, b"a"),
                            idempotent_application(3, 1, 1, 1, b"b"),
                        ]),
                        PersistenceOp::AdvanceCommit {
                            to: LogIndex::new(3),
                        },
                    ],
                ),
                0,
            )
            .unwrap();
        let snapshot = SnapshotMetadata::new(
            LogIndex::new(3),
            Term::new(1),
            config(),
            SnapshotId::new([0x71; 16]),
        )
        .unwrap();
        let package = source.build_rebootstrap_package(&snapshot).unwrap();

        let target_dir = temp_dir("p7-rebootstrap-target");
        let (mut target, _) = open(&target_dir);
        target
            .execute(
                &PersistenceBatch::new(
                    PersistenceToken::new(1),
                    vec![
                        PersistenceOp::UpdateHardState(HardState {
                            current_term: Term::new(2),
                            voted_for: None,
                            durable_commit_index: LogIndex::ZERO,
                        }),
                        PersistenceOp::Append(vec![LogEntry::new(
                            Term::new(1),
                            LogIndex::FIRST,
                            EntryKind::LeaderBarrier,
                        )]),
                        PersistenceOp::AdvanceCommit {
                            to: LogIndex::FIRST,
                        },
                    ],
                ),
                0,
            )
            .unwrap();
        target
            .execute_with_rebootstrap(
                &PersistenceBatch::new(
                    PersistenceToken::new(2),
                    vec![
                        PersistenceOp::InstallSnapshot(snapshot.clone()),
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
                1,
                Some(&package),
            )
            .unwrap();

        let state = target
            .recover_committed_producer_states()
            .unwrap()
            .remove(&ProducerId::new(7).unwrap())
            .unwrap();
        assert_eq!(state.accepted_epoch(), ProducerEpoch::new(1).unwrap());
        assert_eq!(state.next_sequence(), ProducerSequence::new(2));

        target
            .execute(
                &PersistenceBatch::new(
                    PersistenceToken::new(3),
                    vec![
                        PersistenceOp::Append(vec![idempotent_application(4, 2, 1, 2, b"suffix")]),
                        PersistenceOp::AdvanceCommit {
                            to: LogIndex::new(4),
                        },
                    ],
                ),
                2,
            )
            .unwrap();
        let state = target
            .recover_committed_producer_states()
            .unwrap()
            .remove(&ProducerId::new(7).unwrap())
            .unwrap();
        assert_eq!(state.next_sequence(), ProducerSequence::new(3));
        assert_eq!(state.recent_committed_batches().len(), 3);
        fs::remove_dir_all(source_dir).unwrap();
        fs::remove_dir_all(target_dir).unwrap();
    }

    #[test]
    fn p8_compressed_rebootstrap_preserves_codecs_and_committed_logical_history() {
        let source_dir = temp_dir("p8-compressed-rebootstrap-source");
        let (mut source, _) = open(&source_dir);
        let first = idempotent_application(2, 0, 1, 0, &vec![b'L'; 4096]);
        let second = idempotent_application(3, 1, 1, 1, &vec![b'Z'; 4096]);

        source.set_data_compression(Compression::Lz4);
        source
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
                            LogEntry::new(Term::new(1), LogIndex::FIRST, EntryKind::LeaderBarrier),
                            first.clone(),
                        ]),
                    ],
                ),
                0,
            )
            .unwrap();
        source.set_data_compression(Compression::Zstd);
        source
            .execute(
                &PersistenceBatch::new(
                    PersistenceToken::new(2),
                    vec![
                        PersistenceOp::Append(vec![second.clone()]),
                        PersistenceOp::AdvanceCommit {
                            to: LogIndex::new(3),
                        },
                    ],
                ),
                1,
            )
            .unwrap();

        let source_frames = source.log().read_frames().unwrap();
        assert_eq!(source_frames.len(), 3);
        assert!(matches!(source_frames[0], Frame::Control(_)));
        assert_eq!(
            source_frames
                .iter()
                .skip(1)
                .map(|frame| match frame {
                    Frame::Data(batch) => batch.compression,
                    Frame::Control(_) => unreachable!(),
                })
                .collect::<Vec<_>>(),
            vec![Compression::Lz4, Compression::Zstd]
        );

        let snapshot = SnapshotMetadata::new(
            LogIndex::new(3),
            Term::new(1),
            config(),
            SnapshotId::new([0x81; 16]),
        )
        .unwrap();
        let package = source.build_rebootstrap_package(&snapshot).unwrap();
        let encoded = encode_rebootstrap_package(&package, RebootstrapLimits::default()).unwrap();
        let package = decode_rebootstrap_package(&encoded, RebootstrapLimits::default()).unwrap();
        assert_eq!(package.frames, source_frames);

        let target_dir = temp_dir("p8-compressed-rebootstrap-target");
        let (mut target, _) = open(&target_dir);
        target.set_data_compression(Compression::Zstd);
        target
            .execute(
                &PersistenceBatch::new(
                    PersistenceToken::new(1),
                    vec![
                        PersistenceOp::UpdateHardState(HardState {
                            current_term: Term::new(2),
                            voted_for: None,
                            durable_commit_index: LogIndex::ZERO,
                        }),
                        PersistenceOp::Append(vec![LogEntry::new(
                            Term::new(1),
                            LogIndex::FIRST,
                            EntryKind::LeaderBarrier,
                        )]),
                        PersistenceOp::AdvanceCommit {
                            to: LogIndex::FIRST,
                        },
                    ],
                ),
                0,
            )
            .unwrap();
        target
            .execute_with_rebootstrap(
                &PersistenceBatch::new(
                    PersistenceToken::new(2),
                    vec![
                        PersistenceOp::InstallSnapshot(snapshot.clone()),
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
                2,
                Some(&package),
            )
            .unwrap();

        assert_eq!(target.log().read_frames().unwrap(), source_frames);
        let recovered = target.recover_committed_producer_states().unwrap();
        let state = recovered.get(&ProducerId::new(7).unwrap()).unwrap();
        assert_eq!(state.accepted_epoch(), ProducerEpoch::new(1).unwrap());
        assert_eq!(state.next_sequence(), ProducerSequence::new(2));
        assert_eq!(state.recent_committed_batches().len(), 2);

        target
            .execute(
                &PersistenceBatch::new(
                    PersistenceToken::new(3),
                    vec![
                        PersistenceOp::Append(vec![idempotent_application(
                            4,
                            2,
                            1,
                            2,
                            &vec![b'S'; 4096],
                        )]),
                        PersistenceOp::AdvanceCommit {
                            to: LogIndex::new(4),
                        },
                    ],
                ),
                3,
            )
            .unwrap();
        let frames_after_suffix = target.log().read_frames().unwrap();
        let Frame::Data(suffix) = frames_after_suffix.last().unwrap() else {
            unreachable!()
        };
        assert_eq!(suffix.compression, Compression::Zstd);
        assert_eq!(
            target
                .recover_committed_producer_states()
                .unwrap()
                .get(&ProducerId::new(7).unwrap())
                .unwrap()
                .next_sequence(),
            ProducerSequence::new(3)
        );

        drop(target);
        let (reopened, _) = open(&target_dir);
        assert_eq!(reopened.log().read_frames().unwrap(), frames_after_suffix);
        assert_eq!(
            reopened
                .recover_committed_producer_states()
                .unwrap()
                .get(&ProducerId::new(7).unwrap())
                .unwrap()
                .next_sequence(),
            ProducerSequence::new(3)
        );
        fs::remove_dir_all(source_dir).unwrap();
        fs::remove_dir_all(target_dir).unwrap();
    }

    #[test]
    fn p8_compressed_history_reconstructs_identical_p7_producer_state_as_none() {
        fn persist_history(
            dir: &Path,
            codecs: [Compression; 2],
        ) -> BTreeMap<ProducerId, PartitionProducerStateV1> {
            let (mut adapter, _) = open(dir);
            let entries = [
                idempotent_application(1, 0, 1, 0, &vec![b'A'; 4096]),
                idempotent_application(2, 1, 1, 1, &vec![b'B'; 4096]),
            ];
            adapter.set_data_compression(codecs[0]);
            adapter
                .execute(
                    &PersistenceBatch::new(
                        PersistenceToken::new(1),
                        vec![
                            PersistenceOp::UpdateHardState(HardState {
                                current_term: Term::new(1),
                                voted_for: None,
                                durable_commit_index: LogIndex::ZERO,
                            }),
                            PersistenceOp::Append(vec![entries[0].clone()]),
                        ],
                    ),
                    0,
                )
                .unwrap();
            adapter.set_data_compression(codecs[1]);
            adapter
                .execute(
                    &PersistenceBatch::new(
                        PersistenceToken::new(2),
                        vec![
                            PersistenceOp::Append(vec![entries[1].clone()]),
                            PersistenceOp::AdvanceCommit {
                                to: LogIndex::new(2),
                            },
                        ],
                    ),
                    1,
                )
                .unwrap();
            adapter.recover_committed_producer_states().unwrap()
        }

        let compressed_dir = temp_dir("p8-producer-compressed");
        let none_dir = temp_dir("p8-producer-none");
        let compressed = persist_history(&compressed_dir, [Compression::Lz4, Compression::Zstd]);
        let uncompressed = persist_history(&none_dir, [Compression::None, Compression::None]);
        assert_eq!(compressed, uncompressed);

        let state = compressed.get(&ProducerId::new(7).unwrap()).unwrap();
        assert_eq!(state.accepted_epoch(), ProducerEpoch::new(1).unwrap());
        assert_eq!(state.next_sequence(), ProducerSequence::new(2));
        assert_eq!(state.recent_committed_batches().len(), 2);
        assert_eq!(
            state
                .recent_committed_batches()
                .front()
                .unwrap()
                .base_stream_offset,
            StreamOffset::new(0)
        );
        assert_eq!(
            state
                .recent_committed_batches()
                .back()
                .unwrap()
                .base_stream_offset,
            StreamOffset::new(1)
        );

        let (reopened, recovered) = open(&compressed_dir);
        assert_eq!(recovered.hard_state.durable_commit_index, LogIndex::new(2));
        assert_eq!(
            reopened.recover_committed_producer_states().unwrap(),
            uncompressed
        );
        let codecs = reopened
            .log()
            .read_frames()
            .unwrap()
            .into_iter()
            .map(|frame| match frame {
                Frame::Data(batch) => batch.compression,
                Frame::Control(_) => unreachable!(),
            })
            .collect::<Vec<_>>();
        assert_eq!(codecs, vec![Compression::Lz4, Compression::Zstd]);

        fs::remove_dir_all(compressed_dir).unwrap();
        fs::remove_dir_all(none_dir).unwrap();
    }

    #[test]
    fn adapter_materializes_and_recovers_application_and_control_entries() {
        let dir = temp_dir("roundtrip");
        let (mut adapter, _) = open(&dir);
        let new = StableConfiguration::new([node(1), node(2), node(3), node(4)], []).unwrap();
        let entries = vec![
            LogEntry::new(Term::new(1), LogIndex::new(1), EntryKind::LeaderBarrier),
            application(2, 0, b"value"),
            LogEntry::new(
                Term::new(1),
                LogIndex::new(3),
                EntryKind::Configuration(ConfigurationChange::EnterJoint { old: config(), new }),
            ),
        ];
        adapter
            .execute(
                &PersistenceBatch::new(
                    PersistenceToken::new(1),
                    vec![
                        PersistenceOp::UpdateHardState(HardState {
                            current_term: Term::new(1),
                            voted_for: Some(node(1)),
                            durable_commit_index: LogIndex::ZERO,
                        }),
                        PersistenceOp::Append(entries.clone()),
                        PersistenceOp::AdvanceCommit {
                            to: LogIndex::new(2),
                        },
                    ],
                ),
                1,
            )
            .unwrap();
        assert_eq!(adapter.committed_stream_high_watermark().unwrap().get(), 1);
        drop(adapter);

        let (_reopened, recovered) = open(&dir);
        assert_eq!(recovered.log, entries);
        assert_eq!(recovered.hard_state.durable_commit_index, LogIndex::new(2));
        fs::remove_dir_all(dir).unwrap();
    }

    #[test]
    fn p9c_v2_retained_history_is_fetchable_but_never_recovered_as_live_p3_log() {
        let dir = temp_dir("p9c-v2-retained-p3-boundary");
        let (mut adapter, _) = open(&dir);
        adapter
            .execute(
                &PersistenceBatch::new(
                    PersistenceToken::new(1),
                    vec![
                        PersistenceOp::UpdateHardState(HardState {
                            current_term: Term::new(1),
                            voted_for: Some(node(1)),
                            durable_commit_index: LogIndex::ZERO,
                        }),
                        PersistenceOp::Append(vec![application(1, 0, b"retained")]),
                        PersistenceOp::AdvanceCommit {
                            to: LogIndex::FIRST,
                        },
                    ],
                ),
                0,
            )
            .unwrap();
        let snapshot = SnapshotMetadata::new(
            LogIndex::FIRST,
            Term::new(1),
            config(),
            SnapshotId::new([0x9c; 16]),
        )
        .unwrap();
        adapter
            .execute(
                &PersistenceBatch::new(
                    PersistenceToken::new(2),
                    vec![PersistenceOp::InstallSnapshot(snapshot.clone())],
                ),
                1,
            )
            .unwrap();
        adapter.log_mut().roll(2).unwrap();
        adapter.log_mut().adopt_manifest_inventory().unwrap();
        let active_name = adapter
            .log()
            .segment_paths()
            .last()
            .unwrap()
            .file_name()
            .unwrap()
            .to_str()
            .unwrap()
            .to_owned();
        drop(adapter);

        let topic_id = TopicId::new([0x44; 16]);
        let retained_header = RetainedSegmentHeaderV2 {
            topic_id,
            partition_id: PartitionId::new(0),
            generation: 1,
            base_stream_offset: SegmentBaseOffset::new(0),
            base_raft_index: RaftIndex::FIRST,
            created_at_ms: 2,
        };
        let retained_frame = RetainedFrameV2::SparseData(SparseRetainedBatchV2 {
            raft_term: RaftTerm::new(1),
            raft_index: RaftIndex::FIRST,
            compression: Compression::Zstd,
            records: vec![SparseRetainedRecordV2 {
                offset: StreamOffset::new(0),
                record: DataRecord {
                    timestamp_ms: 1,
                    key: Some(b"k".to_vec()),
                    value: Some(b"retained".to_vec()),
                    headers: vec![UserHeader {
                        name: b"h".to_vec(),
                        value: None,
                    }],
                },
            }],
        });
        let mut retained_bytes = encode_retained_segment_header_v2(&retained_header).to_vec();
        retained_bytes.extend_from_slice(
            &encode_retained_frame_v2(&retained_frame, StorageLimits::default()).unwrap(),
        );
        let retained_name =
            SegmentFileId::from_header(&retained_header.identity_header()).log_file_name();
        let mut generation = GenerationTransaction::new(&dir, 2).unwrap();
        generation
            .stage_file(retained_name, &retained_bytes, false)
            .unwrap();
        generation
            .retain(ManifestEntry::mutable_active(active_name))
            .unwrap();
        generation.commit().unwrap();

        let (reopened, recovered) = open(&dir);
        assert_eq!(recovered.snapshot, snapshot);
        assert!(
            recovered.log.is_empty(),
            "V2 retained frames below the snapshot floor must never be presented to P3 as live log entries",
        );
        let records = reopened
            .log()
            .read_committed_records_from_bounded(StreamOffset::new(0), 10, usize::MAX)
            .unwrap()
            .0;
        assert_eq!(records.len(), 1);
        assert_eq!(records[0].offset, StreamOffset::new(0));
        assert_eq!(records[0].record.value.as_deref(), Some(&b"retained"[..]));
        fs::remove_dir_all(dir).unwrap();
    }

    #[test]
    fn unexplained_hardstate_commit_mismatch_is_fatal() {
        let dir = temp_dir("mismatch");
        let (mut adapter, _) = open(&dir);
        adapter
            .execute(
                &PersistenceBatch::new(
                    PersistenceToken::new(1),
                    vec![
                        PersistenceOp::UpdateHardState(HardState {
                            current_term: Term::new(1),
                            voted_for: Some(node(1)),
                            durable_commit_index: LogIndex::ZERO,
                        }),
                        PersistenceOp::Append(vec![application(1, 0, b"value")]),
                    ],
                ),
                0,
            )
            .unwrap();
        adapter.log_mut().mark_committed(1).unwrap();
        drop(adapter);
        assert!(
            PartitionPersistence::open(
                &dir,
                TopicId::new([0x44; 16]),
                PartitionId::new(0),
                1,
                PartitionLogConfig::default(),
                config(),
                CommandLimits::default(),
            )
            .is_err()
        );
        fs::remove_dir_all(dir).unwrap();
    }

    #[test]
    fn durable_commit_intent_completes_interrupted_cross_file_commit() {
        let dir = temp_dir("intent");
        let (mut adapter, _) = open(&dir);
        adapter
            .execute(
                &PersistenceBatch::new(
                    PersistenceToken::new(1),
                    vec![
                        PersistenceOp::UpdateHardState(HardState {
                            current_term: Term::new(1),
                            voted_for: Some(node(1)),
                            durable_commit_index: LogIndex::ZERO,
                        }),
                        PersistenceOp::Append(vec![application(1, 0, b"value")]),
                    ],
                ),
                0,
            )
            .unwrap();
        adapter
            .commit_intent_store
            .persist(CommitIntent {
                previous: LogIndex::ZERO,
                target: LogIndex::FIRST,
            })
            .unwrap();
        adapter.log_mut().mark_committed(1).unwrap();
        drop(adapter);

        let (recovered_adapter, recovered) = open(&dir);
        assert_eq!(recovered.hard_state.durable_commit_index, LogIndex::FIRST);
        assert_eq!(recovered_adapter.log().committed_raft_index(), 1);
        assert!(
            recovered_adapter
                .commit_intent_store
                .load()
                .unwrap()
                .is_none()
        );
        fs::remove_dir_all(dir).unwrap();
    }

    #[test]
    fn reopen_refsyncs_recovered_uncommitted_tail_before_raft_recovery() {
        let dir = temp_dir("reopen-refsync-tail");
        {
            let (mut adapter, _) = open(&dir);
            adapter
                .execute(
                    &PersistenceBatch::new(
                        PersistenceToken::new(1),
                        vec![
                            PersistenceOp::UpdateHardState(HardState {
                                current_term: Term::new(1),
                                voted_for: None,
                                durable_commit_index: LogIndex::ZERO,
                            }),
                            PersistenceOp::Append(vec![application(1, 0, b"tail")]),
                        ],
                    ),
                    0,
                )
                .unwrap();
            assert_eq!(adapter.log().durable_raft_index(), 1);
            assert_eq!(adapter.log().committed_raft_index(), 0);
        }

        let (mut reopened, recovered) = open(&dir);
        assert_eq!(recovered.log.len(), 1);
        assert_eq!(reopened.log().durable_raft_index(), 1);
        assert_eq!(reopened.log().committed_raft_index(), 0);
        reopened
            .execute(
                &PersistenceBatch::new(
                    PersistenceToken::new(2),
                    vec![PersistenceOp::AdvanceCommit {
                        to: LogIndex::FIRST,
                    }],
                ),
                1,
            )
            .unwrap();
        assert_eq!(reopened.log().committed_raft_index(), 1);
        assert_eq!(reopened.hard_state.durable_commit_index, LogIndex::FIRST);
        fs::remove_dir_all(dir).unwrap();
    }

    #[test]
    fn adapter_truncates_only_uncommitted_suffix_and_reuses_stream_offset() {
        let dir = temp_dir("truncate");
        let (mut adapter, _) = open(&dir);
        adapter
            .execute(
                &PersistenceBatch::new(
                    PersistenceToken::new(1),
                    vec![
                        PersistenceOp::UpdateHardState(HardState {
                            current_term: Term::new(1),
                            voted_for: None,
                            durable_commit_index: LogIndex::ZERO,
                        }),
                        PersistenceOp::Append(vec![
                            application(1, 0, b"a"),
                            application(2, 1, b"b"),
                            application(3, 2, b"stale"),
                        ]),
                        PersistenceOp::AdvanceCommit {
                            to: LogIndex::new(2),
                        },
                    ],
                ),
                0,
            )
            .unwrap();
        adapter
            .execute(
                &PersistenceBatch::new(
                    PersistenceToken::new(2),
                    vec![
                        PersistenceOp::TruncateSuffix {
                            from: LogIndex::new(3),
                        },
                        PersistenceOp::Append(vec![application(3, 2, b"replacement")]),
                    ],
                ),
                1,
            )
            .unwrap();
        let values = adapter
            .log()
            .read_records_from(StreamOffset::new(0), 10)
            .unwrap();
        assert_eq!(values.len(), 3);
        assert_eq!(
            values[2].record.value.as_deref(),
            Some(b"replacement".as_slice())
        );
        fs::remove_dir_all(dir).unwrap();
    }

    #[test]
    fn authorized_rebootstrap_atomically_replaces_partition_and_suffix_continues() {
        let source_dir = temp_dir("rebootstrap-source");
        let (mut source, _) = open(&source_dir);
        source
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
                            LogEntry::new(Term::new(1), LogIndex::FIRST, EntryKind::LeaderBarrier),
                            application(2, 0, b"snap-a"),
                            application(3, 1, b"snap-b"),
                        ]),
                        PersistenceOp::AdvanceCommit {
                            to: LogIndex::new(3),
                        },
                    ],
                ),
                0,
            )
            .unwrap();
        let snapshot = SnapshotMetadata::new(
            LogIndex::new(3),
            Term::new(1),
            config(),
            SnapshotId::new([0x33; 16]),
        )
        .unwrap();
        let package = source.build_rebootstrap_package(&snapshot).unwrap();

        let target_dir = temp_dir("rebootstrap-target");
        let (mut target, _) = open(&target_dir);
        target
            .execute(
                &PersistenceBatch::new(
                    PersistenceToken::new(1),
                    vec![
                        PersistenceOp::UpdateHardState(HardState {
                            current_term: Term::new(2),
                            voted_for: None,
                            durable_commit_index: LogIndex::ZERO,
                        }),
                        PersistenceOp::Append(vec![LogEntry::new(
                            Term::new(1),
                            LogIndex::FIRST,
                            EntryKind::LeaderBarrier,
                        )]),
                        PersistenceOp::AdvanceCommit {
                            to: LogIndex::FIRST,
                        },
                    ],
                ),
                0,
            )
            .unwrap();

        target
            .execute_with_rebootstrap(
                &PersistenceBatch::new(
                    PersistenceToken::new(2),
                    vec![
                        PersistenceOp::InstallSnapshot(snapshot.clone()),
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
                1,
                Some(&package),
            )
            .unwrap();

        assert_eq!(target.snapshot(), &snapshot);
        assert_eq!(target.hard_state().durable_commit_index, LogIndex::new(3));
        assert_eq!(target.log().committed_raft_index(), 3);
        assert_eq!(target.committed_stream_high_watermark().unwrap().get(), 2);
        assert!(!rebootstrap_backup_path(&target_dir).exists());
        assert!(!rebootstrap_intent_path(&target_dir).exists());
        assert!(!rebootstrap_staging_path(&target_dir).exists());

        // Ordinary suffix replication must continue at snapshot + 1 without
        // special rebootstrap handling.
        target
            .execute(
                &PersistenceBatch::new(
                    PersistenceToken::new(3),
                    vec![
                        PersistenceOp::Append(vec![application(4, 2, b"suffix")]),
                        PersistenceOp::AdvanceCommit {
                            to: LogIndex::new(4),
                        },
                    ],
                ),
                2,
            )
            .unwrap();
        assert_eq!(target.committed_stream_high_watermark().unwrap().get(), 3);
        assert_eq!(
            target
                .log()
                .read_records_from(StreamOffset::new(0), 10)
                .unwrap()
                .len(),
            3
        );

        drop(source);
        drop(target);
        fs::remove_dir_all(source_dir).unwrap();
        fs::remove_dir_all(target_dir).unwrap();
    }

    // The source/receiver fixture is intentionally separate from direct
    // in-memory P3 tests. This crosses real authenticated RF3 peer TCP frames
    // with exact ASTRBTP2 bytes while preserving the sparse V2 source layout.
    fn p9g_rf3_send_sparse_v2_via_authenticated_peers(
        package: &RebootstrapPackageV2,
    ) -> RebootstrapPackageV2 {
        use crate::{
            GroupKey, PeerDescriptor, PeerTransportConfig, TransportLimits, start_peer_transport,
        };
        use astralog_raft::{InstallSnapshotRequest, RaftMessage};
        use std::{net::Ipv4Addr, time::Duration};
        let addresses = std::array::from_fn::<_, 3, _>(|_| {
            let listener = std::net::TcpListener::bind((Ipv4Addr::LOCALHOST, 0)).unwrap();
            let address = listener.local_addr().unwrap();
            drop(listener);
            address
        });
        let peers = (0..3_u32)
            .map(|id| PeerDescriptor {
                broker_id: id,
                incarnation: u64::from(id) + 41,
                address: addresses[id as usize],
            })
            .collect::<Vec<_>>();
        let group = GroupKey {
            topic_id: package.topic_id,
            partition: package.partition_id.get(),
        };
        let cfg = |id: u32| PeerTransportConfig {
            local_broker_id: id,
            local_incarnation: u64::from(id) + 41,
            bind_addr: addresses[id as usize],
            peers: peers
                .iter()
                .filter(|peer| peer.broker_id != id)
                .map(|peer| (peer.broker_id, *peer))
                .collect(),
            allowed_groups: [group].into_iter().collect(),
            limits: TransportLimits::default(),
            max_inbound_frames: 16,
            max_outbound_frames_per_peer: 16,
            max_outbound_bytes_per_peer: 1024 * 1024,
            reconnect_min_delay: Duration::from_millis(5),
            reconnect_max_delay: Duration::from_millis(20),
        };
        let bytes =
            crate::encode_rebootstrap_package_v2(package, RebootstrapLimitsV2::default()).unwrap();
        assert!(bytes.starts_with(b"ASTRBTP2"));
        let request = RaftMessage::InstallSnapshotRequest(InstallSnapshotRequest {
            term: Term::new(2),
            leader_id: node(1),
            snapshot: package.snapshot.clone(),
        });
        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        rt.block_on(async {
            let (source, _rx_source) = start_peer_transport(cfg(0)).await.unwrap();
            let (receiver, mut rx_receiver) = start_peer_transport(cfg(1)).await.unwrap();
            let (third, _rx_third) = start_peer_transport(cfg(2)).await.unwrap();
            source
                .sender()
                .try_send_rebootstrap(1, group, request.clone(), bytes)
                .unwrap();
            let inbound = tokio::time::timeout(Duration::from_secs(3), rx_receiver.recv())
                .await
                .unwrap()
                .unwrap();
            assert!(inbound.connection_generation > 0);
            assert_eq!(inbound.frame.source_broker_id, 0);
            assert_eq!(inbound.frame.source_incarnation, 41);
            assert_eq!(inbound.frame.message, Some(request));
            assert_eq!(inbound.frame.topic_id, group.topic_id);
            assert_eq!(inbound.frame.partition, group.partition);
            let on_wire = inbound.frame.rebootstrap.unwrap();
            let recovered =
                crate::decode_rebootstrap_package_v2(&on_wire, RebootstrapLimitsV2::default())
                    .unwrap();
            source.shutdown().await;
            receiver.shutdown().await;
            third.shutdown().await;
            recovered
        })
    }

    fn copy_test_directory(source: &Path, destination: &Path) {
        fs::create_dir_all(destination).unwrap();
        for entry in fs::read_dir(source).unwrap() {
            let entry = entry.unwrap();
            let source_path = entry.path();
            let destination_path = destination.join(entry.file_name());
            let file_type = entry.file_type().unwrap();
            if file_type.is_dir() {
                copy_test_directory(&source_path, &destination_path);
            } else {
                assert!(file_type.is_file());
                fs::copy(source_path, destination_path).unwrap();
            }
        }
    }

    fn p9g_sparse_producer_package_with_acceleration(
        label: &str,
    ) -> (PathBuf, SnapshotMetadata, RebootstrapPackageV2) {
        let source_dir = temp_dir(label);
        let (mut source, _) = open(&source_dir);
        source
            .execute(
                &PersistenceBatch::new(
                    PersistenceToken::new(1),
                    vec![
                        PersistenceOp::UpdateHardState(HardState {
                            current_term: Term::new(2),
                            voted_for: Some(node(1)),
                            durable_commit_index: LogIndex::ZERO,
                        }),
                        PersistenceOp::Append(vec![idempotent_application(
                            1,
                            0,
                            1,
                            0,
                            b"producer-before-checkpoint",
                        )]),
                        PersistenceOp::AdvanceCommit {
                            to: LogIndex::FIRST,
                        },
                    ],
                ),
                0,
            )
            .unwrap();
        source.log_mut().roll(2).unwrap();
        source
            .execute(
                &PersistenceBatch::new(
                    PersistenceToken::new(2),
                    vec![
                        PersistenceOp::Append(vec![application(2, 1, b"retained-winner")]),
                        PersistenceOp::AdvanceCommit {
                            to: LogIndex::new(2),
                        },
                    ],
                ),
                2,
            )
            .unwrap();
        let expected = source.recover_committed_producer_states().unwrap();
        assert_eq!(expected.len(), 1);
        let checkpoint = source.producer_checkpoint_candidate().unwrap().unwrap();
        assert_eq!(checkpoint.committed_raft_index, 2);
        let encoded = encode_application_producer_state_checkpoint_command_v1(
            &ApplicationProducerStateCheckpointCommandV1 { checkpoint },
            CommandLimits::default(),
        )
        .unwrap();
        source
            .execute(
                &PersistenceBatch::new(
                    PersistenceToken::new(3),
                    vec![
                        PersistenceOp::Append(vec![LogEntry::new(
                            Term::new(1),
                            LogIndex::new(3),
                            EntryKind::Application(encoded),
                        )]),
                        PersistenceOp::AdvanceCommit {
                            to: LogIndex::new(3),
                        },
                    ],
                ),
                3,
            )
            .unwrap();
        let snapshot = SnapshotMetadata::new(
            LogIndex::new(3),
            Term::new(1),
            config(),
            SnapshotId::new([0xc7; 16]),
        )
        .unwrap();
        source
            .execute(
                &PersistenceBatch::new(
                    PersistenceToken::new(4),
                    vec![PersistenceOp::InstallSnapshot(snapshot.clone())],
                ),
                4,
            )
            .unwrap();
        let plan = source
            .plan_keyed_compaction(107, CompactionLimits::default())
            .unwrap();
        assert!(!plan.is_empty());
        source.execute_keyed_compaction(&plan, 107).unwrap();
        assert!(
            source
                .log()
                .read_frames()
                .unwrap()
                .iter()
                .all(|frame| !matches!(frame, Frame::Data(batch) if batch.producer.is_some()))
        );
        source.log_mut().roll(5).unwrap();
        source.log_mut().adopt_manifest_inventory().unwrap();
        let mut package = source
            .build_rebootstrap_package_v2(&snapshot, RebootstrapLimitsV2::default())
            .unwrap();
        assert!(package.producer_checkpoint.is_none());
        package.producer_checkpoint = Some(
            ProducerSnapshotV1::new(
                TopicId::new([0x44; 16]),
                PartitionId::new(0),
                snapshot.last_included_index.get(),
                expected,
            )
            .unwrap(),
        );
        let bytes =
            crate::encode_rebootstrap_package_v2(&package, RebootstrapLimitsV2::default()).unwrap();
        let package =
            crate::decode_rebootstrap_package_v2(&bytes, RebootstrapLimitsV2::default()).unwrap();
        drop(source);
        (source_dir, snapshot, package)
    }

    #[test]
    fn p9g_v2_rebootstrap_installs_sparse_manifest_under_p3_and_catches_up_live_suffix() {
        let source_dir = temp_dir("p9g-v2-sparse-source");
        let (mut source, _) = open(&source_dir);
        source
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
                            application(1, 0, b"obsolete"),
                            application(2, 1, b"current"),
                        ]),
                        PersistenceOp::AdvanceCommit {
                            to: LogIndex::new(2),
                        },
                    ],
                ),
                0,
            )
            .unwrap();
        let snapshot = SnapshotMetadata::new(
            LogIndex::new(2),
            Term::new(1),
            config(),
            SnapshotId::new([0xa1; 16]),
        )
        .unwrap();
        source
            .execute(
                &PersistenceBatch::new(
                    PersistenceToken::new(2),
                    vec![PersistenceOp::InstallSnapshot(snapshot.clone())],
                ),
                1,
            )
            .unwrap();
        source.log_mut().roll(2).unwrap();
        source.log_mut().adopt_manifest_inventory().unwrap();
        let active_name = source
            .log()
            .segment_paths()
            .last()
            .unwrap()
            .file_name()
            .unwrap()
            .to_str()
            .unwrap()
            .to_owned();
        drop(source);

        let header = RetainedSegmentHeaderV2 {
            topic_id: TopicId::new([0x44; 16]),
            partition_id: PartitionId::new(0),
            generation: 1,
            base_stream_offset: SegmentBaseOffset::new(0),
            base_raft_index: RaftIndex::FIRST,
            created_at_ms: 0,
        };
        let retained = RetainedFrameV2::SparseData(SparseRetainedBatchV2 {
            raft_term: RaftTerm::new(1),
            raft_index: RaftIndex::new(2).unwrap(),
            compression: Compression::None,
            records: vec![SparseRetainedRecordV2 {
                offset: StreamOffset::new(1),
                record: DataRecord {
                    timestamp_ms: 1,
                    key: Some(b"k".to_vec()),
                    value: Some(b"current".to_vec()),
                    headers: vec![UserHeader {
                        name: b"h".to_vec(),
                        value: None,
                    }],
                },
            }],
        });
        let mut retained_bytes = encode_retained_segment_header_v2(&header).to_vec();
        retained_bytes.extend_from_slice(
            &encode_retained_frame_v2(&retained, StorageLimits::default()).unwrap(),
        );
        let retained_name = SegmentFileId::from_header(&header.identity_header()).log_file_name();
        let mut generation = GenerationTransaction::new(&source_dir, 2).unwrap();
        generation
            .stage_file(retained_name, &retained_bytes, false)
            .unwrap();
        generation
            .retain(ManifestEntry::mutable_active(active_name))
            .unwrap();
        generation.commit().unwrap();

        let (source, _) = open(&source_dir);
        let limits = RebootstrapLimitsV2::default();
        let package = source
            .build_rebootstrap_package_v2(&snapshot, limits)
            .unwrap();
        let encoded = crate::encode_rebootstrap_package_v2(&package, limits).unwrap();
        let package = crate::decode_rebootstrap_package_v2(&encoded, limits).unwrap();
        assert_eq!(package.manifest.generation, 2);
        assert_eq!(package.files.len(), 2);
        assert!(
            package.files.iter().any(|file| file.bytes.len() >= 10
                && file.bytes.starts_with(b"ASTRALOG")
                && file.bytes[8..10] == 2_u16.to_le_bytes()),
            "retained V2 sparse source must cross the network, not only its V1 suffix"
        );
        let received = p9g_rf3_send_sparse_v2_via_authenticated_peers(&package);
        assert_eq!(received, package);
        let package = received;
        let target_dir = temp_dir("p9g-v2-sparse-target");
        let (mut target, _) = open(&target_dir);
        target
            .execute(
                &PersistenceBatch::new(
                    PersistenceToken::new(2),
                    vec![PersistenceOp::UpdateHardState(HardState {
                        current_term: Term::new(2),
                        voted_for: None,
                        durable_commit_index: LogIndex::ZERO,
                    })],
                ),
                2,
            )
            .unwrap();
        // A package with a mismatched P3 snapshot must be rejected without
        // changing the original empty authoritative partition.
        let other = SnapshotMetadata::new(
            LogIndex::new(2),
            Term::new(1),
            config(),
            SnapshotId::new([0xee; 16]),
        )
        .unwrap();
        let batch = PersistenceBatch::new(
            PersistenceToken::new(3),
            vec![PersistenceOp::InstallSnapshot(other)],
        );
        assert!(
            target
                .execute_with_rebootstrap_versioned(&batch, 3, None, Some(&package))
                .is_err()
        );
        assert_eq!(target.log().committed_raft_index(), 0);

        // A package with self-consistent SHA/CRC but damaged Storage V2
        // bytes must fail staged semantic validation, leaving old authority
        // untouched. A later valid package may safely replace the stage.
        let mut forged_storage = package.clone();
        forged_storage.files[0].bytes[0] ^= 0x33;
        assert!(
            target
                .execute_with_rebootstrap_versioned(
                    &PersistenceBatch::new(
                        PersistenceToken::new(5),
                        vec![PersistenceOp::InstallSnapshot(snapshot.clone()),]
                    ),
                    3,
                    None,
                    Some(&forged_storage),
                )
                .is_err()
        );
        assert_eq!(target.log().committed_raft_index(), 0);
        assert!(!rebootstrap_intent_path(&target_dir).exists());
        // Simulate a crash with only partially validated V2 files staged,
        // before the publication intent exists. Startup must discard the
        // abandoned retained generation and reopen the old local partition.
        assert!(rebootstrap_staging_path(&target_dir).exists());
        drop(target);
        let (target, old_state) = open(&target_dir);
        assert_eq!(old_state.hard_state.durable_commit_index, LogIndex::ZERO);
        assert_eq!(target.log().committed_raft_index(), 0);
        assert!(!rebootstrap_staging_path(&target_dir).exists());
        // Simulate the next crash point: V2 staged, intent durably present,
        // old generation already moved to backup, new generation not yet
        // published. Recovery must restore the old authoritative partition.
        let incomplete_stage = rebootstrap_staging_path(&target_dir);
        fs::create_dir_all(&incomplete_stage).unwrap();
        fs::write(
            incomplete_stage.join(&package.files[0].file_name),
            &package.files[0].bytes,
        )
        .unwrap();
        atomic_write(&rebootstrap_intent_path(&target_dir), b"ASTRSWP1").unwrap();
        drop(target);
        fs::rename(&target_dir, rebootstrap_backup_path(&target_dir)).unwrap();
        let (mut target, recovered_old) = open(&target_dir);
        assert_eq!(
            recovered_old.hard_state.durable_commit_index,
            LogIndex::ZERO
        );
        assert_eq!(target.log().committed_raft_index(), 0);
        assert!(!rebootstrap_backup_path(&target_dir).exists());
        assert!(!rebootstrap_staging_path(&target_dir).exists());
        assert!(!rebootstrap_intent_path(&target_dir).exists());

        target
            .execute_with_rebootstrap_versioned(
                &PersistenceBatch::new(
                    PersistenceToken::new(4),
                    vec![
                        PersistenceOp::InstallSnapshot(snapshot.clone()),
                        PersistenceOp::TruncateSuffix {
                            from: LogIndex::new(3),
                        },
                        PersistenceOp::DiscardPrefix {
                            through: LogIndex::new(2),
                        },
                        PersistenceOp::AdvanceCommit {
                            to: LogIndex::new(2),
                        },
                    ],
                ),
                4,
                None,
                Some(&package),
            )
            .unwrap();
        assert_eq!(target.snapshot(), &snapshot);
        assert_eq!(target.log().manifest_generation(), Some(2));
        assert_eq!(target.log().committed_raft_index(), 2);
        let records = target
            .log()
            .read_committed_records_from_bounded(StreamOffset::new(0), 10, usize::MAX)
            .unwrap()
            .0;
        assert_eq!(records.len(), 1);
        assert_eq!(records[0].offset, StreamOffset::new(1));
        assert_eq!(records[0].record.value.as_deref(), Some(&b"current"[..]));
        assert!(!rebootstrap_backup_path(&target_dir).exists());
        assert!(!rebootstrap_staging_path(&target_dir).exists());
        assert!(!rebootstrap_intent_path(&target_dir).exists());
        target
            .execute(
                &PersistenceBatch::new(
                    PersistenceToken::new(5),
                    vec![
                        PersistenceOp::Append(vec![application(3, 2, b"suffix")]),
                        PersistenceOp::AdvanceCommit {
                            to: LogIndex::new(3),
                        },
                    ],
                ),
                5,
            )
            .unwrap();
        assert_eq!(
            target
                .log()
                .committed_stream_high_watermark()
                .unwrap()
                .get(),
            3
        );
        drop(target);
        // Simulate power loss after the new V2 directory was atomically
        // published but before the prior generation's backup/intent was
        // retired. Startup must retain the *validated new* V2 generation,
        // not accidentally prefer the stale prior partition inventory.
        let old_backup_dir = temp_dir("p9g-v2-old-backup-generation");
        let (old_backup, _) = open(&old_backup_dir);
        drop(old_backup);
        fs::rename(&old_backup_dir, rebootstrap_backup_path(&target_dir)).unwrap();
        atomic_write(&rebootstrap_intent_path(&target_dir), b"ASTRSWP1").unwrap();
        let (reopened, state) = open(&target_dir);
        assert_eq!(state.snapshot, snapshot);
        assert_eq!(state.hard_state.durable_commit_index, LogIndex::new(3));
        assert!(!rebootstrap_backup_path(&target_dir).exists());
        assert!(!rebootstrap_intent_path(&target_dir).exists());
        assert_eq!(
            reopened
                .log()
                .read_committed_records_from_bounded(StreamOffset::new(0), 10, usize::MAX,)
                .unwrap()
                .0
                .len(),
            2
        );
        drop(reopened);
        drop(source);
        fs::remove_dir_all(source_dir).unwrap();
        fs::remove_dir_all(target_dir).unwrap();
    }

    #[test]
    fn p9g_v2_sparse_producer_checkpoint_restores_exact_retry_without_acceleration_sidecar() {
        let source_dir = temp_dir("p9g-v2-producer-source");
        let target_dir = temp_dir("p9g-v2-producer-target");
        let (mut source, _) = open(&source_dir);
        source
            .execute(
                &PersistenceBatch::new(
                    PersistenceToken::new(1),
                    vec![
                        PersistenceOp::UpdateHardState(HardState {
                            current_term: Term::new(2),
                            voted_for: Some(node(1)),
                            durable_commit_index: LogIndex::ZERO,
                        }),
                        PersistenceOp::Append(vec![idempotent_application(
                            1,
                            0,
                            1,
                            0,
                            b"obsolete",
                        )]),
                        PersistenceOp::AdvanceCommit {
                            to: LogIndex::FIRST,
                        },
                    ],
                ),
                0,
            )
            .unwrap();
        source.log_mut().roll(2).unwrap();
        source
            .execute(
                &PersistenceBatch::new(
                    PersistenceToken::new(2),
                    vec![
                        PersistenceOp::Append(vec![application(2, 1, b"new-winner")]),
                        PersistenceOp::AdvanceCommit {
                            to: LogIndex::new(2),
                        },
                    ],
                ),
                2,
            )
            .unwrap();
        let expected = source.recover_committed_producer_states().unwrap();
        assert_eq!(expected.len(), 1);
        let checkpoint = source.producer_checkpoint_candidate().unwrap().unwrap();
        assert_eq!(checkpoint.committed_raft_index, 2);
        let encoded = encode_application_producer_state_checkpoint_command_v1(
            &ApplicationProducerStateCheckpointCommandV1 { checkpoint },
            CommandLimits::default(),
        )
        .unwrap();
        source
            .execute(
                &PersistenceBatch::new(
                    PersistenceToken::new(3),
                    vec![
                        PersistenceOp::Append(vec![LogEntry::new(
                            Term::new(1),
                            LogIndex::new(3),
                            EntryKind::Application(encoded),
                        )]),
                        PersistenceOp::AdvanceCommit {
                            to: LogIndex::new(3),
                        },
                    ],
                ),
                3,
            )
            .unwrap();
        let snapshot = SnapshotMetadata::new(
            LogIndex::new(3),
            Term::new(1),
            config(),
            SnapshotId::new([0x9f; 16]),
        )
        .unwrap();
        source
            .execute(
                &PersistenceBatch::new(
                    PersistenceToken::new(4),
                    vec![PersistenceOp::InstallSnapshot(snapshot.clone())],
                ),
                4,
            )
            .unwrap();
        let plan = source
            .plan_keyed_compaction(76, CompactionLimits::default())
            .unwrap();
        assert!(!plan.is_empty());
        source.execute_keyed_compaction(&plan, 76).unwrap();
        source.discard_producer_snapshot().unwrap();
        assert!(
            source
                .log()
                .read_frames()
                .unwrap()
                .iter()
                .all(|frame| !matches!(frame, Frame::Data(batch) if batch.producer.is_some()))
        );
        source.log_mut().roll(5).unwrap();
        source.log_mut().adopt_manifest_inventory().unwrap();
        let package = source
            .build_rebootstrap_package_v2(&snapshot, RebootstrapLimitsV2::default())
            .unwrap();
        assert!(
            package.producer_checkpoint.is_none(),
            "P7 acceleration must not be mistaken for P9-F committed authority"
        );
        // The producer-recovery source must also survive authenticated RF3
        // TCP transfer, not merely a direct in-process package install.
        // This preserves the authoritative committed P9-F checkpoint control
        // even though the original producer-bearing V1 data was compacted.
        let transferred = p9g_rf3_send_sparse_v2_via_authenticated_peers(&package);
        assert_eq!(transferred, package);
        let package = transferred;

        let (mut target, _) = open(&target_dir);
        target
            .execute(
                &PersistenceBatch::new(
                    PersistenceToken::new(1),
                    vec![PersistenceOp::UpdateHardState(HardState {
                        current_term: Term::new(2),
                        voted_for: None,
                        durable_commit_index: LogIndex::ZERO,
                    })],
                ),
                1,
            )
            .unwrap();
        target
            .execute_with_rebootstrap_versioned(
                &PersistenceBatch::new(
                    PersistenceToken::new(2),
                    vec![
                        PersistenceOp::InstallSnapshot(snapshot.clone()),
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
                6,
                None,
                Some(&package),
            )
            .unwrap();
        assert_eq!(
            target.recover_committed_producer_states().unwrap(),
            expected
        );
        assert!(!target.producer_snapshot_path().exists());
        target
            .execute(
                &PersistenceBatch::new(
                    PersistenceToken::new(3),
                    vec![
                        PersistenceOp::Append(vec![idempotent_application(
                            4,
                            2,
                            1,
                            1,
                            b"successor",
                        )]),
                        PersistenceOp::AdvanceCommit {
                            to: LogIndex::new(4),
                        },
                    ],
                ),
                7,
            )
            .unwrap();
        let actual = target.recover_committed_producer_states().unwrap();
        let state = &actual[&ProducerId::new(7).unwrap()];
        assert_eq!(state.next_sequence(), ProducerSequence::new(2));
        assert_eq!(
            state
                .recent_committed_batches()
                .front()
                .unwrap()
                .base_stream_offset,
            StreamOffset::new(0)
        );
        assert_eq!(
            state
                .recent_committed_batches()
                .back()
                .unwrap()
                .base_stream_offset,
            StreamOffset::new(2)
        );
        drop(target);
        let (reopened, _) = open(&target_dir);
        assert_eq!(
            reopened.recover_committed_producer_states().unwrap(),
            actual
        );
        drop(reopened);
        drop(source);
        fs::remove_dir_all(source_dir).unwrap();
        fs::remove_dir_all(target_dir).unwrap();
    }

    #[test]
    fn p9g_v2_producer_authority_survives_swap_crash_matrix_and_corruption() {
        let (source_dir, snapshot, package) =
            p9g_sparse_producer_package_with_acceleration("p9g-v2-crash-source");
        let expected = package.producer_checkpoint.as_ref().unwrap().states.clone();

        // First perform a real P3-authorized V2 install. This directory is the
        // exact fully validated generation copied into each synthetic crash
        // point below; no hand-built replacement files stand in for install.
        let new_seed = temp_dir("p9g-v2-crash-new-seed");
        let (mut new_generation, _) = open(&new_seed);
        new_generation
            .execute_with_rebootstrap_versioned(
                &PersistenceBatch::new(
                    PersistenceToken::new(1),
                    vec![
                        PersistenceOp::UpdateHardState(HardState {
                            current_term: Term::new(2),
                            voted_for: None,
                            durable_commit_index: LogIndex::ZERO,
                        }),
                        PersistenceOp::InstallSnapshot(snapshot.clone()),
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
                6,
                None,
                Some(&package),
            )
            .unwrap();
        assert_eq!(
            new_generation.recover_committed_producer_states().unwrap(),
            expected
        );
        assert_eq!(
            ProducerSnapshotStore::new(&new_seed).load().unwrap(),
            package.producer_checkpoint
        );
        drop(new_generation);

        let old_seed = temp_dir("p9g-v2-crash-old-seed");
        let (mut old_generation, _) = open(&old_seed);
        old_generation
            .execute(
                &PersistenceBatch::new(
                    PersistenceToken::new(1),
                    vec![
                        PersistenceOp::UpdateHardState(HardState {
                            current_term: Term::new(1),
                            voted_for: None,
                            durable_commit_index: LogIndex::ZERO,
                        }),
                        PersistenceOp::Append(vec![application(1, 0, b"old-authority")]),
                        PersistenceOp::AdvanceCommit {
                            to: LogIndex::FIRST,
                        },
                    ],
                ),
                0,
            )
            .unwrap();
        drop(old_generation);

        let make_old_target = |label: &str| {
            let target = temp_dir(label);
            copy_test_directory(&old_seed, &target);
            target
        };
        let publish_new_with_backup = |target: &Path| {
            let staging = rebootstrap_staging_path(target);
            copy_test_directory(&new_seed, &staging);
            atomic_write(&rebootstrap_intent_path(target), b"ASTRSWP1").unwrap();
            fs::rename(target, rebootstrap_backup_path(target)).unwrap();
            fs::rename(&staging, target).unwrap();
        };
        let assert_old = |target: &PartitionPersistence, state: &RecoveredState| {
            assert_eq!(state.hard_state.durable_commit_index, LogIndex::FIRST);
            assert_eq!(
                target
                    .log()
                    .read_records_from(StreamOffset::new(0), 10)
                    .unwrap()[0]
                    .record
                    .value
                    .as_deref(),
                Some(b"old-authority".as_slice())
            );
        };

        // Crash before publication intent: a complete producer-bearing stage
        // is disposable; restart keeps the old authoritative generation.
        let staged_only = make_old_target("p9g-v2-crash-staged-only");
        copy_test_directory(&new_seed, &rebootstrap_staging_path(&staged_only));
        let (recovered, state) = open(&staged_only);
        assert_old(&recovered, &state);
        assert!(!rebootstrap_staging_path(&staged_only).exists());
        drop(recovered);
        fs::remove_dir_all(&staged_only).unwrap();

        // Crash after intent + old->backup but before publishing the V2 stage.
        let backup_only = make_old_target("p9g-v2-crash-backup-only");
        copy_test_directory(&new_seed, &rebootstrap_staging_path(&backup_only));
        atomic_write(&rebootstrap_intent_path(&backup_only), b"ASTRSWP1").unwrap();
        fs::rename(&backup_only, rebootstrap_backup_path(&backup_only)).unwrap();
        let (recovered, state) = open(&backup_only);
        assert_old(&recovered, &state);
        assert!(!rebootstrap_backup_path(&backup_only).exists());
        assert!(!rebootstrap_staging_path(&backup_only).exists());
        assert!(!rebootstrap_intent_path(&backup_only).exists());
        drop(recovered);
        fs::remove_dir_all(&backup_only).unwrap();

        // Crash after publishing the complete new V2 generation. Corrupting
        // only the optional acceleration sidecar must not change authority:
        // the retained committed P9-F checkpoint reconstructs exact state.
        let published = make_old_target("p9g-v2-crash-published");
        publish_new_with_backup(&published);
        fs::write(
            published.join("producer.snapshot"),
            b"corrupt-derived-sidecar",
        )
        .unwrap();
        let (recovered, state) = open(&published);
        assert_eq!(state.snapshot, snapshot);
        assert_eq!(
            recovered.recover_committed_producer_states().unwrap(),
            expected
        );
        assert!(!rebootstrap_backup_path(&published).exists());
        assert!(!rebootstrap_intent_path(&published).exists());
        drop(recovered);
        fs::remove_dir_all(&published).unwrap();

        // A damaged authoritative manifest after publication cannot be
        // accepted merely because the directory rename completed.
        let corrupt_manifest = make_old_target("p9g-v2-crash-manifest-corrupt");
        publish_new_with_backup(&corrupt_manifest);
        let manifest_path = corrupt_manifest.join("segments.manifest");
        let mut manifest_bytes = fs::read(&manifest_path).unwrap();
        assert!(manifest_bytes.len() > 16);
        manifest_bytes[16] ^= 0x01;
        fs::write(&manifest_path, manifest_bytes).unwrap();
        let (recovered, state) = open(&corrupt_manifest);
        assert_old(&recovered, &state);
        assert!(!rebootstrap_backup_path(&corrupt_manifest).exists());
        assert!(!rebootstrap_intent_path(&corrupt_manifest).exists());
        drop(recovered);
        fs::remove_dir_all(&corrupt_manifest).unwrap();

        // Likewise, corrupt an immutable compacted retained file. Segment
        // framing/CRC validation must reject the published V2 generation and
        // recovery must restore the old backup.
        let corrupt_retained = make_old_target("p9g-v2-crash-retained-corrupt");
        publish_new_with_backup(&corrupt_retained);
        let retained_name = package
            .manifest
            .entries
            .iter()
            .find(|entry| !entry.is_mutable_active())
            .unwrap()
            .file_name
            .clone();
        let retained_path = corrupt_retained.join(retained_name);
        let mut retained_bytes = fs::read(&retained_path).unwrap();
        let retained_last = retained_bytes.len() - 1;
        retained_bytes[retained_last] ^= 0x01;
        fs::write(&retained_path, retained_bytes).unwrap();
        let (recovered, state) = open(&corrupt_retained);
        assert_old(&recovered, &state);
        assert!(!rebootstrap_backup_path(&corrupt_retained).exists());
        assert!(!rebootstrap_intent_path(&corrupt_retained).exists());
        drop(recovered);
        fs::remove_dir_all(&corrupt_retained).unwrap();

        fs::remove_dir_all(source_dir).unwrap();
        fs::remove_dir_all(new_seed).unwrap();
        fs::remove_dir_all(old_seed).unwrap();
    }

    #[test]
    fn p9g_v2_external_process_kill_preserves_committed_authority() {
        const FILTER: &str =
            "storage_adapter::tests::p9g_v2_external_process_kill_preserves_committed_authority";
        const TARGET: &str = "ASTRALOG_P9G_V2_PROCESS_KILL_TARGET";
        const PACKAGE: &str = "ASTRALOG_P9G_V2_PROCESS_KILL_PACKAGE";

        // Only the child executes the real, P3-authorized V2 installer.
        // The parent kills it at the test-only, fsynced readiness barrier.
        if let Some(target) = std::env::var_os(TARGET) {
            let target = PathBuf::from(target);
            let encoded = fs::read(std::env::var_os(PACKAGE).unwrap()).unwrap();
            let package =
                crate::decode_rebootstrap_package_v2(&encoded, RebootstrapLimitsV2::default())
                    .unwrap();
            let (mut old, prior) = open(&target);
            assert_eq!(prior.hard_state.durable_commit_index, LogIndex::FIRST);
            old.execute_with_rebootstrap_versioned(
                &PersistenceBatch::new(
                    PersistenceToken::new(1),
                    vec![
                        PersistenceOp::UpdateHardState(HardState {
                            current_term: Term::new(2),
                            voted_for: None,
                            durable_commit_index: LogIndex::FIRST,
                        }),
                        PersistenceOp::InstallSnapshot(package.snapshot.clone()),
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
                6,
                None,
                Some(&package),
            )
            .unwrap();
            panic!("P9-G V2 child installer escaped its mandatory external-kill barrier");
        }

        let (source_dir, snapshot, package) =
            p9g_sparse_producer_package_with_acceleration("p9g-v2-kill-source");
        let expected_producers = package.producer_checkpoint.as_ref().unwrap().states.clone();
        let fixtures = temp_dir("p9g-v2-kill-fixtures");
        let package_file = fixtures.join("producer-bearing-v2.package");
        fs::write(
            &package_file,
            crate::encode_rebootstrap_package_v2(&package, RebootstrapLimitsV2::default()).unwrap(),
        )
        .unwrap();

        let old_seed = temp_dir("p9g-v2-kill-old-seed");
        let (mut old, _) = open(&old_seed);
        old.execute(
            &PersistenceBatch::new(
                PersistenceToken::new(1),
                vec![
                    PersistenceOp::UpdateHardState(HardState {
                        current_term: Term::new(1),
                        voted_for: None,
                        durable_commit_index: LogIndex::ZERO,
                    }),
                    PersistenceOp::Append(vec![application(1, 0, b"old-before-kill")]),
                    PersistenceOp::AdvanceCommit {
                        to: LogIndex::FIRST,
                    },
                ],
            ),
            0,
        )
        .unwrap();
        drop(old);

        for (stage, expect_new) in [
            ("intent_published", false),
            ("old_moved_to_backup", false),
            ("new_generation_published", true),
        ] {
            let target = temp_dir(&format!("p9g-v2-kill-{stage}"));
            copy_test_directory(&old_seed, &target);
            let marker = fixtures.join(format!("{stage}.ready"));
            let mut child = std::process::Command::new(std::env::current_exe().unwrap())
                .arg("--exact")
                .arg(FILTER)
                .arg("--test-threads=1")
                .env(TARGET, &target)
                .env(PACKAGE, &package_file)
                .env("ASTRALOG_P9G_V2_PROCESS_KILL_STAGE", stage)
                .env("ASTRALOG_P9G_V2_PROCESS_KILL_MARKER", &marker)
                .stdout(std::process::Stdio::inherit())
                .stderr(std::process::Stdio::inherit())
                .spawn()
                .unwrap();
            let deadline = std::time::Instant::now() + std::time::Duration::from_secs(30);
            loop {
                if marker.exists() {
                    break;
                }
                if let Some(status) = child.try_wait().unwrap() {
                    panic!(
                        "P9-G V2 child exited before reaching {stage} barrier: {status}; no crash proof"
                    );
                }
                if std::time::Instant::now() >= deadline {
                    let _ = child.kill();
                    let _ = child.wait();
                    panic!("P9-G V2 child never reached {stage} crash barrier within 30s");
                }
                std::thread::sleep(std::time::Duration::from_millis(15));
            }
            child
                .kill()
                .expect("P9-G V2 child must still be paused at the real swap barrier");
            assert!(!child.wait().unwrap().success());
            assert_eq!(fs::read(&marker).unwrap(), stage.as_bytes());

            // This reopen is AFTER an actual OS termination, with no copied
            // synthetic crash fixtures. The old/new generations each carry
            // independent committed authority and exact producer state.
            let (mut recovered, state) = open(&target);
            if expect_new {
                assert_eq!(state.snapshot, snapshot);
                assert_eq!(state.hard_state.durable_commit_index, LogIndex::new(3));
                assert_eq!(
                    recovered.recover_committed_producer_states().unwrap(),
                    expected_producers,
                    "new V2 history must recover the original committed producer window"
                );
                // Unlike a synthetic swap reconstruction, this target just
                // survived real OS process termination mid-publication. The
                // original producer-bearing source frame is absent. Require
                // a fresh committed P7 sequence-1 suffix, exact prior batch
                // offset recovery, then durable reopening at index 4.
                recovered
                    .execute(
                        &PersistenceBatch::new(
                            PersistenceToken::new(3),
                            vec![
                                PersistenceOp::Append(vec![idempotent_application(
                                    4,
                                    2,
                                    1,
                                    1,
                                    b"successor-after-process-kill",
                                )]),
                                PersistenceOp::AdvanceCommit {
                                    to: LogIndex::new(4),
                                },
                            ],
                        ),
                        7,
                    )
                    .unwrap();
                let recovered_producers = recovered.recover_committed_producer_states().unwrap();
                let producer = &recovered_producers[&ProducerId::new(7).unwrap()];
                assert_eq!(producer.next_sequence(), ProducerSequence::new(2));
                assert_eq!(
                    producer
                        .recent_committed_batches()
                        .front()
                        .unwrap()
                        .base_stream_offset,
                    StreamOffset::new(0),
                    "post-kill V2 producer retry window must preserve original committed offset"
                );
                assert_eq!(
                    producer
                        .recent_committed_batches()
                        .back()
                        .unwrap()
                        .base_stream_offset,
                    StreamOffset::new(2),
                    "post-kill P7 successor must advance exactly one sequence without old replay"
                );
                drop(recovered);
                let (reopened, reopened_state) = open(&target);
                assert_eq!(
                    reopened_state.hard_state.durable_commit_index,
                    LogIndex::new(4)
                );
                assert_eq!(
                    reopened.recover_committed_producer_states().unwrap(),
                    recovered_producers,
                    "restart after a post-kill committed suffix must recover exact producer authority"
                );
                drop(reopened);
            } else {
                assert_eq!(state.hard_state.durable_commit_index, LogIndex::FIRST);
                assert_eq!(
                    recovered
                        .log()
                        .read_records_from(StreamOffset::new(0), 10)
                        .unwrap()[0]
                        .record
                        .value
                        .as_deref(),
                    Some(b"old-before-kill".as_slice()),
                    "an incomplete V2 swap must never replace the committed old history"
                );
                drop(recovered);
            }
            assert!(!rebootstrap_staging_path(&target).exists());
            assert!(!rebootstrap_backup_path(&target).exists());
            assert!(!rebootstrap_intent_path(&target).exists());
            fs::remove_dir_all(target).unwrap();
        }

        fs::remove_dir_all(old_seed).unwrap();
        fs::remove_dir_all(source_dir).unwrap();
        fs::remove_dir_all(fixtures).unwrap();
    }

    #[test]
    fn rebootstrap_recovery_preserves_old_generation_before_publication() {
        let dir = temp_dir("rebootstrap-recover-before-publish");
        let (mut original, _) = open(&dir);
        original
            .execute(
                &PersistenceBatch::new(
                    PersistenceToken::new(1),
                    vec![
                        PersistenceOp::UpdateHardState(HardState {
                            current_term: Term::new(1),
                            voted_for: None,
                            durable_commit_index: LogIndex::ZERO,
                        }),
                        PersistenceOp::Append(vec![application(1, 0, b"old")]),
                        PersistenceOp::AdvanceCommit {
                            to: LogIndex::FIRST,
                        },
                    ],
                ),
                0,
            )
            .unwrap();
        drop(original);

        let staging = rebootstrap_staging_path(&dir);
        fs::create_dir_all(&staging).unwrap();
        fs::write(staging.join("partial"), b"incomplete").unwrap();
        atomic_write(&rebootstrap_intent_path(&dir), b"ASTRSWP1").unwrap();

        let (recovered, state) = open(&dir);
        assert_eq!(state.hard_state.durable_commit_index, LogIndex::FIRST);
        assert_eq!(
            recovered
                .log()
                .read_records_from(StreamOffset::new(0), 10)
                .unwrap()[0]
                .record
                .value
                .as_deref(),
            Some(b"old".as_slice())
        );
        assert!(!staging.exists());
        assert!(!rebootstrap_intent_path(&dir).exists());
        drop(recovered);
        fs::remove_dir_all(dir).unwrap();
    }

    #[test]
    fn rebootstrap_recovery_rolls_back_if_old_was_moved_but_new_not_published() {
        let dir = temp_dir("rebootstrap-recover-after-backup");
        let (mut original, _) = open(&dir);
        original
            .execute(
                &PersistenceBatch::new(
                    PersistenceToken::new(1),
                    vec![
                        PersistenceOp::UpdateHardState(HardState {
                            current_term: Term::new(1),
                            voted_for: None,
                            durable_commit_index: LogIndex::ZERO,
                        }),
                        PersistenceOp::Append(vec![application(1, 0, b"old")]),
                        PersistenceOp::AdvanceCommit {
                            to: LogIndex::FIRST,
                        },
                    ],
                ),
                0,
            )
            .unwrap();
        drop(original);
        let staging = rebootstrap_staging_path(&dir);
        fs::create_dir_all(&staging).unwrap();
        fs::write(staging.join("partial"), b"incomplete").unwrap();
        atomic_write(&rebootstrap_intent_path(&dir), b"ASTRSWP1").unwrap();
        fs::rename(&dir, rebootstrap_backup_path(&dir)).unwrap();

        let (recovered, state) = open(&dir);
        assert_eq!(state.hard_state.durable_commit_index, LogIndex::FIRST);
        assert_eq!(
            recovered
                .log()
                .read_records_from(StreamOffset::new(0), 10)
                .unwrap()[0]
                .record
                .value
                .as_deref(),
            Some(b"old".as_slice())
        );
        assert!(!rebootstrap_backup_path(&dir).exists());
        assert!(!rebootstrap_intent_path(&dir).exists());
        drop(recovered);
        fs::remove_dir_all(dir).unwrap();
    }

    #[test]
    fn rebootstrap_recovery_keeps_valid_published_new_generation() {
        let dir = temp_dir("rebootstrap-recover-new-valid");
        let (mut old, _) = open(&dir);
        old.execute(
            &PersistenceBatch::new(
                PersistenceToken::new(1),
                vec![
                    PersistenceOp::UpdateHardState(HardState {
                        current_term: Term::new(1),
                        voted_for: None,
                        durable_commit_index: LogIndex::ZERO,
                    }),
                    PersistenceOp::Append(vec![application(1, 0, b"old")]),
                    PersistenceOp::AdvanceCommit {
                        to: LogIndex::FIRST,
                    },
                ],
            ),
            0,
        )
        .unwrap();
        drop(old);

        let staging = rebootstrap_staging_path(&dir);
        let (mut new_generation, _) = open(&staging);
        new_generation
            .execute(
                &PersistenceBatch::new(
                    PersistenceToken::new(1),
                    vec![
                        PersistenceOp::UpdateHardState(HardState {
                            current_term: Term::new(1),
                            voted_for: None,
                            durable_commit_index: LogIndex::ZERO,
                        }),
                        PersistenceOp::Append(vec![application(1, 0, b"new")]),
                        PersistenceOp::AdvanceCommit {
                            to: LogIndex::FIRST,
                        },
                    ],
                ),
                0,
            )
            .unwrap();
        drop(new_generation);
        atomic_write(&rebootstrap_intent_path(&dir), b"ASTRSWP1").unwrap();
        fs::rename(&dir, rebootstrap_backup_path(&dir)).unwrap();
        fs::rename(&staging, &dir).unwrap();

        let (recovered, _) = open(&dir);
        assert_eq!(
            recovered
                .log()
                .read_records_from(StreamOffset::new(0), 10)
                .unwrap()[0]
                .record
                .value
                .as_deref(),
            Some(b"new".as_slice())
        );
        assert!(!rebootstrap_backup_path(&dir).exists());
        assert!(!rebootstrap_intent_path(&dir).exists());
        drop(recovered);
        fs::remove_dir_all(dir).unwrap();
    }

    #[test]
    fn rebootstrap_recovery_rejects_corrupt_new_and_restores_backup() {
        let dir = temp_dir("rebootstrap-recover-new-corrupt");
        let (mut old, _) = open(&dir);
        old.execute(
            &PersistenceBatch::new(
                PersistenceToken::new(1),
                vec![
                    PersistenceOp::UpdateHardState(HardState {
                        current_term: Term::new(1),
                        voted_for: None,
                        durable_commit_index: LogIndex::ZERO,
                    }),
                    PersistenceOp::Append(vec![application(1, 0, b"old")]),
                    PersistenceOp::AdvanceCommit {
                        to: LogIndex::FIRST,
                    },
                ],
            ),
            0,
        )
        .unwrap();
        drop(old);
        atomic_write(&rebootstrap_intent_path(&dir), b"ASTRSWP1").unwrap();
        fs::rename(&dir, rebootstrap_backup_path(&dir)).unwrap();
        fs::create_dir_all(&dir).unwrap();
        fs::write(dir.join("raft.hardstate.v1"), b"corrupt").unwrap();

        let (recovered, _) = open(&dir);
        assert_eq!(
            recovered
                .log()
                .read_records_from(StreamOffset::new(0), 10)
                .unwrap()[0]
                .record
                .value
                .as_deref(),
            Some(b"old".as_slice())
        );
        assert!(!rebootstrap_backup_path(&dir).exists());
        assert!(!rebootstrap_intent_path(&dir).exists());
        drop(recovered);
        fs::remove_dir_all(dir).unwrap();
    }
}
