use std::{
    collections::{BTreeMap, BTreeSet},
    fs, mem,
    path::Path,
    sync::{Arc, RwLock},
};

use astralog_common::StreamOffset;

use super::*;
use crate::{
    RetainedControlFrameV2, RetainedSegmentHeaderV2, SparseRetainedBatchV2, SparseRetainedRecordV2,
    encode_retained_frame_v2, encode_retained_segment_header_v2, read_retained_segment_header_v2,
    read_segment_header,
};

const KEY_INDEX_ENTRY_OVERHEAD: usize = mem::size_of::<StreamOffset>() + 24;
const RECORD_ACCOUNTING_OVERHEAD: usize = 48;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct CompactionLimits {
    pub max_source_segments: usize,
    pub max_source_bytes: u64,
    pub max_decoded_records: usize,
    pub max_decoded_record_bytes: usize,
    pub max_key_index_entries: usize,
    pub max_key_index_bytes: usize,
    pub max_output_bytes: u64,
}

impl Default for CompactionLimits {
    fn default() -> Self {
        Self {
            max_source_segments: 128,
            max_source_bytes: 512 * 1024 * 1024,
            max_decoded_records: 1_000_000,
            max_decoded_record_bytes: 512 * 1024 * 1024,
            max_key_index_entries: 500_000,
            max_key_index_bytes: 256 * 1024 * 1024,
            max_output_bytes: 512 * 1024 * 1024,
        }
    }
}

impl CompactionLimits {
    fn validate(self) -> Result<()> {
        if self.max_source_segments == 0
            || self.max_source_bytes == 0
            || self.max_decoded_records == 0
            || self.max_decoded_record_bytes == 0
            || self.max_key_index_entries == 0
            || self.max_key_index_bytes == 0
            || self.max_output_bytes == 0
        {
            return Err(StorageError::InvalidFormat(
                "P9-E compaction limits must all be nonzero",
            ));
        }
        Ok(())
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct CompactionPlanSpec {
    pub policy_epoch: u64,
    pub snapshot_floor_raft_index: u64,
    pub producer_checkpoint: Option<ProducerCheckpointFence>,
    /// Explicit controller-gated CompactDelete tombstone reclaim window.
    /// None retains every tombstone (the historical P9-E contract).
    pub tombstone_removal: Option<TombstoneRemovalGate>,
    pub limits: CompactionLimits,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct TombstoneRemovalGate {
    pub now_ms: i64,
    pub grace_ms: u64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ProducerCheckpointFence {
    pub control_raft_index: u64,
    pub covered_raft_index: u64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CompactionPlan {
    pub base_manifest_generation: u64,
    pub policy_epoch: u64,
    pub snapshot_floor_raft_index: u64,
    pub maintenance_epoch: u64,
    pub committed_raft_index: u64,
    pub log_start_offset: StreamOffset,
    pub producer_checkpoint: Option<ProducerCheckpointFence>,
    pub tombstone_removal: Option<TombstoneRemovalGate>,
    pub committed_compaction_safe_offset: StreamOffset,
    pub active_file_name: String,
    pub source_file_names: Vec<String>,
    pub source_bytes: u64,
    pub decoded_records: usize,
    pub decoded_record_bytes: usize,
    pub key_index_entries: usize,
    pub key_index_bytes: usize,
    pub limits: CompactionLimits,
}

impl CompactionPlan {
    pub fn is_empty(&self) -> bool {
        self.source_file_names.is_empty()
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct CompactionOutcome {
    pub manifest_generation: u64,
    pub replaced_segments: usize,
    pub reclaimed_segments: usize,
    pub deferred_segments: usize,
    pub output_bytes: u64,
}

struct HistoryIndex {
    latest_by_key: BTreeMap<Vec<u8>, StreamOffset>,
    decoded_records: usize,
    decoded_record_bytes: usize,
    key_index_bytes: usize,
    latest_log_start_anchor: Option<RetainedControlFrameV2>,
    latest_producer_checkpoint_anchor: Option<RetainedControlFrameV2>,
    latest_compaction_safe_anchor: Option<RetainedControlFrameV2>,
}

struct PreparedCompactionGeneration {
    plan: CompactionPlan,
    transaction: GenerationTransaction,
    replacement_names: Vec<String>,
    output_bytes: u64,
}

impl PartitionLog {
    /// Verify a conservative *whole immutable prefix* of the currently
    /// authoritative manifest. Never use the set of selected cleaner source
    /// files as evidence: a partial COW pass may leave an older value behind
    /// in an unselected segment.
    ///
    /// This verification is bounded. If the full current partition inventory
    /// cannot be inspected within the limits, return zero rather than claim
    /// cleaning progress. The mutable V1 active segment is always excluded
    /// from the certified prefix; it is scanned only by the bounded global
    /// latest-key index to detect newer superseding records.
    pub fn verify_whole_clean_compacted_prefix(
        &self,
        snapshot_floor_raft_index: u64,
        limits: CompactionLimits,
    ) -> Result<StreamOffset> {
        limits.validate()?;
        let Some(manifest) = load_manifest(&self.directory)? else {
            return Ok(StreamOffset::new(0));
        };
        if Some(manifest.generation) != self.manifest_generation
            || manifest.entries.len() < 2
            || manifest.entries.len() > limits.max_source_segments.saturating_add(1)
        {
            return Ok(StreamOffset::new(0));
        }
        let authoritative = validate_partition_manifest_files(
            &self.directory,
            &manifest,
            self.topic_id,
            self.partition_id,
        )?;
        if authoritative != self.segment_paths {
            return Err(StorageError::InvalidFormat(
                "P9-G whole-prefix verification inventory changed",
            ));
        }
        let Some(active) = manifest.entries.last() else {
            return Ok(StreamOffset::new(0));
        };
        if !active.is_mutable_active() {
            return Err(StorageError::InvalidFormat(
                "P9-G whole-prefix verification requires a mutable V1 active file",
            ));
        }
        let active_path = authoritative.last().expect("non-empty checked manifest");
        if read_segment_format(active_path)? != StorageFormatVersion::V1 {
            return Err(StorageError::InvalidFormat(
                "P9-G active file must be Storage V1",
            ));
        }
        let mut total_bytes = 0_u64;
        for path in &authoritative {
            total_bytes = total_bytes.checked_add(fs::metadata(path)?.len()).ok_or(
                StorageError::ArithmeticOverflow("P9-G full prefix source bytes"),
            )?;
            if total_bytes > limits.max_source_bytes {
                return Ok(StreamOffset::new(0));
            }
        }
        let active_boundary =
            StreamOffset::new(read_segment_header(active_path)?.base_stream_offset.get());
        let high_watermark = self.committed_stream_high_watermark()?;
        let upper = active_boundary.min(high_watermark);
        if upper == StreamOffset::new(0) {
            return Ok(upper);
        }
        let current_safe = self.committed_compaction_safe_offset()?;
        let fresh = self.verify_clean_prefix_below(upper, snapshot_floor_raft_index, limits)?;
        if current_safe > StreamOffset::new(0) && current_safe <= upper && fresh < current_safe {
            // Later appends can supersede formerly-live values below an
            // already committed safe floor. This must never invalidate that
            // previously proven immutable prefix, but neither can a stale
            // sidecar confer authority: independently recheck *every* record
            // below the replicated safe bound against only keys below that
            // same bound. All sources are still current-manifest validated.
            let recovered =
                self.verify_clean_prefix_below(current_safe, snapshot_floor_raft_index, limits)?;
            return Ok(fresh.max(recovered));
        }
        Ok(fresh)
    }

    /// The clean certificate is an exclusive, append-monotonic stream prefix.
    /// Records at/after `upper` must not retroactively make an older value in
    /// the certified prefix unclean when a new generation rolls the active
    /// file. The full inventory is still byte/count bounded and validated.
    fn verify_clean_prefix_below(
        &self,
        upper: StreamOffset,
        snapshot_floor_raft_index: u64,
        limits: CompactionLimits,
    ) -> Result<StreamOffset> {
        let history = build_history_index(self, StreamOffset::new(0), limits, Some(upper))?;
        let mut proven = upper;
        for path in self.segment_paths.iter().take(self.segment_paths.len() - 1) {
            match read_segment_format(path)? {
                StorageFormatVersion::V1 => {
                    let file_len = fs::metadata(path)?.len();
                    let mut position = crate::format::SEGMENT_HEADER_LEN as u64;
                    while position < file_len {
                        let scanned =
                            crate::read_scanned_frame_at(path, position, self.config.limits)?;
                        position = position.checked_add(u64::from(scanned.encoded_len)).ok_or(
                            StorageError::ArithmeticOverflow("P9-G V1 prefix scan position"),
                        )?;
                        if scanned.frame.raft_index().get() > self.meta.committed_raft_index {
                            // An uncommitted suffix is not authority. It may
                            // already have rolled into a closed file, but it
                            // cannot invalidate a previously proven prefix
                            // when every one of its offsets is >= upper.
                            match &scanned.frame {
                                Frame::Data(batch) if batch.base_stream_offset >= upper => continue,
                                Frame::Control(_) => continue,
                                Frame::Data(_) => return Ok(StreamOffset::new(0)),
                            }
                        }
                        let Frame::Data(batch) = scanned.frame else {
                            // Maintenance/config controls carry no stream
                            // records. A committed control above the P3
                            // snapshot floor can remain in a closed V1 file
                            // after a normal roll without invalidating the
                            // *older* data-only clean prefix.
                            continue;
                        };
                        for (ordinal, record) in batch.records.iter().enumerate() {
                            let offset = batch
                                .base_stream_offset
                                .checked_add(u64::try_from(ordinal).map_err(|_| {
                                    StorageError::ArithmeticOverflow("P9-G V1 record ordinal")
                                })?)
                                .ok_or(StorageError::ArithmeticOverflow("P9-G V1 offset"))?;
                            if offset < upper
                                && (batch.raft_index.get() > snapshot_floor_raft_index
                                    || is_uncleaned_superseded_value(
                                        offset,
                                        record,
                                        &history.latest_by_key,
                                    ))
                            {
                                proven = proven.min(offset);
                            }
                        }
                    }
                }
                StorageFormatVersion::V2 => {
                    let file_len = fs::metadata(path)?.len();
                    let mut position = crate::SEGMENT_HEADER_LEN_V2 as u64;
                    while position < file_len {
                        let scanned =
                            read_retained_frame_at_v2(path, position, self.config.limits)?;
                        position = position.checked_add(u64::from(scanned.encoded_len)).ok_or(
                            StorageError::ArithmeticOverflow("P9-G V2 prefix scan position"),
                        )?;
                        if scanned.frame.raft_index().get() > snapshot_floor_raft_index
                            || scanned.frame.raft_index().get() > self.meta.committed_raft_index
                        {
                            return Ok(StreamOffset::new(0));
                        }
                        if let RetainedFrameV2::SparseData(batch) = scanned.frame {
                            for retained in batch.records {
                                if retained.offset < upper
                                    && is_uncleaned_superseded_value(
                                        retained.offset,
                                        &retained.record,
                                        &history.latest_by_key,
                                    )
                                {
                                    proven = proven.min(retained.offset);
                                }
                            }
                        }
                    }
                }
            }
        }
        Ok(proven)
    }
    /// Publish the verified prefix only for this manifest generation.
    /// Called after the immutable COW manifest has been published and all
    /// requisite index validation has succeeded. Any verification failure
    /// leaves no new authority; existing older-generation sidecars are stale.
    fn try_publish_whole_clean_progress(
        &self,
        snapshot_floor_raft_index: u64,
        limits: CompactionLimits,
    ) -> Result<()> {
        let candidate =
            self.verify_whole_clean_compacted_prefix(snapshot_floor_raft_index, limits)?;
        let Some(generation) = self.manifest_generation else {
            return Ok(());
        };
        let store =
            crate::LocalCleanProgressStore::new(&self.directory, self.topic_id, self.partition_id);
        store.persist_verified(
            generation,
            candidate,
            self.committed_stream_high_watermark()?,
        )
    }

    /// Rebootstrap does not inherit the sender's local progress sidecar.
    /// The transferred manifest must independently prove the full committed
    /// safety prefix on this receiver before it can become leader-eligible.
    /// A corrupted/oversized/partial retained package is rejected without
    /// installing an optimistic watermark.
    pub fn validate_and_publish_rebootstrap_clean_progress(
        &self,
        persisted_snapshot_floor: u64,
        limits: CompactionLimits,
    ) -> Result<StreamOffset> {
        let candidate =
            self.verify_whole_clean_compacted_prefix(persisted_snapshot_floor, limits)?;
        let committed = self.committed_compaction_safe_offset()?;
        if candidate < committed {
            return Err(StorageError::InvalidFormat(
                "P9-G rebootstrap retained history is behind committed safe prefix",
            ));
        }
        let generation = self.manifest_generation.ok_or(StorageError::InvalidFormat(
            "P9-G rebootstrap has no authoritative manifest generation",
        ))?;
        crate::LocalCleanProgressStore::new(&self.directory, self.topic_id, self.partition_id)
            .persist_verified(
                generation,
                candidate,
                self.committed_stream_high_watermark()?,
            )?;
        Ok(candidate)
    }

    pub fn plan_keyed_compaction(&mut self, spec: CompactionPlanSpec) -> Result<CompactionPlan> {
        spec.limits.validate()?;
        let safe = self.committed_compaction_safe_offset()?;
        // The replicated safe offset alone does not permit tombstone
        // removal. Ordinary superseded-value COW remains legal without the
        // CompactDelete gate and preserves the committed safe control anchor.
        if spec.tombstone_removal.is_some()
            && safe > StreamOffset::new(0)
            && self
                .verify_whole_clean_compacted_prefix(spec.snapshot_floor_raft_index, spec.limits)?
                < safe
        {
            return Err(StorageError::InvalidFormat(
                "P9-G local whole-prefix cleaning proof is behind committed safety authority",
            ));
        }
        self.adopt_manifest_inventory()?;
        let manifest = load_manifest(&self.directory)?.ok_or(StorageError::InvalidFormat(
            "P9-E compaction planning requires manifest authority",
        ))?;
        if Some(manifest.generation) != self.manifest_generation {
            return Err(StorageError::InvalidFormat(
                "manifest changed before P9-E compaction planning",
            ));
        }
        let authoritative = validate_partition_manifest_files(
            &self.directory,
            &manifest,
            self.topic_id,
            self.partition_id,
        )?;
        if authoritative != self.segment_paths || authoritative.len() != manifest.entries.len() {
            return Err(StorageError::InvalidFormat(
                "P9-E authoritative inventory changed during planning",
            ));
        }
        let active = manifest.entries.last().ok_or(StorageError::InvalidFormat(
            "P9-E manifest has no mutable active segment",
        ))?;
        if !active.is_mutable_active() {
            return Err(StorageError::InvalidFormat(
                "P9-E manifest last entry is not mutable active",
            ));
        }

        let log_start_offset = self.committed_log_start_offset()?;
        let history = build_history_index(self, log_start_offset, spec.limits, None)?;
        let mut source_file_names = Vec::new();
        let mut source_bytes = 0_u64;

        for (index, entry) in manifest.entries.iter().enumerate() {
            if entry.is_mutable_active() || index + 1 == manifest.entries.len() {
                continue;
            }
            let path = &authoritative[index];
            let close_bound_ms = successor_closed_segment_bound(&authoritative, index)?;
            let eligible_safe =
                source_tombstone_safe_bound(spec.tombstone_removal, close_bound_ms, safe);
            let inspection = inspect_source_candidate(
                path,
                self.config.limits,
                self.meta.committed_raft_index,
                spec.snapshot_floor_raft_index,
                log_start_offset,
                &history.latest_by_key,
                eligible_safe,
            )?;
            let producer_safe = inspection
                .max_producer_raft_index
                .is_none_or(|producer_raft| {
                    spec.producer_checkpoint.is_some_and(|checkpoint| {
                        checkpoint.covered_raft_index >= producer_raft
                            && checkpoint.control_raft_index <= spec.snapshot_floor_raft_index
                            && checkpoint.control_raft_index <= self.meta.committed_raft_index
                    })
                });
            if !inspection.snapshot_safe || !producer_safe || !inspection.changes {
                continue;
            }
            let bytes = fs::metadata(path)?.len();
            if source_file_names.len() == spec.limits.max_source_segments {
                break;
            }
            let Some(next_bytes) = source_bytes.checked_add(bytes) else {
                return Err(StorageError::ArithmeticOverflow("P9-E source bytes"));
            };
            if next_bytes > spec.limits.max_source_bytes {
                if source_file_names.is_empty() {
                    return Err(StorageError::LimitExceeded {
                        what: "P9-E source segment bytes",
                        actual: usize::try_from(bytes).unwrap_or(usize::MAX),
                        max: usize::try_from(spec.limits.max_source_bytes).unwrap_or(usize::MAX),
                    });
                }
                break;
            }
            source_bytes = next_bytes;
            source_file_names.push(entry.file_name.clone());
        }

        Ok(CompactionPlan {
            base_manifest_generation: manifest.generation,
            policy_epoch: spec.policy_epoch,
            snapshot_floor_raft_index: spec.snapshot_floor_raft_index,
            maintenance_epoch: self.meta.metadata_epoch,
            committed_raft_index: self.meta.committed_raft_index,
            log_start_offset,
            producer_checkpoint: spec.producer_checkpoint,
            tombstone_removal: spec.tombstone_removal,
            committed_compaction_safe_offset: safe,
            active_file_name: active.file_name.clone(),
            source_file_names,
            source_bytes,
            decoded_records: history.decoded_records,
            decoded_record_bytes: history.decoded_record_bytes,
            key_index_entries: history.latest_by_key.len(),
            key_index_bytes: history.key_index_bytes,
            limits: spec.limits,
        })
    }

    pub fn execute_keyed_compaction(
        &mut self,
        plan: &CompactionPlan,
        current_policy_epoch: u64,
        current_snapshot_floor_raft_index: u64,
    ) -> Result<CompactionOutcome> {
        self.execute_keyed_compaction_inner(
            plan,
            current_policy_epoch,
            current_snapshot_floor_raft_index,
            None,
        )
    }

    /// Same P9-E publication path with a broker-observed metadata epoch lock.
    /// The read guard is acquired only for the final validation + manifest
    /// switch, so expensive source decoding/staging does not block metadata
    /// refresh. A refresh that has observed newer committed metadata takes the
    /// write side and therefore either precedes publication (causing abort) or
    /// follows the completed atomic switch.
    pub fn execute_keyed_compaction_with_publication_guard(
        &mut self,
        plan: &CompactionPlan,
        current_policy_epoch: u64,
        current_snapshot_floor_raft_index: u64,
        publication_metadata_epoch: &RwLock<u64>,
    ) -> Result<CompactionOutcome> {
        self.execute_keyed_compaction_inner(
            plan,
            current_policy_epoch,
            current_snapshot_floor_raft_index,
            Some(publication_metadata_epoch),
        )
    }

    fn execute_keyed_compaction_inner(
        &mut self,
        plan: &CompactionPlan,
        current_policy_epoch: u64,
        current_snapshot_floor_raft_index: u64,
        publication_metadata_epoch: Option<&RwLock<u64>>,
    ) -> Result<CompactionOutcome> {
        if plan.is_empty() {
            self.validate_compaction_plan_current(
                plan,
                current_policy_epoch,
                current_snapshot_floor_raft_index,
            )?;
            return Ok(CompactionOutcome {
                manifest_generation: plan.base_manifest_generation,
                replaced_segments: 0,
                reclaimed_segments: self.reclaim_retired_read_generations()?,
                deferred_segments: self.retired_unreachable_path_count(),
                output_bytes: 0,
            });
        }
        let prepared = self.prepare_compaction_generation(
            plan,
            current_policy_epoch,
            current_snapshot_floor_raft_index,
        )?;
        self.publish_prepared_compaction(
            prepared,
            current_policy_epoch,
            current_snapshot_floor_raft_index,
            publication_metadata_epoch,
        )
    }

    fn validate_compaction_plan_current(
        &self,
        plan: &CompactionPlan,
        current_policy_epoch: u64,
        current_snapshot_floor_raft_index: u64,
    ) -> Result<SegmentManifest> {
        if current_policy_epoch != plan.policy_epoch {
            return Err(StorageError::InvalidFormat(
                "P9-E cleaner plan policy epoch is stale",
            ));
        }
        if current_snapshot_floor_raft_index != plan.snapshot_floor_raft_index {
            return Err(StorageError::InvalidFormat(
                "P9-E cleaner plan snapshot floor is stale",
            ));
        }
        if self.meta.metadata_epoch != plan.maintenance_epoch
            || self.meta.committed_raft_index != plan.committed_raft_index
        {
            return Err(StorageError::InvalidFormat(
                "P9-E cleaner plan maintenance epoch is stale",
            ));
        }
        if self.committed_log_start_offset()? != plan.log_start_offset {
            return Err(StorageError::InvalidFormat(
                "P9-E cleaner plan log-start maintenance state is stale",
            ));
        }
        if self.committed_compaction_safe_offset()? != plan.committed_compaction_safe_offset {
            return Err(StorageError::InvalidFormat(
                "P9-G cleaner plan compaction-safe maintenance state is stale",
            ));
        }
        let manifest = load_manifest(&self.directory)?.ok_or(StorageError::InvalidFormat(
            "P9-E cleaner plan manifest disappeared",
        ))?;
        if manifest.generation != plan.base_manifest_generation
            || Some(manifest.generation) != self.manifest_generation
        {
            return Err(StorageError::InvalidFormat(
                "P9-E cleaner plan manifest generation is stale",
            ));
        }
        let active = manifest.entries.last().ok_or(StorageError::InvalidFormat(
            "P9-E cleaner manifest has no active entry",
        ))?;
        if active.file_name != plan.active_file_name || !active.is_mutable_active() {
            return Err(StorageError::InvalidFormat(
                "P9-E cleaner active segment identity changed",
            ));
        }
        let source_set = plan.source_file_names.iter().collect::<BTreeSet<_>>();
        if source_set.len() != plan.source_file_names.len() {
            return Err(StorageError::InvalidFormat(
                "P9-E cleaner plan contains duplicate source files",
            ));
        }
        for source in &plan.source_file_names {
            let entry = manifest
                .entries
                .iter()
                .find(|entry| entry.file_name == *source)
                .ok_or(StorageError::InvalidFormat(
                    "P9-E cleaner source is no longer in the authoritative manifest",
                ))?;
            if entry.is_mutable_active() {
                return Err(StorageError::InvalidFormat(
                    "P9-E cleaner plan includes the mutable active segment",
                ));
            }
        }
        Ok(manifest)
    }

    fn prepare_compaction_generation(
        &self,
        plan: &CompactionPlan,
        current_policy_epoch: u64,
        current_snapshot_floor_raft_index: u64,
    ) -> Result<PreparedCompactionGeneration> {
        let manifest = self.validate_compaction_plan_current(
            plan,
            current_policy_epoch,
            current_snapshot_floor_raft_index,
        )?;
        let history = build_history_index(self, plan.log_start_offset, plan.limits, None)?;
        if history.decoded_records != plan.decoded_records
            || history.decoded_record_bytes != plan.decoded_record_bytes
            || history.latest_by_key.len() != plan.key_index_entries
            || history.key_index_bytes != plan.key_index_bytes
        {
            return Err(StorageError::InvalidFormat(
                "P9-E cleaner history changed after planning",
            ));
        }

        let next_generation = manifest
            .generation
            .checked_add(1)
            .ok_or(StorageError::ArithmeticOverflow("manifest generation"))?;
        let source_set = plan.source_file_names.iter().collect::<BTreeSet<_>>();
        let mut transaction = GenerationTransaction::new(&self.directory, next_generation)?;
        let mut replacement_names = Vec::new();
        let mut output_bytes = 0_u64;

        for (index, entry) in manifest.entries.iter().enumerate() {
            if !source_set.contains(&entry.file_name) {
                transaction.retain(entry.clone())?;
                continue;
            }
            if entry.is_mutable_active() {
                return Err(StorageError::InvalidFormat(
                    "P9-E cleaner attempted to stage the mutable active segment",
                ));
            }
            let source_path = self.directory.join(&entry.file_name);
            let close_bound_ms = successor_closed_segment_bound(&self.segment_paths, index)?;
            let eligible_safe = source_tombstone_safe_bound(
                plan.tombstone_removal,
                close_bound_ms,
                plan.committed_compaction_safe_offset,
            );
            let replacement = build_replacement_segment(
                self,
                &source_path,
                plan,
                &history,
                &replacement_names,
                eligible_safe,
            )?;
            let Some((name, bytes)) = replacement else {
                continue;
            };
            let bytes_len = u64::try_from(bytes.len())
                .map_err(|_| StorageError::ArithmeticOverflow("P9-E output bytes"))?;
            output_bytes = output_bytes
                .checked_add(bytes_len)
                .ok_or(StorageError::ArithmeticOverflow("P9-E output bytes"))?;
            if output_bytes > plan.limits.max_output_bytes {
                return Err(StorageError::LimitExceeded {
                    what: "P9-E replacement output bytes",
                    actual: usize::try_from(output_bytes).unwrap_or(usize::MAX),
                    max: usize::try_from(plan.limits.max_output_bytes).unwrap_or(usize::MAX),
                });
            }
            transaction.stage_file(name.clone(), &bytes, false)?;
            replacement_names.push(name);
        }

        Ok(PreparedCompactionGeneration {
            plan: plan.clone(),
            transaction,
            replacement_names,
            output_bytes,
        })
    }

    fn publish_prepared_compaction(
        &mut self,
        prepared: PreparedCompactionGeneration,
        current_policy_epoch: u64,
        current_snapshot_floor_raft_index: u64,
        publication_metadata_epoch: Option<&RwLock<u64>>,
    ) -> Result<CompactionOutcome> {
        let publication_guard = publication_metadata_epoch.map(|guard| {
            guard
                .read()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
        });
        if publication_guard
            .as_deref()
            .is_some_and(|epoch| *epoch != prepared.plan.policy_epoch)
        {
            return Err(StorageError::InvalidFormat(
                "P9-E cleaner publication metadata epoch is stale",
            ));
        }
        self.validate_compaction_plan_current(
            &prepared.plan,
            current_policy_epoch,
            current_snapshot_floor_raft_index,
        )?;
        let old_owner = Arc::clone(&self.read_generation);
        let old_paths = self.segment_paths.clone();
        let published = prepared.transaction.commit()?;
        let new_paths = validate_partition_manifest_files(
            &self.directory,
            &published,
            self.topic_id,
            self.partition_id,
        )?;
        let new_path_set = new_paths.iter().collect::<BTreeSet<_>>();
        let unreachable_paths = old_paths
            .iter()
            .filter(|path| !new_path_set.contains(path))
            .cloned()
            .collect::<Vec<_>>();

        self.segment_paths = new_paths;
        self.manifest_generation = Some(published.generation);
        self.install_read_generation_with_retirement(old_owner, unreachable_paths);

        for name in &prepared.replacement_names {
            let path = self.directory.join(name);
            let scan = scan_retained_segment_v2(&path, self.config.limits)?;
            rebuild_retained_segment_indexes_v2(
                &path,
                &scan.header,
                &scan.frames,
                self.config.index_interval_bytes,
            )?;
        }
        let reclaimed_segments = self.reclaim_retired_read_generations()?;
        // The manifest has *already* atomically switched. Progress is best
        // effort derived evidence, not a reason to report a failed COW commit.
        // Missing/invalid generation evidence reads as zero on the next run.
        let _ = self.try_publish_whole_clean_progress(
            prepared.plan.snapshot_floor_raft_index,
            prepared.plan.limits,
        );
        Ok(CompactionOutcome {
            manifest_generation: published.generation,
            replaced_segments: prepared.plan.source_file_names.len(),
            reclaimed_segments,
            deferred_segments: self.retired_unreachable_path_count(),
            output_bytes: prepared.output_bytes,
        })
    }

    fn retired_unreachable_path_count(&self) -> usize {
        self.retired_read_generations
            .iter()
            .map(|retired| retired.unreachable_paths.len())
            .sum()
    }
}

struct SourceInspection {
    snapshot_safe: bool,
    max_producer_raft_index: Option<u64>,
    changes: bool,
}

fn successor_closed_segment_bound(
    paths: &[std::path::PathBuf],
    index: usize,
) -> Result<Option<i64>> {
    let Some(source) = paths.get(index) else {
        return Ok(None);
    };
    let Some(successor) = paths.get(index + 1) else {
        return Ok(None);
    };
    let created = |path: &Path| -> Result<i64> {
        match read_segment_format(path)? {
            StorageFormatVersion::V1 => Ok(read_segment_header(path)?.created_at_ms),
            StorageFormatVersion::V2 => Ok(read_retained_segment_header_v2(path)?.created_at_ms),
        }
    };
    let previous = created(source)?;
    let close_bound = created(successor)?;
    if previous < 0 || close_bound < previous {
        // Negative creation time or a reversed successor clock is not proof
        // that the source has aged; postpone tombstone cleanup.
        return Ok(None);
    }
    Ok(Some(close_bound))
}

fn source_tombstone_safe_bound(
    gate: Option<TombstoneRemovalGate>,
    close_bound_ms: Option<i64>,
    committed_safe: StreamOffset,
) -> Option<StreamOffset> {
    let gate = gate?;
    // The controller and public topic contracts require a strictly positive
    // tombstone grace. Preserve that invariant at the final storage gate too:
    // direct/internal callers cannot turn a zero interval into authority.
    if gate.grace_ms == 0 {
        return None;
    }
    let close = close_bound_ms?;
    let elapsed = gate.now_ms.checked_sub(close)?;
    let age = u64::try_from(elapsed).ok()?;
    (committed_safe > StreamOffset::new(0) && age >= gate.grace_ms).then_some(committed_safe)
}

fn inspect_source_candidate(
    path: &Path,
    limits: StorageLimits,
    committed_raft_index: u64,
    snapshot_floor_raft_index: u64,
    log_start_offset: StreamOffset,
    latest_by_key: &BTreeMap<Vec<u8>, StreamOffset>,
    eligible_safe: Option<StreamOffset>,
) -> Result<SourceInspection> {
    let mut snapshot_safe = true;
    let mut max_producer_raft_index = None;
    let mut changes = false;
    match read_segment_format(path)? {
        StorageFormatVersion::V1 => {
            let file_len = fs::metadata(path)?.len();
            let mut position = crate::format::SEGMENT_HEADER_LEN as u64;
            while position < file_len {
                let scanned = crate::read_scanned_frame_at(path, position, limits)?;
                position = position.checked_add(u64::from(scanned.encoded_len)).ok_or(
                    StorageError::ArithmeticOverflow("P9-E V1 source scan position"),
                )?;
                let raft = scanned.frame.raft_index().get();
                if raft > committed_raft_index || raft > snapshot_floor_raft_index {
                    snapshot_safe = false;
                }
                if let Frame::Data(batch) = scanned.frame {
                    if batch.producer.is_some() {
                        max_producer_raft_index = Some(
                            max_producer_raft_index.map_or(raft, |current: u64| current.max(raft)),
                        );
                    }
                    for (ordinal, record) in batch.records.iter().enumerate() {
                        let ordinal = u64::try_from(ordinal).map_err(|_| {
                            StorageError::ArithmeticOverflow("P9-E V1 source ordinal")
                        })?;
                        let offset = batch
                            .base_stream_offset
                            .checked_add(ordinal)
                            .ok_or(StorageError::ArithmeticOverflow("P9-E V1 source offset"))?;
                        if record_is_omittable(
                            offset,
                            record,
                            log_start_offset,
                            latest_by_key,
                            eligible_safe,
                        ) {
                            changes = true;
                        }
                    }
                }
            }
        }
        StorageFormatVersion::V2 => {
            let file_len = fs::metadata(path)?.len();
            let mut position = crate::SEGMENT_HEADER_LEN_V2 as u64;
            while position < file_len {
                let scanned = read_retained_frame_at_v2(path, position, limits)?;
                position = position.checked_add(u64::from(scanned.encoded_len)).ok_or(
                    StorageError::ArithmeticOverflow("P9-E V2 source scan position"),
                )?;
                let raft = scanned.frame.raft_index().get();
                if raft > committed_raft_index || raft > snapshot_floor_raft_index {
                    snapshot_safe = false;
                }
                match scanned.frame {
                    RetainedFrameV2::SparseData(batch) => {
                        for retained in &batch.records {
                            if record_is_omittable(
                                retained.offset,
                                &retained.record,
                                log_start_offset,
                                latest_by_key,
                                eligible_safe,
                            ) {
                                changes = true;
                            }
                        }
                    }
                    RetainedFrameV2::Control(control)
                        if control.kind == RetainedControlKindV2::AdvanceCompactionSafeOffset =>
                    {
                        let _ = decode_compaction_safe_offset_body(&control.body)?;
                    }
                    RetainedFrameV2::Control(_) => {}
                }
            }
        }
    }
    Ok(SourceInspection {
        snapshot_safe,
        max_producer_raft_index,
        changes,
    })
}

fn build_history_index(
    log: &PartitionLog,
    log_start_offset: StreamOffset,
    limits: CompactionLimits,
    latest_before: Option<StreamOffset>,
) -> Result<HistoryIndex> {
    let mut history = HistoryIndex {
        latest_by_key: BTreeMap::new(),
        decoded_records: 0,
        decoded_record_bytes: 0,
        key_index_bytes: 0,
        latest_log_start_anchor: None,
        latest_producer_checkpoint_anchor: None,
        latest_compaction_safe_anchor: None,
    };
    for path in &log.segment_paths {
        match read_segment_format(path)? {
            StorageFormatVersion::V1 => {
                let file_len = fs::metadata(path)?.len();
                let mut position = crate::format::SEGMENT_HEADER_LEN as u64;
                while position < file_len {
                    let scanned = crate::read_scanned_frame_at(path, position, log.config.limits)?;
                    position = position.checked_add(u64::from(scanned.encoded_len)).ok_or(
                        StorageError::ArithmeticOverflow("P9-E V1 history scan position"),
                    )?;
                    if scanned.frame.raft_index().get() > log.meta.committed_raft_index {
                        break;
                    }
                    match scanned.frame {
                        Frame::Data(batch) => {
                            for (ordinal, record) in batch.records.into_iter().enumerate() {
                                let ordinal = u64::try_from(ordinal).map_err(|_| {
                                    StorageError::ArithmeticOverflow("P9-E history ordinal")
                                })?;
                                let offset = batch.base_stream_offset.checked_add(ordinal).ok_or(
                                    StorageError::ArithmeticOverflow("P9-E history stream offset"),
                                )?;
                                account_record(&mut history, &record, limits)?;
                                if offset >= log_start_offset
                                    && latest_before.is_none_or(|end| offset < end)
                                {
                                    update_latest_key(&mut history, &record, offset, limits)?;
                                }
                            }
                        }
                        Frame::Control(control)
                            if control.control_kind == P9_CONTROL_ADVANCE_LOG_START_OFFSET =>
                        {
                            if control.control_version != P9_MAINTENANCE_CONTROL_VERSION {
                                return Err(StorageError::InvalidFormat(
                                    "P9-E found unsupported log-start maintenance version",
                                ));
                            }
                            let _ = decode_log_start_offset_body(&control.body)?;
                            history.latest_log_start_anchor = Some(RetainedControlFrameV2 {
                                raft_term: control.raft_term,
                                raft_index: control.raft_index,
                                kind: RetainedControlKindV2::AdvanceLogStartOffset,
                                body: control.body,
                            });
                        }
                        Frame::Control(control)
                            if control.control_kind == P9_CONTROL_PRODUCER_STATE_CHECKPOINT =>
                        {
                            if control.control_version != P9_MAINTENANCE_CONTROL_VERSION {
                                return Err(StorageError::InvalidFormat(
                                    "P9-F found unsupported producer checkpoint control version",
                                ));
                            }
                            history.latest_producer_checkpoint_anchor =
                                Some(RetainedControlFrameV2 {
                                    raft_term: control.raft_term,
                                    raft_index: control.raft_index,
                                    kind: RetainedControlKindV2::ProducerStateCheckpoint,
                                    body: control.body,
                                });
                        }
                        Frame::Control(control)
                            if control.control_kind
                                == P9_CONTROL_ADVANCE_COMPACTION_SAFE_OFFSET =>
                        {
                            if control.control_version != P9_MAINTENANCE_CONTROL_VERSION {
                                return Err(StorageError::InvalidFormat(
                                    "unsupported P9-G compaction-safe source control version",
                                ));
                            }
                            let _ = decode_compaction_safe_offset_body(&control.body)?;
                            history.latest_compaction_safe_anchor = Some(RetainedControlFrameV2 {
                                raft_term: control.raft_term,
                                raft_index: control.raft_index,
                                kind: RetainedControlKindV2::AdvanceCompactionSafeOffset,
                                body: control.body,
                            });
                        }
                        Frame::Control(_) => {}
                    }
                }
            }
            StorageFormatVersion::V2 => {
                let file_len = fs::metadata(path)?.len();
                let mut position = crate::SEGMENT_HEADER_LEN_V2 as u64;
                while position < file_len {
                    let scanned = read_retained_frame_at_v2(path, position, log.config.limits)?;
                    position = position.checked_add(u64::from(scanned.encoded_len)).ok_or(
                        StorageError::ArithmeticOverflow("P9-E V2 history scan position"),
                    )?;
                    if scanned.frame.raft_index().get() > log.meta.committed_raft_index {
                        return Err(StorageError::InvalidFormat(
                            "P9-E V2 retained history extends above committed boundary",
                        ));
                    }
                    match scanned.frame {
                        RetainedFrameV2::SparseData(batch) => {
                            for retained in batch.records {
                                account_record(&mut history, &retained.record, limits)?;
                                if retained.offset >= log_start_offset
                                    && latest_before.is_none_or(|end| retained.offset < end)
                                {
                                    update_latest_key(
                                        &mut history,
                                        &retained.record,
                                        retained.offset,
                                        limits,
                                    )?;
                                }
                            }
                        }
                        RetainedFrameV2::Control(control) => match control.kind {
                            RetainedControlKindV2::AdvanceLogStartOffset => {
                                let _ = decode_log_start_offset_body(&control.body)?;
                                history.latest_log_start_anchor = Some(control);
                            }
                            RetainedControlKindV2::ProducerStateCheckpoint => {
                                history.latest_producer_checkpoint_anchor = Some(control);
                            }
                            RetainedControlKindV2::AdvanceCompactionSafeOffset => {
                                let _ = decode_compaction_safe_offset_body(&control.body)?;
                                history.latest_compaction_safe_anchor = Some(control);
                            }
                        },
                    }
                }
            }
        }
    }
    Ok(history)
}

fn account_record(
    history: &mut HistoryIndex,
    record: &DataRecord,
    limits: CompactionLimits,
) -> Result<()> {
    history.decoded_records =
        history
            .decoded_records
            .checked_add(1)
            .ok_or(StorageError::ArithmeticOverflow(
                "P9-E decoded record count",
            ))?;
    if history.decoded_records > limits.max_decoded_records {
        return Err(StorageError::LimitExceeded {
            what: "P9-E decoded records",
            actual: history.decoded_records,
            max: limits.max_decoded_records,
        });
    }
    let bytes = record_accounted_bytes(record)?;
    history.decoded_record_bytes =
        history
            .decoded_record_bytes
            .checked_add(bytes)
            .ok_or(StorageError::ArithmeticOverflow(
                "P9-E decoded record bytes",
            ))?;
    if history.decoded_record_bytes > limits.max_decoded_record_bytes {
        return Err(StorageError::LimitExceeded {
            what: "P9-E decoded record bytes",
            actual: history.decoded_record_bytes,
            max: limits.max_decoded_record_bytes,
        });
    }
    Ok(())
}

fn record_accounted_bytes(record: &DataRecord) -> Result<usize> {
    let mut total = RECORD_ACCOUNTING_OVERHEAD;
    total = total
        .checked_add(record.key.as_ref().map_or(0, Vec::len))
        .and_then(|value| value.checked_add(record.value.as_ref().map_or(0, Vec::len)))
        .ok_or(StorageError::ArithmeticOverflow("P9-E record memory bytes"))?;
    for header in &record.headers {
        total = total
            .checked_add(header.name.len())
            .and_then(|value| value.checked_add(header.value.as_ref().map_or(0, Vec::len)))
            .and_then(|value| value.checked_add(16))
            .ok_or(StorageError::ArithmeticOverflow(
                "P9-E record header memory bytes",
            ))?;
    }
    Ok(total)
}

fn update_latest_key(
    history: &mut HistoryIndex,
    record: &DataRecord,
    offset: StreamOffset,
    limits: CompactionLimits,
) -> Result<()> {
    let Some(key) = record.key.as_deref() else {
        return Ok(());
    };
    if let Some(existing) = history.latest_by_key.get_mut(key) {
        if offset > *existing {
            *existing = offset;
        }
        return Ok(());
    }
    let next_entries = history
        .latest_by_key
        .len()
        .checked_add(1)
        .ok_or(StorageError::ArithmeticOverflow("P9-E key index entries"))?;
    if next_entries > limits.max_key_index_entries {
        return Err(StorageError::LimitExceeded {
            what: "P9-E key index entries",
            actual: next_entries,
            max: limits.max_key_index_entries,
        });
    }
    let entry_bytes = key
        .len()
        .checked_add(KEY_INDEX_ENTRY_OVERHEAD)
        .ok_or(StorageError::ArithmeticOverflow("P9-E key index bytes"))?;
    let next_bytes = history
        .key_index_bytes
        .checked_add(entry_bytes)
        .ok_or(StorageError::ArithmeticOverflow("P9-E key index bytes"))?;
    if next_bytes > limits.max_key_index_bytes {
        return Err(StorageError::LimitExceeded {
            what: "P9-E key index bytes",
            actual: next_bytes,
            max: limits.max_key_index_bytes,
        });
    }
    history.key_index_bytes = next_bytes;
    history.latest_by_key.insert(key.to_vec(), offset);
    Ok(())
}

fn record_is_omittable(
    offset: StreamOffset,
    record: &DataRecord,
    log_start_offset: StreamOffset,
    latest_by_key: &BTreeMap<Vec<u8>, StreamOffset>,
    eligible_safe: Option<StreamOffset>,
) -> bool {
    // A committed logical log-start advance is *not* sufficient evidence to
    // physically erase a keyed tombstone. Even below the fetch floor, an
    // offline P5 voter could retain an older value. Only the independently
    // committed all-voter safe floor plus grace/snapshot gates may remove it.
    if record.key.is_some() && record.value.is_none() {
        return eligible_safe.is_some_and(|safe| offset < safe);
    }
    if offset < log_start_offset {
        return true;
    }
    let Some(key) = record.key.as_deref() else {
        return false;
    };
    latest_by_key
        .get(key)
        .is_some_and(|latest| *latest != offset)
}

fn is_uncleaned_superseded_value(
    offset: StreamOffset,
    record: &DataRecord,
    latest_by_key: &BTreeMap<Vec<u8>, StreamOffset>,
) -> bool {
    if record.value.is_none() {
        return false; // tombstones are retained until the separate P9-G gate
    }
    record.key.as_deref().is_some_and(|key| {
        latest_by_key
            .get(key)
            .is_some_and(|latest| *latest != offset)
    })
}

fn build_replacement_segment(
    log: &PartitionLog,
    source_path: &Path,
    plan: &CompactionPlan,
    history: &HistoryIndex,
    already_staged: &[String],
    eligible_safe: Option<StreamOffset>,
) -> Result<Option<(String, Vec<u8>)>> {
    let (identity, created_at_ms) = match read_segment_format(source_path)? {
        StorageFormatVersion::V1 => {
            let header = read_segment_header(source_path)?;
            (SegmentFileId::from_header(&header), header.created_at_ms)
        }
        StorageFormatVersion::V2 => {
            let header = read_retained_segment_header_v2(source_path)?;
            (
                SegmentFileId::from_header(&header.identity_header()),
                header.created_at_ms,
            )
        }
    };
    let mut replacement_generation = identity
        .generation
        .checked_add(1)
        .ok_or(StorageError::ArithmeticOverflow("P9-E segment generation"))?;
    let replacement_name = loop {
        let candidate = SegmentFileId {
            base_stream_offset: identity.base_stream_offset,
            base_raft_index: identity.base_raft_index,
            generation: replacement_generation,
        }
        .log_file_name();
        if !log.directory.join(&candidate).exists()
            && !already_staged.iter().any(|name| name == &candidate)
        {
            break candidate;
        }
        replacement_generation = replacement_generation
            .checked_add(1)
            .ok_or(StorageError::ArithmeticOverflow("P9-E segment generation"))?;
    };
    let header = RetainedSegmentHeaderV2 {
        topic_id: log.topic_id,
        partition_id: log.partition_id,
        generation: replacement_generation,
        base_stream_offset: identity.base_stream_offset,
        base_raft_index: identity.base_raft_index,
        created_at_ms,
    };
    let mut bytes = encode_retained_segment_header_v2(&header).to_vec();
    let mut emitted_frames = 0_usize;

    match read_segment_format(source_path)? {
        StorageFormatVersion::V1 => {
            let file_len = fs::metadata(source_path)?.len();
            let mut position = crate::format::SEGMENT_HEADER_LEN as u64;
            while position < file_len {
                let scanned =
                    crate::read_scanned_frame_at(source_path, position, log.config.limits)?;
                position = position
                    .checked_add(u64::from(scanned.encoded_len))
                    .ok_or(StorageError::ArithmeticOverflow("P9-E V1 rewrite position"))?;
                if scanned.frame.raft_index().get() > plan.snapshot_floor_raft_index
                    || scanned.frame.raft_index().get() > plan.committed_raft_index
                {
                    return Err(StorageError::InvalidFormat(
                        "P9-E rewrite source frame exceeds captured snapshot/commit floor",
                    ));
                }
                match scanned.frame {
                    Frame::Data(batch) => {
                        if batch.producer.is_some()
                            && !plan.producer_checkpoint.is_some_and(|checkpoint| {
                                checkpoint.covered_raft_index >= batch.raft_index.get()
                                    && checkpoint.control_raft_index
                                        <= plan.snapshot_floor_raft_index
                            })
                        {
                            return Err(StorageError::InvalidFormat(
                                "P9-F cannot compact P7 producer source without a snapshot-safe covering checkpoint",
                            ));
                        }
                        let mut survivors = Vec::new();
                        for (ordinal, record) in batch.records.into_iter().enumerate() {
                            let ordinal = u64::try_from(ordinal).map_err(|_| {
                                StorageError::ArithmeticOverflow("P9-E V1 rewrite ordinal")
                            })?;
                            let offset = batch.base_stream_offset.checked_add(ordinal).ok_or(
                                StorageError::ArithmeticOverflow("P9-E V1 rewrite offset"),
                            )?;
                            if !record_is_omittable(
                                offset,
                                &record,
                                plan.log_start_offset,
                                &history.latest_by_key,
                                eligible_safe,
                            ) {
                                survivors.push(SparseRetainedRecordV2 { offset, record });
                            }
                        }
                        if !survivors.is_empty() {
                            append_retained_frame(
                                &mut bytes,
                                RetainedFrameV2::SparseData(SparseRetainedBatchV2 {
                                    raft_term: batch.raft_term,
                                    raft_index: batch.raft_index,
                                    compression: batch.compression,
                                    records: survivors,
                                }),
                                log.config.limits,
                                plan.limits.max_output_bytes,
                            )?;
                            emitted_frames += 1;
                        }
                    }
                    Frame::Control(control)
                        if control.control_kind == P9_CONTROL_ADVANCE_LOG_START_OFFSET =>
                    {
                        if history
                            .latest_log_start_anchor
                            .as_ref()
                            .is_some_and(|anchor| anchor.raft_index == control.raft_index)
                        {
                            append_retained_frame(
                                &mut bytes,
                                RetainedFrameV2::Control(RetainedControlFrameV2 {
                                    raft_term: control.raft_term,
                                    raft_index: control.raft_index,
                                    kind: RetainedControlKindV2::AdvanceLogStartOffset,
                                    body: control.body,
                                }),
                                log.config.limits,
                                plan.limits.max_output_bytes,
                            )?;
                            emitted_frames += 1;
                        }
                    }
                    Frame::Control(control)
                        if control.control_kind == P9_CONTROL_PRODUCER_STATE_CHECKPOINT =>
                    {
                        if history
                            .latest_producer_checkpoint_anchor
                            .as_ref()
                            .is_some_and(|anchor| anchor.raft_index == control.raft_index)
                        {
                            append_retained_frame(
                                &mut bytes,
                                RetainedFrameV2::Control(RetainedControlFrameV2 {
                                    raft_term: control.raft_term,
                                    raft_index: control.raft_index,
                                    kind: RetainedControlKindV2::ProducerStateCheckpoint,
                                    body: control.body,
                                }),
                                log.config.limits,
                                plan.limits.max_output_bytes,
                            )?;
                            emitted_frames += 1;
                        }
                    }
                    Frame::Control(control)
                        if control.control_kind == P9_CONTROL_ADVANCE_COMPACTION_SAFE_OFFSET =>
                    {
                        if control.control_version != P9_MAINTENANCE_CONTROL_VERSION {
                            return Err(StorageError::InvalidFormat(
                                "unsupported P9-G compaction-safe rewrite source version",
                            ));
                        }
                        let _ = decode_compaction_safe_offset_body(&control.body)?;
                        if history
                            .latest_compaction_safe_anchor
                            .as_ref()
                            .is_some_and(|anchor| anchor.raft_index == control.raft_index)
                        {
                            append_retained_frame(
                                &mut bytes,
                                RetainedFrameV2::Control(RetainedControlFrameV2 {
                                    raft_term: control.raft_term,
                                    raft_index: control.raft_index,
                                    kind: RetainedControlKindV2::AdvanceCompactionSafeOffset,
                                    body: control.body,
                                }),
                                log.config.limits,
                                plan.limits.max_output_bytes,
                            )?;
                            emitted_frames += 1;
                        }
                    }
                    Frame::Control(_) => {
                        // P4 leader/configuration controls below the P3 snapshot
                        // floor are represented by SnapshotMetadata, not retained
                        // stream history. They are intentionally not re-emitted.
                    }
                }
            }
        }
        StorageFormatVersion::V2 => {
            let file_len = fs::metadata(source_path)?.len();
            let mut position = crate::SEGMENT_HEADER_LEN_V2 as u64;
            while position < file_len {
                let scanned = read_retained_frame_at_v2(source_path, position, log.config.limits)?;
                position = position
                    .checked_add(u64::from(scanned.encoded_len))
                    .ok_or(StorageError::ArithmeticOverflow("P9-E V2 rewrite position"))?;
                if scanned.frame.raft_index().get() > plan.snapshot_floor_raft_index
                    || scanned.frame.raft_index().get() > plan.committed_raft_index
                {
                    return Err(StorageError::InvalidFormat(
                        "P9-E V2 rewrite source frame exceeds captured snapshot/commit floor",
                    ));
                }
                match scanned.frame {
                    RetainedFrameV2::SparseData(batch) => {
                        let survivors = batch
                            .records
                            .into_iter()
                            .filter(|retained| {
                                !record_is_omittable(
                                    retained.offset,
                                    &retained.record,
                                    plan.log_start_offset,
                                    &history.latest_by_key,
                                    eligible_safe,
                                )
                            })
                            .collect::<Vec<_>>();
                        if !survivors.is_empty() {
                            append_retained_frame(
                                &mut bytes,
                                RetainedFrameV2::SparseData(SparseRetainedBatchV2 {
                                    raft_term: batch.raft_term,
                                    raft_index: batch.raft_index,
                                    compression: batch.compression,
                                    records: survivors,
                                }),
                                log.config.limits,
                                plan.limits.max_output_bytes,
                            )?;
                            emitted_frames += 1;
                        }
                    }
                    RetainedFrameV2::Control(control) => match control.kind {
                        RetainedControlKindV2::AdvanceLogStartOffset => {
                            if history
                                .latest_log_start_anchor
                                .as_ref()
                                .is_some_and(|anchor| anchor.raft_index == control.raft_index)
                            {
                                append_retained_frame(
                                    &mut bytes,
                                    RetainedFrameV2::Control(control),
                                    log.config.limits,
                                    plan.limits.max_output_bytes,
                                )?;
                                emitted_frames += 1;
                            }
                        }
                        RetainedControlKindV2::ProducerStateCheckpoint => {
                            if history
                                .latest_producer_checkpoint_anchor
                                .as_ref()
                                .is_some_and(|anchor| anchor.raft_index == control.raft_index)
                            {
                                append_retained_frame(
                                    &mut bytes,
                                    RetainedFrameV2::Control(control),
                                    log.config.limits,
                                    plan.limits.max_output_bytes,
                                )?;
                                emitted_frames += 1;
                            }
                        }
                        RetainedControlKindV2::AdvanceCompactionSafeOffset => {
                            let _ = decode_compaction_safe_offset_body(&control.body)?;
                            if history
                                .latest_compaction_safe_anchor
                                .as_ref()
                                .is_some_and(|anchor| anchor.raft_index == control.raft_index)
                            {
                                append_retained_frame(
                                    &mut bytes,
                                    RetainedFrameV2::Control(control),
                                    log.config.limits,
                                    plan.limits.max_output_bytes,
                                )?;
                                emitted_frames += 1;
                            }
                        }
                    },
                }
            }
        }
    }

    if emitted_frames == 0 {
        return Ok(None);
    }
    Ok(Some((replacement_name, bytes)))
}

fn append_retained_frame(
    output: &mut Vec<u8>,
    frame: RetainedFrameV2,
    storage_limits: StorageLimits,
    max_output_bytes: u64,
) -> Result<()> {
    let encoded = encode_retained_frame_v2(&frame, storage_limits)?;
    let next_len = output
        .len()
        .checked_add(encoded.len())
        .ok_or(StorageError::ArithmeticOverflow("P9-E replacement bytes"))?;
    if u64::try_from(next_len)
        .map_err(|_| StorageError::ArithmeticOverflow("P9-E replacement bytes"))?
        > max_output_bytes
    {
        return Err(StorageError::LimitExceeded {
            what: "P9-E replacement output bytes",
            actual: next_len,
            max: usize::try_from(max_output_bytes).unwrap_or(usize::MAX),
        });
    }
    output.extend_from_slice(&encoded);
    Ok(())
}

#[cfg(test)]
mod tests {
    use std::{
        fs,
        path::PathBuf,
        time::{SystemTime, UNIX_EPOCH},
    };

    use astralog_common::{PartitionId, RaftIndex, RaftTerm, StreamOffset, TopicId};

    use super::*;
    use crate::{
        Compression, ControlEntry, DataRecord, Frame, PartitionLogConfig, ProducerIdentity,
        RecordBatch, UserHeader, load_manifest, scan_retained_segment_v2,
    };

    #[test]
    fn p9g_log_start_advance_cannot_bypass_tombstone_safe_offset_grace() {
        let tombstone = DataRecord {
            timestamp_ms: 0,
            key: Some(b"key".to_vec()),
            value: None,
            headers: Vec::new(),
        };
        let none = BTreeMap::new();
        assert!(
            !record_is_omittable(
                StreamOffset::new(3),
                &tombstone,
                StreamOffset::new(10),
                &none,
                None,
            ),
            "log-start floor cannot grant tombstone removal authority"
        );
        assert!(
            !record_is_omittable(
                StreamOffset::new(3),
                &tombstone,
                StreamOffset::new(10),
                &none,
                Some(StreamOffset::new(3)),
            ),
            "committed safe lower bound is exclusive"
        );
        assert!(record_is_omittable(
            StreamOffset::new(3),
            &tombstone,
            StreamOffset::new(10),
            &none,
            Some(StreamOffset::new(4)),
        ));
    }

    #[test]
    fn p9g_real_segment_clock_rollback_and_future_successor_never_establish_grace() {
        let directory = test_directory("p9g-real-clock-rollback");
        let mut log = open(&directory, 0xbd);
        log.append(
            &batch(
                1,
                0,
                Compression::None,
                vec![record(0, Some(b"clock-key"), Some(b"old"), None)],
            ),
            0,
        )
        .unwrap();
        // Real on-disk V1 successor timestamps, rather than a fabricated
        // clean-progress sidecar, are the conservative segment-close oracle.
        log.roll(10).unwrap();
        assert_eq!(
            successor_closed_segment_bound(&log.segment_paths, 0).unwrap(),
            Some(10),
        );
        let safe = StreamOffset::new(2);
        let gate = |now_ms, grace_ms| {
            source_tombstone_safe_bound(
                Some(TombstoneRemovalGate { now_ms, grace_ms }),
                successor_closed_segment_bound(&log.segment_paths, 0).unwrap(),
                safe,
            )
        };
        assert_eq!(
            gate(109, 100),
            None,
            "grace is exclusive of the last millisecond"
        );
        assert_eq!(gate(110, 100), Some(safe), "grace holds at the exact bound");
        assert_eq!(
            gate(110, 0),
            None,
            "storage must reject zero tombstone grace even if direct callers bypass controller validation",
        );
        assert_eq!(
            gate(9, 100),
            None,
            "a future successor provides no elapsed age"
        );
        assert_eq!(
            gate(i64::MAX, u64::MAX),
            None,
            "unrepresentable grace is never assumed elapsed"
        );
        assert_eq!(
            source_tombstone_safe_bound(
                Some(TombstoneRemovalGate {
                    now_ms: 110,
                    grace_ms: 100,
                }),
                Some(10),
                StreamOffset::new(0),
            ),
            None,
            "even elapsed grace cannot substitute for committed safe authority",
        );

        // The filesystem can contain a successor created under a later
        // clock rollback. The previous segment must not be assumed closed
        // long enough merely because now is far in the future.
        log.append(
            &batch(
                2,
                1,
                Compression::None,
                vec![record(0, Some(b"clock-key"), None, None)],
            ),
            10,
        )
        .unwrap();
        log.roll(9).unwrap();
        assert_eq!(
            successor_closed_segment_bound(&log.segment_paths, 1).unwrap(),
            None,
            "a successor created before its source is not evidence of closure age",
        );
        assert_eq!(
            source_tombstone_safe_bound(
                Some(TombstoneRemovalGate {
                    now_ms: i64::MAX,
                    grace_ms: 1,
                }),
                successor_closed_segment_bound(&log.segment_paths, 1).unwrap(),
                safe,
            ),
            None,
            "extreme future time must not override a reversed on-disk clock",
        );
        assert_eq!(
            source_tombstone_safe_bound(
                Some(TombstoneRemovalGate {
                    now_ms: i64::MAX,
                    grace_ms: 1,
                }),
                Some(i64::MIN),
                safe,
            ),
            None,
            "timestamp subtraction overflow must fail closed",
        );

        drop(log);
        fs::remove_dir_all(directory).unwrap();
    }

    fn test_directory(label: &str) -> PathBuf {
        let nonce = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let path = std::env::temp_dir().join(format!(
            "astralog-p9e-{label}-{}-{nonce}",
            std::process::id()
        ));
        fs::create_dir_all(&path).unwrap();
        path
    }

    fn record(
        timestamp_ms: i64,
        key: Option<&[u8]>,
        value: Option<&[u8]>,
        header: Option<(&[u8], &[u8])>,
    ) -> DataRecord {
        DataRecord {
            timestamp_ms,
            key: key.map(ToOwned::to_owned),
            value: value.map(ToOwned::to_owned),
            headers: header
                .map(|(name, value)| {
                    vec![UserHeader {
                        name: name.to_vec(),
                        value: Some(value.to_vec()),
                    }]
                })
                .unwrap_or_default(),
        }
    }

    fn batch(
        raft: u64,
        base_offset: u64,
        compression: Compression,
        records: Vec<DataRecord>,
    ) -> Frame {
        Frame::Data(RecordBatch {
            raft_term: RaftTerm::new(1),
            raft_index: RaftIndex::new(raft).unwrap(),
            base_stream_offset: StreamOffset::new(base_offset),
            producer: None,
            compression,
            records,
        })
    }

    fn producer_batch(raft: u64, base_offset: u64, records: Vec<DataRecord>) -> Frame {
        Frame::Data(RecordBatch {
            raft_term: RaftTerm::new(1),
            raft_index: RaftIndex::new(raft).unwrap(),
            base_stream_offset: StreamOffset::new(base_offset),
            producer: Some(ProducerIdentity {
                producer_id: 7,
                producer_epoch: 1,
                base_sequence: 0,
            }),
            compression: Compression::None,
            records,
        })
    }

    fn open(directory: &Path, topic_byte: u8) -> PartitionLog {
        PartitionLog::open_or_create(
            directory,
            TopicId::new([topic_byte; 16]),
            PartitionId::new(0),
            0,
            PartitionLogConfig::default(),
        )
        .unwrap()
    }

    fn plan(log: &mut PartitionLog, snapshot_floor: u64) -> CompactionPlan {
        log.plan_keyed_compaction(CompactionPlanSpec {
            policy_epoch: 44,
            snapshot_floor_raft_index: snapshot_floor,
            producer_checkpoint: None,
            tombstone_removal: None,
            limits: CompactionLimits::default(),
        })
        .unwrap()
    }

    fn offsets(log: &PartitionLog) -> Vec<u64> {
        log.read_committed_records_from_bounded(StreamOffset::new(0), 1024, usize::MAX)
            .unwrap()
            .0
            .into_iter()
            .map(|stored| stored.offset.get())
            .collect()
    }

    #[test]
    fn p9e_same_batch_sparse_compaction_preserves_null_latest_tombstone_and_exact_bytes() {
        let directory = test_directory("same-batch");
        let mut log = open(&directory, 0xe1);
        let null_record = record(101, None, Some(b"null-key"), Some((b"h", b"n")));
        let latest_a = record(102, Some(b"A"), Some(b"new-A"), Some((b"h", b"a")));
        let tombstone_b = record(104, Some(b"B"), None, Some((b"h", b"t")));
        log.append(
            &batch(
                1,
                0,
                Compression::None,
                vec![
                    record(100, Some(b"A"), Some(b"old-A"), None),
                    null_record.clone(),
                    latest_a.clone(),
                    record(103, Some(b"B"), Some(b"old-B"), None),
                    tombstone_b.clone(),
                ],
            ),
            0,
        )
        .unwrap();
        log.roll(10).unwrap();
        log.append(
            &batch(
                2,
                5,
                Compression::None,
                vec![record(105, Some(b"C"), Some(b"active"), None)],
            ),
            10,
        )
        .unwrap();
        log.flush_data().unwrap();
        log.mark_committed(2).unwrap();

        let before_high = log.committed_stream_high_watermark().unwrap();
        let compact = plan(&mut log, 1);
        assert_eq!(compact.source_file_names.len(), 1);
        let outcome = log.execute_keyed_compaction(&compact, 44, 1).unwrap();
        assert_eq!(outcome.replaced_segments, 1);
        assert_eq!(log.committed_stream_high_watermark().unwrap(), before_high);
        assert_eq!(offsets(&log), vec![1, 2, 4, 5]);

        let retained = log
            .read_committed_records_from_bounded(StreamOffset::new(0), 32, usize::MAX)
            .unwrap()
            .0;
        assert_eq!(retained[0].record, null_record);
        assert_eq!(retained[1].record, latest_a);
        assert_eq!(retained[2].record, tombstone_b);
        let hole = log
            .read_committed_records_from_bounded(StreamOffset::new(3), 1, usize::MAX)
            .unwrap()
            .0;
        assert_eq!(hole[0].offset.get(), 4);
        fs::remove_dir_all(directory).unwrap();
    }

    fn build_cross_codec(directory: &Path) -> PartitionLog {
        let mut log = open(directory, 0xe2);
        let large_a = vec![b'a'; 512];
        let large_b = vec![b'b'; 512];
        let large_c = vec![b'c'; 512];
        log.append(
            &batch(
                1,
                0,
                Compression::None,
                vec![
                    record(10, Some(b"shared"), Some(b"v1"), None),
                    record(11, None, Some(&large_a), None),
                ],
            ),
            0,
        )
        .unwrap();
        log.roll(10).unwrap();
        log.append(
            &batch(
                2,
                2,
                Compression::Lz4,
                vec![
                    record(20, Some(b"shared"), Some(b"v2"), None),
                    record(21, None, Some(&large_b), None),
                ],
            ),
            10,
        )
        .unwrap();
        log.roll(20).unwrap();
        log.append(
            &batch(
                3,
                4,
                Compression::Zstd,
                vec![
                    record(30, Some(b"shared"), Some(b"v3"), None),
                    record(31, Some(b"other"), Some(b"old-other"), None),
                    record(32, None, Some(&large_c), None),
                ],
            ),
            20,
        )
        .unwrap();
        log.roll(30).unwrap();
        log.append(
            &batch(
                4,
                7,
                Compression::None,
                vec![record(40, Some(b"other"), Some(b"new-other"), None)],
            ),
            30,
        )
        .unwrap();
        log.flush_data().unwrap();
        log.mark_committed(4).unwrap();
        log
    }

    #[test]
    fn p9g_full_prefix_proof_requires_all_unselected_segments_and_persists_generation() {
        let directory = test_directory("p9g-clean-prefix");
        let mut log = build_cross_codec(&directory);
        let plan = plan(&mut log, 3);
        let limits = CompactionLimits::default();
        assert_eq!(
            log.verify_whole_clean_compacted_prefix(3, limits).unwrap(),
            StreamOffset::new(0),
            "superseded oldest keyed value still survives before COW"
        );
        log.execute_keyed_compaction(&plan, 44, 3).unwrap();
        let generation = log.manifest_generation().unwrap();
        let store = crate::LocalCleanProgressStore::new(
            &directory,
            TopicId::new([0xe2; 16]),
            PartitionId::new(0),
        );
        assert_eq!(
            log.verify_whole_clean_compacted_prefix(3, limits).unwrap(),
            StreamOffset::new(7),
        );
        assert_eq!(
            store.load_conservative(Some(generation)),
            StreamOffset::new(7)
        );
        assert_eq!(
            store.load_conservative(Some(generation + 1)),
            StreamOffset::new(0),
            "a newer manifest never inherits sidecar progress without proof"
        );
        drop(log);
        let restarted = open(&directory, 0xe2);
        assert_eq!(restarted.manifest_generation(), Some(generation));
        assert_eq!(
            store.load_conservative(restarted.manifest_generation()),
            StreamOffset::new(7),
        );
        drop(restarted);
        fs::remove_dir_all(directory).unwrap();
    }

    #[test]
    fn p9g_proven_safe_prefix_survives_control_roll_and_uncommitted_suffix() {
        let directory = test_directory("p9g-safe-prefix-roll");
        let mut log = build_cross_codec(&directory);
        let cleaner = plan(&mut log, 3);
        log.execute_keyed_compaction(&cleaner, 44, 3).unwrap();
        let limits = CompactionLimits::default();
        let safe = StreamOffset::new(7);
        assert_eq!(
            log.verify_whole_clean_compacted_prefix(3, limits).unwrap(),
            safe
        );

        // Exactly this ordering occurs with RF3 safe-offset maintenance:
        // the control itself is quorum committed after the source P3 snapshot.
        log.append(
            &Frame::Control(ControlEntry {
                raft_term: RaftTerm::new(1),
                raft_index: RaftIndex::new(5).unwrap(),
                control_kind: P9_CONTROL_ADVANCE_COMPACTION_SAFE_OFFSET,
                control_version: P9_MAINTENANCE_CONTROL_VERSION,
                body: crate::encode_compaction_safe_offset_body(safe).to_vec(),
            }),
            40,
        )
        .unwrap();
        log.flush_data().unwrap();
        log.mark_committed(5).unwrap();
        assert_eq!(log.committed_compaction_safe_offset().unwrap(), safe);
        log.roll(50).unwrap();
        // A subsequent leader may append a newer keyed record and roll it to
        // another immutable V1 file, but this suffix has NOT quorum committed.
        // The data is strictly outside the certified exclusive bound. It must
        // neither grant new safe authority nor revoke the proven old prefix.
        log.append(
            &batch(
                6,
                8,
                Compression::None,
                vec![record(51, Some(b"shared"), Some(b"uncommitted-new"), None)],
            ),
            51,
        )
        .unwrap();
        log.roll(60).unwrap();
        assert_eq!(log.committed_raft_index(), 5);
        assert_eq!(
            log.verify_whole_clean_compacted_prefix(3, limits).unwrap(),
            safe,
            "new outside-prefix, uncommitted data and controls may not revoke committed safe"
        );
        assert_eq!(
            log.validate_and_publish_rebootstrap_clean_progress(3, limits)
                .unwrap(),
            safe,
        );
        let generation = log.manifest_generation().unwrap();
        let store = crate::LocalCleanProgressStore::new(
            &directory,
            TopicId::new([0xe2; 16]),
            PartitionId::new(0),
        );
        assert_eq!(store.load_conservative(Some(generation)), safe);
        assert_eq!(
            store.load_conservative(Some(generation + 1)),
            StreamOffset::new(0)
        );
        drop(log);
        let restarted = open(&directory, 0xe2);
        assert_eq!(restarted.committed_compaction_safe_offset().unwrap(), safe);
        assert_eq!(
            restarted
                .verify_whole_clean_compacted_prefix(3, limits)
                .unwrap(),
            safe,
        );
        drop(restarted);
        fs::remove_dir_all(directory).unwrap();
    }
    #[test]
    fn p9g_partial_clean_does_not_certify_uncleaned_immutable_history() {
        let directory = test_directory("p9g-partial");
        let mut log = build_cross_codec(&directory);
        let limits = CompactionLimits {
            max_source_segments: 1,
            ..CompactionLimits::default()
        };
        let selected = log
            .plan_keyed_compaction(CompactionPlanSpec {
                policy_epoch: 44,
                snapshot_floor_raft_index: 3,
                producer_checkpoint: None,
                tombstone_removal: None,
                limits,
            })
            .unwrap();
        assert_eq!(selected.source_file_names.len(), 1);
        log.execute_keyed_compaction(&selected, 44, 3).unwrap();
        let store = crate::LocalCleanProgressStore::new(
            &directory,
            TopicId::new([0xe2; 16]),
            PartitionId::new(0),
        );
        assert_eq!(
            store.load_conservative(log.manifest_generation()),
            StreamOffset::new(0),
            "a bounded partial pass must not claim an unverified whole prefix"
        );
        assert_eq!(
            log.verify_whole_clean_compacted_prefix(3, CompactionLimits::default())
                .unwrap(),
            StreamOffset::new(2),
            "older keyed value still remains in an unselected source"
        );
        assert_eq!(
            log.verify_whole_clean_compacted_prefix(3, limits).unwrap(),
            StreamOffset::new(0),
            "whole-inventory verification cannot exceed its segment count bound"
        );
        drop(log);
        fs::remove_dir_all(directory).unwrap();
    }

    #[test]
    fn p9g_cow_source_rewrite_preserves_newest_committed_safe_control_anchor() {
        let directory = test_directory("p9g-safe-anchor");
        let mut log = open(&directory, 0xe5);
        log.append(
            &batch(
                1,
                0,
                Compression::None,
                vec![record(100, Some(b"k"), Some(b"old"), None)],
            ),
            0,
        )
        .unwrap();
        log.append(
            &Frame::Control(ControlEntry {
                raft_term: RaftTerm::new(1),
                raft_index: RaftIndex::new(2).unwrap(),
                control_kind: P9_CONTROL_ADVANCE_COMPACTION_SAFE_OFFSET,
                control_version: P9_MAINTENANCE_CONTROL_VERSION,
                body: crate::encode_compaction_safe_offset_body(StreamOffset::new(1)).to_vec(),
            }),
            1,
        )
        .unwrap();
        log.roll(10).unwrap();
        log.append(
            &batch(
                3,
                1,
                Compression::None,
                vec![record(110, Some(b"k"), Some(b"new"), None)],
            ),
            10,
        )
        .unwrap();
        log.flush_data().unwrap();
        log.mark_committed(3).unwrap();
        log.adopt_manifest_inventory().unwrap();
        assert_eq!(
            log.committed_compaction_safe_offset().unwrap(),
            StreamOffset::new(1),
        );
        let history = build_history_index(
            &log,
            StreamOffset::new(0),
            CompactionLimits::default(),
            None,
        )
        .unwrap();
        assert_eq!(
            history
                .latest_compaction_safe_anchor
                .as_ref()
                .unwrap()
                .raft_index
                .get(),
            2,
        );
        let manifest = load_manifest(&directory).unwrap().unwrap();
        let plan = CompactionPlan {
            base_manifest_generation: manifest.generation,
            policy_epoch: 44,
            snapshot_floor_raft_index: 2,
            maintenance_epoch: log.meta.metadata_epoch,
            committed_raft_index: 3,
            log_start_offset: StreamOffset::new(0),
            producer_checkpoint: None,
            tombstone_removal: None,
            committed_compaction_safe_offset: StreamOffset::new(1),
            active_file_name: manifest.entries.last().unwrap().file_name.clone(),
            source_file_names: vec![manifest.entries[0].file_name.clone()],
            source_bytes: 0,
            decoded_records: history.decoded_records,
            decoded_record_bytes: history.decoded_record_bytes,
            key_index_entries: history.latest_by_key.len(),
            key_index_bytes: history.key_index_bytes,
            limits: CompactionLimits::default(),
        };
        let source = directory.join(&manifest.entries[0].file_name);
        let (_, encoded) = build_replacement_segment(&log, &source, &plan, &history, &[], None)
            .unwrap()
            .expect("a retained safe-control anchor makes nonempty V2 replacement");
        let inspect_path = directory.join("p9g-anchor-inspect.log");
        fs::write(&inspect_path, encoded).unwrap();
        let scan = scan_retained_segment_v2(&inspect_path, StorageLimits::default()).unwrap();
        assert_eq!(scan.frames.len(), 1);
        match &scan.frames[0].frame {
            RetainedFrameV2::Control(control) => {
                assert_eq!(
                    control.kind,
                    RetainedControlKindV2::AdvanceCompactionSafeOffset
                );
                assert_eq!(control.raft_index.get(), 2);
                assert_eq!(
                    decode_compaction_safe_offset_body(&control.body).unwrap(),
                    StreamOffset::new(1),
                );
            }
            _ => panic!("P9-G safe authority was lost during V1-to-V2 COW rewrite"),
        }
        // A positive committed safe bound permits ordinary value COW while
        // an absent CompactDelete gate still retains all tombstones.
        assert!(
            log.plan_keyed_compaction(CompactionPlanSpec {
                policy_epoch: 44,
                snapshot_floor_raft_index: 2,
                producer_checkpoint: None,
                tombstone_removal: None,
                limits: CompactionLimits::default(),
            })
            .is_ok()
        );
        drop(log);
        fs::remove_dir_all(directory).unwrap();
    }

    #[test]
    fn p9g_tombstone_gate_requires_full_prefix_safe_commit_snapshot_and_conservative_grace() {
        let directory = test_directory("p9g-tombstone-gate");
        let mut log = open(&directory, 0xb9);
        log.append(
            &batch(
                1,
                0,
                Compression::None,
                vec![record(0, Some(b"delete-key"), Some(b"old-value"), None)],
            ),
            0,
        )
        .unwrap();
        log.roll(10).unwrap();
        log.append(
            &batch(
                2,
                1,
                Compression::None,
                vec![record(0, Some(b"delete-key"), None, None)],
            ),
            10,
        )
        .unwrap();
        log.roll(20).unwrap();
        log.flush_data().unwrap();
        log.mark_committed(2).unwrap();
        // The latest tombstone is never discarded by P9-E even when every
        // older value in the manifest can be compacted.
        let older_plan = plan(&mut log, 2);
        assert_eq!(older_plan.source_file_names.len(), 1);
        assert_eq!(
            log.verify_whole_clean_compacted_prefix(2, CompactionLimits::default())
                .unwrap(),
            StreamOffset::new(0),
        );
        log.execute_keyed_compaction(&older_plan, 44, 2).unwrap();
        assert_eq!(offsets(&log), vec![1]);
        assert_eq!(
            log.verify_whole_clean_compacted_prefix(2, CompactionLimits::default())
                .unwrap(),
            StreamOffset::new(2),
        );
        log.append(
            &Frame::Control(ControlEntry {
                raft_term: RaftTerm::new(1),
                raft_index: RaftIndex::new(3).unwrap(),
                control_kind: P9_CONTROL_ADVANCE_COMPACTION_SAFE_OFFSET,
                control_version: P9_MAINTENANCE_CONTROL_VERSION,
                body: crate::encode_compaction_safe_offset_body(StreamOffset::new(2)).to_vec(),
            }),
            21,
        )
        .unwrap();
        log.flush_data().unwrap();
        log.mark_committed(3).unwrap();
        assert_eq!(
            log.committed_compaction_safe_offset().unwrap(),
            StreamOffset::new(2)
        );
        let gated = |log: &mut PartitionLog, now_ms: i64, grace_ms: u64, floor: u64| {
            log.plan_keyed_compaction(CompactionPlanSpec {
                policy_epoch: 47,
                snapshot_floor_raft_index: floor,
                producer_checkpoint: None,
                tombstone_removal: Some(TombstoneRemovalGate { now_ms, grace_ms }),
                limits: CompactionLimits::default(),
            })
        };
        assert!(
            gated(&mut log, 500, 100, 1).is_err(),
            "source frame cannot be cleaned above persisted P3 snapshot floor"
        );
        assert!(
            gated(&mut log, -1, 100, 2).unwrap().is_empty(),
            "backward clock must delay tombstone removal"
        );
        assert!(
            gated(&mut log, 119, 100, 2).unwrap().is_empty(),
            "successor-created-at 20 means now=119 is one millisecond too young"
        );
        let ready = gated(&mut log, 120, 100, 2).unwrap();
        assert_eq!(ready.source_file_names.len(), 1);
        assert_eq!(ready.committed_compaction_safe_offset, StreamOffset::new(2));
        assert_eq!(ready.tombstone_removal.unwrap().grace_ms, 100);
        assert!(
            log.execute_keyed_compaction(&ready, 48, 2).is_err(),
            "stale controller policy epoch must block COW publication"
        );
        let outcome = log.execute_keyed_compaction(&ready, 47, 2).unwrap();
        assert_eq!(outcome.replaced_segments, 1);
        assert!(
            offsets(&log).is_empty(),
            "no older value can resurrect after marker removal"
        );
        assert_eq!(
            log.committed_compaction_safe_offset().unwrap(),
            StreamOffset::new(2),
            "the committed safety anchor must survive source COW"
        );
        drop(log);
        let reopened = open(&directory, 0xb9);
        assert!(offsets(&reopened).is_empty());
        assert_eq!(
            reopened.committed_compaction_safe_offset().unwrap(),
            StreamOffset::new(2)
        );
        drop(reopened);
        fs::remove_dir_all(directory).unwrap();
    }

    #[test]
    fn p9g_rf3_retained_history_rewrite_does_not_resurrect_value_on_replica_change() {
        let mut replicas = Vec::new();
        let mut directories = Vec::new();
        for index in 0..3 {
            let dir = test_directory(&format!("p9g-rf3-model-{index}"));
            let mut log = open(&dir, 0xd9);
            log.append(
                &batch(
                    1,
                    0,
                    Compression::None,
                    vec![record(0, Some(b"shared-key"), Some(b"obsolete"), None)],
                ),
                0,
            )
            .unwrap();
            log.roll(10).unwrap();
            log.append(
                &batch(
                    2,
                    1,
                    Compression::None,
                    vec![record(0, Some(b"shared-key"), None, None)],
                ),
                10,
            )
            .unwrap();
            log.roll(20).unwrap();
            log.flush_data().unwrap();
            log.mark_committed(2).unwrap();
            let old = plan(&mut log, 2);
            log.execute_keyed_compaction(&old, 44, 2).unwrap();
            assert_eq!(offsets(&log), vec![1]);
            assert_eq!(
                log.verify_whole_clean_compacted_prefix(2, CompactionLimits::default())
                    .unwrap(),
                StreamOffset::new(2)
            );
            log.append(
                &Frame::Control(ControlEntry {
                    raft_term: RaftTerm::new(1),
                    raft_index: RaftIndex::new(3).unwrap(),
                    control_kind: P9_CONTROL_ADVANCE_COMPACTION_SAFE_OFFSET,
                    control_version: P9_MAINTENANCE_CONTROL_VERSION,
                    body: crate::encode_compaction_safe_offset_body(StreamOffset::new(2)).to_vec(),
                }),
                22,
            )
            .unwrap();
            log.flush_data().unwrap();
            log.mark_committed(3).unwrap();
            directories.push(dir);
            replicas.push(log);
        }
        // A first leader physically removes its marker only after all three
        // have cleaned the same older value and observed the same committed
        // replicated safe anchor. Two other replicas keep their tombstone.
        let gate = TombstoneRemovalGate {
            now_ms: 120,
            grace_ms: 100,
        };
        let leader_plan = replicas[0]
            .plan_keyed_compaction(CompactionPlanSpec {
                policy_epoch: 47,
                snapshot_floor_raft_index: 2,
                producer_checkpoint: None,
                tombstone_removal: Some(gate),
                limits: CompactionLimits::default(),
            })
            .unwrap();
        replicas[0]
            .execute_keyed_compaction(&leader_plan, 47, 2)
            .unwrap();
        assert!(offsets(&replicas[0]).is_empty());
        assert_eq!(offsets(&replicas[1]), vec![1]);
        assert_eq!(offsets(&replicas[2]), vec![1]);
        // Elect replica 2 and remove its marker. It cannot reveal an older
        // value because its complete prefix was independently cleaned before
        // it contributed to the committed safe boundary.
        let successor_plan = replicas[1]
            .plan_keyed_compaction(CompactionPlanSpec {
                policy_epoch: 47,
                snapshot_floor_raft_index: 2,
                producer_checkpoint: None,
                tombstone_removal: Some(gate),
                limits: CompactionLimits::default(),
            })
            .unwrap();
        replicas[1]
            .execute_keyed_compaction(&successor_plan, 47, 2)
            .unwrap();
        assert!(offsets(&replicas[1]).is_empty());
        for replica in &replicas {
            assert_eq!(
                replica.committed_compaction_safe_offset().unwrap(),
                StreamOffset::new(2)
            );
            assert!(
                offsets(replica).iter().all(|offset| *offset != 0),
                "no replica may re-expose the obsolete value"
            );
        }
        drop(replicas);
        for directory in directories {
            let reopened = open(&directory, 0xd9);
            assert!(offsets(&reopened).iter().all(|offset| *offset != 0));
            assert_eq!(
                reopened.committed_compaction_safe_offset().unwrap(),
                StreamOffset::new(2)
            );
            drop(reopened);
            fs::remove_dir_all(directory).unwrap();
        }
    }

    #[test]
    fn p9g_physical_delete_retention_preserves_latest_safe_anchor_segment() {
        let directory = test_directory("p9g-retention-anchor");
        let mut log = open(&directory, 0xe9);
        log.append(
            &batch(
                1,
                0,
                Compression::None,
                vec![record(1, Some(b"key"), Some(b"old"), None)],
            ),
            0,
        )
        .unwrap();
        log.roll(10).unwrap();
        log.append(
            &Frame::Control(ControlEntry {
                raft_term: RaftTerm::new(1),
                raft_index: RaftIndex::new(2).unwrap(),
                control_kind: P9_CONTROL_ADVANCE_COMPACTION_SAFE_OFFSET,
                control_version: P9_MAINTENANCE_CONTROL_VERSION,
                body: crate::encode_compaction_safe_offset_body(StreamOffset::new(1)).to_vec(),
            }),
            10,
        )
        .unwrap();
        log.roll(20).unwrap();
        log.append(
            &Frame::Control(ControlEntry {
                raft_term: RaftTerm::new(1),
                raft_index: RaftIndex::new(3).unwrap(),
                control_kind: P9_CONTROL_ADVANCE_LOG_START_OFFSET,
                control_version: P9_MAINTENANCE_CONTROL_VERSION,
                body: crate::encode_log_start_offset_body(StreamOffset::new(1)).to_vec(),
            }),
            20,
        )
        .unwrap();
        log.roll(30).unwrap();
        log.flush_data().unwrap();
        log.mark_committed(3).unwrap();
        log.adopt_manifest_inventory().unwrap();
        let initial_generation = log.manifest_generation().unwrap();
        assert_eq!(
            log.committed_compaction_safe_offset().unwrap(),
            StreamOffset::new(1)
        );
        assert_eq!(
            log.committed_log_start_offset().unwrap(),
            StreamOffset::new(1)
        );
        let deleted = log
            .publish_delete_retention_generation(initial_generation, 3)
            .unwrap();
        assert_eq!(
            deleted, 1,
            "only the oldest obsolete data segment may be retired"
        );
        assert_eq!(
            log.committed_compaction_safe_offset().unwrap(),
            StreamOffset::new(1)
        );
        assert_eq!(
            log.committed_log_start_offset().unwrap(),
            StreamOffset::new(1)
        );
        // A second publication may never delete the later safe anchor merely
        // because another log-start or physical-age gate permits it.
        assert_eq!(
            log.publish_delete_retention_generation(log.manifest_generation().unwrap(), 3,)
                .unwrap(),
            0
        );
        drop(log);
        let reopened = open(&directory, 0xe9);
        assert_eq!(
            reopened.committed_compaction_safe_offset().unwrap(),
            StreamOffset::new(1)
        );
        assert_eq!(
            reopened.committed_log_start_offset().unwrap(),
            StreamOffset::new(1)
        );
        drop(reopened);
        fs::remove_dir_all(directory).unwrap();
    }

    #[test]
    fn p9g_retention_preserves_p9f_checkpoint_even_when_safe_anchor_is_newer() {
        let directory = test_directory("p9g-retention-p7-checkpoint");
        let mut log = open(&directory, 0xf9);
        log.append(
            &batch(
                1,
                0,
                Compression::None,
                vec![record(0, Some(b"k"), Some(b"old"), None)],
            ),
            0,
        )
        .unwrap();
        log.roll(10).unwrap();
        log.append(
            &Frame::Control(ControlEntry {
                raft_term: RaftTerm::new(1),
                raft_index: RaftIndex::new(2).unwrap(),
                control_kind: P9_CONTROL_PRODUCER_STATE_CHECKPOINT,
                control_version: P9_MAINTENANCE_CONTROL_VERSION,
                body: vec![0x61, 0x62], // Storage treats committed P7 bytes as opaque.
            }),
            10,
        )
        .unwrap();
        log.roll(20).unwrap();
        log.append(
            &Frame::Control(ControlEntry {
                raft_term: RaftTerm::new(1),
                raft_index: RaftIndex::new(3).unwrap(),
                control_kind: P9_CONTROL_ADVANCE_COMPACTION_SAFE_OFFSET,
                control_version: P9_MAINTENANCE_CONTROL_VERSION,
                body: crate::encode_compaction_safe_offset_body(StreamOffset::new(1)).to_vec(),
            }),
            20,
        )
        .unwrap();
        log.roll(30).unwrap();
        log.append(
            &Frame::Control(ControlEntry {
                raft_term: RaftTerm::new(1),
                raft_index: RaftIndex::new(4).unwrap(),
                control_kind: P9_CONTROL_ADVANCE_LOG_START_OFFSET,
                control_version: P9_MAINTENANCE_CONTROL_VERSION,
                body: crate::encode_log_start_offset_body(StreamOffset::new(1)).to_vec(),
            }),
            30,
        )
        .unwrap();
        log.roll(40).unwrap();
        log.flush_data().unwrap();
        log.mark_committed(4).unwrap();
        log.adopt_manifest_inventory().unwrap();
        assert_eq!(
            log.committed_compaction_safe_offset().unwrap(),
            StreamOffset::new(1)
        );
        let generation = log.manifest_generation().unwrap();
        let projected = log
            .plan_delete_retention(crate::DeleteRetentionSpec {
                now_ms: 100,
                retention_ms: Some(1),
                retention_bytes: Some(1),
                protected_log_start_ceiling: None,
                snapshot_floor_raft_index: 4,
            })
            .unwrap();
        assert_eq!(projected.proposed_log_start_offset, StreamOffset::new(1));
        assert_eq!(
            log.publish_delete_retention_generation(generation, 4)
                .unwrap(),
            1
        );
        let physical_retained_bytes = log
            .segment_paths()
            .iter()
            .map(|path| fs::metadata(path).unwrap().len())
            .sum::<u64>();
        assert_eq!(
            projected.retained_log_bytes_after_plan, physical_retained_bytes,
            "retention metrics cannot count the retained P9-F checkpoint as reclaimed bytes"
        );
        assert_eq!(
            log.latest_committed_producer_checkpoint_control()
                .unwrap()
                .unwrap()
                .raft_index
                .get(),
            2
        );
        assert_eq!(
            log.publish_delete_retention_generation(log.manifest_generation().unwrap(), 4,)
                .unwrap(),
            0,
            "newer log-start and compaction-safe controls cannot permit deleting the sole P7 checkpoint"
        );
        drop(log);
        let reopened = open(&directory, 0xf9);
        assert_eq!(
            reopened
                .latest_committed_producer_checkpoint_control()
                .unwrap()
                .unwrap()
                .raft_index
                .get(),
            2
        );
        assert_eq!(
            reopened.committed_compaction_safe_offset().unwrap(),
            StreamOffset::new(1)
        );
        drop(reopened);
        fs::remove_dir_all(directory).unwrap();
    }

    #[test]
    fn p9e_cross_codec_multi_segment_compaction_is_deterministic_and_keeps_offsets() {
        let first_dir = test_directory("codec-a");
        let second_dir = test_directory("codec-b");
        let mut first = build_cross_codec(&first_dir);
        let mut second = build_cross_codec(&second_dir);
        let first_plan = plan(&mut first, 3);
        let second_plan = plan(&mut second, 3);
        assert_eq!(first_plan, second_plan);
        assert_eq!(first_plan.source_file_names.len(), 3);
        first.execute_keyed_compaction(&first_plan, 44, 3).unwrap();
        second
            .execute_keyed_compaction(&second_plan, 44, 3)
            .unwrap();
        assert_eq!(
            fs::read(first_dir.join("segments.manifest")).unwrap(),
            fs::read(second_dir.join("segments.manifest")).unwrap()
        );
        assert_eq!(offsets(&first), vec![1, 3, 4, 6, 7]);
        assert_eq!(offsets(&first), offsets(&second));

        let manifest = load_manifest(&first_dir).unwrap().unwrap();
        let mut codecs = Vec::new();
        for entry in manifest
            .entries
            .iter()
            .filter(|entry| !entry.is_mutable_active())
        {
            let path = first_dir.join(&entry.file_name);
            if read_segment_format(&path).unwrap() == StorageFormatVersion::V2 {
                for frame in scan_retained_segment_v2(&path, StorageLimits::default())
                    .unwrap()
                    .frames
                {
                    if let RetainedFrameV2::SparseData(batch) = frame.frame {
                        codecs.push(batch.compression);
                    }
                }
            }
        }
        assert!(codecs.contains(&Compression::None));
        assert!(codecs.contains(&Compression::Lz4));
        assert!(codecs.contains(&Compression::Zstd));
        fs::remove_dir_all(first_dir).unwrap();
        fs::remove_dir_all(second_dir).unwrap();
    }

    #[test]
    fn p9e_latest_log_start_anchor_survives_rewrite() {
        let directory = test_directory("maintenance-anchor");
        let mut log = open(&directory, 0xe3);
        log.append(
            &batch(
                1,
                0,
                Compression::None,
                vec![record(1, Some(b"K"), Some(b"expired"), None)],
            ),
            0,
        )
        .unwrap();
        log.append(
            &Frame::Control(ControlEntry {
                raft_term: RaftTerm::new(1),
                raft_index: RaftIndex::new(2).unwrap(),
                control_kind: P9_CONTROL_ADVANCE_LOG_START_OFFSET,
                control_version: P9_MAINTENANCE_CONTROL_VERSION,
                body: crate::encode_log_start_offset_body(StreamOffset::new(1)).to_vec(),
            }),
            1,
        )
        .unwrap();
        log.roll(10).unwrap();
        log.append(
            &batch(
                3,
                1,
                Compression::None,
                vec![record(2, Some(b"K"), Some(b"live"), None)],
            ),
            10,
        )
        .unwrap();
        log.flush_data().unwrap();
        log.mark_committed(3).unwrap();
        assert_eq!(log.committed_log_start_offset().unwrap().get(), 1);
        let compact = plan(&mut log, 2);
        log.execute_keyed_compaction(&compact, 44, 2).unwrap();
        assert_eq!(log.committed_log_start_offset().unwrap().get(), 1);
        assert_eq!(offsets(&log), vec![1]);
        let manifest = load_manifest(&directory).unwrap().unwrap();
        let retained = scan_retained_segment_v2(
            &directory.join(&manifest.entries[0].file_name),
            StorageLimits::default(),
        )
        .unwrap();
        assert!(retained.frames.iter().any(|frame| matches!(
            &frame.frame,
            RetainedFrameV2::Control(control)
                if control.kind == RetainedControlKindV2::AdvanceLogStartOffset
                    && control.raft_index.get() == 2
        )));
        fs::remove_dir_all(directory).unwrap();
    }

    #[test]
    fn p9e_bounds_stale_fences_and_p7_source_guard_fail_closed() {
        let directory = test_directory("bounds-stale");
        let mut log = open(&directory, 0xe4);
        log.append(
            &batch(
                1,
                0,
                Compression::None,
                vec![
                    record(1, Some(b"A"), Some(b"old"), None),
                    record(2, Some(b"A"), Some(b"new"), None),
                ],
            ),
            0,
        )
        .unwrap();
        log.roll(10).unwrap();
        log.append(
            &batch(
                2,
                2,
                Compression::None,
                vec![record(3, Some(b"B"), Some(b"active"), None)],
            ),
            10,
        )
        .unwrap();
        log.flush_data().unwrap();
        log.mark_committed(2).unwrap();

        let tiny = CompactionLimits {
            max_decoded_records: 2,
            ..CompactionLimits::default()
        };
        assert!(matches!(
            log.plan_keyed_compaction(CompactionPlanSpec {
                policy_epoch: 44,
                snapshot_floor_raft_index: 1,
                producer_checkpoint: None,
                tombstone_removal: None,
                limits: tiny,
            }),
            Err(StorageError::LimitExceeded { .. })
        ));
        assert!(matches!(
            log.plan_keyed_compaction(CompactionPlanSpec {
                policy_epoch: 44,
                snapshot_floor_raft_index: 1,
                producer_checkpoint: None,
                tombstone_removal: None,
                limits: CompactionLimits {
                    max_source_bytes: 1,
                    ..CompactionLimits::default()
                },
            }),
            Err(StorageError::LimitExceeded { .. })
        ));
        assert!(matches!(
            log.plan_keyed_compaction(CompactionPlanSpec {
                policy_epoch: 44,
                snapshot_floor_raft_index: 1,
                producer_checkpoint: None,
                tombstone_removal: None,
                limits: CompactionLimits {
                    max_key_index_entries: 1,
                    ..CompactionLimits::default()
                },
            }),
            Err(StorageError::LimitExceeded { .. })
        ));

        let compact = plan(&mut log, 1);
        let base_generation = log.manifest_generation().unwrap();
        let mut active_source = compact.clone();
        active_source.source_file_names = vec![active_source.active_file_name.clone()];
        assert!(log.execute_keyed_compaction(&active_source, 44, 1).is_err());
        let mut above_snapshot = compact.clone();
        above_snapshot.snapshot_floor_raft_index = 0;
        assert!(
            log.execute_keyed_compaction(&above_snapshot, 44, 0)
                .is_err(),
            "a forged source above the captured snapshot floor must fail closed"
        );
        let mut stale_manifest = compact.clone();
        stale_manifest.base_manifest_generation = stale_manifest
            .base_manifest_generation
            .checked_add(1)
            .unwrap();
        assert!(
            log.execute_keyed_compaction(&stale_manifest, 44, 1)
                .is_err()
        );
        assert!(log.execute_keyed_compaction(&compact, 45, 1).is_err());
        assert!(log.execute_keyed_compaction(&compact, 44, 2).is_err());
        let prepared = log.prepare_compaction_generation(&compact, 44, 1).unwrap();
        let publication_epoch = RwLock::new(44_u64);
        *publication_epoch.write().unwrap() = 45;
        assert!(
            log.publish_prepared_compaction(prepared, 44, 1, Some(&publication_epoch))
                .is_err(),
            "metadata drift observed after staging must abort before manifest publication"
        );
        assert_eq!(log.manifest_generation().unwrap(), base_generation);
        log.append(
            &batch(
                3,
                3,
                Compression::None,
                vec![record(4, Some(b"C"), Some(b"later"), None)],
            ),
            20,
        )
        .unwrap();
        log.flush_data().unwrap();
        log.mark_committed(3).unwrap();
        assert!(
            log.execute_keyed_compaction(&compact, 44, 1).is_err(),
            "new committed maintenance/log state must invalidate the old cleaner plan"
        );
        fs::remove_dir_all(directory).unwrap();

        let producer_dir = test_directory("producer-guard");
        let mut producer_log = open(&producer_dir, 0xe5);
        producer_log
            .append(
                &producer_batch(1, 0, vec![record(1, Some(b"P"), Some(b"old"), None)]),
                0,
            )
            .unwrap();
        producer_log.roll(10).unwrap();
        producer_log
            .append(
                &batch(
                    2,
                    1,
                    Compression::None,
                    vec![record(2, Some(b"P"), Some(b"new"), None)],
                ),
                10,
            )
            .unwrap();
        producer_log.flush_data().unwrap();
        producer_log.mark_committed(2).unwrap();
        let guarded = plan(&mut producer_log, 1);
        assert!(guarded.source_file_names.is_empty());
        fs::remove_dir_all(producer_dir).unwrap();
    }

    #[test]
    fn p9e_pre_switch_drop_keeps_old_generation_and_post_switch_restart_uses_new() {
        let pre_dir = test_directory("pre-switch");
        let mut pre = open(&pre_dir, 0xe6);
        pre.append(
            &batch(
                1,
                0,
                Compression::None,
                vec![
                    record(1, Some(b"K"), Some(b"old"), None),
                    record(2, Some(b"K"), Some(b"new"), None),
                ],
            ),
            0,
        )
        .unwrap();
        pre.roll(10).unwrap();
        pre.append(
            &batch(
                2,
                2,
                Compression::None,
                vec![record(3, None, Some(b"active"), None)],
            ),
            10,
        )
        .unwrap();
        pre.flush_data().unwrap();
        pre.mark_committed(2).unwrap();
        let pre_plan = plan(&mut pre, 1);
        let old_generation = pre.manifest_generation().unwrap();
        let prepared = pre.prepare_compaction_generation(&pre_plan, 44, 1).unwrap();
        drop(prepared);
        assert_eq!(
            load_manifest(&pre_dir).unwrap().unwrap().generation,
            old_generation
        );
        drop(pre);
        let reopened = open(&pre_dir, 0xe6);
        assert_eq!(offsets(&reopened), vec![0, 1, 2]);
        drop(reopened);
        fs::remove_dir_all(pre_dir).unwrap();

        let post_dir = test_directory("post-switch");
        let mut post = open(&post_dir, 0xe7);
        post.append(
            &batch(
                1,
                0,
                Compression::None,
                vec![
                    record(1, Some(b"K"), Some(b"old"), None),
                    record(2, Some(b"K"), Some(b"new"), None),
                ],
            ),
            0,
        )
        .unwrap();
        post.roll(10).unwrap();
        post.append(
            &batch(
                2,
                2,
                Compression::None,
                vec![record(3, None, Some(b"active"), None)],
            ),
            10,
        )
        .unwrap();
        post.flush_data().unwrap();
        post.mark_committed(2).unwrap();
        let post_plan = plan(&mut post, 1);
        let held = post.acquire_read_generation().unwrap();
        let outcome = post.execute_keyed_compaction(&post_plan, 44, 1).unwrap();
        assert!(outcome.manifest_generation > post_plan.base_manifest_generation);
        assert!(outcome.deferred_segments > 0);
        drop(post);
        let reopened = open(&post_dir, 0xe7);
        assert_eq!(offsets(&reopened), vec![1, 2]);
        drop(reopened);
        drop(held);
        fs::remove_dir_all(post_dir).unwrap();
    }

    #[test]
    fn p9e_reader_generation_defers_old_file_reclaim_until_last_reader_drops() {
        let directory = test_directory("reader-lifetime");
        let mut log = open(&directory, 0xe8);
        log.append(
            &batch(
                1,
                0,
                Compression::None,
                vec![
                    record(1, Some(b"K"), Some(b"old"), None),
                    record(2, Some(b"K"), Some(b"new"), None),
                ],
            ),
            0,
        )
        .unwrap();
        log.roll(10).unwrap();
        log.append(
            &batch(
                2,
                2,
                Compression::None,
                vec![record(3, None, Some(b"active"), None)],
            ),
            10,
        )
        .unwrap();
        log.flush_data().unwrap();
        log.mark_committed(2).unwrap();
        let compact = plan(&mut log, 1);
        let old_reader = log.acquire_read_generation().unwrap();
        let old_source = old_reader.segment_paths()[0].clone();
        assert_eq!(
            old_reader
                .read_committed_records_from_bounded(StreamOffset::new(0), 16, usize::MAX)
                .unwrap()
                .0
                .iter()
                .map(|record| record.offset.get())
                .collect::<Vec<_>>(),
            vec![0, 1, 2]
        );
        let outcome = log.execute_keyed_compaction(&compact, 44, 1).unwrap();
        assert!(outcome.deferred_segments > 0);
        assert!(old_source.exists());
        assert_eq!(offsets(&log), vec![1, 2]);
        assert_eq!(log.reclaim_retired_read_generations().unwrap(), 0);
        drop(old_reader);
        assert_eq!(log.reclaim_retired_read_generations().unwrap(), 1);
        assert!(!old_source.exists());
        fs::remove_dir_all(directory).unwrap();
    }
}
