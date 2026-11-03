use std::{
    fs,
    path::{Path, PathBuf},
    sync::Arc,
};

use astralog_common::{PartitionId, RaftIndex, SegmentBaseOffset, StreamOffset, TopicId};

use crate::{
    DataRecord, Frame, GenerationTransaction, ManifestEntry,
    P9_CONTROL_ADVANCE_COMPACTION_SAFE_OFFSET, P9_CONTROL_ADVANCE_LOG_START_OFFSET,
    P9_CONTROL_PRODUCER_STATE_CHECKPOINT, P9_MAINTENANCE_CONTROL_VERSION, PartitionMeta, Result,
    RetainedControlKindV2, RetainedFrameV2, ScanFailureKind, SegmentFileId, SegmentHeader,
    SegmentManifest, SegmentWriter, StorageError, StorageFormatVersion, StorageLimits,
    decode_compaction_safe_offset_body, decode_log_start_offset_body, decode_partition_meta,
    encode_frame, encode_partition_meta, encode_segment_header,
    index::{rebuild_retained_segment_indexes_v2, rebuild_segment_indexes},
    io_util::{atomic_write, sync_directory},
    load_manifest, read_retained_frame_at_v2, read_segment_format, scan_retained_segment_v2,
    scan_segment, validate_partition_manifest_files,
};

mod compaction;

pub use compaction::{
    CompactionLimits, CompactionOutcome, CompactionPlan, CompactionPlanSpec,
    ProducerCheckpointFence, TombstoneRemovalGate,
};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PartitionLogConfig {
    pub max_segment_bytes: u64,
    pub max_segment_age_ms: Option<i64>,
    pub index_interval_bytes: u64,
    pub limits: StorageLimits,
}

impl Default for PartitionLogConfig {
    fn default() -> Self {
        Self {
            max_segment_bytes: 1024 * 1024 * 1024,
            max_segment_age_ms: Some(60 * 60 * 1000),
            index_interval_bytes: 4096,
            limits: StorageLimits::default(),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StoredRecord {
    pub offset: StreamOffset,
    pub record: DataRecord,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct DeleteRetentionSpec {
    pub now_ms: i64,
    pub retention_ms: Option<u64>,
    pub retention_bytes: Option<u64>,
    pub protected_log_start_ceiling: Option<StreamOffset>,
    pub snapshot_floor_raft_index: u64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct DeleteRetentionPlan {
    pub base_manifest_generation: u64,
    pub current_log_start_offset: StreamOffset,
    pub proposed_log_start_offset: StreamOffset,
    pub authoritative_log_bytes: u64,
    pub retained_log_bytes_after_plan: u64,
}

#[derive(Debug)]
struct ReadGenerationInner {
    generation: Option<u64>,
    paths: Vec<PathBuf>,
}

#[derive(Debug)]
struct RetiredReadGeneration {
    owner: Arc<ReadGenerationInner>,
    unreachable_paths: Vec<PathBuf>,
}

/// Immutable reader ownership for one complete manifest generation.
///
/// P9-E cleaners may publish a newer generation while a reader retains this
/// handle. The old files are not reclaimed until the final handle is dropped.
#[derive(Debug, Clone)]
pub struct PartitionReadGeneration {
    inner: Arc<ReadGenerationInner>,
    limits: StorageLimits,
    committed_raft_index: u64,
    log_start_offset: StreamOffset,
}

impl PartitionReadGeneration {
    pub fn manifest_generation(&self) -> Option<u64> {
        self.inner.generation
    }

    pub fn segment_paths(&self) -> &[PathBuf] {
        &self.inner.paths
    }

    pub fn read_committed_records_from_bounded(
        &self,
        start: StreamOffset,
        max_records: usize,
        max_bytes: usize,
    ) -> Result<(Vec<StoredRecord>, usize)> {
        let start = StreamOffset::new(start.get().max(self.log_start_offset.get()));
        read_records_from_paths_bounded(
            &self.inner.paths,
            self.limits,
            start,
            max_records,
            max_bytes,
            Some(self.committed_raft_index),
        )
    }
}

pub struct PartitionLog {
    directory: PathBuf,
    topic_id: TopicId,
    partition_id: PartitionId,
    config: PartitionLogConfig,
    meta: PartitionMeta,
    segment_paths: Vec<PathBuf>,
    active: SegmentWriter,
    next_raft_index: RaftIndex,
    next_stream_offset: StreamOffset,
    durable_raft_index: u64,
    physical_sync_count: u64,
    manifest_generation: Option<u64>,
    read_generation: Arc<ReadGenerationInner>,
    retired_read_generations: Vec<RetiredReadGeneration>,
}

impl PartitionLog {
    pub fn open_or_create(
        directory: &Path,
        topic_id: TopicId,
        partition_id: PartitionId,
        now_ms: i64,
        config: PartitionLogConfig,
    ) -> Result<Self> {
        if config.max_segment_bytes <= crate::format::SEGMENT_HEADER_LEN as u64 {
            return Err(StorageError::InvalidFormat(
                "max segment bytes must exceed segment header length",
            ));
        }
        if config.index_interval_bytes == 0 {
            return Err(StorageError::InvalidFormat(
                "index interval bytes must be greater than zero",
            ));
        }
        fs::create_dir_all(directory)?;
        let meta = load_or_create_meta(directory, topic_id, partition_id)?;
        if meta.topic_id != topic_id || meta.partition_id != partition_id {
            return Err(StorageError::InvalidFormat(
                "partition metadata identity mismatch",
            ));
        }

        let manifest = load_manifest(directory)?;
        let mut segment_paths = match &manifest {
            Some(manifest) => {
                validate_partition_manifest_files(directory, manifest, topic_id, partition_id)?
            }
            None => list_segments(directory)?,
        };
        if segment_paths.is_empty() {
            if manifest.is_some() {
                return Err(StorageError::InvalidFormat(
                    "manifest-governed partition has no segment inventory",
                ));
            }
            let header = SegmentHeader {
                topic_id,
                partition_id,
                generation: 0,
                base_stream_offset: SegmentBaseOffset::new(0),
                base_raft_index: RaftIndex::FIRST,
                created_at_ms: now_ms,
            };
            let writer = SegmentWriter::create(directory, header)?;
            segment_paths.push(writer.path().to_path_buf());
            let read_generation = Arc::new(ReadGenerationInner {
                generation: None,
                paths: segment_paths.clone(),
            });
            return Ok(Self {
                directory: directory.to_path_buf(),
                topic_id,
                partition_id,
                config,
                meta,
                segment_paths,
                active: writer,
                next_raft_index: RaftIndex::FIRST,
                next_stream_offset: StreamOffset::new(0),
                durable_raft_index: 0,
                physical_sync_count: 0,
                manifest_generation: None,
                read_generation,
                retired_read_generations: Vec::new(),
            });
        }

        let (next_raft_index, next_stream_offset) = if manifest.is_some() {
            recover_manifest_segments(
                &segment_paths,
                topic_id,
                partition_id,
                meta.committed_raft_index,
                config,
            )?
        } else {
            recover_segments(
                &segment_paths,
                topic_id,
                partition_id,
                meta.committed_raft_index,
                config,
            )?
        };
        let active_path = segment_paths.last().ok_or(StorageError::InvalidFormat(
            "partition has no active segment",
        ))?;
        let active = SegmentWriter::open_existing(active_path)?;
        let durable_raft_index = meta.committed_raft_index;
        let manifest_generation = manifest.map(|manifest| manifest.generation);
        let read_generation = Arc::new(ReadGenerationInner {
            generation: manifest_generation,
            paths: segment_paths.clone(),
        });
        Ok(Self {
            directory: directory.to_path_buf(),
            topic_id,
            partition_id,
            config,
            meta,
            segment_paths,
            active,
            next_raft_index,
            next_stream_offset,
            durable_raft_index,
            physical_sync_count: 0,
            manifest_generation,
            read_generation,
            retired_read_generations: Vec::new(),
        })
    }

    /// Atomically adopts the current validated legacy V1 segment set into
    /// manifest generation 1 without rewriting any segment bytes.
    ///
    /// This is deliberately explicit: merely opening a historical P1
    /// partition without `segments.manifest` preserves the exact generation-0
    /// discovery behavior until P9 chooses to adopt it.
    pub fn adopt_manifest_inventory(&mut self) -> Result<u64> {
        if let Some(generation) = self.manifest_generation {
            return Ok(generation);
        }
        if self.segment_paths.is_empty() {
            return Err(StorageError::InvalidFormat(
                "cannot adopt an empty partition inventory",
            ));
        }
        for path in &self.segment_paths {
            if read_segment_format(path)? != StorageFormatVersion::V1 {
                return Err(StorageError::InvalidFormat(
                    "legacy adoption accepts only V1 segments",
                ));
            }
        }
        self.active.flush_all()?;
        let mut transaction = GenerationTransaction::new(&self.directory, 1)?;
        for (index, path) in self.segment_paths.iter().enumerate() {
            let name = segment_file_name(path)?;
            if index + 1 == self.segment_paths.len() {
                transaction.retain(ManifestEntry::mutable_active(name))?;
            } else {
                transaction.retain(ManifestEntry::immutable(name, fs::metadata(path)?.len()))?;
            }
        }
        let manifest = transaction.commit()?;
        let paths = validate_partition_manifest_files(
            &self.directory,
            &manifest,
            self.topic_id,
            self.partition_id,
        )?;
        if paths != self.segment_paths {
            return Err(StorageError::InvalidFormat(
                "manifest adoption changed live segment ordering",
            ));
        }
        self.manifest_generation = Some(manifest.generation);
        self.refresh_read_generation();
        Ok(manifest.generation)
    }

    pub fn append(&mut self, frame: &Frame, now_ms: i64) -> Result<u64> {
        self.validate_next_frame(frame)?;
        let encoded = encode_frame(frame, self.config.limits)?;
        if self.active.position() > crate::format::SEGMENT_HEADER_LEN as u64
            && self.active.should_roll(
                encoded.len(),
                self.config.max_segment_bytes,
                self.config.max_segment_age_ms,
                now_ms,
            )
        {
            self.roll(now_ms)?;
        }
        let position = self.active.append_encoded(&encoded)?;
        self.advance_after(frame)?;
        Ok(position)
    }

    pub fn flush_data(&mut self) -> Result<()> {
        let next_sync_count =
            self.physical_sync_count
                .checked_add(1)
                .ok_or(StorageError::ArithmeticOverflow(
                    "partition physical sync count",
                ))?;
        self.active.flush_data()?;
        self.durable_raft_index = self.next_raft_index.get().saturating_sub(1);
        self.physical_sync_count = next_sync_count;
        Ok(())
    }

    pub fn roll(&mut self, now_ms: i64) -> Result<()> {
        if self.active.position() == crate::format::SEGMENT_HEADER_LEN as u64 {
            return Ok(());
        }
        let next_sync_count =
            self.physical_sync_count
                .checked_add(1)
                .ok_or(StorageError::ArithmeticOverflow(
                    "partition physical sync count",
                ))?;
        self.active.flush_all()?;
        self.durable_raft_index = self.next_raft_index.get().saturating_sub(1);
        self.physical_sync_count = next_sync_count;
        let header = SegmentHeader {
            topic_id: self.topic_id,
            partition_id: self.partition_id,
            generation: 0,
            base_stream_offset: SegmentBaseOffset::new(self.next_stream_offset.get()),
            base_raft_index: self.next_raft_index,
            created_at_ms: now_ms,
        };
        let writer = SegmentWriter::create(&self.directory, header)?;
        if let Some(base_generation) = self.manifest_generation {
            let current = load_manifest(&self.directory)?.ok_or(StorageError::InvalidFormat(
                "manifest generation disappeared during segment roll",
            ))?;
            if current.generation != base_generation {
                return Err(StorageError::InvalidFormat(
                    "manifest generation changed during segment roll",
                ));
            }
            let next_generation = base_generation
                .checked_add(1)
                .ok_or(StorageError::ArithmeticOverflow("manifest generation"))?;
            let mut transaction = GenerationTransaction::new(&self.directory, next_generation)?;
            for entry in &current.entries {
                if entry.is_mutable_active() {
                    let old_path = self.directory.join(&entry.file_name);
                    transaction.retain(ManifestEntry::immutable(
                        entry.file_name.clone(),
                        fs::metadata(old_path)?.len(),
                    ))?;
                } else {
                    transaction.retain(entry.clone())?;
                }
            }
            transaction.retain(ManifestEntry::mutable_active(segment_file_name(
                writer.path(),
            )?))?;
            let published = transaction.commit()?;
            validate_partition_manifest_files(
                &self.directory,
                &published,
                self.topic_id,
                self.partition_id,
            )?;
            self.manifest_generation = Some(published.generation);
        }
        self.segment_paths.push(writer.path().to_path_buf());
        self.active = writer;
        self.refresh_read_generation();
        Ok(())
    }

    pub const fn physical_sync_count(&self) -> u64 {
        self.physical_sync_count
    }

    pub const fn manifest_generation(&self) -> Option<u64> {
        self.manifest_generation
    }

    fn refresh_read_generation(&mut self) {
        self.read_generation = Arc::new(ReadGenerationInner {
            generation: self.manifest_generation,
            paths: self.segment_paths.clone(),
        });
    }

    fn install_read_generation_with_retirement(
        &mut self,
        old_owner: Arc<ReadGenerationInner>,
        unreachable_paths: Vec<PathBuf>,
    ) {
        self.refresh_read_generation();
        if !unreachable_paths.is_empty() {
            self.retired_read_generations.push(RetiredReadGeneration {
                owner: old_owner,
                unreachable_paths,
            });
        }
    }

    /// Reclaims files from old manifest generations only after every immutable
    /// reader handle for that generation has been released.
    pub fn reclaim_retired_read_generations(&mut self) -> Result<usize> {
        let mut reclaimed = 0_usize;
        let mut retained = Vec::with_capacity(self.retired_read_generations.len());
        for retired in self.retired_read_generations.drain(..) {
            if Arc::strong_count(&retired.owner) != 1 {
                retained.push(retired);
                continue;
            }
            for path in &retired.unreachable_paths {
                remove_segment_family(path)?;
                reclaimed = reclaimed
                    .checked_add(1)
                    .ok_or(StorageError::ArithmeticOverflow(
                        "retired generation reclaim count",
                    ))?;
            }
        }
        self.retired_read_generations = retained;
        if reclaimed > 0 {
            sync_directory(&self.directory)?;
        }
        Ok(reclaimed)
    }

    pub fn mark_committed(&mut self, raft_index: u64) -> Result<()> {
        if raft_index < self.meta.committed_raft_index {
            return Err(StorageError::InvalidFormat(
                "committed Raft index cannot move backwards",
            ));
        }
        let last_appended = self.next_raft_index.get().saturating_sub(1);
        if raft_index > last_appended {
            return Err(StorageError::InvalidFormat(
                "cannot commit beyond last appended Raft index",
            ));
        }
        if raft_index > self.durable_raft_index {
            return Err(StorageError::InvalidFormat(
                "cannot commit beyond locally durable Raft index",
            ));
        }
        if raft_index == self.meta.committed_raft_index {
            return Ok(());
        }
        let new_epoch = self
            .meta
            .metadata_epoch
            .checked_add(1)
            .ok_or(StorageError::ArithmeticOverflow("partition metadata epoch"))?;
        let next = PartitionMeta {
            topic_id: self.topic_id,
            partition_id: self.partition_id,
            committed_raft_index: raft_index,
            metadata_epoch: new_epoch,
        };
        write_meta(&self.directory, &next)?;
        self.meta = next;
        Ok(())
    }

    pub fn read_records_from(
        &self,
        start: StreamOffset,
        max_records: usize,
    ) -> Result<Vec<StoredRecord>> {
        self.read_records_from_bounded(start, max_records, usize::MAX)
            .map(|(records, _)| records)
    }

    /// Reads records without ever materializing an entire segment and stops as
    /// soon as either the record or logical Fetch-V0 byte budget is exhausted.
    ///
    /// `max_bytes` uses the public Fetch V0 record representation size:
    /// stream-offset + timestamp + nullable key/value lengths + headers.  The
    /// broker can therefore enforce its request budget before response
    /// assembly without introducing a protocol dependency into Storage.
    pub fn read_records_from_bounded(
        &self,
        start: StreamOffset,
        max_records: usize,
        max_bytes: usize,
    ) -> Result<(Vec<StoredRecord>, usize)> {
        let floor = self.committed_log_start_offset()?;
        let start = StreamOffset::new(start.get().max(floor.get()));
        self.read_records_from_bounded_through(start, max_records, max_bytes, None)
    }

    pub fn read_committed_records_from_bounded(
        &self,
        start: StreamOffset,
        max_records: usize,
        max_bytes: usize,
    ) -> Result<(Vec<StoredRecord>, usize)> {
        let floor = self.committed_log_start_offset()?;
        let start = StreamOffset::new(start.get().max(floor.get()));
        self.read_records_from_bounded_through(
            start,
            max_records,
            max_bytes,
            Some(self.meta.committed_raft_index),
        )
    }

    /// Captures one immutable manifest generation for a reader that may outlive
    /// a concurrent P9-E cleaner publication.
    pub fn acquire_read_generation(&self) -> Result<PartitionReadGeneration> {
        if self.manifest_generation.is_none() {
            return Err(StorageError::InvalidFormat(
                "immutable read generations require manifest authority",
            ));
        }
        Ok(PartitionReadGeneration {
            inner: Arc::clone(&self.read_generation),
            limits: self.config.limits,
            committed_raft_index: self.meta.committed_raft_index,
            log_start_offset: self.committed_log_start_offset()?,
        })
    }

    fn read_records_from_bounded_through(
        &self,
        start: StreamOffset,
        max_records: usize,
        max_bytes: usize,
        max_raft_index: Option<u64>,
    ) -> Result<(Vec<StoredRecord>, usize)> {
        read_records_from_paths_bounded(
            &self.segment_paths,
            self.config.limits,
            start,
            max_records,
            max_bytes,
            max_raft_index,
        )
    }

    pub fn read_frames(&self) -> Result<Vec<Frame>> {
        let mut out = Vec::new();
        for path in &self.segment_paths {
            if read_segment_format(path)? == StorageFormatVersion::V2 {
                continue;
            }
            let scan = scan_segment(path, self.config.limits)?;
            if let Some(failure) = scan.failure {
                return Err(StorageError::InvalidFormatOwned(format!(
                    "cannot read through damaged segment at byte {}: {:?}",
                    failure.position, failure.kind,
                )));
            }
            out.extend(scan.frames.into_iter().map(|scanned| scanned.frame));
        }
        Ok(out)
    }

    pub fn read_frames_from_raft(
        &self,
        start_raft_index: RaftIndex,
        max_frames: usize,
    ) -> Result<Vec<Frame>> {
        if max_frames == 0 {
            return Ok(Vec::new());
        }
        let mut out = Vec::with_capacity(max_frames.min(1024));
        for path in &self.segment_paths {
            if read_segment_format(path)? == StorageFormatVersion::V2 {
                continue;
            }
            let file_len = fs::metadata(path)?.len();
            let mut position = crate::format::SEGMENT_HEADER_LEN as u64;
            while position < file_len {
                let scanned = crate::read_scanned_frame_at(path, position, self.config.limits)?;
                position = position.checked_add(u64::from(scanned.encoded_len)).ok_or(
                    StorageError::ArithmeticOverflow("bounded frame page position"),
                )?;
                if scanned.frame.raft_index() < start_raft_index {
                    continue;
                }
                out.push(scanned.frame);
                if out.len() >= max_frames {
                    return Ok(out);
                }
            }
        }
        Ok(out)
    }

    pub fn committed_stream_high_watermark(&self) -> Result<StreamOffset> {
        let committed = self.meta.committed_raft_index;
        let mut high_watermark = StreamOffset::new(0);
        for path in &self.segment_paths {
            match read_segment_format(path)? {
                StorageFormatVersion::V1 => {
                    if self.manifest_generation.is_some() {
                        let header = crate::read_segment_header(path)?;
                        if header.base_raft_index.get() <= committed.saturating_add(1) {
                            let base = StreamOffset::new(header.base_stream_offset.get());
                            if base > high_watermark {
                                high_watermark = base;
                            }
                        }
                    }
                    let file_len = fs::metadata(path)?.len();
                    let mut position = crate::format::SEGMENT_HEADER_LEN as u64;
                    while position < file_len {
                        let scanned =
                            crate::read_scanned_frame_at(path, position, self.config.limits)?;
                        position = position.checked_add(u64::from(scanned.encoded_len)).ok_or(
                            StorageError::ArithmeticOverflow(
                                "committed high-watermark page position",
                            ),
                        )?;
                        if scanned.frame.raft_index().get() > committed {
                            return Ok(high_watermark);
                        }
                        if let Frame::Data(batch) = scanned.frame {
                            let count = u64::try_from(batch.records.len())
                                .map_err(|_| StorageError::ArithmeticOverflow("record count"))?;
                            high_watermark = batch.base_stream_offset.checked_add(count).ok_or(
                                StorageError::ArithmeticOverflow("committed stream high watermark"),
                            )?;
                        }
                    }
                }
                StorageFormatVersion::V2 => {
                    let scan = scan_retained_segment_v2(path, self.config.limits)?;
                    if self.manifest_generation.is_some()
                        && scan.header.base_raft_index.get() <= committed
                    {
                        let base = StreamOffset::new(scan.header.base_stream_offset.get());
                        if base > high_watermark {
                            high_watermark = base;
                        }
                    }
                    for retained in scan.frames {
                        if retained.frame.raft_index().get() > committed {
                            return Err(StorageError::InvalidFormat(
                                "V2 retained history extends above committed boundary",
                            ));
                        }
                        if let RetainedFrameV2::SparseData(batch) = retained.frame
                            && let Some(last) = batch.records.last()
                        {
                            let next = last.offset.checked_add(1).ok_or(
                                StorageError::ArithmeticOverflow(
                                    "V2 committed stream high watermark",
                                ),
                            )?;
                            if next > high_watermark {
                                high_watermark = next;
                            }
                        }
                    }
                }
            }
        }
        Ok(high_watermark)
    }

    /// Returns the replicated P9 logical lower bound reconstructed only from
    /// committed maintenance controls. Historical partitions with no such
    /// control deterministically return zero.
    pub fn committed_log_start_offset(&self) -> Result<StreamOffset> {
        let committed = self.meta.committed_raft_index;
        let high_watermark = self.committed_stream_high_watermark()?;
        let mut floor = StreamOffset::new(0);
        for path in &self.segment_paths {
            match read_segment_format(path)? {
                StorageFormatVersion::V1 => {
                    let scan = scan_segment(path, self.config.limits)?;
                    if let Some(failure) = scan.failure {
                        return Err(StorageError::InvalidFormatOwned(format!(
                            "cannot reconstruct P9 log start through damaged V1 segment at byte {}: {:?}",
                            failure.position, failure.kind,
                        )));
                    }
                    for scanned in scan.frames {
                        if scanned.frame.raft_index().get() > committed {
                            break;
                        }
                        let Frame::Control(control) = scanned.frame else {
                            continue;
                        };
                        if control.control_kind != P9_CONTROL_ADVANCE_LOG_START_OFFSET {
                            continue;
                        }
                        if control.control_version != P9_MAINTENANCE_CONTROL_VERSION {
                            return Err(StorageError::InvalidFormat(
                                "unsupported committed P9 log-start control version",
                            ));
                        }
                        let next = decode_log_start_offset_body(&control.body)?;
                        validate_log_start_advance(floor, next, high_watermark)?;
                        floor = next;
                    }
                }
                StorageFormatVersion::V2 => {
                    let scan = scan_retained_segment_v2(path, self.config.limits)?;
                    for scanned in scan.frames {
                        if scanned.frame.raft_index().get() > committed {
                            return Err(StorageError::InvalidFormat(
                                "V2 retained maintenance extends above committed boundary",
                            ));
                        }
                        let RetainedFrameV2::Control(control) = scanned.frame else {
                            continue;
                        };
                        if control.kind != RetainedControlKindV2::AdvanceLogStartOffset {
                            continue;
                        }
                        let next = decode_log_start_offset_body(&control.body)?;
                        validate_log_start_advance(floor, next, high_watermark)?;
                        floor = next;
                    }
                }
            }
        }
        Ok(floor)
    }

    /// Replicated P9-G compaction-safe exclusive lower bound, reconstructed
    /// from *committed* V1/V2 controls, never from local cleaner progress.
    /// A historical partition with no control has safe offset zero.
    pub fn committed_compaction_safe_offset(&self) -> Result<StreamOffset> {
        let committed = self.meta.committed_raft_index;
        let high_watermark = self.committed_stream_high_watermark()?;
        let mut safe = StreamOffset::new(0);
        for path in &self.segment_paths {
            match read_segment_format(path)? {
                StorageFormatVersion::V1 => {
                    let scan = scan_segment(path, self.config.limits)?;
                    if let Some(failure) = scan.failure {
                        return Err(StorageError::InvalidFormatOwned(format!(
                            "cannot reconstruct P9 compaction-safe offset through damaged V1 segment at byte {}: {:?}",
                            failure.position, failure.kind,
                        )));
                    }
                    for scanned in scan.frames {
                        if scanned.frame.raft_index().get() > committed {
                            break;
                        }
                        let Frame::Control(control) = scanned.frame else {
                            continue;
                        };
                        if control.control_kind != P9_CONTROL_ADVANCE_COMPACTION_SAFE_OFFSET {
                            continue;
                        }
                        if control.control_version != P9_MAINTENANCE_CONTROL_VERSION {
                            return Err(StorageError::InvalidFormat(
                                "unsupported committed P9 compaction-safe control version",
                            ));
                        }
                        let next = decode_compaction_safe_offset_body(&control.body)?;
                        if next <= safe || next > high_watermark {
                            return Err(StorageError::InvalidFormat(
                                "committed P9 compaction-safe offset must strictly advance within high watermark",
                            ));
                        }
                        safe = next;
                    }
                }
                StorageFormatVersion::V2 => {
                    let scan = scan_retained_segment_v2(path, self.config.limits)?;
                    for scanned in scan.frames {
                        if scanned.frame.raft_index().get() > committed {
                            return Err(StorageError::InvalidFormat(
                                "V2 retained compaction-safe control extends above committed boundary",
                            ));
                        }
                        let RetainedFrameV2::Control(control) = scanned.frame else {
                            continue;
                        };
                        if control.kind != RetainedControlKindV2::AdvanceCompactionSafeOffset {
                            continue;
                        }
                        let next = decode_compaction_safe_offset_body(&control.body)?;
                        if next <= safe || next > high_watermark {
                            return Err(StorageError::InvalidFormat(
                                "committed P9 retained compaction-safe offset is nonmonotonic or above high watermark",
                            ));
                        }
                        safe = next;
                    }
                }
            }
        }
        Ok(safe)
    }

    /// Returns the newest committed P9 producer-state checkpoint control from
    /// manifest-authoritative V1/V2 retained history. The body is opaque to
    /// Storage; replication owns the canonical P7 checkpoint codec.
    pub fn latest_committed_producer_checkpoint_control(
        &self,
    ) -> Result<Option<crate::RetainedControlFrameV2>> {
        let committed = self.meta.committed_raft_index;
        let mut latest: Option<crate::RetainedControlFrameV2> = None;
        for path in &self.segment_paths {
            match read_segment_format(path)? {
                StorageFormatVersion::V1 => {
                    let scan = scan_segment(path, self.config.limits)?;
                    if let Some(failure) = scan.failure {
                        return Err(StorageError::InvalidFormatOwned(format!(
                            "cannot reconstruct P9 producer checkpoint through damaged V1 segment at byte {}: {:?}",
                            failure.position, failure.kind,
                        )));
                    }
                    for scanned in scan.frames {
                        if scanned.frame.raft_index().get() > committed {
                            break;
                        }
                        let Frame::Control(control) = scanned.frame else {
                            continue;
                        };
                        if control.control_kind != P9_CONTROL_PRODUCER_STATE_CHECKPOINT {
                            continue;
                        }
                        if control.control_version != P9_MAINTENANCE_CONTROL_VERSION {
                            return Err(StorageError::InvalidFormat(
                                "unsupported committed P9 producer checkpoint control version",
                            ));
                        }
                        let normalized = crate::RetainedControlFrameV2 {
                            raft_term: control.raft_term,
                            raft_index: control.raft_index,
                            kind: RetainedControlKindV2::ProducerStateCheckpoint,
                            body: control.body,
                        };
                        if latest
                            .as_ref()
                            .is_some_and(|previous| normalized.raft_index <= previous.raft_index)
                        {
                            return Err(StorageError::InvalidFormat(
                                "committed P9 producer checkpoint Raft index did not advance",
                            ));
                        }
                        latest = Some(normalized);
                    }
                }
                StorageFormatVersion::V2 => {
                    let scan = scan_retained_segment_v2(path, self.config.limits)?;
                    for scanned in scan.frames {
                        if scanned.frame.raft_index().get() > committed {
                            return Err(StorageError::InvalidFormat(
                                "V2 retained producer checkpoint extends above committed boundary",
                            ));
                        }
                        let RetainedFrameV2::Control(control) = scanned.frame else {
                            continue;
                        };
                        if control.kind != RetainedControlKindV2::ProducerStateCheckpoint {
                            continue;
                        }
                        if latest
                            .as_ref()
                            .is_some_and(|previous| control.raft_index <= previous.raft_index)
                        {
                            return Err(StorageError::InvalidFormat(
                                "committed P9 producer checkpoint Raft index did not advance",
                            ));
                        }
                        latest = Some(control);
                    }
                }
            }
        }
        Ok(latest)
    }

    pub fn list_committed_offset(&self, timestamp_selector: i64) -> Result<(u64, i64)> {
        if timestamp_selector < -2 {
            return Err(StorageError::InvalidFormat("invalid timestamp selector"));
        }
        let floor = self.committed_log_start_offset()?;
        let high_watermark = self.committed_stream_high_watermark()?;
        match timestamp_selector {
            -2 => return Ok((floor.get(), -1)),
            -1 => return Ok((high_watermark.get(), -1)),
            _ => {}
        }
        let committed = self.meta.committed_raft_index;
        for path in &self.segment_paths {
            match read_segment_format(path)? {
                StorageFormatVersion::V1 => {
                    let scan = scan_segment(path, self.config.limits)?;
                    if let Some(failure) = scan.failure {
                        return Err(StorageError::InvalidFormatOwned(format!(
                            "cannot list offsets through damaged V1 segment at byte {}: {:?}",
                            failure.position, failure.kind,
                        )));
                    }
                    for scanned in scan.frames {
                        if scanned.frame.raft_index().get() > committed {
                            return Ok((u64::MAX, -1));
                        }
                        let Frame::Data(batch) = scanned.frame else {
                            continue;
                        };
                        for (ordinal, record) in batch.records.into_iter().enumerate() {
                            let ordinal = u64::try_from(ordinal)
                                .map_err(|_| StorageError::ArithmeticOverflow("record ordinal"))?;
                            let offset = batch.base_stream_offset.checked_add(ordinal).ok_or(
                                StorageError::ArithmeticOverflow("timestamp scan stream offset"),
                            )?;
                            if offset < floor {
                                continue;
                            }
                            if record.timestamp_ms >= timestamp_selector {
                                return Ok((offset.get(), record.timestamp_ms));
                            }
                        }
                    }
                }
                StorageFormatVersion::V2 => {
                    let scan = scan_retained_segment_v2(path, self.config.limits)?;
                    for scanned in scan.frames {
                        if scanned.frame.raft_index().get() > committed {
                            return Err(StorageError::InvalidFormat(
                                "V2 retained history extends above committed boundary",
                            ));
                        }
                        let RetainedFrameV2::SparseData(batch) = scanned.frame else {
                            continue;
                        };
                        for retained in batch.records {
                            if retained.offset < floor {
                                continue;
                            }
                            if retained.record.timestamp_ms >= timestamp_selector {
                                return Ok((retained.offset.get(), retained.record.timestamp_ms));
                            }
                        }
                    }
                }
            }
        }
        Ok((u64::MAX, -1))
    }

    pub fn plan_delete_retention(
        &mut self,
        spec: DeleteRetentionSpec,
    ) -> Result<DeleteRetentionPlan> {
        self.adopt_manifest_inventory()?;
        let manifest = load_manifest(&self.directory)?.ok_or(StorageError::InvalidFormat(
            "P9 delete-retention planning requires manifest authority",
        ))?;
        if Some(manifest.generation) != self.manifest_generation {
            return Err(StorageError::InvalidFormat(
                "manifest changed before P9 delete-retention planning",
            ));
        }
        let views = retention_segment_views(self, &manifest)?;
        let current_floor = self.committed_log_start_offset()?;
        let high_watermark = self.committed_stream_high_watermark()?;
        let committed_safe = self.committed_compaction_safe_offset()?;
        let latest_checkpoint = self.latest_committed_producer_checkpoint_control()?;
        let safe_anchor = if committed_safe == StreamOffset::new(0) {
            None
        } else {
            Some(
                views
                    .iter()
                    .rposition(|view| view.last_compaction_safe_offset == Some(committed_safe))
                    .ok_or(StorageError::InvalidFormat(
                        "P9-G retention planning cannot locate latest safe anchor",
                    ))?,
            )
        };
        let producer_anchor = latest_checkpoint
            .as_ref()
            .map(|checkpoint| {
                views
                    .iter()
                    .rposition(|view| {
                        view.last_producer_checkpoint_raft_index
                            == Some(checkpoint.raft_index.get())
                    })
                    .ok_or(StorageError::InvalidFormat(
                        "P9-G retention planning cannot locate latest P9-F checkpoint",
                    ))
            })
            .transpose()?;
        let retained_anchor_floor = safe_anchor.into_iter().chain(producer_anchor).min();
        let authoritative_log_bytes = views.iter().try_fold(0_u64, |total, view| {
            total
                .checked_add(view.log_bytes)
                .ok_or(StorageError::ArithmeticOverflow("retention log bytes"))
        })?;

        let mut candidate = current_floor;
        let mut retained_bytes = authoritative_log_bytes;
        // Bytes already wholly below the committed logical floor are reclaimable
        // under existing authority and must not force an unnecessary further
        // semantic advance when evaluating the size target.
        for index in 0..views.len().saturating_sub(1) {
            let boundary = views[index + 1].base_stream_offset;
            if retained_anchor_floor.is_some_and(|anchor| index >= anchor) {
                break;
            }
            if boundary <= current_floor
                && views[index].safe_through(
                    spec.snapshot_floor_raft_index,
                    self.meta.committed_raft_index,
                )
            {
                retained_bytes = retained_bytes.checked_sub(views[index].log_bytes).ok_or(
                    StorageError::ArithmeticOverflow("retention existing obsolete bytes"),
                )?;
            }
        }

        if let Some(retention_ms) = spec.retention_ms {
            for index in 0..views.len().saturating_sub(1) {
                let source = &views[index];
                let successor = &views[index + 1];
                let boundary = successor.base_stream_offset;
                if boundary <= current_floor
                    || boundary > high_watermark
                    || !source.safe_through(
                        spec.snapshot_floor_raft_index,
                        self.meta.committed_raft_index,
                    )
                {
                    continue;
                }
                // A successor timestamp earlier than the source's own creation
                // proves the wall clock moved backwards across the roll. Such a
                // timestamp is not a conservative close lower-bound: treating it
                // as older would accelerate deletion. Fail toward retention and
                // stop the contiguous time-retention prefix instead.
                if successor.created_at_ms < source.created_at_ms {
                    break;
                }
                if conservative_age_expired(spec.now_ms, successor.created_at_ms, retention_ms) {
                    candidate = StreamOffset::new(candidate.get().max(boundary.get()));
                } else {
                    // Retention removes only a contiguous oldest prefix. Once an
                    // older source is not safely expired, keeping all newer
                    // sources is conservative.
                    break;
                }
            }
        }

        if let Some(target) = spec.retention_bytes {
            let mut remaining = retained_bytes;
            for index in 0..views.len().saturating_sub(1) {
                if retained_anchor_floor.is_some_and(|anchor| index >= anchor) {
                    // A size target cannot be made true by counting an
                    // authoritative safe/P7 checkpoint as reclaimable.
                    break;
                }
                if remaining <= target {
                    break;
                }
                let source = &views[index];
                let successor = &views[index + 1];
                let boundary = successor.base_stream_offset;
                if boundary <= current_floor {
                    continue;
                }
                if boundary > high_watermark
                    || !source.safe_through(
                        spec.snapshot_floor_raft_index,
                        self.meta.committed_raft_index,
                    )
                {
                    break;
                }
                remaining = remaining.checked_sub(source.log_bytes).ok_or(
                    StorageError::ArithmeticOverflow("retention size subtraction"),
                )?;
                candidate = StreamOffset::new(candidate.get().max(boundary.get()));
            }
        }

        if let Some(ceiling) = spec.protected_log_start_ceiling
            && candidate > ceiling
        {
            candidate = current_floor;
            for index in 0..views.len().saturating_sub(1) {
                let boundary = views[index + 1].base_stream_offset;
                if boundary > current_floor
                    && boundary <= ceiling
                    && boundary <= high_watermark
                    && views[index].safe_through(
                        spec.snapshot_floor_raft_index,
                        self.meta.committed_raft_index,
                    )
                {
                    candidate = boundary;
                }
            }
        }

        let retained_log_bytes_after_plan = retained_bytes_after_floor(
            &views,
            candidate,
            spec.snapshot_floor_raft_index,
            self.meta.committed_raft_index,
            retained_anchor_floor,
        )?;
        Ok(DeleteRetentionPlan {
            base_manifest_generation: manifest.generation,
            current_log_start_offset: current_floor,
            proposed_log_start_offset: candidate,
            authoritative_log_bytes,
            retained_log_bytes_after_plan,
        })
    }

    /// Publishes a new authoritative manifest generation that omits only
    /// immutable files wholly below the already-committed logical start.
    pub fn publish_delete_retention_generation(
        &mut self,
        expected_manifest_generation: u64,
        snapshot_floor_raft_index: u64,
    ) -> Result<usize> {
        let floor = self.committed_log_start_offset()?;
        let committed_safe = self.committed_compaction_safe_offset()?;
        let current = load_manifest(&self.directory)?.ok_or(StorageError::InvalidFormat(
            "P9 physical delete retention requires manifest authority",
        ))?;
        if current.generation != expected_manifest_generation
            || Some(current.generation) != self.manifest_generation
        {
            return Err(StorageError::InvalidFormat(
                "manifest generation changed before P9 physical delete retention",
            ));
        }
        let views = retention_segment_views(self, &current)?;
        let authority_segment = if floor == StreamOffset::new(0) {
            None
        } else {
            Some(
                views
                    .iter()
                    .rposition(|view| view.last_log_start_offset == Some(floor))
                    .ok_or(StorageError::InvalidFormat(
                        "committed P9 log-start authority is missing from retained history",
                    ))?,
            )
        };
        let safe_anchor_segment = if committed_safe == StreamOffset::new(0) {
            None
        } else {
            Some(
                views
                    .iter()
                    .rposition(|view| view.last_compaction_safe_offset == Some(committed_safe))
                    .ok_or(StorageError::InvalidFormat(
                        "committed P9-G safe-offset anchor missing from retained history",
                    ))?,
            )
        };
        // P9-G can reclaim old values/tombstones, but physical deletion must
        // never silently destroy P9-F's newest committed P7 producer recovery
        // checkpoint. Its authoritative retained control is independent of
        // derived producer.snapshot and may predate the current safe offset.
        let producer_checkpoint_segment =
            self.latest_committed_producer_checkpoint_control()?
                .map(|checkpoint| {
                    views.iter().rposition(|view| {
                    view.last_producer_checkpoint_raft_index
                        == Some(checkpoint.raft_index.get())
                }).ok_or(StorageError::InvalidFormat(
                    "latest committed P9-F producer checkpoint missing in retention inventory",
                ))
                })
                .transpose()?;
        let mut remove_count = 0_usize;
        for index in 0..views.len().saturating_sub(1) {
            if authority_segment.is_some_and(|authority| index >= authority)
                || safe_anchor_segment.is_some_and(|authority| index >= authority)
                || producer_checkpoint_segment.is_some_and(|authority| index >= authority)
            {
                break;
            }
            let boundary = views[index + 1].base_stream_offset;
            if boundary <= floor
                && views[index]
                    .safe_through(snapshot_floor_raft_index, self.meta.committed_raft_index)
            {
                remove_count += 1;
            } else {
                break;
            }
        }
        if remove_count == 0 {
            return Ok(0);
        }
        let next_generation = current
            .generation
            .checked_add(1)
            .ok_or(StorageError::ArithmeticOverflow("manifest generation"))?;
        let mut transaction = GenerationTransaction::new(&self.directory, next_generation)?;
        for entry in current.entries.iter().skip(remove_count) {
            transaction.retain(entry.clone())?;
        }
        let published = transaction.commit()?;
        let new_paths = validate_partition_manifest_files(
            &self.directory,
            &published,
            self.topic_id,
            self.partition_id,
        )?;
        let old_owner = Arc::clone(&self.read_generation);
        let old_paths = std::mem::replace(&mut self.segment_paths, new_paths);
        self.manifest_generation = Some(published.generation);
        self.install_read_generation_with_retirement(
            old_owner,
            old_paths.iter().take(remove_count).cloned().collect(),
        );
        let _ = self.reclaim_retired_read_generations()?;
        Ok(remove_count)
    }

    pub fn truncate_raft_suffix(&mut self, from_raft_index: u64) -> Result<()> {
        if from_raft_index == 0 {
            return Err(StorageError::InvalidFormat(
                "Raft suffix truncation index cannot be zero",
            ));
        }
        if from_raft_index <= self.meta.committed_raft_index {
            return Err(StorageError::CommittedCorruption {
                raft_index: from_raft_index,
                reason: "refusing to truncate committed Raft history".to_owned(),
            });
        }

        let next = self.next_raft_index.get();
        if from_raft_index > next {
            return Err(StorageError::InvalidFormat(
                "Raft suffix truncation starts beyond local tail",
            ));
        }
        if from_raft_index == next {
            return Ok(());
        }

        let mut scans = Vec::with_capacity(self.segment_paths.len());
        let mut target_segment = None;
        let mut target_position = None;
        let mut next_stream_offset = StreamOffset::new(0);

        for (segment_index, path) in self.segment_paths.iter().enumerate() {
            if read_segment_format(path)? == StorageFormatVersion::V2 {
                continue;
            }
            let scan = scan_segment(path, self.config.limits)?;
            if let Some(failure) = &scan.failure {
                return Err(StorageError::InvalidFormatOwned(format!(
                    "cannot truncate damaged segment at byte {}: {:?}",
                    failure.position, failure.kind,
                )));
            }
            if scan.header.base_raft_index.get() <= from_raft_index {
                next_stream_offset = StreamOffset::new(scan.header.base_stream_offset.get());
            }
            for scanned in &scan.frames {
                if scanned.frame.raft_index().get() >= from_raft_index {
                    target_segment.get_or_insert(segment_index);
                    target_position.get_or_insert(scanned.position);
                    break;
                }
                if let Frame::Data(batch) = &scanned.frame {
                    let count = u64::try_from(batch.records.len())
                        .map_err(|_| StorageError::ArithmeticOverflow("record count"))?;
                    next_stream_offset = batch.base_stream_offset.checked_add(count).ok_or(
                        StorageError::ArithmeticOverflow("stream offset after suffix truncation"),
                    )?;
                }
            }
            scans.push(scan);
            if target_segment.is_some() {
                break;
            }
        }

        let segment_index = target_segment.ok_or(StorageError::InvalidFormat(
            "Raft suffix truncation target was not found",
        ))?;
        let truncate_position = target_position.ok_or(StorageError::InvalidFormat(
            "Raft suffix truncation target has no byte position",
        ))?;
        let target_path = self.segment_paths[segment_index].clone();

        if self.manifest_generation.is_some() && segment_index + 1 != self.segment_paths.len() {
            return self.truncate_manifest_suffix_cow(
                segment_index,
                truncate_position,
                from_raft_index,
                next_stream_offset,
            );
        }

        // Repoint the active writer first so the previous active file handle is
        // closed before later segment files are removed (important on Windows
        // development hosts). The target segment becomes the new active tail.
        if segment_index + 1 != self.segment_paths.len() {
            self.active = SegmentWriter::open_existing(&target_path)?;
        }

        // Delete later segments first. A crash here leaves a valid, shorter
        // uncommitted suffix that Raft may repair again after restart; committed
        // history is untouched.
        for path in self.segment_paths.iter().skip(segment_index + 1).rev() {
            remove_segment_family(path)?;
        }
        sync_directory(&self.directory)?;

        self.active.truncate_to(truncate_position)?;
        let clean = scan_segment(&target_path, self.config.limits)?;
        if clean.failure.is_some() {
            return Err(StorageError::Corruption {
                reason: "segment remained invalid after Raft suffix truncation".to_owned(),
            });
        }
        rebuild_segment_indexes(
            &target_path,
            &clean.header,
            &clean.frames,
            self.config.index_interval_bytes,
        )?;

        self.segment_paths.truncate(segment_index + 1);
        self.next_raft_index = RaftIndex::new(from_raft_index).ok_or(
            StorageError::InvalidFormat("Raft suffix truncation produced zero next index"),
        )?;
        self.next_stream_offset = next_stream_offset;
        self.durable_raft_index = self
            .durable_raft_index
            .min(from_raft_index.saturating_sub(1));
        Ok(())
    }

    fn truncate_manifest_suffix_cow(
        &mut self,
        segment_index: usize,
        truncate_position: u64,
        from_raft_index: u64,
        next_stream_offset: StreamOffset,
    ) -> Result<()> {
        let base_generation = self.manifest_generation.ok_or(StorageError::InvalidFormat(
            "manifest COW truncation requires active manifest",
        ))?;
        let current = load_manifest(&self.directory)?.ok_or(StorageError::InvalidFormat(
            "manifest disappeared during COW suffix truncation",
        ))?;
        if current.generation != base_generation
            || current.entries.len() != self.segment_paths.len()
        {
            return Err(StorageError::InvalidFormat(
                "manifest changed during COW suffix truncation",
            ));
        }
        let target_path = self.segment_paths[segment_index].clone();
        if read_segment_format(&target_path)? != StorageFormatVersion::V1 {
            return Err(StorageError::InvalidFormat(
                "Raft suffix truncation cannot target V2 retained history",
            ));
        }
        let scan = scan_segment(&target_path, self.config.limits)?;
        if scan.failure.is_some() {
            return Err(StorageError::InvalidFormat(
                "cannot COW-truncate damaged manifest V1 segment",
            ));
        }
        let mut bytes = fs::read(&target_path)?;
        let truncate_len = usize::try_from(truncate_position)
            .map_err(|_| StorageError::ArithmeticOverflow("COW truncate position"))?;
        if truncate_len < crate::format::SEGMENT_HEADER_LEN || truncate_len > bytes.len() {
            return Err(StorageError::InvalidFormat(
                "COW truncate position is outside target segment",
            ));
        }
        bytes.truncate(truncate_len);

        let mut replacement_header = scan.header.clone();
        replacement_header.generation = replacement_header
            .generation
            .checked_add(1)
            .ok_or(StorageError::ArithmeticOverflow("segment generation"))?;
        loop {
            let candidate = SegmentFileId::from_header(&replacement_header).log_file_name();
            if !self.directory.join(&candidate).exists() {
                break;
            }
            replacement_header.generation = replacement_header
                .generation
                .checked_add(1)
                .ok_or(StorageError::ArithmeticOverflow("segment generation"))?;
        }
        bytes[..crate::format::SEGMENT_HEADER_LEN]
            .copy_from_slice(&encode_segment_header(&replacement_header));
        let replacement_name = SegmentFileId::from_header(&replacement_header).log_file_name();

        let next_generation = base_generation
            .checked_add(1)
            .ok_or(StorageError::ArithmeticOverflow("manifest generation"))?;
        let mut transaction = GenerationTransaction::new(&self.directory, next_generation)?;
        for entry in current.entries.iter().take(segment_index) {
            transaction.retain(entry.clone())?;
        }
        transaction.stage_file(replacement_name.clone(), &bytes, true)?;
        let published = transaction.commit()?;
        let new_paths = validate_partition_manifest_files(
            &self.directory,
            &published,
            self.topic_id,
            self.partition_id,
        )?;
        let new_active_path = self.directory.join(&replacement_name);
        let new_active = SegmentWriter::open_existing(&new_active_path)?;

        // Close the old active handle before reclaiming files that the new
        // authoritative manifest no longer references (important on Windows).
        self.active = new_active;
        let old_owner = Arc::clone(&self.read_generation);
        let old_paths = std::mem::replace(&mut self.segment_paths, new_paths);
        self.manifest_generation = Some(published.generation);
        self.install_read_generation_with_retirement(
            old_owner,
            old_paths
                .iter()
                .skip(segment_index)
                .filter(|path| *path != &new_active_path)
                .cloned()
                .collect(),
        );
        let _ = self.reclaim_retired_read_generations()?;

        let clean = scan_segment(&new_active_path, self.config.limits)?;
        if clean.failure.is_some() {
            return Err(StorageError::Corruption {
                reason: "COW-truncated manifest segment is invalid".to_owned(),
            });
        }
        rebuild_segment_indexes(
            &new_active_path,
            &clean.header,
            &clean.frames,
            self.config.index_interval_bytes,
        )?;
        self.next_raft_index = RaftIndex::new(from_raft_index).ok_or(
            StorageError::InvalidFormat("Raft suffix truncation produced zero next index"),
        )?;
        self.next_stream_offset = next_stream_offset;
        self.durable_raft_index = self
            .durable_raft_index
            .min(from_raft_index.saturating_sub(1));
        Ok(())
    }

    pub fn next_raft_index(&self) -> RaftIndex {
        self.next_raft_index
    }

    pub fn next_stream_offset(&self) -> StreamOffset {
        self.next_stream_offset
    }

    pub fn committed_raft_index(&self) -> u64 {
        self.meta.committed_raft_index
    }

    pub fn durable_raft_index(&self) -> u64 {
        self.durable_raft_index
    }

    pub fn segment_paths(&self) -> &[PathBuf] {
        &self.segment_paths
    }

    fn validate_next_frame(&self, frame: &Frame) -> Result<()> {
        if frame.raft_index() != self.next_raft_index {
            return Err(StorageError::InvalidFormatOwned(format!(
                "expected Raft index {}, got {}",
                self.next_raft_index.get(),
                frame.raft_index().get(),
            )));
        }
        if let Frame::Data(batch) = frame
            && batch.base_stream_offset != self.next_stream_offset
        {
            return Err(StorageError::InvalidFormatOwned(format!(
                "expected stream offset {}, got {}",
                self.next_stream_offset.get(),
                batch.base_stream_offset.get(),
            )));
        }
        Ok(())
    }

    fn advance_after(&mut self, frame: &Frame) -> Result<()> {
        self.next_raft_index = self
            .next_raft_index
            .checked_next()
            .ok_or(StorageError::ArithmeticOverflow("next Raft index"))?;
        if let Frame::Data(batch) = frame {
            let count = u64::try_from(batch.records.len())
                .map_err(|_| StorageError::ArithmeticOverflow("record count"))?;
            self.next_stream_offset = self
                .next_stream_offset
                .checked_add(count)
                .ok_or(StorageError::ArithmeticOverflow("next stream offset"))?;
        }
        Ok(())
    }
}

#[derive(Debug, Clone, Copy)]
struct RetentionSegmentView {
    base_stream_offset: StreamOffset,
    created_at_ms: i64,
    max_raft_index: u64,
    log_bytes: u64,
    last_log_start_offset: Option<StreamOffset>,
    last_compaction_safe_offset: Option<StreamOffset>,
    last_producer_checkpoint_raft_index: Option<u64>,
}

impl RetentionSegmentView {
    fn safe_through(&self, snapshot_floor_raft_index: u64, committed_raft_index: u64) -> bool {
        self.max_raft_index <= snapshot_floor_raft_index
            && self.max_raft_index <= committed_raft_index
    }
}

fn validate_log_start_advance(
    current: StreamOffset,
    next: StreamOffset,
    high_watermark: StreamOffset,
) -> Result<()> {
    if next < current {
        return Err(StorageError::InvalidFormat(
            "committed P9 log start cannot regress",
        ));
    }
    if next > high_watermark {
        return Err(StorageError::InvalidFormat(
            "committed P9 log start cannot exceed committed stream high watermark",
        ));
    }
    Ok(())
}

fn conservative_age_expired(now_ms: i64, close_bound_ms: i64, retention_ms: u64) -> bool {
    let Ok(required_age) = i64::try_from(retention_ms) else {
        return false;
    };
    now_ms
        .checked_sub(close_bound_ms)
        .is_some_and(|age| age >= 0 && age >= required_age)
}

fn retention_segment_views(
    log: &PartitionLog,
    manifest: &SegmentManifest,
) -> Result<Vec<RetentionSegmentView>> {
    let authoritative = validate_partition_manifest_files(
        &log.directory,
        manifest,
        log.topic_id,
        log.partition_id,
    )?;
    if authoritative != log.segment_paths || authoritative.len() != manifest.entries.len() {
        return Err(StorageError::InvalidFormat(
            "live manifest inventory changed during retention planning",
        ));
    }
    let mut views = Vec::with_capacity(authoritative.len());
    for path in authoritative {
        let log_bytes = fs::metadata(&path)?.len();
        match read_segment_format(&path)? {
            StorageFormatVersion::V1 => {
                let scan = scan_segment(&path, log.config.limits)?;
                if let Some(failure) = scan.failure {
                    return Err(StorageError::InvalidFormatOwned(format!(
                        "cannot plan retention through damaged V1 segment at byte {}: {:?}",
                        failure.position, failure.kind,
                    )));
                }
                let max_raft_index = scan
                    .frames
                    .last()
                    .map(|frame| frame.frame.raft_index().get())
                    .unwrap_or_else(|| scan.header.base_raft_index.get().saturating_sub(1));
                let mut last_log_start_offset = None;
                let mut last_compaction_safe_offset = None;
                let mut last_producer_checkpoint_raft_index = None;
                for scanned in &scan.frames {
                    if scanned.frame.raft_index().get() > log.meta.committed_raft_index {
                        break;
                    }
                    if let Frame::Control(control) = &scanned.frame
                        && control.control_kind == P9_CONTROL_ADVANCE_LOG_START_OFFSET
                        && control.control_version == P9_MAINTENANCE_CONTROL_VERSION
                    {
                        last_log_start_offset = Some(decode_log_start_offset_body(&control.body)?);
                    }
                    if let Frame::Control(control) = &scanned.frame
                        && control.control_kind == P9_CONTROL_ADVANCE_COMPACTION_SAFE_OFFSET
                    {
                        if control.control_version != P9_MAINTENANCE_CONTROL_VERSION {
                            return Err(StorageError::InvalidFormat(
                                "P9-G retained safe-anchor control version is unsupported",
                            ));
                        }
                        last_compaction_safe_offset =
                            Some(decode_compaction_safe_offset_body(&control.body)?);
                    }
                    if let Frame::Control(control) = &scanned.frame
                        && control.control_kind == P9_CONTROL_PRODUCER_STATE_CHECKPOINT
                    {
                        if control.control_version != P9_MAINTENANCE_CONTROL_VERSION {
                            return Err(StorageError::InvalidFormat(
                                "P9-F retained checkpoint control version is unsupported",
                            ));
                        }
                        last_producer_checkpoint_raft_index = Some(control.raft_index.get());
                    }
                }
                views.push(RetentionSegmentView {
                    base_stream_offset: StreamOffset::new(scan.header.base_stream_offset.get()),
                    created_at_ms: scan.header.created_at_ms,
                    max_raft_index,
                    log_bytes,
                    last_log_start_offset,
                    last_compaction_safe_offset,
                    last_producer_checkpoint_raft_index,
                });
            }
            StorageFormatVersion::V2 => {
                let scan = scan_retained_segment_v2(&path, log.config.limits)?;
                let max_raft_index = scan
                    .frames
                    .last()
                    .map(|frame| frame.frame.raft_index().get())
                    .unwrap_or_else(|| scan.header.base_raft_index.get().saturating_sub(1));
                let mut last_log_start_offset = None;
                let mut last_compaction_safe_offset = None;
                let mut last_producer_checkpoint_raft_index = None;
                for scanned in &scan.frames {
                    if let RetainedFrameV2::Control(control) = &scanned.frame
                        && control.kind == RetainedControlKindV2::AdvanceLogStartOffset
                    {
                        last_log_start_offset = Some(decode_log_start_offset_body(&control.body)?);
                    }
                    if let RetainedFrameV2::Control(control) = &scanned.frame
                        && control.kind == RetainedControlKindV2::AdvanceCompactionSafeOffset
                    {
                        last_compaction_safe_offset =
                            Some(decode_compaction_safe_offset_body(&control.body)?);
                    }
                    if let RetainedFrameV2::Control(control) = &scanned.frame
                        && control.kind == RetainedControlKindV2::ProducerStateCheckpoint
                    {
                        last_producer_checkpoint_raft_index = Some(control.raft_index.get());
                    }
                }
                views.push(RetentionSegmentView {
                    base_stream_offset: StreamOffset::new(scan.header.base_stream_offset.get()),
                    created_at_ms: scan.header.created_at_ms,
                    max_raft_index,
                    log_bytes,
                    last_log_start_offset,
                    last_compaction_safe_offset,
                    last_producer_checkpoint_raft_index,
                });
            }
        }
    }
    Ok(views)
}

fn retained_bytes_after_floor(
    views: &[RetentionSegmentView],
    floor: StreamOffset,
    snapshot_floor_raft_index: u64,
    committed_raft_index: u64,
    protected_anchor_index: Option<usize>,
) -> Result<u64> {
    let mut total = views.iter().try_fold(0_u64, |total, view| {
        total
            .checked_add(view.log_bytes)
            .ok_or(StorageError::ArithmeticOverflow("retention retained bytes"))
    })?;
    for index in 0..views.len().saturating_sub(1) {
        if protected_anchor_index.is_some_and(|anchor| index >= anchor) {
            break;
        }
        if views[index + 1].base_stream_offset <= floor
            && views[index].safe_through(snapshot_floor_raft_index, committed_raft_index)
        {
            total = total.checked_sub(views[index].log_bytes).ok_or(
                StorageError::ArithmeticOverflow("retention reclaimed bytes"),
            )?;
        } else {
            break;
        }
    }
    Ok(total)
}

fn remove_segment_family(log_path: &Path) -> Result<()> {
    let file_name =
        log_path
            .file_name()
            .and_then(|name| name.to_str())
            .ok_or(StorageError::InvalidFormat(
                "segment file name is not valid UTF-8",
            ))?;
    let id = SegmentFileId::parse_log_file_name(file_name)?;
    let directory = log_path.parent().ok_or(StorageError::InvalidFormat(
        "segment path has no parent directory",
    ))?;
    for path in [
        log_path.to_path_buf(),
        directory.join(id.index_file_name("index")),
        directory.join(id.index_file_name("timeindex")),
        directory.join(id.index_file_name("raftindex")),
    ] {
        match fs::remove_file(&path) {
            Ok(()) => {}
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
            Err(error) => return Err(error.into()),
        }
    }
    Ok(())
}

fn fetch_v0_record_encoded_size(record: &DataRecord) -> Result<usize> {
    // stream_offset(u64) + timestamp_ms(i64)
    let mut size = 16_usize;
    // nullable key/value use a signed/unsigned 32-bit length on Wire V1.
    size = size
        .checked_add(4)
        .and_then(|value| value.checked_add(record.key.as_ref().map_or(0, Vec::len)))
        .and_then(|value| value.checked_add(4))
        .and_then(|value| value.checked_add(record.value.as_ref().map_or(0, Vec::len)))
        .and_then(|value| value.checked_add(2))
        .ok_or(StorageError::ArithmeticOverflow(
            "Fetch V0 record byte count",
        ))?;
    for header in &record.headers {
        size = size
            .checked_add(2)
            .and_then(|value| value.checked_add(header.name.len()))
            .and_then(|value| value.checked_add(4))
            .and_then(|value| value.checked_add(header.value.as_ref().map_or(0, Vec::len)))
            .ok_or(StorageError::ArithmeticOverflow(
                "Fetch V0 header byte count",
            ))?;
    }
    Ok(size)
}

fn push_bounded_record(
    out: &mut Vec<StoredRecord>,
    used_bytes: &mut usize,
    offset: StreamOffset,
    record: DataRecord,
    max_records: usize,
    max_bytes: usize,
) -> Result<bool> {
    let encoded_size = fetch_v0_record_encoded_size(&record)?;
    if used_bytes
        .checked_add(encoded_size)
        .is_none_or(|bytes| bytes > max_bytes)
    {
        return Ok(true);
    }
    *used_bytes = used_bytes
        .checked_add(encoded_size)
        .ok_or(StorageError::ArithmeticOverflow(
            "bounded record byte count",
        ))?;
    out.push(StoredRecord { offset, record });
    Ok(out.len() >= max_records)
}

fn read_records_from_paths_bounded(
    paths: &[PathBuf],
    limits: StorageLimits,
    start: StreamOffset,
    max_records: usize,
    max_bytes: usize,
    max_raft_index: Option<u64>,
) -> Result<(Vec<StoredRecord>, usize)> {
    if max_records == 0 || max_bytes == 0 {
        return Ok((Vec::new(), 0));
    }
    let mut out = Vec::with_capacity(max_records.min(1024));
    let mut used_bytes = 0_usize;
    for path in paths {
        match read_segment_format(path)? {
            StorageFormatVersion::V1 => {
                let file_len = fs::metadata(path)?.len();
                let mut position = crate::format::SEGMENT_HEADER_LEN as u64;
                while position < file_len {
                    let scanned = crate::read_scanned_frame_at(path, position, limits)?;
                    position = position.checked_add(u64::from(scanned.encoded_len)).ok_or(
                        StorageError::ArithmeticOverflow("bounded record page position"),
                    )?;
                    if max_raft_index
                        .is_some_and(|committed| scanned.frame.raft_index().get() > committed)
                    {
                        return Ok((out, used_bytes));
                    }
                    let Frame::Data(batch) = scanned.frame else {
                        continue;
                    };
                    for (ordinal, record) in batch.records.into_iter().enumerate() {
                        let ordinal = u64::try_from(ordinal)
                            .map_err(|_| StorageError::ArithmeticOverflow("record ordinal"))?;
                        let offset = batch.base_stream_offset.checked_add(ordinal).ok_or(
                            StorageError::ArithmeticOverflow("bounded record stream offset"),
                        )?;
                        if offset < start {
                            continue;
                        }
                        if push_bounded_record(
                            &mut out,
                            &mut used_bytes,
                            offset,
                            record,
                            max_records,
                            max_bytes,
                        )? {
                            return Ok((out, used_bytes));
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
                        StorageError::ArithmeticOverflow("bounded V2 record page position"),
                    )?;
                    if max_raft_index
                        .is_some_and(|committed| scanned.frame.raft_index().get() > committed)
                    {
                        return Ok((out, used_bytes));
                    }
                    let RetainedFrameV2::SparseData(batch) = scanned.frame else {
                        continue;
                    };
                    for retained in batch.records {
                        if retained.offset < start {
                            continue;
                        }
                        if push_bounded_record(
                            &mut out,
                            &mut used_bytes,
                            retained.offset,
                            retained.record,
                            max_records,
                            max_bytes,
                        )? {
                            return Ok((out, used_bytes));
                        }
                    }
                }
            }
        }
    }
    Ok((out, used_bytes))
}

fn segment_file_name(path: &Path) -> Result<String> {
    path.file_name()
        .and_then(|name| name.to_str())
        .map(str::to_owned)
        .ok_or(StorageError::InvalidFormat(
            "segment file name is not valid UTF-8",
        ))
}

fn load_or_create_meta(
    directory: &Path,
    topic_id: TopicId,
    partition_id: PartitionId,
) -> Result<PartitionMeta> {
    let path = directory.join("partition.meta");
    match fs::read(&path) {
        Ok(bytes) => decode_partition_meta(&bytes),
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => {
            let meta = PartitionMeta {
                topic_id,
                partition_id,
                committed_raft_index: 0,
                metadata_epoch: 0,
            };
            write_meta(directory, &meta)?;
            Ok(meta)
        }
        Err(err) => Err(err.into()),
    }
}

fn write_meta(directory: &Path, meta: &PartitionMeta) -> Result<()> {
    let path = directory.join("partition.meta");
    atomic_write(&path, &encode_partition_meta(meta))
}

fn list_segments(directory: &Path) -> Result<Vec<PathBuf>> {
    let mut segments = Vec::new();
    for entry in fs::read_dir(directory)? {
        let entry = entry?;
        if !entry.file_type()?.is_file() {
            continue;
        }
        let name = entry.file_name();
        let Some(name) = name.to_str() else {
            continue;
        };
        if !name.ends_with(".log") {
            continue;
        }
        let id = SegmentFileId::parse_log_file_name(name)?;
        segments.push((id, entry.path()));
    }
    segments.sort_by_key(|(id, _)| (id.base_raft_index, id.base_stream_offset, id.generation));
    Ok(segments.into_iter().map(|(_, path)| path).collect())
}

fn recover_segments(
    paths: &[PathBuf],
    topic_id: TopicId,
    partition_id: PartitionId,
    committed_raft_index: u64,
    config: PartitionLogConfig,
) -> Result<(RaftIndex, StreamOffset)> {
    let mut expected_raft = RaftIndex::FIRST;
    let mut expected_stream = StreamOffset::new(0);

    for (position, path) in paths.iter().enumerate() {
        let is_active = position + 1 == paths.len();
        let scan = scan_segment(path, config.limits)?;
        if scan.header.topic_id != topic_id || scan.header.partition_id != partition_id {
            return Err(StorageError::InvalidFormat(
                "segment identity does not match partition",
            ));
        }
        if scan.header.base_raft_index != expected_raft
            || scan.header.base_stream_offset.get() != expected_stream.get()
        {
            return Err(StorageError::InvalidFormatOwned(format!(
                "segment boundary mismatch at {}: expected stream={}, raft={}, got stream={}, raft={}",
                path.display(),
                expected_stream.get(),
                expected_raft.get(),
                scan.header.base_stream_offset.get(),
                scan.header.base_raft_index.get(),
            )));
        }

        for scanned in &scan.frames {
            if scanned.frame.raft_index() != expected_raft {
                return Err(StorageError::InvalidFormatOwned(format!(
                    "non-contiguous Raft index: expected {}, got {}",
                    expected_raft.get(),
                    scanned.frame.raft_index().get(),
                )));
            }
            if let Frame::Data(batch) = &scanned.frame {
                if batch.base_stream_offset != expected_stream {
                    return Err(StorageError::InvalidFormatOwned(format!(
                        "non-contiguous stream offset: expected {}, got {}",
                        expected_stream.get(),
                        batch.base_stream_offset.get(),
                    )));
                }
                let count = u64::try_from(batch.records.len())
                    .map_err(|_| StorageError::ArithmeticOverflow("record count"))?;
                expected_stream = expected_stream
                    .checked_add(count)
                    .ok_or(StorageError::ArithmeticOverflow("next stream offset"))?;
            }
            expected_raft = expected_raft
                .checked_next()
                .ok_or(StorageError::ArithmeticOverflow("next Raft index"))?;
        }

        if let Some(failure) = scan.failure {
            if !is_active {
                return Err(StorageError::Corruption {
                    reason: format!(
                        "rolled segment {} is damaged at byte {} near Raft index {}: {:?}",
                        path.display(),
                        failure.position,
                        expected_raft.get(),
                        failure.kind,
                    ),
                });
            }
            if expected_raft.get() <= committed_raft_index {
                return Err(StorageError::CommittedCorruption {
                    raft_index: expected_raft.get(),
                    reason: format!(
                        "active segment damage at byte {} reaches committed boundary: {:?}",
                        failure.position, failure.kind,
                    ),
                });
            }

            match failure.kind {
                ScanFailureKind::IncompleteTail | ScanFailureKind::InvalidFrame(_) => {
                    let mut writer = SegmentWriter::open_existing(path)?;
                    writer.truncate_to(scan.valid_end)?;
                }
            }
        }

        let clean_scan = scan_segment(path, config.limits)?;
        if clean_scan.failure.is_some() {
            return Err(StorageError::InvalidFormat(
                "segment remained invalid after recovery",
            ));
        }
        rebuild_segment_indexes(
            path,
            &clean_scan.header,
            &clean_scan.frames,
            config.index_interval_bytes,
        )?;
    }

    let last_appended = expected_raft.get().saturating_sub(1);
    if committed_raft_index > last_appended {
        return Err(StorageError::CommittedCorruption {
            raft_index: last_appended.saturating_add(1),
            reason: format!(
                "partition metadata commits through {committed_raft_index} but log ends at {last_appended}",
            ),
        });
    }

    Ok((expected_raft, expected_stream))
}

fn recover_manifest_segments(
    paths: &[PathBuf],
    topic_id: TopicId,
    partition_id: PartitionId,
    committed_raft_index: u64,
    config: PartitionLogConfig,
) -> Result<(RaftIndex, StreamOffset)> {
    let active_path = paths.last().ok_or(StorageError::InvalidFormat(
        "manifest-governed partition has no active segment",
    ))?;
    if read_segment_format(active_path)? != StorageFormatVersion::V1 {
        return Err(StorageError::InvalidFormat(
            "manifest active segment must be V1",
        ));
    }

    let mut previous_raft = None;
    let mut previous_stream = None;
    for path in &paths[..paths.len() - 1] {
        match read_segment_format(path)? {
            StorageFormatVersion::V1 => {
                let scan = scan_segment(path, config.limits)?;
                if scan.header.topic_id != topic_id || scan.header.partition_id != partition_id {
                    return Err(StorageError::InvalidFormat(
                        "manifest V1 retained segment identity mismatch",
                    ));
                }
                if let Some(failure) = scan.failure {
                    return Err(StorageError::Corruption {
                        reason: format!(
                            "immutable manifest V1 segment {} is damaged at byte {}: {:?}",
                            path.display(),
                            failure.position,
                            failure.kind,
                        ),
                    });
                }
                for scanned in &scan.frames {
                    let raft = scanned.frame.raft_index().get();
                    if previous_raft.is_some_and(|previous| raft <= previous) {
                        return Err(StorageError::InvalidFormat(
                            "manifest retained Raft indexes are not strictly increasing",
                        ));
                    }
                    previous_raft = Some(raft);
                    if let Frame::Data(batch) = &scanned.frame {
                        for ordinal in 0..batch.records.len() {
                            let ordinal = u64::try_from(ordinal).map_err(|_| {
                                StorageError::ArithmeticOverflow("manifest V1 record ordinal")
                            })?;
                            let offset = batch.base_stream_offset.checked_add(ordinal).ok_or(
                                StorageError::ArithmeticOverflow(
                                    "manifest V1 retained stream offset",
                                ),
                            )?;
                            if previous_stream.is_some_and(|previous| offset.get() <= previous) {
                                return Err(StorageError::InvalidFormat(
                                    "manifest retained stream offsets are not strictly increasing",
                                ));
                            }
                            previous_stream = Some(offset.get());
                        }
                    }
                }
                rebuild_segment_indexes(
                    path,
                    &scan.header,
                    &scan.frames,
                    config.index_interval_bytes,
                )?;
            }
            StorageFormatVersion::V2 => {
                let scan = scan_retained_segment_v2(path, config.limits)?;
                if scan.header.topic_id != topic_id || scan.header.partition_id != partition_id {
                    return Err(StorageError::InvalidFormat(
                        "manifest V2 retained segment identity mismatch",
                    ));
                }
                for scanned in &scan.frames {
                    let raft = scanned.frame.raft_index().get();
                    if raft > committed_raft_index {
                        return Err(StorageError::InvalidFormat(
                            "V2 retained history extends above committed boundary",
                        ));
                    }
                    if previous_raft.is_some_and(|previous| raft <= previous) {
                        return Err(StorageError::InvalidFormat(
                            "manifest retained Raft indexes are not strictly increasing",
                        ));
                    }
                    previous_raft = Some(raft);
                    if let RetainedFrameV2::SparseData(batch) = &scanned.frame {
                        for retained in &batch.records {
                            let offset = retained.offset.get();
                            if previous_stream.is_some_and(|previous| offset <= previous) {
                                return Err(StorageError::InvalidFormat(
                                    "manifest retained stream offsets are not strictly increasing",
                                ));
                            }
                            previous_stream = Some(offset);
                        }
                    }
                }
                rebuild_retained_segment_indexes_v2(
                    path,
                    &scan.header,
                    &scan.frames,
                    config.index_interval_bytes,
                )?;
            }
        }
    }

    let mut scan = scan_segment(active_path, config.limits)?;
    if scan.header.topic_id != topic_id || scan.header.partition_id != partition_id {
        return Err(StorageError::InvalidFormat(
            "manifest active V1 segment identity mismatch",
        ));
    }
    if previous_raft.is_some_and(|previous| scan.header.base_raft_index.get() <= previous) {
        return Err(StorageError::InvalidFormat(
            "manifest active V1 segment does not begin after retained Raft history",
        ));
    }
    if previous_stream.is_some_and(|previous| scan.header.base_stream_offset.get() <= previous) {
        return Err(StorageError::InvalidFormat(
            "manifest active V1 segment does not begin after retained stream history",
        ));
    }

    let mut expected_raft = scan.header.base_raft_index;
    let mut expected_stream = StreamOffset::new(scan.header.base_stream_offset.get());
    for scanned in &scan.frames {
        if scanned.frame.raft_index() != expected_raft {
            return Err(StorageError::InvalidFormatOwned(format!(
                "manifest active V1 Raft gap: expected {}, got {}",
                expected_raft.get(),
                scanned.frame.raft_index().get(),
            )));
        }
        if let Frame::Data(batch) = &scanned.frame {
            if batch.base_stream_offset != expected_stream {
                return Err(StorageError::InvalidFormatOwned(format!(
                    "manifest active V1 stream gap: expected {}, got {}",
                    expected_stream.get(),
                    batch.base_stream_offset.get(),
                )));
            }
            let count = u64::try_from(batch.records.len())
                .map_err(|_| StorageError::ArithmeticOverflow("record count"))?;
            expected_stream = expected_stream
                .checked_add(count)
                .ok_or(StorageError::ArithmeticOverflow("next stream offset"))?;
        }
        expected_raft = expected_raft
            .checked_next()
            .ok_or(StorageError::ArithmeticOverflow("next Raft index"))?;
    }

    if let Some(failure) = scan.failure.take() {
        if expected_raft.get() <= committed_raft_index {
            return Err(StorageError::CommittedCorruption {
                raft_index: expected_raft.get(),
                reason: format!(
                    "manifest active segment damage at byte {} reaches committed boundary: {:?}",
                    failure.position, failure.kind,
                ),
            });
        }
        match failure.kind {
            ScanFailureKind::IncompleteTail | ScanFailureKind::InvalidFrame(_) => {
                let mut writer = SegmentWriter::open_existing(active_path)?;
                writer.truncate_to(scan.valid_end)?;
            }
        }
    }

    let clean = scan_segment(active_path, config.limits)?;
    if clean.failure.is_some() {
        return Err(StorageError::InvalidFormat(
            "manifest active V1 segment remained invalid after recovery",
        ));
    }
    rebuild_segment_indexes(
        active_path,
        &clean.header,
        &clean.frames,
        config.index_interval_bytes,
    )?;

    let last_appended = expected_raft.get().saturating_sub(1);
    if committed_raft_index > last_appended {
        return Err(StorageError::CommittedCorruption {
            raft_index: last_appended.saturating_add(1),
            reason: format!(
                "partition metadata commits through {committed_raft_index} but manifest live suffix ends at {last_appended}",
            ),
        });
    }
    Ok((expected_raft, expected_stream))
}

#[cfg(test)]
mod tests {
    use std::{
        fs::{self, OpenOptions},
        io::{Seek, SeekFrom, Write},
        time::{SystemTime, UNIX_EPOCH},
    };

    use astralog_common::{RaftTerm, TopicId};

    use super::*;
    use crate::{
        Compression, ControlEntry, RecordBatch, RetainedFrameV2, RetainedSegmentHeaderV2,
        SparseRetainedBatchV2, SparseRetainedRecordV2, encode_retained_frame_v2,
        encode_retained_segment_header_v2, index::index_paths,
    };

    fn test_directory(label: &str) -> PathBuf {
        let nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let path = std::env::temp_dir().join(format!(
            "astralog-partition-{label}-{}-{nanos}",
            std::process::id(),
        ));
        fs::create_dir_all(&path).unwrap();
        path
    }

    fn data_frame_with_compression(
        raft: u64,
        offset: u64,
        value: &[u8],
        compression: Compression,
    ) -> Frame {
        Frame::Data(RecordBatch {
            raft_term: RaftTerm::new(1),
            raft_index: RaftIndex::new(raft).unwrap(),
            base_stream_offset: StreamOffset::new(offset),
            producer: None,
            compression,
            records: vec![DataRecord {
                timestamp_ms: 1000 + offset as i64,
                key: Some(vec![offset as u8]),
                value: Some(value.to_vec()),
                headers: Vec::new(),
            }],
        })
    }

    fn data_frame(raft: u64, offset: u64, value: &[u8]) -> Frame {
        data_frame_with_compression(raft, offset, value, Compression::None)
    }

    fn p9d_log_start_frame(raft: u64, floor: u64) -> Frame {
        Frame::Control(ControlEntry {
            raft_term: RaftTerm::new(1),
            raft_index: RaftIndex::new(raft).unwrap(),
            control_kind: P9_CONTROL_ADVANCE_LOG_START_OFFSET,
            control_version: P9_MAINTENANCE_CONTROL_VERSION,
            body: crate::encode_log_start_offset_body(StreamOffset::new(floor)).to_vec(),
        })
    }

    fn three_record_frame(raft: u64) -> Frame {
        Frame::Data(RecordBatch {
            raft_term: RaftTerm::new(1),
            raft_index: RaftIndex::new(raft).unwrap(),
            base_stream_offset: StreamOffset::new(0),
            producer: None,
            compression: Compression::None,
            records: b"ABC"
                .iter()
                .copied()
                .enumerate()
                .map(|(index, byte)| DataRecord {
                    timestamp_ms: 1000 + index as i64,
                    key: Some(vec![byte]),
                    value: Some(vec![byte; 64]),
                    headers: Vec::new(),
                })
                .collect(),
        })
    }

    #[test]
    fn append_roll_reopen_and_read() {
        let directory = test_directory("roundtrip");
        let topic = TopicId::new([9; 16]);
        let config = PartitionLogConfig {
            max_segment_bytes: 400,
            max_segment_age_ms: None,
            index_interval_bytes: 1,
            limits: StorageLimits::default(),
        };
        {
            let mut log =
                PartitionLog::open_or_create(&directory, topic, PartitionId::new(0), 0, config)
                    .unwrap();
            for i in 0..5 {
                log.append(&data_frame(i + 1, i, b"value"), i as i64)
                    .unwrap();
                log.flush_data().unwrap();
            }
            log.mark_committed(5).unwrap();
            assert!(log.segment_paths().len() > 1);
        }

        let reopened =
            PartitionLog::open_or_create(&directory, topic, PartitionId::new(0), 100, config)
                .unwrap();
        assert_eq!(reopened.next_raft_index().get(), 6);
        assert_eq!(reopened.next_stream_offset().get(), 5);
        assert_eq!(reopened.committed_raft_index(), 5);
        let records = reopened
            .read_records_from(StreamOffset::new(2), 10)
            .unwrap();
        assert_eq!(records.len(), 3);
        assert_eq!(records[0].offset.get(), 2);
        fs::remove_dir_all(directory).unwrap();
    }

    #[test]
    fn p8_mixed_none_lz4_zstd_segment_restarts_with_identical_logical_records() {
        let directory = test_directory("p8-mixed-codecs");
        let topic = TopicId::new([0x81; 16]);
        let config = PartitionLogConfig::default();
        let values = [vec![b'N'; 1024], vec![b'L'; 1024], vec![b'Z'; 1024]];
        {
            let mut log =
                PartitionLog::open_or_create(&directory, topic, PartitionId::new(0), 0, config)
                    .unwrap();
            for (ordinal, codec) in [Compression::None, Compression::Lz4, Compression::Zstd]
                .into_iter()
                .enumerate()
            {
                log.append(
                    &data_frame_with_compression(
                        u64::try_from(ordinal + 1).unwrap(),
                        u64::try_from(ordinal).unwrap(),
                        &values[ordinal],
                        codec,
                    ),
                    i64::try_from(ordinal).unwrap(),
                )
                .unwrap();
                log.flush_data().unwrap();
            }
            log.mark_committed(3).unwrap();
            let frames = log.read_frames().unwrap();
            assert_eq!(frames.len(), 3);
            assert_eq!(
                frames
                    .iter()
                    .map(|frame| match frame {
                        Frame::Data(batch) => batch.compression,
                        Frame::Control(_) => unreachable!(),
                    })
                    .collect::<Vec<_>>(),
                vec![Compression::None, Compression::Lz4, Compression::Zstd]
            );
        }

        let reopened =
            PartitionLog::open_or_create(&directory, topic, PartitionId::new(0), 100, config)
                .unwrap();
        assert_eq!(reopened.next_raft_index().get(), 4);
        assert_eq!(reopened.next_stream_offset().get(), 3);
        assert_eq!(reopened.committed_raft_index(), 3);
        let records = reopened
            .read_records_from(StreamOffset::new(0), 10)
            .unwrap();
        assert_eq!(records.len(), 3);
        for (ordinal, record) in records.iter().enumerate() {
            assert_eq!(record.offset.get(), u64::try_from(ordinal).unwrap());
            assert_eq!(
                record.record.value.as_deref(),
                Some(values[ordinal].as_slice())
            );
        }
        fs::remove_dir_all(directory).unwrap();
    }

    #[test]
    fn control_only_roll_has_unique_segment_name() {
        let directory = test_directory("control-roll");
        let topic = TopicId::new([7; 16]);
        let config = PartitionLogConfig {
            max_segment_bytes: 250,
            max_segment_age_ms: None,
            index_interval_bytes: 1,
            limits: StorageLimits::default(),
        };
        let mut log =
            PartitionLog::open_or_create(&directory, topic, PartitionId::new(0), 0, config)
                .unwrap();
        for i in 1..=4_u64 {
            log.append(
                &Frame::Control(ControlEntry {
                    raft_term: RaftTerm::new(1),
                    raft_index: RaftIndex::new(i).unwrap(),
                    control_kind: 1,
                    control_version: 1,
                    body: vec![i as u8; 32],
                }),
                i as i64,
            )
            .unwrap();
            log.flush_data().unwrap();
        }
        assert!(log.segment_paths().len() > 1);
        let mut names = log
            .segment_paths()
            .iter()
            .map(|path| path.file_name().unwrap().to_owned())
            .collect::<Vec<_>>();
        names.sort();
        names.dedup();
        assert_eq!(names.len(), log.segment_paths().len());
        fs::remove_dir_all(directory).unwrap();
    }

    #[test]
    fn uncommitted_corrupt_tail_is_truncated() {
        let directory = test_directory("tail-truncate");
        let topic = TopicId::new([6; 16]);
        let config = PartitionLogConfig::default();
        let active_path;
        let clean_len;
        {
            let mut log =
                PartitionLog::open_or_create(&directory, topic, PartitionId::new(0), 0, config)
                    .unwrap();
            log.append(&data_frame(1, 0, b"committed"), 0).unwrap();
            log.flush_data().unwrap();
            log.mark_committed(1).unwrap();
            clean_len = fs::metadata(&log.segment_paths()[0]).unwrap().len();
            let second = data_frame(2, 1, b"uncommitted");
            log.append(&second, 1).unwrap();
            log.flush_data().unwrap();
            active_path = log.segment_paths()[0].clone();
        }
        let damaged_len = fs::metadata(&active_path).unwrap().len() - 3;
        OpenOptions::new()
            .write(true)
            .open(&active_path)
            .unwrap()
            .set_len(damaged_len)
            .unwrap();

        let reopened =
            PartitionLog::open_or_create(&directory, topic, PartitionId::new(0), 2, config)
                .unwrap();
        assert_eq!(reopened.next_raft_index().get(), 2);
        assert_eq!(fs::metadata(&active_path).unwrap().len(), clean_len);
        fs::remove_dir_all(directory).unwrap();
    }

    #[test]
    fn committed_corruption_is_never_truncated() {
        let directory = test_directory("committed-corrupt");
        let topic = TopicId::new([5; 16]);
        let config = PartitionLogConfig::default();
        let active_path;
        {
            let mut log =
                PartitionLog::open_or_create(&directory, topic, PartitionId::new(0), 0, config)
                    .unwrap();
            log.append(&data_frame(1, 0, b"must-survive"), 0).unwrap();
            log.flush_data().unwrap();
            log.mark_committed(1).unwrap();
            active_path = log.segment_paths()[0].clone();
        }

        let mut file = OpenOptions::new()
            .read(true)
            .write(true)
            .open(&active_path)
            .unwrap();
        file.seek(SeekFrom::End(-5)).unwrap();
        file.write_all(&[0xff]).unwrap();
        file.sync_all().unwrap();

        let result =
            PartitionLog::open_or_create(&directory, topic, PartitionId::new(0), 2, config);
        assert!(matches!(
            result,
            Err(StorageError::CommittedCorruption { raft_index: 1, .. })
        ));
        fs::remove_dir_all(directory).unwrap();
    }

    #[test]
    fn missing_indexes_are_rebuilt_on_restart() {
        let directory = test_directory("missing-index");
        let topic = TopicId::new([4; 16]);
        let config = PartitionLogConfig::default();
        let log_path;
        {
            let mut log =
                PartitionLog::open_or_create(&directory, topic, PartitionId::new(0), 0, config)
                    .unwrap();
            log.append(&data_frame(1, 0, b"indexed"), 0).unwrap();
            log.flush_data().unwrap();
            log.mark_committed(1).unwrap();
            log_path = log.segment_paths()[0].clone();
        }
        let header = crate::read_segment_header(&log_path).unwrap();
        let id = SegmentFileId::from_header(&header);
        for extension in ["index", "timeindex", "raftindex"] {
            let path = directory.join(id.index_file_name(extension));
            if path.exists() {
                fs::remove_file(&path).unwrap();
            }
        }

        let _reopened =
            PartitionLog::open_or_create(&directory, topic, PartitionId::new(0), 1, config)
                .unwrap();
        for extension in ["index", "timeindex", "raftindex"] {
            assert!(directory.join(id.index_file_name(extension)).is_file());
        }
        fs::remove_dir_all(directory).unwrap();
    }

    #[test]
    fn stale_index_bytes_are_rebuilt_from_log() {
        let directory = test_directory("stale-index");
        let topic = TopicId::new([3; 16]);
        let config = PartitionLogConfig::default();
        let log_path;
        {
            let mut log =
                PartitionLog::open_or_create(&directory, topic, PartitionId::new(0), 0, config)
                    .unwrap();
            log.append(&data_frame(1, 0, b"truth-is-log"), 0).unwrap();
            log.flush_data().unwrap();
            log.mark_committed(1).unwrap();
            log_path = log.segment_paths()[0].clone();
        }
        let header = crate::read_segment_header(&log_path).unwrap();
        let id = SegmentFileId::from_header(&header);
        let offset_index = directory.join(id.index_file_name("index"));
        fs::write(&offset_index, b"stale-garbage-index").unwrap();

        let _reopened =
            PartitionLog::open_or_create(&directory, topic, PartitionId::new(0), 1, config)
                .unwrap();
        let rebuilt = fs::read(&offset_index).unwrap();
        assert_ne!(rebuilt, b"stale-garbage-index");
        fs::remove_dir_all(directory).unwrap();
    }

    #[test]
    fn restart_after_each_append_and_roll_stage_preserves_inventory() {
        let directory = test_directory("restart-stages");
        let topic = TopicId::new([2; 16]);
        let config = PartitionLogConfig {
            max_segment_bytes: 1024,
            max_segment_age_ms: None,
            index_interval_bytes: 1,
            limits: StorageLimits::default(),
        };

        {
            let log =
                PartitionLog::open_or_create(&directory, topic, PartitionId::new(0), 0, config)
                    .unwrap();
            assert_eq!(log.next_raft_index(), RaftIndex::FIRST);
        }

        {
            let mut log =
                PartitionLog::open_or_create(&directory, topic, PartitionId::new(0), 1, config)
                    .unwrap();
            log.append(
                &Frame::Control(ControlEntry {
                    raft_term: RaftTerm::new(1),
                    raft_index: RaftIndex::FIRST,
                    control_kind: 1,
                    control_version: 1,
                    body: b"control".to_vec(),
                }),
                1,
            )
            .unwrap();
            log.flush_data().unwrap();
        }

        {
            let mut log =
                PartitionLog::open_or_create(&directory, topic, PartitionId::new(0), 2, config)
                    .unwrap();
            assert_eq!(log.next_raft_index().get(), 2);
            assert_eq!(log.next_stream_offset().get(), 0);
            log.append(&data_frame(2, 0, b"first-data"), 2).unwrap();
            log.flush_data().unwrap();
        }

        {
            let mut log =
                PartitionLog::open_or_create(&directory, topic, PartitionId::new(0), 3, config)
                    .unwrap();
            assert_eq!(log.next_raft_index().get(), 3);
            assert_eq!(log.next_stream_offset().get(), 1);
            log.roll(3).unwrap();
            assert_eq!(log.segment_paths().len(), 2);
        }

        {
            let mut log =
                PartitionLog::open_or_create(&directory, topic, PartitionId::new(0), 4, config)
                    .unwrap();
            assert_eq!(log.segment_paths().len(), 2);
            assert_eq!(log.next_raft_index().get(), 3);
            assert_eq!(log.next_stream_offset().get(), 1);
            log.append(&data_frame(3, 1, b"second-data"), 4).unwrap();
            log.flush_data().unwrap();
            log.mark_committed(3).unwrap();
        }

        let final_log =
            PartitionLog::open_or_create(&directory, topic, PartitionId::new(0), 5, config)
                .unwrap();
        assert_eq!(final_log.next_raft_index().get(), 4);
        assert_eq!(final_log.next_stream_offset().get(), 2);
        assert_eq!(final_log.committed_raft_index(), 3);
        assert_eq!(
            final_log
                .read_records_from(StreamOffset::new(0), 10)
                .unwrap()
                .len(),
            2,
        );
        fs::remove_dir_all(directory).unwrap();
    }

    #[test]
    fn commit_cannot_outrun_durable_flush() {
        let directory = test_directory("commit-flush-order");
        let topic = TopicId::new([1; 16]);
        let config = PartitionLogConfig::default();
        let mut log =
            PartitionLog::open_or_create(&directory, topic, PartitionId::new(0), 0, config)
                .unwrap();
        log.append(&data_frame(1, 0, b"not-durable-yet"), 0)
            .unwrap();
        assert_eq!(log.durable_raft_index(), 0);
        assert!(log.mark_committed(1).is_err());
        assert_eq!(log.committed_raft_index(), 0);

        log.flush_data().unwrap();
        assert_eq!(log.durable_raft_index(), 1);
        log.mark_committed(1).unwrap();
        assert_eq!(log.committed_raft_index(), 1);
        fs::remove_dir_all(directory).unwrap();
    }

    #[test]
    fn restart_does_not_assume_uncommitted_tail_was_durable() {
        let directory = test_directory("restart-durability");
        let topic = TopicId::new([0x21; 16]);
        let config = PartitionLogConfig::default();
        {
            let mut log =
                PartitionLog::open_or_create(&directory, topic, PartitionId::new(0), 0, config)
                    .unwrap();
            log.append(&data_frame(1, 0, b"committed"), 0).unwrap();
            log.flush_data().unwrap();
            log.mark_committed(1).unwrap();

            log.append(&data_frame(2, 1, b"visible-but-not-proven-durable"), 1)
                .unwrap();
        }

        let mut reopened =
            PartitionLog::open_or_create(&directory, topic, PartitionId::new(0), 2, config)
                .unwrap();
        assert_eq!(reopened.next_raft_index().get(), 3);
        assert_eq!(reopened.durable_raft_index(), 1);
        assert!(reopened.mark_committed(2).is_err());

        reopened.flush_data().unwrap();
        assert_eq!(reopened.durable_raft_index(), 2);
        reopened.mark_committed(2).unwrap();
        fs::remove_dir_all(directory).unwrap();
    }

    #[test]
    fn raft_suffix_truncation_inside_active_segment_restores_positions() {
        let directory = test_directory("raft-truncate-active");
        let topic = TopicId::new([0x31; 16]);
        let config = PartitionLogConfig::default();
        let mut log =
            PartitionLog::open_or_create(&directory, topic, PartitionId::new(0), 0, config)
                .unwrap();
        for i in 0..4_u64 {
            log.append(&data_frame(i + 1, i, &[i as u8]), i as i64)
                .unwrap();
            log.flush_data().unwrap();
        }
        log.mark_committed(2).unwrap();

        log.truncate_raft_suffix(3).unwrap();
        assert_eq!(log.next_raft_index().get(), 3);
        assert_eq!(log.next_stream_offset().get(), 2);
        assert_eq!(log.durable_raft_index(), 2);
        assert_eq!(log.committed_raft_index(), 2);
        assert_eq!(
            log.read_records_from(StreamOffset::new(0), 10)
                .unwrap()
                .len(),
            2
        );

        log.append(&data_frame(3, 2, b"replacement"), 10).unwrap();
        log.flush_data().unwrap();
        drop(log);
        let reopened =
            PartitionLog::open_or_create(&directory, topic, PartitionId::new(0), 11, config)
                .unwrap();
        assert_eq!(reopened.next_raft_index().get(), 4);
        assert_eq!(reopened.next_stream_offset().get(), 3);
        fs::remove_dir_all(directory).unwrap();
    }

    #[test]
    fn p8_uncommitted_compressed_suffix_truncates_without_crossing_commit() {
        let directory = test_directory("p8-compressed-truncate");
        let topic = TopicId::new([0x82; 16]);
        let config = PartitionLogConfig::default();
        let mut log =
            PartitionLog::open_or_create(&directory, topic, PartitionId::new(0), 0, config)
                .unwrap();

        for (index, codec, byte) in [
            (1_u64, Compression::None, b'A'),
            (2, Compression::Lz4, b'B'),
            (3, Compression::Zstd, b'C'),
            (4, Compression::Lz4, b'D'),
        ] {
            log.append(
                &data_frame_with_compression(index, index - 1, &vec![byte; 2048], codec),
                i64::try_from(index).unwrap(),
            )
            .unwrap();
            log.flush_data().unwrap();
        }
        log.mark_committed(2).unwrap();
        assert!(matches!(
            log.truncate_raft_suffix(2),
            Err(StorageError::CommittedCorruption { raft_index: 2, .. })
        ));

        log.truncate_raft_suffix(3).unwrap();
        assert_eq!(log.next_raft_index().get(), 3);
        assert_eq!(log.next_stream_offset().get(), 2);
        assert_eq!(log.committed_raft_index(), 2);
        assert_eq!(log.read_frames().unwrap().len(), 2);

        log.append(
            &data_frame_with_compression(3, 2, &vec![b'R'; 2048], Compression::Zstd),
            10,
        )
        .unwrap();
        log.flush_data().unwrap();
        drop(log);

        let reopened =
            PartitionLog::open_or_create(&directory, topic, PartitionId::new(0), 11, config)
                .unwrap();
        assert_eq!(reopened.next_raft_index().get(), 4);
        assert_eq!(reopened.next_stream_offset().get(), 3);
        let frames = reopened.read_frames().unwrap();
        let Frame::Data(replacement) = &frames[2] else {
            unreachable!()
        };
        assert_eq!(replacement.compression, Compression::Zstd);
        assert_eq!(
            replacement.records[0].value.as_deref(),
            Some(&vec![b'R'; 2048][..])
        );
        fs::remove_dir_all(directory).unwrap();
    }

    #[test]
    fn raft_suffix_truncation_across_rolled_segments_deletes_later_families() {
        let directory = test_directory("raft-truncate-rolled");
        let topic = TopicId::new([0x32; 16]);
        let config = PartitionLogConfig {
            max_segment_bytes: 250,
            max_segment_age_ms: None,
            index_interval_bytes: 1,
            limits: StorageLimits::default(),
        };
        let mut log =
            PartitionLog::open_or_create(&directory, topic, PartitionId::new(0), 0, config)
                .unwrap();
        for i in 0..7_u64 {
            log.append(&data_frame(i + 1, i, &[i as u8; 24]), i as i64)
                .unwrap();
            log.flush_data().unwrap();
        }
        log.mark_committed(2).unwrap();
        assert!(log.segment_paths().len() >= 3);
        let before = log.segment_paths().len();

        log.truncate_raft_suffix(4).unwrap();
        assert_eq!(log.next_raft_index().get(), 4);
        assert_eq!(log.next_stream_offset().get(), 3);
        assert!(log.segment_paths().len() < before);
        assert_eq!(log.read_frames().unwrap().len(), 3);

        log.append(&data_frame(4, 3, b"new-tail"), 20).unwrap();
        log.flush_data().unwrap();
        drop(log);
        let reopened =
            PartitionLog::open_or_create(&directory, topic, PartitionId::new(0), 21, config)
                .unwrap();
        assert_eq!(reopened.next_raft_index().get(), 5);
        assert_eq!(reopened.next_stream_offset().get(), 4);
        fs::remove_dir_all(directory).unwrap();
    }

    #[test]
    fn raft_suffix_truncation_never_crosses_committed_boundary() {
        let directory = test_directory("raft-truncate-committed");
        let topic = TopicId::new([0x33; 16]);
        let config = PartitionLogConfig::default();
        let mut log =
            PartitionLog::open_or_create(&directory, topic, PartitionId::new(0), 0, config)
                .unwrap();
        for i in 0..3_u64 {
            log.append(&data_frame(i + 1, i, b"value"), i as i64)
                .unwrap();
            log.flush_data().unwrap();
        }
        log.mark_committed(2).unwrap();
        assert!(matches!(
            log.truncate_raft_suffix(2),
            Err(StorageError::CommittedCorruption { raft_index: 2, .. })
        ));
        assert_eq!(log.next_raft_index().get(), 4);
        fs::remove_dir_all(directory).unwrap();
    }

    #[test]
    fn committed_stream_high_watermark_ignores_control_and_uncommitted_tail() {
        let directory = test_directory("committed-stream-watermark");
        let topic = TopicId::new([0x34; 16]);
        let config = PartitionLogConfig::default();
        let mut log =
            PartitionLog::open_or_create(&directory, topic, PartitionId::new(0), 0, config)
                .unwrap();
        log.append(
            &Frame::Control(ControlEntry {
                raft_term: RaftTerm::new(1),
                raft_index: RaftIndex::new(1).unwrap(),
                control_kind: 1,
                control_version: 1,
                body: Vec::new(),
            }),
            0,
        )
        .unwrap();
        log.append(&data_frame(2, 0, b"a"), 1).unwrap();
        log.append(&data_frame(3, 1, b"b"), 2).unwrap();
        log.flush_data().unwrap();
        log.mark_committed(2).unwrap();

        assert_eq!(log.committed_stream_high_watermark().unwrap().get(), 1);
        fs::remove_dir_all(directory).unwrap();
    }

    #[test]
    fn p9c_legacy_adoption_preserves_v1_bytes_and_manifest_rolls_generation() {
        let directory = test_directory("p9c-adopt-roll");
        let topic = TopicId::new([0x91; 16]);
        let config = PartitionLogConfig::default();
        let mut log =
            PartitionLog::open_or_create(&directory, topic, PartitionId::new(0), 0, config)
                .unwrap();
        log.append(&data_frame(1, 0, b"legacy-v1"), 0).unwrap();
        log.flush_data().unwrap();
        log.mark_committed(1).unwrap();
        let legacy_path = log.segment_paths()[0].clone();
        let legacy_bytes = fs::read(&legacy_path).unwrap();
        assert!(load_manifest(&directory).unwrap().is_none());

        drop(log);
        let mut log =
            PartitionLog::open_or_create(&directory, topic, PartitionId::new(0), 1, config)
                .unwrap();
        assert_eq!(log.manifest_generation(), None);
        assert!(load_manifest(&directory).unwrap().is_none());
        assert_eq!(fs::read(&legacy_path).unwrap(), legacy_bytes);

        assert_eq!(log.adopt_manifest_inventory().unwrap(), 1);
        assert_eq!(log.manifest_generation(), Some(1));
        assert_eq!(fs::read(&legacy_path).unwrap(), legacy_bytes);
        let adopted = load_manifest(&directory).unwrap().unwrap();
        assert_eq!(adopted.generation, 1);
        assert_eq!(adopted.entries.len(), 1);
        assert!(adopted.entries[0].is_mutable_active());

        log.roll(10).unwrap();
        assert_eq!(log.manifest_generation(), Some(2));
        let rolled = load_manifest(&directory).unwrap().unwrap();
        assert_eq!(rolled.generation, 2);
        assert_eq!(rolled.entries.len(), 2);
        assert_eq!(
            rolled.entries[0].expected_size,
            Some(legacy_bytes.len() as u64)
        );
        assert!(rolled.entries[1].is_mutable_active());
        drop(log);

        let reopened =
            PartitionLog::open_or_create(&directory, topic, PartitionId::new(0), 11, config)
                .unwrap();
        assert_eq!(reopened.manifest_generation(), Some(2));
        assert_eq!(reopened.next_raft_index().get(), 2);
        assert_eq!(reopened.next_stream_offset().get(), 1);
        assert_eq!(fs::read(&legacy_path).unwrap(), legacy_bytes);
        fs::remove_dir_all(directory).unwrap();
    }

    #[test]
    fn p9c_live_manifest_requires_exactly_one_mutable_v1_active_segment() {
        let topic = TopicId::new([0x95; 16]);
        let partition = PartitionId::new(0);
        let config = PartitionLogConfig::default();

        let no_mutable = test_directory("p9c-manifest-no-mutable");
        let v1_path = {
            let writer = SegmentWriter::create(
                &no_mutable,
                SegmentHeader {
                    topic_id: topic,
                    partition_id: partition,
                    generation: 0,
                    base_stream_offset: SegmentBaseOffset::new(0),
                    base_raft_index: RaftIndex::FIRST,
                    created_at_ms: 0,
                },
            )
            .unwrap();
            writer.path().to_path_buf()
        };
        let manifest = crate::SegmentManifest {
            generation: 1,
            entries: vec![ManifestEntry::immutable(
                segment_file_name(&v1_path).unwrap(),
                fs::metadata(&v1_path).unwrap().len(),
            )],
        };
        fs::write(
            no_mutable.join("segments.manifest"),
            crate::encode_manifest(&manifest).unwrap(),
        )
        .unwrap();
        assert!(PartitionLog::open_or_create(&no_mutable, topic, partition, 0, config).is_err());
        fs::remove_dir_all(no_mutable).unwrap();

        let v2_mutable = test_directory("p9c-manifest-v2-mutable");
        let header = RetainedSegmentHeaderV2 {
            topic_id: topic,
            partition_id: partition,
            generation: 1,
            base_stream_offset: SegmentBaseOffset::new(0),
            base_raft_index: RaftIndex::FIRST,
            created_at_ms: 0,
        };
        let v2_name = SegmentFileId::from_header(&header.identity_header()).log_file_name();
        fs::write(
            v2_mutable.join(&v2_name),
            encode_retained_segment_header_v2(&header),
        )
        .unwrap();
        let manifest = crate::SegmentManifest {
            generation: 1,
            entries: vec![ManifestEntry::mutable_active(v2_name)],
        };
        fs::write(
            v2_mutable.join("segments.manifest"),
            crate::encode_manifest(&manifest).unwrap(),
        )
        .unwrap();
        assert!(PartitionLog::open_or_create(&v2_mutable, topic, partition, 0, config).is_err());
        fs::remove_dir_all(v2_mutable).unwrap();
    }

    #[test]
    fn p9c_manifest_missing_reference_fails_closed_without_directory_fallback() {
        let directory = test_directory("p9c-manifest-fail-closed");
        let topic = TopicId::new([0x92; 16]);
        let config = PartitionLogConfig::default();
        let active_path = {
            let mut log =
                PartitionLog::open_or_create(&directory, topic, PartitionId::new(0), 0, config)
                    .unwrap();
            log.append(&data_frame(1, 0, b"one"), 0).unwrap();
            log.flush_data().unwrap();
            log.mark_committed(1).unwrap();
            log.adopt_manifest_inventory().unwrap();
            log.segment_paths()[0].clone()
        };
        let stray_header = SegmentHeader {
            topic_id: topic,
            partition_id: PartitionId::new(0),
            generation: 9,
            base_stream_offset: SegmentBaseOffset::new(99),
            base_raft_index: RaftIndex::new(99).unwrap(),
            created_at_ms: 0,
        };
        let stray = SegmentWriter::create(&directory, stray_header).unwrap();
        drop(stray);
        fs::remove_file(active_path).unwrap();

        assert!(
            PartitionLog::open_or_create(&directory, topic, PartitionId::new(0), 1, config)
                .is_err(),
            "a present manifest must never fall back to stray directory-scanned segments"
        );
        fs::remove_dir_all(directory).unwrap();
    }

    #[test]
    fn p9c_mixed_v2_history_fetches_sparse_offsets_but_p3_reads_only_v1_suffix() {
        let directory = test_directory("p9c-mixed-v2-v1");
        let topic = TopicId::new([0x93; 16]);
        let partition = PartitionId::new(0);
        let config = PartitionLogConfig {
            index_interval_bytes: 1,
            ..PartitionLogConfig::default()
        };

        let (active_name, old_v1_history) = {
            let mut log =
                PartitionLog::open_or_create(&directory, topic, partition, 0, config).unwrap();
            log.append(&three_record_frame(1), 0).unwrap();
            log.flush_data().unwrap();
            log.mark_committed(1).unwrap();
            log.roll(1).unwrap();
            let old_v1 = log.segment_paths()[0].clone();
            log.adopt_manifest_inventory().unwrap();
            let active_name = segment_file_name(log.segment_paths().last().unwrap()).unwrap();
            (active_name, old_v1)
        };

        let v2_header = RetainedSegmentHeaderV2 {
            topic_id: topic,
            partition_id: partition,
            generation: 1,
            base_stream_offset: SegmentBaseOffset::new(0),
            base_raft_index: RaftIndex::FIRST,
            created_at_ms: 2,
        };
        let v2_frame = RetainedFrameV2::SparseData(SparseRetainedBatchV2 {
            raft_term: RaftTerm::new(1),
            raft_index: RaftIndex::FIRST,
            compression: Compression::Lz4,
            records: vec![
                SparseRetainedRecordV2 {
                    offset: StreamOffset::new(0),
                    record: DataRecord {
                        timestamp_ms: 1000,
                        key: Some(vec![b'A']),
                        value: Some(vec![b'A'; 64]),
                        headers: Vec::new(),
                    },
                },
                SparseRetainedRecordV2 {
                    offset: StreamOffset::new(2),
                    record: DataRecord {
                        timestamp_ms: 1002,
                        key: Some(vec![b'C']),
                        value: Some(vec![b'C'; 64]),
                        headers: Vec::new(),
                    },
                },
            ],
        });
        let mut v2_bytes = encode_retained_segment_header_v2(&v2_header).to_vec();
        v2_bytes.extend_from_slice(&encode_retained_frame_v2(&v2_frame, config.limits).unwrap());
        let v2_name = SegmentFileId::from_header(&v2_header.identity_header()).log_file_name();

        let mut generation = GenerationTransaction::new(&directory, 2).unwrap();
        generation
            .stage_file(v2_name.clone(), &v2_bytes, false)
            .unwrap();
        generation
            .retain(ManifestEntry::mutable_active(active_name))
            .unwrap();
        generation.commit().unwrap();

        let mut reopened =
            PartitionLog::open_or_create(&directory, topic, partition, 3, config).unwrap();
        assert_eq!(reopened.manifest_generation(), Some(2));
        assert_eq!(reopened.next_raft_index().get(), 2);
        assert_eq!(reopened.next_stream_offset().get(), 3);
        assert_eq!(reopened.committed_stream_high_watermark().unwrap().get(), 3);
        let records = reopened
            .read_records_from(StreamOffset::new(0), 10)
            .unwrap();
        assert_eq!(
            records
                .iter()
                .map(|record| record.offset.get())
                .collect::<Vec<_>>(),
            vec![0, 2]
        );
        assert!(reopened.read_frames().unwrap().is_empty());
        assert!(
            reopened
                .read_frames_from_raft(RaftIndex::FIRST, 10)
                .unwrap()
                .is_empty(),
            "V2 retained history must never be reconstructed into P3 live history"
        );

        let v2_path = directory.join(&v2_name);
        let v2_identity = v2_header.identity_header();
        for index in index_paths(&v2_path, &v2_identity).unwrap() {
            assert!(index.exists(), "V2 derived indexes must rebuild on open");
        }
        assert!(
            old_v1_history.exists(),
            "unreferenced old V1 may remain garbage"
        );

        reopened.append(&data_frame(2, 3, b"live-v1"), 4).unwrap();
        reopened.flush_data().unwrap();
        reopened.mark_committed(2).unwrap();
        let live = reopened.read_frames().unwrap();
        assert_eq!(live.len(), 1);
        assert_eq!(live[0].raft_index().get(), 2);
        drop(reopened);

        let reopened =
            PartitionLog::open_or_create(&directory, topic, partition, 5, config).unwrap();
        let records = reopened
            .read_records_from(StreamOffset::new(0), 10)
            .unwrap();
        assert_eq!(
            records
                .iter()
                .map(|record| record.offset.get())
                .collect::<Vec<_>>(),
            vec![0, 2, 3]
        );
        assert_eq!(reopened.read_frames().unwrap().len(), 1);
        fs::remove_dir_all(directory).unwrap();
    }

    #[test]
    fn p9c_manifest_boundary_preserves_committed_high_watermark_after_trailing_holes() {
        let directory = test_directory("p9c-v2-high-watermark");
        let topic = TopicId::new([0x96; 16]);
        let partition = PartitionId::new(0);
        let config = PartitionLogConfig::default();
        let active_name = {
            let mut log =
                PartitionLog::open_or_create(&directory, topic, partition, 0, config).unwrap();
            log.append(&three_record_frame(1), 0).unwrap();
            log.flush_data().unwrap();
            log.mark_committed(1).unwrap();
            log.roll(1).unwrap();
            log.adopt_manifest_inventory().unwrap();
            segment_file_name(log.segment_paths().last().unwrap()).unwrap()
        };

        let header = RetainedSegmentHeaderV2 {
            topic_id: topic,
            partition_id: partition,
            generation: 1,
            base_stream_offset: SegmentBaseOffset::new(0),
            base_raft_index: RaftIndex::FIRST,
            created_at_ms: 2,
        };
        let frame = RetainedFrameV2::SparseData(SparseRetainedBatchV2 {
            raft_term: RaftTerm::new(1),
            raft_index: RaftIndex::FIRST,
            compression: Compression::None,
            records: vec![SparseRetainedRecordV2 {
                offset: StreamOffset::new(0),
                record: DataRecord {
                    timestamp_ms: 1000,
                    key: Some(b"only-survivor".to_vec()),
                    value: Some(b"v".to_vec()),
                    headers: Vec::new(),
                },
            }],
        });
        let mut bytes = encode_retained_segment_header_v2(&header).to_vec();
        bytes.extend_from_slice(&encode_retained_frame_v2(&frame, config.limits).unwrap());
        let name = SegmentFileId::from_header(&header.identity_header()).log_file_name();
        let mut generation = GenerationTransaction::new(&directory, 2).unwrap();
        generation.stage_file(name, &bytes, false).unwrap();
        generation
            .retain(ManifestEntry::mutable_active(active_name))
            .unwrap();
        generation.commit().unwrap();

        let reopened =
            PartitionLog::open_or_create(&directory, topic, partition, 3, config).unwrap();
        assert_eq!(
            reopened.committed_stream_high_watermark().unwrap().get(),
            3,
            "manifest active V1 base_stream_offset preserves original committed HWM even when V2 survivors end at offset 0",
        );
        let records = reopened
            .read_records_from(StreamOffset::new(0), 10)
            .unwrap();
        assert_eq!(records.len(), 1);
        assert_eq!(records[0].offset.get(), 0);
        fs::remove_dir_all(directory).unwrap();
    }

    #[test]
    fn p9c_manifest_suffix_truncation_cow_publishes_new_generation() {
        let directory = test_directory("p9c-manifest-truncate-cow");
        let topic = TopicId::new([0x94; 16]);
        let partition = PartitionId::new(0);
        let config = PartitionLogConfig {
            index_interval_bytes: 1,
            ..PartitionLogConfig::default()
        };
        let mut log =
            PartitionLog::open_or_create(&directory, topic, partition, 0, config).unwrap();
        log.append(&data_frame(1, 0, b"committed"), 0).unwrap();
        log.flush_data().unwrap();
        log.mark_committed(1).unwrap();
        log.roll(1).unwrap();
        log.append(&data_frame(2, 1, b"discard-me"), 2).unwrap();
        log.flush_data().unwrap();
        log.roll(3).unwrap();
        assert_eq!(log.segment_paths().len(), 3);
        log.adopt_manifest_inventory().unwrap();
        assert_eq!(log.manifest_generation(), Some(1));

        log.truncate_raft_suffix(2).unwrap();
        assert_eq!(log.manifest_generation(), Some(2));
        assert_eq!(log.next_raft_index().get(), 2);
        assert_eq!(log.next_stream_offset().get(), 1);
        let manifest = load_manifest(&directory).unwrap().unwrap();
        assert_eq!(manifest.generation, 2);
        assert_eq!(manifest.entries.len(), 2);
        assert!(!manifest.entries[0].is_mutable_active());
        assert!(manifest.entries[1].is_mutable_active());
        log.append(&data_frame(2, 1, b"replacement"), 4).unwrap();
        log.flush_data().unwrap();
        drop(log);

        let reopened =
            PartitionLog::open_or_create(&directory, topic, partition, 5, config).unwrap();
        assert_eq!(reopened.manifest_generation(), Some(2));
        assert_eq!(reopened.next_raft_index().get(), 3);
        assert_eq!(reopened.next_stream_offset().get(), 2);
        assert_eq!(reopened.read_frames().unwrap().len(), 2);
        fs::remove_dir_all(directory).unwrap();
    }

    #[test]
    fn p9d_committed_log_start_is_zero_monotonic_bounded_and_filters_reads() {
        let directory = test_directory("p9d-log-start");
        let topic = TopicId::new([0xd1; 16]);
        let config = PartitionLogConfig::default();
        let mut log =
            PartitionLog::open_or_create(&directory, topic, PartitionId::new(0), 0, config)
                .unwrap();
        log.append(&data_frame(1, 0, b"expired"), 0).unwrap();
        log.append(&data_frame(2, 1, b"visible"), 1).unwrap();
        log.flush_data().unwrap();
        log.mark_committed(2).unwrap();
        assert_eq!(log.committed_log_start_offset().unwrap().get(), 0);

        log.append(&p9d_log_start_frame(3, 1), 2).unwrap();
        log.flush_data().unwrap();
        log.mark_committed(3).unwrap();
        assert_eq!(log.committed_log_start_offset().unwrap().get(), 1);
        assert_eq!(log.list_committed_offset(-2).unwrap(), (1, -1));
        assert_eq!(log.list_committed_offset(-1).unwrap(), (2, -1));
        assert_eq!(log.list_committed_offset(1000).unwrap(), (1, 1001));
        let (records, _) = log
            .read_committed_records_from_bounded(StreamOffset::new(0), 10, usize::MAX)
            .unwrap();
        assert_eq!(records.len(), 1);
        assert_eq!(records[0].offset.get(), 1);

        drop(log);
        let reopened =
            PartitionLog::open_or_create(&directory, topic, PartitionId::new(0), 3, config)
                .unwrap();
        assert_eq!(reopened.committed_log_start_offset().unwrap().get(), 1);
        fs::remove_dir_all(directory).unwrap();

        let invalid = test_directory("p9d-log-start-invalid");
        let mut log =
            PartitionLog::open_or_create(&invalid, topic, PartitionId::new(0), 0, config).unwrap();
        log.append(&data_frame(1, 0, b"one"), 0).unwrap();
        log.flush_data().unwrap();
        log.mark_committed(1).unwrap();
        log.append(&p9d_log_start_frame(2, 2), 1).unwrap();
        log.flush_data().unwrap();
        log.mark_committed(2).unwrap();
        assert!(log.committed_log_start_offset().is_err());
        fs::remove_dir_all(invalid).unwrap();

        let regression = test_directory("p9d-log-start-regression");
        let mut log =
            PartitionLog::open_or_create(&regression, topic, PartitionId::new(0), 0, config)
                .unwrap();
        log.append(&data_frame(1, 0, b"one"), 0).unwrap();
        log.append(&data_frame(2, 1, b"two"), 0).unwrap();
        log.flush_data().unwrap();
        log.mark_committed(2).unwrap();
        log.append(&p9d_log_start_frame(3, 1), 1).unwrap();
        log.append(&p9d_log_start_frame(4, 0), 1).unwrap();
        log.flush_data().unwrap();
        log.mark_committed(4).unwrap();
        assert!(log.committed_log_start_offset().is_err());
        fs::remove_dir_all(regression).unwrap();
    }

    #[test]
    fn p9d_time_size_and_protected_planning_are_closed_segment_and_clock_safe() {
        let directory = test_directory("p9d-retention-plan");
        let topic = TopicId::new([0xd2; 16]);
        let config = PartitionLogConfig::default();
        let mut log =
            PartitionLog::open_or_create(&directory, topic, PartitionId::new(0), 0, config)
                .unwrap();
        log.append(&data_frame(1, 0, &[b'A'; 128]), 0).unwrap();
        log.flush_data().unwrap();
        log.roll(100).unwrap();
        log.append(&data_frame(2, 1, &[b'B'; 128]), 100).unwrap();
        log.flush_data().unwrap();
        log.roll(200).unwrap();
        log.append(&data_frame(3, 2, &[b'C'; 128]), 200).unwrap();
        log.flush_data().unwrap();
        log.mark_committed(3).unwrap();

        let backwards = log
            .plan_delete_retention(DeleteRetentionSpec {
                now_ms: 50,
                retention_ms: Some(50),
                retention_bytes: None,
                protected_log_start_ceiling: None,
                snapshot_floor_raft_index: 3,
            })
            .unwrap();
        assert_eq!(backwards.proposed_log_start_offset.get(), 0);

        let timed = log
            .plan_delete_retention(DeleteRetentionSpec {
                now_ms: 150,
                retention_ms: Some(50),
                retention_bytes: None,
                protected_log_start_ceiling: None,
                snapshot_floor_raft_index: 3,
            })
            .unwrap();
        assert_eq!(timed.proposed_log_start_offset.get(), 1);

        let protected = log
            .plan_delete_retention(DeleteRetentionSpec {
                now_ms: 1000,
                retention_ms: Some(1),
                retention_bytes: None,
                protected_log_start_ceiling: Some(StreamOffset::new(1)),
                snapshot_floor_raft_index: 3,
            })
            .unwrap();
        assert_eq!(protected.proposed_log_start_offset.get(), 1);

        let sized = log
            .plan_delete_retention(DeleteRetentionSpec {
                now_ms: 0,
                retention_ms: None,
                retention_bytes: Some(0),
                protected_log_start_ceiling: None,
                snapshot_floor_raft_index: 3,
            })
            .unwrap();
        assert_eq!(sized.proposed_log_start_offset.get(), 2);
        assert!(sized.retained_log_bytes_after_plan > 0);
        assert!(sized.retained_log_bytes_after_plan < sized.authoritative_log_bytes);

        let snapshot_blocked = log
            .plan_delete_retention(DeleteRetentionSpec {
                now_ms: 1000,
                retention_ms: Some(1),
                retention_bytes: Some(0),
                protected_log_start_ceiling: None,
                snapshot_floor_raft_index: 0,
            })
            .unwrap();
        assert_eq!(snapshot_blocked.proposed_log_start_offset.get(), 0);
        fs::remove_dir_all(directory).unwrap();

        let backwards_roll = test_directory("p9d-backwards-roll-clock");
        let mut log =
            PartitionLog::open_or_create(&backwards_roll, topic, PartitionId::new(0), 100, config)
                .unwrap();
        log.append(&data_frame(1, 0, b"old"), 100).unwrap();
        log.flush_data().unwrap();
        // Wall clock moves backwards before the next durable segment header.
        // That successor timestamp cannot be treated as an older close bound.
        log.roll(50).unwrap();
        log.append(&data_frame(2, 1, b"new"), 50).unwrap();
        log.flush_data().unwrap();
        log.mark_committed(2).unwrap();
        let delayed = log
            .plan_delete_retention(DeleteRetentionSpec {
                now_ms: 1_000,
                retention_ms: Some(10),
                retention_bytes: None,
                protected_log_start_ceiling: None,
                snapshot_floor_raft_index: 2,
            })
            .unwrap();
        assert_eq!(
            delayed.proposed_log_start_offset.get(),
            0,
            "backward roll time must delay retention instead of making the source appear older"
        );
        fs::remove_dir_all(backwards_roll).unwrap();
    }

    #[test]
    fn p9d_physical_delete_requires_committed_floor_and_preserves_floor_authority_restart() {
        let directory = test_directory("p9d-commit-before-delete");
        let topic = TopicId::new([0xd3; 16]);
        let config = PartitionLogConfig::default();
        let mut log =
            PartitionLog::open_or_create(&directory, topic, PartitionId::new(0), 0, config)
                .unwrap();
        for (raft, offset, created) in [(1_u64, 0_u64, 0_i64), (2, 1, 100), (3, 2, 200)] {
            if raft > 1 {
                log.roll(created).unwrap();
            }
            log.append(&data_frame(raft, offset, &[offset as u8; 64]), created)
                .unwrap();
            log.flush_data().unwrap();
        }
        log.mark_committed(3).unwrap();
        let plan = log
            .plan_delete_retention(DeleteRetentionSpec {
                now_ms: 1000,
                retention_ms: None,
                retention_bytes: Some(0),
                protected_log_start_ceiling: None,
                snapshot_floor_raft_index: 3,
            })
            .unwrap();
        assert_eq!(plan.proposed_log_start_offset.get(), 2);
        let paths_before = log.segment_paths().len();
        assert_eq!(
            log.publish_delete_retention_generation(plan.base_manifest_generation, 3)
                .unwrap(),
            0,
            "physical omission must not create retention semantics before the floor commits"
        );
        assert_eq!(log.segment_paths().len(), paths_before);

        log.append(&p9d_log_start_frame(4, 2), 1001).unwrap();
        log.flush_data().unwrap();
        log.mark_committed(4).unwrap();
        assert_eq!(log.committed_log_start_offset().unwrap().get(), 2);
        assert_eq!(
            log.publish_delete_retention_generation(plan.base_manifest_generation, 3)
                .unwrap(),
            2
        );
        assert_eq!(log.segment_paths().len(), 1);
        let (records, _) = log
            .read_committed_records_from_bounded(StreamOffset::new(0), 10, usize::MAX)
            .unwrap();
        assert_eq!(records.len(), 1);
        assert_eq!(records[0].offset.get(), 2);
        drop(log);

        let reopened =
            PartitionLog::open_or_create(&directory, topic, PartitionId::new(0), 1002, config)
                .unwrap();
        assert_eq!(reopened.committed_log_start_offset().unwrap().get(), 2);
        assert_eq!(reopened.list_committed_offset(-2).unwrap(), (2, -1));
        fs::remove_dir_all(directory).unwrap();
    }

    #[test]
    fn p9d_straddling_prefix_is_logically_hidden_without_physical_rewrite() {
        let directory = test_directory("p9d-straddling-prefix");
        let topic = TopicId::new([0xd4; 16]);
        let config = PartitionLogConfig::default();
        let mut log =
            PartitionLog::open_or_create(&directory, topic, PartitionId::new(0), 0, config)
                .unwrap();
        log.append(&three_record_frame(1), 0).unwrap();
        log.flush_data().unwrap();
        log.mark_committed(1).unwrap();
        log.adopt_manifest_inventory().unwrap();
        log.append(&p9d_log_start_frame(2, 1), 1).unwrap();
        log.flush_data().unwrap();
        log.mark_committed(2).unwrap();

        let path = log.segment_paths()[0].clone();
        let bytes_before = fs::read(&path).unwrap();
        assert_eq!(
            log.publish_delete_retention_generation(log.manifest_generation().unwrap(), 2)
                .unwrap(),
            0
        );
        assert_eq!(fs::read(&path).unwrap(), bytes_before);
        let (records, _) = log
            .read_committed_records_from_bounded(StreamOffset::new(0), 10, usize::MAX)
            .unwrap();
        assert_eq!(
            records
                .iter()
                .map(|record| record.offset.get())
                .collect::<Vec<_>>(),
            vec![1, 2]
        );
        assert_eq!(log.list_committed_offset(1000).unwrap(), (1, 1001));
        fs::remove_dir_all(directory).unwrap();
    }

    #[test]
    fn p9d_physical_cleanup_pins_latest_floor_authority_segment_until_newer_authority_exists() {
        let directory = test_directory("p9d-pin-floor-authority");
        let topic = TopicId::new([0xd5; 16]);
        let config = PartitionLogConfig::default();
        let mut log =
            PartitionLog::open_or_create(&directory, topic, PartitionId::new(0), 0, config)
                .unwrap();
        log.append(&data_frame(1, 0, b"old"), 0).unwrap();
        log.flush_data().unwrap();
        log.mark_committed(1).unwrap();
        log.roll(100).unwrap();
        log.append(&data_frame(2, 1, b"live"), 100).unwrap();
        log.flush_data().unwrap();
        log.mark_committed(2).unwrap();
        assert_eq!(log.adopt_manifest_inventory().unwrap(), 1);

        log.append(&p9d_log_start_frame(3, 1), 101).unwrap();
        log.flush_data().unwrap();
        log.mark_committed(3).unwrap();
        assert_eq!(log.committed_log_start_offset().unwrap().get(), 1);
        log.roll(200).unwrap();
        assert_eq!(log.manifest_generation(), Some(2));
        let before = log.segment_paths().to_vec();
        assert_eq!(before.len(), 3);

        assert_eq!(log.publish_delete_retention_generation(2, 3).unwrap(), 1);
        assert_eq!(log.segment_paths().len(), 2);
        assert!(
            log.segment_paths().contains(&before[1]),
            "the segment carrying the latest committed floor control must remain authoritative"
        );
        drop(log);

        let reopened =
            PartitionLog::open_or_create(&directory, topic, PartitionId::new(0), 201, config)
                .unwrap();
        assert_eq!(reopened.committed_log_start_offset().unwrap().get(), 1);
        assert_eq!(reopened.list_committed_offset(-2).unwrap(), (1, -1));
        fs::remove_dir_all(directory).unwrap();
    }
}
