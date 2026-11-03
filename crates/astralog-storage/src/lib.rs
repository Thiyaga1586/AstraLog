#![forbid(unsafe_code)]

mod clean_progress;
mod compression;
mod error;
mod format;
mod format_v2;
mod index;
mod io_util;
mod limits;
mod maintenance;
mod manifest;
mod partition;
mod segment;
#[cfg(test)]
mod test_support;

pub use clean_progress::{LocalCleanProgress, LocalCleanProgressStore};
pub use error::{Result, StorageError};
pub use format::{
    Compression, ControlEntry, DataRecord, FRAME_CRC_LEN, FRAME_HEADER_LEN, Frame, FrameType,
    PartitionMeta, ProducerIdentity, RecordBatch, SegmentHeader, UserHeader, decode_frame,
    decode_partition_meta, decode_segment_header, encode_frame, encode_partition_meta,
    encode_segment_header,
};
pub use format_v2::{
    FORMAT_VERSION_V2, FRAME_CRC_LEN_V2, FRAME_HEADER_LEN_V2, MIN_FRAME_LEN_V2,
    RETAINED_HISTORY_FLAG, RetainedControlFrameV2, RetainedControlKindV2, RetainedFrameV2,
    RetainedScannedFrameV2, RetainedSegmentHeaderV2, RetainedSegmentScanV2, SEGMENT_HEADER_LEN_V2,
    SparseRetainedBatchV2, SparseRetainedRecordV2, StorageFormatVersion, decode_retained_frame_v2,
    decode_retained_segment_header_v2, encode_retained_frame_v2, encode_retained_segment_header_v2,
    read_retained_frame_at_v2, read_retained_segment_header_v2, read_segment_format,
    scan_retained_segment_v2, segment_format_version,
};
pub use index::{
    IndexEntry, IndexKind, rebuild_retained_segment_indexes_v2, rebuild_segment_indexes,
    validate_index_file,
};
pub use limits::StorageLimits;
pub use maintenance::{
    P9_CONTROL_ADVANCE_COMPACTION_SAFE_OFFSET, P9_CONTROL_ADVANCE_LOG_START_OFFSET,
    P9_CONTROL_PRODUCER_STATE_CHECKPOINT, P9_MAINTENANCE_CONTROL_VERSION,
    decode_compaction_safe_offset_body, decode_log_start_offset_body,
    encode_compaction_safe_offset_body, encode_log_start_offset_body,
};
pub use manifest::{
    GenerationTransaction, ManifestEntry, SegmentManifest, decode_manifest, encode_manifest,
    load_manifest, validate_manifest_files, validate_partition_manifest_files,
};
pub use partition::{
    CompactionLimits, CompactionOutcome, CompactionPlan, CompactionPlanSpec, DeleteRetentionPlan,
    DeleteRetentionSpec, PartitionLog, PartitionLogConfig, PartitionReadGeneration,
    ProducerCheckpointFence, StoredRecord, TombstoneRemovalGate,
};
pub use segment::{
    ScanFailure, ScanFailureKind, ScannedFrame, SegmentFileId, SegmentScan, SegmentWriter,
    read_frame_at, read_scanned_frame_at, read_segment_header, scan_segment,
};
