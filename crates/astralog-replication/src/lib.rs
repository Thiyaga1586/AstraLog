#![forbid(unsafe_code)]

mod codec;
mod command;
mod compaction_progress;
mod error;
mod peer_transport;
mod producer_digest;
mod producer_snapshot;
mod producer_state;
mod rebootstrap;
mod rebootstrap_v2;
mod runtime;
mod sidecar;
#[cfg(test)]
mod simulation;
mod storage_adapter;
mod transport;

pub use codec::{decode_raft_message, encode_raft_message};
pub use command::{
    ApplicationCommand, ApplicationCommandV2, ApplicationCompactionSafeCommandV1,
    ApplicationMaintenanceCommandV1, ApplicationProducerStateCheckpointCommandV1, CommandLimits,
    decode_application_command, decode_application_command_v2,
    decode_application_compaction_safe_command_v1, decode_application_maintenance_command_v1,
    decode_application_producer_state_checkpoint_command_v1, encode_application_command,
    encode_application_command_v2, encode_application_compaction_safe_command_v1,
    encode_application_maintenance_command_v1,
    encode_application_producer_state_checkpoint_command_v1,
};
pub use compaction_progress::{
    CLEAN_PROGRESS_REPORT_BYTES, CleanProgressReport, CleanProgressTracker, VoterIdentity,
    requires_retained_history_rebootstrap,
};
pub use error::{ReplicationError, Result};
pub use peer_transport::{
    GroupKey, PeerDescriptor, PeerInboundFrame, PeerTransportConfig, PeerTransportHandle,
    PeerTransportSender, start_peer_transport,
};
pub use producer_snapshot::{
    MAX_PRODUCER_SNAPSHOT_BYTES, ProducerSnapshotStore, ProducerSnapshotV1,
    decode_producer_snapshot, encode_producer_snapshot,
};
pub use producer_state::{
    AppendCandidateV1, CommitApplyOutcomeV1, CommittedBatchV1, CommittedRetryV1,
    MAX_PARTITION_PRODUCER_ENTRIES, MAX_RECENT_COMMITTED_BATCHES, PartitionProducerStateV1,
    ProducerAdmissionV1, ProducerBatchIdentityV1, ProducerStateError, RecentBatchV1,
};
pub use rebootstrap::{
    RebootstrapLimits, RebootstrapPackage, decode_rebootstrap_package, encode_rebootstrap_package,
};
pub use rebootstrap_v2::{
    RebootstrapFileV2, RebootstrapLimitsV2, RebootstrapPackageV2, decode_rebootstrap_package_v2,
    encode_rebootstrap_package_v2,
};
pub use runtime::{
    AckRequirement, ControllerAuthorityEvidence, IdempotentProduceError,
    IdempotentProposalDisposition, MAX_PRODUCER_CACHE_ENTRIES, MaintenanceProposalReceipt,
    MembershipObservation, OutboundMessage, PersistenceGroupMetrics, ProposalOutcome,
    ProposalReceipt, ReplicaLimits, ReplicatedPartition,
};
pub use sidecar::{HardStateStore, SnapshotStore};
pub use storage_adapter::{
    P4_CONTROL_CONFIGURATION, P4_CONTROL_LEADER_BARRIER, PartitionPersistence,
};
pub use transport::{
    InternalFrame, InternalFrameDecoder, TransportLimits, decode_internal_frame,
    encode_internal_frame,
};
