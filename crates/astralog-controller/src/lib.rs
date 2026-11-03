#![forbid(unsafe_code)]

mod bootstrap;
mod command;
mod error;
mod feature;
mod heartbeat;
mod membership;
mod metadata;
mod model;
mod movement;
mod peer_transport;
mod performance;
mod performance_codec;
mod persistence;
mod placement;
mod producer_codec;
mod reassignment;
mod retention;
mod retention_codec;
mod runtime;
mod snapshot;
mod snapshot_v2;
mod snapshot_v3;
mod snapshot_v4;
mod state;
mod topic;
mod transport;
mod types;

#[cfg(test)]
mod failure_qualification;

pub use bootstrap::{
    BootstrapInstallOutcome, ControllerBootstrapManifest, initialize_controller_bootstrap,
    load_controller_bootstrap, validate_controller_bootstrap,
};
pub use command::{
    CommandLimits, ControllerCommand, MAX_PRODUCER_IDS_PER_ALLOCATION, decode_command,
    encode_command,
};
pub use error::{ControllerError, Result};
pub use feature::{FeatureGate, FeatureRange};
pub use heartbeat::{
    BrokerHeartbeatRequest, BrokerHeartbeatResponse, BrokerLivenessObservation,
    BrokerLivenessState, FailureDetectorConfig, HeartbeatLimits, MovementProgressReport,
    ReplicaProgressReport,
};
pub use membership::{
    BrokerAuthority, BrokerLifecycleChange, BrokerMembershipMutation, BrokerRegistration,
    BrokerRejoin,
};
pub use metadata::{
    ControllerMetadataSnapshot, MAX_METADATA_DELTA_RESPONSE_BYTES,
    MAX_METADATA_DELTAS_PER_RESPONSE, MAX_RETAINED_METADATA_BYTES, MAX_RETAINED_METADATA_DELTAS,
    MetadataDelta, MetadataSyncResponse,
};
pub use model::{
    AssignmentRecord, BrokerLifecycle, BrokerRecord, FailureDomainPolicy, MAX_TOPIC_PARTITIONS,
    MovementRecord, MovementStage, TopicLifecycle, TopicRecord,
};
pub use movement::{MovementProgressObservation, ReplicaProgressObservation};
pub use peer_transport::{
    ControllerPeerDescriptor, ControllerPeerTransportConfig, ControllerPeerTransportHandle,
    ControllerPeerTransportSender, start_controller_peer_transport,
};
pub use performance::{
    CompressionPolicyV1, DEFAULT_TOPIC_BATCH_UNCOMPRESSED_BYTES, MAX_TOPIC_BATCH_RECORDS,
    MAX_TOPIC_RECORD_BYTES, P8_PERFORMANCE_FEATURE_LEVEL, TopicPerformancePolicyV1,
    TopicPerformanceSpecV1,
};
pub use performance_codec::{
    TopicPerformanceCommandV3, decode_topic_performance_command_v3,
    encode_topic_performance_command_v3,
};
pub use persistence::{
    ControllerPersistence, ControllerPersistenceConfig, ControllerSnapshotPackage,
    decode_controller_snapshot_package, encode_controller_snapshot_package,
};
pub use placement::{
    PlacementBrokerSnapshot, PlacementInputSnapshot, PlacementPlan, plan_assignments,
};
pub use producer_codec::{
    ControllerLogCommand, ProducerSessionCommandV2, decode_controller_log_command,
    decode_producer_session_command_v2, encode_controller_log_command,
    encode_producer_session_command_v2,
};
pub use reassignment::{ReassignmentResult, ReassignmentSpec};
pub use retention::{
    CleanupPolicyV1, DEFAULT_TOMBSTONE_GRACE_MS, P9_RETENTION_FEATURE_LEVEL,
    SubscriptionProtectionV1, TopicRetentionPolicyV1, TopicRetentionSpecV1,
};
pub use retention_codec::{
    TopicRetentionCommandV4, decode_topic_retention_command_v4, encode_topic_retention_command_v4,
};
pub use runtime::{
    ControllerClient, ControllerNodeConfig, ControllerNodeHandle, ControllerRuntime,
    ControllerRuntimeConfig, ControllerRuntimeLimits, ControllerStatus, MutationOutcome,
    MutationReceipt, start_controller_node,
};
pub use snapshot::{
    ControllerSnapshot, MAX_CONTROLLER_ASSIGNMENTS, MAX_CONTROLLER_BROKERS,
    MAX_CONTROLLER_MOVEMENTS, MAX_CONTROLLER_SNAPSHOT_BYTES, MAX_CONTROLLER_TOPICS, SnapshotLimits,
    decode_controller_snapshot, encode_controller_snapshot,
};
pub use snapshot_v2::{
    ControllerSnapshotV2, DecodedControllerSnapshot, MAX_PRODUCER_SESSIONS,
    ProducerSessionRecordV1, decode_controller_snapshot_any, decode_controller_snapshot_v2,
    decode_controller_state_snapshot, encode_controller_snapshot_v2,
    encode_controller_state_snapshot,
};
pub use snapshot_v3::{
    ControllerSnapshotV3, decode_controller_snapshot_v3, encode_controller_snapshot_v3,
};
pub use snapshot_v4::{
    ControllerSnapshotV4, decode_controller_snapshot_v4, encode_controller_snapshot_v4,
};
pub use state::{ApplyOutcome, ControllerState, P7_PRODUCER_FEATURE_LEVEL, ProducerIdRange};
pub use topic::{
    CreateTopicSpec, PartitioningEpochChange, TopicCreateResult, TopicDeletionResult,
    TopicExpansionResult, TopicExpansionSpec,
};
pub use transport::{
    ControllerFrame, ControllerFrameDecoder, ControllerTransportLimits, decode_controller_frame,
    encode_controller_frame,
};
pub use types::{AssignmentGeneration, ControllerNodeId, MovementId};
