#![forbid(unsafe_code)]

mod catalog;
mod controller;
mod error;
mod group_coordinator;
mod load_shaping;
mod metadata_cache;
mod offset_coordinator;
mod p6_service;
mod replication;
mod server;
mod state;

pub use controller::{
    DynamicControllerConfig, GroupCoordinatorObservation, P6InternalTopics,
    P7InternalPartitionObservation, P7InternalProduceError, P7InternalProduceOutcome,
    PreferredLeaderDisposition, PreferredLeaderOutcome, PreferredLeaderRequest,
};
pub use error::{BrokerError, Result};
pub use group_coordinator::{
    GroupCommitOutcome, GroupCoordinatorError, GroupCoordinatorRuntimeConfig,
};
pub use metadata_cache::BrokerMetadataCache;
pub use offset_coordinator::{
    OffsetCommitOutcome, OffsetCoordinatorError, OffsetCoordinatorRuntimeConfig,
    OffsetFenceCommitOutcome, OffsetFetchOutcome, OffsetShardObservation,
};
pub use replication::{
    CompactionRunResult, DynamicReplicationConfig, RetentionAdvanceResult, StaticBrokerConfig,
    StaticReplicationConfig, StaticTopicConfig,
};
pub use server::{BrokerConfig, BrokerHandle, start};
