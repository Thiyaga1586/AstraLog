use astralog_common::TopicId;

use crate::{AssignmentGeneration, MovementId};

pub const MAX_TOPIC_PARTITIONS: u32 = astralog_protocol::api::MAX_PARTITIONS_PER_REQUEST as u32;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum BrokerLifecycle {
    Joining = 1,
    Active = 2,
    Draining = 3,
    Fenced = 4,
    Removed = 5,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum TopicLifecycle {
    Active = 1,
    Deleting = 2,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum FailureDomainPolicy {
    Disabled = 0,
    Preferred = 1,
    Required = 2,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum MovementStage {
    Planned = 1,
    LearnerRequested = 2,
    LearnerCreated = 3,
    CatchingUp = 4,
    PromotionReady = 5,
    EnterJointRequested = 6,
    JointCommitted = 7,
    LeaveJointRequested = 8,
    NewStableCommitted = 9,
    LeadershipTransfer = 10,
    OldReplicaRemoveRequested = 11,
    OldReplicaRemoved = 12,
    Complete = 13,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BrokerRecord {
    pub broker_id: u32,
    pub node_uuid: [u8; 16],
    pub incarnation_epoch: u64,
    pub lifecycle: BrokerLifecycle,
    pub client_host: String,
    pub client_port: u16,
    pub internal_host: String,
    pub internal_port: u16,
    pub rack: Option<String>,
    pub zone: Option<String>,
    pub min_feature: u16,
    pub max_feature: u16,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TopicRecord {
    pub topic_id: TopicId,
    pub topic_name: String,
    pub partition_count: u32,
    pub replication_factor: u16,
    pub lifecycle: TopicLifecycle,
    pub failure_domain_policy: FailureDomainPolicy,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AssignmentRecord {
    pub topic_id: TopicId,
    pub partition_id: u32,
    pub assignment_generation: AssignmentGeneration,
    pub voters: Vec<u32>,
    pub learners: Vec<u32>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MovementRecord {
    pub movement_id: MovementId,
    pub topic_id: TopicId,
    pub partition_id: u32,
    pub expected_assignment_generation: AssignmentGeneration,
    pub source_broker_id: u32,
    pub destination_broker_id: u32,
    pub stage: MovementStage,
    pub target_voters: Vec<u32>,
    pub preferred_leader: Option<u32>,
}
