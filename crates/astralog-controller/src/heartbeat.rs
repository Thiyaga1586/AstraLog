use std::collections::BTreeMap;

use astralog_common::TopicId;

use crate::{
    AssignmentGeneration, BrokerAuthority, BrokerRecord, ControllerError, ControllerNodeId,
    MetadataSyncResponse, MovementId, MovementStage, Result,
};

const MAX_FAILURE_DETECTOR_THRESHOLD_TICKS: u64 = 1_000_000_000;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct HeartbeatLimits {
    pub max_replica_progress: usize,
    pub max_movement_progress: usize,
}

impl Default for HeartbeatLimits {
    fn default() -> Self {
        Self {
            max_replica_progress: 4096,
            max_movement_progress: 1024,
        }
    }
}

impl HeartbeatLimits {
    pub fn validate(self) -> Result<()> {
        if self.max_replica_progress == 0 || self.max_movement_progress == 0 {
            return Err(ControllerError::Invalid(
                "heartbeat progress limits must be non-zero",
            ));
        }
        Ok(())
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub struct ReplicaProgressReport {
    pub topic_id: TopicId,
    pub partition_id: u32,
    pub assignment_generation: AssignmentGeneration,
    pub durable_raft_index: u64,
    pub committed_raft_index: u64,
}

impl ReplicaProgressReport {
    fn validate(self) -> Result<()> {
        if self.topic_id.as_bytes() == &[0; 16] {
            return Err(ControllerError::Invalid(
                "heartbeat replica progress topic id zero is reserved",
            ));
        }
        if self.committed_raft_index > self.durable_raft_index {
            return Err(ControllerError::Invalid(
                "heartbeat replica committed index exceeds durable index",
            ));
        }
        Ok(())
    }

    fn key(self) -> (TopicId, u32) {
        (self.topic_id, self.partition_id)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct MovementProgressReport {
    pub movement_id: MovementId,
    pub topic_id: TopicId,
    pub partition_id: u32,
    pub assignment_generation: AssignmentGeneration,
    pub observed_stage: MovementStage,
}

impl MovementProgressReport {
    fn validate(self) -> Result<()> {
        if self.topic_id.as_bytes() == &[0; 16] {
            return Err(ControllerError::Invalid(
                "heartbeat movement progress topic id zero is reserved",
            ));
        }
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BrokerHeartbeatRequest {
    pub authority: BrokerAuthority,
    pub applied_metadata_epoch: u64,
    pub min_feature: u16,
    pub max_feature: u16,
    pub replica_progress: Vec<ReplicaProgressReport>,
    pub movement_progress: Vec<MovementProgressReport>,
}

impl BrokerHeartbeatRequest {
    pub fn validate_shape(&self, limits: HeartbeatLimits) -> Result<()> {
        limits.validate()?;
        if self.authority.broker_id == 0 || self.authority.broker_id == u32::MAX {
            return Err(ControllerError::Invalid("heartbeat broker id is reserved"));
        }
        if self.authority.node_uuid == [0; 16] {
            return Err(ControllerError::Invalid(
                "heartbeat node UUID zero is reserved",
            ));
        }
        if self.authority.incarnation_epoch == 0 {
            return Err(ControllerError::Invalid(
                "heartbeat incarnation zero is reserved",
            ));
        }
        if self.min_feature == 0 || self.min_feature > self.max_feature {
            return Err(ControllerError::Invalid(
                "heartbeat feature range is invalid",
            ));
        }
        if self.replica_progress.len() > limits.max_replica_progress {
            return Err(ControllerError::LimitExceeded {
                what: "heartbeat replica progress count",
                actual: self.replica_progress.len(),
                max: limits.max_replica_progress,
            });
        }
        if self.movement_progress.len() > limits.max_movement_progress {
            return Err(ControllerError::LimitExceeded {
                what: "heartbeat movement progress count",
                actual: self.movement_progress.len(),
                max: limits.max_movement_progress,
            });
        }
        for report in &self.replica_progress {
            report.validate()?;
        }
        if !self
            .replica_progress
            .windows(2)
            .all(|window| window[0].key() < window[1].key())
        {
            return Err(ControllerError::Invalid(
                "heartbeat replica progress must be strictly ordered and distinct",
            ));
        }
        for report in &self.movement_progress {
            report.validate()?;
        }
        if !self
            .movement_progress
            .windows(2)
            .all(|window| window[0].movement_id < window[1].movement_id)
        {
            return Err(ControllerError::Invalid(
                "heartbeat movement progress must be strictly ordered and distinct",
            ));
        }
        Ok(())
    }

    pub fn validate_against_committed(
        &self,
        broker: &BrokerRecord,
        controller_metadata_epoch: u64,
    ) -> Result<()> {
        if self.applied_metadata_epoch > controller_metadata_epoch {
            return Err(ControllerError::Invalid(
                "broker heartbeat metadata epoch is ahead of controller authority",
            ));
        }
        if self.min_feature != broker.min_feature || self.max_feature != broker.max_feature {
            return Err(ControllerError::Invalid(
                "broker heartbeat feature range differs from committed BrokerRecord",
            ));
        }
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BrokerHeartbeatResponse {
    pub controller_id: ControllerNodeId,
    pub controller_term: u64,
    pub controller_metadata_epoch: u64,
    pub observed_at_tick: u64,
    pub metadata_sync: MetadataSyncResponse,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct FailureDetectorConfig {
    pub suspect_after_ticks: u64,
    pub unavailable_after_ticks: u64,
}

impl Default for FailureDetectorConfig {
    fn default() -> Self {
        Self {
            suspect_after_ticks: 3,
            unavailable_after_ticks: 10,
        }
    }
}

impl FailureDetectorConfig {
    pub fn validate(self) -> Result<()> {
        if self.suspect_after_ticks == 0
            || self.unavailable_after_ticks <= self.suspect_after_ticks
            || self.unavailable_after_ticks > MAX_FAILURE_DETECTOR_THRESHOLD_TICKS
        {
            return Err(ControllerError::Invalid(
                "failure-detector thresholds must satisfy 0 < suspect < unavailable <= limit",
            ));
        }
        Ok(())
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BrokerLivenessState {
    Unknown,
    Healthy,
    Suspect,
    Unavailable,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct BrokerLivenessObservation {
    pub authority: BrokerAuthority,
    pub state: BrokerLivenessState,
    pub last_seen_tick: Option<u64>,
    pub elapsed_ticks: Option<u64>,
}

impl BrokerLivenessObservation {
    pub const fn available_for_new_placement(self) -> bool {
        matches!(self.state, BrokerLivenessState::Healthy)
    }
}

#[derive(Debug, Clone, Copy)]
struct LastSeen {
    authority: BrokerAuthority,
    tick: u64,
}

#[derive(Debug, Clone)]
pub(crate) struct FailureDetector {
    config: FailureDetectorConfig,
    current_tick: Option<u64>,
    last_seen: BTreeMap<u32, LastSeen>,
}

impl FailureDetector {
    pub(crate) fn new(config: FailureDetectorConfig) -> Result<Self> {
        config.validate()?;
        Ok(Self {
            config,
            current_tick: None,
            last_seen: BTreeMap::new(),
        })
    }

    pub(crate) fn reset_for_leadership_change(&mut self) {
        self.current_tick = None;
        self.last_seen.clear();
    }

    pub(crate) fn inject_tick(&mut self, tick: u64) -> Result<()> {
        if self.current_tick.is_some_and(|current| tick < current) {
            return Err(ControllerError::Invalid(
                "failure-detector tick moved backwards",
            ));
        }
        self.current_tick = Some(tick);
        Ok(())
    }

    pub(crate) fn current_tick(&self) -> Result<u64> {
        self.current_tick.ok_or(ControllerError::Invalid(
            "failure-detector tick is not initialized for current controller leadership",
        ))
    }

    pub(crate) fn observe_heartbeat(&mut self, authority: BrokerAuthority) -> Result<u64> {
        let tick = self.current_tick()?;
        self.last_seen
            .insert(authority.broker_id, LastSeen { authority, tick });
        Ok(tick)
    }

    pub(crate) fn observation(&self, broker: &BrokerRecord) -> BrokerLivenessObservation {
        let authority = BrokerAuthority {
            broker_id: broker.broker_id,
            node_uuid: broker.node_uuid,
            incarnation_epoch: broker.incarnation_epoch,
        };
        let Some(now) = self.current_tick else {
            return BrokerLivenessObservation {
                authority,
                state: BrokerLivenessState::Unknown,
                last_seen_tick: None,
                elapsed_ticks: None,
            };
        };
        let Some(last_seen) = self.last_seen.get(&broker.broker_id) else {
            return BrokerLivenessObservation {
                authority,
                state: BrokerLivenessState::Unknown,
                last_seen_tick: None,
                elapsed_ticks: None,
            };
        };
        if last_seen.authority != authority {
            return BrokerLivenessObservation {
                authority,
                state: BrokerLivenessState::Unknown,
                last_seen_tick: None,
                elapsed_ticks: None,
            };
        }
        let elapsed = now.saturating_sub(last_seen.tick);
        let state = if elapsed >= self.config.unavailable_after_ticks {
            BrokerLivenessState::Unavailable
        } else if elapsed >= self.config.suspect_after_ticks {
            BrokerLivenessState::Suspect
        } else {
            BrokerLivenessState::Healthy
        };
        BrokerLivenessObservation {
            authority,
            state,
            last_seen_tick: Some(last_seen.tick),
            elapsed_ticks: Some(elapsed),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::BrokerLifecycle;

    fn broker() -> BrokerRecord {
        BrokerRecord {
            broker_id: 7,
            node_uuid: [7; 16],
            incarnation_epoch: 2,
            lifecycle: BrokerLifecycle::Active,
            client_host: "broker-7".into(),
            client_port: 9092,
            internal_host: "broker-7-internal".into(),
            internal_port: 9093,
            rack: None,
            zone: None,
            min_feature: 1,
            max_feature: 3,
        }
    }

    fn authority() -> BrokerAuthority {
        BrokerAuthority {
            broker_id: 7,
            node_uuid: [7; 16],
            incarnation_epoch: 2,
        }
    }

    #[test]
    fn heartbeat_shape_is_bounded_and_canonical() {
        let topic = TopicId::new([1; 16]);
        let movement = MovementId::new([1; 16]).unwrap();
        let mut request = BrokerHeartbeatRequest {
            authority: authority(),
            applied_metadata_epoch: 10,
            min_feature: 1,
            max_feature: 3,
            replica_progress: vec![ReplicaProgressReport {
                topic_id: topic,
                partition_id: 0,
                assignment_generation: AssignmentGeneration::new(1).unwrap(),
                durable_raft_index: 9,
                committed_raft_index: 8,
            }],
            movement_progress: vec![MovementProgressReport {
                movement_id: movement,
                topic_id: topic,
                partition_id: 0,
                assignment_generation: AssignmentGeneration::new(1).unwrap(),
                observed_stage: MovementStage::CatchingUp,
            }],
        };
        assert!(request.validate_shape(HeartbeatLimits::default()).is_ok());
        request.replica_progress.push(request.replica_progress[0]);
        assert!(request.validate_shape(HeartbeatLimits::default()).is_err());
    }

    #[test]
    fn heartbeat_feature_and_metadata_claims_must_match_committed_authority() {
        let mut request = BrokerHeartbeatRequest {
            authority: authority(),
            applied_metadata_epoch: 10,
            min_feature: 1,
            max_feature: 3,
            replica_progress: Vec::new(),
            movement_progress: Vec::new(),
        };
        assert!(request.validate_against_committed(&broker(), 10).is_ok());
        request.applied_metadata_epoch = 11;
        assert!(request.validate_against_committed(&broker(), 10).is_err());
        request.applied_metadata_epoch = 10;
        request.max_feature = 4;
        assert!(request.validate_against_committed(&broker(), 10).is_err());
    }

    #[test]
    fn detector_ticks_are_monotonic_and_thresholds_are_deterministic() {
        let mut detector = FailureDetector::new(FailureDetectorConfig::default()).unwrap();
        detector.inject_tick(100).unwrap();
        detector.observe_heartbeat(authority()).unwrap();
        assert_eq!(
            detector.observation(&broker()).state,
            BrokerLivenessState::Healthy
        );
        detector.inject_tick(103).unwrap();
        assert_eq!(
            detector.observation(&broker()).state,
            BrokerLivenessState::Suspect
        );
        detector.inject_tick(110).unwrap();
        let unavailable = detector.observation(&broker());
        assert_eq!(unavailable.state, BrokerLivenessState::Unavailable);
        assert!(!unavailable.available_for_new_placement());
        assert!(detector.inject_tick(109).is_err());
    }

    #[test]
    fn leadership_reset_discards_clock_and_observations() {
        let mut detector = FailureDetector::new(FailureDetectorConfig::default()).unwrap();
        detector.inject_tick(500).unwrap();
        detector.observe_heartbeat(authority()).unwrap();
        detector.reset_for_leadership_change();
        assert_eq!(
            detector.observation(&broker()).state,
            BrokerLivenessState::Unknown
        );
        assert!(detector.current_tick().is_err());
        detector.inject_tick(1).unwrap();
        assert_eq!(
            detector.observation(&broker()).state,
            BrokerLivenessState::Unknown
        );
    }

    #[test]
    fn changed_incarnation_never_inherits_old_liveness() {
        let mut detector = FailureDetector::new(FailureDetectorConfig::default()).unwrap();
        detector.inject_tick(10).unwrap();
        detector.observe_heartbeat(authority()).unwrap();
        let mut rejoined = broker();
        rejoined.incarnation_epoch = 3;
        assert_eq!(
            detector.observation(&rejoined).state,
            BrokerLivenessState::Unknown
        );
    }
}
