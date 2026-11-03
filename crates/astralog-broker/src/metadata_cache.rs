use astralog_controller::{
    ControllerMetadataSnapshot, ControllerState, FeatureGate, MetadataDelta, MetadataSyncResponse,
};

use crate::{BrokerError, Result};

#[derive(Debug, Clone, Default)]
pub struct BrokerMetadataCache {
    state: Option<ControllerState>,
}

impl BrokerMetadataCache {
    pub const fn new() -> Self {
        Self { state: None }
    }

    pub fn from_snapshot(snapshot: ControllerMetadataSnapshot) -> Result<Self> {
        let mut cache = Self::new();
        cache.install_snapshot(snapshot)?;
        Ok(cache)
    }

    pub fn applied_metadata_epoch(&self) -> u64 {
        self.state
            .as_ref()
            .map_or(0, ControllerState::metadata_epoch)
    }

    pub fn state(&self) -> Option<&ControllerState> {
        self.state.as_ref()
    }

    pub fn require_feature(&self, gate: FeatureGate) -> Result<()> {
        self.state
            .as_ref()
            .ok_or(BrokerError::Metadata(
                "broker metadata cache is uninitialized for feature gating",
            ))?
            .require_feature(gate)?;
        Ok(())
    }

    pub fn apply_sync(&mut self, response: &MetadataSyncResponse) -> Result<()> {
        let mut staged = self.clone();
        staged.apply_sync_inner(response)?;
        *self = staged;
        Ok(())
    }

    pub fn install_snapshot(&mut self, snapshot: ControllerMetadataSnapshot) -> Result<()> {
        let mut staged = self.clone();
        staged.install_snapshot_inner(snapshot)?;
        *self = staged;
        Ok(())
    }

    fn apply_sync_inner(&mut self, response: &MetadataSyncResponse) -> Result<()> {
        match response {
            MetadataSyncResponse::UpToDate { controller_epoch } => {
                if *controller_epoch != self.applied_metadata_epoch() {
                    return Err(BrokerError::Metadata(
                        "controller marked metadata up-to-date at a different broker epoch",
                    ));
                }
                Ok(())
            }
            MetadataSyncResponse::Deltas {
                controller_epoch,
                deltas,
            } => {
                if deltas.is_empty() {
                    return Err(BrokerError::Metadata(
                        "metadata delta response cannot be empty",
                    ));
                }
                if self.state.is_none() {
                    return Err(BrokerError::Metadata(
                        "uninitialized broker metadata cache requires a snapshot",
                    ));
                }
                for delta in deltas {
                    self.apply_delta(delta)?;
                }
                if self.applied_metadata_epoch() > *controller_epoch {
                    return Err(BrokerError::Metadata(
                        "metadata delta batch advanced beyond controller epoch",
                    ));
                }
                Ok(())
            }
            MetadataSyncResponse::Snapshot {
                controller_epoch,
                snapshot,
            } => {
                if snapshot.epoch != *controller_epoch {
                    return Err(BrokerError::Metadata(
                        "metadata snapshot response epoch mismatch",
                    ));
                }
                self.install_snapshot_inner(snapshot.as_ref().clone())
            }
        }
    }

    fn apply_delta(&mut self, delta: &MetadataDelta) -> Result<()> {
        let state = self.state.as_mut().ok_or(BrokerError::Metadata(
            "uninitialized broker metadata cache requires a snapshot",
        ))?;
        let local = state.metadata_epoch();
        if delta.epoch < local {
            return Err(BrokerError::Metadata("stale metadata delta rejected"));
        }
        if delta.epoch == local {
            return Ok(());
        }
        let expected = local
            .checked_add(1)
            .ok_or(BrokerError::Metadata("broker metadata epoch overflow"))?;
        if delta.epoch != expected {
            return Err(BrokerError::Metadata(
                "out-of-order metadata delta requires snapshot fallback",
            ));
        }
        state.apply(delta.epoch, &delta.command)?;
        Ok(())
    }

    fn install_snapshot_inner(&mut self, snapshot: ControllerMetadataSnapshot) -> Result<()> {
        snapshot.validate()?;
        if let Some(current) = &self.state {
            let local = current.metadata_epoch();
            if snapshot.epoch < local {
                return Err(BrokerError::Metadata("stale metadata snapshot rejected"));
            }
            if snapshot.state.cluster_id() != current.cluster_id() {
                return Err(BrokerError::Metadata(
                    "foreign-cluster metadata snapshot rejected",
                ));
            }
            if snapshot.epoch == local {
                if snapshot.state != *current {
                    return Err(BrokerError::Metadata(
                        "same-epoch metadata snapshot differs from local cache",
                    ));
                }
                return Ok(());
            }
        }
        self.state = Some(snapshot.state);
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use astralog_controller::{
        ControllerCommand, ControllerMetadataSnapshot, FeatureGate, MetadataDelta,
    };

    use super::*;

    fn snapshot_at(epoch: u64, cluster: u8) -> ControllerMetadataSnapshot {
        let mut state = ControllerState::new([cluster; 16], 1, 1).unwrap();
        for index in 1..=epoch {
            state
                .apply(
                    index,
                    &ControllerCommand::AllocateProducerIds {
                        allocation_count: 1,
                    },
                )
                .unwrap();
        }
        ControllerMetadataSnapshot::from_state(&state)
    }

    fn delta(epoch: u64) -> MetadataDelta {
        MetadataDelta {
            epoch,
            command: ControllerCommand::AllocateProducerIds {
                allocation_count: 1,
            },
        }
    }

    #[test]
    fn snapshot_bootstrap_then_contiguous_delta_advances_monotonically() {
        let mut cache = BrokerMetadataCache::from_snapshot(snapshot_at(1, 7)).unwrap();
        cache
            .apply_sync(&MetadataSyncResponse::Deltas {
                controller_epoch: 2,
                deltas: vec![delta(2)],
            })
            .unwrap();
        assert_eq!(cache.applied_metadata_epoch(), 2);
    }

    #[test]
    fn stale_and_gap_deltas_are_rejected_without_mutation() {
        let mut cache = BrokerMetadataCache::from_snapshot(snapshot_at(2, 7)).unwrap();
        let before = cache.state().unwrap().clone();
        assert!(
            cache
                .apply_sync(&MetadataSyncResponse::Deltas {
                    controller_epoch: 3,
                    deltas: vec![delta(1)],
                })
                .is_err()
        );
        assert_eq!(cache.state().unwrap(), &before);
        assert!(
            cache
                .apply_sync(&MetadataSyncResponse::Deltas {
                    controller_epoch: 4,
                    deltas: vec![delta(4)],
                })
                .is_err()
        );
        assert_eq!(cache.state().unwrap(), &before);
    }

    #[test]
    fn multi_delta_application_is_atomic_on_late_failure() {
        let mut cache = BrokerMetadataCache::from_snapshot(snapshot_at(1, 7)).unwrap();
        let before = cache.state().unwrap().clone();
        assert!(
            cache
                .apply_sync(&MetadataSyncResponse::Deltas {
                    controller_epoch: 4,
                    deltas: vec![delta(2), delta(4)],
                })
                .is_err()
        );
        assert_eq!(cache.state().unwrap(), &before);
        assert_eq!(cache.applied_metadata_epoch(), 1);
    }

    #[test]
    fn snapshot_install_is_non_regressing_and_cluster_fenced() {
        let mut cache = BrokerMetadataCache::from_snapshot(snapshot_at(2, 7)).unwrap();
        assert!(cache.install_snapshot(snapshot_at(1, 7)).is_err());
        assert!(cache.install_snapshot(snapshot_at(3, 8)).is_err());
        assert_eq!(cache.applied_metadata_epoch(), 2);
        cache.install_snapshot(snapshot_at(3, 7)).unwrap();
        assert_eq!(cache.applied_metadata_epoch(), 3);
    }

    #[test]
    fn irreversible_feature_gate_tracks_only_committed_metadata_level() {
        let mut cache = BrokerMetadataCache::from_snapshot(snapshot_at(1, 7)).unwrap();
        let gate = FeatureGate::new(2).unwrap();
        assert!(cache.require_feature(gate).is_err());

        cache
            .apply_sync(&MetadataSyncResponse::Deltas {
                controller_epoch: 2,
                deltas: vec![MetadataDelta {
                    epoch: 2,
                    command: ControllerCommand::SetFeatureLevel {
                        expected_feature_level: 1,
                        new_feature_level: 2,
                    },
                }],
            })
            .unwrap();
        cache.require_feature(gate).unwrap();
        assert_eq!(cache.state().unwrap().active_feature_level(), 2);
    }

    #[test]
    fn broker_restart_from_stale_snapshot_converges_to_newer_authoritative_snapshot() {
        let stale = snapshot_at(4, 7);
        let mut cache = BrokerMetadataCache::from_snapshot(stale).unwrap();
        assert_eq!(cache.applied_metadata_epoch(), 4);

        let newer = snapshot_at(9, 7);
        cache
            .apply_sync(&MetadataSyncResponse::Snapshot {
                controller_epoch: 9,
                snapshot: Box::new(newer),
            })
            .unwrap();
        assert_eq!(cache.applied_metadata_epoch(), 9);

        let before = cache.state().unwrap().clone();
        assert!(
            cache
                .apply_sync(&MetadataSyncResponse::Snapshot {
                    controller_epoch: 3,
                    snapshot: Box::new(snapshot_at(3, 7)),
                })
                .is_err()
        );
        assert_eq!(cache.state().unwrap(), &before);
    }
}
