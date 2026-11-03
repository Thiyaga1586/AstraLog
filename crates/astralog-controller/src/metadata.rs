use std::collections::BTreeMap;

use crate::{
    CommandLimits, ControllerCommand, ControllerError, ControllerState, Result, encode_command,
};

pub const MAX_RETAINED_METADATA_DELTAS: usize = 256;
pub const MAX_METADATA_DELTAS_PER_RESPONSE: usize = 64;
pub const MAX_RETAINED_METADATA_BYTES: usize = 64 * 1024 * 1024;
pub const MAX_METADATA_DELTA_RESPONSE_BYTES: usize = 8 * 1024 * 1024;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MetadataDelta {
    pub epoch: u64,
    pub command: ControllerCommand,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ControllerMetadataSnapshot {
    pub epoch: u64,
    pub state: ControllerState,
}

impl ControllerMetadataSnapshot {
    pub fn from_state(state: &ControllerState) -> Self {
        Self {
            epoch: state.metadata_epoch(),
            state: state.clone(),
        }
    }

    pub fn validate(&self) -> Result<()> {
        if self.epoch != self.state.metadata_epoch() {
            return Err(ControllerError::Invalid(
                "metadata snapshot epoch differs from embedded controller state",
            ));
        }
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum MetadataSyncResponse {
    UpToDate {
        controller_epoch: u64,
    },
    Deltas {
        controller_epoch: u64,
        deltas: Vec<MetadataDelta>,
    },
    Snapshot {
        controller_epoch: u64,
        snapshot: Box<ControllerMetadataSnapshot>,
    },
}

impl MetadataSyncResponse {
    pub const fn controller_epoch(&self) -> u64 {
        match self {
            Self::UpToDate { controller_epoch }
            | Self::Deltas {
                controller_epoch, ..
            }
            | Self::Snapshot {
                controller_epoch, ..
            } => *controller_epoch,
        }
    }
}

#[derive(Debug, Clone)]
pub(crate) struct MetadataDisseminator {
    retained: BTreeMap<u64, RetainedDelta>,
    retained_bytes: usize,
}

#[derive(Debug, Clone)]
struct RetainedDelta {
    delta: MetadataDelta,
    encoded_bytes: usize,
}

impl MetadataDisseminator {
    pub(crate) fn new() -> Self {
        Self {
            retained: BTreeMap::new(),
            retained_bytes: 0,
        }
    }

    pub(crate) fn clear(&mut self) {
        self.retained.clear();
        self.retained_bytes = 0;
    }

    pub(crate) fn record(&mut self, epoch: u64, command: ControllerCommand) -> Result<()> {
        if epoch == 0 {
            return Err(ControllerError::Invalid(
                "metadata delta epoch zero is reserved",
            ));
        }
        if self.retained.contains_key(&epoch) {
            return Err(ControllerError::Invalid(
                "metadata delta epoch was retained twice",
            ));
        }
        let encoded_bytes = encode_command(&command, CommandLimits::default())?.len();
        self.retained_bytes =
            self.retained_bytes
                .checked_add(encoded_bytes)
                .ok_or(ControllerError::Invalid(
                    "metadata delta byte accounting overflow",
                ))?;
        self.retained.insert(
            epoch,
            RetainedDelta {
                delta: MetadataDelta { epoch, command },
                encoded_bytes,
            },
        );
        while self.retained.len() > MAX_RETAINED_METADATA_DELTAS
            || self.retained_bytes > MAX_RETAINED_METADATA_BYTES
        {
            let Some(oldest) = self.retained.first_key_value().map(|(epoch, _)| *epoch) else {
                break;
            };
            if let Some(removed) = self.retained.remove(&oldest) {
                self.retained_bytes = self.retained_bytes.saturating_sub(removed.encoded_bytes);
            }
        }
        Ok(())
    }

    pub(crate) fn response_for(
        &self,
        state: &ControllerState,
        applied_epoch: u64,
    ) -> Result<MetadataSyncResponse> {
        let controller_epoch = state.metadata_epoch();
        if applied_epoch > controller_epoch {
            return Err(ControllerError::Invalid(
                "broker metadata epoch is ahead of controller authority",
            ));
        }
        if applied_epoch == controller_epoch {
            return Ok(MetadataSyncResponse::UpToDate { controller_epoch });
        }

        let Some(mut expected) = applied_epoch.checked_add(1) else {
            return Err(ControllerError::Invalid("broker metadata epoch overflow"));
        };
        let mut deltas = Vec::new();
        let mut response_bytes = 0usize;
        while expected <= controller_epoch && deltas.len() < MAX_METADATA_DELTAS_PER_RESPONSE {
            let Some(retained) = self.retained.get(&expected) else {
                return Ok(MetadataSyncResponse::Snapshot {
                    controller_epoch,
                    snapshot: Box::new(ControllerMetadataSnapshot::from_state(state)),
                });
            };
            if !deltas.is_empty()
                && response_bytes.saturating_add(retained.encoded_bytes)
                    > MAX_METADATA_DELTA_RESPONSE_BYTES
            {
                break;
            }
            if retained.encoded_bytes > MAX_METADATA_DELTA_RESPONSE_BYTES {
                return Ok(MetadataSyncResponse::Snapshot {
                    controller_epoch,
                    snapshot: Box::new(ControllerMetadataSnapshot::from_state(state)),
                });
            }
            response_bytes += retained.encoded_bytes;
            deltas.push(retained.delta.clone());
            expected = expected
                .checked_add(1)
                .ok_or(ControllerError::Invalid("metadata delta epoch overflow"))?;
        }

        Ok(MetadataSyncResponse::Deltas {
            controller_epoch,
            deltas,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn state() -> ControllerState {
        ControllerState::new([7; 16], 1, 1).unwrap()
    }

    fn command() -> ControllerCommand {
        ControllerCommand::AllocateProducerIds {
            allocation_count: 1,
        }
    }

    #[test]
    fn contiguous_history_returns_bounded_incremental_deltas() {
        let mut state = state();
        let mut disseminator = MetadataDisseminator::new();
        for epoch in 1..=70 {
            let command = command();
            state.apply(epoch, &command).unwrap();
            disseminator.record(epoch, command).unwrap();
        }
        let response = disseminator.response_for(&state, 0).unwrap();
        let MetadataSyncResponse::Deltas {
            controller_epoch,
            deltas,
        } = response
        else {
            panic!("expected deltas");
        };
        assert_eq!(controller_epoch, 70);
        assert_eq!(deltas.len(), MAX_METADATA_DELTAS_PER_RESPONSE);
        assert_eq!(deltas.first().unwrap().epoch, 1);
        assert_eq!(deltas.last().unwrap().epoch, 64);
    }

    #[test]
    fn retention_miss_or_raft_index_gap_forces_snapshot() {
        let mut state = state();
        let mut disseminator = MetadataDisseminator::new();
        let command = command();
        state.apply(2, &command).unwrap();
        disseminator.record(2, command).unwrap();
        let response = disseminator.response_for(&state, 0).unwrap();
        assert!(matches!(response, MetadataSyncResponse::Snapshot { .. }));
    }

    #[test]
    fn retained_history_is_hard_bounded() {
        let mut state = state();
        let mut disseminator = MetadataDisseminator::new();
        for epoch in 1..=(MAX_RETAINED_METADATA_DELTAS as u64 + 4) {
            let command = command();
            state.apply(epoch, &command).unwrap();
            disseminator.record(epoch, command).unwrap();
        }
        assert_eq!(disseminator.retained.len(), MAX_RETAINED_METADATA_DELTAS);
        assert!(disseminator.retained_bytes <= MAX_RETAINED_METADATA_BYTES);
        assert!(matches!(
            disseminator.response_for(&state, 0).unwrap(),
            MetadataSyncResponse::Snapshot { .. }
        ));
    }
}
