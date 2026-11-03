use std::collections::BTreeMap;

use astralog_common::TopicId;
use astralog_protocol::api::{AckMode, WireRecordV0};
use tokio::sync::oneshot;

use crate::{ClientError, Result};

pub(crate) type PartitionKey = (TopicId, u32);

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum BatchMode {
    V0(AckMode),
    V1,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct BufferedOffsets {
    pub base_offset: Option<u64>,
    pub last_offset: Option<u64>,
}

pub(crate) type DeliveryResult = std::result::Result<BufferedOffsets, String>;

#[derive(Debug, Clone, Copy)]
pub(crate) struct AccumulatorLimits {
    pub max_total_bytes: usize,
    pub max_total_records: usize,
    pub max_partitions: usize,
}

#[derive(Debug, Clone, Copy)]
pub(crate) struct TopicBatchLimits {
    pub max_record_bytes: usize,
    pub max_batch_records: usize,
    pub max_batch_bytes: usize,
}

#[derive(Debug, Clone, Copy)]
pub(crate) struct AdmissionSpec {
    pub limits: AccumulatorLimits,
    pub topic_limits: TopicBatchLimits,
    pub key: PartitionKey,
    pub mode: BatchMode,
    pub logical_bytes: usize,
    pub now_ms: u64,
    pub linger_ms: u64,
    pub sticky: bool,
}

pub(crate) enum AdmissionAttempt {
    DrainFirst,
    Accepted(Admission),
}

pub(crate) struct Admission {
    pub receiver: oneshot::Receiver<DeliveryResult>,
    pub deadline_ms: u64,
    pub drive_now: bool,
}

struct BufferedRecord {
    record: WireRecordV0,
    sender: oneshot::Sender<DeliveryResult>,
}

struct OpenBatch {
    mode: BatchMode,
    records: Vec<BufferedRecord>,
    logical_bytes: usize,
    deadline_ms: u64,
    close_requested: bool,
    has_sticky_record: bool,
}

struct PartitionAccumulator {
    open: Option<OpenBatch>,
    driver_active: bool,
    in_flight: bool,
}

pub(crate) struct ClosedBatch {
    pub mode: BatchMode,
    pub records: Vec<WireRecordV0>,
    senders: Vec<oneshot::Sender<DeliveryResult>>,
    pub has_sticky_record: bool,
}

impl ClosedBatch {
    pub fn complete_success(self, outcomes: Vec<BufferedOffsets>) -> Result<()> {
        if outcomes.len() != self.senders.len() {
            return Err(ClientError::InvalidResponse(
                "batched Produce outcome count does not match admitted records",
            ));
        }
        for (sender, outcome) in self.senders.into_iter().zip(outcomes) {
            let _ = sender.send(Ok(outcome));
        }
        Ok(())
    }

    pub fn complete_error(self, error: impl Into<String>) {
        let error = error.into();
        for sender in self.senders {
            let _ = sender.send(Err(error.clone()));
        }
    }
}

#[derive(Default)]
pub(crate) struct ProducerAccumulator {
    partitions: BTreeMap<PartitionKey, PartitionAccumulator>,
    total_bytes: usize,
    total_records: usize,
    closed: bool,
}

impl ProducerAccumulator {
    pub fn admit(
        &mut self,
        record: &WireRecordV0,
        spec: AdmissionSpec,
    ) -> Result<AdmissionAttempt> {
        let AdmissionSpec {
            limits,
            topic_limits,
            key,
            mode,
            logical_bytes,
            now_ms,
            linger_ms,
            sticky,
        } = spec;
        if self.closed {
            return Err(ClientError::AccumulatorClosed);
        }
        if logical_bytes == 0
            || logical_bytes > topic_limits.max_record_bytes
            || logical_bytes > topic_limits.max_batch_bytes
        {
            return Err(ClientError::AccumulatorRecordTooLarge);
        }

        if let Some(slot) = self.partitions.get_mut(&key)
            && let Some(open) = slot.open.as_mut()
        {
            let would_exceed_records = open.records.len() >= topic_limits.max_batch_records;
            let would_exceed_bytes = open
                .logical_bytes
                .checked_add(logical_bytes)
                .is_none_or(|bytes| bytes > topic_limits.max_batch_bytes);
            if open.mode != mode || would_exceed_records || would_exceed_bytes {
                open.close_requested = true;
                return Ok(AdmissionAttempt::DrainFirst);
            }
        }

        if !self.partitions.contains_key(&key) && self.partitions.len() >= limits.max_partitions {
            return Err(ClientError::AccumulatorFull);
        }
        if self.total_records >= limits.max_total_records
            || self
                .total_bytes
                .checked_add(logical_bytes)
                .is_none_or(|bytes| bytes > limits.max_total_bytes)
        {
            return Err(ClientError::AccumulatorFull);
        }

        let (sender, receiver) = oneshot::channel();
        let slot = self
            .partitions
            .entry(key)
            .or_insert_with(|| PartitionAccumulator {
                open: None,
                driver_active: false,
                in_flight: false,
            });
        let open = slot.open.get_or_insert_with(|| OpenBatch {
            mode,
            records: Vec::new(),
            logical_bytes: 0,
            deadline_ms: now_ms.saturating_add(linger_ms),
            close_requested: false,
            has_sticky_record: false,
        });
        open.logical_bytes = open
            .logical_bytes
            .checked_add(logical_bytes)
            .ok_or(ClientError::AccumulatorFull)?;
        open.records.push(BufferedRecord {
            record: record.clone(),
            sender,
        });
        open.has_sticky_record |= sticky;
        self.total_bytes = self
            .total_bytes
            .checked_add(logical_bytes)
            .ok_or(ClientError::AccumulatorFull)?;
        self.total_records = self
            .total_records
            .checked_add(1)
            .ok_or(ClientError::AccumulatorFull)?;

        if open.records.len() == topic_limits.max_batch_records
            || open.logical_bytes == topic_limits.max_batch_bytes
        {
            open.close_requested = true;
        }
        let deadline_ms = open.deadline_ms;
        Ok(AdmissionAttempt::Accepted(Admission {
            receiver,
            deadline_ms,
            drive_now: open.close_requested,
        }))
    }

    pub fn request_close(&mut self, key: PartitionKey) -> bool {
        self.partitions
            .get_mut(&key)
            .and_then(|slot| slot.open.as_mut())
            .is_some_and(|open| {
                open.close_requested = true;
                true
            })
    }

    pub fn request_close_all(&mut self) -> Vec<PartitionKey> {
        let mut keys = Vec::new();
        for (key, slot) in &mut self.partitions {
            if let Some(open) = slot.open.as_mut() {
                open.close_requested = true;
                keys.push(*key);
            } else if slot.in_flight {
                keys.push(*key);
            }
        }
        keys
    }

    pub fn close_admission(&mut self) -> Vec<PartitionKey> {
        self.closed = true;
        self.request_close_all()
    }

    pub fn begin_drive(&mut self, key: PartitionKey) -> bool {
        let Some(slot) = self.partitions.get_mut(&key) else {
            return false;
        };
        if slot.driver_active {
            return false;
        }
        slot.driver_active = true;
        true
    }

    pub fn take_closed(&mut self, key: PartitionKey) -> Option<ClosedBatch> {
        let slot = self.partitions.get_mut(&key)?;
        if slot.in_flight || !slot.open.as_ref().is_some_and(|open| open.close_requested) {
            return None;
        }
        let open = slot.open.take()?;
        slot.in_flight = true;
        self.total_bytes = self.total_bytes.saturating_sub(open.logical_bytes);
        self.total_records = self.total_records.saturating_sub(open.records.len());
        let (records, senders): (Vec<_>, Vec<_>) = open
            .records
            .into_iter()
            .map(|buffered| (buffered.record, buffered.sender))
            .unzip();
        Some(ClosedBatch {
            mode: open.mode,
            records,
            senders,
            has_sticky_record: open.has_sticky_record,
        })
    }

    pub fn close_expired(&mut self, now_ms: u64) -> Vec<PartitionKey> {
        let mut keys = Vec::new();
        for (key, slot) in &mut self.partitions {
            if let Some(open) = slot.open.as_mut()
                && now_ms >= open.deadline_ms
                && !open.close_requested
            {
                open.close_requested = true;
                keys.push(*key);
            }
        }
        keys
    }

    pub fn closed_mode(&self, key: PartitionKey) -> Option<BatchMode> {
        let slot = self.partitions.get(&key)?;
        slot.open
            .as_ref()
            .filter(|open| open.close_requested)
            .map(|open| open.mode)
    }

    pub fn complete_in_flight(&mut self, key: PartitionKey) {
        if let Some(slot) = self.partitions.get_mut(&key) {
            slot.in_flight = false;
        }
    }

    pub fn end_drive(&mut self, key: PartitionKey) {
        let remove = if let Some(slot) = self.partitions.get_mut(&key) {
            slot.driver_active = false;
            slot.open.is_none() && !slot.in_flight
        } else {
            false
        };
        if remove {
            self.partitions.remove(&key);
        }
    }

    pub fn counts(&self) -> (usize, usize, usize) {
        (self.total_bytes, self.total_records, self.partitions.len())
    }

    #[cfg(test)]
    pub fn is_closed(&self) -> bool {
        self.closed
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn record(byte: u8, len: usize) -> WireRecordV0 {
        WireRecordV0 {
            timestamp_ms: 10,
            key: Some(vec![byte]),
            value: Some(vec![byte; len]),
            headers: Vec::new(),
        }
    }

    fn limits() -> AccumulatorLimits {
        AccumulatorLimits {
            max_total_bytes: 1_000,
            max_total_records: 8,
            max_partitions: 2,
        }
    }

    fn topic_limits() -> TopicBatchLimits {
        TopicBatchLimits {
            max_record_bytes: 500,
            max_batch_records: 2,
            max_batch_bytes: 700,
        }
    }

    fn spec(key: PartitionKey, mode: BatchMode, logical_bytes: usize) -> AdmissionSpec {
        AdmissionSpec {
            limits: limits(),
            topic_limits: topic_limits(),
            key,
            mode,
            logical_bytes,
            now_ms: 0,
            linger_ms: 10,
            sticky: false,
        }
    }

    #[test]
    fn p8_accumulator_is_partition_local_count_byte_and_linger_bounded() {
        let mut accumulator = ProducerAccumulator::default();
        let t = TopicId::new([1; 16]);
        let a = record(1, 10);
        let b = record(2, 10);
        let a_len = a.encoded_logical_len().unwrap();
        let b_len = b.encoded_logical_len().unwrap();
        let AdmissionAttempt::Accepted(first) = accumulator
            .admit(
                &a,
                AdmissionSpec {
                    now_ms: 100,
                    linger_ms: 25,
                    sticky: true,
                    ..spec((t, 0), BatchMode::V0(AckMode::Quorum), a_len)
                },
            )
            .unwrap()
        else {
            panic!("first record must be admitted")
        };
        assert_eq!(first.deadline_ms, 125);
        assert!(!first.drive_now);
        assert!(accumulator.close_expired(124).is_empty());
        assert_eq!(accumulator.close_expired(125), vec![(t, 0)]);
        // Re-open the batch for the exact count-bound proof below.
        let mut accumulator = ProducerAccumulator::default();
        let AdmissionAttempt::Accepted(first) = accumulator
            .admit(
                &a,
                AdmissionSpec {
                    now_ms: 100,
                    linger_ms: 25,
                    sticky: true,
                    ..spec((t, 0), BatchMode::V0(AckMode::Quorum), a_len)
                },
            )
            .unwrap()
        else {
            panic!("first record must be admitted")
        };
        assert!(!first.drive_now);
        let AdmissionAttempt::Accepted(second) = accumulator
            .admit(
                &b,
                AdmissionSpec {
                    now_ms: 101,
                    linger_ms: 25,
                    ..spec((t, 0), BatchMode::V0(AckMode::Quorum), b_len)
                },
            )
            .unwrap()
        else {
            panic!("second record must be admitted")
        };
        assert!(second.drive_now, "exact record-count boundary closes batch");
        assert_eq!(accumulator.counts().1, 2);
        assert!(accumulator.begin_drive((t, 0)));
        let closed = accumulator.take_closed((t, 0)).unwrap();
        assert_eq!(closed.records, vec![a, b]);
        assert!(closed.has_sticky_record);
        accumulator.complete_in_flight((t, 0));
        accumulator.end_drive((t, 0));
        assert_eq!(accumulator.counts(), (0, 0, 0));
    }

    #[test]
    fn p8_accumulator_closes_on_exact_byte_boundary_and_drains_before_one_over() {
        let mut accumulator = ProducerAccumulator::default();
        let t = TopicId::new([4; 16]);
        let a = record(1, 8);
        let b = record(2, 8);
        let len = a.encoded_logical_len().unwrap();
        assert_eq!(len, b.encoded_logical_len().unwrap());
        let local = TopicBatchLimits {
            max_record_bytes: len,
            max_batch_records: 10,
            max_batch_bytes: len * 2,
        };
        assert!(matches!(
            accumulator
                .admit(
                    &a,
                    AdmissionSpec {
                        topic_limits: local,
                        ..spec((t, 0), BatchMode::V1, len)
                    },
                )
                .unwrap(),
            AdmissionAttempt::Accepted(Admission {
                drive_now: false,
                ..
            })
        ));
        let AdmissionAttempt::Accepted(second) = accumulator
            .admit(
                &b,
                AdmissionSpec {
                    topic_limits: local,
                    now_ms: 1,
                    ..spec((t, 0), BatchMode::V1, len)
                },
            )
            .unwrap()
        else {
            panic!()
        };
        assert!(second.drive_now);

        let c = record(3, 8);
        assert!(matches!(
            accumulator
                .admit(
                    &c,
                    AdmissionSpec {
                        topic_limits: local,
                        now_ms: 2,
                        ..spec((t, 0), BatchMode::V1, len)
                    },
                )
                .unwrap(),
            AdmissionAttempt::DrainFirst
        ));
    }

    #[test]
    fn p8_accumulator_rejects_global_one_over_and_partition_cardinality() {
        let mut accumulator = ProducerAccumulator::default();
        let mut global = limits();
        global.max_total_records = 1;
        let a = record(1, 10);
        let len = a.encoded_logical_len().unwrap();
        let t = TopicId::new([2; 16]);
        assert!(matches!(
            accumulator
                .admit(
                    &a,
                    AdmissionSpec {
                        limits: global,
                        ..spec((t, 0), BatchMode::V1, len)
                    },
                )
                .unwrap(),
            AdmissionAttempt::Accepted(_)
        ));
        assert!(matches!(
            accumulator.admit(
                &a,
                AdmissionSpec {
                    limits: global,
                    ..spec((t, 1), BatchMode::V1, len)
                },
            ),
            Err(ClientError::AccumulatorFull)
        ));

        let mut accumulator = ProducerAccumulator::default();
        let mut global = limits();
        global.max_total_bytes = len;
        accumulator
            .admit(
                &a,
                AdmissionSpec {
                    limits: global,
                    ..spec((t, 0), BatchMode::V1, len)
                },
            )
            .unwrap();
        assert!(matches!(
            accumulator.admit(
                &a,
                AdmissionSpec {
                    limits: global,
                    ..spec((t, 1), BatchMode::V1, len)
                },
            ),
            Err(ClientError::AccumulatorFull)
        ));

        let mut accumulator = ProducerAccumulator::default();
        let mut global = limits();
        global.max_partitions = 1;
        accumulator
            .admit(
                &a,
                AdmissionSpec {
                    limits: global,
                    ..spec((t, 0), BatchMode::V1, len)
                },
            )
            .unwrap();
        assert!(matches!(
            accumulator.admit(
                &a,
                AdmissionSpec {
                    limits: global,
                    ..spec((t, 1), BatchMode::V1, len)
                },
            ),
            Err(ClientError::AccumulatorFull)
        ));
    }

    #[test]
    fn p8_accumulator_closes_before_one_over_local_batch_and_shutdown_is_explicit() {
        let mut accumulator = ProducerAccumulator::default();
        let t = TopicId::new([3; 16]);
        let a = record(1, 10);
        let len = a.encoded_logical_len().unwrap();
        let mut local = topic_limits();
        local.max_batch_records = 1;
        let AdmissionAttempt::Accepted(first) = accumulator
            .admit(
                &a,
                AdmissionSpec {
                    topic_limits: local,
                    ..spec((t, 0), BatchMode::V1, len)
                },
            )
            .unwrap()
        else {
            panic!()
        };
        assert!(first.drive_now);
        let keys = accumulator.close_admission();
        assert_eq!(keys, vec![(t, 0)]);
        assert!(accumulator.is_closed());
        assert!(matches!(
            accumulator.admit(
                &a,
                AdmissionSpec {
                    topic_limits: local,
                    ..spec((t, 1), BatchMode::V1, len)
                },
            ),
            Err(ClientError::AccumulatorClosed)
        ));
    }
}
