use std::{
    collections::BTreeMap,
    sync::{Arc, Mutex},
    time::Instant,
};

use astralog_common::TopicId;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct DataAdmissionLimits {
    pub max_in_flight: usize,
    pub max_bytes: usize,
    pub low_watermark_bytes: usize,
    pub high_watermark_bytes: usize,
}

impl DataAdmissionLimits {
    pub(crate) fn validate(self) -> Result<(), &'static str> {
        if self.max_in_flight == 0 || self.max_bytes == 0 {
            return Err("data-admission count and byte budgets must be positive");
        }
        if self.low_watermark_bytes == 0 {
            return Err("data memory low watermark must be positive");
        }
        if self.high_watermark_bytes <= self.low_watermark_bytes {
            return Err("data memory high watermark must exceed low watermark");
        }
        if self.high_watermark_bytes > self.max_bytes {
            return Err("data memory high watermark exceeds global data byte budget");
        }
        Ok(())
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum DataAdmissionRejection {
    Pressure,
    Count,
    Bytes,
    Poisoned,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
struct DataAdmissionState {
    in_flight: usize,
    bytes: usize,
    pressure: bool,
}

#[derive(Debug)]
pub(crate) struct DataAdmissionGate {
    limits: DataAdmissionLimits,
    state: Mutex<DataAdmissionState>,
}

impl DataAdmissionGate {
    pub(crate) fn new(limits: DataAdmissionLimits) -> Result<Arc<Self>, &'static str> {
        limits.validate()?;
        Ok(Arc::new(Self {
            limits,
            state: Mutex::new(DataAdmissionState::default()),
        }))
    }

    pub(crate) fn try_acquire(
        self: &Arc<Self>,
        bytes: usize,
    ) -> Result<DataAdmissionPermit, DataAdmissionRejection> {
        let mut state = self
            .state
            .lock()
            .map_err(|_| DataAdmissionRejection::Poisoned)?;
        if state.pressure {
            return Err(DataAdmissionRejection::Pressure);
        }
        if state.in_flight >= self.limits.max_in_flight {
            return Err(DataAdmissionRejection::Count);
        }
        let next_bytes = state
            .bytes
            .checked_add(bytes)
            .ok_or(DataAdmissionRejection::Bytes)?;
        if next_bytes > self.limits.max_bytes {
            return Err(DataAdmissionRejection::Bytes);
        }
        state.in_flight += 1;
        state.bytes = next_bytes;
        if state.bytes >= self.limits.high_watermark_bytes {
            state.pressure = true;
        }
        Ok(DataAdmissionPermit {
            gate: Arc::clone(self),
            bytes,
        })
    }

    fn release(&self, bytes: usize) {
        let Ok(mut state) = self.state.lock() else {
            return;
        };
        state.in_flight = state.in_flight.saturating_sub(1);
        state.bytes = state.bytes.saturating_sub(bytes);
        if state.pressure && state.bytes < self.limits.low_watermark_bytes {
            state.pressure = false;
        }
    }

    #[cfg(test)]
    pub(crate) fn snapshot(&self) -> (usize, usize, bool) {
        self.state
            .lock()
            .map(|state| (state.in_flight, state.bytes, state.pressure))
            .unwrap_or((usize::MAX, usize::MAX, true))
    }
}

#[derive(Debug)]
pub(crate) struct DataAdmissionPermit {
    gate: Arc<DataAdmissionGate>,
    bytes: usize,
}

impl Drop for DataAdmissionPermit {
    fn drop(&mut self) {
        self.gate.release(self.bytes);
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct QuotaLimits {
    pub anonymous_rate_bytes_per_second: u64,
    pub anonymous_burst_bytes: u64,
    pub topic_rate_bytes_per_second: u64,
    pub topic_burst_bytes: u64,
    pub max_topic_buckets: usize,
    pub minimum_throttle_ms: u32,
}

impl QuotaLimits {
    pub(crate) fn validate(self) -> Result<(), &'static str> {
        if self.anonymous_rate_bytes_per_second == 0
            || self.anonymous_burst_bytes == 0
            || self.topic_rate_bytes_per_second == 0
            || self.topic_burst_bytes == 0
        {
            return Err("quota rates and burst capacities must be positive");
        }
        if self.max_topic_buckets == 0 {
            return Err("topic quota bucket count must be positive");
        }
        if self.minimum_throttle_ms == 0 {
            return Err("quota throttle hint must be non-zero");
        }
        Ok(())
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct QuotaRejection {
    pub throttle_ms: u32,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct TokenBucket {
    capacity: u64,
    tokens: u64,
    rate_bytes_per_second: u64,
    last_ms: u64,
    remainder: u64,
}

impl TokenBucket {
    fn full(capacity: u64, rate_bytes_per_second: u64, now_ms: u64) -> Self {
        Self {
            capacity,
            tokens: capacity,
            rate_bytes_per_second,
            last_ms: now_ms,
            remainder: 0,
        }
    }

    fn refill(&mut self, now_ms: u64) -> Result<(), ()> {
        let elapsed = now_ms.checked_sub(self.last_ms).ok_or(())?;
        if elapsed == 0 {
            return Ok(());
        }
        self.last_ms = now_ms;
        if self.tokens == self.capacity {
            self.remainder = 0;
            return Ok(());
        }
        let produced = u128::from(elapsed)
            .saturating_mul(u128::from(self.rate_bytes_per_second))
            .saturating_add(u128::from(self.remainder));
        let whole = produced / 1000;
        let remainder = produced % 1000;
        let added = u64::try_from(whole).unwrap_or(u64::MAX);
        self.tokens = self.tokens.saturating_add(added).min(self.capacity);
        self.remainder = if self.tokens == self.capacity {
            0
        } else {
            u64::try_from(remainder).unwrap_or(999)
        };
        Ok(())
    }

    fn wait_ms_for(&self, cost: u64) -> u32 {
        if self.tokens >= cost {
            return 0;
        }
        let deficit = cost.saturating_sub(self.tokens);
        let required_numerator = u128::from(deficit)
            .saturating_mul(1000)
            .saturating_sub(u128::from(self.remainder));
        let rate = u128::from(self.rate_bytes_per_second);
        let wait = required_numerator.saturating_add(rate.saturating_sub(1)) / rate;
        u32::try_from(wait.max(1)).unwrap_or(u32::MAX)
    }
}

#[derive(Debug)]
struct QuotaState {
    anonymous: TokenBucket,
    topics: BTreeMap<TopicId, TokenBucket>,
}

#[derive(Debug)]
pub(crate) struct QuotaGate {
    limits: QuotaLimits,
    started: Instant,
    state: Mutex<QuotaState>,
}

impl QuotaGate {
    pub(crate) fn new(limits: QuotaLimits) -> Result<Arc<Self>, &'static str> {
        limits.validate()?;
        Ok(Arc::new(Self {
            limits,
            started: Instant::now(),
            state: Mutex::new(QuotaState {
                anonymous: TokenBucket::full(
                    limits.anonymous_burst_bytes,
                    limits.anonymous_rate_bytes_per_second,
                    0,
                ),
                topics: BTreeMap::new(),
            }),
        }))
    }

    pub(crate) fn try_consume(
        &self,
        topics: &[TopicId],
        bytes: usize,
    ) -> Result<(), QuotaRejection> {
        let now_ms = u64::try_from(self.started.elapsed().as_millis()).unwrap_or(u64::MAX);
        self.try_consume_at(topics, bytes, now_ms)
    }

    fn try_consume_at(
        &self,
        topics: &[TopicId],
        bytes: usize,
        now_ms: u64,
    ) -> Result<(), QuotaRejection> {
        let cost = u64::try_from(bytes).unwrap_or(u64::MAX);
        if cost > self.limits.anonymous_burst_bytes
            || (!topics.is_empty() && cost > self.limits.topic_burst_bytes)
        {
            return Err(QuotaRejection {
                throttle_ms: self.limits.minimum_throttle_ms,
            });
        }
        let mut unique_topics = topics.to_vec();
        unique_topics.sort();
        unique_topics.dedup();
        let mut state = self.state.lock().map_err(|_| QuotaRejection {
            throttle_ms: self.limits.minimum_throttle_ms,
        })?;
        if state.anonymous.refill(now_ms).is_err() {
            return Err(QuotaRejection {
                throttle_ms: self.limits.minimum_throttle_ms,
            });
        }
        let missing = unique_topics
            .iter()
            .filter(|topic| !state.topics.contains_key(topic))
            .count();
        if state.topics.len().saturating_add(missing) > self.limits.max_topic_buckets {
            return Err(QuotaRejection {
                throttle_ms: self.limits.minimum_throttle_ms,
            });
        }
        for topic in &unique_topics {
            if let Some(bucket) = state.topics.get_mut(topic)
                && bucket.refill(now_ms).is_err()
            {
                return Err(QuotaRejection {
                    throttle_ms: self.limits.minimum_throttle_ms,
                });
            }
        }

        let mut throttle_ms = state.anonymous.wait_ms_for(cost);
        for topic in &unique_topics {
            if let Some(bucket) = state.topics.get(topic) {
                throttle_ms = throttle_ms.max(bucket.wait_ms_for(cost));
            }
        }
        if throttle_ms > 0 {
            return Err(QuotaRejection {
                throttle_ms: throttle_ms.max(self.limits.minimum_throttle_ms),
            });
        }

        state.anonymous.tokens = state.anonymous.tokens.saturating_sub(cost);
        for topic in unique_topics {
            let bucket = state.topics.entry(topic).or_insert_with(|| {
                TokenBucket::full(
                    self.limits.topic_burst_bytes,
                    self.limits.topic_rate_bytes_per_second,
                    now_ms,
                )
            });
            bucket.tokens = bucket.tokens.saturating_sub(cost);
        }
        Ok(())
    }

    #[cfg(test)]
    fn token_snapshot(&self, topic: Option<TopicId>) -> Option<u64> {
        let state = self.state.lock().ok()?;
        match topic {
            Some(topic) => state.topics.get(&topic).map(|bucket| bucket.tokens),
            None => Some(state.anonymous.tokens),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn p8g_data_admission_count_and_bytes_reject_one_over_without_growth() {
        let gate = DataAdmissionGate::new(DataAdmissionLimits {
            max_in_flight: 2,
            max_bytes: 100,
            low_watermark_bytes: 40,
            high_watermark_bytes: 90,
        })
        .unwrap();

        let first = gate.try_acquire(30).unwrap();
        let second = gate.try_acquire(30).unwrap();
        assert_eq!(gate.snapshot(), (2, 60, false));
        assert!(matches!(
            gate.try_acquire(1),
            Err(DataAdmissionRejection::Count)
        ));
        assert_eq!(gate.snapshot(), (2, 60, false));

        drop(second);
        assert!(matches!(
            gate.try_acquire(80),
            Err(DataAdmissionRejection::Bytes)
        ));
        assert_eq!(gate.snapshot(), (1, 30, false));
        drop(first);
        assert_eq!(gate.snapshot(), (0, 0, false));
    }

    #[test]
    fn p8g_memory_watermarks_use_strict_hysteresis() {
        let gate = DataAdmissionGate::new(DataAdmissionLimits {
            max_in_flight: 8,
            max_bytes: 120,
            low_watermark_bytes: 40,
            high_watermark_bytes: 80,
        })
        .unwrap();

        let first = gate.try_acquire(35).unwrap();
        let second = gate.try_acquire(45).unwrap();
        assert_eq!(gate.snapshot(), (2, 80, true));
        assert!(matches!(
            gate.try_acquire(1),
            Err(DataAdmissionRejection::Pressure)
        ));

        drop(second);
        assert_eq!(gate.snapshot(), (1, 35, false));
        let third = gate.try_acquire(45).unwrap();
        assert_eq!(gate.snapshot(), (2, 80, true));

        // Returning exactly to the low watermark keeps pressure active.
        drop(first);
        assert_eq!(gate.snapshot(), (1, 45, true));
        assert!(matches!(
            gate.try_acquire(1),
            Err(DataAdmissionRejection::Pressure)
        ));
        drop(third);
        assert_eq!(gate.snapshot(), (0, 0, false));
    }

    #[test]
    fn p8g_watermark_configuration_fails_closed() {
        for limits in [
            DataAdmissionLimits {
                max_in_flight: 0,
                max_bytes: 100,
                low_watermark_bytes: 20,
                high_watermark_bytes: 80,
            },
            DataAdmissionLimits {
                max_in_flight: 1,
                max_bytes: 100,
                low_watermark_bytes: 0,
                high_watermark_bytes: 80,
            },
            DataAdmissionLimits {
                max_in_flight: 1,
                max_bytes: 100,
                low_watermark_bytes: 80,
                high_watermark_bytes: 80,
            },
            DataAdmissionLimits {
                max_in_flight: 1,
                max_bytes: 100,
                low_watermark_bytes: 80,
                high_watermark_bytes: 101,
            },
        ] {
            assert!(limits.validate().is_err());
        }
    }

    #[test]
    fn p8g_token_bucket_refill_and_burst_are_deterministic() {
        let gate = QuotaGate::new(QuotaLimits {
            anonymous_rate_bytes_per_second: 1000,
            anonymous_burst_bytes: 10,
            topic_rate_bytes_per_second: 1000,
            topic_burst_bytes: 10,
            max_topic_buckets: 4,
            minimum_throttle_ms: 1,
        })
        .unwrap();
        let topic = TopicId::new([1; 16]);
        assert!(gate.try_consume_at(&[topic], 10, 0).is_ok());
        assert_eq!(gate.token_snapshot(None), Some(0));
        assert_eq!(gate.token_snapshot(Some(topic)), Some(0));
        let rejected = gate.try_consume_at(&[topic], 1, 0).unwrap_err();
        assert_eq!(rejected.throttle_ms, 1);
        assert!(gate.try_consume_at(&[topic], 1, 1).is_ok());
        assert_eq!(gate.token_snapshot(None), Some(0));
        assert_eq!(gate.token_snapshot(Some(topic)), Some(0));
    }

    #[test]
    fn p8g_topic_buckets_are_independent_bounded_and_fail_before_partial_charge() {
        let gate = QuotaGate::new(QuotaLimits {
            anonymous_rate_bytes_per_second: 100_000,
            anonymous_burst_bytes: 1000,
            topic_rate_bytes_per_second: 1,
            topic_burst_bytes: 10,
            max_topic_buckets: 2,
            minimum_throttle_ms: 5,
        })
        .unwrap();
        let first = TopicId::new([1; 16]);
        let second = TopicId::new([2; 16]);
        let third = TopicId::new([3; 16]);
        assert!(gate.try_consume_at(&[first], 10, 0).is_ok());
        assert!(gate.try_consume_at(&[second], 5, 0).is_ok());
        assert!(gate.try_consume_at(&[first], 1, 0).is_err());
        let anonymous_before = gate.token_snapshot(None).unwrap();
        let second_before = gate.token_snapshot(Some(second)).unwrap();
        assert!(gate.try_consume_at(&[third], 1, 0).is_err());
        assert_eq!(gate.token_snapshot(None), Some(anonymous_before));
        assert_eq!(gate.token_snapshot(Some(second)), Some(second_before));
    }
}
