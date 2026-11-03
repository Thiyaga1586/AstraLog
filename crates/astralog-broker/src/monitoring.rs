//! Optional local-only metrics for public Produce and Fetch request outcomes.
//!
//! No per-topic, partition, record key, client, or user-provided labels.
//! Counters reset when the broker process restarts. All samples are derived
//! from actual wire requests and actual wire responses.

use std::{
    fmt::Write,
    sync::atomic::{AtomicU64, Ordering},
    time::Duration,
};

use astralog_protocol::{
    ApiKey, PublicError, WireFrame,
    api::{
        BodyCodec, FetchResponseV0, ProduceRequestV0, ProduceRequestV1, ProduceResponseV0,
        ProduceResponseV1,
    },
};

const BOUNDS_SECONDS: [f64; 14] = [
    0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0, 30.0, 60.0,
];
const BOUNDS_TEXT: [&str; 14] = [
    "0.001", "0.005", "0.01", "0.025", "0.05", "0.1", "0.25", "0.5", "1", "2.5", "5", "10", "30",
    "60",
];

struct ApiSeries {
    ok: AtomicU64,
    error: AtomicU64,
    buckets: [AtomicU64; BOUNDS_SECONDS.len()],
    durations: AtomicU64,
    duration_nanos: AtomicU64,
}

impl ApiSeries {
    fn new() -> Self {
        Self {
            ok: AtomicU64::new(0),
            error: AtomicU64::new(0),
            buckets: std::array::from_fn(|_| AtomicU64::new(0)),
            durations: AtomicU64::new(0),
            duration_nanos: AtomicU64::new(0),
        }
    }

    fn observe(&self, ok: bool, latency: Duration) {
        let counter = if ok { &self.ok } else { &self.error };
        counter.fetch_add(1, Ordering::Relaxed);
        self.durations.fetch_add(1, Ordering::Relaxed);
        let nanos = u64::try_from(latency.as_nanos()).unwrap_or(u64::MAX);
        let _ = self
            .duration_nanos
            .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |old| {
                Some(old.saturating_add(nanos))
            });
        let duration = latency.as_secs_f64();
        for (index, upper) in BOUNDS_SECONDS.iter().enumerate() {
            if duration <= *upper {
                self.buckets[index].fetch_add(1, Ordering::Relaxed);
            }
        }
    }
}

pub(crate) struct BrokerMetrics {
    broker_id: u32,
    produce: ApiSeries,
    fetch: ApiSeries,
    produce_records: AtomicU64,
    produce_bytes: AtomicU64,
    fetch_records: AtomicU64,
    fetch_bytes: AtomicU64,
}

impl BrokerMetrics {
    pub(crate) fn new(broker_id: u32) -> Self {
        Self {
            broker_id,
            produce: ApiSeries::new(),
            fetch: ApiSeries::new(),
            produce_records: AtomicU64::new(0),
            produce_bytes: AtomicU64::new(0),
            fetch_records: AtomicU64::new(0),
            fetch_bytes: AtomicU64::new(0),
        }
    }

    /// Count only acknowledged requests. A top-level success with a failing
    /// partition is an error, not a successful operation.
    ///
    /// Produce record counters count successful acknowledged record
    /// observations, including successful duplicate retries. They do not
    /// claim unique newly committed records or end-to-end exactly once.
    pub(crate) fn observe_response(
        &self,
        request: &WireFrame,
        response: &WireFrame,
        latency: Duration,
    ) {
        let Ok(api) = ApiKey::try_from(request.api_key) else {
            return;
        };
        match api {
            ApiKey::Produce => {
                let (good, records, bytes) = produce_outcome(request, response);
                self.produce.observe(good, latency);
                if records > 0 {
                    self.produce_records.fetch_add(records, Ordering::Relaxed);
                    self.produce_bytes.fetch_add(bytes, Ordering::Relaxed);
                }
            }
            ApiKey::Fetch => {
                let (good, records, bytes) = fetch_outcome(response);
                self.fetch.observe(good, latency);
                if records > 0 {
                    self.fetch_records.fetch_add(records, Ordering::Relaxed);
                    self.fetch_bytes.fetch_add(bytes, Ordering::Relaxed);
                }
            }
            _ => {}
        }
    }

    pub(crate) fn render(&self) -> String {
        let mut out = String::with_capacity(6_000);
        writeln!(
            out,
            "# HELP astralog_broker_info Broker identity for this scrape endpoint."
        )
        .unwrap();
        writeln!(out, "# TYPE astralog_broker_info gauge").unwrap();
        writeln!(
            out,
            "astralog_broker_info{{broker_id=\"{}\"}} 1",
            self.broker_id
        )
        .unwrap();
        writeln!(out, "# HELP astralog_broker_requests_total Completed public Produce or Fetch requests, including partition errors.").unwrap();
        writeln!(out, "# TYPE astralog_broker_requests_total counter").unwrap();
        for (api, series) in [("produce", &self.produce), ("fetch", &self.fetch)] {
            for (result, counter) in [("ok", &series.ok), ("error", &series.error)] {
                writeln!(
                    out,
                    "astralog_broker_requests_total{{api=\"{api}\",result=\"{result}\"}} {}",
                    counter.load(Ordering::Relaxed)
                )
                .unwrap();
            }
        }
        writeln!(out, "# HELP astralog_broker_request_duration_seconds Server-side processing duration for completed public Produce/Fetch requests.").unwrap();
        writeln!(
            out,
            "# TYPE astralog_broker_request_duration_seconds histogram"
        )
        .unwrap();
        for (api, series) in [("produce", &self.produce), ("fetch", &self.fetch)] {
            for (index, upper) in BOUNDS_TEXT.iter().enumerate() {
                writeln!(
                    out,
                    "astralog_broker_request_duration_seconds_bucket{{api=\"{api}\",le=\"{upper}\"}} {}",
                    series.buckets[index].load(Ordering::Relaxed)
                )
                .unwrap();
            }
            let count = series.durations.load(Ordering::Relaxed);
            writeln!(out, "astralog_broker_request_duration_seconds_bucket{{api=\"{api}\",le=\"+Inf\"}} {count}").unwrap();
            writeln!(
                out,
                "astralog_broker_request_duration_seconds_sum{{api=\"{api}\"}} {:.9}",
                (series.duration_nanos.load(Ordering::Relaxed) as f64) / 1_000_000_000.0
            )
            .unwrap();
            writeln!(
                out,
                "astralog_broker_request_duration_seconds_count{{api=\"{api}\"}} {count}"
            )
            .unwrap();
        }
        for (name, doc, counter) in [
            (
                "astralog_produce_records_total",
                "Records in successful acknowledged Produce partition responses, including successful duplicate retries.",
                &self.produce_records,
            ),
            (
                "astralog_produce_bytes_total",
                "Payload value bytes in successful acknowledged Produce partition responses.",
                &self.produce_bytes,
            ),
            (
                "astralog_fetch_records_total",
                "Records returned in successful Fetch partition responses.",
                &self.fetch_records,
            ),
            (
                "astralog_fetch_bytes_total",
                "Payload value bytes returned in successful Fetch partition responses.",
                &self.fetch_bytes,
            ),
        ] {
            writeln!(out, "# HELP {name} {doc}").unwrap();
            writeln!(out, "# TYPE {name} counter").unwrap();
            writeln!(out, "{name} {}", counter.load(Ordering::Relaxed)).unwrap();
        }
        out
    }
}

fn produce_outcome(request: &WireFrame, response: &WireFrame) -> (bool, u64, u64) {
    if response.error_code != PublicError::None as u16 {
        return (false, 0, 0);
    }
    match request.api_version {
        0 => {
            let (Ok(request), Ok(response)) = (
                ProduceRequestV0::decode_body(&request.body),
                ProduceResponseV0::decode_body(&response.body),
            ) else {
                return (false, 0, 0);
            };
            if request.partitions.len() != response.partitions.len() {
                return (false, 0, 0);
            }
            let mut result = (true, 0_u64, 0_u64);
            for requested in request.partitions {
                let Some(observed) = response.partitions.iter().find(|r| {
                    r.topic_id == requested.topic_id && r.partition == requested.partition
                }) else {
                    return (false, 0, 0);
                };
                if observed.error_code != PublicError::None as u16 {
                    result.0 = false;
                } else {
                    record_totals(&requested.records, &mut result.1, &mut result.2);
                }
            }
            result
        }
        1 => {
            let (Ok(request), Ok(response)) = (
                ProduceRequestV1::decode_body(&request.body),
                ProduceResponseV1::decode_body(&response.body),
            ) else {
                return (false, 0, 0);
            };
            if request.partitions.len() != response.partitions.len() {
                return (false, 0, 0);
            }
            let mut result = (true, 0_u64, 0_u64);
            for requested in request.partitions {
                let Some(observed) = response.partitions.iter().find(|r| {
                    r.topic_id == requested.topic_id && r.partition == requested.partition
                }) else {
                    return (false, 0, 0);
                };
                if observed.error_code != PublicError::None as u16 {
                    result.0 = false;
                } else {
                    record_totals(&requested.records, &mut result.1, &mut result.2);
                }
            }
            result
        }
        _ => (false, 0, 0),
    }
}

fn record_totals(
    records: &[astralog_protocol::api::WireRecordV0],
    count: &mut u64,
    bytes: &mut u64,
) {
    *count = count.saturating_add(u64::try_from(records.len()).unwrap_or(u64::MAX));
    for record in records {
        let value_len = record.value.as_ref().map_or(0, Vec::len);
        *bytes = bytes.saturating_add(u64::try_from(value_len).unwrap_or(u64::MAX));
    }
}

fn fetch_outcome(response: &WireFrame) -> (bool, u64, u64) {
    if response.error_code != PublicError::None as u16 {
        return (false, 0, 0);
    }
    let Ok(response) = FetchResponseV0::decode_body(&response.body) else {
        return (false, 0, 0);
    };
    let mut result = (true, 0_u64, 0_u64);
    for part in &response.partitions {
        if part.error_code != PublicError::None as u16 {
            result.0 = false;
            continue;
        }
        result.1 = result
            .1
            .saturating_add(u64::try_from(part.records.len()).unwrap_or(u64::MAX));
        for record in &part.records {
            result.2 = result.2.saturating_add(
                u64::try_from(record.record.value.as_ref().map_or(0, Vec::len)).unwrap_or(u64::MAX),
            );
        }
    }
    result
}

#[cfg(test)]
mod tests {
    use super::*;
    use astralog_common::TopicId;
    use astralog_protocol::{
        FrameKind,
        api::{
            AckMode, FetchPartitionResponseV0, FetchRecordV0, ProducePartitionRequestV0,
            ProducePartitionResponseV0, WireRecordV0,
        },
    };

    fn frame(api: ApiKey, version: u16, body: Vec<u8>) -> WireFrame {
        WireFrame {
            kind: FrameKind::Response,
            flags: 0,
            api_key: api as u16,
            api_version: version,
            error_code: 0,
            correlation_id: 7,
            timeout_ms: 0,
            throttle_ms: 0,
            header_tags: vec![],
            body,
        }
    }

    fn record(value: &[u8]) -> WireRecordV0 {
        WireRecordV0 {
            timestamp_ms: 17,
            key: None,
            value: Some(value.to_vec()),
            headers: vec![],
        }
    }

    #[test]
    fn success_counts_real_partition_records_and_value_bytes() {
        let metrics = BrokerMetrics::new(3);
        let id = TopicId::new([9; 16]);
        let req = ProduceRequestV0 {
            acks: AckMode::Quorum,
            partitions: vec![ProducePartitionRequestV0 {
                topic_id: id,
                partition: 0,
                leader_epoch: 1,
                records: vec![record(b"abc"), record(b"xy")],
            }],
            tags: vec![],
        };
        let response = ProduceResponseV0 {
            partitions: vec![ProducePartitionResponseV0 {
                topic_id: id,
                partition: 0,
                error_code: 0,
                base_offset: 0,
                last_offset: 1,
                leader_broker_id: 3,
                leader_epoch: 1,
            }],
            tags: vec![],
        };
        metrics.observe_response(
            &frame(ApiKey::Produce, 0, req.encode_body().unwrap()),
            &frame(ApiKey::Produce, 0, response.encode_body().unwrap()),
            Duration::from_millis(125),
        );
        let text = metrics.render();
        assert!(text.contains("astralog_produce_records_total 2"));
        assert!(text.contains("astralog_produce_bytes_total 5"));
        assert!(text.contains("astralog_broker_requests_total{api=\"produce\",result=\"ok\"} 1"));
        assert!(text.contains(
            "astralog_broker_request_duration_seconds_bucket{api=\"produce\",le=\"0.25\"} 1"
        ));
        assert!(text.contains(
            "astralog_broker_request_duration_seconds_bucket{api=\"produce\",le=\"0.1\"} 0"
        ));
    }

    #[test]
    fn partition_failure_counts_as_error_not_success() {
        let metrics = BrokerMetrics::new(2);
        let id = TopicId::new([4; 16]);
        let req = ProduceRequestV0 {
            acks: AckMode::Quorum,
            partitions: vec![ProducePartitionRequestV0 {
                topic_id: id,
                partition: 0,
                leader_epoch: 1,
                records: vec![record(b"value")],
            }],
            tags: vec![],
        };
        let response = ProduceResponseV0 {
            partitions: vec![ProducePartitionResponseV0 {
                topic_id: id,
                partition: 0,
                error_code: PublicError::NotLeader as u16,
                base_offset: 0,
                last_offset: 0,
                leader_broker_id: 1,
                leader_epoch: 1,
            }],
            tags: vec![],
        };
        metrics.observe_response(
            &frame(ApiKey::Produce, 0, req.encode_body().unwrap()),
            &frame(ApiKey::Produce, 0, response.encode_body().unwrap()),
            Duration::from_millis(1),
        );
        let text = metrics.render();
        assert!(
            text.contains("astralog_broker_requests_total{api=\"produce\",result=\"error\"} 1")
        );
        assert!(text.contains("astralog_produce_records_total 0"));
    }

    #[test]
    fn fetch_counts_only_successful_partitions() {
        let metrics = BrokerMetrics::new(2);
        let id = TopicId::new([4; 16]);
        let response = FetchResponseV0 {
            partitions: vec![FetchPartitionResponseV0 {
                topic_id: id,
                partition: 0,
                error_code: 0,
                leader_broker_id: 2,
                leader_epoch: 4,
                log_start_offset: 0,
                high_watermark: 1,
                next_offset: 1,
                records: vec![FetchRecordV0 {
                    stream_offset: 0,
                    record: record(b"abc"),
                }],
            }],
            tags: vec![],
        };
        metrics.observe_response(
            &frame(ApiKey::Fetch, 0, vec![]),
            &frame(ApiKey::Fetch, 0, response.encode_body().unwrap()),
            Duration::from_millis(2),
        );
        let text = metrics.render();
        assert!(text.contains("astralog_fetch_records_total 1"));
        assert!(text.contains("astralog_fetch_bytes_total 3"));
    }
}
