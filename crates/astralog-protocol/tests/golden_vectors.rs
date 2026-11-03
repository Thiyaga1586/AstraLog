use astralog_common::TopicId;
use astralog_protocol::{
    ClientHello, FrameKind, HelloStatus, MAX_WIRE_FRAME_LEN_V1, ServerHello, TaggedField,
    WireFrame,
    api::{
        AckMode, ApiVersionRange, ApiVersionsRequestV0, ApiVersionsResponseV0, BodyCodec,
        BrokerMetadataV0, CreateTopicRequestV0, CreateTopicResponseV0, DeleteTopicRequestV0,
        DeleteTopicResponseV0, DescribeTopicRequestV0, DescribeTopicResponseV0, FeatureRange,
        FetchPartitionRequestV0, FetchPartitionResponseV0, FetchRecordV0, FetchRequestV0,
        FetchResponseV0, IsolationLevel, ListOffsetsQueryV0, ListOffsetsRequestV0,
        ListOffsetsResponseV0, ListOffsetsResultV0, MetadataPartitionV0, MetadataRequestV0,
        MetadataResponseV0, MetadataTopicV0, ProducePartitionRequestV0, ProducePartitionResponseV0,
        ProduceRequestV0, ProduceResponseV0, WireHeaderV0, WireRecordV0,
    },
    encode_frame, murmur3_x86_32, partition_for_key,
};

fn decode_hex(input: &str) -> Vec<u8> {
    let input = input.trim();
    assert_eq!(input.len() % 2, 0);
    let (pairs, remainder) = input.as_bytes().as_chunks::<2>();
    assert!(remainder.is_empty());
    pairs
        .iter()
        .map(|pair| {
            let text = core::str::from_utf8(pair).unwrap();
            u8::from_str_radix(text, 16).unwrap()
        })
        .collect()
}

fn request_frame(api_key: u16, correlation_id: u64, timeout_ms: u32, body: Vec<u8>) -> WireFrame {
    WireFrame {
        kind: FrameKind::Request,
        flags: 0,
        api_key,
        api_version: 0,
        error_code: 0,
        correlation_id,
        timeout_ms,
        throttle_ms: 0,
        header_tags: Vec::new(),
        body,
    }
}

fn response_frame(api_key: u16, correlation_id: u64, error_code: u16, body: Vec<u8>) -> WireFrame {
    WireFrame {
        kind: FrameKind::Response,
        flags: 0,
        api_key,
        api_version: 0,
        error_code,
        correlation_id,
        timeout_ms: 0,
        throttle_ms: 0,
        header_tags: Vec::new(),
        body,
    }
}

fn assert_golden(actual: &[u8], expected_hex: &str) {
    assert_eq!(actual, decode_hex(expected_hex));
}

#[test]
fn hello_v1_goldens_are_immutable() {
    assert_golden(
        &ClientHello::v1().encode().unwrap(),
        include_str!("golden/client_hello_v1.hex"),
    );
    assert_golden(
        &ServerHello {
            selected_wire_version: 1,
            status: HelloStatus::Ok,
        }
        .encode(),
        include_str!("golden/server_hello_v1.hex"),
    );
}

#[test]
fn api_versions_request_v0_golden_is_immutable() {
    let request = ApiVersionsRequestV0 {
        client_software_name: "astralog-client".into(),
        client_software_version: "0.1.0".into(),
        tags: vec![],
    };
    let mut frame = request_frame(0, 1, 5_000, request.encode_body().unwrap());
    frame.header_tags = vec![TaggedField {
        id: 1,
        value: b"golden-client".to_vec(),
    }];
    assert_golden(
        &encode_frame(&frame, MAX_WIRE_FRAME_LEN_V1).unwrap(),
        include_str!("golden/api_versions_request_v0.hex"),
    );
}

#[test]
fn metadata_request_v0_golden_is_immutable() {
    let request = MetadataRequestV0 {
        all_topics: false,
        topic_names: vec!["orders".into()],
        tags: vec![],
    };
    assert_golden(
        &encode_frame(
            &request_frame(2, 2, 5_000, request.encode_body().unwrap()),
            MAX_WIRE_FRAME_LEN_V1,
        )
        .unwrap(),
        include_str!("golden/metadata_request_v0.hex"),
    );
}

#[test]
fn produce_request_v0_golden_is_immutable() {
    let topic_id = TopicId::new([0x11; 16]);
    let request = ProduceRequestV0 {
        acks: AckMode::Quorum,
        partitions: vec![ProducePartitionRequestV0 {
            topic_id,
            partition: 2,
            leader_epoch: u64::MAX,
            records: vec![WireRecordV0 {
                timestamp_ms: -1,
                key: Some(b"order-7".to_vec()),
                value: Some(br#"{"total":42}"#.to_vec()),
                headers: vec![WireHeaderV0 {
                    key: "trace-id".into(),
                    value: Some(vec![1, 2, 3]),
                }],
            }],
        }],
        tags: vec![],
    };
    assert_golden(
        &encode_frame(
            &request_frame(10, 3, 10_000, request.encode_body().unwrap()),
            MAX_WIRE_FRAME_LEN_V1,
        )
        .unwrap(),
        include_str!("golden/produce_request_v0.hex"),
    );
}

#[test]
fn fetch_request_v0_golden_is_immutable() {
    let request = FetchRequestV0 {
        isolation: IsolationLevel::ReadCommitted,
        min_bytes: 1,
        max_wait_ms: 500,
        partitions: vec![FetchPartitionRequestV0 {
            topic_id: TopicId::new([0x11; 16]),
            partition: 2,
            leader_epoch: u64::MAX,
            fetch_offset: 0,
            max_bytes: 4096,
            max_records: 100,
        }],
        tags: vec![],
    };
    assert_golden(
        &encode_frame(
            &request_frame(11, 4, 5_000, request.encode_body().unwrap()),
            MAX_WIRE_FRAME_LEN_V1,
        )
        .unwrap(),
        include_str!("golden/fetch_request_v0.hex"),
    );
}

#[test]
fn list_offsets_request_v0_golden_is_immutable() {
    let request = ListOffsetsRequestV0 {
        queries: vec![ListOffsetsQueryV0 {
            topic_id: TopicId::new([0x11; 16]),
            partition: 2,
            leader_epoch: u64::MAX,
            timestamp_selector: -2,
        }],
        tags: vec![],
    };
    assert_golden(
        &encode_frame(
            &request_frame(12, 5, 5_000, request.encode_body().unwrap()),
            MAX_WIRE_FRAME_LEN_V1,
        )
        .unwrap(),
        include_str!("golden/list_offsets_request_v0.hex"),
    );
}

#[test]
fn topic_admin_request_v0_goldens_are_immutable() {
    let create = CreateTopicRequestV0 {
        topic_name: "orders.v1".into(),
        partition_count: 3,
        replication_factor: 1,
        tags: Vec::new(),
    };
    assert_golden(
        &encode_frame(
            &request_frame(3, 6, 5_000, create.encode_body().unwrap()),
            MAX_WIRE_FRAME_LEN_V1,
        )
        .unwrap(),
        include_str!("golden/create_topic_request_v0.hex"),
    );

    let delete = DeleteTopicRequestV0 {
        topic_name: "orders.v1".into(),
        tags: Vec::new(),
    };
    assert_golden(
        &encode_frame(
            &request_frame(4, 7, 5_000, delete.encode_body().unwrap()),
            MAX_WIRE_FRAME_LEN_V1,
        )
        .unwrap(),
        include_str!("golden/delete_topic_request_v0.hex"),
    );

    let describe = DescribeTopicRequestV0 {
        topic_name: "orders.v1".into(),
        tags: Vec::new(),
    };
    assert_golden(
        &encode_frame(
            &request_frame(5, 8, 5_000, describe.encode_body().unwrap()),
            MAX_WIRE_FRAME_LEN_V1,
        )
        .unwrap(),
        include_str!("golden/describe_topic_request_v0.hex"),
    );
}

#[test]
fn api_versions_and_metadata_response_v0_goldens_are_immutable() {
    let api_versions = ApiVersionsResponseV0 {
        server_software_name: "astralog-broker".into(),
        server_software_version: "0.1.0".into(),
        selected_wire_version: 1,
        max_frame_len: 16 * 1024 * 1024,
        apis: vec![
            ApiVersionRange {
                api_key: 0,
                min_api_version: 0,
                max_api_version: 0,
            },
            ApiVersionRange {
                api_key: 2,
                min_api_version: 0,
                max_api_version: 0,
            },
        ],
        features: vec![FeatureRange {
            feature_key: 1,
            min_supported_version: 1,
            max_supported_version: 1,
            active_version: 1,
        }],
        tags: Vec::new(),
    };
    assert_golden(
        &encode_frame(
            &response_frame(0, 1, 0, api_versions.encode_body().unwrap()),
            MAX_WIRE_FRAME_LEN_V1,
        )
        .unwrap(),
        include_str!("golden/api_versions_response_v0.hex"),
    );

    let metadata = MetadataResponseV0 {
        cluster_id: [0xAA; 16],
        metadata_epoch: 9,
        brokers: vec![BrokerMetadataV0 {
            broker_id: 1,
            node_uuid: [0xBB; 16],
            incarnation_epoch: 4,
            host: "127.0.0.1".into(),
            port: 9092,
        }],
        topics: vec![MetadataTopicV0 {
            topic_error: 0,
            topic_name: "orders.v1".into(),
            topic_id: TopicId::new([0x22; 16]),
            partition_count: 1,
            replication_factor: 1,
            partitions: vec![MetadataPartitionV0 {
                partition: 0,
                partition_error: 0,
                leader_broker_id: 1,
                leader_epoch: 0,
                replica_broker_ids: vec![1],
            }],
        }],
        tags: Vec::new(),
    };
    assert_golden(
        &encode_frame(
            &response_frame(2, 2, 0, metadata.encode_body().unwrap()),
            MAX_WIRE_FRAME_LEN_V1,
        )
        .unwrap(),
        include_str!("golden/metadata_response_v0.hex"),
    );
}

#[test]
fn topic_admin_response_v0_goldens_are_immutable() {
    let topic_id = TopicId::new([0x22; 16]);
    let create = CreateTopicResponseV0 {
        topic_id,
        metadata_epoch: 10,
        tags: Vec::new(),
    };
    let delete = DeleteTopicResponseV0 {
        deleted_topic_id: topic_id,
        metadata_epoch: 11,
        tags: Vec::new(),
    };
    let describe = DescribeTopicResponseV0 {
        topic_name: "orders.v1".into(),
        topic_id,
        partition_count: 3,
        replication_factor: 1,
        metadata_epoch: 10,
        tags: Vec::new(),
    };
    for (actual, golden) in [
        (
            encode_frame(
                &response_frame(3, 6, 0, create.encode_body().unwrap()),
                MAX_WIRE_FRAME_LEN_V1,
            )
            .unwrap(),
            include_str!("golden/create_topic_response_v0.hex"),
        ),
        (
            encode_frame(
                &response_frame(4, 7, 0, delete.encode_body().unwrap()),
                MAX_WIRE_FRAME_LEN_V1,
            )
            .unwrap(),
            include_str!("golden/delete_topic_response_v0.hex"),
        ),
        (
            encode_frame(
                &response_frame(5, 8, 0, describe.encode_body().unwrap()),
                MAX_WIRE_FRAME_LEN_V1,
            )
            .unwrap(),
            include_str!("golden/describe_topic_response_v0.hex"),
        ),
    ] {
        assert_golden(&actual, golden);
    }
}

#[test]
fn streaming_and_error_response_v0_goldens_are_immutable() {
    let topic_id = TopicId::new([0x22; 16]);
    let produce = ProduceResponseV0 {
        partitions: vec![ProducePartitionResponseV0 {
            topic_id,
            partition: 0,
            error_code: 0,
            base_offset: 12,
            last_offset: 13,
            leader_broker_id: 1,
            leader_epoch: 0,
        }],
        tags: Vec::new(),
    };
    assert_golden(
        &encode_frame(
            &response_frame(10, 3, 0, produce.encode_body().unwrap()),
            MAX_WIRE_FRAME_LEN_V1,
        )
        .unwrap(),
        include_str!("golden/produce_response_v0.hex"),
    );

    let fetch = FetchResponseV0 {
        partitions: vec![FetchPartitionResponseV0 {
            topic_id,
            partition: 0,
            error_code: 0,
            leader_broker_id: 1,
            leader_epoch: 0,
            log_start_offset: 0,
            high_watermark: 14,
            next_offset: 14,
            records: vec![FetchRecordV0 {
                stream_offset: 13,
                record: WireRecordV0 {
                    timestamp_ms: 1_700_000_000_000,
                    key: Some(b"k".to_vec()),
                    value: Some(b"v".to_vec()),
                    headers: Vec::new(),
                },
            }],
        }],
        tags: Vec::new(),
    };
    assert_golden(
        &encode_frame(
            &response_frame(11, 4, 0, fetch.encode_body().unwrap()),
            MAX_WIRE_FRAME_LEN_V1,
        )
        .unwrap(),
        include_str!("golden/fetch_response_v0.hex"),
    );

    let offsets = ListOffsetsResponseV0 {
        results: vec![ListOffsetsResultV0 {
            topic_id,
            partition: 0,
            error_code: 0,
            leader_broker_id: 1,
            leader_epoch: 0,
            offset: 14,
            timestamp_ms: -1,
        }],
        tags: Vec::new(),
    };
    assert_golden(
        &encode_frame(
            &response_frame(12, 5, 0, offsets.encode_body().unwrap()),
            MAX_WIRE_FRAME_LEN_V1,
        )
        .unwrap(),
        include_str!("golden/list_offsets_response_v0.hex"),
    );

    assert_golden(
        &encode_frame(
            &response_frame(2, 77, 10, Vec::new()),
            MAX_WIRE_FRAME_LEN_V1,
        )
        .unwrap(),
        include_str!("golden/error_response_v0.hex"),
    );
}

#[test]
fn keyed_partition_vectors_v1_are_immutable() {
    let vectors = include_str!("golden/partition_vectors_v1.txt");
    for line in vectors.lines() {
        let line = line.trim();
        if line.is_empty() || line.starts_with('#') {
            continue;
        }
        let fields: Vec<_> = line.split_whitespace().collect();
        assert_eq!(fields.len(), 6);
        let key = if fields[0] == "-" {
            Vec::new()
        } else {
            decode_hex(fields[0])
        };
        let expected_hash = u32::from_str_radix(fields[1], 16).unwrap();
        let expected_positive = fields[2].parse::<u32>().unwrap();
        let p3 = fields[3].parse::<u32>().unwrap();
        let p7 = fields[4].parse::<u32>().unwrap();
        let p16 = fields[5].parse::<u32>().unwrap();

        let hash = murmur3_x86_32(&key, 0);
        assert_eq!(hash, expected_hash);
        assert_eq!(hash & 0x7FFF_FFFF, expected_positive);
        assert_eq!(partition_for_key(&key, 3).unwrap(), p3);
        assert_eq!(partition_for_key(&key, 7).unwrap(), p7);
        assert_eq!(partition_for_key(&key, 16).unwrap(), p16);
    }
}
