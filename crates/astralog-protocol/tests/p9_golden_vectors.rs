use astralog_common::TopicId;
use astralog_protocol::api::{
    AlterTopicConfigRequestV0, AlterTopicConfigResponseV0, BodyCodec, CreateTopicRequestV2,
    CreateTopicResponseV2, DescribeTopicResponseV2, TopicCleanupPolicyV1, TopicCompressionV1,
    TopicPerformanceConfigV1, TopicRetentionConfigV1, TopicRetentionPatchV1,
};

fn decode_hex(input: &str) -> Vec<u8> {
    let input = input.trim();
    assert_eq!(input.len() % 2, 0);
    let (pairs, remainder) = input.as_bytes().as_chunks::<2>();
    assert!(remainder.is_empty());
    pairs
        .iter()
        .map(|pair| u8::from_str_radix(core::str::from_utf8(pair).unwrap(), 16).unwrap())
        .collect()
}

fn performance() -> TopicPerformanceConfigV1 {
    TopicPerformanceConfigV1 {
        compression: TopicCompressionV1::None,
        max_record_bytes: 1024,
        max_batch_records: 2,
        max_batch_uncompressed_bytes: 4096,
    }
}

fn retention() -> TopicRetentionConfigV1 {
    TopicRetentionConfigV1 {
        cleanup_policy: TopicCleanupPolicyV1::CompactDelete,
        retention_ms: Some(1000),
        retention_bytes: None,
        tombstone_grace_ms: 86_400_000,
    }
}

#[test]
fn p9_create_and_describe_v2_body_goldens_are_immutable() {
    let create = CreateTopicRequestV2 {
        topic_name: "p9".into(),
        partition_count: 1,
        replication_factor: 1,
        performance: performance(),
        retention: retention(),
        tags: Vec::new(),
    };
    assert_eq!(
        create.encode_body().unwrap(),
        decode_hex(
            "00027039000000010001000000000000040000000002000010000201000000000000000003e800000000000000000000000005265c000000"
        )
    );

    let create_response = CreateTopicResponseV2 {
        topic_id: TopicId::new([0x11; 16]),
        metadata_epoch: 9,
        performance: performance(),
        retention: retention(),
        tags: Vec::new(),
    };
    assert_eq!(
        create_response.encode_body().unwrap(),
        decode_hex(
            "111111111111111111111111111111110000000000000009000000000000040000000002000010000201000000000000000003e800000000000000000000000005265c000000"
        )
    );

    let describe = DescribeTopicResponseV2 {
        topic_name: "p9".into(),
        topic_id: TopicId::new([0x11; 16]),
        partition_count: 1,
        replication_factor: 1,
        metadata_epoch: 9,
        performance: performance(),
        retention: retention(),
        tags: Vec::new(),
    };
    assert_eq!(
        describe.encode_body().unwrap(),
        decode_hex(
            "00027039111111111111111111111111111111110000000100010000000000000009000000000000040000000002000010000201000000000000000003e800000000000000000000000005265c000000"
        )
    );
}

#[test]
fn p9_alter_topic_config_v0_body_goldens_are_immutable() {
    let request = AlterTopicConfigRequestV0 {
        topic_name: "p9".into(),
        retention: TopicRetentionPatchV1 {
            cleanup_policy: Some(TopicCleanupPolicyV1::Delete),
            retention_ms: Some(None),
            retention_bytes: Some(Some(2048)),
            tombstone_grace_ms: Some(45_000),
        },
        tags: Vec::new(),
    };
    assert_eq!(
        request.encode_body().unwrap(),
        decode_hex("00027039000f00020000000000000000000000000000000000000800000000000000afc80000")
    );

    let response = AlterTopicConfigResponseV0 {
        topic_id: TopicId::new([0x11; 16]),
        metadata_epoch: 10,
        retention: TopicRetentionConfigV1 {
            cleanup_policy: TopicCleanupPolicyV1::Delete,
            retention_ms: None,
            retention_bytes: Some(2048),
            tombstone_grace_ms: 45_000,
        },
        tags: Vec::new(),
    };
    assert_eq!(
        response.encode_body().unwrap(),
        decode_hex(
            "11111111111111111111111111111111000000000000000a0002000000000000000000000000000000000800000000000000afc80000"
        )
    );
}
