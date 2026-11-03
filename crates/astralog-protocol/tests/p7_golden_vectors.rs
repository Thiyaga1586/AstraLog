use astralog_common::{ProducerAuthority, ProducerEpoch, ProducerId, ProducerSequence, TopicId};
use astralog_protocol::api::{
    BodyCodec, InitProducerRequestV0, InitProducerResponseV0, ProducePartitionRequestV1,
    ProducePartitionResponseV1, ProduceRequestV1, ProduceResponseV1, WireRecordV0,
    canonical_record_set_digest,
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

fn authority() -> ProducerAuthority {
    ProducerAuthority::new(ProducerId::new(7).unwrap(), ProducerEpoch::new(2).unwrap())
}

fn record() -> WireRecordV0 {
    WireRecordV0 {
        timestamp_ms: 1,
        key: None,
        value: Some(vec![0xaa]),
        headers: vec![],
    }
}

#[test]
fn init_producer_v0_body_goldens_are_immutable() {
    let request = InitProducerRequestV0 {
        expected_authority: None,
        tags: vec![],
    };
    assert_eq!(
        request.encode_body().unwrap(),
        decode_hex("0000000000000000000000000000")
    );

    let response = InitProducerResponseV0 {
        authority: authority(),
        tags: vec![],
    };
    assert_eq!(
        response.encode_body().unwrap(),
        decode_hex("0000000000000007000000020000")
    );
}

#[test]
fn produce_v1_body_goldens_are_immutable() {
    let request = ProduceRequestV1 {
        authority: authority(),
        partitions: vec![ProducePartitionRequestV1 {
            topic_id: TopicId::new([0x11; 16]),
            partition: 3,
            leader_epoch: 4,
            base_sequence: ProducerSequence::new(5),
            records: vec![record()],
        }],
        tags: vec![],
    };
    assert_eq!(
        request.encode_body().unwrap(),
        decode_hex(
            "02000000000000000700000002000000011111111111111111111111111111111100000003000000000000000400000005000000010000000000000001ffffffff00000001aa00000000"
        )
    );

    let response = ProduceResponseV1 {
        partitions: vec![ProducePartitionResponseV1 {
            topic_id: TopicId::new([0x11; 16]),
            partition: 3,
            error_code: 0,
            base_offset: 12,
            last_offset: 12,
            leader_broker_id: 1,
            leader_epoch: 4,
        }],
        tags: vec![],
    };
    assert_eq!(
        response.encode_body().unwrap(),
        decode_hex(
            "0000000111111111111111111111111111111111000000030000000000000000000c000000000000000c0000000100000000000000040000"
        )
    );
}

#[test]
fn canonical_record_set_sha256_golden_is_immutable() {
    assert_eq!(
        canonical_record_set_digest(&[record()]).unwrap(),
        <[u8; 32]>::try_from(decode_hex(
            "090e5db72735ab045bd82af0dc1fc927465bced52a5e59e617559c3b7966af4a"
        ))
        .unwrap()
    );
}

#[test]
fn p7_public_decoders_reject_malformed_authority_ack_timestamp_and_trailing_bytes() {
    let mut sentinel_mismatch = decode_hex("0000000000000000000000010000");
    assert!(InitProducerRequestV0::decode_body(&sentinel_mismatch).is_err());

    let request = ProduceRequestV1 {
        authority: authority(),
        partitions: vec![ProducePartitionRequestV1 {
            topic_id: TopicId::new([0x11; 16]),
            partition: 3,
            leader_epoch: 4,
            base_sequence: ProducerSequence::new(5),
            records: vec![record()],
        }],
        tags: vec![],
    };
    let mut bytes = request.encode_body().unwrap();
    bytes[0] = 1;
    assert!(ProduceRequestV1::decode_body(&bytes).is_err());

    let mut bytes = request.encode_body().unwrap();
    bytes[1..9].fill(0);
    assert!(ProduceRequestV1::decode_body(&bytes).is_err());

    let mut hidden_timestamp = request;
    hidden_timestamp.partitions[0].records[0].timestamp_ms = -1;
    assert!(hidden_timestamp.encode_body().is_err());

    sentinel_mismatch.extend_from_slice(&[0]);
    assert!(InitProducerRequestV0::decode_body(&sentinel_mismatch).is_err());
}
