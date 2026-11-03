use astralog_common::TopicId;
use astralog_protocol::api::{
    BodyCodec, CommitOffsetV0, CommitOffsetsRequestV0, FetchOffsetsRequestV0, GroupEpoch, GroupId,
    HeartbeatRequestV0, JoinSubscribeRequestV0, LeaveRequestV0, MemberAuthorityV0, MemberEpoch,
    MemberId, RevokeAckRequestV0, TopicPartitionV0,
};

fn topic(byte: u8) -> TopicId {
    TopicId::new([byte; 16])
}

fn authority() -> MemberAuthorityV0 {
    MemberAuthorityV0 {
        group_id: GroupId::new("g").unwrap(),
        member_id: MemberId::new([1; 16]).unwrap(),
        group_epoch: GroupEpoch::new(2).unwrap(),
        member_epoch: MemberEpoch::new(3).unwrap(),
    }
}

#[test]
fn join_subscribe_v0_request_golden_is_immutable() {
    let request = JoinSubscribeRequestV0 {
        group_id: GroupId::new("g").unwrap(),
        member_id: [0; 16],
        instance_id: None,
        expected_group_epoch: 0,
        expected_member_epoch: 0,
        session_timeout_ms: 6_000,
        subscriptions: vec![topic(2)],
        tags: vec![],
    };
    let expected = vec![
        0, 1, b'g', 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
        0, 0, 0, 0, 0, 0, 0, 0, 0, 0x17, 0x70, 0, 0, 0, 1, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2,
        2, 2, 2, 0, 0,
    ];
    assert_eq!(request.encode_body().unwrap(), expected);
}

#[test]
fn heartbeat_v0_request_golden_is_immutable() {
    let request = HeartbeatRequestV0 {
        authority: authority(),
        tags: vec![],
    };
    let expected = vec![
        0, 1, b'g', 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 0, 0, 0, 0, 0, 0, 0, 2, 0, 0,
        0, 0, 0, 0, 0, 3, 0, 0,
    ];
    assert_eq!(request.encode_body().unwrap(), expected);
}

#[test]
fn revoke_ack_v0_request_golden_is_immutable() {
    let request = RevokeAckRequestV0 {
        authority: authority(),
        revoked: vec![TopicPartitionV0 {
            topic_id: topic(2),
            partition: 4,
        }],
        tags: vec![],
    };
    let mut expected = HeartbeatRequestV0 {
        authority: authority(),
        tags: vec![],
    }
    .encode_body()
    .unwrap();
    expected.truncate(expected.len() - 2);
    expected.extend_from_slice(&[0, 0, 0, 1]);
    expected.extend_from_slice(&[2; 16]);
    expected.extend_from_slice(&[0, 0, 0, 4]);
    expected.extend_from_slice(&[0, 0]);
    assert_eq!(request.encode_body().unwrap(), expected);
}

#[test]
fn leave_v0_request_golden_is_immutable() {
    let request = LeaveRequestV0 {
        authority: authority(),
        tags: vec![],
    };
    let heartbeat = HeartbeatRequestV0 {
        authority: authority(),
        tags: vec![],
    };
    assert_eq!(
        request.encode_body().unwrap(),
        heartbeat.encode_body().unwrap()
    );
}

#[test]
fn commit_offsets_v0_request_golden_is_immutable() {
    let request = CommitOffsetsRequestV0 {
        authority: authority(),
        offsets: vec![CommitOffsetV0 {
            topic_id: topic(2),
            partition: 4,
            next_offset: 5,
            metadata: "m".into(),
        }],
        tags: vec![],
    };
    let mut expected = HeartbeatRequestV0 {
        authority: authority(),
        tags: vec![],
    }
    .encode_body()
    .unwrap();
    expected.truncate(expected.len() - 2);
    expected.extend_from_slice(&[0, 0, 0, 1]);
    expected.extend_from_slice(&[2; 16]);
    expected.extend_from_slice(&[0, 0, 0, 4]);
    expected.extend_from_slice(&[0, 0, 0, 0, 0, 0, 0, 5]);
    expected.extend_from_slice(&[0, 1, b'm']);
    expected.extend_from_slice(&[0, 0]);
    assert_eq!(request.encode_body().unwrap(), expected);
}

#[test]
fn fetch_offsets_v0_request_golden_is_immutable() {
    let request = FetchOffsetsRequestV0 {
        group_id: GroupId::new("g").unwrap(),
        partitions: vec![TopicPartitionV0 {
            topic_id: topic(2),
            partition: 4,
        }],
        tags: vec![],
    };
    let expected = vec![
        0, 1, b'g', 0, 0, 0, 1, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 0, 0, 0, 4, 0, 0,
    ];
    assert_eq!(request.encode_body().unwrap(), expected);
}
