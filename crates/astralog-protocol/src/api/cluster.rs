use crate::{
    ProtocolError, Reader, Result, TaggedField, UNKNOWN_BROKER_ID, Writer,
    api::{
        BodyCodec, BrokerMetadataV0, MAX_HOST_LEN, MAX_PARTITIONS_PER_REQUEST, finish_body,
        put_top_level_tags, read_bounded_count,
    },
};
use astralog_common::TopicId;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DescribeClusterRequestV0 {
    pub tags: Vec<TaggedField>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DescribeClusterResponseV0 {
    pub cluster_id: [u8; 16],
    pub metadata_epoch: u64,
    pub active_feature_level: u16,
    /// Controller node id. Zero means that this broker does not currently
    /// have a qualified controller-leader observation.
    pub controller_leader_id: u64,
    pub brokers: Vec<BrokerMetadataV0>,
    pub tags: Vec<TaggedField>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ReassignPartitionV0 {
    pub topic_id: TopicId,
    pub partition: u32,
    pub source_broker_id: u32,
    pub destination_broker_id: u32,
    /// UNKNOWN_BROKER_ID means no leadership preference.
    pub preferred_leader_broker_id: u32,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ReassignPartitionsRequestV0 {
    pub partitions: Vec<ReassignPartitionV0>,
    pub tags: Vec<TaggedField>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ReassignPartitionResultV0 {
    pub topic_id: TopicId,
    pub partition: u32,
    pub movement_id: [u8; 16],
    pub metadata_epoch: u64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ReassignPartitionsResponseV0 {
    pub results: Vec<ReassignPartitionResultV0>,
    pub tags: Vec<TaggedField>,
}

impl BodyCodec for DescribeClusterRequestV0 {
    fn encode_body(&self) -> Result<Vec<u8>> {
        let mut writer = Writer::new();
        put_top_level_tags(&mut writer, &self.tags)?;
        Ok(writer.into_inner())
    }

    fn decode_body(bytes: &[u8]) -> Result<Self> {
        let mut reader = Reader::new(bytes);
        let tags = finish_body(&mut reader)?;
        Ok(Self { tags })
    }
}

impl BodyCodec for DescribeClusterResponseV0 {
    fn encode_body(&self) -> Result<Vec<u8>> {
        if self.active_feature_level == 0 {
            return Err(ProtocolError::InvalidFrame(
                "active feature level zero is reserved",
            ));
        }
        if self.brokers.len() > MAX_PARTITIONS_PER_REQUEST {
            return Err(ProtocolError::FieldTooLarge {
                field: "cluster_brokers",
                len: self.brokers.len(),
                max: MAX_PARTITIONS_PER_REQUEST,
            });
        }
        let mut writer = Writer::new();
        writer.put_uuid(&self.cluster_id);
        writer.put_u64(self.metadata_epoch);
        writer.put_u16(self.active_feature_level);
        writer.put_u64(self.controller_leader_id);
        writer
            .put_u32(u32::try_from(self.brokers.len()).map_err(|_| ProtocolError::LengthOverflow)?);
        for broker in &self.brokers {
            writer.put_u32(broker.broker_id);
            writer.put_uuid(&broker.node_uuid);
            writer.put_u64(broker.incarnation_epoch);
            writer.put_string16(&broker.host, MAX_HOST_LEN, "broker_host")?;
            writer.put_u16(broker.port);
        }
        put_top_level_tags(&mut writer, &self.tags)?;
        Ok(writer.into_inner())
    }

    fn decode_body(bytes: &[u8]) -> Result<Self> {
        let mut reader = Reader::new(bytes);
        let cluster_id = reader.read_uuid()?;
        let metadata_epoch = reader.read_u64()?;
        let active_feature_level = reader.read_u16()?;
        if active_feature_level == 0 {
            return Err(ProtocolError::InvalidFrame(
                "active feature level zero is reserved",
            ));
        }
        let controller_leader_id = reader.read_u64()?;
        let broker_count = read_bounded_count(
            &mut reader,
            MAX_PARTITIONS_PER_REQUEST,
            32,
            "cluster_brokers",
        )?;
        let mut brokers = Vec::with_capacity(broker_count);
        for _ in 0..broker_count {
            brokers.push(BrokerMetadataV0 {
                broker_id: reader.read_u32()?,
                node_uuid: reader.read_uuid()?,
                incarnation_epoch: reader.read_u64()?,
                host: reader.read_string16(MAX_HOST_LEN, "broker_host")?,
                port: reader.read_u16()?,
            });
        }
        let tags = finish_body(&mut reader)?;
        Ok(Self {
            cluster_id,
            metadata_epoch,
            active_feature_level,
            controller_leader_id,
            brokers,
            tags,
        })
    }
}

impl BodyCodec for ReassignPartitionsRequestV0 {
    fn encode_body(&self) -> Result<Vec<u8>> {
        validate_reassignments(&self.partitions)?;
        let mut writer = Writer::new();
        writer.put_u32(
            u32::try_from(self.partitions.len()).map_err(|_| ProtocolError::LengthOverflow)?,
        );
        for assignment in &self.partitions {
            writer.put_topic_id(assignment.topic_id);
            writer.put_u32(assignment.partition);
            writer.put_u32(assignment.source_broker_id);
            writer.put_u32(assignment.destination_broker_id);
            writer.put_u32(assignment.preferred_leader_broker_id);
        }
        put_top_level_tags(&mut writer, &self.tags)?;
        Ok(writer.into_inner())
    }

    fn decode_body(bytes: &[u8]) -> Result<Self> {
        let mut reader = Reader::new(bytes);
        let count = read_bounded_count(
            &mut reader,
            MAX_PARTITIONS_PER_REQUEST,
            32,
            "reassign_partitions",
        )?;
        if count == 0 {
            return Err(ProtocolError::InvalidFrame(
                "reassignment request must contain at least one partition",
            ));
        }
        let mut partitions = Vec::with_capacity(count);
        for _ in 0..count {
            partitions.push(ReassignPartitionV0 {
                topic_id: reader.read_topic_id()?,
                partition: reader.read_u32()?,
                source_broker_id: reader.read_u32()?,
                destination_broker_id: reader.read_u32()?,
                preferred_leader_broker_id: reader.read_u32()?,
            });
        }
        validate_reassignments(&partitions)?;
        let tags = finish_body(&mut reader)?;
        Ok(Self { partitions, tags })
    }
}

impl BodyCodec for ReassignPartitionsResponseV0 {
    fn encode_body(&self) -> Result<Vec<u8>> {
        if self.results.len() > MAX_PARTITIONS_PER_REQUEST {
            return Err(ProtocolError::FieldTooLarge {
                field: "reassign_results",
                len: self.results.len(),
                max: MAX_PARTITIONS_PER_REQUEST,
            });
        }
        let mut writer = Writer::new();
        writer
            .put_u32(u32::try_from(self.results.len()).map_err(|_| ProtocolError::LengthOverflow)?);
        for result in &self.results {
            writer.put_topic_id(result.topic_id);
            writer.put_u32(result.partition);
            writer.put_uuid(&result.movement_id);
            writer.put_u64(result.metadata_epoch);
        }
        put_top_level_tags(&mut writer, &self.tags)?;
        Ok(writer.into_inner())
    }

    fn decode_body(bytes: &[u8]) -> Result<Self> {
        let mut reader = Reader::new(bytes);
        let count = read_bounded_count(
            &mut reader,
            MAX_PARTITIONS_PER_REQUEST,
            32,
            "reassign_results",
        )?;
        let mut results = Vec::with_capacity(count);
        for _ in 0..count {
            results.push(ReassignPartitionResultV0 {
                topic_id: reader.read_topic_id()?,
                partition: reader.read_u32()?,
                movement_id: reader.read_uuid()?,
                metadata_epoch: reader.read_u64()?,
            });
        }
        let tags = finish_body(&mut reader)?;
        Ok(Self { results, tags })
    }
}

fn validate_reassignments(partitions: &[ReassignPartitionV0]) -> Result<()> {
    if partitions.is_empty() {
        return Err(ProtocolError::InvalidFrame(
            "reassignment request must contain at least one partition",
        ));
    }
    if partitions.len() > MAX_PARTITIONS_PER_REQUEST {
        return Err(ProtocolError::FieldTooLarge {
            field: "reassign_partitions",
            len: partitions.len(),
            max: MAX_PARTITIONS_PER_REQUEST,
        });
    }
    let mut previous = None;
    for assignment in partitions {
        if assignment.topic_id.as_bytes() == &[0; 16]
            || assignment.source_broker_id == assignment.destination_broker_id
            || assignment.source_broker_id == UNKNOWN_BROKER_ID
            || assignment.destination_broker_id == UNKNOWN_BROKER_ID
        {
            return Err(ProtocolError::InvalidFrame(
                "invalid reassignment source/destination identity",
            ));
        }
        let key = (assignment.topic_id, assignment.partition);
        if previous.is_some_and(|value| value >= key) {
            return Err(ProtocolError::InvalidFrame(
                "reassignments must be strictly ordered and distinct",
            ));
        }
        previous = Some(key);
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn describe_cluster_round_trip() {
        let response = DescribeClusterResponseV0 {
            cluster_id: [1; 16],
            metadata_epoch: 9,
            active_feature_level: 2,
            controller_leader_id: 3,
            brokers: vec![BrokerMetadataV0 {
                broker_id: 7,
                node_uuid: [7; 16],
                incarnation_epoch: 4,
                host: "127.0.0.1".into(),
                port: 9092,
            }],
            tags: Vec::new(),
        };
        let expected = [
            0x01, 0x01, 0x01, 0x01, 0x01, 0x01, 0x01, 0x01, 0x01, 0x01, 0x01, 0x01, 0x01, 0x01,
            0x01, 0x01, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x09, 0x00, 0x02, 0x00, 0x00,
            0x00, 0x00, 0x00, 0x00, 0x00, 0x03, 0x00, 0x00, 0x00, 0x01, 0x00, 0x00, 0x00, 0x07,
            0x07, 0x07, 0x07, 0x07, 0x07, 0x07, 0x07, 0x07, 0x07, 0x07, 0x07, 0x07, 0x07, 0x07,
            0x07, 0x07, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x04, 0x00, 0x09, b'1', b'2',
            b'7', b'.', b'0', b'.', b'0', b'.', b'1', 0x23, 0x84, 0x00, 0x00,
        ];
        assert_eq!(response.encode_body().unwrap(), expected);
        assert_eq!(
            DescribeClusterResponseV0::decode_body(&response.encode_body().unwrap()).unwrap(),
            response
        );
    }

    #[test]
    fn reassignment_round_trip_and_canonical_order() {
        let request = ReassignPartitionsRequestV0 {
            partitions: vec![ReassignPartitionV0 {
                topic_id: TopicId::new([3; 16]),
                partition: 1,
                source_broker_id: 1,
                destination_broker_id: 4,
                preferred_leader_broker_id: UNKNOWN_BROKER_ID,
            }],
            tags: Vec::new(),
        };
        let expected = [
            0x00, 0x00, 0x00, 0x01, 0x03, 0x03, 0x03, 0x03, 0x03, 0x03, 0x03, 0x03, 0x03, 0x03,
            0x03, 0x03, 0x03, 0x03, 0x03, 0x03, 0x00, 0x00, 0x00, 0x01, 0x00, 0x00, 0x00, 0x01,
            0x00, 0x00, 0x00, 0x04, 0xff, 0xff, 0xff, 0xff, 0x00, 0x00,
        ];
        assert_eq!(request.encode_body().unwrap(), expected);
        assert_eq!(
            ReassignPartitionsRequestV0::decode_body(&request.encode_body().unwrap()).unwrap(),
            request
        );

        let duplicate = ReassignPartitionsRequestV0 {
            partitions: vec![request.partitions[0], request.partitions[0]],
            tags: Vec::new(),
        };
        assert!(duplicate.encode_body().is_err());
    }
}
