use astralog_common::TopicId;

use crate::{
    ProtocolError, Reader, Result, TaggedField, Writer,
    api::{
        BodyCodec, MAX_HOST_LEN, MAX_PARTITIONS_PER_REQUEST, MAX_TOPIC_NAME_LEN,
        MAX_TOPICS_PER_REQUEST, finish_body, put_top_level_tags, read_bounded_count,
    },
};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MetadataRequestV0 {
    pub all_topics: bool,
    pub topic_names: Vec<String>,
    pub tags: Vec<TaggedField>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BrokerMetadataV0 {
    pub broker_id: u32,
    pub node_uuid: [u8; 16],
    pub incarnation_epoch: u64,
    pub host: String,
    pub port: u16,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MetadataPartitionV0 {
    pub partition: u32,
    pub partition_error: u16,
    pub leader_broker_id: u32,
    pub leader_epoch: u64,
    pub replica_broker_ids: Vec<u32>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MetadataTopicV0 {
    pub topic_error: u16,
    pub topic_name: String,
    pub topic_id: TopicId,
    pub partition_count: u32,
    pub replication_factor: u16,
    pub partitions: Vec<MetadataPartitionV0>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MetadataResponseV0 {
    pub cluster_id: [u8; 16],
    pub metadata_epoch: u64,
    pub brokers: Vec<BrokerMetadataV0>,
    pub topics: Vec<MetadataTopicV0>,
    pub tags: Vec<TaggedField>,
}

impl BodyCodec for MetadataRequestV0 {
    fn encode_body(&self) -> Result<Vec<u8>> {
        if self.all_topics && !self.topic_names.is_empty() {
            return Err(ProtocolError::InvalidFrame(
                "all_topics request must not include explicit topic names",
            ));
        }
        if !self.all_topics && self.topic_names.is_empty() {
            return Err(ProtocolError::InvalidFrame(
                "explicit metadata request requires at least one topic",
            ));
        }
        if self.topic_names.len() > MAX_TOPICS_PER_REQUEST {
            return Err(ProtocolError::FieldTooLarge {
                field: "topic_names",
                len: self.topic_names.len(),
                max: MAX_TOPICS_PER_REQUEST,
            });
        }

        let mut writer = Writer::new();
        writer.put_bool(self.all_topics);
        writer.put_u32(
            u32::try_from(self.topic_names.len()).map_err(|_| ProtocolError::LengthOverflow)?,
        );
        for name in &self.topic_names {
            writer.put_string16(name, MAX_TOPIC_NAME_LEN, "topic_name")?;
        }
        put_top_level_tags(&mut writer, &self.tags)?;
        Ok(writer.into_inner())
    }

    fn decode_body(bytes: &[u8]) -> Result<Self> {
        let mut reader = Reader::new(bytes);
        let all_topics = reader.read_bool()?;
        let count = read_bounded_count(&mut reader, MAX_TOPICS_PER_REQUEST, 2, "topic_names")?;
        let mut topic_names = Vec::with_capacity(count);
        for _ in 0..count {
            topic_names.push(reader.read_string16(MAX_TOPIC_NAME_LEN, "topic_name")?);
        }
        if all_topics && !topic_names.is_empty() {
            return Err(ProtocolError::InvalidFrame(
                "all_topics request must not include explicit topic names",
            ));
        }
        if !all_topics && topic_names.is_empty() {
            return Err(ProtocolError::InvalidFrame(
                "explicit metadata request requires at least one topic",
            ));
        }
        let tags = finish_body(&mut reader)?;
        Ok(Self {
            all_topics,
            topic_names,
            tags,
        })
    }
}

impl BodyCodec for MetadataResponseV0 {
    fn encode_body(&self) -> Result<Vec<u8>> {
        if self.brokers.len() > MAX_PARTITIONS_PER_REQUEST {
            return Err(ProtocolError::FieldTooLarge {
                field: "brokers",
                len: self.brokers.len(),
                max: MAX_PARTITIONS_PER_REQUEST,
            });
        }
        if self.topics.len() > MAX_TOPICS_PER_REQUEST {
            return Err(ProtocolError::FieldTooLarge {
                field: "topics",
                len: self.topics.len(),
                max: MAX_TOPICS_PER_REQUEST,
            });
        }

        let mut writer = Writer::new();
        writer.put_uuid(&self.cluster_id);
        writer.put_u64(self.metadata_epoch);
        writer
            .put_u32(u32::try_from(self.brokers.len()).map_err(|_| ProtocolError::LengthOverflow)?);
        for broker in &self.brokers {
            writer.put_u32(broker.broker_id);
            writer.put_uuid(&broker.node_uuid);
            writer.put_u64(broker.incarnation_epoch);
            writer.put_string16(&broker.host, MAX_HOST_LEN, "broker_host")?;
            writer.put_u16(broker.port);
        }

        writer
            .put_u32(u32::try_from(self.topics.len()).map_err(|_| ProtocolError::LengthOverflow)?);
        for topic in &self.topics {
            if topic.partitions.len() > MAX_PARTITIONS_PER_REQUEST {
                return Err(ProtocolError::FieldTooLarge {
                    field: "topic_partitions",
                    len: topic.partitions.len(),
                    max: MAX_PARTITIONS_PER_REQUEST,
                });
            }
            writer.put_u16(topic.topic_error);
            writer.put_string16(&topic.topic_name, MAX_TOPIC_NAME_LEN, "topic_name")?;
            writer.put_topic_id(topic.topic_id);
            writer.put_u32(topic.partition_count);
            writer.put_u16(topic.replication_factor);
            writer.put_u32(
                u32::try_from(topic.partitions.len()).map_err(|_| ProtocolError::LengthOverflow)?,
            );
            for partition in &topic.partitions {
                writer.put_u32(partition.partition);
                writer.put_u16(partition.partition_error);
                writer.put_u32(partition.leader_broker_id);
                writer.put_u64(partition.leader_epoch);
                writer.put_u16(
                    u16::try_from(partition.replica_broker_ids.len())
                        .map_err(|_| ProtocolError::LengthOverflow)?,
                );
                for broker_id in &partition.replica_broker_ids {
                    writer.put_u32(*broker_id);
                }
            }
        }
        put_top_level_tags(&mut writer, &self.tags)?;
        Ok(writer.into_inner())
    }

    fn decode_body(bytes: &[u8]) -> Result<Self> {
        let mut reader = Reader::new(bytes);
        let cluster_id = reader.read_uuid()?;
        let metadata_epoch = reader.read_u64()?;

        let broker_count =
            read_bounded_count(&mut reader, MAX_PARTITIONS_PER_REQUEST, 32, "brokers")?;
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

        let topic_count = read_bounded_count(&mut reader, MAX_TOPICS_PER_REQUEST, 28, "topics")?;
        let mut topics = Vec::with_capacity(topic_count);
        for _ in 0..topic_count {
            let topic_error = reader.read_u16()?;
            let topic_name = reader.read_string16(MAX_TOPIC_NAME_LEN, "topic_name")?;
            let topic_id = reader.read_topic_id()?;
            let partition_count = reader.read_u32()?;
            let replication_factor = reader.read_u16()?;
            let metadata_partition_count = read_bounded_count(
                &mut reader,
                MAX_PARTITIONS_PER_REQUEST,
                20,
                "topic_partitions",
            )?;
            let mut partitions = Vec::with_capacity(metadata_partition_count);
            for _ in 0..metadata_partition_count {
                let partition = reader.read_u32()?;
                let partition_error = reader.read_u16()?;
                let leader_broker_id = reader.read_u32()?;
                let leader_epoch = reader.read_u64()?;
                let replica_count = usize::from(reader.read_u16()?);
                if replica_count > MAX_PARTITIONS_PER_REQUEST
                    || replica_count > reader.remaining() / 4
                {
                    return Err(ProtocolError::InvalidFrame(
                        "replica count cannot fit remaining body bytes",
                    ));
                }
                let mut replica_broker_ids = Vec::with_capacity(replica_count);
                for _ in 0..replica_count {
                    replica_broker_ids.push(reader.read_u32()?);
                }
                partitions.push(MetadataPartitionV0 {
                    partition,
                    partition_error,
                    leader_broker_id,
                    leader_epoch,
                    replica_broker_ids,
                });
            }
            topics.push(MetadataTopicV0 {
                topic_error,
                topic_name,
                topic_id,
                partition_count,
                replication_factor,
                partitions,
            });
        }

        let tags = finish_body(&mut reader)?;
        Ok(Self {
            cluster_id,
            metadata_epoch,
            brokers,
            topics,
            tags,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn metadata_round_trip() {
        let response = MetadataResponseV0 {
            cluster_id: [1; 16],
            metadata_epoch: 9,
            brokers: vec![BrokerMetadataV0 {
                broker_id: 1,
                node_uuid: [2; 16],
                incarnation_epoch: 3,
                host: "127.0.0.1".into(),
                port: 9092,
            }],
            topics: vec![MetadataTopicV0 {
                topic_error: 0,
                topic_name: "orders".into(),
                topic_id: TopicId::new([3; 16]),
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
            tags: vec![],
        };
        assert_eq!(
            MetadataResponseV0::decode_body(&response.encode_body().unwrap()).unwrap(),
            response
        );
    }
}
