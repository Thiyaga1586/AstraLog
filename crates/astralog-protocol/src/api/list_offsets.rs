use astralog_common::TopicId;

use crate::{
    ProtocolError, Reader, Result, TaggedField, Writer,
    api::{
        BodyCodec, MAX_PARTITIONS_PER_REQUEST, finish_body, put_top_level_tags, read_bounded_count,
    },
};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ListOffsetsQueryV0 {
    pub topic_id: TopicId,
    pub partition: u32,
    pub leader_epoch: u64,
    pub timestamp_selector: i64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ListOffsetsRequestV0 {
    pub queries: Vec<ListOffsetsQueryV0>,
    pub tags: Vec<TaggedField>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ListOffsetsResultV0 {
    pub topic_id: TopicId,
    pub partition: u32,
    pub error_code: u16,
    pub leader_broker_id: u32,
    pub leader_epoch: u64,
    pub offset: u64,
    pub timestamp_ms: i64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ListOffsetsResponseV0 {
    pub results: Vec<ListOffsetsResultV0>,
    pub tags: Vec<TaggedField>,
}

fn validate_selector(value: i64) -> Result<()> {
    if value < -2 {
        return Err(ProtocolError::InvalidFrame(
            "invalid ListOffsets timestamp selector",
        ));
    }
    Ok(())
}

impl BodyCodec for ListOffsetsRequestV0 {
    fn encode_body(&self) -> Result<Vec<u8>> {
        if self.queries.len() > MAX_PARTITIONS_PER_REQUEST {
            return Err(ProtocolError::FieldTooLarge {
                field: "list_offsets_queries",
                len: self.queries.len(),
                max: MAX_PARTITIONS_PER_REQUEST,
            });
        }
        let mut writer = Writer::new();
        writer
            .put_u32(u32::try_from(self.queries.len()).map_err(|_| ProtocolError::LengthOverflow)?);
        for query in &self.queries {
            validate_selector(query.timestamp_selector)?;
            writer.put_topic_id(query.topic_id);
            writer.put_u32(query.partition);
            writer.put_u64(query.leader_epoch);
            writer.put_i64(query.timestamp_selector);
        }
        put_top_level_tags(&mut writer, &self.tags)?;
        Ok(writer.into_inner())
    }

    fn decode_body(bytes: &[u8]) -> Result<Self> {
        let mut reader = Reader::new(bytes);
        let count = read_bounded_count(
            &mut reader,
            MAX_PARTITIONS_PER_REQUEST,
            36,
            "list_offsets_queries",
        )?;
        let mut queries = Vec::with_capacity(count);
        for _ in 0..count {
            let query = ListOffsetsQueryV0 {
                topic_id: reader.read_topic_id()?,
                partition: reader.read_u32()?,
                leader_epoch: reader.read_u64()?,
                timestamp_selector: reader.read_i64()?,
            };
            validate_selector(query.timestamp_selector)?;
            queries.push(query);
        }
        let tags = finish_body(&mut reader)?;
        Ok(Self { queries, tags })
    }
}

impl BodyCodec for ListOffsetsResponseV0 {
    fn encode_body(&self) -> Result<Vec<u8>> {
        if self.results.len() > MAX_PARTITIONS_PER_REQUEST {
            return Err(ProtocolError::FieldTooLarge {
                field: "list_offsets_results",
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
            writer.put_u16(result.error_code);
            writer.put_u32(result.leader_broker_id);
            writer.put_u64(result.leader_epoch);
            writer.put_u64(result.offset);
            writer.put_i64(result.timestamp_ms);
        }
        put_top_level_tags(&mut writer, &self.tags)?;
        Ok(writer.into_inner())
    }

    fn decode_body(bytes: &[u8]) -> Result<Self> {
        let mut reader = Reader::new(bytes);
        let count = read_bounded_count(
            &mut reader,
            MAX_PARTITIONS_PER_REQUEST,
            50,
            "list_offsets_results",
        )?;
        let mut results = Vec::with_capacity(count);
        for _ in 0..count {
            results.push(ListOffsetsResultV0 {
                topic_id: reader.read_topic_id()?,
                partition: reader.read_u32()?,
                error_code: reader.read_u16()?,
                leader_broker_id: reader.read_u32()?,
                leader_epoch: reader.read_u64()?,
                offset: reader.read_u64()?,
                timestamp_ms: reader.read_i64()?,
            });
        }
        let tags = finish_body(&mut reader)?;
        Ok(Self { results, tags })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn list_offsets_round_trip() {
        let request = ListOffsetsRequestV0 {
            queries: vec![ListOffsetsQueryV0 {
                topic_id: TopicId::new([5; 16]),
                partition: 1,
                leader_epoch: u64::MAX,
                timestamp_selector: -2,
            }],
            tags: vec![],
        };
        assert_eq!(
            ListOffsetsRequestV0::decode_body(&request.encode_body().unwrap()).unwrap(),
            request
        );
    }
}
