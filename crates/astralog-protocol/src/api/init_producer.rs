use astralog_common::{ProducerAuthority, ProducerEpoch, ProducerId};

use crate::{
    ProtocolError, Reader, Result, TaggedField, Writer,
    api::{BodyCodec, finish_body, put_top_level_tags},
};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct InitProducerRequestV0 {
    pub expected_authority: Option<ProducerAuthority>,
    pub tags: Vec<TaggedField>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct InitProducerResponseV0 {
    pub authority: ProducerAuthority,
    pub tags: Vec<TaggedField>,
}

impl BodyCodec for InitProducerRequestV0 {
    fn encode_body(&self) -> Result<Vec<u8>> {
        let mut writer = Writer::new();
        match self.expected_authority {
            None => {
                writer.put_u64(0);
                writer.put_u32(0);
            }
            Some(authority) => {
                writer.put_u64(authority.producer_id.get());
                writer.put_u32(authority.producer_epoch.get());
            }
        }
        put_top_level_tags(&mut writer, &self.tags)?;
        Ok(writer.into_inner())
    }

    fn decode_body(bytes: &[u8]) -> Result<Self> {
        let mut reader = Reader::new(bytes);
        let producer_id = reader.read_u64()?;
        let producer_epoch = reader.read_u32()?;
        let expected_authority = match (producer_id, producer_epoch) {
            (0, 0) => None,
            (0, _) | (_, 0) => {
                return Err(ProtocolError::InvalidFrame(
                    "InitProducer producer_id and expected_epoch must both be zero or both non-zero",
                ));
            }
            (producer_id, producer_epoch) => Some(ProducerAuthority::new(
                ProducerId::new(producer_id).ok_or(ProtocolError::InvalidFrame(
                    "InitProducer producer id zero is reserved",
                ))?,
                ProducerEpoch::new(producer_epoch).ok_or(ProtocolError::InvalidFrame(
                    "InitProducer producer epoch zero is reserved",
                ))?,
            )),
        };
        let tags = finish_body(&mut reader)?;
        Ok(Self {
            expected_authority,
            tags,
        })
    }
}

impl BodyCodec for InitProducerResponseV0 {
    fn encode_body(&self) -> Result<Vec<u8>> {
        let mut writer = Writer::new();
        writer.put_u64(self.authority.producer_id.get());
        writer.put_u32(self.authority.producer_epoch.get());
        put_top_level_tags(&mut writer, &self.tags)?;
        Ok(writer.into_inner())
    }

    fn decode_body(bytes: &[u8]) -> Result<Self> {
        let mut reader = Reader::new(bytes);
        let producer_id = ProducerId::new(reader.read_u64()?).ok_or(
            ProtocolError::InvalidFrame("InitProducer response producer id zero is reserved"),
        )?;
        let producer_epoch = ProducerEpoch::new(reader.read_u32()?).ok_or(
            ProtocolError::InvalidFrame("InitProducer response producer epoch zero is reserved"),
        )?;
        let tags = finish_body(&mut reader)?;
        Ok(Self {
            authority: ProducerAuthority::new(producer_id, producer_epoch),
            tags,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn request_round_trip_and_sentinel_forms_are_canonical() {
        let allocate = InitProducerRequestV0 {
            expected_authority: None,
            tags: vec![],
        };
        let bytes = allocate.encode_body().unwrap();
        assert_eq!(
            InitProducerRequestV0::decode_body(&bytes).unwrap(),
            allocate
        );

        let bump = InitProducerRequestV0 {
            expected_authority: Some(ProducerAuthority::new(
                ProducerId::new(7).unwrap(),
                ProducerEpoch::new(3).unwrap(),
            )),
            tags: vec![],
        };
        let bytes = bump.encode_body().unwrap();
        assert_eq!(InitProducerRequestV0::decode_body(&bytes).unwrap(), bump);

        let mut malformed = Vec::new();
        malformed.extend_from_slice(&0_u64.to_be_bytes());
        malformed.extend_from_slice(&1_u32.to_be_bytes());
        malformed.extend_from_slice(&0_u16.to_be_bytes());
        assert!(InitProducerRequestV0::decode_body(&malformed).is_err());
    }

    #[test]
    fn response_requires_nonzero_authority() {
        let response = InitProducerResponseV0 {
            authority: ProducerAuthority::new(
                ProducerId::new(9).unwrap(),
                ProducerEpoch::new(2).unwrap(),
            ),
            tags: vec![],
        };
        let bytes = response.encode_body().unwrap();
        assert_eq!(
            InitProducerResponseV0::decode_body(&bytes).unwrap(),
            response
        );

        let mut malformed = Vec::new();
        malformed.extend_from_slice(&0_u64.to_be_bytes());
        malformed.extend_from_slice(&1_u32.to_be_bytes());
        malformed.extend_from_slice(&0_u16.to_be_bytes());
        assert!(InitProducerResponseV0::decode_body(&malformed).is_err());
    }
}
