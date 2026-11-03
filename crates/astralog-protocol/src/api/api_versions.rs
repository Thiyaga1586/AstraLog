use crate::{
    ProtocolError, Reader, Result, TaggedField, Writer,
    api::{
        BodyCodec, MAX_CLIENT_SOFTWARE_NAME_LEN, MAX_CLIENT_SOFTWARE_VERSION_LEN,
        MAX_SERVER_SOFTWARE_NAME_LEN, MAX_SERVER_SOFTWARE_VERSION_LEN, finish_body,
        put_top_level_tags,
    },
};

const MAX_ADVERTISED_APIS: usize = 256;
const MAX_ADVERTISED_FEATURES: usize = 256;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ApiVersionsRequestV0 {
    pub client_software_name: String,
    pub client_software_version: String,
    pub tags: Vec<TaggedField>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ApiVersionRange {
    pub api_key: u16,
    pub min_api_version: u16,
    pub max_api_version: u16,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct FeatureRange {
    pub feature_key: u16,
    pub min_supported_version: u16,
    pub max_supported_version: u16,
    pub active_version: u16,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ApiVersionsResponseV0 {
    pub server_software_name: String,
    pub server_software_version: String,
    pub selected_wire_version: u16,
    pub max_frame_len: u32,
    pub apis: Vec<ApiVersionRange>,
    pub features: Vec<FeatureRange>,
    pub tags: Vec<TaggedField>,
}

impl BodyCodec for ApiVersionsRequestV0 {
    fn encode_body(&self) -> Result<Vec<u8>> {
        let mut writer = Writer::new();
        writer.put_string16(
            &self.client_software_name,
            MAX_CLIENT_SOFTWARE_NAME_LEN,
            "client_software_name",
        )?;
        writer.put_string16(
            &self.client_software_version,
            MAX_CLIENT_SOFTWARE_VERSION_LEN,
            "client_software_version",
        )?;
        put_top_level_tags(&mut writer, &self.tags)?;
        Ok(writer.into_inner())
    }

    fn decode_body(bytes: &[u8]) -> Result<Self> {
        let mut reader = Reader::new(bytes);
        let client_software_name =
            reader.read_string16(MAX_CLIENT_SOFTWARE_NAME_LEN, "client_software_name")?;
        let client_software_version =
            reader.read_string16(MAX_CLIENT_SOFTWARE_VERSION_LEN, "client_software_version")?;
        let tags = finish_body(&mut reader)?;
        Ok(Self {
            client_software_name,
            client_software_version,
            tags,
        })
    }
}

impl BodyCodec for ApiVersionsResponseV0 {
    fn encode_body(&self) -> Result<Vec<u8>> {
        if self.apis.len() > MAX_ADVERTISED_APIS {
            return Err(ProtocolError::FieldTooLarge {
                field: "advertised_apis",
                len: self.apis.len(),
                max: MAX_ADVERTISED_APIS,
            });
        }
        if self.features.len() > MAX_ADVERTISED_FEATURES {
            return Err(ProtocolError::FieldTooLarge {
                field: "advertised_features",
                len: self.features.len(),
                max: MAX_ADVERTISED_FEATURES,
            });
        }
        for api in &self.apis {
            if api.min_api_version > api.max_api_version {
                return Err(ProtocolError::InvalidFrame(
                    "ApiVersions API minimum exceeds maximum",
                ));
            }
        }
        for feature in &self.features {
            if feature.min_supported_version > feature.max_supported_version {
                return Err(ProtocolError::InvalidFrame(
                    "ApiVersions feature minimum exceeds maximum",
                ));
            }
            if feature.active_version != u16::MAX
                && !(feature.min_supported_version..=feature.max_supported_version)
                    .contains(&feature.active_version)
            {
                return Err(ProtocolError::InvalidFrame(
                    "ApiVersions active feature version is outside supported range",
                ));
            }
        }
        let mut writer = Writer::new();
        writer.put_string16(
            &self.server_software_name,
            MAX_SERVER_SOFTWARE_NAME_LEN,
            "server_software_name",
        )?;
        writer.put_string16(
            &self.server_software_version,
            MAX_SERVER_SOFTWARE_VERSION_LEN,
            "server_software_version",
        )?;
        writer.put_u16(self.selected_wire_version);
        writer.put_u32(self.max_frame_len);
        writer.put_u16(
            u16::try_from(self.apis.len()).map_err(|_| crate::ProtocolError::LengthOverflow)?,
        );
        for api in &self.apis {
            writer.put_u16(api.api_key);
            writer.put_u16(api.min_api_version);
            writer.put_u16(api.max_api_version);
        }
        writer.put_u16(
            u16::try_from(self.features.len()).map_err(|_| crate::ProtocolError::LengthOverflow)?,
        );
        for feature in &self.features {
            writer.put_u16(feature.feature_key);
            writer.put_u16(feature.min_supported_version);
            writer.put_u16(feature.max_supported_version);
            writer.put_u16(feature.active_version);
        }
        put_top_level_tags(&mut writer, &self.tags)?;
        Ok(writer.into_inner())
    }

    fn decode_body(bytes: &[u8]) -> Result<Self> {
        let mut reader = Reader::new(bytes);
        let server_software_name =
            reader.read_string16(MAX_SERVER_SOFTWARE_NAME_LEN, "server_software_name")?;
        let server_software_version =
            reader.read_string16(MAX_SERVER_SOFTWARE_VERSION_LEN, "server_software_version")?;
        let selected_wire_version = reader.read_u16()?;
        let max_frame_len = reader.read_u32()?;

        let api_count = usize::from(reader.read_u16()?);
        if api_count > MAX_ADVERTISED_APIS || api_count > reader.remaining() / 6 {
            return Err(ProtocolError::InvalidFrame(
                "ApiVersions API count cannot fit remaining body bytes",
            ));
        }
        let mut apis = Vec::with_capacity(api_count);
        for _ in 0..api_count {
            let api = ApiVersionRange {
                api_key: reader.read_u16()?,
                min_api_version: reader.read_u16()?,
                max_api_version: reader.read_u16()?,
            };
            if api.min_api_version > api.max_api_version {
                return Err(ProtocolError::InvalidFrame(
                    "ApiVersions API minimum exceeds maximum",
                ));
            }
            apis.push(api);
        }

        let feature_count = usize::from(reader.read_u16()?);
        if feature_count > MAX_ADVERTISED_FEATURES || feature_count > reader.remaining() / 8 {
            return Err(ProtocolError::InvalidFrame(
                "ApiVersions feature count cannot fit remaining body bytes",
            ));
        }
        let mut features = Vec::with_capacity(feature_count);
        for _ in 0..feature_count {
            let feature = FeatureRange {
                feature_key: reader.read_u16()?,
                min_supported_version: reader.read_u16()?,
                max_supported_version: reader.read_u16()?,
                active_version: reader.read_u16()?,
            };
            if feature.min_supported_version > feature.max_supported_version {
                return Err(ProtocolError::InvalidFrame(
                    "ApiVersions feature minimum exceeds maximum",
                ));
            }
            if feature.active_version != u16::MAX
                && !(feature.min_supported_version..=feature.max_supported_version)
                    .contains(&feature.active_version)
            {
                return Err(ProtocolError::InvalidFrame(
                    "ApiVersions active feature version is outside supported range",
                ));
            }
            features.push(feature);
        }

        let tags = finish_body(&mut reader)?;
        Ok(Self {
            server_software_name,
            server_software_version,
            selected_wire_version,
            max_frame_len,
            apis,
            features,
            tags,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn api_versions_round_trip() {
        let request = ApiVersionsRequestV0 {
            client_software_name: "astralog-client".into(),
            client_software_version: "0.1.0".into(),
            tags: vec![],
        };
        assert_eq!(
            ApiVersionsRequestV0::decode_body(&request.encode_body().unwrap()).unwrap(),
            request
        );

        let response = ApiVersionsResponseV0 {
            server_software_name: "astralog-broker".into(),
            server_software_version: "0.1.0".into(),
            selected_wire_version: 1,
            max_frame_len: 1024 * 1024,
            apis: vec![ApiVersionRange {
                api_key: 0,
                min_api_version: 0,
                max_api_version: 0,
            }],
            features: vec![FeatureRange {
                feature_key: 1,
                min_supported_version: 1,
                max_supported_version: 1,
                active_version: 1,
            }],
            tags: vec![],
        };
        assert_eq!(
            ApiVersionsResponseV0::decode_body(&response.encode_body().unwrap()).unwrap(),
            response
        );
    }
}
