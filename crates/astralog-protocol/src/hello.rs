use crate::{ProtocolError, Result, WIRE_VERSION};

pub const HELLO_LEN: usize = 16;
pub const HELLO_MAGIC: &[u8; 8] = b"ASTRALOG";
pub const HELLO_VERSION: u16 = 1;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ClientHello {
    pub min_wire_version: u16,
    pub max_wire_version: u16,
}

impl ClientHello {
    pub fn v1() -> Self {
        Self {
            min_wire_version: WIRE_VERSION,
            max_wire_version: WIRE_VERSION,
        }
    }

    pub fn encode(self) -> Result<[u8; HELLO_LEN]> {
        if self.min_wire_version > self.max_wire_version {
            return Err(ProtocolError::InvalidHello(
                "minimum wire version exceeds maximum",
            ));
        }
        let mut out = [0_u8; HELLO_LEN];
        out[0..8].copy_from_slice(HELLO_MAGIC);
        out[8..10].copy_from_slice(&HELLO_VERSION.to_be_bytes());
        out[10..12].copy_from_slice(&self.min_wire_version.to_be_bytes());
        out[12..14].copy_from_slice(&self.max_wire_version.to_be_bytes());
        Ok(out)
    }

    pub fn decode(bytes: &[u8]) -> Result<Self> {
        if bytes.len() != HELLO_LEN {
            return Err(ProtocolError::InvalidHello("client hello length"));
        }
        if &bytes[0..8] != HELLO_MAGIC {
            return Err(ProtocolError::InvalidHello("client hello magic"));
        }
        if u16::from_be_bytes([bytes[8], bytes[9]]) != HELLO_VERSION {
            return Err(ProtocolError::InvalidHello("hello version"));
        }
        let min_wire_version = u16::from_be_bytes([bytes[10], bytes[11]]);
        let max_wire_version = u16::from_be_bytes([bytes[12], bytes[13]]);
        let flags = u16::from_be_bytes([bytes[14], bytes[15]]);
        if flags != 0 {
            return Err(ProtocolError::InvalidHello("unknown client hello flags"));
        }
        if min_wire_version > max_wire_version {
            return Err(ProtocolError::InvalidHello(
                "minimum wire version exceeds maximum",
            ));
        }
        Ok(Self {
            min_wire_version,
            max_wire_version,
        })
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u16)]
pub enum HelloStatus {
    Ok = 0,
    NoCommonWireVersion = 1,
    MalformedHello = 2,
}

impl TryFrom<u16> for HelloStatus {
    type Error = ProtocolError;

    fn try_from(value: u16) -> Result<Self> {
        match value {
            0 => Ok(Self::Ok),
            1 => Ok(Self::NoCommonWireVersion),
            2 => Ok(Self::MalformedHello),
            _ => Err(ProtocolError::InvalidHello("unknown server hello status")),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ServerHello {
    pub selected_wire_version: u16,
    pub status: HelloStatus,
}

impl ServerHello {
    pub fn encode(self) -> [u8; HELLO_LEN] {
        let mut out = [0_u8; HELLO_LEN];
        out[0..8].copy_from_slice(HELLO_MAGIC);
        out[8..10].copy_from_slice(&HELLO_VERSION.to_be_bytes());
        out[10..12].copy_from_slice(&self.selected_wire_version.to_be_bytes());
        out[12..14].copy_from_slice(&(self.status as u16).to_be_bytes());
        out
    }

    pub fn decode(bytes: &[u8]) -> Result<Self> {
        if bytes.len() != HELLO_LEN {
            return Err(ProtocolError::InvalidHello("server hello length"));
        }
        if &bytes[0..8] != HELLO_MAGIC {
            return Err(ProtocolError::InvalidHello("server hello magic"));
        }
        if u16::from_be_bytes([bytes[8], bytes[9]]) != HELLO_VERSION {
            return Err(ProtocolError::InvalidHello("hello version"));
        }
        let selected_wire_version = u16::from_be_bytes([bytes[10], bytes[11]]);
        let status = HelloStatus::try_from(u16::from_be_bytes([bytes[12], bytes[13]]))?;
        let flags = u16::from_be_bytes([bytes[14], bytes[15]]);
        if flags != 0 {
            return Err(ProtocolError::InvalidHello("unknown server hello flags"));
        }
        if status == HelloStatus::Ok && selected_wire_version == 0 {
            return Err(ProtocolError::InvalidHello(
                "successful hello selected zero wire version",
            ));
        }
        if status != HelloStatus::Ok && selected_wire_version != 0 {
            return Err(ProtocolError::InvalidHello(
                "failed hello selected non-zero wire version",
            ));
        }
        Ok(Self {
            selected_wire_version,
            status,
        })
    }
}

pub fn select_wire_version(client: ClientHello, server_min: u16, server_max: u16) -> ServerHello {
    let min = client.min_wire_version.max(server_min);
    let max = client.max_wire_version.min(server_max);
    if min > max {
        ServerHello {
            selected_wire_version: 0,
            status: HelloStatus::NoCommonWireVersion,
        }
    } else {
        ServerHello {
            selected_wire_version: max,
            status: HelloStatus::Ok,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn hello_v1_round_trip_and_negotiation() {
        let client = ClientHello::v1();
        let bytes = client.encode().unwrap();
        assert_eq!(ClientHello::decode(&bytes).unwrap(), client);

        let server = select_wire_version(client, 1, 1);
        assert_eq!(server.status, HelloStatus::Ok);
        assert_eq!(server.selected_wire_version, 1);
        assert_eq!(ServerHello::decode(&server.encode()).unwrap(), server);
    }

    #[test]
    fn no_common_version_selects_zero() {
        let client = ClientHello {
            min_wire_version: 2,
            max_wire_version: 3,
        };
        let server = select_wire_version(client, 1, 1);
        assert_eq!(server.status, HelloStatus::NoCommonWireVersion);
        assert_eq!(server.selected_wire_version, 0);
    }
}
