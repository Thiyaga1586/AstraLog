#![forbid(unsafe_code)]

pub mod api;
mod codec;
mod decoder;
mod error;
mod frame;
mod hash;
mod hello;
mod partitioner;
mod registry;

pub use codec::{Reader, TaggedField, Writer};
pub use decoder::FrameDecoder;
pub use error::{ProtocolError, Result};
pub use frame::{
    FRAME_FIXED_HEADER_LEN, FRAME_MAGIC, FrameKind, MAX_WIRE_FRAME_LEN_V1, MIN_WIRE_FRAME_LEN,
    NO_RESPONSE_FLAG, WireFrame, decode_frame, encode_frame,
};
pub(crate) use hash::sha256;
pub use hello::{
    ClientHello, HELLO_LEN, HELLO_MAGIC, HELLO_VERSION, HelloStatus, ServerHello,
    select_wire_version,
};
pub use partitioner::{PartitionRng, StickyPartitioner, murmur3_x86_32, partition_for_key};
pub use registry::{ApiKey, P9_RETENTION_COMPACTION_FEATURE_KEY, PublicError, RetryClass};

pub const WIRE_VERSION: u16 = 1;
pub const UNKNOWN_BROKER_ID: u32 = u32::MAX;
pub const UNKNOWN_EPOCH: u64 = u64::MAX;
