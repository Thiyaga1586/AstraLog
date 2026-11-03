use core::fmt;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ProtocolError {
    UnexpectedEof {
        needed: usize,
        remaining: usize,
    },
    LengthOverflow,
    InvalidUtf8,
    InvalidBoolean(u8),
    InvalidHello(&'static str),
    InvalidFrame(&'static str),
    InvalidFrameOwned(String),
    FrameTooLarge {
        len: usize,
        max: usize,
    },
    FrameLengthMismatch {
        declared: usize,
        actual: usize,
    },
    CrcMismatch {
        expected: u32,
        actual: u32,
    },
    TooManyTags(usize),
    TagTooLarge {
        tag: u16,
        len: usize,
    },
    InvalidTagOrder {
        previous: u16,
        current: u16,
    },
    FieldTooLarge {
        field: &'static str,
        len: usize,
        max: usize,
    },
    InvalidPartitionCount,
    NoWritablePartitions,
}

impl fmt::Display for ProtocolError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::UnexpectedEof { needed, remaining } => {
                write!(
                    f,
                    "unexpected end of input: need {needed} bytes, {remaining} remain"
                )
            }
            Self::LengthOverflow => f.write_str("encoded length does not fit protocol field"),
            Self::InvalidUtf8 => f.write_str("invalid UTF-8 string"),
            Self::InvalidBoolean(value) => write!(f, "invalid boolean byte {value}"),
            Self::InvalidHello(message) => write!(f, "invalid hello: {message}"),
            Self::InvalidFrame(message) => write!(f, "invalid frame: {message}"),
            Self::InvalidFrameOwned(message) => write!(f, "invalid frame: {message}"),
            Self::FrameTooLarge { len, max } => {
                write!(f, "wire frame length {len} exceeds limit {max}")
            }
            Self::FrameLengthMismatch { declared, actual } => {
                write!(
                    f,
                    "wire frame declared {declared} bytes but input contains {actual}"
                )
            }
            Self::CrcMismatch { expected, actual } => write!(
                f,
                "wire frame CRC32C mismatch: stored {expected:#010x}, computed {actual:#010x}"
            ),
            Self::TooManyTags(count) => write!(f, "tag count {count} exceeds protocol maximum"),
            Self::TagTooLarge { tag, len } => {
                write!(f, "tag {tag} value length {len} exceeds protocol maximum")
            }
            Self::InvalidTagOrder { previous, current } => write!(
                f,
                "tag ids must be strictly increasing: {previous} then {current}"
            ),
            Self::FieldTooLarge { field, len, max } => {
                write!(f, "{field} length {len} exceeds maximum {max}")
            }
            Self::InvalidPartitionCount => f.write_str("partition count must be positive"),
            Self::NoWritablePartitions => f.write_str("no writable partitions are available"),
        }
    }
}

impl std::error::Error for ProtocolError {}

pub type Result<T> = core::result::Result<T, ProtocolError>;
