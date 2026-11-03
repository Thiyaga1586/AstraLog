#![forbid(unsafe_code)]

mod manifest;
mod matrix;
mod report;
mod result;
mod runner;

pub use manifest::{AckModeSpec, BenchmarkManifest, CompressionSpec, KeyMode, WorkloadMode};
pub use matrix::{WorkloadCell, coverage_matrix, validate_coverage_matrix};
pub use report::{generate_comparison_markdown, generate_report_markdown};
pub use result::{
    BenchmarkProvenance, BenchmarkRun, RawBundle, SystemMetricsSnapshot, percentile_ns,
};
pub use runner::{ensure_release_profile, run_benchmark, validate_runtime_provenance};

pub type Result<T> = std::result::Result<T, BenchmarkError>;

#[derive(Debug)]
pub enum BenchmarkError {
    Invalid(&'static str),
    InvalidOwned(String),
    Io(std::io::Error),
    Client(astralog_client::ClientError),
    Protocol(astralog_protocol::ProtocolError),
}

impl std::fmt::Display for BenchmarkError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Invalid(message) => f.write_str(message),
            Self::InvalidOwned(message) => f.write_str(message),
            Self::Io(error) => write!(f, "I/O error: {error}"),
            Self::Client(error) => write!(f, "client error: {error}"),
            Self::Protocol(error) => write!(f, "protocol error: {error}"),
        }
    }
}

impl std::error::Error for BenchmarkError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Io(error) => Some(error),
            Self::Client(error) => Some(error),
            Self::Protocol(error) => Some(error),
            Self::Invalid(_) | Self::InvalidOwned(_) => None,
        }
    }
}

impl From<std::io::Error> for BenchmarkError {
    fn from(value: std::io::Error) -> Self {
        Self::Io(value)
    }
}

impl From<astralog_client::ClientError> for BenchmarkError {
    fn from(value: astralog_client::ClientError) -> Self {
        Self::Client(value)
    }
}

impl From<astralog_protocol::ProtocolError> for BenchmarkError {
    fn from(value: astralog_protocol::ProtocolError) -> Self {
        Self::Protocol(value)
    }
}
