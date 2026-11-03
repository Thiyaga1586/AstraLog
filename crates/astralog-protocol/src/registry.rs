use crate::{ProtocolError, Result};

pub const P9_RETENTION_COMPACTION_FEATURE_KEY: u16 = 15;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[repr(u16)]
pub enum ApiKey {
    ApiVersions = 0,
    DescribeCluster = 1,
    Metadata = 2,
    CreateTopic = 3,
    DeleteTopic = 4,
    DescribeTopic = 5,
    AlterTopicConfig = 6,
    ReassignPartitions = 7,
    DrainBroker = 8,
    Produce = 10,
    Fetch = 11,
    ListOffsets = 12,
    InitProducer = 20,
    JoinSubscribe = 30,
    Heartbeat = 31,
    RevokeAck = 32,
    Leave = 33,
    CommitOffsets = 34,
    FetchOffsets = 35,
    QueueReceive = 40,
    QueueAck = 41,
    QueueNack = 42,
    ExtendVisibility = 43,
}

impl TryFrom<u16> for ApiKey {
    type Error = ProtocolError;

    fn try_from(value: u16) -> Result<Self> {
        match value {
            0 => Ok(Self::ApiVersions),
            1 => Ok(Self::DescribeCluster),
            2 => Ok(Self::Metadata),
            3 => Ok(Self::CreateTopic),
            4 => Ok(Self::DeleteTopic),
            5 => Ok(Self::DescribeTopic),
            6 => Ok(Self::AlterTopicConfig),
            7 => Ok(Self::ReassignPartitions),
            8 => Ok(Self::DrainBroker),
            10 => Ok(Self::Produce),
            11 => Ok(Self::Fetch),
            12 => Ok(Self::ListOffsets),
            20 => Ok(Self::InitProducer),
            30 => Ok(Self::JoinSubscribe),
            31 => Ok(Self::Heartbeat),
            32 => Ok(Self::RevokeAck),
            33 => Ok(Self::Leave),
            34 => Ok(Self::CommitOffsets),
            35 => Ok(Self::FetchOffsets),
            40 => Ok(Self::QueueReceive),
            41 => Ok(Self::QueueAck),
            42 => Ok(Self::QueueNack),
            43 => Ok(Self::ExtendVisibility),
            _ => Err(ProtocolError::InvalidFrameOwned(format!(
                "unknown API key {value}"
            ))),
        }
    }
}

impl ApiKey {
    /// Codec versions implemented by the protocol crate. Runtime advertisement
    /// remains a broker readiness/feature-gate decision.
    pub const fn codec_version_range(self) -> Option<(u16, u16)> {
        match self {
            Self::CreateTopic | Self::DescribeTopic => Some((0, 2)),
            Self::ApiVersions
            | Self::DescribeCluster
            | Self::Metadata
            | Self::DeleteTopic
            | Self::ReassignPartitions
            | Self::Fetch
            | Self::ListOffsets
            | Self::InitProducer
            | Self::JoinSubscribe
            | Self::Heartbeat
            | Self::RevokeAck
            | Self::Leave
            | Self::CommitOffsets
            | Self::FetchOffsets => Some((0, 0)),
            Self::Produce => Some((0, 1)),
            Self::AlterTopicConfig => Some((0, 0)),
            Self::DrainBroker
            | Self::QueueReceive
            | Self::QueueAck
            | Self::QueueNack
            | Self::ExtendVisibility => None,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RetryClass {
    Retryable,
    Terminal,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u16)]
pub enum PublicError {
    None = 0,
    UnknownServerError = 1,
    MalformedRequest = 2,
    FrameTooLarge = 3,
    UnsupportedWireVersion = 4,
    UnsupportedApiKey = 5,
    UnsupportedApiVersion = 6,
    RequestTimeout = 7,
    BrokerBusy = 8,
    QuotaExceeded = 9,
    NotLeader = 10,
    StaleEpoch = 11,
    BrokerNotAvailable = 12,
    UnknownTopic = 13,
    UnknownPartition = 14,
    TopicAlreadyExists = 15,
    InvalidTopic = 16,
    InvalidRequest = 17,
    MessageTooLarge = 18,
    OffsetOutOfRange = 19,
    CorruptMessage = 20,
    StorageError = 21,
    NotEnoughReplicas = 22,
    AuthenticationFailed = 23,
    AuthorizationFailed = 24,
    OutOfOrderSequence = 25,
    ProducerFenced = 26,
    UnknownProducer = 27,
    GroupEpochStale = 28,
    UnknownMember = 29,
    RebalanceInProgress = 30,
    InvalidOffsetCommit = 31,
    QueueLeaseStale = 32,
    QueueLeaseExpired = 33,
    UnsupportedFeature = 34,
    ClusterFeatureTooOld = 35,
    DuplicateCorrelationId = 36,
    InvalidAckMode = 37,
    InvalidIsolationLevel = 38,
    TopicDeleting = 39,
    PartitionCountDecreaseNotAllowed = 40,
    InvalidReplicationFactor = 41,
}

impl PublicError {
    pub const fn retry_class(self) -> Option<RetryClass> {
        match self {
            Self::None => None,
            Self::UnknownServerError
            | Self::RequestTimeout
            | Self::BrokerBusy
            | Self::QuotaExceeded
            | Self::NotLeader
            | Self::StaleEpoch
            | Self::BrokerNotAvailable
            | Self::StorageError
            | Self::NotEnoughReplicas
            | Self::GroupEpochStale
            | Self::UnknownMember
            | Self::RebalanceInProgress
            | Self::TopicDeleting => Some(RetryClass::Retryable),
            _ => Some(RetryClass::Terminal),
        }
    }
}

impl TryFrom<u16> for PublicError {
    type Error = ProtocolError;

    fn try_from(value: u16) -> Result<Self> {
        match value {
            0 => Ok(Self::None),
            1 => Ok(Self::UnknownServerError),
            2 => Ok(Self::MalformedRequest),
            3 => Ok(Self::FrameTooLarge),
            4 => Ok(Self::UnsupportedWireVersion),
            5 => Ok(Self::UnsupportedApiKey),
            6 => Ok(Self::UnsupportedApiVersion),
            7 => Ok(Self::RequestTimeout),
            8 => Ok(Self::BrokerBusy),
            9 => Ok(Self::QuotaExceeded),
            10 => Ok(Self::NotLeader),
            11 => Ok(Self::StaleEpoch),
            12 => Ok(Self::BrokerNotAvailable),
            13 => Ok(Self::UnknownTopic),
            14 => Ok(Self::UnknownPartition),
            15 => Ok(Self::TopicAlreadyExists),
            16 => Ok(Self::InvalidTopic),
            17 => Ok(Self::InvalidRequest),
            18 => Ok(Self::MessageTooLarge),
            19 => Ok(Self::OffsetOutOfRange),
            20 => Ok(Self::CorruptMessage),
            21 => Ok(Self::StorageError),
            22 => Ok(Self::NotEnoughReplicas),
            23 => Ok(Self::AuthenticationFailed),
            24 => Ok(Self::AuthorizationFailed),
            25 => Ok(Self::OutOfOrderSequence),
            26 => Ok(Self::ProducerFenced),
            27 => Ok(Self::UnknownProducer),
            28 => Ok(Self::GroupEpochStale),
            29 => Ok(Self::UnknownMember),
            30 => Ok(Self::RebalanceInProgress),
            31 => Ok(Self::InvalidOffsetCommit),
            32 => Ok(Self::QueueLeaseStale),
            33 => Ok(Self::QueueLeaseExpired),
            34 => Ok(Self::UnsupportedFeature),
            35 => Ok(Self::ClusterFeatureTooOld),
            36 => Ok(Self::DuplicateCorrelationId),
            37 => Ok(Self::InvalidAckMode),
            38 => Ok(Self::InvalidIsolationLevel),
            39 => Ok(Self::TopicDeleting),
            40 => Ok(Self::PartitionCountDecreaseNotAllowed),
            41 => Ok(Self::InvalidReplicationFactor),
            _ => Err(ProtocolError::InvalidFrameOwned(format!(
                "unknown public error code {value}"
            ))),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn retry_class_matches_frozen_taxonomy_examples() {
        assert_eq!(
            PublicError::NotLeader.retry_class(),
            Some(RetryClass::Retryable)
        );
        assert_eq!(
            PublicError::RequestTimeout.retry_class(),
            Some(RetryClass::Retryable)
        );
        assert_eq!(
            PublicError::ProducerFenced.retry_class(),
            Some(RetryClass::Terminal)
        );
        assert_eq!(PublicError::None.retry_class(), None);
    }

    #[test]
    fn every_frozen_public_error_code_has_an_explicit_retry_class() {
        for code in 0_u16..=41 {
            let error = PublicError::try_from(code).unwrap();
            if error == PublicError::None {
                assert_eq!(error.retry_class(), None);
            } else {
                assert!(matches!(
                    error.retry_class(),
                    Some(RetryClass::Retryable | RetryClass::Terminal)
                ));
            }
        }
        assert!(PublicError::try_from(42).is_err());
    }

    #[test]
    fn p7_codec_versions_are_registered_without_implying_runtime_readiness() {
        assert_eq!(ApiKey::InitProducer.codec_version_range(), Some((0, 0)));
        assert_eq!(ApiKey::Produce.codec_version_range(), Some((0, 1)));
        assert_eq!(ApiKey::QueueReceive.codec_version_range(), None);
    }

    #[test]
    fn p9_codec_versions_and_feature_identity_are_additive_without_runtime_activation() {
        assert_eq!(P9_RETENTION_COMPACTION_FEATURE_KEY, 15);
        assert_eq!(ApiKey::CreateTopic.codec_version_range(), Some((0, 2)));
        assert_eq!(ApiKey::DescribeTopic.codec_version_range(), Some((0, 2)));
        assert_eq!(ApiKey::AlterTopicConfig.codec_version_range(), Some((0, 0)));
        assert_eq!(ApiKey::DrainBroker.codec_version_range(), None);
        assert_eq!(ApiKey::QueueReceive.codec_version_range(), None);
    }
}
