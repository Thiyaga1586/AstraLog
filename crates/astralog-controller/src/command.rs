use astralog_common::TopicId;
use astralog_protocol::api::{MAX_HOST_LEN, MAX_TOPIC_NAME_LEN};

use crate::{
    AssignmentGeneration, AssignmentRecord, BrokerLifecycle, ControllerError, FailureDomainPolicy,
    MAX_TOPIC_PARTITIONS, MovementId, MovementStage, Result, TopicLifecycle,
};

const COMMAND_VERSION: u16 = 1;

const KIND_REGISTER_BROKER: u16 = 1;
const KIND_REJOIN_BROKER: u16 = 2;
const KIND_SET_BROKER_LIFECYCLE: u16 = 3;
const KIND_CREATE_TOPIC: u16 = 4;
const KIND_BEGIN_DELETE_TOPIC: u16 = 5;
const KIND_FINALIZE_DELETE_TOPIC: u16 = 6;
const KIND_EXPAND_TOPIC: u16 = 7;
const KIND_BEGIN_MOVEMENT: u16 = 8;
const KIND_ADVANCE_MOVEMENT: u16 = 9;
const KIND_FINALIZE_MOVEMENT: u16 = 10;
const KIND_SET_FEATURE_LEVEL: u16 = 11;
const KIND_ALLOCATE_PRODUCER_IDS: u16 = 12;

/// Hard P5 bound for one durable producer-ID allocation command. The frozen
/// V1 encoding remains a `u32`; this tighter implementation bound prevents a
/// single request from consuming an effectively unbounded logical ID range.
pub const MAX_PRODUCER_IDS_PER_ALLOCATION: u32 = 1_000_000;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct CommandLimits {
    pub max_command_bytes: usize,
    pub max_host_bytes: usize,
    pub max_label_bytes: usize,
    pub max_assignments_per_command: usize,
    pub max_replicas_per_assignment: usize,
}

impl Default for CommandLimits {
    fn default() -> Self {
        Self {
            max_command_bytes: 4 * 1024 * 1024,
            max_host_bytes: MAX_HOST_LEN,
            max_label_bytes: 128,
            max_assignments_per_command: 100_000,
            max_replicas_per_assignment: 5,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ControllerCommand {
    RegisterBroker {
        broker_id: u32,
        node_uuid: [u8; 16],
        initial_incarnation: u64,
        client_host: String,
        client_port: u16,
        internal_host: String,
        internal_port: u16,
        rack: Option<String>,
        zone: Option<String>,
        min_feature: u16,
        max_feature: u16,
    },
    RejoinBroker {
        broker_id: u32,
        node_uuid: [u8; 16],
        expected_incarnation: u64,
        new_incarnation: u64,
        client_host: String,
        client_port: u16,
        internal_host: String,
        internal_port: u16,
        rack: Option<String>,
        zone: Option<String>,
        min_feature: u16,
        max_feature: u16,
    },
    SetBrokerLifecycle {
        broker_id: u32,
        expected_incarnation: u64,
        expected_lifecycle: BrokerLifecycle,
        new_lifecycle: BrokerLifecycle,
    },
    CreateTopic {
        topic_id: TopicId,
        topic_name: String,
        partition_count: u32,
        replication_factor: u16,
        failure_domain_policy: FailureDomainPolicy,
        assignments: Vec<AssignmentRecord>,
    },
    BeginDeleteTopic {
        topic_id: TopicId,
        expected_topic_lifecycle: TopicLifecycle,
    },
    FinalizeDeleteTopic {
        topic_id: TopicId,
        expected_topic_lifecycle: TopicLifecycle,
    },
    ExpandTopic {
        topic_id: TopicId,
        expected_partition_count: u32,
        new_partition_count: u32,
        assignments: Vec<AssignmentRecord>,
    },
    BeginMovement {
        movement_id: MovementId,
        topic_id: TopicId,
        partition_id: u32,
        expected_assignment_generation: AssignmentGeneration,
        source_broker_id: u32,
        destination_broker_id: u32,
        target_voters: Vec<u32>,
        preferred_leader: Option<u32>,
    },
    AdvanceMovement {
        movement_id: MovementId,
        expected_stage: MovementStage,
        new_stage: MovementStage,
    },
    FinalizeMovement {
        movement_id: MovementId,
        expected_stage: MovementStage,
        replacement: AssignmentRecord,
    },
    SetFeatureLevel {
        expected_feature_level: u16,
        new_feature_level: u16,
    },
    AllocateProducerIds {
        allocation_count: u32,
    },
}

pub fn encode_command(command: &ControllerCommand, limits: CommandLimits) -> Result<Vec<u8>> {
    let mut body = Vec::new();
    let kind = match command {
        ControllerCommand::RegisterBroker {
            broker_id,
            node_uuid,
            initial_incarnation,
            client_host,
            client_port,
            internal_host,
            internal_port,
            rack,
            zone,
            min_feature,
            max_feature,
        } => {
            if *initial_incarnation != 1 {
                return Err(ControllerError::Invalid(
                    "first broker registration incarnation must be one",
                ));
            }
            validate_broker_fields(
                BrokerFields {
                    broker_id: *broker_id,
                    node_uuid,
                    incarnation: *initial_incarnation,
                    client_host,
                    client_port: *client_port,
                    internal_host,
                    internal_port: *internal_port,
                    rack: rack.as_deref(),
                    zone: zone.as_deref(),
                    min_feature: *min_feature,
                    max_feature: *max_feature,
                },
                limits,
            )?;
            put_u32(&mut body, *broker_id);
            body.extend_from_slice(node_uuid);
            put_u64(&mut body, *initial_incarnation);
            put_string16(&mut body, client_host, limits.max_host_bytes, "client host")?;
            put_u16(&mut body, *client_port);
            put_string16(
                &mut body,
                internal_host,
                limits.max_host_bytes,
                "internal host",
            )?;
            put_u16(&mut body, *internal_port);
            put_optional_string(&mut body, rack.as_deref(), limits.max_label_bytes, "rack")?;
            put_optional_string(&mut body, zone.as_deref(), limits.max_label_bytes, "zone")?;
            put_u16(&mut body, *min_feature);
            put_u16(&mut body, *max_feature);
            KIND_REGISTER_BROKER
        }
        ControllerCommand::RejoinBroker {
            broker_id,
            node_uuid,
            expected_incarnation,
            new_incarnation,
            client_host,
            client_port,
            internal_host,
            internal_port,
            rack,
            zone,
            min_feature,
            max_feature,
        } => {
            validate_broker_fields(
                BrokerFields {
                    broker_id: *broker_id,
                    node_uuid,
                    incarnation: *new_incarnation,
                    client_host,
                    client_port: *client_port,
                    internal_host,
                    internal_port: *internal_port,
                    rack: rack.as_deref(),
                    zone: zone.as_deref(),
                    min_feature: *min_feature,
                    max_feature: *max_feature,
                },
                limits,
            )?;
            if *expected_incarnation == 0
                || expected_incarnation.checked_add(1) != Some(*new_incarnation)
            {
                return Err(ControllerError::Invalid(
                    "rejoin incarnation must be exactly expected + 1",
                ));
            }
            put_u32(&mut body, *broker_id);
            body.extend_from_slice(node_uuid);
            put_u64(&mut body, *expected_incarnation);
            put_u64(&mut body, *new_incarnation);
            put_string16(&mut body, client_host, limits.max_host_bytes, "client host")?;
            put_u16(&mut body, *client_port);
            put_string16(
                &mut body,
                internal_host,
                limits.max_host_bytes,
                "internal host",
            )?;
            put_u16(&mut body, *internal_port);
            put_optional_string(&mut body, rack.as_deref(), limits.max_label_bytes, "rack")?;
            put_optional_string(&mut body, zone.as_deref(), limits.max_label_bytes, "zone")?;
            put_u16(&mut body, *min_feature);
            put_u16(&mut body, *max_feature);
            KIND_REJOIN_BROKER
        }
        ControllerCommand::SetBrokerLifecycle {
            broker_id,
            expected_incarnation,
            expected_lifecycle,
            new_lifecycle,
        } => {
            validate_broker_id(*broker_id)?;
            if *expected_incarnation == 0 || expected_lifecycle == new_lifecycle {
                return Err(ControllerError::Invalid(
                    "invalid broker lifecycle transition command",
                ));
            }
            put_u32(&mut body, *broker_id);
            put_u64(&mut body, *expected_incarnation);
            put_u8(&mut body, *expected_lifecycle as u8);
            put_u8(&mut body, *new_lifecycle as u8);
            KIND_SET_BROKER_LIFECYCLE
        }
        ControllerCommand::CreateTopic {
            topic_id,
            topic_name,
            partition_count,
            replication_factor,
            failure_domain_policy,
            assignments,
        } => {
            validate_topic_id(*topic_id)?;
            crate::topic::validate_controller_topic_name_and_topology(
                topic_name,
                *partition_count,
                *replication_factor,
                *failure_domain_policy,
            )?;
            if topic_name.len() > MAX_TOPIC_NAME_LEN {
                return Err(ControllerError::LimitExceeded {
                    what: "topic name bytes",
                    actual: topic_name.len(),
                    max: MAX_TOPIC_NAME_LEN,
                });
            }
            validate_rf(*replication_factor)?;
            if *partition_count == 0
                || *partition_count > MAX_TOPIC_PARTITIONS
                || usize::try_from(*partition_count).ok() != Some(assignments.len())
            {
                return Err(ControllerError::Invalid(
                    "create topic assignments must cover every bounded V1 partition",
                ));
            }
            validate_assignment_array(
                assignments,
                *topic_id,
                0,
                *partition_count,
                Some(*replication_factor),
                true,
                limits,
            )?;
            body.extend_from_slice(topic_id.as_bytes());
            put_string16(&mut body, topic_name, MAX_TOPIC_NAME_LEN, "topic name")?;
            put_u32(&mut body, *partition_count);
            put_u16(&mut body, *replication_factor);
            put_u8(&mut body, *failure_domain_policy as u8);
            put_assignments(&mut body, assignments, limits)?;
            KIND_CREATE_TOPIC
        }
        ControllerCommand::BeginDeleteTopic {
            topic_id,
            expected_topic_lifecycle,
        } => {
            validate_topic_id(*topic_id)?;
            body.extend_from_slice(topic_id.as_bytes());
            put_u8(&mut body, *expected_topic_lifecycle as u8);
            KIND_BEGIN_DELETE_TOPIC
        }
        ControllerCommand::FinalizeDeleteTopic {
            topic_id,
            expected_topic_lifecycle,
        } => {
            validate_topic_id(*topic_id)?;
            body.extend_from_slice(topic_id.as_bytes());
            put_u8(&mut body, *expected_topic_lifecycle as u8);
            KIND_FINALIZE_DELETE_TOPIC
        }
        ControllerCommand::ExpandTopic {
            topic_id,
            expected_partition_count,
            new_partition_count,
            assignments,
        } => {
            validate_topic_id(*topic_id)?;
            if *expected_partition_count == 0
                || *new_partition_count <= *expected_partition_count
                || *new_partition_count > MAX_TOPIC_PARTITIONS
            {
                return Err(ControllerError::Invalid(
                    "topic expansion must strictly increase within the V1 partition bound",
                ));
            }
            validate_assignment_array(
                assignments,
                *topic_id,
                *expected_partition_count,
                *new_partition_count,
                None,
                true,
                limits,
            )?;
            body.extend_from_slice(topic_id.as_bytes());
            put_u32(&mut body, *expected_partition_count);
            put_u32(&mut body, *new_partition_count);
            put_assignments(&mut body, assignments, limits)?;
            KIND_EXPAND_TOPIC
        }
        ControllerCommand::BeginMovement {
            movement_id,
            topic_id,
            partition_id,
            expected_assignment_generation,
            source_broker_id,
            destination_broker_id,
            target_voters,
            preferred_leader,
        } => {
            validate_topic_id(*topic_id)?;
            validate_broker_id(*source_broker_id)?;
            validate_broker_id(*destination_broker_id)?;
            if source_broker_id == destination_broker_id {
                return Err(ControllerError::Invalid(
                    "movement source and destination must differ",
                ));
            }
            validate_broker_set(
                target_voters,
                limits.max_replicas_per_assignment,
                "target voters",
            )?;
            if !target_voters.contains(destination_broker_id)
                || target_voters.contains(source_broker_id)
                || preferred_leader.is_some_and(|leader| !target_voters.contains(&leader))
            {
                return Err(ControllerError::Invalid(
                    "invalid movement target voter set",
                ));
            }
            body.extend_from_slice(movement_id.as_bytes());
            body.extend_from_slice(topic_id.as_bytes());
            put_u32(&mut body, *partition_id);
            put_u64(&mut body, expected_assignment_generation.get());
            put_u32(&mut body, *source_broker_id);
            put_u32(&mut body, *destination_broker_id);
            put_u32_array(&mut body, target_voters)?;
            put_optional_u32(&mut body, *preferred_leader);
            KIND_BEGIN_MOVEMENT
        }
        ControllerCommand::AdvanceMovement {
            movement_id,
            expected_stage,
            new_stage,
        } => {
            if expected_stage == new_stage {
                return Err(ControllerError::Invalid("movement stage must advance"));
            }
            body.extend_from_slice(movement_id.as_bytes());
            put_u8(&mut body, *expected_stage as u8);
            put_u8(&mut body, *new_stage as u8);
            KIND_ADVANCE_MOVEMENT
        }
        ControllerCommand::FinalizeMovement {
            movement_id,
            expected_stage,
            replacement,
        } => {
            validate_assignment(replacement, limits)?;
            body.extend_from_slice(movement_id.as_bytes());
            put_u8(&mut body, *expected_stage as u8);
            put_assignment(&mut body, replacement, limits)?;
            KIND_FINALIZE_MOVEMENT
        }
        ControllerCommand::SetFeatureLevel {
            expected_feature_level,
            new_feature_level,
        } => {
            if *new_feature_level <= *expected_feature_level {
                return Err(ControllerError::Invalid(
                    "feature level must strictly increase",
                ));
            }
            put_u16(&mut body, *expected_feature_level);
            put_u16(&mut body, *new_feature_level);
            KIND_SET_FEATURE_LEVEL
        }
        ControllerCommand::AllocateProducerIds { allocation_count } => {
            if *allocation_count == 0 || *allocation_count > MAX_PRODUCER_IDS_PER_ALLOCATION {
                return Err(ControllerError::Invalid(
                    "producer id allocation count is outside the P5 bound",
                ));
            }
            put_u32(&mut body, *allocation_count);
            KIND_ALLOCATE_PRODUCER_IDS
        }
    };

    let total = 8_usize
        .checked_add(body.len())
        .ok_or(ControllerError::Invalid(
            "controller command length overflow",
        ))?;
    if total > limits.max_command_bytes {
        return Err(ControllerError::LimitExceeded {
            what: "controller command bytes",
            actual: total,
            max: limits.max_command_bytes,
        });
    }
    let body_len = u32::try_from(body.len())
        .map_err(|_| ControllerError::Invalid("controller command body length overflow"))?;
    let mut out = Vec::with_capacity(total);
    put_u16(&mut out, COMMAND_VERSION);
    put_u16(&mut out, kind);
    put_u32(&mut out, body_len);
    out.extend_from_slice(&body);
    Ok(out)
}

pub fn decode_command(bytes: &[u8], limits: CommandLimits) -> Result<ControllerCommand> {
    if bytes.len() > limits.max_command_bytes {
        return Err(ControllerError::LimitExceeded {
            what: "controller command bytes",
            actual: bytes.len(),
            max: limits.max_command_bytes,
        });
    }
    let mut reader = Reader::new(bytes);
    if reader.u16()? != COMMAND_VERSION {
        return Err(ControllerError::Invalid(
            "unsupported controller command version",
        ));
    }
    let kind = reader.u16()?;
    let body_len = usize::try_from(reader.u32()?)
        .map_err(|_| ControllerError::Invalid("controller command body length overflow"))?;
    if body_len != reader.remaining() {
        return Err(ControllerError::Invalid(
            "controller command body length mismatch",
        ));
    }
    let command = match kind {
        KIND_REGISTER_BROKER => ControllerCommand::RegisterBroker {
            broker_id: reader.u32()?,
            node_uuid: reader.uuid()?,
            initial_incarnation: reader.u64()?,
            client_host: reader.string16(limits.max_host_bytes, "client host")?,
            client_port: reader.u16()?,
            internal_host: reader.string16(limits.max_host_bytes, "internal host")?,
            internal_port: reader.u16()?,
            rack: reader.optional_string(limits.max_label_bytes, "rack")?,
            zone: reader.optional_string(limits.max_label_bytes, "zone")?,
            min_feature: reader.u16()?,
            max_feature: reader.u16()?,
        },
        KIND_REJOIN_BROKER => ControllerCommand::RejoinBroker {
            broker_id: reader.u32()?,
            node_uuid: reader.uuid()?,
            expected_incarnation: reader.u64()?,
            new_incarnation: reader.u64()?,
            client_host: reader.string16(limits.max_host_bytes, "client host")?,
            client_port: reader.u16()?,
            internal_host: reader.string16(limits.max_host_bytes, "internal host")?,
            internal_port: reader.u16()?,
            rack: reader.optional_string(limits.max_label_bytes, "rack")?,
            zone: reader.optional_string(limits.max_label_bytes, "zone")?,
            min_feature: reader.u16()?,
            max_feature: reader.u16()?,
        },
        KIND_SET_BROKER_LIFECYCLE => ControllerCommand::SetBrokerLifecycle {
            broker_id: reader.u32()?,
            expected_incarnation: reader.u64()?,
            expected_lifecycle: decode_broker_lifecycle(reader.u8()?)?,
            new_lifecycle: decode_broker_lifecycle(reader.u8()?)?,
        },
        KIND_CREATE_TOPIC => {
            let topic_id = TopicId::new(reader.uuid()?);
            ControllerCommand::CreateTopic {
                topic_id,
                topic_name: reader.string16(MAX_TOPIC_NAME_LEN, "topic name")?,
                partition_count: reader.u32()?,
                replication_factor: reader.u16()?,
                failure_domain_policy: decode_failure_domain_policy(reader.u8()?)?,
                assignments: reader.assignments(limits)?,
            }
        }
        KIND_BEGIN_DELETE_TOPIC => ControllerCommand::BeginDeleteTopic {
            topic_id: TopicId::new(reader.uuid()?),
            expected_topic_lifecycle: decode_topic_lifecycle(reader.u8()?)?,
        },
        KIND_FINALIZE_DELETE_TOPIC => ControllerCommand::FinalizeDeleteTopic {
            topic_id: TopicId::new(reader.uuid()?),
            expected_topic_lifecycle: decode_topic_lifecycle(reader.u8()?)?,
        },
        KIND_EXPAND_TOPIC => {
            let topic_id = TopicId::new(reader.uuid()?);
            ControllerCommand::ExpandTopic {
                topic_id,
                expected_partition_count: reader.u32()?,
                new_partition_count: reader.u32()?,
                assignments: reader.assignments(limits)?,
            }
        }
        KIND_BEGIN_MOVEMENT => ControllerCommand::BeginMovement {
            movement_id: MovementId::new(reader.uuid()?)?,
            topic_id: TopicId::new(reader.uuid()?),
            partition_id: reader.u32()?,
            expected_assignment_generation: AssignmentGeneration::new(reader.u64()?)?,
            source_broker_id: reader.u32()?,
            destination_broker_id: reader.u32()?,
            target_voters: reader.u32_array(limits.max_replicas_per_assignment, "target voters")?,
            preferred_leader: reader.optional_u32()?,
        },
        KIND_ADVANCE_MOVEMENT => ControllerCommand::AdvanceMovement {
            movement_id: MovementId::new(reader.uuid()?)?,
            expected_stage: decode_movement_stage(reader.u8()?)?,
            new_stage: decode_movement_stage(reader.u8()?)?,
        },
        KIND_FINALIZE_MOVEMENT => ControllerCommand::FinalizeMovement {
            movement_id: MovementId::new(reader.uuid()?)?,
            expected_stage: decode_movement_stage(reader.u8()?)?,
            replacement: reader.assignment(limits)?,
        },
        KIND_SET_FEATURE_LEVEL => ControllerCommand::SetFeatureLevel {
            expected_feature_level: reader.u16()?,
            new_feature_level: reader.u16()?,
        },
        KIND_ALLOCATE_PRODUCER_IDS => ControllerCommand::AllocateProducerIds {
            allocation_count: reader.u32()?,
        },
        _ => return Err(ControllerError::Invalid("unknown controller command kind")),
    };
    if !reader.is_empty() {
        return Err(ControllerError::Invalid(
            "trailing bytes in controller command",
        ));
    }
    let canonical = encode_command(&command, limits)?;
    if canonical != bytes {
        return Err(ControllerError::Invalid(
            "controller command is not canonical",
        ));
    }
    Ok(command)
}

struct BrokerFields<'a> {
    broker_id: u32,
    node_uuid: &'a [u8; 16],
    incarnation: u64,
    client_host: &'a str,
    client_port: u16,
    internal_host: &'a str,
    internal_port: u16,
    rack: Option<&'a str>,
    zone: Option<&'a str>,
    min_feature: u16,
    max_feature: u16,
}

fn validate_broker_fields(fields: BrokerFields<'_>, limits: CommandLimits) -> Result<()> {
    validate_broker_id(fields.broker_id)?;
    if *fields.node_uuid == [0; 16] || fields.incarnation == 0 {
        return Err(ControllerError::Invalid("broker identity is incomplete"));
    }
    validate_nonempty_string(fields.client_host, limits.max_host_bytes, "client host")?;
    validate_nonempty_string(fields.internal_host, limits.max_host_bytes, "internal host")?;
    if fields.client_port == 0 || fields.internal_port == 0 {
        return Err(ControllerError::Invalid("broker ports must be non-zero"));
    }
    for (value, what) in [(fields.rack, "rack"), (fields.zone, "zone")] {
        if let Some(value) = value {
            validate_nonempty_string(value, limits.max_label_bytes, what)?;
        }
    }
    if fields.min_feature == 0 || fields.max_feature == 0 || fields.min_feature > fields.max_feature
    {
        return Err(ControllerError::Invalid("invalid broker feature range"));
    }
    Ok(())
}

fn validate_nonempty_string(value: &str, max: usize, what: &'static str) -> Result<()> {
    if value.is_empty() {
        return Err(ControllerError::InvalidOwned(format!(
            "{what} must not be empty"
        )));
    }
    if value.len() > max {
        return Err(ControllerError::LimitExceeded {
            what,
            actual: value.len(),
            max,
        });
    }
    Ok(())
}

fn validate_topic_id(topic_id: TopicId) -> Result<()> {
    if *topic_id.as_bytes() == [0; 16] {
        return Err(ControllerError::Invalid("topic id zero is reserved"));
    }
    Ok(())
}

fn validate_broker_id(broker_id: u32) -> Result<()> {
    if broker_id == u32::MAX {
        return Err(ControllerError::Invalid("u32::MAX broker id is reserved"));
    }
    Ok(())
}

fn validate_rf(rf: u16) -> Result<()> {
    if !matches!(rf, 1 | 3 | 5) {
        return Err(ControllerError::Invalid(
            "replication factor must be exactly 1, 3, or 5",
        ));
    }
    Ok(())
}

pub(crate) fn validate_assignment(
    assignment: &AssignmentRecord,
    limits: CommandLimits,
) -> Result<()> {
    validate_topic_id(assignment.topic_id)?;
    validate_broker_set(
        &assignment.voters,
        limits.max_replicas_per_assignment,
        "voters",
    )?;
    if !assignment.learners.is_empty() {
        validate_broker_set(
            &assignment.learners,
            limits.max_replicas_per_assignment,
            "learners",
        )?;
    }
    if !matches!(assignment.voters.len(), 1 | 3 | 5) {
        return Err(ControllerError::Invalid(
            "assignment voter count must be exactly 1, 3, or 5",
        ));
    }
    if assignment
        .learners
        .iter()
        .any(|broker| assignment.voters.binary_search(broker).is_ok())
    {
        return Err(ControllerError::Invalid(
            "assignment voters and learners must be disjoint",
        ));
    }
    Ok(())
}

fn validate_assignment_array(
    assignments: &[AssignmentRecord],
    topic_id: TopicId,
    first_partition: u32,
    end_partition: u32,
    required_rf: Option<u16>,
    require_no_learners: bool,
    limits: CommandLimits,
) -> Result<()> {
    if assignments.len() > limits.max_assignments_per_command {
        return Err(ControllerError::LimitExceeded {
            what: "assignments per command",
            actual: assignments.len(),
            max: limits.max_assignments_per_command,
        });
    }
    let expected_len = end_partition
        .checked_sub(first_partition)
        .and_then(|value| usize::try_from(value).ok())
        .ok_or(ControllerError::Invalid("partition range overflow"))?;
    if assignments.len() != expected_len {
        return Err(ControllerError::Invalid(
            "assignment array does not cover expected partition range",
        ));
    }
    for (ordinal, assignment) in assignments.iter().enumerate() {
        validate_assignment(assignment, limits)?;
        let expected_partition = first_partition
            .checked_add(
                u32::try_from(ordinal)
                    .map_err(|_| ControllerError::Invalid("partition ordinal overflow"))?,
            )
            .ok_or(ControllerError::Invalid("partition id overflow"))?;
        if assignment.topic_id != topic_id || assignment.partition_id != expected_partition {
            return Err(ControllerError::Invalid(
                "assignments must be canonical contiguous partition order",
            ));
        }
        if required_rf.is_some_and(|rf| usize::from(rf) != assignment.voters.len()) {
            return Err(ControllerError::Invalid(
                "assignment voter count does not match topic replication factor",
            ));
        }
        if require_no_learners && !assignment.learners.is_empty() {
            return Err(ControllerError::Invalid(
                "initial/expanded assignments cannot contain learners",
            ));
        }
    }
    Ok(())
}

fn validate_broker_set(values: &[u32], max: usize, what: &'static str) -> Result<()> {
    if values.is_empty() {
        return Err(ControllerError::InvalidOwned(format!(
            "{what} must not be empty"
        )));
    }
    if values.len() > max {
        return Err(ControllerError::LimitExceeded {
            what,
            actual: values.len(),
            max,
        });
    }
    let mut previous = None;
    for value in values {
        validate_broker_id(*value)?;
        if previous.is_some_and(|previous| previous >= *value) {
            return Err(ControllerError::InvalidOwned(format!(
                "{what} must be strictly ascending and distinct"
            )));
        }
        previous = Some(*value);
    }
    Ok(())
}

fn put_assignments(
    out: &mut Vec<u8>,
    assignments: &[AssignmentRecord],
    limits: CommandLimits,
) -> Result<()> {
    put_u32(
        out,
        u32::try_from(assignments.len())
            .map_err(|_| ControllerError::Invalid("assignment count overflow"))?,
    );
    for assignment in assignments {
        put_assignment(out, assignment, limits)?;
    }
    Ok(())
}

fn put_assignment(
    out: &mut Vec<u8>,
    assignment: &AssignmentRecord,
    limits: CommandLimits,
) -> Result<()> {
    validate_assignment(assignment, limits)?;
    out.extend_from_slice(assignment.topic_id.as_bytes());
    put_u32(out, assignment.partition_id);
    put_u64(out, assignment.assignment_generation.get());
    put_u32_array(out, &assignment.voters)?;
    put_u32_array(out, &assignment.learners)?;
    Ok(())
}

fn put_u32_array(out: &mut Vec<u8>, values: &[u32]) -> Result<()> {
    put_u32(
        out,
        u32::try_from(values.len())
            .map_err(|_| ControllerError::Invalid("array count overflow"))?,
    );
    for value in values {
        put_u32(out, *value);
    }
    Ok(())
}

fn put_optional_u32(out: &mut Vec<u8>, value: Option<u32>) {
    match value {
        None => put_u8(out, 0),
        Some(value) => {
            put_u8(out, 1);
            put_u32(out, value);
        }
    }
}

fn put_optional_string(
    out: &mut Vec<u8>,
    value: Option<&str>,
    max: usize,
    what: &'static str,
) -> Result<()> {
    match value {
        None => put_u8(out, 0),
        Some(value) => {
            put_u8(out, 1);
            put_string16(out, value, max, what)?;
        }
    }
    Ok(())
}

fn put_string16(out: &mut Vec<u8>, value: &str, max: usize, what: &'static str) -> Result<()> {
    if value.len() > max {
        return Err(ControllerError::LimitExceeded {
            what,
            actual: value.len(),
            max,
        });
    }
    put_u16(
        out,
        u16::try_from(value.len())
            .map_err(|_| ControllerError::Invalid("string16 length overflow"))?,
    );
    out.extend_from_slice(value.as_bytes());
    Ok(())
}

fn put_u8(out: &mut Vec<u8>, value: u8) {
    out.push(value);
}

fn put_u16(out: &mut Vec<u8>, value: u16) {
    out.extend_from_slice(&value.to_le_bytes());
}

fn put_u32(out: &mut Vec<u8>, value: u32) {
    out.extend_from_slice(&value.to_le_bytes());
}

fn put_u64(out: &mut Vec<u8>, value: u64) {
    out.extend_from_slice(&value.to_le_bytes());
}

fn decode_broker_lifecycle(value: u8) -> Result<BrokerLifecycle> {
    match value {
        1 => Ok(BrokerLifecycle::Joining),
        2 => Ok(BrokerLifecycle::Active),
        3 => Ok(BrokerLifecycle::Draining),
        4 => Ok(BrokerLifecycle::Fenced),
        5 => Ok(BrokerLifecycle::Removed),
        _ => Err(ControllerError::Invalid("unknown broker lifecycle")),
    }
}

fn decode_topic_lifecycle(value: u8) -> Result<TopicLifecycle> {
    match value {
        1 => Ok(TopicLifecycle::Active),
        2 => Ok(TopicLifecycle::Deleting),
        _ => Err(ControllerError::Invalid("unknown topic lifecycle")),
    }
}

fn decode_failure_domain_policy(value: u8) -> Result<FailureDomainPolicy> {
    match value {
        0 => Ok(FailureDomainPolicy::Disabled),
        1 => Ok(FailureDomainPolicy::Preferred),
        2 => Ok(FailureDomainPolicy::Required),
        _ => Err(ControllerError::Invalid("unknown failure-domain policy")),
    }
}

fn decode_movement_stage(value: u8) -> Result<MovementStage> {
    match value {
        1 => Ok(MovementStage::Planned),
        2 => Ok(MovementStage::LearnerRequested),
        3 => Ok(MovementStage::LearnerCreated),
        4 => Ok(MovementStage::CatchingUp),
        5 => Ok(MovementStage::PromotionReady),
        6 => Ok(MovementStage::EnterJointRequested),
        7 => Ok(MovementStage::JointCommitted),
        8 => Ok(MovementStage::LeaveJointRequested),
        9 => Ok(MovementStage::NewStableCommitted),
        10 => Ok(MovementStage::LeadershipTransfer),
        11 => Ok(MovementStage::OldReplicaRemoveRequested),
        12 => Ok(MovementStage::OldReplicaRemoved),
        13 => Ok(MovementStage::Complete),
        _ => Err(ControllerError::Invalid("unknown movement stage")),
    }
}

struct Reader<'a> {
    bytes: &'a [u8],
    position: usize,
}

impl<'a> Reader<'a> {
    const fn new(bytes: &'a [u8]) -> Self {
        Self { bytes, position: 0 }
    }

    fn remaining(&self) -> usize {
        self.bytes.len().saturating_sub(self.position)
    }

    fn is_empty(&self) -> bool {
        self.position == self.bytes.len()
    }

    fn take(&mut self, len: usize) -> Result<&'a [u8]> {
        let end = self
            .position
            .checked_add(len)
            .ok_or(ControllerError::Invalid("decoder length overflow"))?;
        let value = self
            .bytes
            .get(self.position..end)
            .ok_or(ControllerError::Invalid("truncated controller command"))?;
        self.position = end;
        Ok(value)
    }

    fn u8(&mut self) -> Result<u8> {
        Ok(self.take(1)?[0])
    }

    fn u16(&mut self) -> Result<u16> {
        Ok(u16::from_le_bytes(
            self.take(2)?.try_into().expect("2-byte slice"),
        ))
    }

    fn u32(&mut self) -> Result<u32> {
        Ok(u32::from_le_bytes(
            self.take(4)?.try_into().expect("4-byte slice"),
        ))
    }

    fn u64(&mut self) -> Result<u64> {
        Ok(u64::from_le_bytes(
            self.take(8)?.try_into().expect("8-byte slice"),
        ))
    }

    fn uuid(&mut self) -> Result<[u8; 16]> {
        Ok(self.take(16)?.try_into().expect("16-byte slice"))
    }

    fn string16(&mut self, max: usize, what: &'static str) -> Result<String> {
        let len = usize::from(self.u16()?);
        if len > max {
            return Err(ControllerError::LimitExceeded {
                what,
                actual: len,
                max,
            });
        }
        let bytes = self.take(len)?;
        let value = core::str::from_utf8(bytes)
            .map_err(|_| ControllerError::Invalid("invalid UTF-8 in controller command"))?;
        Ok(value.to_owned())
    }

    fn optional_string(&mut self, max: usize, what: &'static str) -> Result<Option<String>> {
        match self.u8()? {
            0 => Ok(None),
            1 => Ok(Some(self.string16(max, what)?)),
            _ => Err(ControllerError::Invalid("invalid optional string marker")),
        }
    }

    fn optional_u32(&mut self) -> Result<Option<u32>> {
        match self.u8()? {
            0 => Ok(None),
            1 => Ok(Some(self.u32()?)),
            _ => Err(ControllerError::Invalid("invalid optional u32 marker")),
        }
    }

    fn u32_array(&mut self, max: usize, what: &'static str) -> Result<Vec<u32>> {
        let count = usize::try_from(self.u32()?)
            .map_err(|_| ControllerError::Invalid("array count overflow"))?;
        if count > max {
            return Err(ControllerError::LimitExceeded {
                what,
                actual: count,
                max,
            });
        }
        if count > self.remaining() / 4 {
            return Err(ControllerError::Invalid("array cannot fit remaining bytes"));
        }
        let mut values = Vec::with_capacity(count);
        for _ in 0..count {
            values.push(self.u32()?);
        }
        Ok(values)
    }

    fn assignment(&mut self, limits: CommandLimits) -> Result<AssignmentRecord> {
        let assignment = AssignmentRecord {
            topic_id: TopicId::new(self.uuid()?),
            partition_id: self.u32()?,
            assignment_generation: AssignmentGeneration::new(self.u64()?)?,
            voters: self.u32_array(limits.max_replicas_per_assignment, "voters")?,
            learners: self.u32_array(limits.max_replicas_per_assignment, "learners")?,
        };
        validate_assignment(&assignment, limits)?;
        Ok(assignment)
    }

    fn assignments(&mut self, limits: CommandLimits) -> Result<Vec<AssignmentRecord>> {
        let count = usize::try_from(self.u32()?)
            .map_err(|_| ControllerError::Invalid("assignment count overflow"))?;
        if count > limits.max_assignments_per_command {
            return Err(ControllerError::LimitExceeded {
                what: "assignments per command",
                actual: count,
                max: limits.max_assignments_per_command,
            });
        }
        let mut assignments = Vec::with_capacity(count);
        for _ in 0..count {
            assignments.push(self.assignment(limits)?);
        }
        Ok(assignments)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn assignment(topic: TopicId, partition: u32, voters: Vec<u32>) -> AssignmentRecord {
        AssignmentRecord {
            topic_id: topic,
            partition_id: partition,
            assignment_generation: AssignmentGeneration::new(1).unwrap(),
            voters,
            learners: Vec::new(),
        }
    }

    #[test]
    fn all_command_variants_round_trip_canonically() {
        let topic = TopicId::new([3; 16]);
        let movement = MovementId::new([4; 16]).unwrap();
        let commands = vec![
            ControllerCommand::RegisterBroker {
                broker_id: 1,
                node_uuid: [1; 16],
                initial_incarnation: 1,
                client_host: "broker-a".into(),
                client_port: 9092,
                internal_host: "broker-a-internal".into(),
                internal_port: 9093,
                rack: Some("rack-a".into()),
                zone: Some("zone-a".into()),
                min_feature: 1,
                max_feature: 3,
            },
            ControllerCommand::RejoinBroker {
                broker_id: 1,
                node_uuid: [1; 16],
                expected_incarnation: 7,
                new_incarnation: 8,
                client_host: "broker-a".into(),
                client_port: 9092,
                internal_host: "broker-a-internal".into(),
                internal_port: 9093,
                rack: None,
                zone: None,
                min_feature: 1,
                max_feature: 3,
            },
            ControllerCommand::SetBrokerLifecycle {
                broker_id: 1,
                expected_incarnation: 8,
                expected_lifecycle: BrokerLifecycle::Joining,
                new_lifecycle: BrokerLifecycle::Active,
            },
            ControllerCommand::CreateTopic {
                topic_id: topic,
                topic_name: "orders".into(),
                partition_count: 1,
                replication_factor: 3,
                failure_domain_policy: FailureDomainPolicy::Required,
                assignments: vec![assignment(topic, 0, vec![1, 2, 3])],
            },
            ControllerCommand::BeginDeleteTopic {
                topic_id: topic,
                expected_topic_lifecycle: TopicLifecycle::Active,
            },
            ControllerCommand::FinalizeDeleteTopic {
                topic_id: topic,
                expected_topic_lifecycle: TopicLifecycle::Deleting,
            },
            ControllerCommand::ExpandTopic {
                topic_id: topic,
                expected_partition_count: 1,
                new_partition_count: 2,
                assignments: vec![assignment(topic, 1, vec![1, 2, 3])],
            },
            ControllerCommand::BeginMovement {
                movement_id: movement,
                topic_id: topic,
                partition_id: 0,
                expected_assignment_generation: AssignmentGeneration::new(1).unwrap(),
                source_broker_id: 1,
                destination_broker_id: 4,
                target_voters: vec![2, 3, 4],
                preferred_leader: Some(2),
            },
            ControllerCommand::AdvanceMovement {
                movement_id: movement,
                expected_stage: MovementStage::Planned,
                new_stage: MovementStage::LearnerRequested,
            },
            ControllerCommand::FinalizeMovement {
                movement_id: movement,
                expected_stage: MovementStage::OldReplicaRemoved,
                replacement: AssignmentRecord {
                    topic_id: topic,
                    partition_id: 0,
                    assignment_generation: AssignmentGeneration::new(2).unwrap(),
                    voters: vec![2, 3, 4],
                    learners: Vec::new(),
                },
            },
            ControllerCommand::SetFeatureLevel {
                expected_feature_level: 1,
                new_feature_level: 2,
            },
            ControllerCommand::AllocateProducerIds {
                allocation_count: 100,
            },
        ];

        for command in commands {
            let bytes = encode_command(&command, CommandLimits::default()).unwrap();
            assert_eq!(
                decode_command(&bytes, CommandLimits::default()).unwrap(),
                command
            );
        }
    }

    #[test]
    fn broker_feature_range_requires_nonzero_ordered_bounds() {
        let registration = |min_feature, max_feature| ControllerCommand::RegisterBroker {
            broker_id: 1,
            node_uuid: [1; 16],
            initial_incarnation: 1,
            client_host: "broker-a".into(),
            client_port: 9092,
            internal_host: "broker-a-internal".into(),
            internal_port: 9093,
            rack: None,
            zone: None,
            min_feature,
            max_feature,
        };
        let limits = CommandLimits::default();
        assert!(encode_command(&registration(0, 1), limits).is_err());
        assert!(encode_command(&registration(1, 0), limits).is_err());
        assert!(encode_command(&registration(2, 1), limits).is_err());
        assert!(encode_command(&registration(1, 1), limits).is_ok());
    }

    #[test]
    fn malformed_unknown_trailing_and_oversized_commands_are_rejected() {
        let limits = CommandLimits::default();
        assert!(decode_command(&[1, 0, 99, 0, 0, 0, 0, 0], limits).is_err());

        let command = ControllerCommand::AllocateProducerIds {
            allocation_count: 1,
        };
        let mut bytes = encode_command(&command, limits).unwrap();
        bytes.push(0);
        assert!(decode_command(&bytes, limits).is_err());

        let tiny = CommandLimits {
            max_command_bytes: 8,
            ..limits
        };
        assert!(encode_command(&command, tiny).is_err());
    }

    #[test]
    fn noncanonical_assignment_sets_are_rejected() {
        let topic = TopicId::new([8; 16]);
        let command = ControllerCommand::CreateTopic {
            topic_id: topic,
            topic_name: "orders".into(),
            partition_count: 1,
            replication_factor: 3,
            failure_domain_policy: FailureDomainPolicy::Required,
            assignments: vec![assignment(topic, 0, vec![2, 1, 3])],
        };
        assert!(encode_command(&command, CommandLimits::default()).is_err());
    }

    #[test]
    fn first_registration_incarnation_is_frozen_to_one() {
        let command = ControllerCommand::RegisterBroker {
            broker_id: 1,
            node_uuid: [1; 16],
            initial_incarnation: 2,
            client_host: "broker".into(),
            client_port: 9092,
            internal_host: "broker-internal".into(),
            internal_port: 9093,
            rack: None,
            zone: None,
            min_feature: 1,
            max_feature: 1,
        };
        assert!(encode_command(&command, CommandLimits::default()).is_err());
    }

    #[test]
    fn topic_partition_count_is_bounded_by_wire_v1() {
        let topic = TopicId::new([9; 16]);
        let create = ControllerCommand::CreateTopic {
            topic_id: topic,
            topic_name: "too-many".into(),
            partition_count: MAX_TOPIC_PARTITIONS + 1,
            replication_factor: 1,
            failure_domain_policy: FailureDomainPolicy::Disabled,
            assignments: (0..=MAX_TOPIC_PARTITIONS)
                .map(|partition| assignment(topic, partition, vec![1]))
                .collect(),
        };
        assert!(encode_command(&create, CommandLimits::default()).is_err());

        let expand = ControllerCommand::ExpandTopic {
            topic_id: topic,
            expected_partition_count: MAX_TOPIC_PARTITIONS,
            new_partition_count: MAX_TOPIC_PARTITIONS + 1,
            assignments: vec![assignment(topic, MAX_TOPIC_PARTITIONS, vec![1])],
        };
        assert!(encode_command(&expand, CommandLimits::default()).is_err());
    }

    #[test]
    fn frozen_small_command_goldens_are_exact() {
        let allocate = ControllerCommand::AllocateProducerIds {
            allocation_count: 32,
        };
        assert_eq!(
            encode_command(&allocate, CommandLimits::default()).unwrap(),
            hex("01000c000400000020000000")
        );

        let feature = ControllerCommand::SetFeatureLevel {
            expected_feature_level: 1,
            new_feature_level: 2,
        };
        assert_eq!(
            encode_command(&feature, CommandLimits::default()).unwrap(),
            hex("01000b000400000001000200")
        );
    }

    #[test]
    fn producer_id_allocation_count_is_hard_bounded_without_changing_v1_bytes() {
        assert!(
            encode_command(
                &ControllerCommand::AllocateProducerIds {
                    allocation_count: 0,
                },
                CommandLimits::default(),
            )
            .is_err()
        );
        assert!(
            encode_command(
                &ControllerCommand::AllocateProducerIds {
                    allocation_count: MAX_PRODUCER_IDS_PER_ALLOCATION,
                },
                CommandLimits::default(),
            )
            .is_ok()
        );
        assert!(
            encode_command(
                &ControllerCommand::AllocateProducerIds {
                    allocation_count: MAX_PRODUCER_IDS_PER_ALLOCATION + 1,
                },
                CommandLimits::default(),
            )
            .is_err()
        );
    }

    fn hex(value: &str) -> Vec<u8> {
        let bytes = value.as_bytes();
        assert_eq!(bytes.len() % 2, 0);
        (0..bytes.len())
            .step_by(2)
            .map(|index| (nibble(bytes[index]) << 4) | nibble(bytes[index + 1]))
            .collect()
    }

    fn nibble(value: u8) -> u8 {
        match value {
            b'0'..=b'9' => value - b'0',
            b'a'..=b'f' => value - b'a' + 10,
            _ => panic!("invalid test hex"),
        }
    }
}
