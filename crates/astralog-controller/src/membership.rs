use crate::{
    ApplyOutcome, BrokerLifecycle, BrokerRecord, ControllerCommand, ControllerError,
    ControllerNodeHandle, Result,
};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BrokerRegistration {
    pub broker_id: u32,
    pub node_uuid: [u8; 16],
    pub client_host: String,
    pub client_port: u16,
    pub internal_host: String,
    pub internal_port: u16,
    pub rack: Option<String>,
    pub zone: Option<String>,
    pub min_feature: u16,
    pub max_feature: u16,
}

impl BrokerRegistration {
    pub(crate) fn into_command(self) -> ControllerCommand {
        ControllerCommand::RegisterBroker {
            broker_id: self.broker_id,
            node_uuid: self.node_uuid,
            initial_incarnation: 1,
            client_host: self.client_host,
            client_port: self.client_port,
            internal_host: self.internal_host,
            internal_port: self.internal_port,
            rack: self.rack,
            zone: self.zone,
            min_feature: self.min_feature,
            max_feature: self.max_feature,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BrokerRejoin {
    pub broker_id: u32,
    pub node_uuid: [u8; 16],
    pub expected_incarnation: u64,
    pub client_host: String,
    pub client_port: u16,
    pub internal_host: String,
    pub internal_port: u16,
    pub rack: Option<String>,
    pub zone: Option<String>,
    pub min_feature: u16,
    pub max_feature: u16,
}

impl BrokerRejoin {
    pub fn next_incarnation(&self) -> Result<u64> {
        self.expected_incarnation
            .checked_add(1)
            .ok_or(ControllerError::Invalid("broker incarnation overflow"))
    }

    pub(crate) fn into_command(self) -> Result<ControllerCommand> {
        let new_incarnation = self.next_incarnation()?;
        Ok(ControllerCommand::RejoinBroker {
            broker_id: self.broker_id,
            node_uuid: self.node_uuid,
            expected_incarnation: self.expected_incarnation,
            new_incarnation,
            client_host: self.client_host,
            client_port: self.client_port,
            internal_host: self.internal_host,
            internal_port: self.internal_port,
            rack: self.rack,
            zone: self.zone,
            min_feature: self.min_feature,
            max_feature: self.max_feature,
        })
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct BrokerLifecycleChange {
    pub broker_id: u32,
    pub expected_incarnation: u64,
    pub expected_lifecycle: BrokerLifecycle,
    pub new_lifecycle: BrokerLifecycle,
}

impl BrokerLifecycleChange {
    pub(crate) fn into_command(self) -> ControllerCommand {
        ControllerCommand::SetBrokerLifecycle {
            broker_id: self.broker_id,
            expected_incarnation: self.expected_incarnation,
            expected_lifecycle: self.expected_lifecycle,
            new_lifecycle: self.new_lifecycle,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct BrokerAuthority {
    pub broker_id: u32,
    pub node_uuid: [u8; 16],
    pub incarnation_epoch: u64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BrokerMembershipMutation {
    pub outcome: ApplyOutcome,
    pub broker: BrokerRecord,
}

impl ControllerNodeHandle {
    pub async fn register_broker(
        &self,
        registration: BrokerRegistration,
    ) -> Result<BrokerMembershipMutation> {
        let broker_id = registration.broker_id;
        let outcome = self.mutate(registration.into_command()).await?;
        let broker = self
            .broker_record(broker_id)
            .await?
            .ok_or(ControllerError::Invalid(
                "committed broker registration is missing from controller state",
            ))?;
        Ok(BrokerMembershipMutation { outcome, broker })
    }

    pub async fn rejoin_broker(&self, rejoin: BrokerRejoin) -> Result<BrokerMembershipMutation> {
        let broker_id = rejoin.broker_id;
        let outcome = self.mutate(rejoin.into_command()?).await?;
        let broker = self
            .broker_record(broker_id)
            .await?
            .ok_or(ControllerError::Invalid(
                "committed broker rejoin is missing from controller state",
            ))?;
        Ok(BrokerMembershipMutation { outcome, broker })
    }

    pub async fn change_broker_lifecycle(
        &self,
        change: BrokerLifecycleChange,
    ) -> Result<BrokerMembershipMutation> {
        let broker_id = change.broker_id;
        let outcome = self.mutate(change.into_command()).await?;
        let broker = self
            .broker_record(broker_id)
            .await?
            .ok_or(ControllerError::Invalid(
                "committed broker lifecycle change is missing from controller state",
            ))?;
        Ok(BrokerMembershipMutation { outcome, broker })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn registration() -> BrokerRegistration {
        BrokerRegistration {
            broker_id: 7,
            node_uuid: [7; 16],
            client_host: "broker-7".into(),
            client_port: 9092,
            internal_host: "broker-7-internal".into(),
            internal_port: 9093,
            rack: Some("rack-a".into()),
            zone: Some("zone-a".into()),
            min_feature: 1,
            max_feature: 3,
        }
    }

    #[test]
    fn registration_api_freezes_first_incarnation_to_one() {
        let command = registration().into_command();
        let ControllerCommand::RegisterBroker {
            broker_id,
            node_uuid,
            initial_incarnation,
            ..
        } = command
        else {
            panic!("registration builder emitted wrong command kind");
        };
        assert_eq!(broker_id, 7);
        assert_eq!(node_uuid, [7; 16]);
        assert_eq!(initial_incarnation, 1);
    }

    #[test]
    fn rejoin_api_derives_exact_next_incarnation_and_never_wraps() {
        let registration = registration();
        let rejoin = BrokerRejoin {
            broker_id: registration.broker_id,
            node_uuid: registration.node_uuid,
            expected_incarnation: 41,
            client_host: registration.client_host.clone(),
            client_port: registration.client_port,
            internal_host: registration.internal_host.clone(),
            internal_port: registration.internal_port,
            rack: registration.rack.clone(),
            zone: registration.zone.clone(),
            min_feature: registration.min_feature,
            max_feature: registration.max_feature,
        };
        assert_eq!(rejoin.next_incarnation().unwrap(), 42);
        assert!(matches!(
            rejoin.into_command().unwrap(),
            ControllerCommand::RejoinBroker {
                expected_incarnation: 41,
                new_incarnation: 42,
                ..
            }
        ));

        let overflow = BrokerRejoin {
            expected_incarnation: u64::MAX,
            ..BrokerRejoin {
                broker_id: registration.broker_id,
                node_uuid: registration.node_uuid,
                expected_incarnation: 1,
                client_host: registration.client_host,
                client_port: registration.client_port,
                internal_host: registration.internal_host,
                internal_port: registration.internal_port,
                rack: registration.rack,
                zone: registration.zone,
                min_feature: registration.min_feature,
                max_feature: registration.max_feature,
            }
        };
        assert!(overflow.next_incarnation().is_err());
        assert!(overflow.into_command().is_err());
    }
}
