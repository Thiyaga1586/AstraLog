use std::{
    collections::{BTreeMap, BTreeSet},
    net::SocketAddr,
    sync::Arc,
    time::Duration,
};

use astralog_common::TopicId;
use astralog_raft::RaftMessage;
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{TcpListener, TcpStream},
    sync::{Semaphore, broadcast, mpsc, mpsc::error::TrySendError},
    task::JoinHandle,
    time,
};

use crate::{
    CleanProgressReport, InternalFrame, InternalFrameDecoder, ReplicationError, Result,
    TransportLimits, encode_internal_frame,
};

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct GroupKey {
    pub topic_id: TopicId,
    pub partition: u32,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PeerInboundFrame {
    pub frame: InternalFrame,
    pub connection_generation: u64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PeerDescriptor {
    pub broker_id: u32,
    pub incarnation: u64,
    pub address: SocketAddr,
}

#[derive(Debug, Clone)]
pub struct PeerTransportConfig {
    pub local_broker_id: u32,
    pub local_incarnation: u64,
    pub bind_addr: SocketAddr,
    pub peers: BTreeMap<u32, PeerDescriptor>,
    pub allowed_groups: BTreeSet<GroupKey>,
    pub limits: TransportLimits,
    pub max_inbound_frames: usize,
    pub max_outbound_frames_per_peer: usize,
    pub max_outbound_bytes_per_peer: usize,
    pub reconnect_min_delay: Duration,
    pub reconnect_max_delay: Duration,
}

impl PeerTransportConfig {
    pub fn validate(&self) -> Result<()> {
        // Peer IDs and CRCs are not cryptographic authentication. Until real
        // TLS/mutual identity exists, never accept Raft frames from a LAN.
        if !self.bind_addr.ip().is_loopback() {
            return Err(ReplicationError::Invalid(
                "unauthenticated replication transport must bind to loopback",
            ));
        }
        if self.local_broker_id == u32::MAX {
            return Err(ReplicationError::Invalid(
                "u32::MAX broker id cannot map to P3 NodeId",
            ));
        }
        if self.max_inbound_frames == 0
            || self.max_outbound_frames_per_peer == 0
            || self.max_outbound_bytes_per_peer == 0
        {
            return Err(ReplicationError::Invalid(
                "peer transport queue limits must be non-zero",
            ));
        }
        if self.reconnect_min_delay.is_zero() || self.reconnect_max_delay < self.reconnect_min_delay
        {
            return Err(ReplicationError::Invalid(
                "invalid peer transport reconnect delay bounds",
            ));
        }
        for (key, peer) in &self.peers {
            if *key != peer.broker_id || peer.broker_id == self.local_broker_id {
                return Err(ReplicationError::Invalid(
                    "peer map key/identity is invalid",
                ));
            }
        }
        Ok(())
    }
}

struct QueuedFrame {
    bytes: Vec<u8>,
    _byte_budget: tokio::sync::OwnedSemaphorePermit,
}

#[derive(Clone)]
struct PeerSender {
    tx: mpsc::Sender<QueuedFrame>,
    byte_budget: Arc<Semaphore>,
}

pub struct PeerTransportHandle {
    local_addr: SocketAddr,
    sender: PeerTransportSender,
    shutdown: broadcast::Sender<()>,
    tasks: Vec<JoinHandle<()>>,
}

#[derive(Clone)]
pub struct PeerTransportSender {
    local_broker_id: u32,
    local_incarnation: u64,
    limits: TransportLimits,
    peers: BTreeMap<u32, PeerSender>,
}

impl PeerTransportSender {
    pub fn try_send(
        &self,
        target_broker_id: u32,
        group: GroupKey,
        message: RaftMessage,
    ) -> Result<()> {
        let peer = self
            .peers
            .get(&target_broker_id)
            .ok_or(ReplicationError::Invalid("unknown Raft transport peer"))?;
        let bytes = encode_internal_frame(
            &InternalFrame {
                source_broker_id: self.local_broker_id,
                source_incarnation: self.local_incarnation,
                topic_id: group.topic_id,
                partition: group.partition,
                message: Some(message),
                rebootstrap: None,
                clean_progress: None,
            },
            self.limits,
        )?;
        self.enqueue(peer, bytes)
    }

    pub fn try_send_rebootstrap(
        &self,
        target_broker_id: u32,
        group: GroupKey,
        message: RaftMessage,
        package: Vec<u8>,
    ) -> Result<()> {
        let peer = self
            .peers
            .get(&target_broker_id)
            .ok_or(ReplicationError::Invalid("unknown Raft transport peer"))?;
        let bytes = encode_internal_frame(
            &InternalFrame {
                source_broker_id: self.local_broker_id,
                source_incarnation: self.local_incarnation,
                topic_id: group.topic_id,
                partition: group.partition,
                message: Some(message),
                rebootstrap: Some(package),
                clean_progress: None,
            },
            self.limits,
        )?;
        self.enqueue(peer, bytes)
    }

    pub fn try_send_clean_progress(
        &self,
        target_broker_id: u32,
        group: GroupKey,
        report: CleanProgressReport,
    ) -> Result<()> {
        let peer = self
            .peers
            .get(&target_broker_id)
            .ok_or(ReplicationError::Invalid("unknown P9 clean-progress peer"))?;
        let bytes = encode_internal_frame(
            &InternalFrame {
                source_broker_id: self.local_broker_id,
                source_incarnation: self.local_incarnation,
                topic_id: group.topic_id,
                partition: group.partition,
                message: None,
                rebootstrap: None,
                clean_progress: Some(report),
            },
            self.limits,
        )?;
        self.enqueue(peer, bytes)
    }

    fn enqueue(&self, peer: &PeerSender, bytes: Vec<u8>) -> Result<()> {
        let permits = u32::try_from(bytes.len())
            .map_err(|_| ReplicationError::Invalid("transport frame byte count exceeds u32"))?;
        let permit = peer
            .byte_budget
            .clone()
            .try_acquire_many_owned(permits)
            .map_err(|_| ReplicationError::Busy("peer outbound byte budget exhausted"))?;
        match peer.tx.try_send(QueuedFrame {
            bytes,
            _byte_budget: permit,
        }) {
            Ok(()) => Ok(()),
            Err(TrySendError::Full(_)) => {
                Err(ReplicationError::Busy("peer outbound frame queue is full"))
            }
            Err(TrySendError::Closed(_)) => Err(ReplicationError::Busy(
                "peer outbound transport task is closed",
            )),
        }
    }
}

impl PeerTransportHandle {
    pub const fn local_addr(&self) -> SocketAddr {
        self.local_addr
    }

    pub fn try_send(
        &self,
        target_broker_id: u32,
        group: GroupKey,
        message: RaftMessage,
    ) -> Result<()> {
        self.sender.try_send(target_broker_id, group, message)
    }

    pub fn try_send_rebootstrap(
        &self,
        target_broker_id: u32,
        group: GroupKey,
        message: RaftMessage,
        package: Vec<u8>,
    ) -> Result<()> {
        self.sender
            .try_send_rebootstrap(target_broker_id, group, message, package)
    }

    pub fn sender(&self) -> PeerTransportSender {
        self.sender.clone()
    }

    pub async fn shutdown(self) {
        let _ = self.shutdown.send(());
        for task in self.tasks {
            let _ = task.await;
        }
    }
}

pub async fn start_peer_transport(
    config: PeerTransportConfig,
) -> Result<(PeerTransportHandle, mpsc::Receiver<PeerInboundFrame>)> {
    config.validate()?;
    let listener = TcpListener::bind(config.bind_addr).await?;
    let local_addr = listener.local_addr()?;
    let (inbound_tx, inbound_rx) = mpsc::channel(config.max_inbound_frames);
    let (shutdown, _) = broadcast::channel::<()>(1);
    let peers = Arc::new(config.peers.clone());
    let allowed_groups = Arc::new(config.allowed_groups.clone());

    let mut tasks = Vec::new();
    let listener_shutdown = shutdown.clone();
    let listener_peers = peers.clone();
    let listener_groups = allowed_groups.clone();
    let listener_limits = config.limits;
    let listener_inbound = inbound_tx.clone();
    tasks.push(tokio::spawn(async move {
        run_listener(
            listener,
            listener_peers,
            listener_groups,
            listener_limits,
            listener_inbound,
            listener_shutdown,
        )
        .await;
    }));

    let mut senders = BTreeMap::new();
    for peer in config.peers.values().copied() {
        let (tx, rx) = mpsc::channel(config.max_outbound_frames_per_peer);
        let byte_budget = Arc::new(Semaphore::new(config.max_outbound_bytes_per_peer));
        senders.insert(
            peer.broker_id,
            PeerSender {
                tx,
                byte_budget: byte_budget.clone(),
            },
        );
        let writer_shutdown = shutdown.subscribe();
        let min_delay = config.reconnect_min_delay;
        let max_delay = config.reconnect_max_delay;
        tasks.push(tokio::spawn(async move {
            run_peer_writer(peer, rx, writer_shutdown, min_delay, max_delay).await;
        }));
    }

    Ok((
        PeerTransportHandle {
            local_addr,
            sender: PeerTransportSender {
                local_broker_id: config.local_broker_id,
                local_incarnation: config.local_incarnation,
                limits: config.limits,
                peers: senders,
            },
            shutdown,
            tasks,
        },
        inbound_rx,
    ))
}

async fn run_listener(
    listener: TcpListener,
    peers: Arc<BTreeMap<u32, PeerDescriptor>>,
    allowed_groups: Arc<BTreeSet<GroupKey>>,
    limits: TransportLimits,
    inbound: mpsc::Sender<PeerInboundFrame>,
    shutdown: broadcast::Sender<()>,
) {
    let mut shutdown_rx = shutdown.subscribe();
    let mut next_connection_generation = 1_u64;
    loop {
        tokio::select! {
            _ = shutdown_rx.recv() => return,
            accepted = listener.accept() => {
                let Ok((stream, _)) = accepted else { continue; };
                let connection_generation = next_connection_generation;
                next_connection_generation = next_connection_generation.saturating_add(1).max(1);
                let connection_peers = peers.clone();
                let connection_groups = allowed_groups.clone();
                let connection_inbound = inbound.clone();
                let connection_shutdown = shutdown.subscribe();
                tokio::spawn(async move {
                    let _ = read_connection(
                        stream,
                        connection_peers,
                        connection_groups,
                        limits,
                        connection_generation,
                        connection_inbound,
                        connection_shutdown,
                    ).await;
                });
            }
        }
    }
}

async fn read_connection(
    mut stream: TcpStream,
    peers: Arc<BTreeMap<u32, PeerDescriptor>>,
    allowed_groups: Arc<BTreeSet<GroupKey>>,
    limits: TransportLimits,
    connection_generation: u64,
    inbound: mpsc::Sender<PeerInboundFrame>,
    mut shutdown: broadcast::Receiver<()>,
) -> Result<()> {
    let mut decoder = InternalFrameDecoder::new(limits);
    let mut buffer = [0_u8; 8192];
    let mut connection_source = None;
    loop {
        let read = tokio::select! {
            _ = shutdown.recv() => return Ok(()),
            read = stream.read(&mut buffer) => read?,
        };
        if read == 0 {
            return Ok(());
        }
        for frame in decoder.push(&buffer[..read])? {
            let expected = peers
                .get(&frame.source_broker_id)
                .ok_or(ReplicationError::Invalid(
                    "frame source is not a configured peer",
                ))?;
            if expected.incarnation != frame.source_incarnation {
                return Err(ReplicationError::Invalid(
                    "frame source incarnation does not match configured peer",
                ));
            }
            if connection_source.is_some_and(|source| source != frame.source_broker_id) {
                return Err(ReplicationError::Invalid(
                    "one internal connection cannot mix source broker identities",
                ));
            }
            connection_source = Some(frame.source_broker_id);
            if !allowed_groups.contains(&GroupKey {
                topic_id: frame.topic_id,
                partition: frame.partition,
            }) {
                return Err(ReplicationError::Invalid(
                    "internal frame targets an unassigned Raft group",
                ));
            }
            inbound
                .send(PeerInboundFrame {
                    frame,
                    connection_generation,
                })
                .await
                .map_err(|_| ReplicationError::Busy("inbound Raft consumer is closed"))?;
        }
    }
}

async fn run_peer_writer(
    peer: PeerDescriptor,
    mut outbound: mpsc::Receiver<QueuedFrame>,
    mut shutdown: broadcast::Receiver<()>,
    min_delay: Duration,
    max_delay: Duration,
) {
    let mut stream = None;
    let mut backoff = min_delay;
    let mut current: Option<QueuedFrame> = None;
    loop {
        if current.is_none() {
            current = tokio::select! {
                _ = shutdown.recv() => return,
                frame = outbound.recv() => frame,
            };
            if current.is_none() {
                return;
            }
        }
        if stream.is_none() {
            let connect = tokio::select! {
                _ = shutdown.recv() => return,
                connect = TcpStream::connect(peer.address) => connect,
            };
            match connect {
                Ok(connected) => {
                    stream = Some(connected);
                    backoff = min_delay;
                }
                Err(_) => {
                    tokio::select! {
                        _ = shutdown.recv() => return,
                        _ = time::sleep(backoff) => {}
                    }
                    backoff = backoff.saturating_mul(2).min(max_delay);
                    continue;
                }
            }
        }
        let frame = current.as_ref().expect("current frame exists");
        let write = tokio::select! {
            _ = shutdown.recv() => return,
            write = stream.as_mut().expect("stream exists").write_all(&frame.bytes) => write,
        };
        match write {
            Ok(()) => current = None,
            Err(_) => {
                stream = None;
                tokio::select! {
                    _ = shutdown.recv() => return,
                    _ = time::sleep(backoff) => {}
                }
                backoff = backoff.saturating_mul(2).min(max_delay);
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use std::net::{IpAddr, Ipv4Addr};
    use std::sync::atomic::{AtomicU32, Ordering};

    use astralog_raft::{
        InstallSnapshotRequest, LogIndex, NodeId, PreVoteRequest, RaftMessage, SnapshotId,
        SnapshotMetadata, StableConfiguration, Term,
    };

    use super::*;

    fn reserve_addr() -> SocketAddr {
        static NEXT_LOOPBACK: AtomicU32 = AtomicU32::new(0);
        let slot = NEXT_LOOPBACK.fetch_add(1, Ordering::Relaxed);
        let process = std::process::id();
        let second = u8::try_from((process >> 8) & 0xff).unwrap();
        let third = u8::try_from(process & 0xff).unwrap();
        let fourth = u8::try_from(1 + slot % 250).unwrap();
        let listener =
            std::net::TcpListener::bind((Ipv4Addr::new(127, second, third, fourth), 0)).unwrap();
        let address = listener.local_addr().unwrap();
        drop(listener);
        address
    }

    fn message(node: u64) -> RaftMessage {
        RaftMessage::PreVoteRequest(PreVoteRequest {
            prospective_term: Term::new(2),
            candidate_id: NodeId::new(node).unwrap(),
            last_log_index: LogIndex::ZERO,
            last_log_term: Term::ZERO,
        })
    }

    fn config(
        local: u32,
        incarnation: u64,
        bind_addr: SocketAddr,
        peer: PeerDescriptor,
        groups: BTreeSet<GroupKey>,
    ) -> PeerTransportConfig {
        PeerTransportConfig {
            local_broker_id: local,
            local_incarnation: incarnation,
            bind_addr,
            peers: [(peer.broker_id, peer)].into_iter().collect(),
            allowed_groups: groups,
            limits: TransportLimits::default(),
            max_inbound_frames: 16,
            max_outbound_frames_per_peer: 16,
            max_outbound_bytes_per_peer: 1024 * 1024,
            reconnect_min_delay: Duration::from_millis(5),
            reconnect_max_delay: Duration::from_millis(20),
        }
    }

    #[test]
    fn unauthenticated_raft_listener_rejects_remote_interfaces() {
        let peer = PeerDescriptor {
            broker_id: 2,
            incarnation: 1,
            address: "127.0.0.1:9093".parse().unwrap(),
        };
        let mut settings = config(1, 1, "127.0.0.1:0".parse().unwrap(), peer, BTreeSet::new());
        assert!(settings.validate().is_ok());
        settings.bind_addr = "0.0.0.0:9093".parse().unwrap();
        assert!(settings.validate().is_err());
        settings.bind_addr = "192.0.2.10:9093".parse().unwrap();
        assert!(settings.validate().is_err());
    }

    #[tokio::test]
    async fn persistent_peer_channel_multiplexes_groups() {
        let addr_a = reserve_addr();
        let addr_b = reserve_addr();
        let topic = TopicId::new([3; 16]);
        let g0 = GroupKey {
            topic_id: topic,
            partition: 0,
        };
        let g1 = GroupKey {
            topic_id: topic,
            partition: 1,
        };
        let groups = [g0, g1].into_iter().collect::<BTreeSet<_>>();
        let peer_a = PeerDescriptor {
            broker_id: 1,
            incarnation: 10,
            address: addr_a,
        };
        let peer_b = PeerDescriptor {
            broker_id: 2,
            incarnation: 20,
            address: addr_b,
        };
        let (transport_a, _rx_a) =
            start_peer_transport(config(1, 10, addr_a, peer_b, groups.clone()))
                .await
                .unwrap();
        let (transport_b, mut rx_b) = start_peer_transport(config(2, 20, addr_b, peer_a, groups))
            .await
            .unwrap();

        transport_a.try_send(2, g0, message(1)).unwrap();
        transport_a.try_send(2, g1, message(1)).unwrap();
        let first = time::timeout(Duration::from_secs(2), rx_b.recv())
            .await
            .unwrap()
            .unwrap();
        let second = time::timeout(Duration::from_secs(2), rx_b.recv())
            .await
            .unwrap()
            .unwrap();
        assert_eq!(first.frame.source_broker_id, 1);
        assert_eq!(second.frame.source_broker_id, 1);
        assert_eq!([first.frame.partition, second.frame.partition], [0, 1]);
        assert!(first.connection_generation > 0);
        assert!(second.connection_generation > 0);

        transport_a.shutdown().await;
        transport_b.shutdown().await;
    }

    #[tokio::test]
    async fn peer_channel_carries_rebootstrap_payload_with_install_snapshot() {
        let addr_a = reserve_addr();
        let addr_b = reserve_addr();
        let topic = TopicId::new([8; 16]);
        let group = GroupKey {
            topic_id: topic,
            partition: 0,
        };
        let peer_a = PeerDescriptor {
            broker_id: 1,
            incarnation: 10,
            address: addr_a,
        };
        let peer_b = PeerDescriptor {
            broker_id: 2,
            incarnation: 20,
            address: addr_b,
        };
        let groups = [group].into_iter().collect::<BTreeSet<_>>();
        let (transport_a, _rx_a) =
            start_peer_transport(config(1, 10, addr_a, peer_b, groups.clone()))
                .await
                .unwrap();
        let (transport_b, mut rx_b) = start_peer_transport(config(2, 20, addr_b, peer_a, groups))
            .await
            .unwrap();
        let snapshot = SnapshotMetadata::new(
            LogIndex::new(3),
            Term::new(2),
            StableConfiguration::new([NodeId::new(1).unwrap(), NodeId::new(2).unwrap()], [])
                .unwrap(),
            SnapshotId::new([9; 16]),
        )
        .unwrap();
        let message = RaftMessage::InstallSnapshotRequest(InstallSnapshotRequest {
            term: Term::new(3),
            leader_id: NodeId::new(1).unwrap(),
            snapshot,
        });
        let package = vec![0xA5; 256];
        transport_a
            .try_send_rebootstrap(2, group, message.clone(), package.clone())
            .unwrap();
        let received = time::timeout(Duration::from_secs(2), rx_b.recv())
            .await
            .unwrap()
            .unwrap();
        assert_eq!(received.frame.topic_id, topic);
        assert_eq!(received.frame.partition, 0);
        assert_eq!(received.frame.message, Some(message));
        assert_eq!(received.frame.rebootstrap, Some(package));
        assert!(received.connection_generation > 0);
        transport_a.shutdown().await;
        transport_b.shutdown().await;
    }

    #[tokio::test]
    async fn source_incarnation_and_group_are_fenced() {
        let addr_a = reserve_addr();
        let addr_b = reserve_addr();
        let topic = TopicId::new([4; 16]);
        let allowed = GroupKey {
            topic_id: topic,
            partition: 0,
        };
        let forbidden = GroupKey {
            topic_id: topic,
            partition: 1,
        };
        let peer_a_expected = PeerDescriptor {
            broker_id: 1,
            incarnation: 10,
            address: addr_a,
        };
        let peer_b = PeerDescriptor {
            broker_id: 2,
            incarnation: 20,
            address: addr_b,
        };
        let (transport_b, mut rx_b) = start_peer_transport(config(
            2,
            20,
            addr_b,
            peer_a_expected,
            [allowed].into_iter().collect(),
        ))
        .await
        .unwrap();
        let (transport_a, _rx_a) = start_peer_transport(config(
            1,
            999,
            addr_a,
            peer_b,
            [allowed, forbidden].into_iter().collect(),
        ))
        .await
        .unwrap();

        transport_a.try_send(2, allowed, message(1)).unwrap();
        assert!(
            time::timeout(Duration::from_millis(150), rx_b.recv())
                .await
                .is_err()
        );
        transport_a.try_send(2, forbidden, message(1)).unwrap();
        assert!(
            time::timeout(Duration::from_millis(150), rx_b.recv())
                .await
                .is_err()
        );

        transport_a.shutdown().await;
        transport_b.shutdown().await;
    }

    #[tokio::test]
    async fn p9g_clean_progress_sideband_delivered_over_bounded_authenticated_peer_channel() {
        let addr_a = reserve_addr();
        let addr_b = reserve_addr();
        let topic_id = TopicId::new([0x99; 16]);
        let group = GroupKey {
            topic_id,
            partition: 5,
        };
        let peer_a = PeerDescriptor {
            broker_id: 1,
            incarnation: 10,
            address: addr_a,
        };
        let peer_b = PeerDescriptor {
            broker_id: 2,
            incarnation: 20,
            address: addr_b,
        };
        let groups = [group].into_iter().collect::<BTreeSet<_>>();
        let (sender, _rx_a) = start_peer_transport(config(1, 10, addr_a, peer_b, groups.clone()))
            .await
            .unwrap();
        let (receiver, mut rx_b) = start_peer_transport(config(2, 20, addr_b, peer_a, groups))
            .await
            .unwrap();
        let report = CleanProgressReport {
            topic_id,
            partition_id: astralog_common::PartitionId::new(5),
            term: 17,
            leader_epoch: 17,
            source_broker_id: 1,
            source_incarnation: 10,
            assignment_generation: 8,
            clean_compacted_offset: astralog_common::StreamOffset::new(37),
        };
        sender
            .sender()
            .try_send_clean_progress(2, group, report)
            .unwrap();
        let delivered = time::timeout(Duration::from_secs(2), rx_b.recv())
            .await
            .unwrap()
            .unwrap();
        assert_eq!(delivered.frame.message, None);
        assert_eq!(delivered.frame.rebootstrap, None);
        assert_eq!(delivered.frame.clean_progress, Some(report));
        assert!(delivered.connection_generation > 0);
        assert!(
            sender
                .sender()
                .try_send_clean_progress(
                    2,
                    group,
                    CleanProgressReport {
                        assignment_generation: 0,
                        ..report
                    },
                )
                .is_err(),
            "invalid assignment generation cannot go on the wire"
        );
        sender.shutdown().await;
        receiver.shutdown().await;
    }

    #[tokio::test]
    async fn p9g_rf3_authenticated_network_progress_blocks_missing_voter_and_stale_incarnation() {
        use crate::{CleanProgressTracker, VoterIdentity};
        use astralog_common::{PartitionId, StreamOffset};
        use astralog_raft::EffectiveConfiguration;

        let addresses = [reserve_addr(), reserve_addr(), reserve_addr()];
        let topic_id = TopicId::new([0xc3; 16]);
        let group = GroupKey {
            topic_id,
            partition: 0,
        };
        let peers = (0..3_u32)
            .map(|id| PeerDescriptor {
                broker_id: id,
                incarnation: u64::from(id) + 10,
                address: addresses[id as usize],
            })
            .collect::<Vec<_>>();
        let mut handles = Vec::new();
        let mut receivers = Vec::new();
        for me in &peers {
            let mut all = config(
                me.broker_id,
                me.incarnation,
                me.address,
                *peers
                    .iter()
                    .find(|peer| peer.broker_id != me.broker_id)
                    .unwrap(),
                [group].into_iter().collect(),
            );
            all.peers = peers
                .iter()
                .filter(|peer| peer.broker_id != me.broker_id)
                .map(|peer| (peer.broker_id, *peer))
                .collect();
            let (handle, rx) = start_peer_transport(all).await.unwrap();
            handles.push(handle);
            receivers.push(rx);
        }

        let voters = StableConfiguration::new(
            [
                NodeId::new(1).unwrap(),
                NodeId::new(2).unwrap(),
                NodeId::new(3).unwrap(),
            ],
            [],
        )
        .unwrap();
        let effective = EffectiveConfiguration::Stable(voters);
        let identities = (0..3_u32)
            .map(|id| {
                (
                    NodeId::new(u64::from(id) + 1).unwrap(),
                    VoterIdentity {
                        broker_id: id,
                        incarnation: u64::from(id) + 10,
                    },
                )
            })
            .collect();
        let mut tracker = CleanProgressTracker::from_committed_assignment(
            topic_id,
            PartitionId::new(0),
            7,
            13,
            &effective,
            &identities,
        )
        .unwrap();
        let report = |id: u32, clean: u64| CleanProgressReport {
            topic_id,
            partition_id: PartitionId::new(0),
            term: 7,
            leader_epoch: 7,
            source_broker_id: id,
            source_incarnation: u64::from(id) + 10,
            assignment_generation: 13,
            clean_compacted_offset: StreamOffset::new(clean),
        };
        assert!(tracker.accept(
            NodeId::new(1).unwrap(),
            0,
            10,
            report(0, 9),
            StreamOffset::new(20)
        ));
        assert_eq!(tracker.candidate_safe_offset(), None);

        handles[1]
            .sender()
            .try_send_clean_progress(0, group, report(1, 8))
            .unwrap();
        let first = time::timeout(Duration::from_secs(2), receivers[0].recv())
            .await
            .unwrap()
            .unwrap();
        assert_eq!(first.frame.clean_progress, Some(report(1, 8)));
        assert!(tracker.accept(
            NodeId::new(2).unwrap(),
            first.frame.source_broker_id,
            first.frame.source_incarnation,
            first.frame.clean_progress.unwrap(),
            StreamOffset::new(20),
        ));
        assert_eq!(
            tracker.candidate_safe_offset(),
            None,
            "one missing live RF3 eligible voter must block progress"
        );
        handles[2]
            .sender()
            .try_send_clean_progress(0, group, report(2, 7))
            .unwrap();
        let second = time::timeout(Duration::from_secs(2), receivers[0].recv())
            .await
            .unwrap()
            .unwrap();
        assert!(tracker.accept(
            NodeId::new(3).unwrap(),
            second.frame.source_broker_id,
            second.frame.source_incarnation,
            second.frame.clean_progress.unwrap(),
            StreamOffset::new(20),
        ));
        assert_eq!(tracker.candidate_safe_offset(), Some(StreamOffset::new(7)));

        tracker.advance_observation_tick(1);
        assert_eq!(
            tracker.candidate_safe_offset(),
            None,
            "offline RF3 reports expire instead of granting stale authority"
        );
        assert!(!tracker.accept(
            NodeId::new(3).unwrap(),
            2,
            9,
            report(2, 7),
            StreamOffset::new(20)
        ));
        assert_eq!(tracker.candidate_safe_offset(), None);
        for handle in handles {
            handle.shutdown().await;
        }
    }

    #[test]
    fn queue_byte_budget_is_nonzero_and_config_validation_is_strict() {
        let address = SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 1);
        let peer = PeerDescriptor {
            broker_id: 2,
            incarnation: 1,
            address,
        };
        let mut config = config(1, 1, address, peer, BTreeSet::new());
        config.max_outbound_bytes_per_peer = 0;
        assert!(config.validate().is_err());
    }

    #[test]
    fn physical_peer_queue_and_byte_budget_return_busy_without_unbounded_growth() {
        let (tx, _rx) = mpsc::channel(1);
        let sender = PeerTransportSender {
            local_broker_id: 1,
            local_incarnation: 10,
            limits: TransportLimits::default(),
            peers: [(
                2,
                PeerSender {
                    tx,
                    byte_budget: Arc::new(Semaphore::new(1024 * 1024)),
                },
            )]
            .into_iter()
            .collect(),
        };
        let group = GroupKey {
            topic_id: TopicId::new([7; 16]),
            partition: 0,
        };
        sender.try_send(2, group, message(1)).unwrap();
        assert!(matches!(
            sender.try_send(2, group, message(1)),
            Err(ReplicationError::Busy("peer outbound frame queue is full"))
        ));

        let (tx, _rx) = mpsc::channel(1);
        let byte_limited = PeerTransportSender {
            local_broker_id: 1,
            local_incarnation: 10,
            limits: TransportLimits::default(),
            peers: [(
                2,
                PeerSender {
                    tx,
                    byte_budget: Arc::new(Semaphore::new(1)),
                },
            )]
            .into_iter()
            .collect(),
        };
        assert!(matches!(
            byte_limited.try_send(2, group, message(1)),
            Err(ReplicationError::Busy(
                "peer outbound byte budget exhausted"
            ))
        ));
    }
}
