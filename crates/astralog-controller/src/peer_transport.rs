use std::{collections::BTreeMap, net::SocketAddr, sync::Arc, time::Duration};

use astralog_raft::RaftMessage;
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{TcpListener, TcpStream},
    sync::{Semaphore, broadcast, mpsc, mpsc::error::TrySendError},
    task::JoinHandle,
    time,
};

use crate::{
    ControllerError, ControllerFrame, ControllerFrameDecoder, ControllerNodeId,
    ControllerTransportLimits, Result, encode_controller_frame,
};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ControllerPeerDescriptor {
    pub controller_id: ControllerNodeId,
    pub address: SocketAddr,
}

#[derive(Debug, Clone)]
pub struct ControllerPeerTransportConfig {
    pub local_controller_id: ControllerNodeId,
    pub cluster_id: [u8; 16],
    pub bind_addr: SocketAddr,
    pub peers: BTreeMap<ControllerNodeId, ControllerPeerDescriptor>,
    pub limits: ControllerTransportLimits,
    pub max_inbound_frames: usize,
    pub max_outbound_frames_per_peer: usize,
    pub max_outbound_bytes_per_peer: usize,
    pub reconnect_min_delay: Duration,
    pub reconnect_max_delay: Duration,
}

impl ControllerPeerTransportConfig {
    pub fn validate(&self) -> Result<()> {
        // Controller peer identity is an asserted protocol field, not TLS.
        // Restrict this educational cluster to loopback-only listeners.
        if !self.bind_addr.ip().is_loopback() {
            return Err(ControllerError::Invalid(
                "unauthenticated controller transport must bind to loopback",
            ));
        }
        self.limits.validate()?;
        if self.cluster_id == [0; 16] {
            return Err(ControllerError::Invalid(
                "controller peer transport cluster id cannot be zero",
            ));
        }
        if self.max_inbound_frames == 0
            || self.max_outbound_frames_per_peer == 0
            || self.max_outbound_bytes_per_peer == 0
        {
            return Err(ControllerError::Invalid(
                "controller peer transport queue limits must be non-zero",
            ));
        }
        if self.reconnect_min_delay.is_zero() || self.reconnect_max_delay < self.reconnect_min_delay
        {
            return Err(ControllerError::Invalid(
                "invalid controller reconnect delay bounds",
            ));
        }
        for (id, peer) in &self.peers {
            if *id != peer.controller_id || *id == self.local_controller_id {
                return Err(ControllerError::Invalid(
                    "controller peer map key/identity is invalid",
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

#[derive(Clone)]
pub struct ControllerPeerTransportSender {
    local_controller_id: ControllerNodeId,
    cluster_id: [u8; 16],
    limits: ControllerTransportLimits,
    peers: BTreeMap<ControllerNodeId, PeerSender>,
}

impl ControllerPeerTransportSender {
    pub fn try_send(
        &self,
        target: ControllerNodeId,
        message: RaftMessage,
        snapshot_package: Option<Vec<u8>>,
    ) -> Result<()> {
        let peer = self.peers.get(&target).ok_or(ControllerError::Invalid(
            "unknown controller transport peer",
        ))?;
        let bytes = encode_controller_frame(
            &ControllerFrame {
                cluster_id: self.cluster_id,
                source_controller_id: self.local_controller_id.get(),
                message,
                snapshot_package,
            },
            self.limits,
        )?;
        let permits = u32::try_from(bytes.len())
            .map_err(|_| ControllerError::Invalid("controller frame byte count exceeds u32"))?;
        let permit = peer
            .byte_budget
            .clone()
            .try_acquire_many_owned(permits)
            .map_err(|_| ControllerError::Busy("controller peer outbound byte budget exhausted"))?;
        match peer.tx.try_send(QueuedFrame {
            bytes,
            _byte_budget: permit,
        }) {
            Ok(()) => Ok(()),
            Err(TrySendError::Full(_)) => Err(ControllerError::Busy(
                "controller peer outbound frame queue is full",
            )),
            Err(TrySendError::Closed(_)) => Err(ControllerError::Busy(
                "controller peer outbound transport task is closed",
            )),
        }
    }
}

pub struct ControllerPeerTransportHandle {
    local_addr: SocketAddr,
    sender: ControllerPeerTransportSender,
    shutdown: broadcast::Sender<()>,
    tasks: Vec<JoinHandle<()>>,
}

impl ControllerPeerTransportHandle {
    pub const fn local_addr(&self) -> SocketAddr {
        self.local_addr
    }

    pub fn sender(&self) -> ControllerPeerTransportSender {
        self.sender.clone()
    }

    pub async fn shutdown(self) {
        let _ = self.shutdown.send(());
        for task in self.tasks {
            let _ = task.await;
        }
    }
}

pub async fn start_controller_peer_transport(
    config: ControllerPeerTransportConfig,
) -> Result<(
    ControllerPeerTransportHandle,
    mpsc::Receiver<ControllerFrame>,
)> {
    config.validate()?;
    let listener = TcpListener::bind(config.bind_addr).await?;
    let local_addr = listener.local_addr()?;
    let (inbound_tx, inbound_rx) = mpsc::channel(config.max_inbound_frames);
    let (shutdown, _) = broadcast::channel::<()>(1);
    let peers = Arc::new(config.peers.clone());

    let mut tasks = Vec::new();
    let listener_shutdown = shutdown.subscribe();
    let listener_shutdown_source = shutdown.clone();
    let listener_peers = peers.clone();
    let listener_inbound = inbound_tx.clone();
    let listener_limits = config.limits;
    let expected_cluster = config.cluster_id;
    tasks.push(tokio::spawn(async move {
        run_listener(
            listener,
            listener_peers,
            expected_cluster,
            listener_limits,
            listener_inbound,
            listener_shutdown,
            listener_shutdown_source,
        )
        .await;
    }));

    let mut senders = BTreeMap::new();
    for peer in config.peers.values().copied() {
        let (tx, rx) = mpsc::channel(config.max_outbound_frames_per_peer);
        let byte_budget = Arc::new(Semaphore::new(config.max_outbound_bytes_per_peer));
        senders.insert(
            peer.controller_id,
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
        ControllerPeerTransportHandle {
            local_addr,
            sender: ControllerPeerTransportSender {
                local_controller_id: config.local_controller_id,
                cluster_id: config.cluster_id,
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
    peers: Arc<BTreeMap<ControllerNodeId, ControllerPeerDescriptor>>,
    cluster_id: [u8; 16],
    limits: ControllerTransportLimits,
    inbound: mpsc::Sender<ControllerFrame>,
    mut shutdown_rx: broadcast::Receiver<()>,
    shutdown: broadcast::Sender<()>,
) {
    loop {
        tokio::select! {
            _ = shutdown_rx.recv() => return,
            accepted = listener.accept() => {
                let Ok((stream, _)) = accepted else { continue; };
                let connection_peers = peers.clone();
                let connection_inbound = inbound.clone();
                let connection_shutdown = shutdown.subscribe();
                tokio::spawn(async move {
                    let _ = read_connection(
                        stream,
                        connection_peers,
                        cluster_id,
                        limits,
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
    peers: Arc<BTreeMap<ControllerNodeId, ControllerPeerDescriptor>>,
    cluster_id: [u8; 16],
    limits: ControllerTransportLimits,
    inbound: mpsc::Sender<ControllerFrame>,
    mut shutdown: broadcast::Receiver<()>,
) -> Result<()> {
    let mut decoder = ControllerFrameDecoder::new(limits);
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
            if frame.cluster_id != cluster_id {
                return Err(ControllerError::Invalid(
                    "controller frame belongs to another cluster/group namespace",
                ));
            }
            let source = ControllerNodeId::new(frame.source_controller_id)?;
            if !peers.contains_key(&source) {
                return Err(ControllerError::Invalid(
                    "controller frame source is not a configured peer",
                ));
            }
            if connection_source.is_some_and(|value| value != source) {
                return Err(ControllerError::Invalid(
                    "one controller connection cannot mix source identities",
                ));
            }
            connection_source = Some(source);
            inbound
                .send(frame)
                .await
                .map_err(|_| ControllerError::Busy("controller inbound consumer is closed"))?;
        }
    }
}

async fn run_peer_writer(
    peer: ControllerPeerDescriptor,
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
        let frame = current.as_ref().expect("current controller frame exists");
        let write = tokio::select! {
            _ = shutdown.recv() => return,
            write = stream.as_mut().expect("controller stream exists").write_all(&frame.bytes) => write,
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
    use std::net::Ipv4Addr;

    use astralog_raft::{LogIndex, NodeId, PreVoteRequest, Term};

    use super::*;

    #[test]
    fn unauthenticated_metadata_raft_listener_rejects_remote_interfaces() {
        let mut settings = ControllerPeerTransportConfig {
            local_controller_id: ControllerNodeId::new(1).unwrap(),
            cluster_id: [1; 16],
            bind_addr: "127.0.0.1:0".parse().unwrap(),
            peers: BTreeMap::new(),
            limits: ControllerTransportLimits::default(),
            max_inbound_frames: 8,
            max_outbound_frames_per_peer: 8,
            max_outbound_bytes_per_peer: 65536,
            reconnect_min_delay: Duration::from_millis(5),
            reconnect_max_delay: Duration::from_millis(20),
        };
        assert!(settings.validate().is_ok());
        settings.bind_addr = "0.0.0.0:9000".parse().unwrap();
        assert!(settings.validate().is_err());
        settings.bind_addr = "192.0.2.10:9000".parse().unwrap();
        assert!(settings.validate().is_err());
    }

    fn reserve_addr() -> SocketAddr {
        let listener = std::net::TcpListener::bind((Ipv4Addr::LOCALHOST, 0)).unwrap();
        let address = listener.local_addr().unwrap();
        drop(listener);
        address
    }

    fn reserve_addr_on(ip: Ipv4Addr) -> SocketAddr {
        let listener = std::net::TcpListener::bind((ip, 0)).unwrap();
        let address = listener.local_addr().unwrap();
        drop(listener);
        address
    }

    fn isolated_loopback(suffix: u8) -> Ipv4Addr {
        let process = std::process::id();
        Ipv4Addr::new(
            127,
            u8::try_from((process >> 8) & 0xff).unwrap(),
            u8::try_from(process & 0xff).unwrap(),
            suffix,
        )
    }

    fn message(node: u64) -> RaftMessage {
        RaftMessage::PreVoteRequest(PreVoteRequest {
            prospective_term: Term::new(2),
            candidate_id: NodeId::new(node).unwrap(),
            last_log_index: LogIndex::ZERO,
            last_log_term: Term::ZERO,
        })
    }

    #[tokio::test]
    async fn persistent_controller_channel_reconnects_and_fences_cluster_and_source() {
        let id1 = ControllerNodeId::new(1).unwrap();
        let id2 = ControllerNodeId::new(2).unwrap();
        // Keep this reconnect test off the 127.0.0.1 ephemeral-address pool used
        // by the other parallel controller transport/runtime tests. On Windows,
        // bind-then-release port reservation can otherwise race another test
        // between the intentional reconnect delay and the second listener bind.
        let addr1 = reserve_addr_on(isolated_loopback(2));
        let addr2 = reserve_addr_on(isolated_loopback(3));
        let mut peers1 = BTreeMap::new();
        peers1.insert(
            id2,
            ControllerPeerDescriptor {
                controller_id: id2,
                address: addr2,
            },
        );
        let mut peers2 = BTreeMap::new();
        peers2.insert(
            id1,
            ControllerPeerDescriptor {
                controller_id: id1,
                address: addr1,
            },
        );
        let base = |local, bind, peers| ControllerPeerTransportConfig {
            local_controller_id: local,
            cluster_id: [9; 16],
            bind_addr: bind,
            peers,
            limits: ControllerTransportLimits::default(),
            max_inbound_frames: 8,
            max_outbound_frames_per_peer: 8,
            max_outbound_bytes_per_peer: 64 * 1024,
            reconnect_min_delay: Duration::from_millis(5),
            reconnect_max_delay: Duration::from_millis(20),
        };
        let (transport1, _) = start_controller_peer_transport(base(id1, addr1, peers1))
            .await
            .unwrap();
        transport1.sender().try_send(id2, message(1), None).unwrap();
        time::sleep(Duration::from_millis(20)).await;
        let (transport2, mut inbound2) = start_controller_peer_transport(base(id2, addr2, peers2))
            .await
            .unwrap();
        let received = time::timeout(Duration::from_secs(1), inbound2.recv())
            .await
            .unwrap()
            .unwrap();
        assert_eq!(received.source_controller_id, 1);
        transport1.shutdown().await;
        transport2.shutdown().await;
    }

    #[tokio::test]
    async fn outbound_frame_and_byte_budgets_are_hard_bounds() {
        let id1 = ControllerNodeId::new(1).unwrap();
        let id2 = ControllerNodeId::new(2).unwrap();
        let mut peers = BTreeMap::new();
        peers.insert(
            id2,
            ControllerPeerDescriptor {
                controller_id: id2,
                address: reserve_addr(),
            },
        );
        let (transport, _) = start_controller_peer_transport(ControllerPeerTransportConfig {
            local_controller_id: id1,
            cluster_id: [3; 16],
            bind_addr: reserve_addr(),
            peers,
            limits: ControllerTransportLimits::default(),
            max_inbound_frames: 1,
            max_outbound_frames_per_peer: 1,
            max_outbound_bytes_per_peer: 1,
            reconnect_min_delay: Duration::from_millis(20),
            reconnect_max_delay: Duration::from_millis(20),
        })
        .await
        .unwrap();
        assert!(matches!(
            transport.sender().try_send(id2, message(1), None),
            Err(ControllerError::Busy(_))
        ));
        transport.shutdown().await;
    }

    #[tokio::test]
    async fn foreign_cluster_and_unknown_source_are_rejected_before_delivery() {
        let id1 = ControllerNodeId::new(1).unwrap();
        let id2 = ControllerNodeId::new(2).unwrap();
        let addr2 = reserve_addr();
        let mut peers = BTreeMap::new();
        peers.insert(
            id1,
            ControllerPeerDescriptor {
                controller_id: id1,
                address: reserve_addr(),
            },
        );
        let (transport, mut inbound) =
            start_controller_peer_transport(ControllerPeerTransportConfig {
                local_controller_id: id2,
                cluster_id: [8; 16],
                bind_addr: addr2,
                peers,
                limits: ControllerTransportLimits::default(),
                max_inbound_frames: 8,
                max_outbound_frames_per_peer: 8,
                max_outbound_bytes_per_peer: 64 * 1024,
                reconnect_min_delay: Duration::from_millis(5),
                reconnect_max_delay: Duration::from_millis(20),
            })
            .await
            .unwrap();

        let send_raw = |cluster_id: [u8; 16], source_controller_id: u64| async move {
            let bytes = encode_controller_frame(
                &ControllerFrame {
                    cluster_id,
                    source_controller_id,
                    message: message(source_controller_id),
                    snapshot_package: None,
                },
                ControllerTransportLimits::default(),
            )
            .unwrap();
            let mut stream = TcpStream::connect(addr2).await.unwrap();
            stream.write_all(&bytes).await.unwrap();
            stream.shutdown().await.unwrap();
        };

        send_raw([9; 16], 1).await;
        assert!(
            time::timeout(Duration::from_millis(50), inbound.recv())
                .await
                .is_err()
        );
        send_raw([8; 16], 3).await;
        assert!(
            time::timeout(Duration::from_millis(50), inbound.recv())
                .await
                .is_err()
        );
        transport.shutdown().await;
    }

    #[tokio::test]
    async fn unreachable_peer_pressure_hits_frame_queue_bound() {
        let id1 = ControllerNodeId::new(1).unwrap();
        let id2 = ControllerNodeId::new(2).unwrap();
        let mut peers = BTreeMap::new();
        peers.insert(
            id2,
            ControllerPeerDescriptor {
                controller_id: id2,
                address: reserve_addr(),
            },
        );
        let (transport, _) = start_controller_peer_transport(ControllerPeerTransportConfig {
            local_controller_id: id1,
            cluster_id: [4; 16],
            bind_addr: reserve_addr(),
            peers,
            limits: ControllerTransportLimits::default(),
            max_inbound_frames: 4,
            max_outbound_frames_per_peer: 1,
            max_outbound_bytes_per_peer: 1024 * 1024,
            reconnect_min_delay: Duration::from_millis(100),
            reconnect_max_delay: Duration::from_millis(100),
        })
        .await
        .unwrap();
        let sender = transport.sender();
        let mut saw_busy = false;
        for _ in 0..32 {
            if matches!(
                sender.try_send(id2, message(1), None),
                Err(ControllerError::Busy(_))
            ) {
                saw_busy = true;
                break;
            }
        }
        assert!(
            saw_busy,
            "bounded controller frame queue never backpressured"
        );
        transport.shutdown().await;
    }
}
