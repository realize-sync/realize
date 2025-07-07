use capnp_rpc::rpc_twoparty_capnp::Side;
use capnp_rpc::twoparty::VatNetwork;
use capnp_rpc::{RpcSystem, VatNetwork as _};
use futures::AsyncReadExt;
use futures::io::{BufReader, BufWriter};
use std::cell::RefCell;
use std::collections::HashMap;
use std::marker::PhantomData;
use std::rc::Rc;
use std::thread;
use std::time::Duration;
use tokio::net::TcpStream;
use tokio::runtime;
use tokio::sync::broadcast;
use tokio::sync::mpsc;
use tokio::task::{JoinHandle, LocalSet};
use tokio_retry::strategy::ExponentialBackoff;
use tokio_util::compat::TokioAsyncReadCompatExt;

use crate::model::Peer;

use super::rate_limit::RateLimitedStream;
use super::{Networking, Server};

/// Connection status of a peer, broadcast by [Household].
#[derive(Clone, PartialEq, Debug)]
pub enum PeerStatus {
    Connected(Peer),
    Disconnected(Peer),
}

/// Messages used to communicate with [CapnpRpcThread].
enum ConnectionMessage {
    /// Send incoming (server) TCP connections to the capnp threads to
    /// be handled there.
    Incoming {
        peer: Peer,
        stream: Box<tokio_rustls::server::TlsStream<RateLimitedStream<TcpStream>>>,
        shutdown_rx: broadcast::Receiver<()>,
    },
    KeepConnected,
}

#[allow(async_fn_in_trait)]
pub trait ConnectionHandler<C>: Send {
    fn tag(&self) -> &'static [u8; 4];
    fn server(&self, peer: &Peer) -> capnp::capability::Client;
    async fn check_connection(&self, peer: &Peer, client: &mut C) -> bool;
    async fn register(&self, peer: &Peer, client: C);
}

pub struct ConnectionManager {
    tag: &'static [u8; 4],
    tx: mpsc::UnboundedSender<ConnectionMessage>,
    broadcast_tx: broadcast::Sender<PeerStatus>,
}

impl ConnectionManager {
    /// Spawn a new RPC thread and return the Household instance that
    /// manages it.
    pub fn spawn<H, C>(
        networking: Networking,
        handler: H,
    ) -> anyhow::Result<(Self, thread::JoinHandle<()>)>
    where
        H: ConnectionHandler<C> + 'static,
        C: capnp::capability::FromClientHook + Clone + 'static,
    {
        let (tx, rx) = mpsc::unbounded_channel();
        let (broadcast_tx, _) = broadcast::channel(128);
        let tag = handler.tag();
        let handle = spawn_rpc_thread(networking, handler, rx, broadcast_tx.clone())?;

        Ok((
            Self {
                tx,
                broadcast_tx,
                tag,
            },
            handle,
        ))
    }

    /// Report peer status changes through the given receiver.
    pub fn peer_status(&self) -> broadcast::Receiver<PeerStatus> {
        self.broadcast_tx.subscribe()
    }

    /// Keep a client connection up to all peers for which an address is known.
    pub fn keep_connected(&self) -> anyhow::Result<()> {
        self.tx.send(ConnectionMessage::KeepConnected)?;

        Ok(())
    }

    /// Register peer connections to the given server.
    ///
    /// With this call, the server answers to PEER calls as Cap'n Proto
    /// PeerConnection, defined in `capnp/peer.capnp`.
    pub fn register(&self, server: &mut Server) {
        let tx = self.tx.clone();
        server.register_raw(self.tag, move |peer, stream, _, shutdown_rx| {
            // TODO: support shutdown_rx
            let _ = tx.send(ConnectionMessage::Incoming {
                peer,
                stream: Box::new(stream),
                shutdown_rx,
            });
        })
    }
}

/// Spawns a single thread that handles all Cap'n Proto RPC connections.
///
/// To communicate with the thread, send [HousehholdConnection]s to the channel.
fn spawn_rpc_thread<H, C>(
    networking: Networking,
    handler: H,
    mut rx: mpsc::UnboundedReceiver<ConnectionMessage>,
    broadcast_tx: broadcast::Sender<PeerStatus>,
) -> anyhow::Result<thread::JoinHandle<()>>
where
    H: ConnectionHandler<C> + 'static,
    C: capnp::capability::FromClientHook + Clone + 'static,
{
    let rt = runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();

    Ok(thread::Builder::new()
        .name("capnprpc".into())
        .spawn(move || {
            let ctx = AppContext::new(networking, handler, broadcast_tx);
            let local = LocalSet::new();

            local.spawn_local(async move {
                while let Some(conn) = rx.recv().await {
                    match conn {
                        ConnectionMessage::Incoming {
                            peer,
                            stream,
                            shutdown_rx,
                        } => ctx.accept(peer, stream, shutdown_rx),
                        ConnectionMessage::KeepConnected => ctx.keep_connected(),
                    }
                }
            });

            rt.block_on(local);
        })?)
}

struct AppContext<H, C>
where
    H: ConnectionHandler<C>,
{
    networking: Networking,
    handler: H,
    broadcast_tx: broadcast::Sender<PeerStatus>,
    connections: RefCell<HashMap<Peer, TrackedPeerConnections<C>>>,
    _phantom1: PhantomData<C>,
}

struct TrackedPeerConnections<C> {
    tracked_client: Option<C>,
    tracker: Option<JoinHandle<()>>,
}

impl<H, C> AppContext<H, C>
where
    H: ConnectionHandler<C> + 'static,
    C: capnp::capability::FromClientHook + Clone + 'static,
{
    fn new(
        networking: Networking,
        handler: H,
        broadcast_tx: broadcast::Sender<PeerStatus>,
    ) -> Rc<Self> {
        Rc::new(Self {
            networking,
            handler,
            broadcast_tx,
            connections: RefCell::new(HashMap::new()),
            _phantom1: PhantomData,
        })
    }

    fn accept(
        self: &Rc<Self>,
        peer: Peer,
        stream: Box<tokio_rustls::server::TlsStream<RateLimitedStream<TcpStream>>>,
        mut shutdown_rx: broadcast::Receiver<()>,
    ) {
        let this = Rc::clone(self);
        tokio::task::spawn_local(async move {
            let (r, w) = TokioAsyncReadCompatExt::compat(stream).split();
            let mut net = Box::new(VatNetwork::new(
                BufReader::new(r),
                BufWriter::new(w),
                Side::Server,
                Default::default(),
            ));
            let until_shutdown = net.drive_until_shutdown();
            let system = RpcSystem::new(net, Some(this.handler.server(&peer)));
            let disconnector = system.get_disconnector();
            tokio::task::spawn_local(system);

            tokio::select!(
                _ = shutdown_rx.recv() => {
                    let _ = disconnector.await;
                },
                res = until_shutdown => {
                    if let Err(err) = res {
                        log::debug!("RPC connection from {peer} failed: {err}")
                    }
                }
            );
        });
    }

    fn keep_connected(self: &Rc<Self>) {
        for peer in self.networking.connectable_peers() {
            let mut borrow = self.connections.borrow_mut();
            let conn = borrow
                .entry(peer.clone())
                .or_insert_with(|| TrackedPeerConnections {
                    tracked_client: None,
                    tracker: None,
                });
            let has_usable_tracker = conn
                .tracker
                .as_ref()
                .map(|t| !t.is_finished())
                .unwrap_or(false);
            if has_usable_tracker {
                continue;
            }

            let this = Rc::clone(self);
            let peer = peer.clone();
            conn.tracker = Some(tokio::task::spawn_local(async move {
                this.track_peer(&peer).await
            }));
        }
    }

    async fn track_peer(self: &Rc<Self>, peer: &Peer) {
        let retry_strategy =
            ExponentialBackoff::from_millis(500).max_delay(Duration::from_secs(5 * 60));
        let mut current_backoff: Option<ExponentialBackoff> = None;
        loop {
            match &mut current_backoff {
                Some(backoff) => match backoff.next() {
                    None => {
                        log::warn!("Giving up connecting to {peer}");
                        return;
                    }
                    Some(delay) => {
                        tokio::time::sleep(delay).await;
                    }
                },
                None => {
                    // Execute immediately, and install backoff for next time.
                    current_backoff = Some(retry_strategy.clone());
                }
            }
            let stream = match self
                .networking
                .connect_raw(peer, self.handler.tag(), None)
                .await
            {
                Ok(stream) => stream,
                Err(err) => {
                    log::debug!("Failed to connect to {peer}: {err}; Will retry.");
                    continue;
                }
            };

            let (r, w) = TokioAsyncReadCompatExt::compat(stream).split();
            let mut net = Box::new(VatNetwork::new(
                BufReader::new(r),
                BufWriter::new(w),
                Side::Client,
                Default::default(),
            ));
            let until_shutdown = net.drive_until_shutdown();
            let mut system = RpcSystem::new(net, None);
            let mut client: C = system.bootstrap(Side::Server);
            let disconnector = system.get_disconnector();
            scopeguard::defer!({
                tokio::task::spawn_local(disconnector);
            });
            tokio::task::spawn_local(system);

            if !self.handler.check_connection(peer, &mut client).await {
                continue;
            }

            self.set_tracked_client(peer, Some(client.clone()));
            let _ = self.broadcast_tx.send(PeerStatus::Connected(peer.clone()));
            scopeguard::defer!({
                self.set_tracked_client(peer, None);
                let _ = self
                    .broadcast_tx
                    .send(PeerStatus::Disconnected(peer.clone()));
            });

            self.handler.register(peer, client).await;

            // We're fully connected. Reset the backoff delay for next time.
            current_backoff = None;

            if let Err(err) = until_shutdown.await {
                log::debug!("Connection to {peer} was shutdown: {err}; Will reconnect.")
            }
        }
    }

    /// Associate the given client with the peer.
    fn set_tracked_client(self: &Rc<Self>, peer: &Peer, client: Option<C>) {
        self.connections
            .borrow_mut()
            .entry(peer.clone())
            .or_insert_with(|| TrackedPeerConnections {
                tracked_client: None,
                tracker: None,
            })
            .tracked_client = client;
    }
}
