#![allow(refining_impl_trait)] // for capnp-rpc
use super::peer_capnp::connected_peer;
use super::result_capnp;
use super::store_capnp::read_callback::{ChunkParams, FinishParams, FinishResults};
use super::store_capnp::store::{
    self, ArenasParams, ArenasResults, ReadParams, ReadResults, RsyncParams, RsyncResults,
    SubscriptionsParams, SubscriptionsResults, WithRateLimitParams, WithRateLimitResults,
};
use super::store_capnp::{io_error, read_callback};
use async_speed_limit::Limiter;
use capnp::capability::Promise;
use capnp_rpc::pry;
use capnp_rpc::rpc_twoparty_capnp::Side;
use realize_network::capnp::{ConnectionHandler, ConnectionManager, ConnectionTracker};
use realize_network::{Networking, Server};
use realize_storage::{Storage, StorageError};
use realize_types::{self, Arena, ByteRange, Delta, Path, Peer, Signature};
use std::cell::RefCell;
use std::collections::{HashMap, HashSet};
use std::io::{self, SeekFrom};
use std::pin;
use std::rc::Rc;
use std::sync::Arc;
use std::time::Duration;
use tokio::io::AsyncSeekExt;
use tokio::io::{AsyncRead, AsyncReadExt};
use tokio::sync::{broadcast, mpsc, oneshot};
use tokio::task::LocalSet;
use tokio_stream::wrappers::ReceiverStream;
use tokio_util::sync::{CancellationToken, DropGuard};

mod convert;
mod rate_limit;
mod subscribe;
#[cfg(test)]
mod testing;

/// Identifies Cap'n Proto ConnectedPeer connections.
const TAG: &[u8; 4] = b"PEER";

#[derive(Copy, Clone, Eq, PartialEq, Debug)]
pub enum ExecutionMode {
    Interactive,
    Batch,
}

/// Connection status of a peer, broadcast by [Household::peer_status]
#[derive(Clone, PartialEq, Debug)]
pub enum PeerStatus {
    // The given remote peer connected to the local peer.
    Connected(Peer),
    // The given remote peer was disconnected from the local peer.
    Disconnected(Peer),
}

#[derive(Copy, Clone, Eq, PartialEq, Debug, Default)]
pub enum ConnectionStatus {
    Connected,
    #[default]
    Disconnected,
}

/// Information about a peer, reported by [Household::query_peers].
#[derive(Clone, Eq, PartialEq, Debug, Default)]
pub struct ConnectionInfo {
    pub connection: ConnectionStatus,
    pub keep_connected: bool,
}

/// A set of peers and their connections.
///
/// Cap'n Proto connections are handled or their own thread. This
/// object serves as a communication channel between that thread and
/// the rest of the application.
///
/// To listen to incoming connections, call [Household::register].
pub struct Household {
    manager: ConnectionManager,
    operation_tx: mpsc::Sender<HouseholdOperation>,
    connection_tx: broadcast::Sender<PeerStatus>,

    /// Set of all known peers
    known_peers: HashSet<Peer>,
}

impl Household {
    /// Build a new Household instance that runs on the given
    /// [LocalSet].
    ///
    /// The [LocalSet] must later be run to run the tasks spawned on
    /// it by the connection manager. This is done by calling
    /// [LocalSet::run_until] or awaiting the local set itself.
    pub fn spawn(
        local: &LocalSet,
        networking: Networking,
        storage: Arc<Storage>,
    ) -> anyhow::Result<Arc<Self>> {
        let peers = networking.peers().collect();
        let (tx, rx) = mpsc::channel(128);
        let (connection_tx, _) = broadcast::channel(128);
        let handler = PeerConnectionHandler::new(storage, &networking, rx, connection_tx.clone());
        let manager = ConnectionManager::spawn(local, networking, handler)?;

        Ok(Arc::new(Self {
            manager,
            operation_tx: tx,
            connection_tx,
            known_peers: peers,
        }))
    }

    /// Keep a client connection up to all peers for which an address is known.
    pub fn keep_all_connected(&self) -> anyhow::Result<()> {
        self.manager.keep_all_connected()
    }

    /// Disconnects from all peers and stop trying to connect to peers.
    pub fn disconnect_all(&self) -> anyhow::Result<()> {
        self.manager.disconnect_all()
    }

    /// Keep a client connection up to the given peer.
    ///
    /// Does nothing if no address is known for the peer.
    pub fn keep_peer_connected(&self, peer: Peer) -> anyhow::Result<()> {
        self.manager.keep_peer_connected(peer)
    }

    /// Disconnects the peer and stop trying to connect to peers.
    pub fn disconnect_peer(&self, peer: Peer) -> anyhow::Result<()> {
        self.manager.disconnect_peer(peer)
    }

    /// Report peer status changes through the given receiver.
    pub fn peer_status(&self) -> broadcast::Receiver<PeerStatus> {
        self.connection_tx.subscribe()
    }

    /// Register peer connections to the given server.
    ///
    /// With this call, the server answers to PEER calls as Cap'n Proto
    /// PeerConnection, defined in `capnp/peer.capnp`.
    pub fn register(&self, server: &mut Server) {
        self.manager.register(server)
    }

    /// Read a file from a connected peer.
    pub fn read<T>(
        &self,
        peers: T,
        mode: ExecutionMode,
        arena: Arena,
        path: Path,
        offset: u64,
        limit: Option<u64>,
    ) -> io::Result<ReceiverStream<Result<(u64, Vec<u8>), HouseholdOperationError>>>
    where
        T: IntoIterator<Item = Peer>,
    {
        let (tx, rx) = mpsc::channel(10);
        let op = HouseholdOperation::Read {
            peers: peers.into_iter().collect(),
            mode,
            arena,
            path,
            offset,
            limit,
            tx,
        };

        // spawn to keep read non-async and report all errors in the stream.
        let operation_tx = self.operation_tx.clone();
        tokio::spawn(async move {
            if let Err(err) = operation_tx.send(op).await {
                if let HouseholdOperation::Read { tx, .. } = err.0 {
                    let _ = tx.send(Err(HouseholdOperationError::Disconnected)).await;
                }
            }
        });

        Ok(ReceiverStream::new(rx))
    }

    /// Generate a rsync patch to apply to a given byte range of a remote file.
    pub async fn rsync<T>(
        &self,
        peers: T,
        mode: ExecutionMode,
        arena: Arena,
        path: &Path,
        range: &ByteRange,
        sig: Signature,
    ) -> Result<Delta, HouseholdOperationError>
    where
        T: IntoIterator<Item = Peer>,
    {
        let (tx, rx) = oneshot::channel::<Result<Delta, HouseholdOperationError>>();
        self.operation_tx
            .send(HouseholdOperation::Rsync {
                peers: peers.into_iter().collect(),
                mode,
                tx,
                arena,
                path: path.clone(),
                range: range.clone(),
                sig,
            })
            .await?;

        let res = rx.await?;
        let delta = res?;

        Ok(delta)
    }

    /// Report peers that are connected or that we're trying to connect to.
    ///
    /// May return empty if no peer is connected and we're not trying
    /// to connect to any peers.
    pub async fn query_peers(&self) -> anyhow::Result<HashMap<Peer, ConnectionInfo>> {
        log::trace!("Query connected peers");
        let connected = self.connected_peers().await?;
        log::trace!("Connected peers: {connected:?}");
        let keep_connected = self.manager.peers_to_keep_connected().await?;
        log::trace!("Peers to keep connected: {keep_connected:?}");
        let mut connection_info: HashMap<Peer, ConnectionInfo> = HashMap::new();
        for peer in &self.known_peers {
            let info = connection_info.entry(*peer).or_default();
            if connected.contains(peer) {
                info.connection = ConnectionStatus::Connected;
            }
            info.keep_connected = keep_connected.contains(peer);
        }

        Ok(connection_info)
    }

    /// Return the set of connected peers.
    async fn connected_peers(&self) -> Result<HashSet<Peer>, anyhow::Error> {
        let (tx, rx) = oneshot::channel();
        self.operation_tx
            .send(HouseholdOperation::QueryConnectedPeers { tx })
            .await?;
        Ok(rx.await?)
    }
}

#[derive(Debug, thiserror::Error)]
pub enum HouseholdOperationError {
    #[error("no suitable peer for operation")]
    NoPeers,

    #[error("connection was lost")]
    Disconnected,

    #[error("I/O error {0}")]
    Io(#[from] std::io::Error),

    #[error("RPC error {0}")]
    Rpc(capnp::Error),

    #[error("{0}")]
    Storage(#[from] realize_storage::StorageError),
}

impl From<capnp::Error> for HouseholdOperationError {
    fn from(err: capnp::Error) -> Self {
        if err.kind == capnp::ErrorKind::Disconnected {
            HouseholdOperationError::Disconnected
        } else {
            HouseholdOperationError::Rpc(err)
        }
    }
}

impl<T> From<mpsc::error::SendError<T>> for HouseholdOperationError {
    fn from(_err: mpsc::error::SendError<T>) -> Self {
        HouseholdOperationError::Disconnected
    }
}

impl From<oneshot::error::RecvError> for HouseholdOperationError {
    fn from(_err: oneshot::error::RecvError) -> Self {
        HouseholdOperationError::Disconnected
    }
}

impl HouseholdOperationError {
    pub fn into_io(self) -> io::Error {
        use HouseholdOperationError::*;
        match self {
            NoPeers => std::io::Error::new(std::io::ErrorKind::NotConnected, "no suitable peer"),
            Disconnected => {
                std::io::Error::new(std::io::ErrorKind::ConnectionAborted, "disconnected")
            }
            Io(ioerr) => ioerr,
            Rpc(err) => io::Error::other(err),
            Storage(err) => err.into(),
        }
    }

    pub fn io_kind(&self) -> std::io::ErrorKind {
        use HouseholdOperationError::*;
        match self {
            NoPeers => std::io::ErrorKind::NotConnected,
            Disconnected => std::io::ErrorKind::ConnectionAborted,
            Io(ioerr) => ioerr.kind(),
            Rpc(_) => std::io::ErrorKind::Other,
            Storage(err) => err.io_kind(),
        }
    }
}

#[derive(Debug)]
enum HouseholdOperation {
    Read {
        peers: Vec<Peer>,
        mode: ExecutionMode,
        tx: mpsc::Sender<Result<(u64, Vec<u8>), HouseholdOperationError>>,
        arena: Arena,
        path: realize_types::Path,
        offset: u64,
        limit: Option<u64>,
    },
    Rsync {
        peers: Vec<Peer>,
        mode: ExecutionMode,
        tx: oneshot::Sender<Result<Delta, HouseholdOperationError>>,
        arena: Arena,
        path: realize_types::Path,
        range: ByteRange,
        sig: Signature,
    },
    QueryConnectedPeers {
        tx: oneshot::Sender<HashSet<Peer>>,
    },
}
struct PeerConnectionHandler {
    storage: Arc<Storage>,
    batch_rate_limits: HashMap<Peer, f64>,
    rx: mpsc::Receiver<HouseholdOperation>,
    connection_tx: broadcast::Sender<PeerStatus>,
}

impl PeerConnectionHandler {
    fn new(
        storage: Arc<Storage>,
        networking: &Networking,
        rx: mpsc::Receiver<HouseholdOperation>,
        connection_tx: broadcast::Sender<PeerStatus>,
    ) -> Self {
        let batch_rate_limits = networking
            .peer_setup()
            .flat_map(|(p, s)| s.batch_rate_limit.map(|l| (p, l as f64)))
            .filter(|(_, l)| *l > 0.0)
            .collect::<HashMap<Peer, f64>>();

        Self {
            storage,
            batch_rate_limits,
            rx,
            connection_tx,
        }
    }
}

impl ConnectionHandler<PeerConnectionTracker, connected_peer::Client> for PeerConnectionHandler {
    fn tag(&self) -> &'static [u8; 4] {
        TAG
    }

    async fn create_tracker(self) -> Rc<PeerConnectionTracker> {
        let PeerConnectionHandler {
            storage,
            batch_rate_limits,
            mut rx,
            connection_tx,
        } = self;
        let tracker = PeerConnectionTracker::new(storage, batch_rate_limits, connection_tx);

        tokio::task::spawn_local({
            let tracker = Rc::clone(&tracker);

            async move {
                log::trace!("Handling household operations");
                let mut opcount = 0;
                while let Some(op) = rx.recv().await {
                    log::trace!("Execute household operation #{opcount}: {op:?}");
                    opcount += 1;
                    tracker.execute(op).await;
                    log::trace!("Done executing household operation #{opcount}");
                }
                log::trace!("Done handling household operations");
            }
        });

        tracker
    }
}

struct PeerConnectionTracker {
    storage: Arc<Storage>,
    clients: Rc<TrackedClientMap>,
}

impl PeerConnectionTracker {
    fn new(
        storage: Arc<Storage>,
        batch_rate_limits: HashMap<Peer, f64>,
        connection_tx: broadcast::Sender<PeerStatus>,
    ) -> Rc<Self> {
        Rc::new(Self {
            storage,
            clients: Rc::new(TrackedClientMap::new(batch_rate_limits, connection_tx)),
        })
    }

    async fn execute(&self, operation: HouseholdOperation) {
        match operation {
            HouseholdOperation::Read {
                peers,
                mode,
                tx,
                arena,
                path,
                offset,
                limit,
            } => {
                let tx_clone = tx.clone();
                if let Err(err) = execute_read(
                    self.clients.find_store(&peers, mode),
                    arena,
                    path,
                    offset,
                    limit,
                    tx,
                )
                .await
                {
                    let _ = tx_clone.send(Err(err)).await;
                }
            }
            HouseholdOperation::Rsync {
                peers,
                mode,
                tx,
                arena,
                path,
                range,
                sig,
            } => {
                let res = execute_rsync(
                    self.clients.find_store(&peers, mode),
                    arena,
                    &path,
                    &range,
                    sig,
                )
                .await;
                let _ = tx.send(res);
            }
            HouseholdOperation::QueryConnectedPeers { tx } => {
                let _ = tx.send(self.clients.connected_peers());
            }
        }
    }

    // Offer our local store to the peer we just connected to (best
    // effort).
    async fn share_local_store_with_peer(&self, peer: Peer, client: &mut connected_peer::Client) {
        let mut request = client.register_request();
        request.get().set_store(
            StoreServer {
                peer: peer,
                storage: self.storage.clone(),
                limiter: None,
            }
            .into_client(),
        );
        if let Err(err) = request.send().promise.await {
            log::debug!("@{peer} ConnectedPeer::register failed: {err:?}",);
        }
    }
}

/// Tracked client stores.
///
/// Stores can come from either connecting to a peer and getting its
/// store ([Side::Server]), or having a peer connect and register its
/// store ([Side::Client]).
///
/// When choosing a client, the server side is preferred if both are
/// available.
struct TrackedClientMap {
    client_stores: RefCell<HashMap<Peer, TrackedClients>>,
    server_stores: RefCell<HashMap<Peer, TrackedClients>>,
    connection_tx: broadcast::Sender<PeerStatus>,
    batch_rate_limits: HashMap<Peer, f64>,
}

struct TrackedClients {
    store: store::Client,
    batch_store: Option<store::Client>,
    token: CancellationToken,
    _drop_guard: DropGuard,
}

impl TrackedClients {
    fn new(store: store::Client, batch_store: Option<store::Client>) -> Self {
        let token = CancellationToken::new();
        let drop_guard = token.clone().drop_guard();
        Self {
            store,
            batch_store,
            token,
            _drop_guard: drop_guard,
        }
    }
}

impl TrackedClientMap {
    fn new(
        batch_rate_limits: HashMap<Peer, f64>,
        connection_tx: broadcast::Sender<PeerStatus>,
    ) -> Self {
        Self {
            client_stores: RefCell::new(HashMap::new()),
            server_stores: RefCell::new(HashMap::new()),
            connection_tx,
            batch_rate_limits,
        }
    }

    fn unregister(&self, peer: Peer, side: Side) {
        log::debug!("@{peer} Store has become unavailable ({side:?}) ",);
        let was_connected = self.is_connected(peer);
        self.stores(side).borrow_mut().remove(&peer);
        if was_connected && !self.is_connected(peer) {
            log::info!("@{peer} Peer has become unavailable");
            let _ = self.connection_tx.send(PeerStatus::Disconnected(peer));
        }
    }

    async fn register(
        &self,
        peer: Peer,
        store: store::Client,
        storage: &Arc<Storage>,
        side: Side,
    ) -> Result<(), capnp::Error> {
        let batch_rate_limit = self.batch_rate_limits.get(&peer);
        log::debug!(
            "@{peer} Store has become available ({side:?}, batch-rate-limit={:?})",
            batch_rate_limit
        );
        let batch_store = if let Some(bytes_per_second) = batch_rate_limit {
            let mut request = store.with_rate_limit_request();
            request.get().set_rate_limit(*bytes_per_second);
            let reply = request.send().promise.await?;

            Some(reply.get()?.get_store()?)
        } else {
            None
        };

        let store_for_subscribe = batch_store.as_ref().unwrap_or(&store).clone();
        let was_connected = self.is_connected(peer);
        let tracked = TrackedClients::new(store, batch_store);
        let token = tracked.token.clone();
        self.stores(side).borrow_mut().insert(peer, tracked);
        if !was_connected {
            log::info!("@{peer} Peer has become available");
            let _ = self.connection_tx.send(PeerStatus::Connected(peer));
        }

        if let Err(err) = subscribe::subscribe_self(storage, peer, store_for_subscribe, token).await
        {
            log::warn!("@{peer} ConnectedPeer::subscribe failed: {err}");
        }

        Ok(())
    }

    fn find_store(&self, peers: &Vec<Peer>, mode: ExecutionMode) -> Option<(Peer, store::Client)> {
        self.find_store_for_side(peers, mode, Side::Server)
            .or_else(|| self.find_store_for_side(peers, mode, Side::Client))
    }

    fn find_store_for_side(
        &self,
        peers: &Vec<Peer>,
        mode: ExecutionMode,
        side: Side,
    ) -> Option<(Peer, store::Client)> {
        let stores = self.stores(side).borrow();
        peers
            .iter()
            .find_map(|p| stores.get(p).map(|s| (*p, s)))
            .map(|(p, c)| {
                (
                    p,
                    match mode {
                        ExecutionMode::Interactive => c.store.clone(),
                        ExecutionMode::Batch => c.batch_store.as_ref().unwrap_or(&c.store).clone(),
                    },
                )
            })
    }

    fn connected_peers(&self) -> HashSet<Peer> {
        self.client_stores
            .borrow()
            .keys()
            .map(|p| *p)
            .chain(self.server_stores.borrow().keys().map(|p| *p))
            .collect()
    }

    fn is_connected(&self, peer: Peer) -> bool {
        self.server_stores.borrow().contains_key(&peer)
            || self.client_stores.borrow().contains_key(&peer)
    }

    fn stores(&self, side: Side) -> &RefCell<HashMap<Peer, TrackedClients>> {
        match side {
            Side::Client => &self.client_stores,
            Side::Server => &self.server_stores,
        }
    }
}

impl ConnectionTracker<connected_peer::Client> for PeerConnectionTracker {
    fn server(&self, peer: Peer) -> capnp::capability::Client {
        ConnectedPeerServer::new(peer, self.storage.clone(), Rc::clone(&self.clients))
            .into_client()
            .client
    }

    async fn register(
        &self,
        peer: Peer,
        client: &mut connected_peer::Client,
    ) -> anyhow::Result<()> {
        let store = get_connected_peer_store(client).await?;
        self.clients
            .register(peer, store, &self.storage, Side::Server)
            .await?;

        self.share_local_store_with_peer(peer, client).await;

        Ok(())
    }

    async fn keep_alive(&self, peer: Peer, client: connected_peer::Client) -> anyhow::Result<()> {
        log::trace!("@{peer} Keepalive");
        loop {
            tokio::time::sleep(Duration::from_secs(6 * 60)).await;
            match tokio::time::timeout(
                Duration::from_secs(5 * 60),
                client.ping_request().send().promise,
            )
            .await
            {
                Ok(Ok(_)) => {
                    log::trace!("@{peer} Ping OK");
                }
                Ok(Err(err)) => {
                    anyhow::bail!("@{peer} Ping failed: {err}");
                }
                Err(_) => {
                    log::trace!("@{peer} Ping timed out");
                    return Ok(());
                }
            }
        }
    }

    fn unregister(&self, peer: Peer) {
        self.clients.unregister(peer, Side::Server);
    }

    fn client_disconnected(&self, peer: Peer) {
        self.clients.unregister(peer, Side::Client);
    }
}

async fn execute_read(
    client: Option<(Peer, store::Client)>,
    arena: Arena,
    path: realize_types::Path,
    offset: u64,
    limit: Option<u64>,
    tx: mpsc::Sender<Result<(u64, Vec<u8>), HouseholdOperationError>>,
) -> Result<(), HouseholdOperationError> {
    let (peer, store) = match client {
        Some(c) => c,
        None => {
            return Err(HouseholdOperationError::NoPeers);
        }
    };
    log::debug!("[{arena}]@{peer} Downloading \"{path}\"");

    let mut request = store.read_request();
    let mut builder = request.get();
    builder.set_cb(capnp_rpc::new_client(ReadCallbackServer {
        tx: RefCell::new(Some(tx)),
    }));
    let mut req = builder.init_req();
    req.set_arena(arena.as_str());
    req.set_path(path.as_str());
    req.set_start_offset(offset);
    if let Some(limit) = limit {
        req.set_limit(limit);
    }

    request.send().promise.await?;

    Ok(())
}

async fn execute_rsync(
    client: Option<(Peer, store::Client)>,
    arena: Arena,
    path: &realize_types::Path,
    range: &ByteRange,
    sig: Signature,
) -> Result<Delta, HouseholdOperationError> {
    let (peer, store) = match client {
        Some(c) => c,
        None => {
            return Err(HouseholdOperationError::NoPeers);
        }
    };
    log::debug!(
        "[{arena}]@{peer} Rsyncing \"{path}\" {range} ({} bytes in)",
        sig.0.len()
    );

    let mut request = store.rsync_request();
    let mut req = request.get().init_req();
    req.set_arena(arena.as_str());
    req.set_path(path.as_str());
    req.set_sig(sig.0.as_slice());
    convert::fill_byterange(req.init_range(), range);
    let reply = request.send().promise.await?;
    let delta = Delta(reply.get()?.get_res()?.get_delta()?.into());
    log::debug!(
        "[{arena}]@{peer} Rsynced \"{path}\" {range} ({} bytes out)",
        delta.0.len()
    );

    Ok(delta)
}

struct ReadCallbackServer {
    tx: RefCell<Option<mpsc::Sender<Result<(u64, Vec<u8>), HouseholdOperationError>>>>,
}

impl read_callback::Server for ReadCallbackServer {
    fn chunk(self: Rc<Self>, params: ChunkParams) -> Promise<(), capnp::Error> {
        let tx = pry!(self.tx.borrow().as_ref().ok_or_else(already_finished)).clone();
        Promise::from_future(async move {
            let params = params.get()?;
            let offset = params.get_offset();
            let data = params.get_data()?.to_vec();
            tx.send(Ok((offset, data))).await.map_err(channel_closed)?;

            Ok(())
        })
    }

    fn finish(self: Rc<Self>, params: FinishParams, _: FinishResults) -> Promise<(), capnp::Error> {
        let tx = pry!(self.tx.borrow_mut().take().ok_or_else(already_finished));
        Promise::from_future(async move {
            let result = params.get()?.get_result()?;
            match result.which()? {
                result_capnp::result::Which::Ok(_) => {}
                result_capnp::result::Which::Err(err) => {
                    tx.send(Err(errno_to_io_error(err?.get_errno()?).into()))
                        .await
                        .map_err(channel_closed)?;
                }
            }
            Ok(())
        })
    }
}

fn errno_to_io_error(errno: io_error::Errno) -> io::Error {
    use io::ErrorKind::*;

    io::Error::new(
        match errno {
            io_error::Errno::Other => Other,
            io_error::Errno::GenericIo => Other,
            io_error::Errno::Unavailable => Other,
            io_error::Errno::PermissionDenied => PermissionDenied,
            io_error::Errno::NotADirectory => NotADirectory,
            io_error::Errno::IsADirectory => IsADirectory,
            io_error::Errno::InvalidInput => InvalidInput,
            io_error::Errno::Closed => Other,
            io_error::Errno::Aborted => ConnectionAborted,
            io_error::Errno::NotFound => NotFound,
            io_error::Errno::ResourceBusy => ResourceBusy,
            io_error::Errno::InvalidPath => InvalidFilename,
        },
        format!("{errno:?}"),
    )
}

fn io_error_to_errno(err: std::io::Error) -> io_error::Errno {
    use io_error::Errno;
    use std::io::ErrorKind;

    match err.kind() {
        ErrorKind::NotFound => Errno::NotFound,
        ErrorKind::PermissionDenied => Errno::PermissionDenied,
        ErrorKind::ConnectionAborted => Errno::Aborted,
        ErrorKind::NotADirectory => Errno::NotADirectory,
        ErrorKind::IsADirectory => Errno::IsADirectory,
        ErrorKind::InvalidInput => Errno::InvalidInput,
        ErrorKind::ResourceBusy => Errno::ResourceBusy,
        _ => Errno::GenericIo,
    }
}

fn already_finished() -> capnp::Error {
    capnp::Error::failed("already finished".to_string())
}

fn channel_closed<T>(_: mpsc::error::SendError<T>) -> capnp::Error {
    capnp::Error::failed("channel closed".to_string())
}

/// Implement capnp interface ConnectedPeer, defined in
/// `capnp/peer.capnp`.
#[derive(Clone)]
struct ConnectedPeerServer {
    peer: Peer,
    storage: Arc<Storage>,
    clients: Rc<TrackedClientMap>,
}

impl ConnectedPeerServer {
    fn new(peer: Peer, storage: Arc<Storage>, clients: Rc<TrackedClientMap>) -> Self {
        Self {
            peer,
            storage,
            clients,
        }
    }

    fn into_client(self) -> connected_peer::Client {
        capnp_rpc::new_client(self)
    }
}

impl connected_peer::Server for ConnectedPeerServer {
    fn ping(
        self: Rc<Self>,
        _: connected_peer::PingParams,
        _: connected_peer::PingResults,
    ) -> Promise<(), capnp::Error> {
        Promise::ok(())
    }

    fn store(
        self: Rc<Self>,
        _: connected_peer::StoreParams,
        mut results: connected_peer::StoreResults,
    ) -> Promise<(), capnp::Error> {
        results.get().set_store(
            StoreServer {
                peer: self.peer,
                storage: self.storage.clone(),
                limiter: None,
            }
            .into_client(),
        );

        Promise::ok(())
    }

    fn register(
        self: Rc<Self>,
        params: connected_peer::RegisterParams,
        _: connected_peer::RegisterResults,
    ) -> Promise<(), capnp::Error> {
        let peer = self.peer;
        let clients = Rc::clone(&self.clients);
        let storage = Arc::clone(&self.storage);
        Promise::from_future(async move {
            let remote_store = params.get()?.get_store()?;
            clients
                .register(peer, remote_store, &storage, Side::Client)
                .await?;

            Ok(())
        })
    }
}

#[derive(Clone)]
struct StoreServer {
    peer: Peer,
    storage: Arc<Storage>,
    limiter: Option<Limiter>,
}

impl StoreServer {
    fn into_client(self) -> store::Client {
        capnp_rpc::new_client(self)
    }

    async fn do_read(&self, params: ReadParams) -> Result<(), capnp::Error> {
        let params = params.get()?;
        let req = params.get_req()?;

        let arena = convert::parse_arena(req.get_arena()?)?;
        let path = convert::parse_path(req.get_path()?)?;
        let offset = req.get_start_offset();
        let limit = req.get_limit();
        let cb = params.get_cb()?;

        let read_result = self.read_all(arena, &path, offset, limit, &cb).await;
        let mut request = cb.finish_request();
        let result = request.get().init_result();
        match read_result {
            Err(err) => {
                result.init_err().set_errno(read_errno(err));
            }
            Ok(complete) => {
                if complete {
                    result.init_ok();
                } else {
                    result.init_err().set_errno(io_error::Errno::Aborted);
                }
            }
        }
        rate_limit::apply(&self.limiter, req.total_size()).await;
        request.send().promise.await?;

        Ok(())
    }

    /// Read data from the given arena and path and send it to the
    /// given callback.
    ///
    /// Returns true if everything was read, false if reading was
    /// interrupted by a callback failing, an error if there was
    /// some error while reading.
    async fn read_all(
        &self,
        arena: Arena,
        path: &Path,
        offset: u64,
        limit: u64,
        cb: &read_callback::Client,
    ) -> Result<bool, StorageError> {
        let mut reader = self.storage.reader(arena, path).await?;
        if offset > 0 {
            reader.seek(SeekFrom::Start(offset)).await?;
        }
        if limit > 0 {
            self.send_chunks(reader.take(limit), offset, cb).await
        } else {
            self.send_chunks(reader, offset, cb).await
        }
    }

    async fn do_rsync(
        &self,
        params: RsyncParams,
        mut results: RsyncResults,
    ) -> Result<(), capnp::Error> {
        let params = params.get()?;
        let req = params.get_req()?;

        let arena = convert::parse_arena(req.get_arena()?)?;
        let path = convert::parse_path(req.get_path()?)?;
        let range = convert::parse_range(req.get_range()?);
        if range.bytecount() > 1024 * 1024 {
            return Err(capnp::Error::failed("range too large".to_string()));
        }
        let sig = Signature(req.get_sig()?.into());
        let delta = self
            .storage
            .rsync(arena, &path, &range, sig)
            .await
            .map_err(convert::storage_to_capnp_err)?;

        results.get().init_res().set_delta(&delta.0);

        rate_limit::apply(&self.limiter, results.get().total_size()).await;
        Ok(())
    }
    async fn send_chunks(
        &self,
        reader: impl AsyncRead,
        mut offset: u64,
        cb: &read_callback::Client,
    ) -> Result<bool, StorageError> {
        let mut reader = pin::pin!(reader);
        let mut buf = vec![0; 8 * 1024];
        loop {
            let n = reader.read(&mut buf).await?;
            if n == 0 {
                return Ok(true);
            }
            let mut request = cb.chunk_request();
            let mut req = request.get();
            req.set_data(&buf.as_slice()[0..n]);
            req.set_offset(offset);
            offset += n as u64;

            rate_limit::apply(&self.limiter, req.total_size()).await;
            if request.send().await.is_err() {
                return Ok(false);
            }
        }
    }
}

fn read_errno(err: StorageError) -> io_error::Errno {
    let io_err: std::io::Error = err.into();
    io_error_to_errno(io_err)
}

impl store::Server for StoreServer {
    fn with_rate_limit(
        self: Rc<Self>,
        params: WithRateLimitParams,
        mut results: WithRateLimitResults,
    ) -> Promise<(), capnp::Error> {
        let rate_limit = pry!(params.get()).get_rate_limit();
        if rate_limit <= 0.0 {
            return Promise::err(capnp::Error::failed("invalid rate limit".to_string()));
        }

        log::debug!("@{} Rate-limiting batch access: {rate_limit}", self.peer);
        results.get().set_store(
            StoreServer {
                peer: self.peer,
                storage: self.storage.clone(),
                limiter: Some(<Limiter>::new(rate_limit)),
            }
            .into_client(),
        );

        Promise::ok(())
    }

    fn arenas(
        self: Rc<Self>,
        _: ArenasParams,
        mut results: ArenasResults,
    ) -> Promise<(), capnp::Error> {
        let arenas = self.storage.arenas();
        let mut list = results.get().init_arenas(arenas.len() as u32);
        for (i, arena) in arenas.into_iter().enumerate() {
            list.set(i as u32, arena.as_str());
        }

        Promise::ok(())
    }

    fn subscriptions(
        self: Rc<Self>,
        params: SubscriptionsParams,
        results: SubscriptionsResults,
    ) -> Promise<(), capnp::Error> {
        let peer = self.peer;
        let storage = Arc::clone(&self.storage);
        let limiter = self.limiter.clone();
        Promise::from_future(async move {
            subscribe::do_subscriptions(peer, storage, limiter, params, results).await
        })
    }

    fn read(self: Rc<Self>, params: ReadParams, _: ReadResults) -> Promise<(), capnp::Error> {
        let this = self.clone();
        Promise::from_future(async move { this.do_read(params).await })
    }

    fn rsync(
        self: Rc<Self>,
        params: RsyncParams,
        results: RsyncResults,
    ) -> Promise<(), capnp::Error> {
        let this = self.clone();
        Promise::from_future(async move { this.do_rsync(params, results).await })
    }
}

async fn get_connected_peer_store(
    client: &mut connected_peer::Client,
) -> anyhow::Result<store::Client> {
    let request = client.store_request();
    let reply = request.send().promise.await?;
    let store = reply.get()?.get_store()?;

    Ok(store)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::rpc;
    use crate::rpc::testing::HouseholdFixture;
    use fast_rsync::SignatureOptions;
    use futures::TryStreamExt as _;
    use realize_storage::{Mark, utils::hash};
    use std::time::Duration;
    use tokio::fs;

    async fn test_read_all(
        household: &Household,
        peer: Peer,
        path_str: &str,
        expected_content: &[u8],
    ) -> anyhow::Result<()> {
        let stream = household.read(
            vec![peer],
            ExecutionMode::Interactive,
            HouseholdFixture::test_arena(),
            realize_types::Path::parse(path_str)?,
            0,
            None,
        )?;
        let collected = stream.try_collect::<Vec<_>>().await?;
        assert_eq!(vec![(0, expected_content.to_vec())], collected);

        Ok(())
    }

    #[tokio::test]
    async fn household_subscribes() -> anyhow::Result<()> {
        let mut fixture = HouseholdFixture::setup().await?;
        fixture
            .with_two_peers()
            .await?
            .interconnected()
            .run(async |_, _| {
                let a = HouseholdFixture::a();
                let b = HouseholdFixture::b();

                // A file created in B's arena should eventually become
                // available in cache A.
                let b_dir = fixture.arena_root(b);
                fs::write(&b_dir.join("bar.txt"), b"test").await?;

                fixture
                    .wait_for_file_in_cache(a, "bar.txt", &hash::digest(b"test"))
                    .await?;

                Ok::<(), anyhow::Error>(())
            })
            .await?;

        Ok(())
    }

    #[tokio::test]
    async fn read_from_peer() -> anyhow::Result<()> {
        let mut fixture = HouseholdFixture::setup().await?;
        fixture
            .with_two_peers()
            .await?
            .interconnected()
            .run(async |household_a, _household_b| {
                let a = HouseholdFixture::a();
                let b = HouseholdFixture::b();

                let b_dir = fixture.arena_root(b);
                fs::write(&b_dir.join("bar.txt"), b"test").await?;

                fixture
                    .wait_for_file_in_cache(a, "bar.txt", &hash::digest(b"test"))
                    .await?;

                test_read_all(&household_a, b, "bar.txt", b"test").await?;

                let stream = household_a.read(
                    vec![b.clone()],
                    ExecutionMode::Interactive,
                    HouseholdFixture::test_arena(),
                    realize_types::Path::parse("bar.txt")?,
                    2,
                    None,
                )?;
                let collected = stream.try_collect::<Vec<_>>().await?;
                assert_eq!(vec![(2, b"st".to_vec())], collected);

                let stream = household_a.read(
                    vec![b.clone()],
                    ExecutionMode::Interactive,
                    HouseholdFixture::test_arena(),
                    realize_types::Path::parse("bar.txt")?,
                    0,
                    Some(2),
                )?;
                let collected = stream.try_collect::<Vec<_>>().await?;
                assert_eq!(vec![(0, b"te".to_vec())], collected);

                Ok::<(), anyhow::Error>(())
            })
            .await?;

        Ok(())
    }

    #[tokio::test]
    async fn rsync_from_peer() -> anyhow::Result<()> {
        let mut fixture = HouseholdFixture::setup().await?;
        fixture
            .with_two_peers()
            .await?
            .interconnected()
            .run(async |household_a, _household_b| {
                let a = HouseholdFixture::a();
                let b = HouseholdFixture::b();

                // Create a file in peer B with some content
                let b_dir = fixture.arena_root(b);
                let content = "Hello, this is a test file for rsync functionality!";
                fs::write(&b_dir.join("rsync_test.txt"), content.as_bytes()).await?;

                // Wait for the file to be available in peer A's cache
                fixture
                    .wait_for_file_in_cache(a, "rsync_test.txt", &hash::digest(content.as_bytes()))
                    .await?;

                // Create a different version of the file in peer A (similar but not identical)
                let a_dir = fixture.arena_root(a);
                let modified_content =
                    "Hello, this is a modified test file for rsync functionality!";
                fs::write(&a_dir.join("rsync_test.txt"), modified_content.as_bytes()).await?;

                // Now test rsync: create a signature of the local file and get a delta from peer B
                let arena = HouseholdFixture::test_arena();
                let path = realize_types::Path::parse("rsync_test.txt")?;
                let range = realize_types::ByteRange::new(0, content.len() as u64);

                // Create signature options similar to those used in verify.rs
                let opts = SignatureOptions {
                    block_size: 4 * 1024,
                    crypto_hash_size: 8,
                };

                // Read the local file content
                let local_content = fs::read(&a_dir.join("rsync_test.txt")).await?;

                // Calculate signature of the local content
                let signature = fast_rsync::Signature::calculate(&local_content, opts);
                let sig = realize_types::Signature(signature.into_serialized());

                // Call rsync to get delta from peer B
                let delta = household_a
                    .rsync(
                        vec![b.clone()],
                        ExecutionMode::Interactive,
                        arena,
                        &path,
                        &range,
                        sig,
                    )
                    .await?;

                // Apply the delta to reconstruct the original content
                let mut reconstructed = Vec::new();
                fast_rsync::apply_limited(
                    &local_content,
                    &delta.0,
                    &mut reconstructed,
                    local_content.len(),
                )?;

                // Verify that the reconstructed content matches the original content from peer B
                assert_eq!(content.as_bytes(), reconstructed.as_slice());

                // Also verify that the reconstructed content is different from the local modified content
                assert_ne!(modified_content.as_bytes(), reconstructed.as_slice());

                Ok::<(), anyhow::Error>(())
            })
            .await?;

        Ok(())
    }

    #[tokio::test]
    async fn rsync_with_identical_files() -> anyhow::Result<()> {
        let mut fixture = HouseholdFixture::setup().await?;
        fixture
            .with_two_peers()
            .await?
            .interconnected()
            .run(async |household_a, _household_b| {
                let arena = HouseholdFixture::test_arena();
                let a = HouseholdFixture::a();
                let b = HouseholdFixture::b();

                fixture.storage(a)?.set_arena_mark(arena, Mark::Own).await?;
                fixture.storage(b)?.set_arena_mark(arena, Mark::Own).await?;

                // Create identical files in both peers
                let content = "Identical content for both peers";
                let a_dir = fixture.arena_root(a);
                let b_dir = fixture.arena_root(b);

                fs::write(&a_dir.join("identical.txt"), content.as_bytes()).await?;
                fs::write(&b_dir.join("identical.txt"), content.as_bytes()).await?;

                // Wait for the file to be available in peer A's cache
                fixture
                    .wait_for_file_in_cache(a, "identical.txt", &hash::digest(content.as_bytes()))
                    .await?;

                let arena = HouseholdFixture::test_arena();
                let path = realize_types::Path::parse("identical.txt")?;
                let range = realize_types::ByteRange::new(0, content.len() as u64);

                let opts = SignatureOptions {
                    block_size: 4 * 1024,
                    crypto_hash_size: 8,
                };

                let local_content = fs::read(&a_dir.join("identical.txt")).await?;
                let signature = fast_rsync::Signature::calculate(&local_content, opts);
                let sig = realize_types::Signature(signature.into_serialized());

                // Call rsync - should return an empty or minimal delta since files are identical
                let delta = household_a
                    .rsync(
                        vec![b.clone()],
                        ExecutionMode::Interactive,
                        arena,
                        &path,
                        &range,
                        sig,
                    )
                    .await?;

                // Apply the delta
                let mut reconstructed = Vec::new();
                fast_rsync::apply_limited(
                    &local_content,
                    &delta.0,
                    &mut reconstructed,
                    local_content.len(),
                )?;

                // Should still match the original content
                assert_eq!(content.as_bytes(), reconstructed.as_slice());

                Ok::<(), anyhow::Error>(())
            })
            .await?;

        Ok(())
    }

    #[tokio::test]
    async fn rsync_with_completely_different_files() -> anyhow::Result<()> {
        let mut fixture = HouseholdFixture::setup().await?;
        fixture
            .with_two_peers()
            .await?
            .interconnected()
            .run(async |household_a, _household_b| {
                let a = HouseholdFixture::a();
                let b = HouseholdFixture::b();

                let b_content = "Completely different content from peer B";
                let b_dir = fixture.arena_root(b);
                fs::write(&b_dir.join("different.txt"), b_content.as_bytes()).await?;
                fixture
                    .wait_for_file_in_cache(a, "different.txt", &hash::digest(b_content.as_bytes()))
                    .await?;

                let arena = HouseholdFixture::test_arena();
                let path = realize_types::Path::parse("different.txt")?;
                let range = realize_types::ByteRange::new(0, b_content.len() as u64);

                let opts = SignatureOptions {
                    block_size: 4 * 1024,
                    crypto_hash_size: 8,
                };

                // Call rsync - should return a delta that transforms local_content into b_content
                let local_content = b"Content from peer A";
                let signature = fast_rsync::Signature::calculate(local_content, opts);
                let sig = realize_types::Signature(signature.into_serialized());
                let delta = household_a
                    .rsync(
                        vec![b.clone()],
                        ExecutionMode::Interactive,
                        arena,
                        &path,
                        &range,
                        sig,
                    )
                    .await?;

                // Apply the delta to local_content, the result should be b_content.
                let mut reconstructed = Vec::new();
                fast_rsync::apply_limited(
                    local_content,
                    &delta.0,
                    &mut reconstructed,
                    b_content.len(),
                )?;
                assert_eq!(b_content.as_bytes(), reconstructed.as_slice());

                Ok::<(), anyhow::Error>(())
            })
            .await?;

        Ok(())
    }

    #[tokio::test]
    async fn household_peers() -> anyhow::Result<()> {
        let mut fixture = HouseholdFixture::setup().await?;
        fixture
            .with_two_peers()
            .await?
            .interconnected()
            .run(async |household_a, _| {
                let b = HouseholdFixture::b();
                let c = HouseholdFixture::c();

                assert_eq!(
                    HashMap::from([
                        (
                            b,
                            ConnectionInfo {
                                connection: ConnectionStatus::Connected,
                                keep_connected: true
                            }
                        ),
                        (
                            c,
                            ConnectionInfo {
                                connection: ConnectionStatus::Disconnected,
                                keep_connected: false
                            }
                        )
                    ]),
                    household_a.query_peers().await?
                );

                let mut status_a = household_a.peer_status();
                fixture.server(b).unwrap().shutdown().await?;
                assert_eq!(PeerStatus::Disconnected(b), status_a.recv().await?);

                assert_eq!(
                    HashMap::from([
                        (
                            b,
                            ConnectionInfo {
                                connection: ConnectionStatus::Disconnected,
                                keep_connected: true
                            }
                        ),
                        (
                            c,
                            ConnectionInfo {
                                connection: ConnectionStatus::Disconnected,
                                keep_connected: false
                            }
                        )
                    ]),
                    household_a.query_peers().await?
                );

                Ok::<(), anyhow::Error>(())
            })
            .await?;

        Ok(())
    }

    #[tokio::test]
    async fn reverse_connection() -> anyhow::Result<()> {
        let mut fixture = HouseholdFixture::setup().await?;
        fixture
            .with_two_peers() // not interconnected
            .await?
            .run(async |household_a, household_b| {
                let a = HouseholdFixture::a();
                let b = HouseholdFixture::b();

                let mut peer_status_a = household_a.peer_status();
                let mut peer_status_b = household_b.peer_status();

                // Only one side is connected, from A to B.
                rpc::testing::connect(&household_a, b).await?;

                // Both sides end up being able to talk to each other
                // thanks to reverse connections.
                assert_eq!(
                    PeerStatus::Connected(b),
                    tokio::time::timeout(Duration::from_secs(3), peer_status_a.recv()).await??,
                );
                assert_eq!(
                    PeerStatus::Connected(a),
                    tokio::time::timeout(Duration::from_secs(3), peer_status_b.recv()).await??,
                );

                // A file created in B's arena should eventually become
                // available in cache A and vice-versa.
                fs::write(&fixture.arena_root(b).join("in_b"), b"test").await?;
                fs::write(&fixture.arena_root(a).join("in_a"), b"test").await?;

                fixture
                    .wait_for_file_in_cache(a, "in_b", &hash::digest(b"test"))
                    .await?;
                fixture
                    .wait_for_file_in_cache(b, "in_a", &hash::digest(b"test"))
                    .await?;

                // Make sure that the remote peer can be read from, no
                // matter the side.
                test_read_all(&household_b, a, "in_a", b"test").await?;
                test_read_all(&household_a, b, "in_b", b"test").await?;

                // A single disconnection disconnects both sides.
                household_a.disconnect_peer(b)?;
                assert_eq!(
                    PeerStatus::Disconnected(b),
                    tokio::time::timeout(Duration::from_secs(3), peer_status_a.recv()).await??,
                );
                assert_eq!(
                    PeerStatus::Disconnected(a),
                    tokio::time::timeout(Duration::from_secs(3), peer_status_b.recv()).await??,
                );

                Ok::<(), anyhow::Error>(())
            })
            .await?;

        Ok(())
    }
    #[tokio::test]
    async fn single_side_disconnect() -> anyhow::Result<()> {
        let mut fixture = HouseholdFixture::setup().await?;
        fixture
            .with_two_peers()
            .await?
            .interconnected()
            .run(async |household_a, household_b| {
                let a = HouseholdFixture::a();
                let b = HouseholdFixture::b();

                // A is connected to B and B to A, since interconnected() was called.

                fs::write(&fixture.arena_root(b).join("in_b"), b"test").await?;
                fs::write(&fixture.arena_root(a).join("in_a"), b"test").await?;
                fixture
                    .wait_for_file_in_cache(a, "in_b", &hash::digest(b"test"))
                    .await?;
                fixture
                    .wait_for_file_in_cache(b, "in_a", &hash::digest(b"test"))
                    .await?;

                test_read_all(&household_b, a, "in_a", b"test").await?;
                test_read_all(&household_a, b, "in_b", b"test").await?;

                // If one side disconnects (A->B), the other side
                // takes over.
                //
                // We can't wait for disconnections, since it's not
                // reported to peer_status; after all both sides are
                // still connected. As a result, the code below is a
                // bit racy, but it shouldn't matter since it should
                // always work no matter the timing.
                household_a.disconnect_all()?;
                tokio::task::yield_now().await; // allow disconnection to happen

                test_read_all(&household_b, a, "in_a", b"test").await?;
                test_read_all(&household_a, b, "in_b", b"test").await?;

                Ok::<(), anyhow::Error>(())
            })
            .await?;

        Ok(())
    }
}
