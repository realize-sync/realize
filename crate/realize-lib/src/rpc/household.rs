use super::peer_capnp::connected_peer;
use super::result_capnp;
use super::store_capnp::read_callback::{ChunkParams, FinishParams, FinishResults};
use super::store_capnp::store::{
    self, ArenasParams, ArenasResults, ReadParams, ReadResults, SubscribeParams, SubscribeResults,
};
use super::store_capnp::subscriber::{self, NotifyParams, NotifyResults};
use super::store_capnp::{notification, read_callback, read_error};
use crate::model::{self, Arena, Hash, Path, Peer, UnixTime};
use crate::network::capnp::{ConnectionHandler, ConnectionManager};
use crate::network::{Networking, Server};
use crate::storage::{Notification, Progress, Storage, StorageError, UnrealCacheAsync};
use crate::utils::holder::ByteConversionError;
use capnp::capability::Promise;
use capnp_rpc::pry;
use std::collections::{HashMap, HashSet};
use std::io::{self, SeekFrom};
use std::pin;
use std::sync::Arc;
use tokio::io::AsyncSeekExt;
use tokio::io::{AsyncRead, AsyncReadExt};
use tokio::sync::mpsc;
use tokio::task::LocalSet;
use tokio_stream::wrappers::ReceiverStream;
use uuid::Uuid;

/// Identifies Cap'n Proto ConnectedPeer connections.
const TAG: &[u8; 4] = b"PEER";

/// A set of peers and their connections.
///
/// Cap'n Proto connections are handled or their own thread. This
/// object serves as a communication channel between that thread and
/// the rest of the application.
///
/// To listen to incoming connections, call [Household::register].
#[derive(Clone)]
pub struct Household {
    manager: Arc<ConnectionManager<HouseholdOperation>>,
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
    ) -> anyhow::Result<Self> {
        let manager =
            ConnectionManager::spawn(local, networking, PeerConnectionHandler::new(storage))?;

        Ok(Self {
            manager: Arc::new(manager),
        })
    }

    /// Keep a client connection up to all peers for which an address is known.
    pub fn keep_connected(&self) -> anyhow::Result<()> {
        self.manager.keep_connected()
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
        arena: Arena,
        path: Path,
        offset: u64,
        limit: Option<u64>,
    ) -> io::Result<ReceiverStream<Result<Vec<u8>, io::Error>>>
    where
        T: IntoIterator<Item = Peer>,
    {
        let (tx, rx) = mpsc::channel(10);
        self.manager
            .with_any_peer_client(
                peers,
                HouseholdOperation::Read {
                    arena,
                    path,
                    offset,
                    limit,
                    tx,
                },
            )
            .map_err(|err| io::Error::other(err))?;

        Ok(ReceiverStream::new(rx))
    }
}

enum HouseholdOperation {
    Read {
        arena: Arena,
        path: model::Path,
        offset: u64,
        limit: Option<u64>,
        tx: mpsc::Sender<Result<Vec<u8>, io::Error>>,
    },
}
struct PeerConnectionHandler {
    storage: Arc<Storage>,
}

impl PeerConnectionHandler {
    fn new(storage: Arc<Storage>) -> Self {
        Self { storage }
    }
}

impl ConnectionHandler<connected_peer::Client, HouseholdOperation> for PeerConnectionHandler {
    fn tag(&self) -> &'static [u8; 4] {
        TAG
    }

    fn server(&self, peer: &Peer) -> capnp::capability::Client {
        ConnectedPeerServer::new(peer.clone(), self.storage.clone())
            .into_connected_peer()
            .client
    }

    async fn check_connection(
        &self,
        _peer: &Peer,
        client: &mut connected_peer::Client,
    ) -> anyhow::Result<()> {
        get_connected_peer_store(client).await?;

        Ok(())
    }

    async fn register(&self, peer: &Peer, client: connected_peer::Client) -> anyhow::Result<()> {
        subscribe_self(&self.storage, peer, client).await?;

        Ok(())
    }

    async fn execute(
        &self,
        client: Option<(Peer, connected_peer::Client)>,
        operation: HouseholdOperation,
    ) {
        match operation {
            HouseholdOperation::Read {
                tx,
                arena,
                path,
                offset,
                limit,
            } => {
                let tx_clone = tx.clone();
                if let Err(err) = execute_read(client, arena, path, offset, limit, tx).await {
                    let _ = tx_clone.send(Err(err)).await;
                }
            }
        }
    }
}

async fn execute_read(
    client: Option<(Peer, connected_peer::Client)>,
    arena: Arena,
    path: model::Path,
    offset: u64,
    limit: Option<u64>,
    tx: mpsc::Sender<io::Result<Vec<u8>>>,
) -> io::Result<()> {
    let (peer, client) = match client {
        Some(c) => c,
        None => {
            return Err(io::Error::new(io::ErrorKind::NotFound, "No available peer"));
        }
    };
    log::debug!("Reading [{arena}]/{path} from {peer}");

    let store = client.store_request().send().pipeline.get_store();
    let mut request = store.read_request();
    let mut builder = request.get();
    builder.set_cb(capnp_rpc::new_client(ReadCallbackServer { tx: Some(tx) }));
    let mut req = builder.init_req();
    req.set_arena(arena.as_str());
    req.set_path(path.as_str());
    req.set_start_offset(offset);
    if let Some(limit) = limit {
        req.set_limit(limit);
    }

    request
        .send()
        .promise
        .await
        .map_err(|err| io::Error::other(err))?;

    Ok(())
}

struct ReadCallbackServer {
    tx: Option<mpsc::Sender<io::Result<Vec<u8>>>>,
}

impl read_callback::Server for ReadCallbackServer {
    fn chunk(&mut self, chunk: ChunkParams) -> Promise<(), capnp::Error> {
        let tx = pry!(self.tx.as_ref().ok_or_else(already_finished)).clone();
        Promise::from_future(async move {
            let data = chunk.get()?.get_data()?.to_vec();
            tx.send(Ok(data)).await.map_err(channel_closed)?;

            Ok(())
        })
    }

    fn finish(&mut self, params: FinishParams, _: FinishResults) -> Promise<(), capnp::Error> {
        let tx = pry!(self.tx.take().ok_or_else(already_finished));
        Promise::from_future(async move {
            let result = params.get()?.get_result()?;
            match result.which()? {
                result_capnp::result::Which::Ok(_) => {}
                result_capnp::result::Which::Err(err) => {
                    tx.send(Err(errno_to_io_error(err?.get_errno()?)))
                        .await
                        .map_err(channel_closed)?;
                }
            }
            Ok(())
        })
    }
}

fn errno_to_io_error(errno: read_error::Errno) -> io::Error {
    use io::ErrorKind::*;

    io::Error::new(
        match errno {
            read_error::Errno::Other => Other,
            read_error::Errno::GenericIo => Other,
            read_error::Errno::Unavailable => Other,
            read_error::Errno::PermissionDenied => PermissionDenied,
            read_error::Errno::NotADirectory => NotADirectory,
            read_error::Errno::IsADirectory => IsADirectory,
            read_error::Errno::InvalidInput => InvalidInput,
            read_error::Errno::Closed => Other,
            read_error::Errno::Aborted => ConnectionAborted,
            read_error::Errno::NotFound => NotFound,
            read_error::Errno::ResourceBusy => ResourceBusy,
            read_error::Errno::InvalidPath => InvalidFilename,
        },
        format!("{errno:?}"),
    )
}

fn already_finished() -> capnp::Error {
    capnp::Error::failed("already finished".to_string())
}

fn channel_closed<T>(_: mpsc::error::SendError<T>) -> capnp::Error {
    capnp::Error::failed("channel closed".to_string())
}

/// Subscribe to notifications from the given client and use it to
/// update the cache.
async fn subscribe_self(
    storage: &Arc<Storage>,
    peer: &Peer,
    mut client: connected_peer::Client,
) -> anyhow::Result<()> {
    let store = get_connected_peer_store(&mut client).await?;
    let cache = match storage.cache() {
        None => {
            return Ok(());
        }
        Some(c) => c,
    };

    let request = store.arenas_request();
    let reply = request.send().promise.await?;
    let arenas = reply.get()?.get_arenas()?;
    let peer_arenas = parse_arena_set(arenas)?;

    let goal_arenas = cache
        .arenas()
        .filter(|a| peer_arenas.contains(*a))
        .map(|a| a.clone())
        .collect::<Vec<_>>();
    if goal_arenas.is_empty() {
        log::debug!(
            "Not subscribing to {peer}: no common arena. {:?} vs {:?}",
            peer_arenas,
            cache.arenas().collect::<Vec<_>>(),
        );

        return Ok(());
    }
    log::debug!(
        "Subscribe to {} on {peer}",
        goal_arenas
            .iter()
            .map(|a| a.as_str())
            .collect::<Vec<_>>()
            .join(", ")
    );
    let mut progress = tokio::spawn({
        let peer = peer.clone();
        let goal_arenas = goal_arenas.clone();
        let cache = cache.clone();
        async move {
            let mut map = HashMap::new();
            for arena in goal_arenas {
                if let Some(progress) = cache.peer_progress(&peer, &arena).await? {
                    map.insert(arena, progress);
                }
            }

            Ok::<_, anyhow::Error>(map)
        }
    })
    .await??;

    let subscriber = ConnectedPeerServer::new(peer.clone(), storage.clone()).into_subscriber();
    for arena in goal_arenas {
        let mut request = store.subscribe_request();
        let mut request_builder = request.get().init_req();
        request_builder.set_arena(arena.as_str());
        request_builder.set_subscriber(subscriber.clone());
        if let Some(progress) = progress.remove(&arena) {
            let mut builder = request_builder.init_progress();
            builder.set_last_seen(progress.last_seen);
            fill_uuid(builder.init_uuid(), &progress.uuid);
        }

        let reply = request.send().promise.await?;
        let result = reply.get()?.get_result()?;

        if let result_capnp::result::Err(err) = result.which()? {
            return Err(anyhow::anyhow!(err?.get_message()?.to_string()?));
        }
    }

    Ok(())
}

/// Implement capnp interface ConnectedPeer, defined in
/// `capnp/peer.capnp`.
#[derive(Clone)]
struct ConnectedPeerServer {
    peer: Peer,
    storage: Arc<Storage>,
}

impl ConnectedPeerServer {
    fn new(peer: Peer, storage: Arc<Storage>) -> Self {
        Self { peer, storage }
    }

    fn into_connected_peer(self) -> connected_peer::Client {
        capnp_rpc::new_client(self)
    }

    fn into_store(self) -> store::Client {
        capnp_rpc::new_client(self)
    }

    fn into_subscriber(self) -> subscriber::Client {
        capnp_rpc::new_client(self)
    }

    async fn do_subscribe(
        &self,
        params: SubscribeParams,
        mut results: SubscribeResults,
    ) -> Result<(), capnp::Error> {
        let req = params.get()?.get_req()?;
        let arena = parse_arena(req.get_arena()?)?;

        let result = results.get().init_result();
        let progress = if req.has_progress() {
            let progress = req.get_progress()?;
            Some(Progress::new(
                parse_uuid(progress.get_uuid()?),
                progress.get_last_seen(),
            ))
        } else {
            None
        };

        let subscriber = req.get_subscriber()?;

        let (tx, mut rx) = mpsc::channel(100);

        if let Err(err) = tokio::spawn({
            let storage = self.storage.clone();
            let arena = arena.clone();
            async move {
                storage.subscribe(&arena, tx, progress).await?;

                Ok::<(), anyhow::Error>(())
            }
        })
        .await
        {
            result.init_err().set_message(err.to_string());
            return Ok(());
        }

        let peer = self.peer.clone();
        log::debug!("{peer} subscribed to notifications from {arena}");
        tokio::task::spawn_local(async move {
            let mut notifications = Vec::new();
            loop {
                let count = rx.recv_many(&mut notifications, 25).await;
                if count == 0 {
                    // Channel has been closed
                    return;
                }
                log::debug!("notify {peer}: {notifications:?}");
                if let Err(err) = send_notifications(notifications.as_slice(), &subscriber).await {
                    if err.kind == capnp::ErrorKind::Disconnected {
                        return;
                    }
                }
                notifications.clear();
            }
        });

        result.init_ok();

        Ok(())
    }

    async fn do_read(&self, params: ReadParams) -> Result<(), capnp::Error> {
        let params = params.get()?;
        let req = params.get_req()?;

        let arena = parse_arena(req.get_arena()?)?;
        let path = parse_path(req.get_path()?)?;
        let offset = req.get_start_offset();
        let limit = req.get_limit();
        let cb = params.get_cb()?;

        let read_result = self.read_all(&arena, &path, offset, limit, &cb).await;
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
                    result.init_err().set_errno(read_error::Errno::Aborted);
                }
            }
        }
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
        arena: &Arena,
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
            send_chunks(reader.take(limit), cb).await
        } else {
            send_chunks(reader, cb).await
        }
    }
}

async fn send_chunks(
    reader: impl AsyncRead,
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
        request.get().set_data(&buf.as_slice()[0..n]);
        if request.send().await.is_err() {
            return Ok(false);
        }
    }
}

fn read_errno(err: StorageError) -> read_error::Errno {
    use read_error::Errno::*;
    match err {
        StorageError::Database(_) => Unavailable,
        StorageError::Io(ioerr) => match ioerr.kind() {
            io::ErrorKind::NotFound => NotFound,
            io::ErrorKind::PermissionDenied => PermissionDenied,
            io::ErrorKind::ConnectionAborted => Aborted,
            io::ErrorKind::NotADirectory => NotADirectory,
            io::ErrorKind::IsADirectory => IsADirectory,
            io::ErrorKind::InvalidInput => InvalidInput,
            io::ErrorKind::ResourceBusy => ResourceBusy,
            _ => GenericIo,
        },
        StorageError::ByteConversion(ByteConversionError::Path(_)) => InvalidPath,
        StorageError::ByteConversion(_) => Unavailable,
        StorageError::Unavailable => Unavailable,
        StorageError::NotFound => NotFound,
        StorageError::NotADirectory => NotADirectory,
        StorageError::IsADirectory => IsADirectory,
        StorageError::JoinError(_) => Other,
        StorageError::UnknownArena(_) => NotFound,
    }
}

impl connected_peer::Server for ConnectedPeerServer {
    fn store(
        &mut self,
        _params: connected_peer::StoreParams,
        mut results: connected_peer::StoreResults,
    ) -> Promise<(), capnp::Error> {
        results.get().set_store(self.clone().into_store());

        Promise::ok(())
    }
}

impl store::Server for ConnectedPeerServer {
    fn arenas(&mut self, _: ArenasParams, mut results: ArenasResults) -> Promise<(), capnp::Error> {
        let arenas = self
            .storage
            .indexed_arenas()
            .map(|a| a.clone())
            .collect::<Vec<_>>();
        let mut list = results.get().init_arenas(arenas.len() as u32);
        for (i, arena) in arenas.into_iter().enumerate() {
            list.set(i as u32, arena.as_str());
        }

        Promise::ok(())
    }

    fn subscribe(
        &mut self,
        params: SubscribeParams,
        results: SubscribeResults,
    ) -> Promise<(), capnp::Error> {
        let this = self.clone();
        Promise::from_future(async move { this.do_subscribe(params, results).await })
    }

    fn read(&mut self, params: ReadParams, _t: ReadResults) -> Promise<(), capnp::Error> {
        let this = self.clone();
        Promise::from_future(async move { this.do_read(params).await })
    }
}

impl subscriber::Server for ConnectedPeerServer {
    fn notify(
        &mut self,
        params: NotifyParams,
        _: NotifyResults,
    ) -> capnp::capability::Promise<(), capnp::Error> {
        if let Some(cache) = self.storage.cache() {
            Promise::from_future(do_notify(cache.clone(), self.peer.clone(), params))
        } else {
            Promise::ok(())
        }
    }
}

async fn do_notify(
    cache: UnrealCacheAsync,
    peer: Peer,
    params: NotifyParams,
) -> Result<(), capnp::Error> {
    let mut notifications = vec![];
    for n in params.get()?.get_notifications()?.iter() {
        notifications.push(match n.which()? {
            notification::Which::Add(add) => {
                let add = add?;

                Notification::Add {
                    arena: parse_arena(add.get_arena()?)?,
                    index: add.get_index(),
                    path: parse_path(add.get_path()?)?,
                    size: add.get_size(),
                    mtime: parse_mtime(add.get_mtime()?),
                    hash: parse_hash(add.get_hash()?)?,
                }
            }
            notification::Which::Replace(replace) => {
                let replace = replace?;

                Notification::Replace {
                    arena: parse_arena(replace.get_arena()?)?,
                    index: replace.get_index(),
                    path: parse_path(replace.get_path()?)?,
                    mtime: parse_mtime(replace.get_mtime()?),
                    size: replace.get_size(),
                    hash: parse_hash(replace.get_hash()?)?,
                    old_hash: parse_hash(replace.get_old_hash()?)?,
                }
            }
            notification::Which::Remove(remove) => {
                let remove = remove?;

                Notification::Remove {
                    arena: parse_arena(remove.get_arena()?)?,
                    index: remove.get_index(),
                    path: parse_path(remove.get_path()?)?,
                    old_hash: parse_hash(remove.get_old_hash()?)?,
                }
            }
            notification::Which::CatchupStart(start) => {
                Notification::CatchupStart(parse_arena(start?.get_arena()?)?)
            }
            notification::Which::Catchup(catchup) => {
                let catchup = catchup?;

                Notification::Catchup {
                    arena: parse_arena(catchup.get_arena()?)?,
                    path: parse_path(catchup.get_path()?)?,
                    size: catchup.get_size(),
                    mtime: parse_mtime(catchup.get_mtime()?),
                    hash: parse_hash(catchup.get_hash()?)?,
                }
            }
            notification::Which::CatchupComplete(complete) => {
                let complete = complete?;

                Notification::CatchupComplete {
                    arena: parse_arena(complete.get_arena()?)?,
                    index: complete.get_index(),
                }
            }
            notification::Which::Connected(connected) => {
                let connected = connected?;

                Notification::Connected {
                    arena: parse_arena(connected.get_arena()?)?,
                    uuid: parse_uuid(connected.get_uuid()?),
                }
            }
        });
    }

    tokio::spawn(async move {
        for notification in notifications {
            cache.update(&peer, notification).await?;
        }

        Ok::<(), StorageError>(())
    })
    .await
    .map_err(|e| capnp::Error::failed(e.to_string()))?
    .map_err(|e| capnp::Error::failed(e.to_string()))?;

    Ok(())
}

async fn send_notifications(
    notifications: &[Notification],
    client: &subscriber::Client,
) -> Result<(), capnp::Error> {
    let mut request = client.notify_request();
    let mut builder = request.get().init_notifications(notifications.len() as u32);
    for (i, notif) in notifications.iter().enumerate() {
        let notif_builder = builder.reborrow().get(i as u32);
        match notif {
            Notification::Add {
                arena,
                index,
                path,
                size,
                mtime,
                hash,
            } => fill_add(
                notif_builder.init_add(),
                arena,
                *index,
                path,
                *size,
                mtime,
                hash,
            ),

            Notification::Replace {
                arena,
                index,
                path,
                size,
                mtime,
                hash,
                old_hash,
            } => fill_replace(
                notif_builder.init_replace(),
                arena,
                *index,
                path,
                *size,
                mtime,
                hash,
                old_hash,
            ),

            Notification::Remove {
                arena,
                index,
                path,
                old_hash,
            } => fill_remove(notif_builder.init_remove(), arena, *index, path, old_hash),

            Notification::Catchup {
                arena,
                path,
                size,
                mtime,
                hash,
            } => fill_catchup(
                notif_builder.init_catchup(),
                arena,
                path,
                *size,
                mtime,
                hash,
            ),

            Notification::CatchupStart(arena) => {
                notif_builder.init_catchup_start().set_arena(arena.as_str())
            }

            Notification::CatchupComplete { arena, index } => {
                let mut builder = notif_builder.init_catchup_complete();
                builder.set_arena(arena.as_str());
                builder.set_index(*index);
            }

            Notification::Connected { arena, uuid } => {
                let mut builder = notif_builder.init_connected();
                builder.set_arena(arena.as_str());
                fill_uuid(builder.init_uuid(), &uuid);
            }
        }
    }
    let _ = request.send().promise.await?;

    Ok(())
}

fn fill_uuid(mut builder: super::store_capnp::uuid::Builder<'_>, uuid: &Uuid) {
    let (hi, lo) = uuid.as_u64_pair();
    builder.set_hi(hi);
    builder.set_lo(lo);
}

fn fill_add(
    mut builder: super::store_capnp::add::Builder<'_>,
    arena: &Arena,
    index: u64,
    path: &crate::model::Path,
    size: u64,
    mtime: &crate::model::UnixTime,
    hash: &crate::model::Hash,
) {
    builder.set_arena(arena.as_str());
    builder.set_index(index);
    builder.set_path(path.as_str());
    builder.set_size(size);
    builder.set_hash(&hash.0);
    fill_time(builder.init_mtime(), mtime);
}

fn fill_replace(
    mut builder: super::store_capnp::replace::Builder<'_>,
    arena: &Arena,
    index: u64,
    path: &crate::model::Path,
    size: u64,
    mtime: &crate::model::UnixTime,
    hash: &crate::model::Hash,
    old_hash: &crate::model::Hash,
) {
    builder.set_arena(arena.as_str());
    builder.set_index(index);
    builder.set_path(path.as_str());
    builder.set_size(size);
    builder.set_hash(&hash.0);
    builder.set_old_hash(&old_hash.0);
    fill_time(builder.init_mtime(), mtime);
}

fn fill_remove(
    mut builder: super::store_capnp::remove::Builder<'_>,
    arena: &Arena,
    index: u64,
    path: &crate::model::Path,
    old_hash: &crate::model::Hash,
) {
    builder.set_arena(arena.as_str());
    builder.set_index(index);
    builder.set_path(path.as_str());
    builder.set_old_hash(&old_hash.0);
}

fn fill_catchup(
    mut builder: super::store_capnp::catchup::Builder<'_>,
    arena: &Arena,
    path: &crate::model::Path,
    size: u64,
    mtime: &crate::model::UnixTime,
    hash: &crate::model::Hash,
) {
    builder.set_arena(arena.as_str());
    builder.set_path(path.as_str());
    builder.set_size(size);
    builder.set_hash(&hash.0);
    fill_time(builder.init_mtime(), mtime);
}

fn fill_time(
    mut mtime_builder: super::store_capnp::time::Builder<'_>,
    mtime: &crate::model::UnixTime,
) {
    mtime_builder.set_secs(mtime.as_secs());
    mtime_builder.set_nsecs(mtime.subsec_nanos());
}

async fn get_connected_peer_store(
    client: &mut connected_peer::Client,
) -> anyhow::Result<store::Client> {
    let request = client.store_request();
    let reply = request.send().promise.await?;
    let store = reply.get()?.get_store()?;

    Ok(store)
}

fn parse_arena(reader: capnp::text::Reader<'_>) -> Result<Arena, capnp::Error> {
    Ok(Arena::from(reader.to_str()?))
}

fn parse_arena_set(arenas: capnp::text_list::Reader<'_>) -> Result<HashSet<Arena>, capnp::Error> {
    let mut set = HashSet::new();
    for arena in arenas.iter() {
        set.insert(parse_arena(arena?)?);
    }
    Ok(set)
}

fn parse_uuid(reader: super::store_capnp::uuid::Reader<'_>) -> Uuid {
    Uuid::from_u64_pair(reader.get_hi(), reader.get_lo())
}

fn parse_mtime(reader: super::store_capnp::time::Reader<'_>) -> UnixTime {
    UnixTime::new(reader.get_secs(), reader.get_nsecs())
}

fn parse_path(reader: capnp::text::Reader<'_>) -> Result<Path, capnp::Error> {
    Path::parse(reader.to_str()?).map_err(|e| capnp::Error::failed(e.to_string()))
}

fn parse_hash(hash: &[u8]) -> Result<Hash, capnp::Error> {
    let hash: [u8; 32] = hash
        .try_into()
        .map_err(|_| capnp::Error::failed("invalid hash".to_string()))?;

    Ok(Hash(hash))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::model::Arena;
    use crate::network::hostport::HostPort;
    use crate::network::testing::TestingPeers;
    use crate::storage;
    use assert_fs::TempDir;
    use assert_fs::prelude::*;
    use futures::TryStreamExt as _;
    use std::path::PathBuf;
    use std::sync::Arc;
    use std::time::Duration;
    use tokio::fs;
    use tokio_retry::strategy::FixedInterval;

    fn a() -> Peer {
        TestingPeers::a()
    }
    fn b() -> Peer {
        TestingPeers::b()
    }
    fn c() -> Peer {
        TestingPeers::c()
    }

    fn test_arena() -> Arena {
        Arena::from("myarena")
    }

    struct Fixture {
        peers: TestingPeers,
        peer_storage: HashMap<Peer, Arc<Storage>>,
        tempdir: TempDir,
    }
    impl Fixture {
        async fn setup() -> anyhow::Result<Self> {
            let _ = env_logger::try_init();

            let tempdir = TempDir::new()?;
            let mut peer_storage = HashMap::new();
            for peer in [a(), b(), c()] {
                let s =
                    storage::testing::storage(tempdir.child(peer.as_str()).path(), [test_arena()])
                        .await?;
                peer_storage.insert(peer, s);
            }
            Ok(Self {
                peers: TestingPeers::new()?,
                peer_storage,
                tempdir,
            })
        }

        fn arena_root(&self, peer: &Peer) -> PathBuf {
            storage::testing::arena_root(self.tempdir.child(peer.as_str()).path(), &test_arena())
        }

        fn household(&self, local: &LocalSet, peer: &Peer) -> anyhow::Result<Household> {
            let storage = self
                .peer_storage
                .get(peer)
                .ok_or_else(|| anyhow::anyhow!("unknown peer: {peer}"))?;

            Household::spawn(local, self.peers.networking(peer)?, storage.clone())
        }

        async fn run_server(
            &mut self,
            peer: &Peer,
            household: &Household,
        ) -> anyhow::Result<Arc<Server>> {
            let mut server = Server::new(self.peers.networking(peer)?);
            household.register(&mut server);

            let server = Arc::new(server);

            let configured = self.peers.hostport(peer).await;
            let addr = server
                .listen(configured.unwrap_or(&HostPort::localhost(0)))
                .await?;
            if configured.is_none() {
                self.peers.set_addr(peer, addr);
            }

            Ok(server)
        }

        /// Wait for the given file to appear in the given peer's cache, in the test arena.
        async fn wait_for_file(&self, peer: &Peer, filename: &str) -> anyhow::Result<()> {
            let cache = &self
                .peer_storage
                .get(peer)
                .ok_or(anyhow::anyhow!("Unknown peer {peer}"))?
                .cache()
                .ok_or(anyhow::anyhow!("No cache for {peer}"))?;

            let mut retry = FixedInterval::new(Duration::from_millis(50)).take(100);
            let arena = test_arena();
            let arena_inode = cache.arena_root(&arena)?;
            while cache.lookup(arena_inode, filename).await.is_err() {
                if let Some(delay) = retry.next() {
                    tokio::time::sleep(delay).await;
                } else {
                    panic!("[arena]/{filename} was never added to the cache");
                }
            }

            Ok(())
        }
    }

    #[tokio::test]
    async fn household_subscribes() -> anyhow::Result<()> {
        let mut fixture = Fixture::setup().await?;
        let local = LocalSet::new();

        let a = &a();
        fixture.peers.pick_port(a)?;

        let b = &b();
        fixture.peers.pick_port(b)?;

        let household_a = fixture.household(&local, a)?;
        let _server_a = fixture.run_server(a, &household_a).await?;

        let household_b = fixture.household(&local, b)?;
        let _server_b = fixture.run_server(b, &household_b).await?;
        household_a.keep_connected()?;

        local
            .run_until(async move {
                // A file created in B's arena should eventually become
                // available in cache A.
                let b_dir = fixture.arena_root(&b);
                fs::write(&b_dir.join("bar.txt"), b"test").await?;

                fixture.wait_for_file(a, "bar.txt").await?;

                Ok::<(), anyhow::Error>(())
            })
            .await?;

        Ok(())
    }

    #[tokio::test]
    async fn read_from_peer() -> anyhow::Result<()> {
        let mut fixture = Fixture::setup().await?;
        let local = LocalSet::new();

        let a = &a();
        fixture.peers.pick_port(a)?;

        let b = &b();
        fixture.peers.pick_port(b)?;

        let household_a = fixture.household(&local, a)?;
        let _server_a = fixture.run_server(a, &household_a).await?;

        let household_b = fixture.household(&local, b)?;
        let _server_b = fixture.run_server(b, &household_b).await?;
        household_a.keep_connected()?;

        local
            .run_until(async move {
                let b_dir = fixture.arena_root(&b);
                fs::write(&b_dir.join("bar.txt"), b"test").await?;

                fixture.wait_for_file(a, "bar.txt").await?;

                let stream = household_a.read(
                    vec![b.clone()],
                    test_arena(),
                    model::Path::parse("bar.txt")?,
                    0,
                    None,
                )?;
                let collected = stream.try_collect::<Vec<_>>().await?;
                assert_eq!(vec![b"test".to_vec()], collected);

                let stream = household_a.read(
                    vec![b.clone()],
                    test_arena(),
                    model::Path::parse("bar.txt")?,
                    2,
                    None,
                )?;
                let collected = stream.try_collect::<Vec<_>>().await?;
                assert_eq!(vec![b"st".to_vec()], collected);

                let stream = household_a.read(
                    vec![b.clone()],
                    test_arena(),
                    model::Path::parse("bar.txt")?,
                    0,
                    Some(2),
                )?;
                let collected = stream.try_collect::<Vec<_>>().await?;
                assert_eq!(vec![b"te".to_vec()], collected);

                Ok::<(), anyhow::Error>(())
            })
            .await?;

        Ok(())
    }
}
