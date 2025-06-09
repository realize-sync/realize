use std::{sync::Arc, time::Instant};

use futures::StreamExt as _;
use tarpc::{
    context::Context,
    server::{BaseChannel, Channel},
    ClientMessage, Response, Transport,
};
use tokio::{
    sync::{mpsc, oneshot, Mutex},
    task::JoinHandle,
    time::timeout,
};

use crate::{
    model::{Arena, Peer},
    network::tcp::Networking,
    storage::real::Notification,
    utils::async_utils::AbortOnDrop,
};

use super::{HistoryService, HistoryServiceRequest, HistoryServiceResponse};

/// Forward history for the given peer to a mpsc channel.
///
/// Connect to the peer, then ask it to send history for the given
/// arenas.
///
/// When this function returns successfully, the connection to the
/// peer has succeeded and the peer has declared itself ready to
/// report any file changes. This might take a while.
///
/// After a successful connection, the forwarder runs until either the
/// channel is closed or the connection is lost. Check the
/// [JoinHandle].
pub async fn forward_peer_history<T>(
    networking: &Networking,
    ctx: Context,
    peer: &Peer,
    arenas: T,
    tx: mpsc::Sender<(Peer, Notification)>,
) -> anyhow::Result<(Vec<Arena>, JoinHandle<anyhow::Result<()>>)>
where
    T: IntoIterator<Item = Arena>,
{
    let transport = networking.connect(&peer, super::TAG, None).await?;

    let (ready_tx, ready_rx) = oneshot::channel();
    let peer = peer.clone();
    let arenas = arenas.into_iter().collect();
    let handle = AbortOnDrop::new(tokio::spawn(run_forwarder(
        peer,
        arenas,
        BaseChannel::with_defaults(transport),
        tx,
        Some(ready_tx),
    )));

    // Wait for the peer to declare itself ready before returning.
    // This might take a while.
    //
    // If the context deadline is reached, abort the forwarder and
    // return a deadline error.
    match timeout(ctx.deadline.duration_since(Instant::now()), ready_rx).await? {
        Err(oneshot::error::RecvError { .. }) => {
            // The forwarder must have already died if the oneshot was
            // killed before anything was sent. Attempt to recover the original error.
            handle.join().await??;

            // The following should never be reached:
            anyhow::bail!("Connection failed");
        }
        Ok(watched) => {
            return Ok((watched, handle.as_handle()));
        }
    }
}

/// Forward notifications the given TARPC channel to a mpsc channel.
///
/// To know when the remote end declared itself ready, pass a oneshot
/// channel and await it.
///
/// The forwarder stops when the given mpsc channel is closed or when
/// there's a RPC error (usually due to a connection error.)
async fn run_forwarder<T>(
    peer: Peer,
    arenas: Vec<Arena>,
    channel: BaseChannel<HistoryServiceRequest, HistoryServiceResponse, T>,
    tx: mpsc::Sender<(Peer, Notification)>,
    ready_tx: Option<oneshot::Sender<Vec<Arena>>>,
) -> anyhow::Result<()>
where
    T: Transport<Response<HistoryServiceResponse>, ClientMessage<HistoryServiceRequest>>,
{
    let server = HistoryServer {
        peer,
        arenas,
        tx,
        watched: Arc::new(Mutex::new(ready_tx)),
    };
    let mut requests = Box::pin(channel.requests());
    loop {
        tokio::select!(
            _ = server.tx.closed() => {
                break;
            }
            next = requests.next() => {
                match next {
                    None => {
                        break;
                    },
                    Some(req) => {
                        req?.execute(server.clone().serve()).await;
                    }
                }
            },
        );
    }

    Ok(())
}

#[derive(Clone)]
struct HistoryServer {
    peer: Peer,
    arenas: Vec<Arena>,
    tx: mpsc::Sender<(Peer, Notification)>,
    watched: Arc<Mutex<Option<oneshot::Sender<Vec<Arena>>>>>,
}

impl HistoryService for HistoryServer {
    async fn arenas(self, _context: Context) -> Vec<Arena> {
        self.arenas
    }

    async fn ready(self, _context: Context, arenas: Vec<Arena>) -> () {
        log::debug!("Arenas watched by {}: {:?}", self.peer, &arenas);
        if let Some(tx) = self.watched.lock().await.take() {
            let _ = tx.send(arenas);
        }
    }

    async fn notify(self, _context: Context, batch: Vec<Notification>) -> () {
        for n in batch {
            let _ = self.tx.send((self.peer.clone(), n)).await;
        }
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use assert_fs::{
        prelude::{FileWriteStr as _, PathChild as _},
        TempDir,
    };
    use tarpc::context;
    use tokio::time::timeout;

    use crate::{
        model,
        network::{
            self,
            hostport::HostPort,
            rpc::history,
            security::{testing, PeerVerifier, RawPublicKeyResolver},
            tcp::Server,
        },
        storage::real::LocalStorage,
    };

    use super::*;

    struct Fixture {
        peer: Peer,
        address: HostPort,
        networking: Networking,
        tempdir: TempDir,
        storage: LocalStorage,
        arena: Arena,
    }
    impl Fixture {
        async fn setup() -> anyhow::Result<Self> {
            let _ = env_logger::try_init();

            let port = portpicker::pick_unused_port().expect("No ports free");
            let address = HostPort::localhost(port);
            let peer = Peer::from("self");
            let mut verifier = PeerVerifier::new();
            verifier.add_peer(&peer, testing::client_public_key());
            let verifier = Arc::new(verifier);
            let networking = Networking::new(
                vec![(&peer, address.to_string().as_ref())],
                RawPublicKeyResolver::from_private_key(testing::client_private_key())?,
                verifier,
            );

            let tempdir = TempDir::new()?;
            let arena = Arena::from("testdir");
            let storage = LocalStorage::single(&arena, tempdir.path());

            Ok(Self {
                peer,
                address,
                networking,
                tempdir,
                storage,
                arena,
            })
        }

        async fn start_server(&self) -> anyhow::Result<Arc<Server>> {
            let mut server = Server::new(self.networking.clone());
            history::client::register(&mut server, self.storage.clone());
            let server = Arc::new(server);
            Arc::clone(&server).listen(&self.address).await?;

            Ok(server)
        }
    }

    #[tokio::test]
    async fn forward_history() -> anyhow::Result<()> {
        let fixture = Fixture::setup().await?;

        let _server = fixture.start_server().await?;

        let (tx, mut rx) = mpsc::channel(10);
        let (watched, handle) = forward_peer_history(
            &fixture.networking,
            context::current(),
            &fixture.peer,
            vec![fixture.arena.clone()],
            tx,
        )
        .await?;

        assert_eq!(vec![fixture.arena.clone()], watched);

        // The forwarder is ready; a new file will generate a notification.

        let foobar = fixture.tempdir.child("foobar.txt");
        foobar.write_str("test")?;
        let foobar_mtime = foobar.metadata()?.modified()?;

        let (peer, notif) = timeout(Duration::from_secs(1), rx.recv()).await?.unwrap();
        assert_eq!(fixture.peer, peer);
        assert_eq!(
            Notification::Link {
                arena: fixture.arena,
                path: model::Path::parse("foobar.txt")?,
                size: 4,
                mtime: foobar_mtime
            },
            notif
        );

        // Dropping rx stops the forwarder.
        drop(rx);
        timeout(Duration::from_secs(1), handle).await???;

        Ok(())
    }

    #[tokio::test]
    async fn forward_history_loses_connection() -> anyhow::Result<()> {
        let fixture = Fixture::setup().await?;
        let server = fixture.start_server().await?;

        let (tx, _rx) = mpsc::channel(10);
        let (_, handle) = forward_peer_history(
            &fixture.networking,
            context::current(),
            &fixture.peer,
            vec![fixture.arena.clone()],
            tx,
        )
        .await?;

        server.shutdown().await?;

        // The server shutting down stops the forwarder
        timeout(Duration::from_secs(1), handle).await???;

        Ok(())
    }

    #[tokio::test]
    async fn forward_history_fails_to_connect() -> anyhow::Result<()> {
        let fixture = Fixture::setup().await?;

        let (tx, rx) = mpsc::channel(10);
        let ret = forward_peer_history(
            &fixture.networking,
            context::current(),
            &fixture.peer,
            vec![fixture.arena.clone()],
            tx,
        )
        .await;

        assert_eq!(
            Some(std::io::ErrorKind::ConnectionRefused),
            network::testing::io_error_kind(ret.err())
        );

        assert!(rx.is_closed());

        Ok(())
    }
}
