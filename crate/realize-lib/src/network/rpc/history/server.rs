use std::time::Duration;

use futures::StreamExt as _;
use tarpc::{
    context::Context,
    server::{BaseChannel, Channel},
    ClientMessage, Response, Transport,
};
use tokio::sync::mpsc;

use crate::{
    model::{Arena, Peer},
    network::Networking,
    storage::real::Notification,
};

use super::{HistoryService, HistoryServiceRequest, HistoryServiceResponse};

/// Forward history for the given peer to a mpsc channel.
///
/// Connect to the peer, then ask it to send history for the given
/// arenas.
///
/// A connection to the peer is kept up and running as long as
/// possible. When the peer disconnects, attempts will be made to
/// reconnect as long as allowed by `retry_strategy` - forever if
/// necessary.
///
/// Close the channel to stop these reconnection attempts.
pub async fn forward_peer_history<T>(
    networking: Networking,
    peer: Peer,
    arenas: T,
    retry_strategy: impl Iterator<Item = Duration> + Clone,
    tx: mpsc::Sender<(Peer, Notification)>,
) -> anyhow::Result<()>
where
    T: IntoIterator<Item = Arena>,
{
    let arenas: Vec<Arena> = arenas.into_iter().collect();

    loop {
        tokio::select!(
        _ = tx.closed() => {
            return Ok(());
        },
        transport = networking
        .connect_with_retries(&peer, super::TAG, None, retry_strategy.clone(), None) => {
            match transport {
                Err(err) => {
                    return Err(err);
                }
                Ok(transport) => {
                    let _= run_forwarder(
                        peer.clone(),
                        arenas.clone(),
                        BaseChannel::with_defaults(transport),
                        tx.clone(),
                    ).await;
                }
            }
        });
    }
}

/// Forward notifications the given TARPC channel to a mpsc channel.
///
/// The forwarder stops when the given mpsc channel is closed or when
/// there's a RPC error (usually due to a connection error.)
async fn run_forwarder<T>(
    peer: Peer,
    arenas: Vec<Arena>,
    channel: BaseChannel<HistoryServiceRequest, HistoryServiceResponse, T>,
    tx: mpsc::Sender<(Peer, Notification)>,
) -> anyhow::Result<()>
where
    T: Transport<Response<HistoryServiceResponse>, ClientMessage<HistoryServiceRequest>>,
{
    let server = HistoryServer { peer, arenas, tx };
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
}

impl HistoryService for HistoryServer {
    async fn arenas(self, _context: Context) -> Vec<Arena> {
        self.arenas
    }

    async fn notify(self, _context: Context, batch: Vec<Notification>) -> () {
        for n in batch {
            let _ = self.tx.send((self.peer.clone(), n)).await;
        }
    }
}

#[cfg(test)]
mod tests {
    use std::{sync::Arc, time::Duration};

    use assert_fs::{
        prelude::{FileWriteStr as _, PathChild as _},
        TempDir,
    };
    use tokio::time::timeout;
    use tokio_retry::strategy::{ExponentialBackoff, FixedInterval};

    use crate::{
        model,
        network::{
            self,
            hostport::HostPort,
            rpc::history,
            security::{testing, PeerVerifier, RawPublicKeyResolver},
            Server,
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
        let handle = tokio::spawn(forward_peer_history(
            fixture.networking.clone(),
            fixture.peer.clone(),
            vec![fixture.arena.clone()],
            ExponentialBackoff::from_millis(10).take(0),
            tx,
        ));

        let (peer, notif) = timeout(Duration::from_secs(5), rx.recv()).await?.unwrap();
        assert_eq!(fixture.peer, peer);
        assert_eq!(
            Notification::CatchingUp {
                arena: fixture.arena.clone(),
            },
            notif
        );
        let (peer, notif) = timeout(Duration::from_secs(5), rx.recv()).await?.unwrap();
        assert_eq!(fixture.peer, peer);
        assert_eq!(
            Notification::Ready {
                arena: fixture.arena.clone(),
            },
            notif
        );

        // The forwarder is ready; a new file will generate a notification.

        let foobar = fixture.tempdir.child("foobar.txt");
        foobar.write_str("test")?;
        let foobar_mtime = foobar.metadata()?.modified()?;

        let (peer, notif) = timeout(Duration::from_secs(5), rx.recv()).await?.unwrap();
        assert_eq!(fixture.peer, peer);
        assert_eq!(
            Notification::Link {
                arena: fixture.arena.clone(),
                path: model::Path::parse("foobar.txt")?,
                size: 4,
                mtime: foobar_mtime,
            },
            notif
        );

        // Dropping rx stops the forwarder.
        drop(rx);
        timeout(Duration::from_secs(1), handle).await???;

        Ok(())
    }

    #[tokio::test]
    async fn forward_history_catchup() -> anyhow::Result<()> {
        let fixture = Fixture::setup().await?;

        let foobar = fixture.tempdir.child("foobar.txt");
        foobar.write_str("test")?;
        let foobar_mtime = foobar.metadata()?.modified()?;

        let _server = fixture.start_server().await?;

        let (tx, mut rx) = mpsc::channel(10);
        let _ = tokio::spawn(forward_peer_history(
            fixture.networking.clone(),
            fixture.peer.clone(),
            vec![fixture.arena.clone()],
            ExponentialBackoff::from_millis(1).take(0),
            tx,
        ));

        let (peer, notif) = timeout(Duration::from_secs(1), rx.recv()).await?.unwrap();
        assert_eq!(fixture.peer, peer);
        assert_eq!(
            Notification::CatchingUp {
                arena: fixture.arena.clone(),
            },
            notif
        );

        let (peer, notif) = timeout(Duration::from_secs(1), rx.recv()).await?.unwrap();
        assert_eq!(fixture.peer, peer);
        assert_eq!(
            Notification::Catchup {
                arena: fixture.arena.clone(),
                path: model::Path::parse("foobar.txt")?,
                size: 4,
                mtime: foobar_mtime,
            },
            notif
        );

        Ok(())
    }

    #[tokio::test]
    async fn forward_loses_connection() -> anyhow::Result<()> {
        let fixture = Fixture::setup().await?;

        let server = fixture.start_server().await?;

        let (tx, mut rx) = mpsc::channel(10);
        let handle = tokio::spawn(forward_peer_history(
            fixture.networking.clone(),
            fixture.peer.clone(),
            vec![fixture.arena.clone()],
            ExponentialBackoff::from_millis(1).take(0),
            tx,
        ));

        let _ = timeout(Duration::from_secs(1), rx.recv()).await?;

        // The server shutting down stops the forwarder, since retries
        // are turned off. The returned error is a "connection
        // refused" from the single failed reconnection attempt.
        server.shutdown().await?;
        assert!(timeout(Duration::from_secs(1), handle).await??.is_err());

        Ok(())
    }

    #[tokio::test]
    async fn forward_history_fails_to_connect() -> anyhow::Result<()> {
        let fixture = Fixture::setup().await?;

        let (tx, rx) = mpsc::channel(10);
        let ret = forward_peer_history(
            fixture.networking,
            fixture.peer,
            vec![fixture.arena.clone()],
            ExponentialBackoff::from_millis(10).take(0),
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

    #[tokio::test]
    async fn forward_history_reconnects() -> anyhow::Result<()> {
        let fixture = Fixture::setup().await?;

        let (tx, mut rx) = mpsc::channel(10);
        let handle = tokio::spawn(forward_peer_history(
            fixture.networking.clone(),
            fixture.peer.clone(),
            vec![fixture.arena.clone()],
            FixedInterval::from_millis(10),
            tx,
        ));

        for _ in 0..3 {
            tokio::time::sleep(Duration::from_millis(15)).await;
            let server = fixture.start_server().await?;
            assert!(matches!(
                timeout(Duration::from_secs(5), rx.recv()).await?.unwrap(),
                (_, Notification::CatchingUp { .. })
            ));
            assert!(matches!(
                timeout(Duration::from_secs(5), rx.recv()).await?.unwrap(),
                (_, Notification::Ready { .. })
            ));
            server.shutdown().await?;
        }

        // Dropping rx stops the forwarder from trying to reconnect.
        drop(rx);
        timeout(Duration::from_secs(1), handle).await???;

        Ok(())
    }
}
