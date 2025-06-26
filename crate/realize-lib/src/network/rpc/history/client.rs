use tarpc::client::stub::Stub;
use tarpc::context;
use tarpc::tokio_serde::formats::Bincode;
use tokio::sync::{broadcast, mpsc};

use crate::model::Peer;
use crate::network::{rpc::history::HistoryServiceClient, Server};
use crate::storage::real::RealStore;

use super::{HistoryServiceRequest, HistoryServiceResponse};

/// Register history collection to the given server.
///
/// With this call, the server answers to HIST calls by reporting file
/// history of the arenas in the local storage.
pub fn register(server: &mut Server, storage: RealStore) {
    server.register(super::TAG, move |peer: Peer, framed, _, shutdown_rx| {
        let transport = tarpc::serde_transport::new(framed, Bincode::default());
        let channel = tarpc::client::new(Default::default(), transport).spawn();
        let client = HistoryServiceClient::from(channel);

        let peer = peer.clone();
        let storage = storage.clone();
        tokio::spawn(async move {
            if let Err(err) = collect(client, storage, shutdown_rx).await {
                log::debug!("{}: history collection failed: {}", peer, err);
            }
        });
    });
}

async fn collect<T>(
    client: HistoryServiceClient<T>,
    storage: RealStore,
    mut shutdown: broadcast::Receiver<()>,
) -> anyhow::Result<()>
where
    T: Stub<Req = HistoryServiceRequest, Resp = HistoryServiceResponse> + Clone,
{
    let arenas = client.arenas(context::current()).await?;

    let (tx, mut rx) = mpsc::channel(100);
    for arena in arenas {
        storage.subscribe(arena, tx.clone(), true)?;
    }
    drop(tx);

    // While there are notifications
    loop {
        let mut notifications = Vec::new();
        tokio::select!(
        count = rx.recv_many(&mut notifications, 25) => {
            if count == 0 {
                break;
            }

            client.notify(context::current(), notifications).await?;

            // Keep waiting for more
            continue;
        },
        _ = shutdown.recv() => {
        });

        break;
    }

    Ok(())
}

// This is tested in module super::server::tests, which puts client
// and server together.
