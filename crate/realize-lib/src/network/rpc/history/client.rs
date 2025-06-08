use tarpc::client::stub::Stub;
use tarpc::context;
use tokio::sync::{broadcast, mpsc};

use crate::network::rpc::history::HistoryServiceClient;
use crate::storage::real::LocalStorage;

use super::{HistoryServiceRequest, HistoryServiceResponse};

pub(crate) async fn collect<T>(
    client: HistoryServiceClient<T>,
    storage: LocalStorage,
    mut shutdown: broadcast::Receiver<()>,
) -> anyhow::Result<()>
where
    T: Stub<Req = HistoryServiceRequest, Resp = HistoryServiceResponse> + Clone,
{
    let arenas = client.arenas(context::current()).await?;

    let (tx, mut rx) = mpsc::channel(100);
    let res = futures::future::join_all(
        arenas
            .iter()
            .map(|a| storage.subscribe(a.clone(), tx.clone())),
    )
    .await;

    let mut watched_arenas = vec![];
    for (arena, watched) in arenas.into_iter().zip(res.into_iter()) {
        if watched? {
            watched_arenas.push(arena);
        }
    }
    drop(tx);

    client.ready(context::current(), watched_arenas).await?;

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
