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
    for res in
        futures::future::join_all(arenas.into_iter().map(|a| storage.subscribe(a, tx.clone())))
            .await
    {
        res?
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
