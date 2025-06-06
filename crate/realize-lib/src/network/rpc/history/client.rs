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
    let (tx, mut rx) = mpsc::channel(100);
    for arena in client.arenas(context::current()).await? {
        storage.subscribe(&arena, tx.clone()).await?;
    }
    drop(tx);

    // TODO:wait for all available arenas to be ready before returning, so
    // the caller knows that notifications are ready and that it can query
    // the full list, if it needs it.

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
