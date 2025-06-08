use std::sync::Arc;

use futures::StreamExt as _;
use tarpc::{
    context::Context,
    server::{BaseChannel, Channel},
    ClientMessage, Response, Transport,
};
use tokio::sync::{mpsc, oneshot, Mutex};

use crate::{
    model::{Arena, Peer},
    storage::real::Notification,
};

use super::{HistoryService, HistoryServiceRequest, HistoryServiceResponse};

/// Forward notifications the given TARPC channel to a mpsc channel.
///
/// To know when the remote end declared itself ready, pass a oneshot
/// channel and await it.
///
/// The forwarder stops when the given mpsc channel is closed or when
/// there's a RPC error (usually due to a connection error.)
pub async fn run_forwarder<T>(
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
