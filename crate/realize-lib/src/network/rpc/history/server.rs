use std::sync::{
    atomic::{AtomicBool, Ordering},
    Arc,
};

use futures::StreamExt as _;
use tarpc::{
    context::Context,
    server::{BaseChannel, Channel},
    ClientMessage, Response, Transport,
};
use tokio::sync::{mpsc, oneshot};

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
    mut ready_tx: Option<oneshot::Sender<()>>,
) -> anyhow::Result<()>
where
    T: Transport<Response<HistoryServiceResponse>, ClientMessage<HistoryServiceRequest>>,
{
    let server = HistoryServer::new(peer, arenas, tx);
    let mut requests = Box::pin(channel.requests());
    loop {
        tokio::select!(@{
    start = {
        tokio::macros::support::thread_rng_n(BRANCHES)
    };
    ()
}_ = server.tx.closed() => {
    break;
}next = requests.next() => {
    match next {
        None => {
            break;
        }Some(req) => {
            req?.execute(server.clone().serve()).await;
            if ready_tx.is_some()&&server.ready.load(Ordering::Relaxed){
                if let Some(ready_tx) = ready_tx.take(){
                    let _ = ready_tx.send(());
                }
            }
        }
    }
},);
    }

    Ok(())
}

#[derive(Clone)]
struct HistoryServer {
    peer: Peer,
    arenas: Vec<Arena>,
    tx: mpsc::Sender<(Peer, Notification)>,
    ready: Arc<AtomicBool>,
}

impl HistoryServer {
    fn new(peer: Peer, arenas: Vec<Arena>, tx: mpsc::Sender<(Peer, Notification)>) -> Self {
        Self {
            peer,
            arenas,
            tx,
            ready: Arc::new(AtomicBool::new(false)),
        }
    }
}

impl HistoryService for HistoryServer {
    async fn arenas(self, _context: Context) -> Vec<Arena> {
        self.arenas
    }

    async fn ready(self, _context: Context) -> () {
        log::debug!("READY");
        self.ready.store(true, Ordering::Relaxed);
    }

    async fn notify(self, _context: Context, batch: Vec<Notification>) -> () {
        for n in batch {
            let _ = self.tx.send((self.peer.clone(), n)).await;
        }
    }
}
