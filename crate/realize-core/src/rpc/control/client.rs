#![allow(refining_impl_trait)]
use crate::consensus::{tracker::JobInfo, types::TransferNotification};
use capnp::capability::Promise;
use realize_network::unixsocket;
use std::rc::Rc;
use tokio::sync::mpsc;

use super::{
    control_capnp::{
        self,
        transfer::subscriber::{NotifyParams, ResetParams},
    },
    convert::{self, parse_job_info},
};

/// Connect to a running daemon through the given socket path.
///
/// Panics if run outside of a [LocalSet].
pub async fn connect(
    socket_path: &std::path::Path,
) -> anyhow::Result<control_capnp::control::Client> {
    unixsocket::connect(socket_path).await
}

/// Get a Transfer client from a Control client
pub async fn get_transfer(
    c: &control_capnp::control::Client,
) -> Result<control_capnp::transfer::Client, capnp::Error> {
    c.transfer_request()
        .send()
        .promise
        .await?
        .get()?
        .get_transfer()
}

/// Subscribe to transfer and get a stream of [TransferUpdates].
pub async fn subscribe_to_transfer(
    transfer: &control_capnp::transfer::Client,
) -> Result<mpsc::Receiver<TransferUpdates>, capnp::Error> {
    let (tx, rx) = mpsc::channel(10);
    let mut request = transfer.subscribe_request();
    request
        .get()
        .set_subscriber(TxTransferSubscriber::new(tx).as_client());
    request.send().promise.await?;

    Ok(rx)
}

/// Subscribe to transfer and get a stream of [TransferUpdates].
pub async fn all_jobs(
    transfer: &control_capnp::transfer::Client,
) -> Result<Vec<JobInfo>, capnp::Error> {
    let res = transfer.all_jobs_request().send().promise.await?;
    let job_list = res.get()?.get_jobs()?;
    let mut job_vec = Vec::with_capacity(job_list.len() as usize);
    for i in 0..job_list.len() {
        job_vec.push(parse_job_info(job_list.get(i as u32))?);
    }

    Ok(job_vec)
}

/// Updates to transfer, running in another process.
#[derive(Clone, PartialEq, Debug)]
pub enum TransferUpdates {
    /// Report the of active jobs.
    ///
    /// This is the first update sent for every subscriber. Afterwards, [TransferNotification]s are
    /// sent as long as the channel isn't backlogged.
    ///
    /// If the channel is full, the process drops notification and send a reset containing all
    /// the currently active jobs. Jobs not on the new list should be considered finished with an unknown status.
    Reset(Vec<JobInfo>),

    /// Change to apply to the set of active jobs.
    ///
    /// [crate::consensus::tracker::JobInfoTracker] might help.
    Notify(TransferNotification),
}

/// A Transfer subscriber server that forwards Subscriber calls to a
/// channel.
pub struct TxTransferSubscriber {
    tx: mpsc::Sender<TransferUpdates>,
}

impl TxTransferSubscriber {
    pub fn new(tx: mpsc::Sender<TransferUpdates>) -> Self {
        Self { tx }
    }

    pub fn as_client(self) -> control_capnp::transfer::subscriber::Client {
        capnp_rpc::new_client(self)
    }
}

impl control_capnp::transfer::subscriber::Server for TxTransferSubscriber {
    fn reset(self: Rc<Self>, params: ResetParams) -> Promise<(), capnp::Error> {
        let tx = self.tx.clone();
        Promise::from_future(async move {
            let job_list = params.get().and_then(|p| p.get_jobs())?;
            let mut job_vec = Vec::with_capacity(job_list.len() as usize);
            for i in 0..job_list.len() {
                job_vec.push(parse_job_info(job_list.get(i as u32))?);
            }
            tx.send(TransferUpdates::Reset(job_vec))
                .await
                .map_err(channel_closed)?;

            Ok(())
        })
    }

    fn notify(self: Rc<Self>, params: NotifyParams) -> Promise<(), capnp::Error> {
        let tx = self.tx.clone();
        Promise::from_future(async move {
            let reader = params.get().and_then(|p| p.get_notification())?;
            let n = convert::parse_notification(reader)?;
            tx.send(TransferUpdates::Notify(n))
                .await
                .map_err(channel_closed)?;

            Ok(())
        })
    }
}

fn channel_closed<T>(_: mpsc::error::SendError<T>) -> capnp::Error {
    capnp::Error::failed("channel closed".to_string())
}
