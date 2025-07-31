use crate::consensus::{tracker::JobInfo, types::ChurtenNotification};
use capnp::capability::Promise;
use realize_network::unixsocket;
use tokio::sync::mpsc;

use super::{
    control_capnp::{
        self,
        churten::subscriber::{NotifyParams, ResetParams},
    },
    convert::{self, parse_job_info},
};

/// Connect to a running daemon through the given socket path.
///
/// Panics if run outside of a [LocalSet].
pub async fn connect(
    socket_path: &std::path::Path,
) -> anyhow::Result<control_capnp::control::Client> {
    unixsocket::connect::<control_capnp::control::Client>(socket_path).await
}

/// Get a Churten client from a Control client
pub async fn get_churten(
    c: &control_capnp::control::Client,
) -> Result<control_capnp::churten::Client, capnp::Error> {
    c.churten_request()
        .send()
        .promise
        .await?
        .get()?
        .get_churten()
}

/// Subscribe to churten and get a stream of [ChurtenUpdates].
pub async fn subscribe_to_churten(
    churten: &control_capnp::churten::Client,
) -> Result<mpsc::Receiver<ChurtenUpdates>, capnp::Error> {
    let (tx, rx) = mpsc::channel(10);
    let mut request = churten.subscribe_request();
    request
        .get()
        .set_subscriber(TxChurtenSubscriber::new(tx).as_client());
    request.send().promise.await?;

    Ok(rx)
}

/// Updates to churten, running in another process.
#[derive(Clone, PartialEq, Debug)]
pub enum ChurtenUpdates {
    /// Report the of active jobs.
    ///
    /// This is the first update sent for every subscriber. Afterwards, [ChurtenNotification]s are
    /// sent as long as the channel isn't backlogged.
    ///
    /// If the channel is full, the process drops notification and send a reset containing all
    /// the currently active jobs. Jobs not on the new list should be considered finished with an unknown status.
    Reset(Vec<JobInfo>),

    /// Change to apply to the set of active jobs.
    ///
    /// [crate::consensus::tracker::JobInfoTracker] might help.
    Notify(ChurtenNotification),
}

/// A Churten subscriber server that forwards Subscriber calls to a
/// channel.
pub struct TxChurtenSubscriber {
    tx: mpsc::Sender<ChurtenUpdates>,
}

impl TxChurtenSubscriber {
    pub fn new(tx: mpsc::Sender<ChurtenUpdates>) -> Self {
        Self { tx }
    }

    pub fn as_client(self) -> control_capnp::churten::subscriber::Client {
        capnp_rpc::new_client(self)
    }
}

impl control_capnp::churten::subscriber::Server for TxChurtenSubscriber {
    fn reset(&mut self, params: ResetParams) -> Promise<(), capnp::Error> {
        let tx = self.tx.clone();
        Promise::from_future(async move {
            let job_list = params.get().and_then(|p| p.get_jobs())?;
            let mut job_vec = Vec::with_capacity(job_list.len() as usize);
            for i in 0..job_list.len() {
                job_vec.push(parse_job_info(job_list.get(i as u32))?);
            }
            tx.send(ChurtenUpdates::Reset(job_vec))
                .await
                .map_err(channel_closed)?;

            Ok(())
        })
    }

    fn notify(&mut self, params: NotifyParams) -> Promise<(), capnp::Error> {
        let tx = self.tx.clone();
        Promise::from_future(async move {
            let reader = params.get().and_then(|p| p.get_notification())?;
            let n = convert::parse_notification(reader)?;
            tx.send(ChurtenUpdates::Notify(n))
                .await
                .map_err(channel_closed)?;

            Ok(())
        })
    }
}

fn channel_closed<T>(_: mpsc::error::SendError<T>) -> capnp::Error {
    capnp::Error::failed("channel closed".to_string())
}
