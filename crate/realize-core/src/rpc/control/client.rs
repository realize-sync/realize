use capnp::capability::Promise;
use realize_network::unixsocket;
use tokio::sync::mpsc;

use crate::consensus::types::ChurtenNotification;

use super::{control_capnp, convert};

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

/// A Churten subscriber server that forwards notifications to a
/// channel.
pub struct TxChurtenSubscriber {
    tx: mpsc::Sender<ChurtenNotification>,
}

impl TxChurtenSubscriber {
    pub fn new(tx: mpsc::Sender<ChurtenNotification>) -> Self {
        Self { tx }
    }

    pub fn as_client(self) -> control_capnp::churten::subscriber::Client {
        capnp_rpc::new_client(self)
    }
}

impl control_capnp::churten::subscriber::Server for TxChurtenSubscriber {
    fn notify(
        &mut self,
        params: control_capnp::churten::subscriber::NotifyParams,
    ) -> Promise<(), capnp::Error> {
        let tx = self.tx.clone();
        Promise::from_future(async move {
            let reader = params.get().and_then(|p| p.get_notification())?;
            let n = convert::parse_notification(reader)?;
            tx.send(n).await.map_err(channel_closed)?;

            Ok(())
        })
    }
}

fn channel_closed<T>(_: mpsc::error::SendError<T>) -> capnp::Error {
    capnp::Error::failed("channel closed".to_string())
}
