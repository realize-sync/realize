use std::sync::Arc;

use tarpc::{
    RequestName,
    client::{RpcError, stub::Stub},
    context,
};
use tokio::sync::RwLock;

#[allow(async_fn_in_trait)] // TODO: fix
pub trait Connect<T>: Clone {
    async fn run(&self) -> anyhow::Result<T>;
}

#[derive(Clone)]
pub struct Reconnect<T, C: Connect<T>, S> {
    inner: Arc<RwLock<Reconnectable<T>>>,
    connect: C,
    strategy: S,
}

/// Holds an object that can be reconnected.
enum Reconnectable<T> {
    /// Holds the inner type and the number of connections, including
    /// the initial connection.
    Connected {
        /// Underlying stub.
        inner: T,

        /// Number of connections, including the firt one, so starts at 1.
        connection_count: u64,

        /// Whether at least on RPC went through.
        ///
        /// Unconfirmed streams are not reconnected, as it's likely
        /// the initial connection didn't actually work.
        confirmed: bool,
    },

    /// Signals that Reconnect has given up on reconnecting; all RPC
    /// calls fail with RpcError::Shutdown from now on.
    GiveUp,
}

impl<T, C, S> Reconnect<T, C, S>
where
    C: Connect<T>,
    S: Iterator<Item = std::time::Duration> + Clone,
{
    pub async fn new(strategy: S, connect: C) -> anyhow::Result<Self> {
        let inner = connect.run().await?;

        Ok(Self {
            inner: Arc::new(RwLock::new(Reconnectable::Connected {
                inner,
                connection_count: 1,
                confirmed: false,
            })),
            connect,
            strategy,
        })
    }

    /// Reconnect and increment the connection count.
    ///
    /// How long to wait and how many times to try is controlled by the
    /// strategy.
    async fn reconnect(&self, connection_count: u64) -> Reconnectable<T> {
        let strategy = self.strategy.clone();
        for duration in strategy {
            tokio::time::sleep(duration).await;
            if let Ok(new_stub) = self.connect.run().await {
                return Reconnectable::Connected {
                    inner: new_stub,
                    connection_count: connection_count + 1,
                    confirmed: true,
                };
            }
        }

        Reconnectable::GiveUp
    }
}

impl<T, C, S, Req> Stub for Reconnect<T, C, S>
where
    Req: RequestName,
    T: Stub<Req = Arc<Req>>,
    C: Connect<T>,
    S: Iterator<Item = std::time::Duration> + Clone,
{
    type Req = Req;
    type Resp = T::Resp;

    async fn call(
        &self,
        ctx: context::Context,
        request: Self::Req,
    ) -> std::result::Result<Self::Resp, RpcError> {
        let request = Arc::new(request);
        loop {
            let guard = self.inner.read().await;
            match &(*guard) {
                Reconnectable::GiveUp => {
                    return Err(RpcError::Shutdown);
                }
                Reconnectable::Connected {
                    inner,
                    confirmed: false,
                    ..
                } => {
                    let resp = inner.call(ctx, Arc::clone(&request)).await;
                    if resp.is_ok() {
                        drop(guard);

                        let mut guard = self.inner.write().await;
                        if let Reconnectable::Connected { confirmed, .. } = &mut (*guard) {
                            *confirmed = true;
                        }
                    }
                    return resp;
                }
                Reconnectable::Connected {
                    inner,
                    connection_count,
                    confirmed: true,
                } => {
                    let resp = inner.call(ctx, Arc::clone(&request)).await;

                    if !should_reconnect(&resp) {
                        return resp;
                    }

                    // Reconnect, but first take a write lock and
                    // check whether the Reconnectable has been
                    // changed so that only one control flow actually
                    // reconnects, while others just wait.
                    let idx = *connection_count;
                    drop(guard);

                    let mut guard = self.inner.write().await;
                    if let Reconnectable::Connected {
                        connection_count, ..
                    } = &*guard
                    {
                        if *connection_count == idx {
                            // We're the winner! We get to reconnect
                            // and increment the connection count,
                            // while holding on to the write lock.
                            *guard = self.reconnect(idx).await;
                        }
                    }
                }
            }
        }
    }
}

fn should_reconnect<T>(result: &Result<T, RpcError>) -> bool {
    match result {
        Err(RpcError::Shutdown) => true,
        Err(RpcError::Channel(_)) => true,
        _ => false,
    }
}
