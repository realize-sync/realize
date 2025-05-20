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

impl<T, F: (AsyncFn() -> anyhow::Result<T>) + Clone> Connect<T> for F {
    async fn run(&self) -> anyhow::Result<T> {
        self().await
    }
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

#[cfg(test)]
mod tests {
    use std::{
        collections::HashSet,
        sync::atomic::{AtomicU32, Ordering},
        time::Duration,
    };

    use futures::future::join_all;
    use tarpc::{
        ChannelError,
        context::{self, Context},
    };
    use tokio_retry::strategy::ExponentialBackoff;

    use super::*;

    #[derive(Clone, Debug)]
    struct FakeError {}

    impl std::error::Error for FakeError {}
    impl std::fmt::Display for FakeError {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            f.write_str("fake")
        }
    }

    #[derive(Clone)]
    struct TestStub {
        max_count: u32,
        counter: Arc<AtomicU32>,
    }

    impl TestStub {
        fn new(max_count: u32) -> TestStub {
            Self {
                max_count,
                counter: Arc::new(AtomicU32::new(0)),
            }
        }
    }

    impl Stub for TestStub {
        type Req = Arc<String>;
        type Resp = String;

        async fn call(
            &self,
            _ctx: Context,
            req: Arc<String>,
        ) -> std::result::Result<String, RpcError> {
            let count = self.counter.fetch_add(1, Ordering::Relaxed);
            if count == self.max_count {
                return Err(RpcError::Channel(ChannelError::Write(Arc::new(
                    FakeError {},
                ))));
            }
            if count > self.max_count {
                return Err(RpcError::Shutdown);
            }
            if *req == "ping" {
                Ok("pong".to_string())
            } else {
                Ok("what?".to_string())
            }
        }
    }

    #[derive(Clone)]
    struct TestStubConnect {
        counter: Arc<AtomicU32>,
        fail: HashSet<u32>,
    }
    impl TestStubConnect {
        fn new(counter: Arc<AtomicU32>) -> TestStubConnect {
            Self {
                counter,
                fail: HashSet::new(),
            }
        }

        fn add_failure(&mut self, call_count: u32) {
            self.fail.insert(call_count);
        }
    }
    impl Connect<TestStub> for TestStubConnect {
        async fn run(&self) -> anyhow::Result<TestStub> {
            let count = self.counter.fetch_add(1, Ordering::Relaxed);

            if self.fail.contains(&count) {
                return Err(anyhow::anyhow!("connection {count} failed"));
            }
            Ok(TestStub::new(2))
        }
    }

    #[tokio::test]
    async fn reconnect() -> anyhow::Result<()> {
        let strategy = ExponentialBackoff::from_millis(1);
        let connection_count = Arc::new(AtomicU32::new(0));
        let reconnect = Reconnect::new(
            strategy,
            TestStubConnect::new(Arc::clone(&connection_count)),
        )
        .await?;

        for _ in 0..10 {
            assert_eq!(
                "pong",
                reconnect
                    .call(context::current(), "ping".to_string())
                    .await?
            );
        }

        assert_eq!(5, connection_count.load(Ordering::Relaxed));

        Ok(())
    }

    #[tokio::test]
    async fn reconnect_parallel() -> anyhow::Result<()> {
        let strategy = ExponentialBackoff::from_millis(1);
        let connection_count = Arc::new(AtomicU32::new(0));
        let reconnect = Reconnect::new(
            strategy,
            TestStubConnect::new(Arc::clone(&connection_count)),
        )
        .await?;

        // Warm up the stub
        assert_eq!(
            "pong",
            reconnect
                .call(context::current(), "ping".to_string())
                .await?
        );

        // Execute requests in parallel. All must (eventually) go through.
        for res in
            join_all((0..19).map(|_| reconnect.call(context::current(), "ping".to_string()))).await
        {
            assert_eq!("pong".to_string(), res?);
        }

        assert_eq!(10, connection_count.load(Ordering::Relaxed));

        Ok(())
    }

    #[tokio::test]
    async fn reconnect_sometimes_fail() -> anyhow::Result<()> {
        let strategy = ExponentialBackoff::from_millis(1);
        let connection_count = Arc::new(AtomicU32::new(0));

        // Connecting fails 4 times, including 3 consecutive failures
        let mut connect = TestStubConnect::new(Arc::clone(&connection_count));
        connect.add_failure(1);
        connect.add_failure(3);
        connect.add_failure(4);
        connect.add_failure(5);
        let reconnect = Reconnect::new(strategy, connect).await?;

        for _ in 0..10 {
            assert_eq!(
                "pong",
                reconnect
                    .call(context::current(), "ping".to_string())
                    .await?
            );
        }

        // 5 successes + 4 failures
        assert_eq!(9, connection_count.load(Ordering::Relaxed));

        Ok(())
    }

    #[tokio::test]
    async fn reconnect_gives_up() -> anyhow::Result<()> {
        let strategy = vec![Duration::from_millis(1), Duration::from_millis(1)].into_iter();
        let connection_count = Arc::new(AtomicU32::new(0));

        // Connecting fails 4 times, including 3 consecutive failures, while the strategy only
        // allows for 2 consecutive failures.
        let mut connect = TestStubConnect::new(Arc::clone(&connection_count));
        connect.add_failure(1);
        connect.add_failure(3);
        connect.add_failure(4);
        connect.add_failure(5);
        let reconnect = Reconnect::new(strategy, connect).await?;

        // The first 4 calls work, with one successful reconnection.
        for _ in 0..4 {
            assert_eq!(
                "pong",
                reconnect
                    .call(context::current(), "ping".to_string())
                    .await?
            );
        }
        // initial connection + 1 reconnection + 1 retried connection
        // failure
        assert_eq!(3, connection_count.load(Ordering::Relaxed));

        // After that reconnecting fails and eventually reconnect
        // gives up.
        assert!(matches!(
            reconnect.call(context::current(), "ping".to_string()).await,
            Err(RpcError::Shutdown),
        ));

        // 2 more than last time: tried to reconnect twice and then gave up.
        assert_eq!(5, connection_count.load(Ordering::Relaxed));

        // no more reconnection attempts
        for _ in 0..3 {
            assert!(matches!(
                reconnect.call(context::current(), "ping".to_string()).await,
                Err(RpcError::Shutdown),
            ));
        }
        assert_eq!(5, connection_count.load(Ordering::Relaxed));

        Ok(())
    }
}
