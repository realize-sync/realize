use std::{
    sync::Arc,
    time::{Duration, Instant},
};

use tarpc::{
    RequestName,
    client::{RpcError, stub::Stub},
    context::Context,
};
use tokio::sync::RwLock;

/// Maximum number of retries for DeadlineExceeded
const MAX_RETRY_COUNT: u32 = 10;

#[allow(async_fn_in_trait)] // TODO: fix
pub(crate) trait Connect<T>: Clone {
    async fn run(&self) -> anyhow::Result<T>;
}

impl<T, F: (AsyncFn() -> anyhow::Result<T>) + Clone> Connect<T> for F {
    async fn run(&self) -> anyhow::Result<T> {
        self().await
    }
}

#[derive(Clone)]
pub(crate) struct Reconnect<T, C: Connect<T>, S> {
    inner: Arc<RwLock<Reconnectable<T>>>,
    connect: C,
    strategy: S,
    short_deadline: Option<Duration>,
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
    pub(crate) async fn new(
        strategy: S,
        connect: C,
        short_deadline: Option<Duration>,
    ) -> anyhow::Result<Self> {
        let inner = connect.run().await?;

        Ok(Self {
            inner: Arc::new(RwLock::new(Reconnectable::Connected {
                inner,
                connection_count: 1,
                confirmed: false,
            })),
            connect,
            strategy,
            short_deadline,
        })
    }

    /// Reconnect and increment the connection count.
    ///
    /// How long to wait and how many times to try is controlled by the
    /// strategy.
    async fn reconnect(
        &self,
        deadline: &Instant,
        connection_count: u64,
    ) -> Result<Reconnectable<T>, RpcError> {
        let strategy = self.strategy.clone();
        for duration in strategy {
            let sleep_end = Instant::now() + duration;
            if sleep_end >= *deadline {
                return Err(RpcError::DeadlineExceeded);
            }

            tokio::time::sleep(duration).await;
            if let Ok(new_stub) = self.connect.run().await {
                return Ok(Reconnectable::Connected {
                    inner: new_stub,
                    connection_count: connection_count + 1,
                    confirmed: true,
                });
            }
        }

        Ok(Reconnectable::GiveUp)
    }

    /// Create a context with a possibly shorter deadline.
    fn short_ctx(&self, ctx: &Context) -> Context {
        let mut short_ctx = *ctx;
        if let Some(duration) = self.short_deadline {
            let short_deadline = Instant::now() + duration;
            if short_deadline < ctx.deadline {
                short_ctx.deadline = short_deadline;
            }
        }

        short_ctx
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
        ctx: Context,
        request: Self::Req,
    ) -> std::result::Result<Self::Resp, RpcError> {
        let mut retry_count = 0;
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
                    let resp = inner.call(self.short_ctx(&ctx), Arc::clone(&request)).await;
                    if should_retry(&resp, &ctx.deadline, &mut retry_count) {
                        continue;
                    }
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
                    let resp = inner.call(self.short_ctx(&ctx), Arc::clone(&request)).await;
                    if should_retry(&resp, &ctx.deadline, &mut retry_count) {
                        continue;
                    }
                    if !should_reconnect(&resp) {
                        return resp;
                    }

                    // Reconnect, but first take a write lock and
                    // check whether the Reconnectable has been
                    // changed so that only one control flow actually
                    // reconnects, while others just wait.
                    let idx = *connection_count;
                    retry_count = 0;
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
                            *guard = self.reconnect(&ctx.deadline, idx).await?;
                        }
                    }
                }
            }
        }
    }
}

/// Check whether the request should be retried.
///
/// Request that fail with DeadlineExceeded are retried once if
/// there's enough time in the overall deadline.
fn should_retry<T>(
    result: &Result<T, RpcError>,
    overall_deadline: &Instant,
    retry_count: &mut u32,
) -> bool {
    if let Err(RpcError::DeadlineExceeded) = result {
        if Instant::now() < *overall_deadline {
            *retry_count += 1;
            if *retry_count > MAX_RETRY_COUNT {
                return false;
            }

            return true;
        }
    }

    false
}

fn should_reconnect<T>(result: &Result<T, RpcError>) -> bool {
    matches!(result, Err(RpcError::Shutdown) | Err(RpcError::Channel(_)))
}

#[cfg(test)]
mod tests {
    use std::{
        collections::HashSet,
        result::Result,
        sync::atomic::{AtomicU32, Ordering},
        time::Duration,
    };

    use anyhow::Context as _;
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
    struct LimitedCallsStub {
        max_count: u32,
        counter: Arc<AtomicU32>,
    }

    impl LimitedCallsStub {
        fn new(max_count: u32) -> LimitedCallsStub {
            Self {
                max_count,
                counter: Arc::new(AtomicU32::new(0)),
            }
        }
    }

    impl Stub for LimitedCallsStub {
        type Req = Arc<String>;
        type Resp = String;

        async fn call(&self, _ctx: Context, req: Arc<String>) -> Result<String, RpcError> {
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
    impl Connect<LimitedCallsStub> for TestStubConnect {
        async fn run(&self) -> anyhow::Result<LimitedCallsStub> {
            let count = self.counter.fetch_add(1, Ordering::Relaxed);

            if self.fail.contains(&count) {
                return Err(anyhow::anyhow!("connection {count} failed"));
            }
            Ok(LimitedCallsStub::new(2))
        }
    }

    #[tokio::test]
    async fn reconnect() -> anyhow::Result<()> {
        let strategy = ExponentialBackoff::from_millis(1);
        let connection_count = Arc::new(AtomicU32::new(0));
        let reconnect = Reconnect::new(
            strategy,
            TestStubConnect::new(Arc::clone(&connection_count)),
            None,
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
            None,
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
        let reconnect = Reconnect::new(strategy, connect, None).await?;

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
        let reconnect = Reconnect::new(strategy, connect, None).await?;

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

    #[derive(Clone)]
    pub(crate) struct DeadlineStub {
        counter: Arc<AtomicU32>,
        fail: HashSet<u32>,
    }

    impl Stub for DeadlineStub {
        type Req = Arc<String>;
        type Resp = Duration;

        async fn call(&self, ctx: Context, _req: Arc<String>) -> Result<Duration, RpcError> {
            let count = self.counter.fetch_add(1, Ordering::Relaxed);
            if self.fail.contains(&count) {
                return Err(RpcError::DeadlineExceeded);
            }
            Ok(ctx.deadline.duration_since(Instant::now()))
        }
    }

    #[derive(Clone)]
    struct DeadlineStubConnect {
        counter: Arc<AtomicU32>,
        fail: HashSet<u32>,
    }
    impl DeadlineStubConnect {
        fn new() -> DeadlineStubConnect {
            Self {
                counter: Arc::new(AtomicU32::new(0)),
                fail: HashSet::new(),
            }
        }

        fn add_fail(&mut self, count: u32) {
            self.fail.insert(count);
        }

        fn counter(&self) -> Arc<AtomicU32> {
            Arc::clone(&self.counter)
        }
    }
    impl Connect<DeadlineStub> for DeadlineStubConnect {
        async fn run(&self) -> anyhow::Result<DeadlineStub> {
            Ok(DeadlineStub {
                counter: Arc::clone(&self.counter),
                fail: self.fail.clone(),
            })
        }
    }

    #[tokio::test]
    async fn reconnect_set_short_deadline() -> anyhow::Result<()> {
        let reconnect = Reconnect::new(
            ExponentialBackoff::from_millis(1),
            DeadlineStubConnect::new(),
            Some(Duration::from_secs(3)),
        )
        .await?;

        let mut ctx = context::current();
        ctx.deadline = Instant::now() + Duration::from_secs(60);

        for _ in 0..3 {
            let remaining = reconnect.call(ctx, "test".to_string()).await?;
            assert!(
                remaining.as_secs() > 0 && remaining.as_secs() <= 3,
                "remaining={remaining:?}"
            );
        }

        Ok(())
    }

    #[tokio::test]
    async fn reconnect_dont_set_short_deadline_if_shorter_than_overall() -> anyhow::Result<()> {
        let reconnect = Reconnect::new(
            ExponentialBackoff::from_millis(1),
            DeadlineStubConnect::new(),
            Some(Duration::from_secs(30)),
        )
        .await?;

        let mut ctx = context::current();
        ctx.deadline = Instant::now() + Duration::from_secs(10);

        let remaining = reconnect.call(ctx, "test".to_string()).await?;
        assert!(
            remaining.as_secs() > 0 && remaining.as_secs() <= 10,
            "remaining={remaining:?}"
        );

        Ok(())
    }

    #[tokio::test]
    async fn reconnect_retry_deadline_exceeded() -> anyhow::Result<()> {
        let mut connect = DeadlineStubConnect::new();
        let call_counter = connect.counter();
        connect.add_fail(1);
        for i in 3..(3 + MAX_RETRY_COUNT + 1) {
            connect.add_fail(i);
        }
        let reconnect = Reconnect::new(
            ExponentialBackoff::from_millis(1),
            connect,
            Some(Duration::from_secs(10)),
        )
        .await?;

        let mut ctx = context::current();
        ctx.deadline = Instant::now() + Duration::from_secs(60);

        // 1st call succeeds without retries
        reconnect
            .call(ctx, "test".to_string())
            .await
            .context("1st call")?;
        assert_eq!(1, call_counter.load(Ordering::Relaxed));

        // 2nd call is retried once and eventually succeeds (2 calls)
        reconnect
            .call(ctx, "test".to_string())
            .await
            .context("2nd call")?;
        assert_eq!(3, call_counter.load(Ordering::Relaxed));

        // 3rd call returns DeadlineExceeded MAX_RETRY_COUNT+1 times,
        // which is forwarded.
        assert!(matches!(
            reconnect.call(ctx, "test".to_string()).await,
            Err(RpcError::DeadlineExceeded)
        ));
        assert_eq!(
            3 + MAX_RETRY_COUNT + 1,
            call_counter.load(Ordering::Relaxed)
        );

        // 4th call succeeds
        reconnect
            .call(ctx, "test".to_string())
            .await
            .context("4th call")?;
        assert_eq!(
            3 + MAX_RETRY_COUNT + 2,
            call_counter.load(Ordering::Relaxed)
        );

        Ok(())
    }

    #[tokio::test]
    async fn reconnect_stop_retrying_at_end() -> anyhow::Result<()> {
        let mut connect = DeadlineStubConnect::new();
        for i in 0..10000 {
            connect.add_fail(i);
        }
        let reconnect = Reconnect::new(
            ExponentialBackoff::from_millis(1),
            connect,
            Some(Duration::from_millis(1)),
        )
        .await?;

        let mut ctx = context::current();
        ctx.deadline = Instant::now() + Duration::from_millis(3);

        assert!(matches!(
            reconnect.call(ctx, "test".to_string()).await,
            Err(RpcError::DeadlineExceeded)
        ),);

        Ok(())
    }

    #[tokio::test]
    async fn reconnect_stop_reconnecting_at_deadline() -> anyhow::Result<()> {
        let strategy = ExponentialBackoff::from_millis(1);
        let mut connect = TestStubConnect::new(Arc::new(AtomicU32::new(0)));
        for i in 1..10 {
            connect.add_failure(i);
        }
        let reconnect = Reconnect::new(strategy, connect, None).await?;

        // Use up the first stub
        for _ in 0..2 {
            assert_eq!(
                "pong",
                reconnect
                    .call(context::current(), "ping".to_string())
                    .await?
            );
        }

        // Stub stops reconnecting after hitting deadline. Deadline
        // only allows 3 attempts.
        let mut ctx = context::current();
        ctx.deadline = Instant::now() + Duration::from_millis(3);
        let res = reconnect.call(ctx, "ping".to_string()).await;
        assert!(matches!(res, Err(RpcError::DeadlineExceeded)), "{res:?}");

        // Stub works again given longer deadline
        assert_eq!(
            "pong",
            reconnect
                .call(context::current(), "ping".to_string())
                .await?
        );

        Ok(())
    }
}
