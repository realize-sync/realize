use std::time::{Duration, Instant};

use tarpc::{
    client::{stub::Stub, RpcError},
    context::Context,
};

/// Stub that sets default deadlines based on method type.
#[derive(Clone)]
pub struct WithDeadline<T>
where
    T: Stub + Clone,
{
    inner: T,
    deadline: Duration,
}

impl<T: Stub + Clone> WithDeadline<T> {
    pub fn new(inner: T, deadline: Duration) -> Self {
        Self { inner, deadline }
    }
}

impl<T: Stub + Clone> Stub for WithDeadline<T> {
    type Req = T::Req;
    type Resp = T::Resp;

    async fn call(&self, mut ctx: Context, req: Self::Req) -> Result<Self::Resp, RpcError> {
        ctx.deadline = Instant::now() + self.deadline;
        self.inner.call(ctx, req).await
    }
}

#[cfg(test)]
mod tests {
    use tarpc::context;

    use super::*;

    #[derive(Clone)]
    pub struct TestStub {}

    impl Stub for TestStub {
        type Req = String;
        type Resp = Duration;

        async fn call(
            &self,
            ctx: Context,
            _req: String,
        ) -> std::result::Result<Duration, tarpc::client::RpcError> {
            Ok(ctx.deadline.duration_since(Instant::now()))
        }
    }

    #[tokio::test]
    async fn set_deadline() -> anyhow::Result<()> {
        let with_60s = WithDeadline::new(TestStub {}, Duration::from_secs(60));

        let remaining = with_60s
            .call(context::current(), "test".to_string())
            .await?;
        assert!(remaining.as_secs() > 10); // 10 is the hardcoded default for context::current()
        assert!(remaining.as_secs() <= 60);

        Ok(())
    }
}
