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
        if tracing::Span::current().is_disabled() {
            ctx.deadline = Instant::now() + self.deadline;
        };

        self.inner.call(ctx, req).await
    }
}
