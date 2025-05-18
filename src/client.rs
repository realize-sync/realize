use std::time::{Duration, Instant};

use tarpc::{
    client::{RpcError, stub::Stub},
    context::Context,
};

use crate::model::service::{RealizeServiceRequest, RealizeServiceResponse};

/// Stub that sets default deadlines based on method type.
#[derive(Clone)]
pub struct WithDeadline<T>
where
    T: Stub<Req = RealizeServiceRequest, Resp = RealizeServiceResponse> + Clone,
{
    inner: T,
    short_deadline: Duration,
    long_deadline: Duration,
}

impl<T: Stub<Req = RealizeServiceRequest, Resp = RealizeServiceResponse> + Clone> WithDeadline<T> {
    pub fn new(inner: T) -> Self {
        Self {
            inner,
            short_deadline: Duration::from_secs(10),
            long_deadline: Duration::from_secs(5 * 60),
        }
    }

    pub fn with_deadline(mut self, deadline: Duration) -> Self {
        self.short_deadline = deadline;
        self.long_deadline = deadline;

        self
    }

    pub fn with_short_deadline(mut self, deadline: Duration) -> Self {
        self.short_deadline = deadline;

        self
    }

    pub fn with_long_deadline(mut self, deadline: Duration) -> Self {
        self.long_deadline = deadline;

        self
    }
}

impl<T: Stub<Req = RealizeServiceRequest, Resp = RealizeServiceResponse> + Clone> Stub
    for WithDeadline<T>
{
    type Req = RealizeServiceRequest;
    type Resp = RealizeServiceResponse;

    async fn call(
        &self,
        mut ctx: Context,
        req: RealizeServiceRequest,
    ) -> Result<RealizeServiceResponse, RpcError> {
        if tracing::Span::current().is_disabled() {
            match req {
                RealizeServiceRequest::Hash { .. } => {
                    ctx.deadline = Instant::now() + self.long_deadline;
                }
                RealizeServiceRequest::List { .. }
                | RealizeServiceRequest::Send { .. }
                | RealizeServiceRequest::Read { .. }
                | RealizeServiceRequest::Finish { .. }
                | RealizeServiceRequest::Delete { .. }
                | RealizeServiceRequest::CalculateSignature { .. }
                | RealizeServiceRequest::Diff { .. }
                | RealizeServiceRequest::ApplyDelta { .. }
                | RealizeServiceRequest::Configure { .. } => {
                    ctx.deadline = Instant::now() + self.short_deadline;
                }
            }
        };

        self.inner.call(ctx, req).await
    }
}
