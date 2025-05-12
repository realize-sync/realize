use std::time::{Duration, Instant};

use tarpc::{
    client::{stub::Stub, RpcError},
    context::Context,
};

use crate::model::service::{RealizeServiceRequest, RealizeServiceResponse};

/// Stub that sets default deadlines based on method type.
#[derive(Clone)]
pub struct DeadlineSetter<T>
where
    T: Stub<Req = RealizeServiceRequest, Resp = RealizeServiceResponse> + Clone,
{
    inner: T,
}

impl<T: Stub<Req = RealizeServiceRequest, Resp = RealizeServiceResponse> + Clone>
    DeadlineSetter<T>
{
    pub fn new(stub: T) -> Self {
        Self { inner: stub }
    }
}

impl<T: Stub<Req = RealizeServiceRequest, Resp = RealizeServiceResponse> + Clone> Stub
    for DeadlineSetter<T>
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
                    ctx.deadline = Instant::now() + Duration::from_secs(5 * 60);
                }
                RealizeServiceRequest::List { .. }
                | RealizeServiceRequest::Send { .. }
                | RealizeServiceRequest::Read { .. }
                | RealizeServiceRequest::Finish { .. }
                | RealizeServiceRequest::Delete { .. }
                | RealizeServiceRequest::CalculateSignature { .. }
                | RealizeServiceRequest::Diff { .. }
                | RealizeServiceRequest::ApplyDelta { .. } => {}
            }
        };

        self.inner.call(ctx, req).await
    }
}
