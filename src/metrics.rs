use hyper::server::conn::http1;
use hyper_util::rt::TokioIo;
use prometheus::Encoder;
use tarpc::{
    ServerError,
    client::{RpcError, stub::Stub},
    context::Context,
    server::Serve,
};
use tokio::net::TcpListener;

use crate::model::service::{RealizeServiceRequest, RealizeServiceResponse};

pub async fn export_metrics(metrics_addr: &str) -> anyhow::Result<()> {
    let listener = TcpListener::bind(metrics_addr).await?;
    log::info!("[metrics] server listening on {}", listener.local_addr()?);
    tokio::spawn(async move {
        loop {
            if let Ok((stream, _)) = listener.accept().await {
                let io = TokioIo::new(stream);
                let _ = http1::Builder::new()
                    .serve_connection(io, hyper::service::service_fn(serve_metrics))
                    .await;
            }
        }
    });

    Ok(())
}

async fn serve_metrics(
    req: hyper::Request<hyper::body::Incoming>,
) -> anyhow::Result<hyper::Response<String>> {
    if req.uri().path() != "/metrics" {
        return Ok(hyper::Response::builder()
            .status(hyper::StatusCode::NOT_FOUND)
            .body("Not found".to_string())?);
    }

    let metrics = prometheus::gather();
    let encoder = prometheus::TextEncoder::new();

    Ok(hyper::Response::builder()
        .status(hyper::StatusCode::OK)
        .header(
            hyper::header::CONTENT_TYPE,
            encoder.format_type().to_string(),
        )
        .body(encoder.encode_to_string(&metrics)?)?)
}

pub async fn push_metrics(
    pushgateway: &str,
    job: &str,
    instance: Option<&str>,
) -> anyhow::Result<()> {
    let mut label_map = prometheus::labels! {};
    if let Some(instance) = instance {
        label_map.insert("instance".to_owned(), instance.to_owned());
    }
    log::debug!(
        "[metrics] push to {}, job={}, instance={:?}",
        pushgateway,
        job,
        instance
    );
    let metric_families = prometheus::gather();
    let pushgateway = pushgateway.to_string();
    let job = job.to_string();
    tokio::task::spawn_blocking(move || {
        prometheus::push_metrics(&job, label_map, &pushgateway, metric_families, None)
    })
    .await??;

    Ok(())
}

/// Add service-specific metrics to a [ServeRealizeService] instance.
#[derive(Clone)]
pub struct MetricsRealizeServer<T> {
    inner: T,
}
impl<T> MetricsRealizeServer<T>
where
    T: Serve<Req = RealizeServiceRequest, Resp = RealizeServiceResponse>,
{
    pub fn new(inner: T) -> Self {
        Self { inner }
    }
}
impl<T> Serve for MetricsRealizeServer<T>
where
    T: Serve<Req = RealizeServiceRequest, Resp = RealizeServiceResponse>,
{
    type Req = RealizeServiceRequest;
    type Resp = RealizeServiceResponse;

    async fn serve(self, ctx: Context, req: Self::Req) -> Result<Self::Resp, ServerError> {
        self.inner.serve(ctx, req).await
    }
}

/// Stub that sets default deadlines based on method type.
#[derive(Clone)]
pub struct MetricsRealizeClient<T>
where
    T: Stub<Req = RealizeServiceRequest, Resp = RealizeServiceResponse> + Clone,
{
    inner: T,
}

impl<T: Stub<Req = RealizeServiceRequest, Resp = RealizeServiceResponse> + Clone>
    MetricsRealizeClient<T>
{
    pub fn new(stub: T) -> Self {
        Self { inner: stub }
    }
}

impl<T: Stub<Req = RealizeServiceRequest, Resp = RealizeServiceResponse> + Clone> Stub
    for MetricsRealizeClient<T>
{
    type Req = RealizeServiceRequest;
    type Resp = RealizeServiceResponse;

    async fn call(
        &self,
        ctx: Context,
        req: RealizeServiceRequest,
    ) -> Result<RealizeServiceResponse, RpcError> {
        self.inner.call(ctx, req).await
    }
}
