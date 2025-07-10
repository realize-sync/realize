//! Prometheus metrics for Realize - Symmetric File Syncer
//!
//! This module provides Prometheus metrics integration for both client and server sides.
//! It tracks RPC call counts, durations, data transferred, and in-flight requests.
//! Metrics are exported via HTTP for Prometheus scraping.
//!
//! # Example
//!
//! To export metrics on an HTTP endpoint:
//! ```rust
//! realize_lib::rpc::realstore::metrics::export_metrics("127.0.0.1:9000");
//! ```

use realize_types::ByteRange;
use crate::rpc::realstore::{RealStoreServiceRequest, RealStoreServiceResponse};
use realize_storage::RealStoreError;
use hyper::server::conn::http1;
use hyper_util::rt::TokioIo;
use prometheus::{
    Encoder, HistogramVec, IntCounterVec, register_histogram_vec, register_int_counter_vec,
};
use std::time::Instant;
use tarpc::ServerError;
use tarpc::client::RpcError;
use tarpc::client::stub::Stub;
use tarpc::context::Context;
use tarpc::server::Serve;
use tokio::net::TcpListener;

lazy_static::lazy_static! {
    pub(crate) static ref METRIC_SERVER_DATA_IN_BYTES: HistogramVec =
        register_histogram_vec!(
            "realize_server_data_in_bytes",
            "Size of the file data received by the server, embedded in RPC calls",
            &["method", "status"],
            bytes_buckets()).unwrap();
    pub(crate) static ref METRIC_SERVER_DATA_RANGE_BYTES: HistogramVec =
        register_histogram_vec!(
            "realize_server_data_range_bytes",
            "Size of the data range of RPC calls. Always >= data in or out.",
            &["method", "status"],
            bytes_buckets()).unwrap();
    pub(crate) static ref METRIC_SERVER_DATA_OUT_BYTES: HistogramVec =
        register_histogram_vec!(
            "realize_server_data_out_bytes",
            "Size of the file data sent by the server, embedded in RPC calls",
            &["method"],
            bytes_buckets()).unwrap();
    pub(crate) static ref METRIC_SERVER_DURATION_SECONDS: HistogramVec =
        register_histogram_vec!(
            "realize_server_duration_seconds",
            "RPC method duration, in seconds",
            &["method", "status"],
            // Default buckets are designed for just this use case
            prometheus::DEFAULT_BUCKETS.to_vec()
            ).unwrap();
    pub(crate) static ref METRIC_SERVER_CALL_COUNT: IntCounterVec =
        register_int_counter_vec!(
            "realize_server_call_count",
            "RPC call count, grouped by status and errors",
            &["method", "status", "error"]).unwrap();

    pub(crate) static ref METRIC_CLIENT_DATA_IN_BYTES: HistogramVec =
        register_histogram_vec!(
            "realize_client_data_in_bytes",
            "Size of the file data received by the client, embedded in RPC calls",
            &["method", "status"],
            bytes_buckets()).unwrap();
    pub(crate) static ref METRIC_CLIENT_DATA_RANGE_BYTES: HistogramVec =
        register_histogram_vec!(
            "realize_client_data_range_bytes",
            "Size of the data range of RPC calls. Always >= data in or out.",
            &["method", "status"],
            bytes_buckets()).unwrap();
    pub(crate) static ref METRIC_CLIENT_DATA_OUT_BYTES: HistogramVec =
        register_histogram_vec!(
            "realize_client_data_out_bytes",
            "Size of the file data sent by the client, embedded in RPC calls",
            &["method"],
            bytes_buckets()).unwrap();
    pub(crate) static ref METRIC_CLIENT_DURATION_SECONDS: HistogramVec =
        register_histogram_vec!(
            "realize_client_duration_seconds",
            "RPC method duration, in seconds",
            &["method", "status"],
            // Default buckets are designed for just this use case
            prometheus::DEFAULT_BUCKETS.to_vec()
            ).unwrap();
    pub(crate) static ref METRIC_CLIENT_CALL_COUNT: IntCounterVec =
        register_int_counter_vec!(
            "realize_client_call_count",
            "RPC call count, grouped by status and errors",
            &["method", "status", "error"]).unwrap();
}

/// 1M to 64G in 16 buckets.
fn bytes_buckets() -> Vec<f64> {
    prometheus::exponential_buckets(1024.0 * 1024.0, 2.0, 16).unwrap()
}

/// Label that identifies methods in metrics.
fn method_label(req: &RealStoreServiceRequest) -> &'static str {
    match req {
        RealStoreServiceRequest::List { .. } => "list",
        RealStoreServiceRequest::Send { .. } => "send",
        RealStoreServiceRequest::Read { .. } => "read",
        RealStoreServiceRequest::Finish { .. } => "finish",
        RealStoreServiceRequest::Hash { .. } => "hash",
        RealStoreServiceRequest::Delete { .. } => "delete",
        RealStoreServiceRequest::CalculateSignature { .. } => "calculate_signature",
        RealStoreServiceRequest::Diff { .. } => "diff",
        RealStoreServiceRequest::ApplyDelta { .. } => "apply_delta",
        RealStoreServiceRequest::Truncate { .. } => "truncate",
        RealStoreServiceRequest::Configure { .. } => "configure",
    }
}

/// Label that describes whether a method call suceeded.
fn status_label<T>(res: &Result<RealStoreServiceResponse, T>) -> &'static str {
    match res {
        Err(_) => "RpcError",
        Ok(res) => {
            if realize_error(res).is_none() {
                "OK"
            } else {
                "AppError"
            }
        }
    }
}

/// Label that describes the error type, client-side.
fn error_label_client(res: &Result<RealStoreServiceResponse, RpcError>) -> &'static str {
    match res {
        Err(err) => rpc_error_label(err),
        Ok(res) => realize_error(res).map(realize_error_label).unwrap_or("OK"),
    }
}

/// Label that describes the error type, server-side.
fn error_label_server(res: &Result<RealStoreServiceResponse, ServerError>) -> &'static str {
    match res {
        Err(_) => "ServerError",
        Ok(res) => realize_error(res).map(realize_error_label).unwrap_or("OK"),
    }
}

/// Extract an error from a [RealStoreServiceResponse].
fn realize_error(res: &RealStoreServiceResponse) -> Option<&RealStoreError> {
    match res {
        RealStoreServiceResponse::List(Err(err)) => Some(err),
        RealStoreServiceResponse::Send(Err(err)) => Some(err),
        RealStoreServiceResponse::Read(Err(err)) => Some(err),
        RealStoreServiceResponse::Finish(Err(err)) => Some(err),
        RealStoreServiceResponse::Hash(Err(err)) => Some(err),
        RealStoreServiceResponse::Delete(Err(err)) => Some(err),
        RealStoreServiceResponse::CalculateSignature(Err(err)) => Some(err),
        RealStoreServiceResponse::Diff(Err(err)) => Some(err),
        RealStoreServiceResponse::ApplyDelta(Err(err)) => Some(err),
        RealStoreServiceResponse::Truncate(Err(err)) => Some(err),
        RealStoreServiceResponse::Configure(Err(err)) => Some(err),
        RealStoreServiceResponse::List(Ok(_)) => None,
        RealStoreServiceResponse::Send(Ok(_)) => None,
        RealStoreServiceResponse::Read(Ok(_)) => None,
        RealStoreServiceResponse::Finish(Ok(_)) => None,
        RealStoreServiceResponse::Hash(Ok(_)) => None,
        RealStoreServiceResponse::Delete(Ok(_)) => None,
        RealStoreServiceResponse::CalculateSignature(Ok(_)) => None,
        RealStoreServiceResponse::Diff(Ok(_)) => None,
        RealStoreServiceResponse::ApplyDelta(Ok(_)) => None,
        RealStoreServiceResponse::Truncate(Ok(_)) => None,
        RealStoreServiceResponse::Configure(Ok(_)) => None,
    }
}

/// Label that describes a [RealizeError] in metrics.
fn realize_error_label(err: &RealStoreError) -> &'static str {
    match err {
        RealStoreError::BadRequest(_) => "BadRequest",
        RealStoreError::Io(_) => "Io",
        RealStoreError::Rsync(_, _) => "Rsync",
        RealStoreError::Other(_) => "Other",
        RealStoreError::HashMismatch => "HashMismatch",
    }
}

/// Label that describes a [RpcError] in metrics.
fn rpc_error_label(err: &RpcError) -> &'static str {
    match err {
        RpcError::Shutdown => "RPC::Shutdown",
        RpcError::Send(_) => "RPC::Send",
        RpcError::Channel(_) => "RPC::Channel",
        RpcError::DeadlineExceeded => "RPC::DeadlineExceeded",
        RpcError::Server(_) => "RPC::Server",
    }
}

/// Extract range from a request for the range metrics.
fn range_bytes(req: &RealStoreServiceRequest) -> Option<u64> {
    let range = match req {
        RealStoreServiceRequest::Send { range, .. } => Some(range),
        RealStoreServiceRequest::Read { range, .. } => Some(range),
        RealStoreServiceRequest::CalculateSignature { range, .. } => Some(range),
        RealStoreServiceRequest::Diff { range, .. } => Some(range),
        RealStoreServiceRequest::ApplyDelta { range, .. } => Some(range),
        RealStoreServiceRequest::List { .. }
        | RealStoreServiceRequest::Finish { .. }
        | RealStoreServiceRequest::Hash { .. }
        | RealStoreServiceRequest::Delete { .. }
        | RealStoreServiceRequest::Truncate { .. }
        | RealStoreServiceRequest::Configure { .. } => None,
    };

    range.map(|r| r.end - r.start)
}

/// Extract data size in bytes from a request for the data_in metrics.
fn bytes_in(req: &RealStoreServiceRequest) -> Option<u64> {
    match req {
        RealStoreServiceRequest::Send { data, .. } => Some(data.len() as u64),
        RealStoreServiceRequest::Diff { signature, .. } => Some(signature.0.len() as u64),
        RealStoreServiceRequest::ApplyDelta { delta, .. } => Some(delta.0.len() as u64),
        RealStoreServiceRequest::List { .. }
        | RealStoreServiceRequest::Read { .. }
        | RealStoreServiceRequest::Finish { .. }
        | RealStoreServiceRequest::Hash { .. }
        | RealStoreServiceRequest::Delete { .. }
        | RealStoreServiceRequest::CalculateSignature { .. }
        | RealStoreServiceRequest::Truncate { .. }
        | RealStoreServiceRequest::Configure { .. } => None,
    }
}

/// Extract data size in bytes from a response for the data_out metrics.
fn bytes_out<T>(res: &Result<RealStoreServiceResponse, T>) -> Option<u64> {
    match res {
        Ok(RealStoreServiceResponse::Read(Ok(data))) => Some(data.len() as u64),
        Ok(RealStoreServiceResponse::CalculateSignature(Ok(sig))) => Some(sig.0.len() as u64),
        Ok(RealStoreServiceResponse::Diff(Ok((delta, _)))) => Some(delta.0.len() as u64),
        _ => None,
    }
}

/// Run a HTTP server in the background to expose metrics at the given
/// address.
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

/// [RealStoreService] Stub that fills in client-side metrics.
#[derive(Clone)]
pub(crate) struct MetricsRealizeClient<T>
where
    T: Stub<Req = RealStoreServiceRequest, Resp = RealStoreServiceResponse> + Clone,
{
    inner: T,
}

impl<T: Stub<Req = RealStoreServiceRequest, Resp = RealStoreServiceResponse> + Clone>
    MetricsRealizeClient<T>
{
    pub(crate) fn new(stub: T) -> Self {
        Self { inner: stub }
    }
}

impl<T: Stub<Req = RealStoreServiceRequest, Resp = RealStoreServiceResponse> + Clone> Stub
    for MetricsRealizeClient<T>
{
    type Req = RealStoreServiceRequest;
    type Resp = RealStoreServiceResponse;

    async fn call(
        &self,
        ctx: Context,
        req: RealStoreServiceRequest,
    ) -> Result<RealStoreServiceResponse, RpcError> {
        let method = method_label(&req);
        let range_bytes = range_bytes(&req);
        let bytes_in = bytes_in(&req);
        let start = Instant::now();

        let res = self.inner.call(ctx, req).await;

        let duration = start.elapsed();
        let bytes_out = bytes_out(&res);
        let status = status_label(&res);
        let error = error_label_client(&res);

        if let Some(val) = bytes_in {
            METRIC_CLIENT_DATA_IN_BYTES
                .with_label_values(&[method, status])
                .observe(val as f64);
        }
        if let Some(val) = range_bytes {
            METRIC_CLIENT_DATA_RANGE_BYTES
                .with_label_values(&[method, status])
                .observe(val as f64);
        }
        if let Some(val) = bytes_out {
            METRIC_CLIENT_DATA_OUT_BYTES
                .with_label_values(&[method])
                .observe(val as f64);
        }
        METRIC_CLIENT_DURATION_SECONDS
            .with_label_values(&[method, status])
            .observe(duration.as_secs_f64());
        METRIC_CLIENT_CALL_COUNT
            .with_label_values(&[method, status, error])
            .inc();

        res
    }
}

/// [RealStoreService] serve function that fills in server-side metrics.
#[derive(Clone)]
pub(crate) struct MetricsRealizeServer<T> {
    inner: T,
}
impl<T> MetricsRealizeServer<T>
where
    T: Serve<Req = RealStoreServiceRequest, Resp = RealStoreServiceResponse>,
{
    pub(crate) fn new(inner: T) -> Self {
        Self { inner }
    }
}
impl<T> Serve for MetricsRealizeServer<T>
where
    T: Serve<Req = RealStoreServiceRequest, Resp = RealStoreServiceResponse>,
{
    type Req = RealStoreServiceRequest;
    type Resp = RealStoreServiceResponse;

    async fn serve(self, ctx: Context, req: Self::Req) -> Result<Self::Resp, ServerError> {
        let method = method_label(&req);
        let range_bytes = range_bytes(&req);
        let bytes_in = bytes_in(&req);
        let start = Instant::now();

        let res = self.inner.serve(ctx, req).await;

        let duration = start.elapsed();
        let bytes_out = bytes_out(&res);
        let status = status_label(&res);
        let error = error_label_server(&res);

        if let Some(val) = bytes_in {
            METRIC_SERVER_DATA_IN_BYTES
                .with_label_values(&[method, status])
                .observe(val as f64);
        }
        if let Some(val) = range_bytes {
            METRIC_SERVER_DATA_RANGE_BYTES
                .with_label_values(&[method, status])
                .observe(val as f64);
        }
        if let Some(val) = bytes_out {
            METRIC_SERVER_DATA_OUT_BYTES
                .with_label_values(&[method])
                .observe(val as f64);
        }
        METRIC_SERVER_DURATION_SECONDS
            .with_label_values(&[method, status])
            .observe(duration.as_secs_f64());
        METRIC_SERVER_CALL_COUNT
            .with_label_values(&[method, status, error])
            .inc();

        res
    }
}

// Unit tests are kept in tests/metric_test.rs to avoid interferences.

pub fn range_len(range: Option<&ByteRange>) -> u64 {
    range.map(|r| r.end - r.start).unwrap_or(0)
}
