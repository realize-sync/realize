use std::time::Instant;

use hyper::server::conn::http1;
use hyper_util::rt::TokioIo;
use prometheus::{
    Encoder, HistogramVec, IntCounterVec, register_histogram_vec, register_int_counter_vec,
};
use tarpc::{
    ServerError,
    client::{RpcError, stub::Stub},
    context::Context,
    server::Serve,
};
use tokio::net::TcpListener;

use crate::model::service::{RealizeError, RealizeServiceRequest, RealizeServiceResponse};

lazy_static::lazy_static! {
    static ref METRIC_SERVER_DATA_IN_BYTES: HistogramVec =
        register_histogram_vec!(
            "realize_server_data_in_bytes",
            "Size of the file data received by the server, embedded in RPC calls",
            &["method", "status"],
            bytes_buckets()).unwrap();
    static ref METRIC_SERVER_DATA_RANGE_BYTES: HistogramVec =
        register_histogram_vec!(
            "realize_server_data_range_bytes",
            "Size of the data range of RPC calls. Always >= data in or out.",
            &["method", "status"],
            bytes_buckets()).unwrap();
    static ref METRIC_SERVER_DATA_OUT_BYTES: HistogramVec =
        register_histogram_vec!(
            "realize_server_data_out_bytes",
            "Size of the file data sent by the server, embedded in RPC calls",
            &["method"],
            bytes_buckets()).unwrap();
    static ref METRIC_SERVER_DURATION_SECONDS: HistogramVec =
        register_histogram_vec!(
            "realize_server_duration_seconds",
            "RPC method duration, in seconds",
            &["method", "status"],
            // Default buckets are designed for just this use case
            prometheus::DEFAULT_BUCKETS.to_vec()
            ).unwrap();
    static ref METRIC_SERVER_CALL_COUNT: IntCounterVec =
        register_int_counter_vec!(
            "realize_server_call_count",
            "RPC call count, grouped by status and errors",
            &["method", "status", "error"]).unwrap();

    static ref METRIC_CLIENT_DATA_IN_BYTES: HistogramVec =
        register_histogram_vec!(
            "realize_client_data_in_bytes",
            "Size of the file data received by the client, embedded in RPC calls",
            &["method", "status"],
            bytes_buckets()).unwrap();
    static ref METRIC_CLIENT_DATA_RANGE_BYTES: HistogramVec =
        register_histogram_vec!(
            "realize_client_data_range_bytes",
            "Size of the data range of RPC calls. Always >= data in or out.",
            &["method", "status"],
            bytes_buckets()).unwrap();
    static ref METRIC_CLIENT_DATA_OUT_BYTES: HistogramVec =
        register_histogram_vec!(
            "realize_client_data_out_bytes",
            "Size of the file data sent by the client, embedded in RPC calls",
            &["method"],
            bytes_buckets()).unwrap();
    static ref METRIC_CLIENT_DURATION_SECONDS: HistogramVec =
        register_histogram_vec!(
            "realize_client_duration_seconds",
            "RPC method duration, in seconds",
            &["method", "status"],
            // Default buckets are designed for just this use case
            prometheus::DEFAULT_BUCKETS.to_vec()
            ).unwrap();
    static ref METRIC_CLIENT_CALL_COUNT: IntCounterVec =
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
fn method_label(req: &RealizeServiceRequest) -> &'static str {
    match req {
        RealizeServiceRequest::List { .. } => "list",
        RealizeServiceRequest::Send { .. } => "send",
        RealizeServiceRequest::Read { .. } => "read",
        RealizeServiceRequest::Finish { .. } => "finish",
        RealizeServiceRequest::Hash { .. } => "hash",
        RealizeServiceRequest::Delete { .. } => "delete",
        RealizeServiceRequest::CalculateSignature { .. } => "calculate_signature",
        RealizeServiceRequest::Diff { .. } => "diff",
        RealizeServiceRequest::ApplyDelta { .. } => "apply_delta",
    }
}

/// Label that describes whether a method call suceeded.
fn status_label<T>(res: &Result<RealizeServiceResponse, T>) -> &'static str {
    match res {
        Err(_) => "RpcError",
        Ok(res) => {
            if realize_error(&res).is_none() {
                "OK"
            } else {
                "AppError"
            }
        }
    }
}

/// Label that describes the error type, client-side.
fn error_label_client(res: &Result<RealizeServiceResponse, RpcError>) -> &'static str {
    match res {
        Err(err) => rpc_error_label(err),
        Ok(res) => realize_error(res).map(realize_error_label).unwrap_or("OK"),
    }
}

/// Label that describes the error type, server-side.
fn error_label_server(res: &Result<RealizeServiceResponse, ServerError>) -> &'static str {
    match res {
        Err(_) => "ServerError",
        Ok(res) => realize_error(res).map(realize_error_label).unwrap_or("OK"),
    }
}

/// Extract an error from a [RealizeServiceResponse].
fn realize_error(res: &RealizeServiceResponse) -> Option<&RealizeError> {
    match res {
        RealizeServiceResponse::List(Err(err)) => Some(err),
        RealizeServiceResponse::Send(Err(err)) => Some(err),
        RealizeServiceResponse::Read(Err(err)) => Some(err),
        RealizeServiceResponse::Finish(Err(err)) => Some(err),
        RealizeServiceResponse::Hash(Err(err)) => Some(err),
        RealizeServiceResponse::Delete(Err(err)) => Some(err),
        RealizeServiceResponse::CalculateSignature(Err(err)) => Some(err),
        RealizeServiceResponse::Diff(Err(err)) => Some(err),
        RealizeServiceResponse::ApplyDelta(Err(err)) => Some(err),
        RealizeServiceResponse::List(Ok(_)) => None,
        RealizeServiceResponse::Send(Ok(_)) => None,
        RealizeServiceResponse::Read(Ok(_)) => None,
        RealizeServiceResponse::Finish(Ok(_)) => None,
        RealizeServiceResponse::Hash(Ok(_)) => None,
        RealizeServiceResponse::Delete(Ok(_)) => None,
        RealizeServiceResponse::CalculateSignature(Ok(_)) => None,
        RealizeServiceResponse::Diff(Ok(_)) => None,
        RealizeServiceResponse::ApplyDelta(Ok(_)) => None,
    }
}

/// Label that describes a [RealizeError] in metrics.
fn realize_error_label(err: &RealizeError) -> &'static str {
    match err {
        RealizeError::BadRequest(_) => "BadRequest",
        RealizeError::Io(_) => "Io",
        RealizeError::Rsync(_, _) => "Rsync",
        RealizeError::Sync(_, _) => "Sync",
        RealizeError::Other(_) => "Other",
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
fn range_bytes(req: &RealizeServiceRequest) -> Option<u64> {
    let range = match req {
        RealizeServiceRequest::Send { range, .. } => Some(range),
        RealizeServiceRequest::Read { range, .. } => Some(range),
        RealizeServiceRequest::CalculateSignature { range, .. } => Some(range),
        RealizeServiceRequest::Diff { range, .. } => Some(range),
        RealizeServiceRequest::ApplyDelta { range, .. } => Some(range),
        _ => None,
    };

    range.map(|r| (r.1 - r.0) as u64)
}

/// Extract data size in bytes from a request for the data_in metrics.
fn bytes_in(req: &RealizeServiceRequest) -> Option<u64> {
    match req {
        RealizeServiceRequest::Send { data, .. } => Some(data.len() as u64),
        RealizeServiceRequest::Diff { signature, .. } => Some(signature.0.len() as u64),
        RealizeServiceRequest::ApplyDelta { delta, .. } => Some(delta.0.len() as u64),
        _ => None,
    }
}

/// Extract data size in bytes from a response for the data_out metrics.
fn bytes_out<T>(res: &Result<RealizeServiceResponse, T>) -> Option<u64> {
    match res {
        Ok(RealizeServiceResponse::Read(Ok(data))) => Some(data.len() as u64),
        Ok(RealizeServiceResponse::CalculateSignature(Ok(sig))) => Some(sig.0.len() as u64),
        Ok(RealizeServiceResponse::Diff(Ok(delta))) => Some(delta.0.len() as u64),
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

/// Send metrics to a prometheus push gateway.
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

/// [RealizeService] Stub that fills in client-side metrics.
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

/// [RealizeService] serve function that fills in server-side metrics.
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

#[cfg(test)]
mod tests {
    use crate::model::service::{DirectoryId, Options};
    use crate::server::RealizeServer;
    use assert_fs::TempDir;
    use prometheus::proto::MetricType;

    fn get_metric_value(name: &str, label_pairs: &[(&str, &str)]) -> f64 {
        let metric_families = prometheus::gather();
        for mf in metric_families {
            if mf.name() == name {
                for m in &mf.metric {
                    let mut all_match = true;
                    for (k, v) in label_pairs {
                        let found = m.label.iter().any(|lp| lp.name() == *k && lp.value() == *v);
                        if !found {
                            all_match = false;
                            break;
                        }
                    }
                    if all_match {
                        match mf.get_field_type() {
                            MetricType::COUNTER => return m.get_counter().value(),
                            MetricType::HISTOGRAM => {
                                return m.get_histogram().get_sample_count() as f64;
                            }
                            _ => {}
                        }
                    }
                }
            }
        }
        0.0
    }

    fn setup_inprocess_client() -> (
        assert_fs::TempDir,
        crate::model::service::DirectoryId,
        crate::model::service::RealizeServiceClient<
            crate::client::DeadlineSetter<
                super::MetricsRealizeClient<
                    tarpc::client::Channel<
                        crate::model::service::RealizeServiceRequest,
                        crate::model::service::RealizeServiceResponse,
                    >,
                >,
            >,
        >,
    ) {
        let temp = TempDir::new().unwrap();
        let dir_id = DirectoryId::from("testdir");
        let server_impl =
            RealizeServer::new(vec![crate::server::Directory::new(&dir_id, temp.path())]);
        let client = server_impl.as_inprocess_client();
        (temp, dir_id, client)
    }

    #[tokio::test]
    #[serial_test::serial]
    async fn test_client_success_call_count() -> anyhow::Result<()> {
        let (_temp, dir_id, client) = setup_inprocess_client();
        let before = get_metric_value(
            "realize_client_call_count",
            &[("method", "list"), ("status", "OK"), ("error", "OK")],
        );
        client
            .list(
                tarpc::context::current(),
                dir_id.clone(),
                Options::default(),
            )
            .await??;
        let after = get_metric_value(
            "realize_client_call_count",
            &[("method", "list"), ("status", "OK"), ("error", "OK")],
        );
        assert_eq!(after, before + 1.0);
        Ok(())
    }

    #[tokio::test]
    #[serial_test::serial]
    async fn test_client_error_call_count() -> anyhow::Result<()> {
        let (_temp, _dir_id, client) = setup_inprocess_client();
        let before_err = get_metric_value(
            "realize_client_call_count",
            &[
                ("method", "list"),
                ("status", "AppError"),
                ("error", "BadRequest"),
            ],
        );
        assert!(
            client
                .list(
                    tarpc::context::current(),
                    DirectoryId::from("doesnotexist"),
                    Options::default(),
                )
                .await?
                .is_err()
        );
        let after_err = get_metric_value(
            "realize_client_call_count",
            &[
                ("method", "list"),
                ("status", "AppError"),
                ("error", "BadRequest"),
            ],
        );
        assert_eq!(after_err, before_err + 1.0);
        Ok(())
    }

    #[tokio::test]
    #[serial_test::serial]
    async fn test_server_success_call_count() -> anyhow::Result<()> {
        let (_temp, dir_id, client) = setup_inprocess_client();
        let before_srv = get_metric_value(
            "realize_server_call_count",
            &[("method", "list"), ("status", "OK"), ("error", "OK")],
        );
        client
            .list(
                tarpc::context::current(),
                dir_id.clone(),
                Options::default(),
            )
            .await??;
        let after_srv = get_metric_value(
            "realize_server_call_count",
            &[("method", "list"), ("status", "OK"), ("error", "OK")],
        );
        assert_eq!(after_srv, before_srv + 1.0);
        Ok(())
    }

    #[tokio::test]
    #[serial_test::serial]
    async fn test_server_error_call_count() -> anyhow::Result<()> {
        let (_temp, _dir_id, client) = setup_inprocess_client();
        let before_srv_err = get_metric_value(
            "realize_server_call_count",
            &[
                ("method", "list"),
                ("status", "AppError"),
                ("error", "BadRequest"),
            ],
        );
        assert!(
            client
                .list(
                    tarpc::context::current(),
                    DirectoryId::from("doesnotexist"),
                    Options::default(),
                )
                .await?
                .is_err()
        );
        let after_srv_err = get_metric_value(
            "realize_server_call_count",
            &[
                ("method", "list"),
                ("status", "AppError"),
                ("error", "BadRequest"),
            ],
        );
        assert_eq!(after_srv_err, before_srv_err + 1.0);
        Ok(())
    }
}
