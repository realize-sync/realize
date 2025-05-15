use prometheus::Encoder;

pub async fn export_metrics(metrics_addr: &str) -> anyhow::Result<()> {
    let metrics_addr = metrics_addr.to_string();
    std::thread::spawn(move || {
        log::info!("[metrics] server listening on {}", metrics_addr);
        rouille::start_server(metrics_addr, move |request| {
            if request.url() == "/metrics" {
                handle_metrics_request().unwrap_or_else(|_| {
                    rouille::Response::text("Internal error").with_status_code(500)
                })
            } else {
                rouille::Response::empty_404()
            }
        });
    });

    Ok(())
}

fn handle_metrics_request() -> anyhow::Result<rouille::Response> {
    let metrics = prometheus::gather();
    let mut buffer = vec![];
    let encoder = prometheus::TextEncoder::new();
    let content_type = encoder.format_type().to_string();
    encoder.encode(&metrics, &mut buffer)?;

    Ok(rouille::Response::from_data(content_type, buffer))
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
