use anyhow;
use log;
use prometheus;
use prometheus::Encoder;
use rouille;
use std::thread;

pub fn export_metrics(metrics_addr: Option<String>) {
    if let Some(metrics_addr) = &metrics_addr {
        let metrics_addr = metrics_addr.to_string();
        thread::spawn(move || {
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
    }
}

fn handle_metrics_request() -> anyhow::Result<rouille::Response> {
    let metrics = prometheus::gather();
    let mut buffer = vec![];
    let encoder = prometheus::TextEncoder::new();
    let content_type = encoder.format_type().to_string();
    encoder.encode(&metrics, &mut buffer)?;

    Ok(rouille::Response::from_data(content_type, buffer))
}
