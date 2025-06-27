pub(crate) async fn push_metrics(
    pushgateway: &str,
    job: &str,
    instance: Option<&str>,
) -> anyhow::Result<()> {
    let mut label_map = prometheus::labels! {};
    if let Some(instance) = instance {
        label_map.insert("instance".to_owned(), instance.to_owned());
    }
    log::debug!(
        "[metrics] push to {pushgateway}, job={job}, instance={instance:?}"
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
