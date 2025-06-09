use prometheus::IntGauge;

lazy_static::lazy_static! {
   static ref METRIC_SERVER_IN_FLIGHT_REQUEST_COUNT: IntGauge =
    prometheus::register_int_gauge!(
        "realize_server_in_flight_request_count",
        "Number of RPCs currently in-flight on the server").unwrap();
}

/// Decorate the given future with a counter for in-flight requests.
pub(crate) async fn track_in_flight_request(fut: impl Future<Output = ()>) {
    METRIC_SERVER_IN_FLIGHT_REQUEST_COUNT.inc();
    fut.await;
    METRIC_SERVER_IN_FLIGHT_REQUEST_COUNT.dec();
}
