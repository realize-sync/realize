use assert_fs::prelude::*;
use assert_fs::TempDir;
use prometheus::proto::MetricType;
use realize_lib::logic::consensus::movedirs;
use realize_lib::logic::consensus::movedirs::METRIC_END_COUNT;
use realize_lib::logic::consensus::movedirs::METRIC_FILE_END_COUNT;
use realize_lib::logic::consensus::movedirs::METRIC_FILE_START_COUNT;
use realize_lib::logic::consensus::movedirs::METRIC_RANGE_READ_BYTES;
use realize_lib::logic::consensus::movedirs::METRIC_RANGE_WRITE_BYTES;
use realize_lib::logic::consensus::movedirs::METRIC_READ_BYTES;
use realize_lib::logic::consensus::movedirs::METRIC_START_COUNT;
use realize_lib::logic::consensus::movedirs::METRIC_WRITE_BYTES;
use realize_lib::model::Arena;
use realize_lib::network::rpc::realstore::server::{self, InProcessRealStoreServiceClient};
use realize_lib::storage::real;
use realize_lib::storage::real::RealStore;

// Metric tests are kept in their own binary to avoid other test
// running in parallel interfering with the counts.
//
// serial_test::serial ensures that tests in this file are run in
// sequence.

#[tokio::test]
#[serial_test::serial]
async fn client_success_call_count() -> anyhow::Result<()> {
    let (_temp, arena, client) = setup_inprocess_client();
    let before = get_metric_value(
        "realize_client_call_count",
        &[("method", "list"), ("status", "OK"), ("error", "OK")],
    );
    client
        .list(
            tarpc::context::current(),
            arena.clone(),
            real::Options::default(),
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
async fn client_error_call_count() -> anyhow::Result<()> {
    let (_temp, _arena, client) = setup_inprocess_client();
    let before_err = get_metric_value(
        "realize_client_call_count",
        &[
            ("method", "list"),
            ("status", "AppError"),
            ("error", "BadRequest"),
        ],
    );
    assert!(client
        .list(
            tarpc::context::current(),
            Arena::from("doesnotexist"),
            real::Options::default(),
        )
        .await?
        .is_err());
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
async fn server_success_call_count() -> anyhow::Result<()> {
    let (_temp, arena, client) = setup_inprocess_client();
    let before_srv = get_metric_value(
        "realize_server_call_count",
        &[("method", "list"), ("status", "OK"), ("error", "OK")],
    );
    client
        .list(
            tarpc::context::current(),
            arena.clone(),
            real::Options::default(),
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
async fn server_error_call_count() -> anyhow::Result<()> {
    let (_temp, _arena, client) = setup_inprocess_client();
    let before_srv_err = get_metric_value(
        "realize_server_call_count",
        &[
            ("method", "list"),
            ("status", "AppError"),
            ("error", "BadRequest"),
        ],
    );
    assert!(client
        .list(
            tarpc::context::current(),
            Arena::from("doesnotexist"),
            real::Options::default(),
        )
        .await?
        .is_err());
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

#[tokio::test]
#[serial_test::serial]
async fn move_files_metrics() -> anyhow::Result<()> {
    // Reset metrics (set to zero) by clearing the registry and re-registering
    // Not strictly necessary for IntCounter, but ensures test isolation
    METRIC_START_COUNT.reset();
    METRIC_END_COUNT.reset();
    METRIC_FILE_START_COUNT.reset();
    METRIC_FILE_END_COUNT.reset();
    METRIC_READ_BYTES.reset();
    METRIC_WRITE_BYTES.reset();
    METRIC_RANGE_READ_BYTES.reset();
    METRIC_RANGE_WRITE_BYTES.reset();

    let src_temp = TempDir::new()?;
    let dst_temp = TempDir::new()?;
    src_temp.child("foo").write_str("abc")?;

    let arena = Arena::from("testdir");
    let (success, error, _interrupted) = movedirs::move_dir(
        tarpc::context::current(),
        &server::create_inprocess_client(RealStore::single(&arena, src_temp.path())),
        &server::create_inprocess_client(RealStore::single(&arena, dst_temp.path())),
        Arena::from("testdir"),
        None,
    )
    .await?;
    assert_eq!(success, 1);
    assert_eq!(error, 0);
    // Check that metrics counters incremented
    assert_eq!(METRIC_START_COUNT.get(), 1);
    assert_eq!(METRIC_END_COUNT.get(), 1);
    assert_eq!(METRIC_FILE_START_COUNT.get(), 1);
    assert_eq!(METRIC_FILE_END_COUNT.with_label_values(&["Ok"]).get(), 1);
    // At least some bytes should have been read/written
    assert_eq!(METRIC_READ_BYTES.with_label_values(&["read"]).get(), 3);
    assert_eq!(METRIC_WRITE_BYTES.with_label_values(&["send"]).get(), 3);
    assert_eq!(
        METRIC_RANGE_READ_BYTES.with_label_values(&["read"]).get(),
        3
    );
    assert_eq!(
        METRIC_RANGE_WRITE_BYTES.with_label_values(&["send"]).get(),
        3
    );
    Ok(())
}

fn setup_inprocess_client() -> (TempDir, Arena, InProcessRealStoreServiceClient) {
    let temp = TempDir::new().unwrap();
    let arena = Arena::from("testdir");
    let client = server::create_inprocess_client(RealStore::single(&arena, temp.path()));

    (temp, arena, client)
}

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
