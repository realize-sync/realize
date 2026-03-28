use super::display::ChurtenDisplay;
use crate::output::{Output, OutputMode};
use anyhow::Result;
use realize_core::rpc::control::client;
use realize_core::rpc::control::client::ChurtenUpdates;
use realize_core::rpc::control::control_capnp;
use tokio::signal::ctrl_c;
use tokio::sync::mpsc;
use tokio::task;
use tokio_util::sync::CancellationToken;

/// Execute the churten start command
pub(crate) async fn execute_churten_start(
    control: &control_capnp::control::Client,
    output: &Output,
) -> Result<i32> {
    let churten = client::get_churten(&control).await?;
    churten.start_request().send().promise.await?;
    output.print_success("OK", "Churten started");

    Ok(0)
}

/// Execute the churten stop command
pub(crate) async fn execute_churten_stop(
    control: &control_capnp::control::Client,
    output: &Output,
) -> Result<i32> {
    let churten = client::get_churten(&control).await?;
    churten.shutdown_request().send().promise.await?;
    output.print_success("OK", "Churten stopped");

    Ok(0)
}

/// Execute the churten is_running command
pub(crate) async fn execute_churten_is_running(
    control: &control_capnp::control::Client,
    output: &Output,
) -> Result<i32> {
    let churten = client::get_churten(&control).await?;
    let is_running_result = churten.is_running_request().send().promise.await?;
    let is_running = is_running_result.get()?.get_running();
    output.print_info(format!("{}", is_running));
    if output.mode() == OutputMode::Quiet {
        if is_running { Ok(0) } else { Ok(10) }
    } else {
        Ok(0)
    }
}

/// Execute the churten connect command
pub(crate) async fn execute_churten_connect(
    control: &control_capnp::control::Client,
    output: &Output,
) -> Result<i32> {
    let shutdown = CancellationToken::new();
    task::spawn({
        let shutdown = shutdown.clone();
        async move {
            if ctrl_c().await.is_ok() {
                shutdown.cancel();
            }
        }
    });

    let churten = client::get_churten(&control).await?;
    churten.start_request().send().promise.await?;

    let rx = client::subscribe_to_churten(&churten).await?;
    let jobs = client::all_jobs(&churten).await?;

    // Run in a normal Tokio environment, outside LocalSet
    task::spawn({
        let output = output.clone();
        async move {
            let mut display = ChurtenDisplay::default(output);
            display.init(&jobs);
            let res = connect(&mut display, rx, shutdown).await;
            display.finished().await;

            res
        }
    })
    .await??;

    output.print_info("Churten stopped");

    Ok(0)
}

async fn connect(
    display: &mut ChurtenDisplay,
    mut rx: mpsc::Receiver<ChurtenUpdates>,
    shutdown: CancellationToken,
) -> Result<(), anyhow::Error> {
    while let Some(update) = tokio::select!(
        _ = shutdown.cancelled() => {
            return Ok(());
        }
        res = rx.recv() =>  { res })
    {
        display.update(update).await;
    }

    return Ok(());
}
