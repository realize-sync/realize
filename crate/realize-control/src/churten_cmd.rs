use super::display::ChurtenDisplay;
use super::output::{self, OutputMode};
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
    output_mode: OutputMode,
) -> Result<i32> {
    let churten = client::get_churten(&control).await?;
    churten.start_request().send().promise.await?;
    output::print_success(output_mode, "OK", "Churten started");

    Ok(0)
}

/// Execute the churten stop command
pub(crate) async fn execute_churten_stop(
    control: &control_capnp::control::Client,
    output_mode: OutputMode,
) -> Result<i32> {
    let churten = client::get_churten(&control).await?;
    churten.shutdown_request().send().promise.await?;
    output::print_success(output_mode, "OK", "Churten stopped");

    Ok(0)
}

/// Execute the churten is_running command
pub(crate) async fn execute_churten_is_running(
    control: &control_capnp::control::Client,
    output_mode: OutputMode,
) -> Result<i32> {
    let churten = client::get_churten(&control).await?;
    let is_running_result = churten.is_running_request().send().promise.await?;
    let is_running = is_running_result.get()?.get_running();
    output::print_info(output_mode, format!("{}", is_running));
    if output_mode == OutputMode::Quiet {
        if is_running { Ok(0) } else { Ok(10) }
    } else {
        Ok(0)
    }
}

/// Execute the churten run command
pub(crate) async fn execute_churten_run(
    control: &control_capnp::control::Client,
    output_mode: OutputMode,
) -> Result<i32> {
    let shutdown = CancellationToken::new();
    task::spawn({
        let shutdown = shutdown.clone();
        async move {
            if ctrl_c().await.is_ok() {
                if output_mode == OutputMode::Progress {
                    println!("\nCTRL-C");
                }
                shutdown.cancel();
            }
        }
    });

    let churten = client::get_churten(&control).await?;
    churten.start_request().send().promise.await?;
    output::print_success(output_mode, "Starting", "...");

    let rx = client::subscribe_to_churten(&churten).await?;

    // Have run_churten run in a normal Tokio environment (outside
    // LocalSet).
    let res = task::spawn(async move { run_churten(output_mode, rx, shutdown).await }).await;

    // Shutdown even if run_churten failed and prioritize showing the
    // error from churten over the error from shutdown.
    let shutdown_res = churten.shutdown_request().send().promise.await;
    res??;
    shutdown_res?;
    output::print_info(output_mode, "Churten stopped");

    Ok(0)
}

async fn run_churten(
    output_mode: OutputMode,
    mut rx: mpsc::Receiver<ChurtenUpdates>,
    shutdown: CancellationToken,
) -> Result<(), anyhow::Error> {
    let mut display = ChurtenDisplay::new(output_mode);
    while let Some(update) = tokio::select!(
        _ = shutdown.cancelled() => {
            return Ok(());
        }
        res = rx.recv() =>  { res })
    {
        display.update(update).await;
    }
    display.finished().await;

    return Ok(());
}
