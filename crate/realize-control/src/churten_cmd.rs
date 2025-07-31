use super::output::{self, OutputMode};
use anyhow::Result;
use realize_core::rpc::control::client;
use realize_core::rpc::control::control_capnp;

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
    let churten = client::get_churten(&control).await?;
    churten.start_request().send().promise.await?;
    output::print_success(output_mode, "Starting", "...");

    let res = run_churten(&churten).await;

    churten.shutdown_request().send().promise.await?;
    output::print_info(output_mode, "Churten stopped");

    res?;

    Ok(0)
}

async fn run_churten(
    churten: &realize_core::rpc::control::control_capnp::churten::Client,
) -> Result<i32, anyhow::Error> {
    let mut rx = client::subscribe_to_churten(churten).await?;

    while let Some(update) = rx.recv().await {
        println!("RECV: {update:?}");
    }
    println!("Done rx.closed: {}", rx.is_closed());

    return Ok(0);
}
