use super::output::{self, OutputMode};
use anyhow::Result;
use realize_core::rpc::control::control_capnp;

/// Execute the peer query command
pub(crate) async fn execute_peer_query(
    control: &control_capnp::control::Client,
    output_mode: OutputMode,
) -> Result<i32> {
    let request = control.list_peers_request();
    let result = request.send().promise.await?;
    let peers = result.get()?.get_res()?;

    if peers.len() == 0 {
        output::print_info(output_mode, "No peers found");
    } else {
        for i in 0..peers.len() {
            let peer_info = peers.get(i);
            let peer = peer_info.get_peer()?.to_str()?;
            let connected = peer_info.get_connected();
            let keep_connected = peer_info.get_keep_connected();

            let status = if connected {
                "connected"
            } else if keep_connected {
                "connecting"
            } else {
                "disconnected"
            };

            output::print_info(output_mode, format!("{peer}: {status}"));
        }
    }

    Ok(0)
}

/// Execute the peer connect command
pub(crate) async fn execute_peer_connect(
    control: &control_capnp::control::Client,
    peer: &str,
    output_mode: OutputMode,
) -> Result<i32> {
    let mut request = control.keep_connected_request();
    request.get().set_peer(peer);
    request.send().promise.await?;

    output::print_success(output_mode, "OK", format!("Connecting to peer: {peer}"));
    Ok(0)
}

/// Execute the peer disconnect command
pub(crate) async fn execute_peer_disconnect(
    control: &control_capnp::control::Client,
    peer: &str,
    output_mode: OutputMode,
) -> Result<i32> {
    let mut request = control.disconnect_request();
    request.get().set_peer(peer);
    request.send().promise.await?;

    output::print_success(output_mode, "OK", format!("Disconnected from peer: {peer}"));
    Ok(0)
}
