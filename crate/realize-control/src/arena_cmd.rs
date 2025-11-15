use std::os::unix::ffi::OsStrExt;

use super::output::{self, OutputMode};
use anyhow::Result;
use realize_core::rpc::{control::control_capnp, result_capnp};

/// Execute the peer query command
pub(crate) async fn execute_arena_create(
    control: &control_capnp::control::Client,
    output_mode: OutputMode,
    name: &str,
    path: &std::path::Path,
) -> Result<i32> {
    let mut request = control.create_arena_request();
    let mut req = request.get().init_req();
    req.set_arena(name);
    req.set_dir(path.as_os_str().as_bytes());

    let result = request.send().promise.await?;
    // TODO: display errors and warnings
    match result.get()?.get_res()?.which()? {
        result_capnp::result::Which::Ok(_) => {
            output::print_success(output_mode, "OK", "Arena {name} created locally");

            Ok(0)
        }
        result_capnp::result::Which::Err(_) => {
            output::print_error(output_mode, "Failed to create arena {name}");

            Ok(1)
        }
    }
}
