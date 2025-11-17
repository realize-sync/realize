use super::output::{self, OutputMode};
use anyhow::Result;
use realize_core::rpc::{control::control_capnp, result_capnp};
use std::{ffi::OsStr, os::unix::ffi::OsStrExt};

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

/// Execute the peer query command
pub(crate) async fn execute_arena_remove(
    control: &control_capnp::control::Client,
    output_mode: OutputMode,
    name: &str,
    delete_files: bool,
    keep_database: bool,
) -> Result<i32> {
    let mut request = control.remove_arena_request();
    let mut req = request.get().init_req();
    req.set_arena(name);

    let result = request.send().promise.await?;
    match result.get()?.get_res()?.which()? {
        result_capnp::result::Which::Ok(res) => {
            let res = res?;
            let dir = if res.has_dir() {
                Some(std::path::Path::new(&OsStr::from_bytes(res.get_dir()?)).to_path_buf())
            } else {
                None
            };
            let workdir = if res.has_workdir() {
                Some(std::path::Path::new(&OsStr::from_bytes(res.get_workdir()?)).to_path_buf())
            } else {
                None
            };

            if let Some(dir) = dir
                && let Some(workdir) = workdir
            {
                if delete_files {
                    std::fs::remove_dir_all(&dir)?;
                    output::print_success(
                        output_mode,
                        "OK",
                        "Arena {name} removed and deleted from: {dir:?}",
                    );
                } else if !keep_database {
                    std::fs::remove_dir_all(workdir)?;
                    output::print_success(
                        output_mode,
                        "OK",
                        "Arena {name} removed and database deleted.\n  Files available in {dir:?}",
                    );
                } else {
                    output::print_success(
                        output_mode,
                        "OK",
                        "Arena {name} removed.\n  Files available in {dir:?}\n  Database available in {workdir:?}",
                    );
                }
            } else {
                output::print_warning(output_mode, "WARN", "Arena {name} did not exist");
            }

            Ok(0)
        }
        result_capnp::result::Which::Err(_) => {
            output::print_error(output_mode, "Failed to delete arena {name}");

            Ok(1)
        }
    }
}
