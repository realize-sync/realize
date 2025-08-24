use super::output::{self, OutputMode};
use anyhow::Result;
use realize_core::rpc::control::control_capnp;

#[derive(Debug, Clone, clap::ValueEnum)]
pub(crate) enum MarkValue {
    Watch,
    Keep,
    Own,
}

/// Execute the mark set command
pub(crate) async fn execute_mark_set(
    control: &control_capnp::control::Client,
    mark: &MarkValue,
    arena: &str,
    paths: &[String],
    output_mode: OutputMode,
) -> Result<i32> {
    let mark_value = match mark {
        MarkValue::Watch => control_capnp::Mark::Watch,
        MarkValue::Keep => control_capnp::Mark::Keep,
        MarkValue::Own => control_capnp::Mark::Own,
    };

    if paths.is_empty() {
        // Set arena mark
        let mut request = control.set_mark_request();
        let mut req = request.get().init_req();
        req.set_arena(arena);
        req.set_path(""); // Empty path means arena mark
        req.set_mark(mark_value);
        request.send().promise.await?;
        println!("Arena mark set successfully");
    } else {
        // Set marks on individual paths
        for path in paths {
            let mut request = control.set_mark_request();
            let mut req = request.get().init_req();
            req.set_arena(arena);
            req.set_path(path);
            req.set_mark(mark_value);
            request.send().promise.await?;
        }
        output::print_success(
            output_mode,
            "OK",
            format!("Marks set on {} paths", paths.len()),
        );
    }

    Ok(0)
}

/// Execute the mark get command
pub(crate) async fn execute_mark_get(
    control: &control_capnp::control::Client,
    arena: &str,
    paths: &[String],
    output_mode: OutputMode,
) -> Result<i32> {
    if paths.is_empty() {
        // Get arena mark
        let mut request = control.get_mark_request();
        let mut req = request.get().init_req();
        req.set_arena(arena);
        req.set_path(""); // Empty path means arena mark
        let result = request.send().promise.await?;
        let mark = result.get()?.get_res()?.get_mark();

        let mark_str = match mark {
            Ok(control_capnp::Mark::Watch) => "watch",
            Ok(control_capnp::Mark::Keep) => "keep",
            Ok(control_capnp::Mark::Own) => "own",
            Err(_) => "unknown",
        };

        output::print_info(output_mode, format!("arena: {mark_str}"));
    } else {
        // Get marks on individual paths
        for path in paths {
            let mut request = control.get_mark_request();
            let mut req = request.get().init_req();
            req.set_arena(arena);
            req.set_path(path);
            let result = request.send().promise.await?;
            let mark = result.get()?.get_res()?.get_mark();

            let mark_str = match mark {
                Ok(control_capnp::Mark::Watch) => "watch",
                Ok(control_capnp::Mark::Keep) => "keep",
                Ok(control_capnp::Mark::Own) => "own",
                Err(_) => "unknown",
            };

            output::print_info(output_mode, format!("{path}: {mark_str}"));
        }
    }

    Ok(0)
}
