use super::output::{self, OutputMode};
use anyhow::Result;
use realize_core::rpc::control::control_capnp;
use realize_core::rpc::result_capnp;

/// Execute the attr list command
pub(crate) async fn execute_attr_list(
    control: &control_capnp::control::Client,
    output_mode: OutputMode,
    arena: &str,
    paths: &Vec<String>,
) -> Result<i32> {
    let mut has_errors = false;
    for path in PathsOrArena::new(paths) {
        let mut request = control.list_attr_request();
        let mut req = request.get().init_req();
        req.set_arena(arena);
        req.set_path(path);

        let result = request.send().promise.await?;
        let response = result.get()?.get_res()?;
        match response.which()? {
            result_capnp::result::Which::Ok(response) => {
                let attrs = response?.get_attrs()?;
                let mut msg = format!("{}: ", msg_tag(arena, path));
                let mut first = true;
                for attr in attrs.iter() {
                    if first {
                        first = false;
                    } else {
                        msg.push_str(", ");
                    }
                    msg.push_str(attr?.to_str()?);
                }
                output::print_info(output_mode, msg);
            }
            result_capnp::result::Which::Err(err) => {
                has_errors = true;
                handle_error(output_mode, arena, path, "", err?)?;
            }
        }
    }

    Ok(if has_errors { 1 } else { 0 })
}

/// Execute the attr get command
pub(crate) async fn execute_attr_get(
    control: &control_capnp::control::Client,
    output_mode: OutputMode,
    attr: &str,
    arena: &str,
    paths: &Vec<String>,
) -> Result<i32> {
    let mut has_errors = false;
    for path in PathsOrArena::new(paths) {
        let mut request = control.get_attr_request();
        let mut req = request.get().init_req();
        req.set_attr(attr);
        req.set_arena(arena);
        req.set_path(&path);

        let result = request.send().promise.await?;
        let response = result.get()?.get_res()?;

        match response.which()? {
            result_capnp::result::Which::Ok(response) => {
                let value = response?.get_value()?.to_str()?;
                output::print_info(
                    output_mode,
                    format!("{}: {attr}={value}", msg_tag(arena, path)),
                );
            }
            result_capnp::result::Which::Err(err) => {
                let err = err?;
                has_errors = true;
                handle_error(output_mode, arena, path, attr, err)?;
            }
        }
    }

    Ok(if has_errors { 1 } else { 0 })
}

/// Execute the attr set command
pub(crate) async fn execute_attr_set(
    control: &control_capnp::control::Client,
    output_mode: OutputMode,
    attr: &str,
    value: &str,
    arena: &str,
    paths: &Vec<String>,
) -> Result<i32> {
    let mut has_errors = false;
    for path in PathsOrArena::new(paths) {
        let mut request = control.set_attr_request();
        let mut req = request.get().init_req();
        req.set_attr(attr);
        req.set_value(value);
        req.set_arena(arena);
        req.set_path(&path);

        let result = request.send().promise.await?;
        let response = result.get()?.get_res()?;

        match response.which()? {
            result_capnp::result::Which::Ok(_) => {
                output::print_success(
                    output_mode,
                    "OK",
                    &format!("{}: attribute set", msg_tag(arena, path)),
                );
            }
            result_capnp::result::Which::Err(err) => {
                let err = err?;
                has_errors = true;
                handle_error(output_mode, arena, path, attr, err)?;
            }
        }
    }

    Ok(if has_errors { 1 } else { 0 })
}

/// Returns all paths in a vector or just a single "", meaning the
/// arena root, if the vector is empty.
enum PathsOrArena<'a> {
    ArenaRoot(usize),
    Paths(usize, &'a Vec<String>),
}

impl<'a> PathsOrArena<'a> {
    fn new(vec: &'a Vec<String>) -> Self {
        log::debug!("====== ook vec={vec:?}");
        if vec.is_empty() {
            PathsOrArena::ArenaRoot(0)
        } else {
            PathsOrArena::Paths(0, vec)
        }
    }
}

impl<'a> Iterator for PathsOrArena<'a> {
    type Item = &'a str;

    fn next(&mut self) -> Option<Self::Item> {
        match self {
            PathsOrArena::ArenaRoot(index) => {
                if *index > 0 {
                    None
                } else {
                    *index += 1;
                    Some("")
                }
            }
            PathsOrArena::Paths(index, vec) => {
                if *index >= vec.len() {
                    None
                } else {
                    let i = *index;
                    *index += 1;
                    Some(vec[i].as_str())
                }
            }
        }
    }
}

/// Put the arena and tag together for including in a display message.
fn msg_tag(arena: &str, path: &str) -> String {
    if path.is_empty() {
        format!("[{arena}]")
    } else {
        format!("[{arena}]/{path}")
    }
}

/// Handle AttrErrors from capnp.
fn handle_error(
    output_mode: OutputMode,
    arena: &str,
    path: &str,
    attrname: &str,
    err: control_capnp::attr_error::Reader<'_>,
) -> Result<(), anyhow::Error> {
    match err.which()? {
        control_capnp::attr_error::Which::NoSuchAttribute(_) => {
            output::print_error(
                output_mode,
                format!("{}: no such attribute '{attrname}'", msg_tag(arena, path)),
            );
        }
        control_capnp::attr_error::Which::InvalidAttributeValue(_) => {
            output::print_error(
                output_mode,
                format!(
                    "{}: invalid value for attribute '{attrname}'",
                    msg_tag(arena, path)
                ),
            );
        }
        control_capnp::attr_error::Which::UnknownArena(_) => {
            // It's not worth it to continue to process other paths;
            // they'll all fail the same way
            anyhow::bail!("unknown arena '{arena}'")
        }
        control_capnp::attr_error::Which::PathNotFound(_) => {
            output::print_error(
                output_mode,
                format!("{}: path not found", msg_tag(arena, path)),
            );
        }
    };

    Ok(())
}
