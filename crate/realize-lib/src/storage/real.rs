pub mod hasher;
pub mod index;
pub mod notifier;
pub mod reader;
#[allow(dead_code)]
#[allow(unknown_lints)]
#[allow(clippy::uninlined_format_args)]
#[allow(clippy::extra_unused_type_parameters)]
mod real_capnp {
    include!(concat!(env!("OUT_DIR"), "/storage/real_capnp.rs"));
}
pub mod store;
pub mod watcher;
