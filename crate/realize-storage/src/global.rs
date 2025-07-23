pub mod cache;
pub mod types;
#[allow(dead_code)]
#[allow(unknown_lints)]
#[allow(clippy::uninlined_format_args)]
#[allow(clippy::extra_unused_type_parameters)]
pub mod cache_capnp {
    include!(concat!(env!("OUT_DIR"), "/global/cache_capnp.rs"));
}
