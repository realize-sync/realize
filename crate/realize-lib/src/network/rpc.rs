mod capnp;
pub mod realstore;

#[allow(dead_code)]
#[allow(unknown_lints)]
#[allow(clippy::uninlined_format_args)]
#[allow(clippy::extra_unused_type_parameters)]
mod peer_capnp {
    include!(concat!(env!("OUT_DIR"), "/peer_capnp.rs"));
}

#[allow(dead_code)]
#[allow(unknown_lints)]
#[allow(clippy::uninlined_format_args)]
#[allow(clippy::extra_unused_type_parameters)]
mod store_capnp {
    include!(concat!(env!("OUT_DIR"), "/store_capnp.rs"));
}

#[allow(dead_code)]
#[allow(unknown_lints)]
#[allow(clippy::uninlined_format_args)]
#[allow(clippy::extra_unused_type_parameters)]
mod result_capnp {
    include!(concat!(env!("OUT_DIR"), "/result_capnp.rs"));
}

pub use capnp::Household;
