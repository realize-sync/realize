mod capnp;
pub mod realstore;

#[allow(dead_code)]
mod peer_capnp {
    include!(concat!(env!("OUT_DIR"), "/peer_capnp.rs"));
}

#[allow(dead_code)]
mod store_capnp {
    include!(concat!(env!("OUT_DIR"), "/store_capnp.rs"));
}

#[allow(dead_code)]
mod result_capnp {
    include!(concat!(env!("OUT_DIR"), "/result_capnp.rs"));
}

pub use capnp::Household;
