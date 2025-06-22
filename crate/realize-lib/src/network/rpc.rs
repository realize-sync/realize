pub mod history;
pub mod realstore;

mod peer_capnp {
    include!(concat!(env!("OUT_DIR"), "/peer_capnp.rs"));
}
mod realstore_capnp {
    include!(concat!(env!("OUT_DIR"), "/realstore_capnp.rs"));
}
mod result_capnp {
    include!(concat!(env!("OUT_DIR"), "/result_capnp.rs"));
}
