fn main() {
    capnpc::CompilerCommand::new()
        .src_prefix("capnp")
        .import_path("capnp")
        .file("capnp/testing/hello.capnp")
        .run()
        .expect("capnpc failed");
}
