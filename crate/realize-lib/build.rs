use std::process;

fn main() {
    match build_all() {
        Ok(_) => process::exit(0),
        Err(err) => {
            eprintln!("{err:?}");
            process::exit(1);
        }
    }
}

fn build_all() -> anyhow::Result<()> {
    capnpc()?;

    Ok(())
}

fn capnpc() -> anyhow::Result<()> {
    capnpc::CompilerCommand::new()
        .src_prefix("capnp")
        .import_path("capnp")
        .file("capnp/network/rpc/peer.capnp")
        .file("capnp/network/rpc/read.capnp")
        .file("capnp/network/rpc/result.capnp")
        .file("capnp/network/rpc/store.capnp")
        .file("capnp/network/testing/hello.capnp")
        .file("capnp/storage/real.capnp")
        .file("capnp/storage/unreal.capnp")
        // keep files sorted
        .run()?;
    Ok(())
}
