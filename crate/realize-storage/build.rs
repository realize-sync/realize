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
        .file("capnp/arena/blob.capnp")
        .file("capnp/arena/cache.capnp")
        .file("capnp/arena/engine.capnp")
        .file("capnp/arena/history.capnp")
        .file("capnp/arena/mark.capnp")
        .file("capnp/global/cache.capnp")
        // keep files sorted
        .run()?;
    Ok(())
}
