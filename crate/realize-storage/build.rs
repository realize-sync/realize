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
        .file("capnp/mark.capnp")
        .file("capnp/real.capnp")
        .file("capnp/unreal.capnp")
        // keep files sorted
        .run()?;
    Ok(())
}
