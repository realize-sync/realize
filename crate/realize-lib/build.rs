use std::process;

fn main() {
    match compile() {
        Ok(_) => process::exit(0),
        Err(err) => {
            eprintln!("{err:?}");
            process::exit(1);
        }
    }
}

fn compile() -> anyhow::Result<()> {
    capnpc::CompilerCommand::new()
        .src_prefix("capnp")
        .default_parent_module(
            "storage::unreal"
                .split("::")
                .map(|s| s.to_string())
                .collect(),
        )
        .file("capnp/unreal.capnp")
        .run()?;

    Ok(())
}
