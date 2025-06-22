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
    storage_capnpc()?;
    rpc_capnpc()?;

    Ok(())
}

fn storage_capnpc() -> anyhow::Result<()> {
    capnpc::CompilerCommand::new()
        .src_prefix("capnp/storage")
        .default_parent_module(module_name_as_vec("storage::unreal"))
        .file("capnp/storage/unreal.capnp")
        .run()?;

    Ok(())
}

fn rpc_capnpc() -> anyhow::Result<()> {
    capnpc::CompilerCommand::new()
        .src_prefix("capnp/network/rpc")
        .default_parent_module(module_name_as_vec("network::rpc"))
        .file("capnp/network/rpc/peer.capnp")
        .file("capnp/network/rpc/result.capnp")
        .file("capnp/network/rpc/realstore.capnp")
        .run()?;

    Ok(())
}

fn module_name_as_vec(module_name: &str) -> Vec<String> {
    module_name.split("::").map(|s| s.to_string()).collect()
}
