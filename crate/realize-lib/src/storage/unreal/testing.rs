use super::UnrealCacheBlocking;

pub fn in_memory_cache() -> anyhow::Result<UnrealCacheBlocking> {
    let cache = UnrealCacheBlocking::new(
        redb::Builder::new().create_with_backend(redb::backends::InMemoryBackend::new())?,
    )?;

    Ok(cache)
}
