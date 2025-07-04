use super::real::index::RealIndexBlocking;
use super::unreal::UnrealCacheBlocking;
use crate::model::Arena;
use std::collections::HashSet;
use std::sync::{Arc, Mutex};
use tokio::sync::mpsc;

pub fn in_memory_cache() -> anyhow::Result<UnrealCacheBlocking> {
    let cache = UnrealCacheBlocking::new(
        redb::Builder::new().create_with_backend(redb::backends::InMemoryBackend::new())?,
    )?;

    Ok(cache)
}

pub fn in_memory_index(arena: Arena) -> anyhow::Result<RealIndexBlocking> {
    let index = RealIndexBlocking::new(
        arena,
        redb::Builder::new().create_with_backend(redb::backends::InMemoryBackend::new())?,
    )?;

    Ok(index)
}
