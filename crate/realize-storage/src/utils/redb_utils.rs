use std::sync::Arc;

/// Open a redb database (async call).
pub async fn open(path: &std::path::Path) -> anyhow::Result<Arc<redb::Database>> {
    let path = path.to_path_buf();

    Ok(Arc::new(
        tokio::task::spawn_blocking(move || redb::Database::create(path)).await??,
    ))
}

// Create an in-memory database, for testing.
#[cfg(any(test, feature = "testing"))]
pub fn in_memory() -> anyhow::Result<Arc<redb::Database>> {
    Ok(Arc::new(redb::Builder::new().create_with_backend(
        redb::backends::InMemoryBackend::new(),
    )?))
}
