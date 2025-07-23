/// Open a redb database (async call).
pub async fn open(path: &std::path::Path) -> anyhow::Result<redb::Database> {
    let path = path.to_path_buf();

    Ok(tokio::task::spawn_blocking(move || redb::Database::create(path)).await??)
}

// Create an in-memory database, for testing.
#[cfg(any(test, feature = "testing"))]
pub fn in_memory() -> anyhow::Result<redb::Database> {
    Ok(redb::Builder::new().create_with_backend(redb::backends::InMemoryBackend::new())?)
}
