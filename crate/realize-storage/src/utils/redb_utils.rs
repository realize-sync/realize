/// Open a redb database (async call).
pub async fn open<P>(path: P) -> anyhow::Result<redb::Database>
where
    P: AsRef<std::path::Path>,
{
    let path = path.as_ref().to_path_buf();

    Ok(tokio::task::spawn_blocking(move || redb::Database::create(path)).await??)
}

// Create an in-memory database, for testing.
#[cfg(any(test, feature = "testing"))]
pub fn in_memory() -> anyhow::Result<redb::Database> {
    Ok(redb::Builder::new().create_with_backend(redb::backends::InMemoryBackend::new())?)
}
