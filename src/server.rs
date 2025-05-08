use crate::model::service::RealizeService;
use std::path::PathBuf;

// Dummy type definitions for compilation
pub type DirectoryId = String;
pub type ByteRange = (u64, u64);
pub type Result<T> = std::result::Result<T, ()>;

pub struct DummyService;

impl RealizeService for DummyService {
    // List files in a directory
    fn list(&self, _dir_id: DirectoryId) -> Result<Vec<SyncedFile>> {
        Ok(vec![])
    }

    // Send a byte range of a file
    fn send(&self, _dir_id: DirectoryId, _path: PathBuf, _range: ByteRange, _data: Vec<u8>) -> Result<()> {
        Ok(())
    }

    // Mark a partial file as complete
    fn finish(&self, _dir_id: DirectoryId, _path: PathBuf) -> Result<()> {
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tarpc::context;
    use tarpc::server::{self, Channel};
    use tarpc::transport::channel;
    use futures::prelude::*;
    use tokio::runtime::Runtime;

    #[tokio::test]
    async fn test_dummy_service_inprocess_channel() {
        // Create the server
        let server = DummyService;
        let (client_transport, server_transport) = channel::unbounded();

        // Spawn the server
        tokio::spawn(async move {
            server::BaseChannel::with_defaults(server_transport)
                .execute(server)
                .await;
        });

        // Create a client (just call the trait directly for now)
        // In a real tarpc service, you'd use a generated client stub.
        // Here, we just call the DummyService directly for demonstration.
        let result = server.list("test_dir".to_string());
        assert_eq!(result.unwrap(), vec![]);
    }
} 