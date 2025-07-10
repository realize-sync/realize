//! RealStoreService server implementation for Realize - Symmetric File Syncer
//!
//! This module implements the RealStoreService trait, providing directory management,
//! file operations, and in-process server/client utilities. It is robust to interruptions
//! and supports secure, restartable sync.

use realize_types;
use realize_types::{Arena, ByteRange, Hash};
use crate::network::Server;
use crate::rpc::realstore::metrics::{MetricsRealizeClient, MetricsRealizeServer};
use crate::rpc::realstore::{
    Config, RealStoreService, RealStoreServiceClient, RealStoreServiceRequest,
    RealStoreServiceResponse,
};
use crate::storage::RealStoreOptions;
use crate::storage::{RealStore, RealStoreError, SyncedFile};
use async_speed_limit::Limiter;
use async_speed_limit::clock::StandardClock;
use futures::StreamExt;
use tarpc::client::RpcError;
use tarpc::client::stub::Stub;
use tarpc::context;
use tarpc::server::Channel;
use tarpc::tokio_serde::formats::Bincode;

/// Type shortcut for client type.
pub type InProcessRealStoreServiceClient = RealStoreServiceClient<InProcessStub>;

/// Creates a in-process client that works on the given directories.
pub fn create_inprocess_client(storage: RealStore) -> InProcessRealStoreServiceClient {
    RealStoreServer::new(storage).into_inprocess_client()
}

pub struct InProcessStub {
    inner: MetricsRealizeClient<
        tarpc::client::Channel<RealStoreServiceRequest, RealStoreServiceResponse>,
    >,
}

impl Stub for InProcessStub {
    type Req = RealStoreServiceRequest;
    type Resp = RealStoreServiceResponse;

    async fn call(
        &self,
        ctx: context::Context,
        request: RealStoreServiceRequest,
    ) -> std::result::Result<RealStoreServiceResponse, RpcError> {
        self.inner.call(ctx, request).await
    }
}

pub fn register(server: &mut Server, realize_storage: RealStore) {
    server.register_server(super::TAG, move |_peer, limiter, framed| {
        let storage = realize_storage.clone();
        let transport = tarpc::serde_transport::new(framed, Bincode::default());
        let server = RealStoreServer::new_limited(storage, limiter);
        let serve_fn = MetricsRealizeServer::new(RealStoreServer::serve(server.clone()));

        tarpc::server::BaseChannel::with_defaults(transport).execute(serve_fn)
    });
}

#[derive(Clone)]
pub(crate) struct RealStoreServer {
    pub(crate) store: RealStore,
    pub(crate) limiter: Option<Limiter<StandardClock>>,
}

impl RealStoreServer {
    pub(crate) fn new(storage: RealStore) -> Self {
        Self {
            store: storage,
            limiter: None,
        }
    }

    pub(crate) fn new_limited(storage: RealStore, limiter: Limiter<StandardClock>) -> Self {
        Self {
            store: storage,
            limiter: Some(limiter),
        }
    }

    /// Create an in-process RealStoreServiceClient for this server instance.
    pub(crate) fn into_inprocess_client(self) -> InProcessRealStoreServiceClient {
        let (client_transport, server_transport) = tarpc::transport::channel::unbounded();
        let server = tarpc::server::BaseChannel::with_defaults(server_transport);
        tokio::spawn(
            server
                .execute(MetricsRealizeServer::new(RealStoreServer::serve(self)))
                .for_each(|fut| async move {
                    tokio::spawn(fut);
                }),
        );
        let client = tarpc::client::new(tarpc::client::Config::default(), client_transport).spawn();
        let stub = InProcessStub {
            inner: MetricsRealizeClient::new(client),
        };

        RealStoreServiceClient::from(stub)
    }
}

impl RealStoreService for RealStoreServer {
    // IMPORTANT: Use async tokio::fs operations or use
    // spawn_blocking, do *not* use blocking std::fs operations
    // outside of spawn_blocking.

    async fn list(
        self,
        _: tarpc::context::Context,
        arena: Arena,
        options: RealStoreOptions,
    ) -> Result<Vec<SyncedFile>, RealStoreError> {
        self.store.list(&arena, &options).await
    }

    async fn read(
        self,
        _: tarpc::context::Context,
        arena: Arena,
        relative_path: realize_types::Path,
        range: ByteRange,
        options: RealStoreOptions,
    ) -> Result<Vec<u8>, RealStoreError> {
        self.store
            .read(&arena, &relative_path, &range, &options)
            .await
    }

    async fn send(
        self,
        _: tarpc::context::Context,
        arena: Arena,
        relative_path: realize_types::Path,
        range: ByteRange,
        data: Vec<u8>,
        options: RealStoreOptions,
    ) -> Result<(), RealStoreError> {
        self.store
            .send(&arena, &relative_path, &range, data, &options)
            .await
    }

    async fn finish(
        self,
        _: tarpc::context::Context,
        arena: Arena,
        relative_path: realize_types::Path,
        options: RealStoreOptions,
    ) -> Result<(), RealStoreError> {
        self.store.finish(&arena, &relative_path, &options).await
    }

    async fn hash(
        self,
        _ctx: tarpc::context::Context,
        arena: Arena,
        relative_path: realize_types::Path,
        range: ByteRange,
        options: RealStoreOptions,
    ) -> Result<Hash, RealStoreError> {
        self.store
            .hash(&arena, &relative_path, &range, &options)
            .await
    }

    async fn delete(
        self,
        _ctx: tarpc::context::Context,
        arena: Arena,
        relative_path: realize_types::Path,
        options: RealStoreOptions,
    ) -> Result<(), RealStoreError> {
        self.store.delete(&arena, &relative_path, &options).await
    }

    async fn calculate_signature(
        self,
        _ctx: tarpc::context::Context,
        arena: Arena,
        relative_path: realize_types::Path,
        range: ByteRange,
        options: RealStoreOptions,
    ) -> Result<realize_types::Signature, RealStoreError> {
        self.store
            .calculate_signature(&arena, &relative_path, &range, &options)
            .await
    }

    async fn diff(
        self,
        _ctx: tarpc::context::Context,
        arena: Arena,
        relative_path: realize_types::Path,
        range: ByteRange,
        signature: realize_types::Signature,
        options: RealStoreOptions,
    ) -> Result<(realize_types::Delta, Hash), RealStoreError> {
        self.store
            .diff(&arena, &relative_path, &range, signature, &options)
            .await
    }

    async fn apply_delta(
        self,
        _ctx: tarpc::context::Context,
        arena: Arena,
        relative_path: realize_types::Path,
        range: ByteRange,
        delta: realize_types::Delta,
        hash: Hash,
        options: RealStoreOptions,
    ) -> Result<(), RealStoreError> {
        self.store
            .apply_delta(&arena, &relative_path, &range, delta, &hash, &options)
            .await
    }

    async fn truncate(
        self,
        _ctx: tarpc::context::Context,
        arena: Arena,
        relative_path: realize_types::Path,
        file_size: u64,
        options: RealStoreOptions,
    ) -> Result<(), RealStoreError> {
        self.store
            .truncate(&arena, &relative_path, file_size, &options)
            .await
    }

    async fn configure(
        self,
        _ctx: tarpc::context::Context,
        config: crate::rpc::realstore::Config,
    ) -> Result<crate::rpc::realstore::Config, RealStoreError> {
        if let (Some(limiter), Some(limit)) = (self.limiter.as_ref(), config.write_limit) {
            limiter.set_speed_limit(limit as f64);
        }
        Ok(Config {
            write_limit: self.limiter.as_ref().and_then(|l| {
                let lim = l.speed_limit();
                if lim.is_finite() {
                    Some(lim as u64)
                } else {
                    None
                }
            }),
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use assert_fs::TempDir;
    use std::path::PathBuf;

    #[tokio::test]
    async fn tarpc_rpc_inprocess() -> anyhow::Result<()> {
        let temp = TempDir::new()?;
        let client =
            create_inprocess_client(RealStore::single(&Arena::from("testdir"), temp.path()));
        let list = client
            .list(
                tarpc::context::current(),
                Arena::from("testdir"),
                RealStoreOptions::default(),
            )
            .await??;
        assert_eq!(list.len(), 0);

        Ok(())
    }

    #[tokio::test]
    async fn configure_noop_returns_none() {
        let server = RealStoreServer::new(RealStore::single(
            &Arena::from("testdir"),
            &PathBuf::from("/tmp/testdir"),
        ));
        let returned = server
            .clone()
            .configure(
                tarpc::context::current(),
                Config {
                    write_limit: Some(12345),
                },
            )
            .await
            .unwrap();
        assert_eq!(returned.write_limit, None);
    }

    #[tokio::test]
    async fn configure_limited_sets_and_returns_limit() {
        let dirs = RealStore::single(&Arena::from("testdir"), &PathBuf::from("/tmp/testdir"));
        let limiter = Limiter::<StandardClock>::new(f64::INFINITY);
        let server = RealStoreServer::new_limited(dirs, limiter.clone());
        let limit = 55555u64;
        let returned = server
            .clone()
            .configure(
                tarpc::context::current(),
                Config {
                    write_limit: Some(limit),
                },
            )
            .await
            .unwrap();
        assert_eq!(returned.write_limit, Some(limit));
    }
}
