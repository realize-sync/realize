use async_speed_limit::Limiter;

/// Wait as long as needed until the rate limit allows sending a
/// message of the given size.
pub(crate) async fn apply(limiter: &Option<Limiter>, size: capnp::Result<::capnp::MessageSize>) {
    if let (Ok(size), Some(limiter)) = (size, limiter) {
        limiter.consume(size_to_bytes(size)).await;
    }
}

/// Convert a [MessageSize] to a byte count
fn size_to_bytes(size: capnp::MessageSize) -> usize {
    (size.word_count as usize + size.cap_count as usize) * 8
}

#[cfg(test)]
mod tests {
    use crate::rpc::household;
    use crate::rpc::household::testing;
    use crate::rpc::testing::HouseholdFixture;
    use realize_network::{
        capnp::{ConnectionHandler, ConnectionTracker},
        testing::TestingPeers,
    };
    use realize_types::{ByteRange, Path, Signature};
    use std::{cell::RefCell, rc::Rc, sync::Arc, time::Duration};
    use tokio::{
        sync::{broadcast, mpsc, oneshot},
        task::LocalSet,
    };

    #[tokio::test]
    async fn apply_rate_limit() -> anyhow::Result<()> {
        // This test uses a minimal fake server to test that the
        // client-side of Household makes the correct calls to setup a
        // rate-limited store and use it for the subscriber as well as
        // when execution mode is batch.

        let mut fixture = HouseholdFixture::setup().await?;
        let a = TestingPeers::a();
        let b = TestingPeers::b();
        let arena = HouseholdFixture::test_arena();
        let path = Path::parse("test.txt")?;
        fixture.peers.set_batch_rate_limit(b, 1024);

        let (tx, rx) = mpsc::channel(128);
        let (connection_tx, _) = broadcast::channel(128);
        let handler = household::PeerConnectionHandler::new(
            Arc::clone(fixture.storage(a)?),
            &fixture.peers.networking(a)?,
            rx,
            connection_tx,
        );

        let local = LocalSet::new();
        local
            .run_until(async move {
                let calls = Rc::new(RefCell::new(vec![]));
                let tracker = handler.create_tracker().await;
                tracker
                    .register(
                        b,
                        capnp_rpc::new_client(testing::FakeConnectedPeer(calls.clone())),
                    )
                    .await?;

                assert_eq!(
                    vec![
                        "ConnectedPeer.store()".to_string(),
                        "Store.with_rate_limit(1024)".to_string(),
                        "Store.subscriptions() rate_limit=Some(1024.0)".to_string(),
                    ],
                    calls.borrow().clone()
                );
                calls.borrow_mut().clear();

                let (read_tx, mut read_rx) = mpsc::channel(10);
                tx.send(household::HouseholdOperation::Read {
                    peers: vec![b],
                    mode: household::ExecutionMode::Batch,
                    arena,
                    path: path.clone(),
                    offset: 0,
                    limit: None,
                    tx: read_tx.clone(),
                })
                .await?;
                assert!(
                    tokio::time::timeout(Duration::from_secs(3), read_rx.recv())
                        .await?
                        .is_some()
                );
                assert_eq!(
                    vec!["Store.read() rate_limit=Some(1024.0)".to_string(),],
                    calls.borrow().clone()
                );
                calls.borrow_mut().clear();

                tx.send(household::HouseholdOperation::Read {
                    peers: vec![b],
                    mode: household::ExecutionMode::Interactive,
                    arena,
                    path: path.clone(),
                    offset: 0,
                    limit: None,
                    tx: read_tx.clone(),
                })
                .await?;
                assert!(
                    tokio::time::timeout(Duration::from_secs(3), read_rx.recv())
                        .await?
                        .is_some()
                );
                assert_eq!(
                    vec!["Store.read() rate_limit=None".to_string(),],
                    calls.borrow().clone()
                );
                calls.borrow_mut().clear();

                let (rsync_tx, rsync_rx) = oneshot::channel();
                tx.send(household::HouseholdOperation::Rsync {
                    peers: vec![b],
                    mode: household::ExecutionMode::Batch,
                    tx: rsync_tx,
                    arena,
                    path: path.clone(),
                    range: ByteRange::new(0, 100),
                    sig: Signature(vec![]),
                })
                .await?;
                rsync_rx.await??;
                assert_eq!(
                    vec!["Store.rsync() rate_limit=Some(1024.0)".to_string(),],
                    calls.borrow().clone()
                );
                calls.borrow_mut().clear();

                let (rsync_tx, rsync_rx) = oneshot::channel();
                tx.send(household::HouseholdOperation::Rsync {
                    peers: vec![b],
                    mode: household::ExecutionMode::Interactive,
                    tx: rsync_tx,
                    arena,
                    path: path.clone(),
                    range: ByteRange::new(0, 100),
                    sig: Signature(vec![]),
                })
                .await?;
                rsync_rx.await??;
                assert_eq!(
                    vec!["Store.rsync() rate_limit=None".to_string(),],
                    calls.borrow().clone()
                );
                calls.borrow_mut().clear();

                Ok::<(), anyhow::Error>(())
            })
            .await?;

        Ok(())
    }
}
