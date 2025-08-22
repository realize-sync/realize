use super::Household;
use assert_fs::TempDir;
use assert_fs::prelude::*;
use realize_network::Server;
use realize_network::capnp::PeerStatus;
use realize_network::hostport::HostPort;
use realize_network::testing::TestingPeers;
use realize_storage::Blob;
use realize_storage::Storage;
use realize_storage::utils::hash;
use realize_storage::{self, GlobalCache};
use realize_types::Path;
use realize_types::{Arena, Hash, Peer};
use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;
use tokio::task::LocalSet;
use tokio::time::timeout;
use tokio_retry::strategy::FixedInterval;

/// Create up to 3 inter-connected [Household]s for testing, with one
/// arena.
///
/// The peers are [TestingHouseholds::a] [TestingHouseholds::b] and
/// [TestingHouseholds::c]. The arena is
/// [TestingHouseholds::test_arena].
pub struct HouseholdFixture {
    pub peers: TestingPeers,
    peer_storage: HashMap<Peer, Arc<Storage>>,
    tempdir: TempDir,
    servers: Vec<Arc<Server>>,
}

impl HouseholdFixture {
    pub fn a() -> Peer {
        TestingPeers::a()
    }
    pub fn b() -> Peer {
        TestingPeers::b()
    }
    pub fn c() -> Peer {
        TestingPeers::c()
    }
    pub fn test_arena() -> Arena {
        Arena::from("myarena")
    }

    /// Setup the fixture.
    pub async fn setup() -> anyhow::Result<Self> {
        let _ = env_logger::try_init();

        let tempdir = TempDir::new()?;
        let mut peer_storage = HashMap::new();
        for peer in [
            HouseholdFixture::a(),
            HouseholdFixture::b(),
            HouseholdFixture::c(),
        ] {
            let s = realize_storage::testing::storage(
                tempdir.child(peer.as_str()).path(),
                [HouseholdFixture::test_arena()],
            )
            .await?;
            peer_storage.insert(peer, s);
        }
        Ok(Self {
            peers: TestingPeers::new()?,
            peer_storage,
            tempdir,
            servers: vec![],
        })
    }

    /// Run a test with two peers, [HouseholdFixture::a] and
    /// [HouseholdFixture::b] running and inter-connected.
    pub async fn with_two_peers(&mut self) -> anyhow::Result<WithTwoPeers> {
        let a = HouseholdFixture::a();
        let b = HouseholdFixture::b();
        let addr_a = self.pick_port(a)?;
        let addr_b = self.pick_port(b)?;
        assert!(addr_a != addr_b);

        let local = LocalSet::new();

        let household_a = self.create_household(&local, a)?;
        let mut server_a = Server::new(self.peers.networking(a)?);
        household_a.register(&mut server_a);
        let server_a = Arc::new(server_a);
        self.servers.push(Arc::clone(&server_a));

        let household_b = self.create_household(&local, b)?;
        let mut server_b = Server::new(self.peers.networking(b)?);
        household_b.register(&mut server_b);
        let server_b = Arc::new(server_b);
        self.servers.push(Arc::clone(&server_b));

        Ok(WithTwoPeers {
            local,
            addr_a,
            household_a,
            server_a,
            addr_b,
            household_b,
            server_b,
            connected: false,
        })
    }

    /// Path where files for the test arena of the given peer are stored.
    pub fn arena_root(&self, peer: Peer) -> PathBuf {
        realize_storage::testing::arena_root(
            self.tempdir.child(peer.as_str()).path(),
            HouseholdFixture::test_arena(),
        )
    }

    /// Get a peer storage.
    pub fn storage(&self, peer: Peer) -> anyhow::Result<&Arc<Storage>> {
        self.peer_storage
            .get(&peer)
            .ok_or(anyhow::anyhow!("Unknown peer {peer}"))
    }

    /// Get a peer cache.
    pub fn cache(&self, peer: Peer) -> anyhow::Result<&Arc<GlobalCache>> {
        Ok(self.storage(peer)?.cache())
    }

    /// Wait for the given file to appear in the given peer's cache, in the test arena.
    ///
    /// This function waits for a specific version of the file (identified by hash) to avoid
    /// flaky tests that see intermediate states on Linux.
    pub async fn wait_for_file_in_cache(
        &self,
        peer: Peer,
        filename: &str,
        hash: &Hash,
    ) -> anyhow::Result<()> {
        let cache = self.cache(peer)?;

        let mut retry = FixedInterval::new(Duration::from_millis(50)).take(100);
        let arena = HouseholdFixture::test_arena();
        let path = Path::parse(filename)?;

        // First, wait for the file to appear in the cache
        let inode = loop {
            match cache.expect(arena, &path).await {
                Ok(inode) => break inode,
                Err(_) => {
                    if let Some(delay) = retry.next() {
                        tokio::time::sleep(delay).await;
                    } else {
                        panic!("[arena]/{filename} was never added to the cache of {peer}");
                    }
                }
            }
        };

        // Then, wait for the file to have the expected hash
        let goal = Some(hash.clone());
        while cache.file_availability(inode).await.ok().map(|e| e.hash) != goal {
            if let Some(delay) = retry.next() {
                tokio::time::sleep(delay).await;
            } else {
                panic!(
                    "[arena]/{filename} in the cache of {peer} never became {} (current: {})",
                    *hash,
                    cache.file_availability(inode).await?.hash
                );
            }
        }

        Ok(())
    }

    pub async fn wait_for_file_version_in_cache(
        &self,
        peer: Peer,
        filename: &str,
        hash: &Hash,
    ) -> anyhow::Result<()> {
        let cache = &self.cache(peer)?;

        let mut retry = FixedInterval::new(Duration::from_millis(50)).take(100);
        let arena = HouseholdFixture::test_arena();
        let inode = cache.expect(arena, &Path::parse(filename)?).await?;
        while cache.file_availability(inode).await?.hash != *hash {
            if let Some(delay) = retry.next() {
                tokio::time::sleep(delay).await;
            } else {
                panic!(
                    "[arena]/{filename} never became {} (current: {})",
                    *hash,
                    cache.file_availability(inode).await?.hash
                );
            }
        }

        Ok(())
    }

    pub async fn write_file(
        &self,
        peer: Peer,
        path_str: &str,
        content: &str,
    ) -> anyhow::Result<(Path, Hash)> {
        let root = self.arena_root(peer);
        let path = Path::parse(path_str)?;
        let realpath = path.within(&root);
        tokio::fs::write(realpath, content).await?;

        Ok((path, hash::digest(content)))
    }

    pub async fn open_file(&self, peer: Peer, path_str: &str) -> anyhow::Result<Blob> {
        let cache = self.cache(peer)?;
        let inode = cache
            .expect(HouseholdFixture::test_arena(), &Path::parse(path_str)?)
            .await?;

        Ok(cache.open_file(inode).await?)
    }

    // Pick a port for the given peer and store it in the network
    // configuration. Call this before calling houshold() to allow
    // households to communicate.
    ///
    /// This is a lower-level call that's usually not called directly. Prefer calling with_two_peers or with_three_peers
    pub fn pick_port(&mut self, peer: Peer) -> anyhow::Result<HostPort> {
        self.peers.pick_port(peer)
    }

    /// Create a household for the given peer.
    ///
    /// This just spawns the Household on the LocalSet. The LocalSet
    /// must be run for the household to do something.
    ///
    /// This is a lower-level call that's usually not called directly. Prefer calling with_two_peers or with_three_peers
    pub fn create_household(&self, local: &LocalSet, peer: Peer) -> anyhow::Result<Household> {
        let storage = self
            .peer_storage
            .get(&peer)
            .ok_or_else(|| anyhow::anyhow!("unknown peer: {peer}"))?;

        Household::spawn(local, self.peers.networking(peer)?, storage.clone())
    }
}

/// Connect from `household` to `peer`.
///
/// Assumes that the household is not already connected to the peer.
pub async fn connect(household: &Household, peer: Peer) -> anyhow::Result<()> {
    let mut status = household.peer_status();
    household.keep_peer_connected(peer)?;

    let delay = Duration::from_secs(3);
    assert_eq!(
        PeerStatus::Connected(peer),
        timeout(delay, status.recv())
            .await
            .map_err(|_| anyhow::anyhow!("timed out waiting to connect to {peer}"))??
    );

    Ok(())
}

/// Disconnects from `household` to `peer`.
///
/// Assumes that the household is connected to the peer.
pub async fn disconnect(household: &Household, peer: Peer) -> anyhow::Result<()> {
    let mut status = household.peer_status();
    household.disconnect_peer(peer)?;

    let delay = Duration::from_secs(3);
    assert_eq!(
        PeerStatus::Disconnected(peer),
        timeout(delay, status.recv())
            .await
            .map_err(|_| anyhow::anyhow!("timed out waiting to disconnect from {peer}"))??
    );

    Ok(())
}

pub struct WithTwoPeers {
    local: LocalSet,
    household_a: Household,
    addr_a: HostPort,
    server_a: Arc<Server>,
    addr_b: HostPort,
    household_b: Household,
    server_b: Arc<Server>,
    connected: bool,
}

impl WithTwoPeers {
    pub fn interconnected(mut self) -> Self {
        self.connected = true;

        self
    }

    pub async fn run(
        self,
        test: impl AsyncFnOnce(Household, Household) -> anyhow::Result<()>,
    ) -> anyhow::Result<()> {
        let Self {
            local,
            household_a,
            addr_a,
            server_a,
            household_b,
            addr_b,
            server_b,
            connected,
        } = self;
        local
            .run_until(async move {
                let a = HouseholdFixture::a();
                let b = HouseholdFixture::b();

                server_a.listen(&addr_a).await?;
                server_b.listen(&addr_b).await?;

                if connected {
                    connect(&household_a, b).await?;
                    connect(&household_b, a).await?;
                }

                test(household_a, household_b).await
            })
            .await?;

        Ok(())
    }
}
