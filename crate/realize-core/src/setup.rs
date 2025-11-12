use super::config::Config;
use crate::consensus::churten::Churten;
use crate::fs::downloader::Downloader;
use crate::fs::fuse::{self, FuseHandle};
use crate::rpc::Household;
use crate::rpc::control::server::ControlServer;
use anyhow::Context;
use realize_network::{Networking, Server, unixsocket};
use realize_storage::Storage;
use std::os::unix::fs::PermissionsExt;
use std::path::PathBuf;
use std::sync::Arc;
use tokio::fs;
use tokio::task::LocalSet;
use tokio_util::sync::CancellationToken;

pub struct SetupHelper {
    pub networking: Networking,
    pub storage: Arc<Storage>,
    pub household: Arc<Household>,
}

impl SetupHelper {
    pub async fn setup(
        config: Config,
        privkey: &std::path::Path,
        local: &LocalSet,
    ) -> anyhow::Result<Self> {
        let networking = Networking::from_config(&config.network.peers, privkey)?;
        let storage = Storage::from_config(&config.storage).await?;
        log::info!(
            "Cached arenas: {:?}",
            storage
                .cache()
                .arenas()
                .map(|a| a.as_str())
                .collect::<Vec<_>>()
                .join(", ")
        );
        log::info!(
            "Indexed arenas: {:?}",
            storage
                .arenas()
                .iter()
                .map(|a| a.as_str())
                .collect::<Vec<_>>()
                .join(", ")
        );
        let household = Household::spawn(local, networking.clone(), storage.clone())?;

        Ok(Self {
            networking,
            storage,
            household,
        })
    }

    /// Mount a FUSE filesystem at the given mountpoint.
    ///
    /// The returned object must be kept to keep the filesytem mounted. Call join() on it to
    pub async fn export_fuse(
        &self,
        mountpoint: &std::path::Path,
        umask: u16,
    ) -> anyhow::Result<FuseHandle> {
        let downloader = Downloader::new(self.household.clone());
        let handle = fuse::export(
            Arc::clone(self.storage.cache()),
            downloader,
            mountpoint,
            umask,
        )?;
        log::info!("FUSE filesystem mounted on {mountpoint:?}");

        Ok(handle)
    }

    /// Bind to a UNIX socket that allows the owning user to control the server
    pub async fn bind_control_socket(
        &self,
        local: &LocalSet,
        path: Option<&std::path::Path>,
        umask: u32,
    ) -> anyhow::Result<CancellationToken> {
        let path = match path {
            Some(p) => p.to_path_buf(),
            None => {
                if let Some(path) = default_control_socket_path().await {
                    path
                } else {
                    anyhow::bail!(
                        "No appropriate default location for a unix socket. Please specify a path."
                    );
                }
            }
        };

        let token = CancellationToken::new();
        let churten = Churten::new(Arc::clone(&self.storage), self.household.clone());
        let control_server =
            ControlServer::new(Arc::clone(&self.storage), churten, self.household.clone());
        unixsocket::bind(
            local,
            path.as_ref(),
            umask,
            move || control_server.clone().into_client().client,
            token.clone(),
        )
        .await
        .with_context(|| format!("binding socket at {path:?}"))?;

        log::info!("Control socket created at {path:?}");

        Ok(token)
    }

    /// Setup server as specified in the configuration.
    ///
    /// The returned server is configured, but not started.
    pub async fn setup_server(self) -> anyhow::Result<Arc<Server>> {
        let SetupHelper {
            networking,
            household,
            ..
        } = self;

        let mut server = Server::new(networking.clone());
        household.keep_all_connected()?;
        household.register(&mut server);

        Ok(Arc::new(server))
    }
}

/// Look for a reasonable default path for the control socket.
pub async fn default_control_socket_path() -> Option<PathBuf> {
    for pathstr in ["/run", "/var/run", "/tmp"] {
        let path = PathBuf::from(pathstr);
        if let Ok(m) = fs::metadata(&path).await
            && m.is_dir()
        {
            let dir = path.join("realize");
            if !make_private_dir(&dir).await.is_ok() {
                continue;
            }
            return Some(dir.join("control.socket"));
        }
    }

    None
}

async fn make_private_dir(dir: &std::path::Path) -> std::io::Result<()> {
    if !fs::metadata(dir).await.is_ok() {
        fs::create_dir(dir).await?;
        let mut permissions = fs::metadata(dir).await?.permissions();
        permissions.set_mode(0o700);
        fs::set_permissions(dir, permissions).await?;
    }

    Ok(())
}
