use assert_fs::TempDir;
use assert_fs::prelude::*;
use realize_core::config::Config;
use realize_core::setup::SetupHelper;
use realize_storage::config::{ArenaConfig, CacheConfig};
use realize_types::{Arena, Peer};
use std::path::PathBuf;
use std::time::Duration;
use tokio::process::Command;
use tokio::task::LocalSet;

fn command_path(cmd: &str) -> PathBuf {
    // Expecting a path for the current exe to look like
    // target/debug/deps/integration_test
    std::env::current_exe()
        .unwrap()
        .parent()
        .unwrap()
        .parent()
        .unwrap()
        .join(cmd)
}

struct Fixture {
    _tempdir: TempDir,
    socket: PathBuf,
    _setup: SetupHelper,
}

impl Fixture {
    pub async fn setup(local: &LocalSet) -> anyhow::Result<Self> {
        let _ = env_logger::try_init();

        let mut config = Config::new();
        let arena = Arena::from("myarena");

        // Setup temp directory for the daemon to serve
        let tempdir = TempDir::new()?;

        let myarena = tempdir.child("myarena");
        myarena.create_dir_all()?;

        config.storage.cache = CacheConfig {
            db: tempdir.child("cache.db").to_path_buf(),
        };

        // Configure arena with required cache and optional local path
        config.storage.arenas.insert(
            arena,
            ArenaConfig::new(
                myarena.to_path_buf(),
                tempdir.child("myarena-cache.db").to_path_buf(),
                tempdir.child("myarena-blobs").to_path_buf(),
            ),
        );

        let resources = PathBuf::from(std::env::var("CARGO_MANIFEST_DIR").unwrap())
            .join("../../resources/test");

        // Add a simple peer configuration
        config.network.peers.insert(
            Peer::from("a"),
            realize_network::config::PeerConfig {
                address: None,
                pubkey: std::fs::read_to_string(resources.join("a-spki.pem"))?,
            },
        );

        let server_privkey = resources.join("a.key");
        let socket = tempdir.path().join("realize/control.socket");
        let setup = SetupHelper::setup(config, &server_privkey, local).await?;
        setup.bind_control_socket(local, Some(&socket)).await?;

        Ok(Self {
            _tempdir: tempdir,
            socket,
            _setup: setup,
        })
    }

    pub fn control_command(&self, args: &[&str]) -> anyhow::Result<Command> {
        let mut cmd = tokio::process::Command::new(command_path("realize-control"));
        cmd.arg("--socket")
            .arg(&self.socket)
            .args(args)
            .env("RUST_LOG", "realize_")
            .kill_on_drop(true);

        Ok(cmd)
    }
}

#[tokio::test]
async fn churten_is_running() -> anyhow::Result<()> {
    let local = LocalSet::new();
    let fixture = Fixture::setup(&local).await?;

    local
        .run_until(async move {
            // Test the control command with timeout
            let mut control_cmd = fixture.control_command(&["churten", "is-running"])?;
            let output =
                tokio::time::timeout(Duration::from_secs(3), control_cmd.output()).await??;
            if !output.status.success() {
                panic!("Control command failed: {output:?}");
            }

            // Check that the output is either "true" or "false"
            let output_str = String::from_utf8(output.stdout)?;
            let output_str = output_str.trim();
            assert!(
                output_str == "true" || output_str == "false",
                "Expected 'true' or 'false', got '{}'",
                output_str
            );

            Ok::<_, anyhow::Error>(())
        })
        .await?;
    Ok(())
}

#[tokio::test]
async fn churten_is_running_quiet() -> anyhow::Result<()> {
    let local = LocalSet::new();
    let fixture = Fixture::setup(&local).await?;

    local
        .run_until(async move {
            // Test the quiet mode
            let output = fixture
                .control_command(&["churten", "is-running", "-q"])?
                .output()
                .await?;

            // Quiet mode should exit with 0 or 10, not print anything
            assert!(
                output.status.code() == Some(0) || output.status.code() == Some(10),
                "Expected exit code 0 or 10, got {}",
                output.status
            );

            let output_str = String::from_utf8(output.stdout)?;
            assert!(
                output_str.is_empty(),
                "Expected empty output in quiet mode, got '{}'",
                output_str
            );

            Ok::<_, anyhow::Error>(())
        })
        .await?;
    Ok(())
}
