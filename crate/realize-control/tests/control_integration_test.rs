use assert_fs::TempDir;
use assert_fs::prelude::*;
use realize_core::config::Config;
use realize_core::setup::SetupHelper;
use realize_storage::Mark;
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
    setup: SetupHelper,
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
                pubkey: std::fs::read_to_string(resources.join("a-spki.pem"))?,
                ..Default::default()
            },
        );

        let server_privkey = resources.join("a.key");
        let socket = tempdir.path().join("realize/control.socket");
        let setup = SetupHelper::setup(config, &server_privkey, local).await?;
        setup.bind_control_socket(local, Some(&socket)).await?;

        Ok(Self {
            _tempdir: tempdir,
            socket,
            setup,
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
                .control_command(&["--output=quiet", "churten", "is-running"])?
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

#[tokio::test]
async fn churten_mark_set_arena() -> anyhow::Result<()> {
    let local = LocalSet::new();
    let fixture = Fixture::setup(&local).await?;

    local
        .run_until(async move {
            // Test setting arena mark
            let output = fixture
                .control_command(&["mark", "set", "keep", "myarena"])?
                .output()
                .await?;

            assert!(
                output.status.success(),
                "Control command failed: {output:?}"
            );

            let output_str = String::from_utf8(output.stdout)?;
            assert!(
                output_str.contains("Arena mark set successfully"),
                "Expected success message, got '{}'",
                output_str
            );

            assert_eq!(
                Mark::Keep,
                fixture
                    .setup
                    .storage
                    .get_mark(Arena::from("myarena"), &realize_types::Path::parse("any")?)
                    .await?
            );

            Ok::<_, anyhow::Error>(())
        })
        .await?;
    Ok(())
}

#[tokio::test]
async fn churten_mark_set_paths() -> anyhow::Result<()> {
    let local = LocalSet::new();
    let fixture = Fixture::setup(&local).await?;

    local
        .run_until(async move {
            // Test setting marks on specific paths
            let output = fixture
                .control_command(&["mark", "set", "own", "myarena", "file1.txt", "file2.txt"])?
                .output()
                .await?;

            assert!(
                output.status.success(),
                "Control command failed: {output:?}"
            );

            let output_str = String::from_utf8(output.stdout)?;
            assert!(
                output_str.contains("set on 2 paths"),
                "Expected success message, got '{}'",
                output_str
            );

            let arena = Arena::from("myarena");
            let storage = &fixture.setup.storage;
            assert_eq!(
                Mark::Own,
                storage
                    .get_mark(arena, &realize_types::Path::parse("file1.txt")?)
                    .await?
            );
            assert_eq!(
                Mark::Own,
                storage
                    .get_mark(arena, &realize_types::Path::parse("file2.txt")?)
                    .await?
            );
            // file3.txt wasn't set. It still has the default (watch)
            assert_eq!(
                Mark::Watch,
                storage
                    .get_mark(arena, &realize_types::Path::parse("file3.txt")?)
                    .await?
            );

            Ok::<_, anyhow::Error>(())
        })
        .await?;
    Ok(())
}

#[tokio::test]
async fn churten_mark_get() -> anyhow::Result<()> {
    let local = LocalSet::new();
    let fixture = Fixture::setup(&local).await?;

    local
        .run_until(async move {
            let arena = Arena::from("myarena");
            let storage = &fixture.setup.storage;
            storage
                .set_mark(arena, &realize_types::Path::parse("file1.txt")?, Mark::Keep)
                .await?;
            storage
                .set_mark(arena, &realize_types::Path::parse("file2.txt")?, Mark::Own)
                .await?;

            let output = fixture
                .control_command(&[
                    "mark",
                    "get",
                    "myarena",
                    "file1.txt",
                    "file2.txt",
                    "file3.txt",
                ])?
                .output()
                .await?;

            assert!(
                output.status.success(),
                "Control command failed: {output:?}"
            );

            let output_str = String::from_utf8(output.stdout)?;
            assert_eq!(
                "file1.txt: keep\nfile2.txt: own\nfile3.txt: watch\n",
                output_str
            );

            Ok::<_, anyhow::Error>(())
        })
        .await?;
    Ok(())
}

#[tokio::test]
async fn churten_mark_get_multiple_paths() -> anyhow::Result<()> {
    let local = LocalSet::new();
    let fixture = Fixture::setup(&local).await?;

    local
        .run_until(async move {
            // First set marks on multiple files
            let set_output = fixture
                .control_command(&["mark", "set", "keep", "myarena", "file1.txt", "file2.txt"])?
                .output()
                .await?;
            assert!(set_output.status.success());

            // Then get marks for multiple files
            let output = fixture
                .control_command(&["mark", "get", "myarena", "file1.txt", "file2.txt"])?
                .output()
                .await?;

            assert!(
                output.status.success(),
                "Control command failed: {output:?}"
            );

            let output_str = String::from_utf8(output.stdout)?;
            assert!(
                output_str.contains("file1.txt: keep"),
                "Expected file1 mark, got '{}'",
                output_str
            );
            assert!(
                output_str.contains("file2.txt: keep"),
                "Expected file2 mark, got '{}'",
                output_str
            );

            Ok::<_, anyhow::Error>(())
        })
        .await?;
    Ok(())
}
