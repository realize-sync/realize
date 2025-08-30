use assert_fs::TempDir;
use assert_fs::prelude::*;
use realize_core::config::Config;
use realize_core::setup::SetupHelper;
use realize_storage::Mark;
use realize_storage::Notification;
use realize_storage::config::{ArenaConfig, CacheConfig};
use realize_types::{Arena, Hash, Path, Peer, UnixTime};
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
    arena: Arena,
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
        config.storage.arenas.push(ArenaConfig::new(
            arena,
            myarena.to_path_buf(),
            tempdir.child("myarena-cache.db").to_path_buf(),
            tempdir.child("myarena-blobs").to_path_buf(),
        ));

        let resources = PathBuf::from(std::env::var("CARGO_MANIFEST_DIR").unwrap())
            .join("../../resources/test");

        // Add a simple peer configuration
        config
            .network
            .peers
            .push(realize_network::config::PeerConfig {
                peer: Peer::from("a"),
                pubkey: std::fs::read_to_string(resources.join("a-spki.pem"))?,
                address: None,
                batch_rate_limit: None,
            });

        let server_privkey = resources.join("a.key");
        let socket = tempdir.path().join("realize/control.socket");
        let setup = SetupHelper::setup(config, &server_privkey, local).await?;
        setup
            .bind_control_socket(local, Some(&socket), 0o077)
            .await?;

        Ok(Self {
            arena,
            _tempdir: tempdir,
            socket,
            setup,
        })
    }

    pub fn control_command(&self, args: &[&str]) -> anyhow::Result<Command> {
        let mut cmd = tokio::process::Command::new(command_path("realize"));
        cmd.arg("--socket")
            .arg(&self.socket)
            .args(args)
            .env("RUST_LOG", "realize_")
            .kill_on_drop(true);

        Ok(cmd)
    }

    async fn add_file_to_cache(&self, path_str: &str) -> anyhow::Result<()> {
        self.setup
            .storage
            .cache()
            .update(
                Peer::from("other"),
                Notification::Add {
                    arena: self.arena,
                    index: 1,
                    path: Path::parse(path_str)?,
                    mtime: UnixTime::from_secs(1234567890),
                    size: 100,
                    hash: Hash([1u8; 32]),
                },
            )
            .await?;

        Ok(())
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
async fn mark_set_arena() -> anyhow::Result<()> {
    let local = LocalSet::new();
    let fixture = Fixture::setup(&local).await?;
    fixture.add_file_to_cache("file1.txt").await?;

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
                    .get_mark(
                        Arena::from("myarena"),
                        &realize_types::Path::parse("file1.txt")?
                    )
                    .await?
            );

            Ok::<_, anyhow::Error>(())
        })
        .await?;
    Ok(())
}

#[tokio::test]
async fn mark_set_paths() -> anyhow::Result<()> {
    let local = LocalSet::new();
    let fixture = Fixture::setup(&local).await?;
    fixture.add_file_to_cache("file1.txt").await?;
    fixture.add_file_to_cache("file2.txt").await?;
    fixture.add_file_to_cache("file3.txt").await?;

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
async fn mark_get() -> anyhow::Result<()> {
    let local = LocalSet::new();
    let fixture = Fixture::setup(&local).await?;
    fixture.add_file_to_cache("file1.txt").await?;
    fixture.add_file_to_cache("file2.txt").await?;
    fixture.add_file_to_cache("file3.txt").await?;

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
async fn mark_get_multiple_paths() -> anyhow::Result<()> {
    let local = LocalSet::new();
    let fixture = Fixture::setup(&local).await?;
    fixture.add_file_to_cache("file1.txt").await?;
    fixture.add_file_to_cache("file2.txt").await?;

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

#[tokio::test]
async fn mark_get_arena() -> anyhow::Result<()> {
    let local = LocalSet::new();
    let fixture = Fixture::setup(&local).await?;

    local
        .run_until(async move {
            // First set an arena mark
            let set_output = fixture
                .control_command(&["mark", "set", "own", "myarena"])?
                .output()
                .await?;
            assert!(set_output.status.success());

            // Then get the arena mark
            let output = fixture
                .control_command(&["mark", "get", "myarena"])?
                .output()
                .await?;

            assert!(
                output.status.success(),
                "Control command failed: {output:?}"
            );

            let output_str = String::from_utf8(output.stdout)?;
            assert_eq!(
                "arena: own\n", output_str,
                "Expected arena mark output, got '{}'",
                output_str
            );

            Ok::<_, anyhow::Error>(())
        })
        .await?;
    Ok(())
}

#[tokio::test]
async fn peer_query() -> anyhow::Result<()> {
    let local = LocalSet::new();
    let fixture = Fixture::setup(&local).await?;

    local
        .run_until(async move {
            // Test the peer query command
            let output = fixture
                .control_command(&["peer", "query"])?
                .output()
                .await?;

            assert!(
                output.status.success(),
                "Control command failed: {output:?}"
            );

            let output_str = String::from_utf8(output.stdout)?;
            // Should show at least one peer (the one configured in setup)
            assert!(
                output_str.contains("a:"),
                "Expected peer 'a' in output, got '{}'",
                output_str
            );

            Ok::<_, anyhow::Error>(())
        })
        .await?;
    Ok(())
}

#[tokio::test]
async fn peer_connect() -> anyhow::Result<()> {
    let local = LocalSet::new();
    let fixture = Fixture::setup(&local).await?;

    local
        .run_until(async move {
            // Test the peer connect command
            let output = fixture
                .control_command(&["peer", "connect", "a"])?
                .output()
                .await?;

            assert!(
                output.status.success(),
                "Control command failed: {output:?}"
            );

            let output_str = String::from_utf8(output.stdout)?;
            assert!(
                output_str.contains("Connecting to peer: a"),
                "Expected success message, got '{}'",
                output_str
            );

            Ok::<_, anyhow::Error>(())
        })
        .await?;
    Ok(())
}

#[tokio::test]
async fn peer_disconnect() -> anyhow::Result<()> {
    let local = LocalSet::new();
    let fixture = Fixture::setup(&local).await?;

    local
        .run_until(async move {
            // Test the peer disconnect command
            let output = fixture
                .control_command(&["peer", "disconnect", "a"])?
                .output()
                .await?;

            assert!(
                output.status.success(),
                "Control command failed: {output:?}"
            );

            let output_str = String::from_utf8(output.stdout)?;
            assert!(
                output_str.contains("Disconnected from peer: a"),
                "Expected success message, got '{}'",
                output_str
            );

            Ok::<_, anyhow::Error>(())
        })
        .await?;
    Ok(())
}
