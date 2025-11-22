use assert_fs::TempDir;
use assert_fs::fixture::ChildPath;
use assert_fs::prelude::*;
use realize_core::config::Config;
use realize_core::setup::SetupHelper;

use realize_storage::Notification;
use realize_storage::config::CacheConfig;
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
    tempdir: TempDir,
    socket: PathBuf,
    arena: Arena,
    arena_dir: ChildPath,
    setup: SetupHelper,
}

impl Fixture {
    pub async fn setup(local: &LocalSet) -> anyhow::Result<Self> {
        let _ = env_logger::try_init();

        let mut config = Config::new();
        let arena = Arena::from("myarena");

        // Setup temp directory for the daemon to serve
        let tempdir = TempDir::new()?;

        let arena_dir = tempdir.child("myarena");
        arena_dir.create_dir_all()?;

        config.storage.cache = CacheConfig {
            db: tempdir.child("cache.db").to_path_buf(),
        };

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

        setup.storage.create_arena(arena, arena_dir.path()).await?;

        Ok(Self {
            arena,
            arena_dir,
            tempdir,
            socket,
            setup,
        })
    }

    pub fn control_command(&self, args: &[&str]) -> anyhow::Result<Command> {
        let mut cmd = tokio::process::Command::new(command_path("realize"));
        cmd.arg("--socket")
            .arg(&self.socket)
            .args(args)
            .env("RUST_LOG", "debug")
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

#[tokio::test]
async fn arena_create() -> anyhow::Result<()> {
    let local = LocalSet::new();
    let fixture = Fixture::setup(&local).await?;

    local
        .run_until(async move {
            let dir = fixture.tempdir.child("other");
            dir.create_dir_all()?;
            let mut control_cmd =
                fixture.control_command(&["arena", "create", "other", dir.to_str().unwrap()])?;
            let output =
                tokio::time::timeout(Duration::from_secs(3), control_cmd.output()).await??;
            if !output.status.success() {
                panic!("Control command failed: {output:?}");
            }

            assert!(
                fixture
                    .setup
                    .storage
                    .arenas()
                    .contains(&Arena::from("other"))
            );

            Ok::<_, anyhow::Error>(())
        })
        .await?;
    Ok(())
}

#[tokio::test]
async fn arena_remove() -> anyhow::Result<()> {
    let local = LocalSet::new();
    let fixture = Fixture::setup(&local).await?;

    local
        .run_until(async move {
            let mut control_cmd = fixture.control_command(&["arena", "remove", "myarena"])?;
            let output =
                tokio::time::timeout(Duration::from_secs(3), control_cmd.output()).await??;
            if !output.status.success() {
                panic!("Control command failed: {output:?}");
            }

            assert!(fixture.setup.storage.arenas().is_empty());
            assert!(fixture.arena_dir.exists());
            assert!(!fixture.arena_dir.child(".realize").exists());

            Ok::<_, anyhow::Error>(())
        })
        .await?;
    Ok(())
}

#[tokio::test]
async fn set_get_list_file_attr() -> anyhow::Result<()> {
    let local = LocalSet::new();
    let fixture = Fixture::setup(&local).await?;
    fixture.add_file_to_cache("file1.txt").await?;
    fixture.add_file_to_cache("file2.txt").await?;

    local
        .run_until(async move {
            // 1. Set attribute
            let output = fixture
                .control_command(&[
                    "attr",
                    "set",
                    "mark",
                    "keep",
                    "myarena",
                    "file1.txt",
                    "file2.txt",
                ])?
                .output()
                .await?;
            assert!(output.status.success(), "Attr set failed: {output:?}",);
            let outstr = str::from_utf8(&output.stdout)?;
            assert!(
                outstr.contains("OK [myarena]/file1.txt: attribute set\n"),
                "{output:?}"
            );
            assert!(
                outstr.contains("OK [myarena]/file2.txt: attribute set\n"),
                "{output:?}"
            );

            // 2. Get attribute
            let output = fixture
                .control_command(&["attr", "get", "mark", "myarena", "file1.txt", "file2.txt"])?
                .output()
                .await?;
            assert!(output.status.success(), "Attr get failed: {output:?}",);
            let outstr = str::from_utf8(&output.stdout)?;
            assert!(
                outstr.contains("[myarena]/file1.txt: mark=keep\n"),
                "{output:?}"
            );
            assert!(
                outstr.contains("[myarena]/file2.txt: mark=keep\n"),
                "{output:?}"
            );

            // 3. List attributes
            let output = fixture
                .control_command(&["attr", "list", "myarena", "file1.txt", "file2.txt"])?
                .output()
                .await?;

            assert!(output.status.success(), "Attr list failed: {output:?}",);
            let outstr = str::from_utf8(&output.stdout)?;
            assert!(outstr.contains("[myarena]/file1.txt: "), "{output:?}");
            assert!(outstr.contains("[myarena]/file2.txt: "), "{output:?}");
            assert!(outstr.contains("mark, "), "{output:?}");

            Ok::<_, anyhow::Error>(())
        })
        .await?;
    Ok(())
}

#[tokio::test]
async fn set_get_list_arena_attr() -> anyhow::Result<()> {
    let local = LocalSet::new();
    let fixture = Fixture::setup(&local).await?;
    fixture.add_file_to_cache("file1.txt").await?;

    local
        .run_until(async move {
            // 1. Set attribute
            let output = fixture
                .control_command(&["attr", "set", "mark", "keep", "myarena"])?
                .output()
                .await?;
            assert!(output.status.success(), "Attr set failed: {output:?}",);
            let outstr = str::from_utf8(&output.stdout)?;
            assert!(
                outstr.contains("OK [myarena]: attribute set\n"),
                "{output:?}"
            );

            // 2. Get attribute
            let output = fixture
                .control_command(&["attr", "get", "mark", "myarena"])?
                .output()
                .await?;
            assert!(output.status.success(), "Attr get failed: {output:?}",);
            let outstr = str::from_utf8(&output.stdout)?;
            assert!(outstr.contains("[myarena]: mark=keep\n"), "{output:?}");

            // 3. List attributes
            let output = fixture
                .control_command(&["attr", "list", "myarena"])?
                .output()
                .await?;

            assert!(output.status.success(), "Attr list failed: {output:?}",);
            let outstr = str::from_utf8(&output.stdout)?;
            assert!(outstr.contains("[myarena]: "), "{output:?}");
            assert!(outstr.contains(", quota"), "{output:?}");

            Ok::<_, anyhow::Error>(())
        })
        .await?;
    Ok(())
}
