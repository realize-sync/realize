use anyhow::Result;
use clap::{Parser, Subcommand};
use realize_core::rpc::control::client;
use realize_core::rpc::control::control_capnp;
use realize_core::utils::logging;
use std::path::PathBuf;
use tokio::task::LocalSet;

/// Command-line tool for controlling a running instance of realize-daemon
#[derive(Parser, Debug)]
#[command(author, version, about, long_about = None)]
struct Cli {
    /// Socket path for connecting to the daemon
    #[arg(short, long, value_name = "SOCKET")]
    socket: Option<PathBuf>,

    #[command(subcommand)]
    command: Commands,
}

#[derive(Subcommand, Debug)]
enum Commands {
    Churten {
        #[command(subcommand)]
        command: ChurtenCommands,
    },
    Mark {
        #[command(subcommand)]
        command: MarkCommands,
    },
}

#[derive(Subcommand, Debug)]
enum ChurtenCommands {
    /// Start churten
    Start,
    /// Stop churten
    Stop,
    /// Check if churten is running
    IsRunning {
        /// Quiet mode - exit with 0 if running, 10 if not running
        #[arg(short, long)]
        quiet: bool,
    },
    /// Run churten and print notifications
    Run,
}

#[derive(Subcommand, Debug)]
enum MarkCommands {
    /// Set marks on paths or arena
    Set {
        /// The mark to set (watch, keep, own)
        #[arg(value_enum)]
        mark: MarkValue,
        /// The arena name
        arena: String,
        /// Paths to mark (optional - if not provided, sets arena mark)
        paths: Vec<String>,
    },
    /// Get marks for paths
    Get {
        /// The arena name
        arena: String,
        /// Paths to get marks for
        paths: Vec<String>,
    },
}

#[derive(Debug, Clone, clap::ValueEnum)]
enum MarkValue {
    Watch,
    Keep,
    Own,
}

/// Get the default socket path by checking for the first existing socket
/// among the standard locations
fn get_default_socket_path() -> Option<PathBuf> {
    let socket_paths = [
        PathBuf::from("/run/realize/control.socket"),
        PathBuf::from("/var/run/realize/control.socket"),
        PathBuf::from("/tmp/realize/control.socket"),
    ];

    for path in socket_paths {
        if path.exists() {
            return Some(path);
        }
    }
    None
}

/// Resolve the socket path from command line arguments or defaults
fn resolve_socket_path(socket_arg: Option<PathBuf>) -> Result<PathBuf> {
    match socket_arg {
        Some(path) => Ok(path),
        None => get_default_socket_path()
            .ok_or_else(|| anyhow::anyhow!("No socket found in default locations")),
    }
}

/// Execute the churten start command
async fn execute_churten_start(control: &control_capnp::control::Client) -> Result<()> {
    let churten = client::get_churten(&control).await?;
    churten.start_request().send().promise.await?;
    println!("Churten started successfully");

    Ok(())
}

/// Execute the churten stop command
async fn execute_churten_stop(control: &control_capnp::control::Client) -> Result<()> {
    let churten = client::get_churten(&control).await?;
    churten.shutdown_request().send().promise.await?;
    println!("Churten stopped successfully");
    Ok(())
}

/// Execute the churten is_running command
async fn execute_churten_is_running(
    control: &control_capnp::control::Client,
    quiet: bool,
) -> Result<()> {
    let churten = client::get_churten(&control).await?;
    let is_running_result = churten.is_running_request().send().promise.await?;
    let is_running = is_running_result.get()?.get_running();
    if quiet {
        if is_running {
            std::process::exit(0);
        } else {
            std::process::exit(10);
        }
    } else {
        println!("{}", is_running);
    }
    Ok(())
}

/// Execute the churten run command
async fn execute_churten_run(control: &control_capnp::control::Client) -> Result<()> {
    let churten = client::get_churten(&control).await?;
    churten.start_request().send().promise.await?;
    println!("Churten started. Subscribing to notifications...");

    let res = run_churten(&churten).await;

    churten.shutdown_request().send().promise.await?;
    println!("Churten stopped.");

    res?;
    Ok(())
}

async fn run_churten(
    churten: &realize_core::rpc::control::control_capnp::churten::Client,
) -> Result<(), anyhow::Error> {
    let mut rx = client::subscribe_to_churten(churten).await?;

    while let Some(update) = rx.recv().await {
        println!("RECV: {update:?}");
    }
    println!("Done rx.closed: {}", rx.is_closed());

    return Ok(());
}

/// Execute the mark set command
async fn execute_mark_set(
    control: &control_capnp::control::Client,
    mark: &MarkValue,
    arena: &str,
    paths: &[String],
) -> Result<()> {
    let mark_value = match mark {
        MarkValue::Watch => control_capnp::Mark::Watch,
        MarkValue::Keep => control_capnp::Mark::Keep,
        MarkValue::Own => control_capnp::Mark::Own,
    };

    if paths.is_empty() {
        // Set arena mark
        let mut request = control.set_arena_mark_request();
        let mut req = request.get().init_req();
        req.set_arena(arena);
        req.set_mark(mark_value);
        request.send().promise.await?;
        println!("Arena mark set successfully");
    } else {
        // Set marks on individual paths
        for path in paths {
            let mut request = control.set_mark_request();
            let mut req = request.get().init_req();
            req.set_arena(arena);
            req.set_path(path);
            req.set_mark(mark_value);
            request.send().promise.await?;
        }
        println!("Marks set successfully on {} paths", paths.len());
    }
    Ok(())
}

/// Execute the mark get command
async fn execute_mark_get(
    control: &control_capnp::control::Client,
    arena: &str,
    paths: &[String],
) -> Result<()> {
    for path in paths {
        let mut request = control.get_mark_request();
        let mut req = request.get().init_req();
        req.set_arena(arena);
        req.set_path(path);
        let result = request.send().promise.await?;
        let mark = result.get()?.get_res()?.get_mark();

        let mark_str = match mark {
            Ok(control_capnp::Mark::Watch) => "watch",
            Ok(control_capnp::Mark::Keep) => "keep",
            Ok(control_capnp::Mark::Own) => "own",
            Err(_) => "unknown",
        };

        println!("{}: {}", path, mark_str);
    }
    Ok(())
}

#[tokio::main]
async fn main() -> Result<()> {
    let cli = Cli::parse();
    logging::init_with_info_modules(vec!["realize_control"]);

    // Resolve socket path
    let socket_path = resolve_socket_path(cli.socket)?;
    log::debug!("Connecting to {socket_path:?}");

    let local = LocalSet::new();
    local
        .run_until(async move {
            let control = client::connect(&socket_path).await?;

            // Execute the appropriate command
            match cli.command {
                Commands::Churten { command } => match command {
                    ChurtenCommands::Start => {
                        execute_churten_start(&control).await?;
                    }
                    ChurtenCommands::Stop => {
                        execute_churten_stop(&control).await?;
                    }
                    ChurtenCommands::IsRunning { quiet } => {
                        execute_churten_is_running(&control, quiet).await?;
                    }
                    ChurtenCommands::Run => {
                        execute_churten_run(&control).await?;
                    }
                },

                Commands::Mark { command } => match command {
                    MarkCommands::Set { mark, arena, paths } => {
                        execute_mark_set(&control, &mark, &arena, &paths).await?;
                    }

                    MarkCommands::Get { arena, paths } => {
                        execute_mark_get(&control, &arena, &paths).await?;
                    }
                },
            }

            Ok::<(), anyhow::Error>(())
        })
        .await?;

    Ok(())
}
