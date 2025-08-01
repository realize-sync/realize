use anyhow::Result;
use clap::{Parser, Subcommand};
use output::OutputMode;
use realize_core::rpc::control::client;
use realize_core::utils::logging;
use std::path::PathBuf;
use tokio::task::LocalSet;

mod churten_cmd;
mod display;
mod mark_cmd;
mod output;

/// Command-line tool for controlling a running instance of realize-daemon
#[derive(Parser, Debug)]
#[command(author, version, about, long_about = None)]
struct Cli {
    /// Socket path for connecting to the daemon
    #[arg(short, long, value_name = "SOCKET")]
    socket: Option<PathBuf>,

    /// Output mode.
    ///
    /// Logging can be further configured by setting the env var
    /// RUST_LOG. For a systemd-friendly output format, set the env
    /// var RUST_LOG_FORMAT=SYSTEMD
    #[arg(long, value_enum, default_value = "progress", verbatim_doc_comment)]
    output: OutputMode,

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
    /// Check if churten is running.
    ///
    /// In quiet mode, print nothing and exit with status 10 if
    /// churten is not running.
    IsRunning,
    /// Run churten and print notifications
    Run,
}

#[derive(Subcommand, Debug)]
enum MarkCommands {
    /// Set marks on paths or arena
    Set {
        /// The mark to set (watch, keep, own)
        #[arg(value_enum)]
        mark: mark_cmd::MarkValue,
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

#[tokio::main]
async fn main() {
    let cli = Cli::parse();
    let output_mode = cli.output;
    if output_mode == OutputMode::Log {
        logging::init_with_info_modules(vec!["realize_control"]);
    } else {
        logging::init(log::LevelFilter::Off);
    }

    let status = match execute(cli).await {
        Ok(code) => code,
        Err(err) => {
            output::print_error(output_mode, &format!("{err:#}"));

            1
        }
    };
    std::process::exit(status);
}

async fn execute(cli: Cli) -> anyhow::Result<i32> {
    // Resolve socket path
    let socket_path = resolve_socket_path(cli.socket)?;
    log::debug!("Connecting to {socket_path:?}");

    let local = LocalSet::new();
    let status = local
        .run_until(async move {
            let control = client::connect(&socket_path).await?;

            // Execute the appropriate command
            match cli.command {
                Commands::Churten { command } => match command {
                    ChurtenCommands::Start => {
                        churten_cmd::execute_churten_start(&control, cli.output).await
                    }
                    ChurtenCommands::Stop => {
                        churten_cmd::execute_churten_stop(&control, cli.output).await
                    }
                    ChurtenCommands::IsRunning => {
                        churten_cmd::execute_churten_is_running(&control, cli.output).await
                    }
                    ChurtenCommands::Run => {
                        churten_cmd::execute_churten_run(&control, cli.output).await
                    }
                },

                Commands::Mark { command } => match command {
                    MarkCommands::Set { mark, arena, paths } => {
                        mark_cmd::execute_mark_set(&control, &mark, &arena, &paths, cli.output)
                            .await
                    }

                    MarkCommands::Get { arena, paths } => {
                        mark_cmd::execute_mark_get(&control, &arena, &paths, cli.output).await
                    }
                },
            }
        })
        .await?;

    Ok(status)
}
