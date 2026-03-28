use anyhow::Result;
use clap::{Parser, Subcommand};
use console::Term;
use output::OutputMode;
use realize_core::rpc::control::client;
use realize_core::utils::logging;
use std::path::PathBuf;
use tokio::task::LocalSet;

use crate::output::Output;

mod arena_cmd;
mod attr_cmd;
mod churten_cmd;
mod display;
mod output;
mod peer_cmd;
#[cfg(test)]
mod testing;

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

    Peer {
        #[command(subcommand)]
        command: PeerCommands,
    },
    Arena {
        #[command(subcommand)]
        command: ArenaCommands,
    },
    /// Attributes
    Attr {
        #[command(subcommand)]
        command: AttrCommands,
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
    /// Connect and print statusnotifications, start churten if
    /// necessary.
    Connect,
}

#[derive(Subcommand, Debug)]
enum PeerCommands {
    /// List all peers and their connection status
    Query,
    /// Connect to a peer
    Connect {
        /// The peer to connect to
        peer: String,
    },
    /// Disconnect from a peer
    Disconnect {
        /// The peer to disconnect from
        peer: String,
    },
}

#[derive(Subcommand, Debug)]
enum ArenaCommands {
    /// Create a local arena that stores its local files and local
    /// database in PATH.
    ///
    /// If an arena with the same name exists on other peers, syncing
    /// between peers starts automatically.
    Create {
        /// Arena name
        name: String,

        /// Directory where Arena's files and database are stored.
        path: PathBuf,
    },

    Remove {
        /// Arena name
        name: String,

        /// Delete all files belonging to the arena.
        #[arg(long)]
        delete_files: bool,

        /// Keep the arena database, so it can be re-added later.
        #[arg(long)]
        keep_database: bool,
    },

    /// Empty the trash of an arena
    EmptyTrash {
        /// Arena name (optional)
        name: Option<String>,
    },

    /// Empty the cache of an arena
    EmptyCache {
        /// Arena name (optional)
        name: Option<String>,
    },
}

#[derive(Subcommand, Debug)]
enum AttrCommands {
    /// List available attributes on an arena, directory or file.
    ///
    /// If no paths is given, the command applies to the arena, otherwise
    /// it applies to each given path within the arena.
    List { arena: String, paths: Vec<String> },
    /// Get the value of an attributes in an arena, directories or files
    ///
    /// If no paths is given, the command applies to the arena, otherwise
    /// it applies to each given path within the arena.
    Get {
        attr: String,
        arena: String,
        paths: Vec<String>,
    },
    /// Set the value of an attribute in an arena, directories or files
    ///
    /// If no paths is given, the command applies to the arena, otherwise
    /// it applies to each given path within the arena.
    Set {
        attr: String,
        value: String,
        arena: String,
        paths: Vec<String>,
    },
}

/// Get the default socket path by checking for the first existing socket
/// among the standard locations
fn get_default_socket_path() -> Option<PathBuf> {
    let socket_paths = [
        PathBuf::from("/run/realized/control.socket"),
        PathBuf::from("/var/run/realized/control.socket"),
        PathBuf::from("/tmp/realized/control.socket"),
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
    let mut cli = Cli::parse();
    if cli.output == OutputMode::Log {
        logging::init_with_info_modules(vec!["realize"]);
    } else {
        logging::init(log::LevelFilter::Off);
    }
    if cli.output == OutputMode::Progress && !Term::stdout().is_term() {
        cli.output = OutputMode::Plain;
    }
    let output = Output::default(cli.output);
    let status = match execute(cli, &output).await {
        Ok(code) => code,
        Err(err) => {
            output.print_error(&format!("{err:#}"));

            1
        }
    };
    std::process::exit(status);
}

async fn execute(cli: Cli, output: &Output) -> anyhow::Result<i32> {
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
                        churten_cmd::execute_churten_start(&control, output).await
                    }
                    ChurtenCommands::Stop => {
                        churten_cmd::execute_churten_stop(&control, output).await
                    }
                    ChurtenCommands::IsRunning => {
                        churten_cmd::execute_churten_is_running(&control, output).await
                    }
                    ChurtenCommands::Connect => {
                        churten_cmd::execute_churten_connect(&control, output).await
                    }
                },

                Commands::Peer { command } => match command {
                    PeerCommands::Query => peer_cmd::execute_peer_query(&control, output).await,
                    PeerCommands::Connect { peer } => {
                        peer_cmd::execute_peer_connect(&control, &peer, output).await
                    }
                    PeerCommands::Disconnect { peer } => {
                        peer_cmd::execute_peer_disconnect(&control, &peer, output).await
                    }
                },
                Commands::Arena { command } => match command {
                    ArenaCommands::Create { name, path } => {
                        arena_cmd::execute_arena_create(&control, output, &name, &path).await
                    }
                    ArenaCommands::Remove {
                        name,
                        delete_files,
                        keep_database,
                    } => {
                        arena_cmd::execute_arena_remove(
                            &control,
                            output,
                            &name,
                            delete_files,
                            keep_database,
                        )
                        .await
                    }
                    ArenaCommands::EmptyTrash { name } => {
                        arena_cmd::execute_arena_empty_trash(&control, output, name.as_deref())
                            .await
                    }
                    ArenaCommands::EmptyCache { name } => {
                        arena_cmd::execute_arena_empty_cache(&control, output, name.as_deref())
                            .await
                    }
                },
                Commands::Attr { command } => match command {
                    AttrCommands::List { arena, paths } => {
                        attr_cmd::execute_attr_list(&control, output, &arena, &paths).await
                    }
                    AttrCommands::Get { attr, arena, paths } => {
                        attr_cmd::execute_attr_get(&control, output, &attr, &arena, &paths).await
                    }
                    AttrCommands::Set {
                        attr,
                        value,
                        arena,
                        paths,
                    } => {
                        attr_cmd::execute_attr_set(&control, output, &attr, &value, &arena, &paths)
                            .await
                    }
                },
            }
        })
        .await?;

    Ok(status)
}
