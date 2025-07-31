#![allow(dead_code)] // WIP

use clap::ValueEnum;
use console::style;

#[derive(Copy, Clone, Debug, ValueEnum, PartialEq, Eq)]
pub(crate) enum OutputMode {
    /// Print only error messages and warnings to stderr.
    Quiet,

    /// Print progress bar and summary  to stdout, and error messages and warnings to stderr.
    Progress,

    /// Disable progress and printing of errors, just log.
    ///
    /// Set RUST_LOG to configure what gets included.
    Log,
}

/// Print a warning message to stderr, with standard format.
pub(crate) fn print_warning<T: AsRef<str>>(mode: OutputMode, msg: T) {
    let msg = msg.as_ref();
    log::warn!("{msg}");
    match mode {
        OutputMode::Log => {}
        OutputMode::Quiet | OutputMode::Progress => {
            eprintln!("{} {msg}", style("WARNING").for_stderr().red());
        }
    }
}

/// Print an error message to stderr, with standard format.
pub(crate) fn print_error<T: AsRef<str>>(mode: OutputMode, msg: T) {
    let msg = msg.as_ref();
    log::error!("{msg}");
    match mode {
        OutputMode::Log => {}
        OutputMode::Quiet | OutputMode::Progress => {
            eprintln!("{} {msg}", style("ERROR").for_stderr().red().bold());
        }
    }
}

/// Print an success message to stdout, with standard format.
pub(crate) fn print_success<T: AsRef<str>, U: AsRef<str>>(mode: OutputMode, tag: T, msg: U) {
    let tag = tag.as_ref();
    let msg = msg.as_ref();
    log::info!("{tag} {msg}");
    match mode {
        OutputMode::Log | OutputMode::Quiet => {}
        OutputMode::Progress => {
            println!("{} {msg}", style(tag).for_stdout().green().bold());
        }
    }
}

/// Print an info message to stdout, with standard format.
pub(crate) fn print_info<T: AsRef<str>>(mode: OutputMode, msg: T) {
    let msg = msg.as_ref();
    log::info!("{msg}");
    match mode {
        OutputMode::Log | OutputMode::Quiet => {}
        OutputMode::Progress => {
            println!("{msg}");
        }
    }
}
