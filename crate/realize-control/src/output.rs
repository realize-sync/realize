#![allow(dead_code)] // WIP

use clap::ValueEnum;
use console::style;
use indicatif::ProgressStyle;

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

#[derive(Copy, Clone, Debug, PartialEq, Eq)]
pub(crate) enum MessageType {
    SUCCESS,
    ERROR,
    WARNING,
    PROGRESS,
}

/// Print a warning message to stderr, with standard format.
pub(crate) fn print_warning<T: AsRef<str>, U: AsRef<str>>(mode: OutputMode, tag: T, msg: U) {
    let tag = tag.as_ref();
    let msg = msg.as_ref();
    log::warn!("{tag} {msg}");
    match mode {
        OutputMode::Log => {}
        OutputMode::Quiet | OutputMode::Progress => {
            let tag = style(tag).for_stderr().red();
            eprintln!("{tag} {msg}");
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
            let tag = style("ERROR").for_stderr().red().bold();
            eprintln!("{tag} {msg}");
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
            let tag = style(tag).for_stdout().green().bold();
            println!("{tag} {msg}");
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

/// Build a progress bar style.
///
/// Formatting is compatible with success/warning/error messages
/// displayed when output mode is [OutputMode::Progress].
pub(crate) fn progress_style(msg: MessageType, with_bytes: bool) -> ProgressStyle {
    ProgressStyle::with_template(match (msg, with_bytes) {
        (MessageType::PROGRESS, false) => "{prefix:>12.cyan.bold} {wide_msg}",
        (MessageType::SUCCESS, false) => "{prefix:>12.green.bold} {wide_msg}",
        (MessageType::ERROR, false) => "{prefix:>12.red.bold} {wide_msg}",
        (MessageType::WARNING, false) => "{prefix:>12.yellow.bold} {wide_msg}",
        (MessageType::PROGRESS, true) => {
            "{prefix:>12.cyan.bold} {wide_msg} ({bytes}/{total_bytes}) {percent}%"
        }
        (MessageType::SUCCESS, true) => {
            "{prefix:>12.green.bold} {wide_msg} ({bytes}/{total_bytes}) {percent}%"
        }
        (MessageType::ERROR, true) => {
            "{prefix:>12.red.bold} {wide_msg} ({bytes}/{total_bytes}) {percent}%"
        }
        (MessageType::WARNING, true) => {
            "{prefix:>12.yellow.bold} {wide_msg} ({bytes}/{total_bytes}) {percent}%"
        }
    })
    .unwrap()
    .progress_chars("=> ")
}
