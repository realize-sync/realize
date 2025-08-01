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

/// A progress bar with bytes per sec and total bytes.
///
/// Formatting is compatible with success/warning/error messages
/// displayed when output mode is [OutputMode::Progress].
fn bar_with_byte_progress() -> ProgressStyle {
    ProgressStyle::with_template(
        "{prefix:<10.cyan.bold} [{wide_bar:.cyan/blue}] {bytes_per_sec} ({bytes}/{total_bytes}) {percent}%",
    )
        .unwrap()
        .progress_chars("=> ")
}

/// A tagged message with bytes per sec and total bytes.
///
/// Formatting is compatible with success/warning/error messages
/// displayed when output mode is [OutputMode::Progress].
fn tagged_message_with_byte_progress(tag: &str, warn: bool) -> ProgressStyle {
    let tag = tag.to_string();
    ProgressStyle::with_template(if warn {
        "{prefix:<10.yellow.bold} [{tag}] {wide_msg} ({bytes}/{total_bytes}) {percent}%"
    } else {
        "{prefix:<10.cyan.bold} [{tag}] {wide_msg} ({bytes}/{total_bytes}) {percent}%"
    })
    .unwrap()
    .progress_chars("=> ")
    .with_key(
        "tag",
        move |_state: &indicatif::ProgressState, w: &mut dyn std::fmt::Write| {
            let _ = w.write_str(&tag);
        },
    )
}
