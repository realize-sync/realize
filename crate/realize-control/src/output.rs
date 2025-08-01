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
pub(crate) fn print_warning<T: AsRef<str>, U: AsRef<str>>(mode: OutputMode, tag: T, msg: U) {
    print_warning_internal(mode, tag.as_ref(), msg.as_ref(), false);
}

/// Print a warning message to stderr, with standard format, aligned with bars.
pub(crate) fn print_warning_aligned<T: AsRef<str>, U: AsRef<str>>(
    mode: OutputMode,
    tag: T,
    msg: U,
) {
    print_warning_internal(mode, tag.as_ref(), msg.as_ref(), true);
}

fn print_warning_internal(mode: OutputMode, tag: &str, msg: &str, align: bool) {
    log::warn!("{tag} {msg}");
    match mode {
        OutputMode::Log => {}
        OutputMode::Quiet | OutputMode::Progress => {
            let tag = style(tag).for_stderr().red();
            if align {
                eprintln!("{tag:>10} {msg}");
            } else {
                eprintln!("{tag} {msg}");
            }
        }
    }
}

/// Print an error message to stderr, with standard format.
pub(crate) fn print_error<T: AsRef<str>>(mode: OutputMode, msg: T) {
    print_error_internal(mode, msg.as_ref(), false);
}

pub(crate) fn print_error_aligned<T: AsRef<str>>(mode: OutputMode, msg: T) {
    print_error_internal(mode, msg.as_ref(), true);
}

pub(crate) fn print_error_internal(mode: OutputMode, msg: &str, align: bool) {
    log::error!("{msg}");
    match mode {
        OutputMode::Log => {}
        OutputMode::Quiet | OutputMode::Progress => {
            let tag = style("ERROR").for_stderr().red().bold();
            if align {
                eprintln!("{tag:>10} {msg}");
            } else {
                eprintln!("{tag} {msg}");
            }
        }
    }
}

/// Print an success message to stdout, with standard format.
pub(crate) fn print_success<T: AsRef<str>, U: AsRef<str>>(mode: OutputMode, tag: T, msg: U) {
    print_success_internal(mode, tag.as_ref(), msg.as_ref(), false);
}

/// Print an success message to stdout, with standard format, aligned with bars.
pub(crate) fn print_success_aligned<T: AsRef<str>, U: AsRef<str>>(
    mode: OutputMode,
    tag: T,
    msg: U,
) {
    print_success_internal(mode, tag.as_ref(), msg.as_ref(), true);
}

fn print_success_internal(mode: OutputMode, tag: &str, msg: &str, align: bool) {
    log::info!("{tag} {msg}");
    match mode {
        OutputMode::Log | OutputMode::Quiet => {}
        OutputMode::Progress => {
            let tag = style(tag).for_stdout().green().bold();
            if align {
                println!("{tag:>10} {msg}");
            } else {
                println!("{tag} {msg}");
            }
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

/// A message with bytes per sec and total bytes.
///
/// Formatting is compatible with success/warning/error messages
/// displayed when output mode is [OutputMode::Progress].
pub(crate) fn wide_message_with_byte_progress(warn: bool) -> ProgressStyle {
    ProgressStyle::with_template(if warn {
        "{prefix:>10.yellow.bold} {wide_msg} ({bytes}/{total_bytes}) {percent}%"
    } else {
        "{prefix:>10.cyan.bold} {wide_msg} ({bytes}/{total_bytes}) {percent}%"
    })
    .unwrap()
    .progress_chars("=> ")
}

/// A message with bytes per sec and total bytes.
///
/// Formatting is compatible with success/warning/error messages
/// displayed when output mode is [OutputMode::Progress].
pub(crate) fn wide_message(warn: bool) -> ProgressStyle {
    ProgressStyle::with_template(if warn {
        "{prefix:>10.yellow.bold} {wide_msg}"
    } else {
        "{prefix:>10.cyan.bold} {wide_msg}"
    })
    .unwrap()
    .progress_chars("=> ")
}
