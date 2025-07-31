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
#[allow(dead_code)]
pub(crate) fn print_warning(mode: OutputMode, msg: &str) {
    log::warn!("{msg}");
    match mode {
        OutputMode::Log => {}
        OutputMode::Quiet | OutputMode::Progress => {
            eprintln!("{}: {}", style("WARNING").for_stderr().red(), msg);
        }
    }
}

/// Print an error message to stderr, with standard format.
pub(crate) fn print_error(mode: OutputMode, msg: &str) {
    log::error!("{msg}");
    match mode {
        OutputMode::Log => {}
        OutputMode::Quiet | OutputMode::Progress => {
            eprintln!("{}: {}", style("ERROR").for_stderr().red().bold(), msg);
        }
    }
}
