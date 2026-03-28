#![allow(dead_code)] // WIP

use std::sync::Arc;

use clap::ValueEnum;
use console::{StyledObject, Term, style};
use indicatif::{ProgressStyle, TermLike};

#[derive(Copy, Clone, Debug, ValueEnum, PartialEq, Eq)]
pub(crate) enum OutputMode {
    /// Print only error messages and warnings to stderr.
    Quiet,

    /// Print success to stdout and errors and warnings to stderr.
    Plain,

    /// Display and update progress on stdout, and error messages and warnings to stderr.
    ///
    /// Falls back to `Plain` if stdout is not a terminal.
    Progress,

    /// Disable progress and printing of errors, just log.
    ///
    /// Set RUST_LOG to configure what gets included.
    Log,
}

#[derive(Clone)]
pub(crate) struct Output {
    mode: OutputMode,
    stdout: Option<Arc<dyn TermLike>>,
    stderr: Option<Arc<dyn TermLike>>,
    stdout_style: bool,
    stderr_style: bool,
}

#[derive(Copy, Clone, Debug, PartialEq, Eq)]
pub(crate) enum MessageType {
    SUCCESS,
    ERROR,
    WARNING,
    PROGRESS,
}

impl Output {
    pub(crate) fn default(mode: OutputMode) -> Output {
        match mode {
            OutputMode::Quiet => Output::new(mode, None, Some(Arc::new(Term::stderr()))),
            OutputMode::Plain | OutputMode::Progress => Output::new(
                mode,
                Some(Arc::new(Term::stdout())),
                Some(Arc::new(Term::stderr())),
            ),
            OutputMode::Log => Output::new(mode, None, None),
        }
    }

    pub(crate) fn new(
        mode: OutputMode,
        stdout: Option<Arc<dyn TermLike>>,
        stderr: Option<Arc<dyn TermLike>>,
    ) -> Self {
        Output {
            mode,
            stdout,
            stderr,
            stdout_style: console::colors_enabled(),
            stderr_style: console::colors_enabled_stderr(),
        }
    }

    /// Enable or disable style on stdout, ignoring automatic
    /// detection.
    pub(crate) fn set_stdout_style(&mut self, enabled: bool) {
        self.stdout_style = enabled;
    }

    /// Enable or disable style on stderr, ignoring automatic
    /// detection.
    pub(crate) fn set_stderr_style(&mut self, enabled: bool) {
        self.stderr_style = enabled;
    }

    /// Returns the current output mode.
    pub(crate) fn mode(&self) -> OutputMode {
        self.mode
    }

    /// Print a warning message to stderr, with standard format.
    pub(crate) fn print_warning<T: AsRef<str>, U: AsRef<str>>(&self, tag: T, msg: U) {
        let tag = tag.as_ref();
        let msg = msg.as_ref();
        log::warn!("{tag} {msg}");
        if let Some(term) = &self.stderr {
            let tag = self.for_stderr(tag).warn();
            let _ = term.write_line(&format!("{tag} {msg}"));
        }
    }

    /// Print a progress message to stderr, with standard format.
    pub(crate) fn print_progress<T: AsRef<str>, U: AsRef<str>>(&self, tag: T, msg: U) {
        let tag = tag.as_ref();
        let msg = msg.as_ref();
        log::warn!("{tag} {msg}");
        if let Some(term) = &self.stdout {
            let tag = self.for_stdout(tag).progress();
            let _ = term.write_line(&format!("{tag} {msg}"));
        }
    }

    /// Print an error message to stderr, with standard format.
    pub(crate) fn print_error<T: AsRef<str>>(&self, msg: T) {
        let msg = msg.as_ref();
        log::error!("{msg}");
        if let Some(term) = &self.stderr {
            let tag = self.for_stderr("ERROR").error();
            let _ = term.write_line(&format!("{tag} {msg}"));
        }
    }

    /// Print an success message to stdout, with standard format.
    pub(crate) fn print_success<T: AsRef<str>, U: AsRef<str>>(&self, tag: T, msg: U) {
        let tag = tag.as_ref();
        let msg = msg.as_ref();
        log::info!("{tag} {msg}");
        if let Some(term) = &self.stdout {
            let tag = self.for_stdout(tag).success();
            let _ = term.write_line(&format!("{tag} {msg}"));
        }
    }

    /// Print an info message to stdout, with standard format.
    pub(crate) fn print_info<T: AsRef<str>>(&self, msg: T) {
        let msg = msg.as_ref();
        log::info!("{msg}");
        if let Some(term) = &self.stdout {
            let _ = term.write_line(msg);
        }
    }

    /// Build a [StyledObject] appropriate for stdout.
    pub(crate) fn for_stdout<T: AsRef<str>>(&self, val: T) -> StyledObject<T> {
        style(val).force_styling(self.stdout_style)
    }

    /// Build a [StyledObject] appropriate for stderr.
    pub(crate) fn for_stderr<T: AsRef<str>>(&self, val: T) -> StyledObject<T> {
        style(val).force_styling(self.stderr_style)
    }
}

/// Extends StyledObject with application-specific styles.
trait StyledObjectExt<T: AsRef<str>> {
    fn success(self) -> StyledObject<T>;
    fn warn(self) -> StyledObject<T>;
    fn progress(self) -> StyledObject<T>;
    fn error(self) -> StyledObject<T>;
}

impl<T: AsRef<str>> StyledObjectExt<T> for StyledObject<T> {
    fn success(self) -> StyledObject<T> {
        self.green().bold()
    }

    fn warn(self) -> StyledObject<T> {
        self.yellow().bold()
    }

    fn progress(self) -> StyledObject<T> {
        self.cyan().bold()
    }

    fn error(self) -> StyledObject<T> {
        self.red().bold()
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::testing::OutputFixture;
    use crate::testing::forced_style;

    #[test]
    fn default_quiet() -> anyhow::Result<()> {
        let output = Output::default(OutputMode::Quiet);
        assert!(output.stdout.is_none());
        assert!(output.stderr.is_some());
        Ok(())
    }

    #[test]
    fn default_plain() -> anyhow::Result<()> {
        let output = Output::default(OutputMode::Plain);
        assert!(output.stdout.is_some());
        assert!(output.stderr.is_some());

        Ok(())
    }

    #[test]
    fn default_progress() -> anyhow::Result<()> {
        let output = Output::default(OutputMode::Progress);
        assert!(output.stdout.is_some());
        assert!(output.stderr.is_some());

        Ok(())
    }

    #[test]
    fn default_log() -> anyhow::Result<()> {
        let output = Output::default(OutputMode::Log);
        assert!(output.stdout.is_none());
        assert!(output.stderr.is_none());

        Ok(())
    }

    #[test]
    fn print_success() -> anyhow::Result<()> {
        let fixture = OutputFixture::setup(OutputMode::Progress)?;
        fixture.output.print_success("OK", "This is a test");

        fixture.expected.write_line(&format!(
            "{} This is a test",
            forced_style("OK").green().bold()
        ))?;
        assert_eq!(fixture.expected(), fixture.actual());

        Ok(())
    }

    #[test]
    fn print_success_no_style() -> anyhow::Result<()> {
        let mut fixture = OutputFixture::setup(OutputMode::Progress)?;
        fixture.output.set_stdout_style(false);
        fixture.output.print_success("OK", "This is a test");

        fixture.expected.write_line("OK This is a test")?;
        assert_eq!(fixture.expected(), fixture.actual());

        Ok(())
    }

    #[test]
    fn print_warning() -> anyhow::Result<()> {
        let fixture = OutputFixture::setup(OutputMode::Progress)?;
        fixture.output.print_warning("WARN", "This is a test");

        fixture.expected.write_line(&format!(
            "{} This is a test",
            forced_style("WARN").yellow().bold()
        ))?;
        assert_eq!(fixture.expected(), fixture.actual());

        Ok(())
    }

    #[test]
    fn print_warning_no_style() -> anyhow::Result<()> {
        let mut fixture = OutputFixture::setup(OutputMode::Progress)?;
        fixture.output.set_stderr_style(false);
        fixture.output.print_warning("WARN", "This is a test");

        fixture.expected.write_line("WARN This is a test")?;
        assert_eq!(fixture.expected(), fixture.actual());

        Ok(())
    }

    #[test]
    fn print_progress() -> anyhow::Result<()> {
        let fixture = OutputFixture::setup(OutputMode::Progress)?;
        fixture.output.print_progress("Download", "This is a test");

        fixture.expected.write_line(&format!(
            "{} This is a test",
            forced_style("Download").cyan().bold()
        ))?;
        assert_eq!(fixture.expected(), fixture.actual());

        Ok(())
    }

    #[test]
    fn print_progress_no_style() -> anyhow::Result<()> {
        let mut fixture = OutputFixture::setup(OutputMode::Progress)?;
        fixture.output.set_stdout_style(false);
        fixture.output.print_success("Download", "This is a test");

        fixture.expected.write_line("Download This is a test")?;
        assert_eq!(fixture.expected(), fixture.actual());

        Ok(())
    }

    #[test]
    fn print_error() -> anyhow::Result<()> {
        let fixture = OutputFixture::setup(OutputMode::Progress)?;
        fixture.output.print_error("This is a test");

        fixture.expected.write_line(&format!(
            "{} This is a test",
            forced_style("ERROR").red().bold()
        ))?;
        assert_eq!(fixture.expected(), fixture.actual());

        Ok(())
    }

    #[test]
    fn print_error_no_style() -> anyhow::Result<()> {
        let mut fixture = OutputFixture::setup(OutputMode::Progress)?;
        fixture.output.set_stderr_style(false);
        fixture.output.print_error("This is a test");

        fixture.expected.write_line("ERROR This is a test")?;
        assert_eq!(fixture.expected(), fixture.actual());

        Ok(())
    }
}
