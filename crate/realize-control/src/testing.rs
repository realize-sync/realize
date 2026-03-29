use std::sync::Arc;

use console::{StyledObject, style};
use indicatif::InMemoryTerm;

use crate::output::{Output, OutputMode};

/// Return a [StyledObject] with color support enabled.
pub fn forced_style<T: AsRef<str>>(val: T) -> StyledObject<T> {
    style(val).force_styling(true)
}

/// Fixture for testing terminal output.
pub struct OutputFixture {
    pub actual: InMemoryTerm,
    pub expected: InMemoryTerm,
    pub output: Output,
}

impl OutputFixture {
    pub fn setup(mode: OutputMode) -> anyhow::Result<Self> {
        let _ = env_logger::try_init();
        let actual = InMemoryTerm::new(24, 80);
        let expected = InMemoryTerm::new(24, 80);
        let mut output = Output::new(
            mode,
            Some(Arc::new(actual.clone())),
            Some(Arc::new(actual.clone())),
        );
        output.set_stdout_style(true);
        output.set_stderr_style(true);

        Ok(Self {
            actual,
            expected,
            output,
        })
    }

    /// Return the actual terminal content.
    pub fn actual(&self) -> String {
        contents_string(self.actual.contents_formatted())
    }

    /// Return some rows from the actual terminal content.
    pub fn actual_rows(&self, range: std::ops::Range<usize>) -> String {
        rows(self.actual(), range)
    }

    /// Return the expected terminal content
    pub fn expected(&self) -> String {
        contents_string(self.expected.contents_formatted())
    }
}

/// Return a string containing a subset of the rows in the given content.
fn rows(content: String, range: std::ops::Range<usize>) -> String {
    content
        .split("\n")
        .skip(range.start)
        .take(range.end - range.start)
        .map(|row| row.strip_suffix("\u{1b}[m").unwrap_or(row))
        .collect::<Vec<_>>()
        .join("\n")
}

/// Cleanup formatted content from InMemoryTerm.
fn contents_string(mut bytes: Vec<u8>) -> String {
    bytes.drain(bytes.len() - 3..); // final reset

    let mut content = String::from_utf8(bytes).unwrap();
    while content.ends_with(" ") {
        content.pop();
    }

    content
}
