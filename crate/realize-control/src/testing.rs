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
            Some(Box::new(actual.clone())),
            Some(Box::new(actual.clone())),
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
        String::from_utf8(self.actual.contents_formatted()).unwrap()
    }

    /// Return the expected terminal content
    pub fn expected(&self) -> String {
        String::from_utf8(self.expected.contents_formatted()).unwrap()
    }
}
