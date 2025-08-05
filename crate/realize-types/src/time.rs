use std::{
    fs::Metadata,
    time::{Duration, SystemTime, SystemTimeError},
};

use time_format::TimeStampMs;

/// Time as duration since the start of the UNIX epoch.
#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, serde::Serialize, serde::Deserialize)]
#[serde(transparent)]
pub struct UnixTime(Duration);

impl UnixTime {
    /// Start of the UNIX epoch.
    pub const ZERO: UnixTime = UnixTime(Duration::ZERO);
    pub const MAX: UnixTime = UnixTime(Duration::MAX);

    /// The current time
    pub fn now() -> Self {
        UnixTime::from_system_time(SystemTime::now())
            // System clocks shouldn't be set before the start of
            // the UNIX epoch.
            .unwrap()
    }

    /// Create a new UNIX time with the given secs and fractional nanosecs.
    pub fn new(secs: u64, nsecs: u32) -> Self {
        UnixTime(Duration::new(secs, nsecs))
    }

    /// Create a new UNIX time with the given secs and no fractional
    /// nanosecs.
    pub fn from_secs(secs: u64) -> Self {
        UnixTime::new(secs, 0)
    }

    fn from_system_time(time: SystemTime) -> Result<Self, SystemTimeError> {
        Ok(UnixTime(time.duration_since(SystemTime::UNIX_EPOCH)?))
    }

    /// Return duration since some other time.
    ///
    /// Return 0 if `self < other`
    pub fn duration_since<T: AsRef<UnixTime>>(&self, other: T) -> Duration {
        let other_duration = other.as_ref().0;
        if self.0 > other_duration {
            self.0 - other_duration
        } else {
            Duration::ZERO
        }
    }

    /// Extract modification time from the given file metadata.
    pub fn mtime(m: &Metadata) -> UnixTime {
        UnixTime::from_system_time(m.modified().expect("OS must support mtime"))
            .unwrap_or(UnixTime::ZERO)
    }

    /// Seconds since start of the UNIX epoch.
    pub fn as_secs(&self) -> u64 {
        self.0.as_secs()
    }

    /// Nanoseconds since the start of the second.
    pub fn subsec_nanos(&self) -> u32 {
        self.0.subsec_nanos()
    }

    /// Return a reference to the underlying duration.
    pub fn as_duration(&self) -> &Duration {
        &self.0
    }

    pub fn plus(self, duration: Duration) -> UnixTime {
        UnixTime(self.0 + duration)
    }
}

impl AsRef<UnixTime> for UnixTime {
    fn as_ref(&self) -> &Self {
        self
    }
}
impl From<Duration> for UnixTime {
    fn from(value: Duration) -> Self {
        UnixTime(value)
    }
}

impl From<&Duration> for UnixTime {
    fn from(value: &Duration) -> Self {
        UnixTime(*value)
    }
}

impl std::fmt::Debug for UnixTime {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let ts = if let (Some(s), Some(m)) = (
            i64::try_from(self.as_secs()).ok(),
            u16::try_from(self.0.subsec_millis()).ok(),
        ) {
            Some(TimeStampMs::new(s, m))
        } else {
            None
        };
        let time_string = ts
            .map(|ts_ms| time_format::format_iso8601_ms_utc(ts_ms).ok())
            .flatten()
            .unwrap_or_else(|| format!("{:?}", self.0));

        f.write_str(time_string.strip_suffix("Z").unwrap_or(&time_string))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn debug_format() -> anyhow::Result<()> {
        assert_eq!(
            "2009-02-13T23:31:30.333",
            format!("{:?}", UnixTime::new(1234567890, 333999111))
        );

        Ok(())
    }
}
