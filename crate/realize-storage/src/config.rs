use realize_types::Arena;
use std::path::PathBuf;
use std::time::Duration;

/// Storage configuration.
#[derive(Clone, serde::Deserialize, serde::Serialize, Debug, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct StorageConfig {
    #[serde(rename = "arena", default)]
    pub arenas: Vec<NamedArenaConfig>,
    pub cache: CacheConfig,
    #[serde(default)]
    pub watcher: WatcherConfig,
}

impl StorageConfig {
    pub fn new<P>(cache_db: P) -> Self
    where
        P: AsRef<std::path::Path>,
    {
        StorageConfig {
            arenas: Vec::new(),
            watcher: WatcherConfig::default(),
            cache: CacheConfig {
                db: cache_db.as_ref().to_path_buf(),
            },
        }
    }

    /// Get arena config by name
    pub fn arena_config(&self, arena: Arena) -> Option<&ArenaConfig> {
        self.arenas
            .iter()
            .find(|c| c.arena == arena)
            .map(|c| &c.config)
    }

    /// Get arena config by name (mutable)
    pub fn arena_config_mut(&mut self, arena: Arena) -> Option<&mut ArenaConfig> {
        self.arenas
            .iter_mut()
            .find(|c| c.arena == arena)
            .map(|c| &mut c.config)
    }
}

/// For the global cache (no blob_dir)
#[derive(Clone, serde::Deserialize, serde::Serialize, Debug, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct CacheConfig {
    /// Path to the cache database.
    pub db: PathBuf,
}

impl CacheConfig {
    pub fn new<P>(db: P) -> Self
    where
        P: AsRef<std::path::Path>,
    {
        Self {
            db: db.as_ref().to_path_buf(),
        }
    }
}

#[derive(Clone, serde::Deserialize, serde::Serialize, Debug, PartialEq, Eq, Default)]
pub struct WatcherConfig {
    /// Maximum number of hashers running in parallel.
    ///
    /// Hashing is CPU intensive, so hashing several large files in
    /// parallel can become a problem. It's a good idea to limit
    /// parallelism to a fraction of the available cores.
    pub max_parallel_hashers: Option<usize>,

    /// Set debounce delay for hashing files. This allows some time for
    /// operations in progress to finish.
    pub debounce: Option<HumanDuration>,
}

#[derive(Clone, serde::Deserialize, serde::Serialize, Debug, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct NamedArenaConfig {
    /// The name of this arena
    #[serde(rename = "name")]
    pub arena: Arena,

    #[serde(flatten)]
    pub config: ArenaConfig,
}

#[derive(Clone, serde::Deserialize, serde::Serialize, Debug, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct ArenaConfig {
    /// Optional local path to the directory where files for that arena are stored.
    /// If specified, an indexer will be created for this arena.
    pub datadir: PathBuf,
    /// Path to the directory where the database, blobs and overlay workdir are kept.
    ///
    /// This directory must be on the same directory as datadir, if specified.
    pub workdir: PathBuf,

    /// Limits how much disk space will be used to store local copies
    /// of remote data.
    ///
    /// Note that it might not be possible to enforce this limitation:
    /// if the size of files to keep goes above that limit, those
    /// files are kept anyways.
    #[serde(default)]
    pub disk_usage: DiskUsageConfig,
}

impl NamedArenaConfig {
    pub fn new<P1, P2>(arena: Arena, root: P1, metadata: P2) -> Self
    where
        P1: AsRef<std::path::Path>,
        P2: AsRef<std::path::Path>,
    {
        Self {
            arena,
            config: ArenaConfig {
                datadir: root.as_ref().to_path_buf(),
                workdir: metadata.as_ref().to_path_buf(),
                disk_usage: DiskUsageConfig::default(),
            },
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq, serde::Deserialize, serde::Serialize, Default)]
#[serde(deny_unknown_fields)]
pub struct DiskUsageConfig {
    /// Try to use at most that many bytes or percent of disk.
    ///
    /// The cache can temporarily go above that value.
    pub max: Option<BytesOrPercent>,

    /// Reduce disk usage to keep at keep that many bytes or percent
    /// of the disk free on the disk.
    ///
    /// This is applied after the `max` value.
    pub leave: Option<BytesOrPercent>,
}

impl DiskUsageConfig {
    pub fn is_empty(&self) -> bool {
        return self.max.is_none() && self.leave.is_none();
    }

    pub fn max_bytes(v: u64) -> DiskUsageConfig {
        Self {
            max: Some(BytesOrPercent::Bytes(v)),
            leave: None,
        }
    }
    pub fn max_percent(v: u32) -> DiskUsageConfig {
        Self {
            max: Some(BytesOrPercent::Percent(v)),
            leave: None,
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq, serde::Serialize)]
pub enum BytesOrPercent {
    Percent(u32),
    Bytes(u64),
}

impl BytesOrPercent {
    /// Convert to a parsable string representation.
    pub fn to_string(&self) -> String {
        match self {
            BytesOrPercent::Percent(p) => format!("{p}%"),
            BytesOrPercent::Bytes(val) => format!("{val}"),
        }
    }

    /// Parse a string that can be either a percentage (e.g., "10%") or a human-readable size (e.g., "1.5G", "512M")
    pub fn parse(s: &str) -> Result<Self, String> {
        if s.ends_with('%') {
            let percent_str = &s[..s.len() - 1];
            let percent: u32 = percent_str
                .parse()
                .map_err(|_| format!("Invalid percentage: {}", s))?;
            Ok(BytesOrPercent::Percent(percent))
        } else {
            // Parse human-readable size
            let (number_str, unit) = if s.ends_with("KB") {
                (&s[..s.len() - 2], "K")
            } else if s.ends_with("MB") {
                (&s[..s.len() - 2], "M")
            } else if s.ends_with("GB") {
                (&s[..s.len() - 2], "G")
            } else if s.ends_with("TB") {
                (&s[..s.len() - 2], "T")
            } else if s.ends_with("B") {
                (&s[..s.len() - 1], "B")
            } else if s.ends_with("K") {
                (&s[..s.len() - 1], "K")
            } else if s.ends_with("M") {
                (&s[..s.len() - 1], "M")
            } else if s.ends_with("G") {
                (&s[..s.len() - 1], "G")
            } else if s.ends_with("T") {
                (&s[..s.len() - 1], "T")
            } else {
                // No unit specified, assume bytes
                (s, "B")
            };

            let number: f64 = number_str
                .parse()
                .map_err(|_| format!("Invalid number in size: {}", s))?;

            let bytes = match unit {
                "B" => number as u64,
                "K" => (number * 1024.0) as u64,
                "M" => (number * 1024.0 * 1024.0) as u64,
                "G" => (number * 1024.0 * 1024.0 * 1024.0) as u64,
                "T" => (number * 1024.0 * 1024.0 * 1024.0 * 1024.0) as u64,
                _ => return Err(format!("Unknown unit: {}", unit)),
            };

            Ok(BytesOrPercent::Bytes(bytes))
        }
    }
}

impl<'de> serde::Deserialize<'de> for BytesOrPercent {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        struct BytesOrPercentVisitor;

        impl<'de> serde::de::Visitor<'de> for BytesOrPercentVisitor {
            type Value = BytesOrPercent;

            fn expecting(&self, formatter: &mut std::fmt::Formatter) -> std::fmt::Result {
                formatter.write_str("a number, a string like '1.5G' or '10%'")
            }

            fn visit_u64<E>(self, v: u64) -> Result<Self::Value, E>
            where
                E: serde::de::Error,
            {
                Ok(BytesOrPercent::Bytes(v))
            }

            fn visit_i64<E>(self, v: i64) -> Result<Self::Value, E>
            where
                E: serde::de::Error,
            {
                if v < 0 {
                    return Err(E::custom("negative values are not allowed"));
                }
                Ok(BytesOrPercent::Bytes(v as u64))
            }

            fn visit_str<E>(self, v: &str) -> Result<Self::Value, E>
            where
                E: serde::de::Error,
            {
                BytesOrPercent::parse(v).map_err(E::custom)
            }
        }

        deserializer.deserialize_any(BytesOrPercentVisitor)
    }
}

/// A wrapper around Duration that supports deserialization from both numbers (seconds) and strings with units
#[derive(Clone, Debug, PartialEq, Eq, serde::Serialize)]
pub struct HumanDuration(pub Duration);

impl HumanDuration {
    pub fn from_secs(secs: u64) -> Self {
        HumanDuration(Duration::from_secs(secs))
    }

    pub fn from_secs_f64(secs: f64) -> Self {
        HumanDuration(Duration::from_secs_f64(secs))
    }

    pub fn from_millis(millis: u64) -> Self {
        HumanDuration(Duration::from_millis(millis))
    }

    /// Parse a string that can be either a number (seconds) or a human-readable duration with units
    fn from_str(s: &str) -> Result<Self, String> {
        // Parse human-readable duration with units
        let (number_str, unit) = if s.ends_with("ms") {
            (&s[..s.len() - 2], "ms")
        } else if s.ends_with("s") {
            (&s[..s.len() - 1], "s")
        } else if s.ends_with("m") {
            (&s[..s.len() - 1], "m")
        } else {
            // No unit specified, assume seconds
            (s, "s")
        };

        let number: f64 = number_str
            .parse()
            .map_err(|_| format!("Invalid number in duration: {}", s))?;

        let duration = match unit {
            "ms" => Duration::from_millis(number as u64),
            "s" => Duration::from_secs_f64(number),
            "m" => Duration::from_secs_f64(number * 60.0),
            _ => return Err(format!("Unknown unit: {}", unit)),
        };

        Ok(HumanDuration(duration))
    }
}

impl From<HumanDuration> for Duration {
    fn from(wrapper: HumanDuration) -> Self {
        wrapper.0
    }
}

impl From<Duration> for HumanDuration {
    fn from(duration: Duration) -> Self {
        HumanDuration(duration)
    }
}

impl<'de> serde::Deserialize<'de> for HumanDuration {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        struct DurationWrapperVisitor;

        impl<'de> serde::de::Visitor<'de> for DurationWrapperVisitor {
            type Value = HumanDuration;

            fn expecting(&self, formatter: &mut std::fmt::Formatter) -> std::fmt::Result {
                formatter.write_str("a number (seconds) or a string like '500ms', '5s', or '3m'")
            }

            fn visit_u64<E>(self, v: u64) -> Result<Self::Value, E>
            where
                E: serde::de::Error,
            {
                Ok(HumanDuration::from_secs(v))
            }

            fn visit_i64<E>(self, v: i64) -> Result<Self::Value, E>
            where
                E: serde::de::Error,
            {
                if v < 0 {
                    return Err(E::custom("negative values are not allowed"));
                }
                Ok(HumanDuration::from_secs(v as u64))
            }

            fn visit_f64<E>(self, v: f64) -> Result<Self::Value, E>
            where
                E: serde::de::Error,
            {
                if v < 0.0 {
                    return Err(E::custom("negative values are not allowed"));
                }
                Ok(HumanDuration::from_secs_f64(v))
            }

            fn visit_str<E>(self, v: &str) -> Result<Self::Value, E>
            where
                E: serde::de::Error,
            {
                HumanDuration::from_str(v).map_err(E::custom)
            }
        }

        deserializer.deserialize_any(DurationWrapperVisitor)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn parse_storage_config() {
        let toml_str = r#"
            [cache]
            db = "/path/to/cache.db"

            [watcher]
            max_parallel_hashers = 4
            debounce = "500ms"

            [[arena]]
            name = "arena1"
            workdir = "/path/to/arena1"
            datadir = "/path/to/arena1/data"

            [[arena]]
            name = "arena2"
            datadir = "/path/to/arena2/data"
            workdir = "/path/to/arena2"
            disk_usage = { max = "1G" }
        "#;

        let config: StorageConfig = toml::from_str(toml_str).unwrap();
        let expected_config = StorageConfig {
            cache: CacheConfig {
                db: PathBuf::from("/path/to/cache.db"),
            },
            watcher: WatcherConfig {
                max_parallel_hashers: Some(4),
                debounce: Some(HumanDuration::from_millis(500)),
            },
            arenas: vec![
                NamedArenaConfig {
                    arena: Arena::from("arena1"),
                    config: ArenaConfig {
                        workdir: PathBuf::from("/path/to/arena1"),
                        datadir: PathBuf::from("/path/to/arena1/data"),
                        disk_usage: DiskUsageConfig::default(),
                    },
                },
                NamedArenaConfig {
                    arena: Arena::from("arena2"),
                    config: ArenaConfig {
                        workdir: PathBuf::from("/path/to/arena2"),
                        datadir: PathBuf::from("/path/to/arena2/data"),
                        disk_usage: DiskUsageConfig {
                            max: Some(BytesOrPercent::Bytes(1073741824)),
                            leave: None,
                        },
                    },
                },
            ],
        };

        assert_eq!(config, expected_config);
    }

    #[test]
    fn parse_disk_usage() {
        #[derive(serde::Deserialize)]
        struct ConfigWithDiskUsage {
            disk_usage: DiskUsageConfig,
        }
        fn parse(str: &str) -> DiskUsageConfig {
            toml::from_str::<ConfigWithDiskUsage>(&format!("disk_usage = {{{str}}}"))
                .unwrap()
                .disk_usage
        }
        assert_eq!(
            parse(r#"max = 1610612736 "#),
            DiskUsageConfig {
                max: Some(BytesOrPercent::Bytes(1610612736)),
                leave: None,
            }
        );
        assert_eq!(parse(""), DiskUsageConfig::default());
        assert_eq!(
            parse(r#"max = "1.5G" "#),
            DiskUsageConfig {
                max: Some(BytesOrPercent::Bytes(1610612736)),
                leave: None,
            }
        );
        assert_eq!(
            parse(r#"max = "10%" "#),
            DiskUsageConfig {
                max: Some(BytesOrPercent::Percent(10)),
                leave: None,
            }
        );
        assert_eq!(
            parse(r#"max = "512M", leave = "5%" "#),
            DiskUsageConfig {
                max: Some(BytesOrPercent::Bytes(536870912)),
                leave: Some(BytesOrPercent::Percent(5))
            }
        );
    }

    #[test]
    fn test_bytes_or_percent_from_str() {
        // Test percentage parsing
        assert_eq!(
            BytesOrPercent::parse("10%").unwrap(),
            BytesOrPercent::Percent(10)
        );
        assert_eq!(
            BytesOrPercent::parse("0%").unwrap(),
            BytesOrPercent::Percent(0)
        );
        assert_eq!(
            BytesOrPercent::parse("100%").unwrap(),
            BytesOrPercent::Percent(100)
        );

        // Test byte parsing (no unit)
        assert_eq!(
            BytesOrPercent::parse("1024").unwrap(),
            BytesOrPercent::Bytes(1024)
        );
        assert_eq!(
            BytesOrPercent::parse("0").unwrap(),
            BytesOrPercent::Bytes(0)
        );

        // Test human-readable size parsing
        assert_eq!(
            BytesOrPercent::parse("1B").unwrap(),
            BytesOrPercent::Bytes(1)
        );
        assert_eq!(
            BytesOrPercent::parse("1KB").unwrap(),
            BytesOrPercent::Bytes(1024)
        );
        assert_eq!(
            BytesOrPercent::parse("1K").unwrap(),
            BytesOrPercent::Bytes(1024)
        );
        assert_eq!(
            BytesOrPercent::parse("1MB").unwrap(),
            BytesOrPercent::Bytes(1024 * 1024)
        );
        assert_eq!(
            BytesOrPercent::parse("1M").unwrap(),
            BytesOrPercent::Bytes(1024 * 1024)
        );
        assert_eq!(
            BytesOrPercent::parse("1GB").unwrap(),
            BytesOrPercent::Bytes(1024 * 1024 * 1024)
        );
        assert_eq!(
            BytesOrPercent::parse("1G").unwrap(),
            BytesOrPercent::Bytes(1024 * 1024 * 1024)
        );
        assert_eq!(
            BytesOrPercent::parse("1TB").unwrap(),
            BytesOrPercent::Bytes(1024 * 1024 * 1024 * 1024)
        );
        assert_eq!(
            BytesOrPercent::parse("1T").unwrap(),
            BytesOrPercent::Bytes(1024 * 1024 * 1024 * 1024)
        );

        // Test fractional values
        assert_eq!(
            BytesOrPercent::parse("1.5G").unwrap(),
            BytesOrPercent::Bytes(1610612736)
        );
        assert_eq!(
            BytesOrPercent::parse("0.5M").unwrap(),
            BytesOrPercent::Bytes(524288)
        );
        assert_eq!(
            BytesOrPercent::parse("2.5K").unwrap(),
            BytesOrPercent::Bytes(2560)
        );

        // Test error cases
        assert!(BytesOrPercent::parse("invalid").is_err());
        assert!(BytesOrPercent::parse("1.5X").is_err()); // Unknown unit
        assert!(BytesOrPercent::parse("10%invalid").is_err()); // Invalid percentage
        assert!(BytesOrPercent::parse("").is_err()); // Empty string
    }

    #[test]
    fn test_duration_wrapper_from_str() {
        // Test milliseconds parsing
        assert_eq!(
            HumanDuration::from_str("500ms").unwrap(),
            HumanDuration::from_millis(500)
        );
        assert_eq!(
            HumanDuration::from_str("1000ms").unwrap(),
            HumanDuration::from_millis(1000)
        );
        assert_eq!(
            HumanDuration::from_str("0ms").unwrap(),
            HumanDuration::from_millis(0)
        );

        // Test seconds parsing
        assert_eq!(
            HumanDuration::from_str("5s").unwrap(),
            HumanDuration::from_secs(5)
        );
        assert_eq!(
            HumanDuration::from_str("0s").unwrap(),
            HumanDuration::from_secs(0)
        );
        assert_eq!(
            HumanDuration::from_str("1.5s").unwrap(),
            HumanDuration::from_secs_f64(1.5)
        );

        // Test minutes parsing
        assert_eq!(
            HumanDuration::from_str("3m").unwrap(),
            HumanDuration::from_secs(180)
        );
        assert_eq!(
            HumanDuration::from_str("0m").unwrap(),
            HumanDuration::from_secs(0)
        );
        assert_eq!(
            HumanDuration::from_str("1.5m").unwrap(),
            HumanDuration::from_secs_f64(90.0)
        );

        // Test no unit specified (assumes seconds)
        assert_eq!(
            HumanDuration::from_str("5").unwrap(),
            HumanDuration::from_secs(5)
        );
        assert_eq!(
            HumanDuration::from_str("0").unwrap(),
            HumanDuration::from_secs(0)
        );
        assert_eq!(
            HumanDuration::from_str("1.5").unwrap(),
            HumanDuration::from_secs_f64(1.5)
        );

        // Test error cases
        assert!(HumanDuration::from_str("invalid").is_err());
        assert!(HumanDuration::from_str("1.5X").is_err()); // Unknown unit
        assert!(HumanDuration::from_str("").is_err()); // Empty string
        assert!(HumanDuration::from_str("5msinvalid").is_err()); // Invalid format
    }

    #[test]
    fn test_duration_wrapper_deserialization() {
        #[derive(serde::Deserialize)]
        struct ConfigWithDebounce {
            debounce: Option<HumanDuration>,
        }

        fn parse(str: &str) -> Option<HumanDuration> {
            toml::from_str::<ConfigWithDebounce>(str).unwrap().debounce
        }

        // Test number deserialization (seconds)
        assert_eq!(parse(r#"debounce = 5"#), Some(HumanDuration::from_secs(5)));
        assert_eq!(parse(r#"debounce = 0"#), Some(HumanDuration::from_secs(0)));
        assert_eq!(
            parse(r#"debounce = 1.5"#),
            Some(HumanDuration::from_secs_f64(1.5))
        );

        // Test string deserialization with units
        assert_eq!(
            parse(r#"debounce = "500ms""#),
            Some(HumanDuration::from_millis(500))
        );
        assert_eq!(
            parse(r#"debounce = "5s""#),
            Some(HumanDuration::from_secs(5))
        );
        assert_eq!(
            parse(r#"debounce = "3m""#),
            Some(HumanDuration::from_secs(180))
        );
        assert_eq!(
            parse(r#"debounce = "1.5s""#),
            Some(HumanDuration::from_secs_f64(1.5))
        );
        assert_eq!(
            parse(r#"debounce = "0.5m""#),
            Some(HumanDuration::from_secs_f64(30.0))
        );

        // Test missing field
        assert_eq!(parse(""), None);
    }

    #[test]
    fn test_duration_wrapper_conversions() {
        let duration = Duration::from_secs(5);
        let wrapper = HumanDuration::from(duration);
        let converted_duration: Duration = wrapper.into();

        assert_eq!(duration, converted_duration);
    }

    #[test]
    fn test_debounce_integration() {
        // Test all the formats mentioned in the user query
        let test_cases = vec![
            (r#"debounce = 5"#, Duration::from_secs(5)),    // 5s
            (r#"debounce = "5s""#, Duration::from_secs(5)), // 5s
            (r#"debounce = "5000ms""#, Duration::from_millis(5000)), // 5s
            (r#"debounce = "3m""#, Duration::from_secs(180)), // 3 minutes
        ];

        for (toml_str, expected_duration) in test_cases {
            let config: WatcherConfig = toml::from_str(&toml_str).unwrap();

            assert_eq!(
                config.debounce,
                Some(HumanDuration(expected_duration)),
                "Failed for TOML: {}",
                toml_str
            );
        }
    }
    #[test]
    fn parse_storage_config_wrong_field_name() {
        let toml_str = r#"
            [cache]
            db = "/path/to/cache.db"

            [[arena]]
            name = "arena1"
            wrongname = "/path/to/arena1" # should be root
            workdir = "/path/to/arena1/"
        "#;

        assert!(toml::from_str::<StorageConfig>(toml_str).is_err())
    }
}
