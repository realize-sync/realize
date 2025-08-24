use realize_types::Arena;
use std::collections::HashMap;
use std::path::PathBuf;
use std::time::Duration;

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

/// Storage configuration.
#[derive(Clone, serde::Deserialize, serde::Serialize, Debug, PartialEq, Eq)]
pub struct StorageConfig {
    pub arenas: HashMap<Arena, ArenaConfig>,
    pub cache: CacheConfig,
}

impl StorageConfig {
    pub fn new<P>(cache_db: P) -> Self
    where
        P: AsRef<std::path::Path>,
    {
        StorageConfig {
            arenas: HashMap::new(),
            cache: CacheConfig {
                db: cache_db.as_ref().to_path_buf(),
            },
        }
    }
}

/// For the global cache (no blob_dir)
#[derive(Clone, serde::Deserialize, serde::Serialize, Debug, PartialEq, Eq)]
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

#[derive(Clone, serde::Deserialize, serde::Serialize, Debug, Default, PartialEq, Eq)]
pub struct ArenaConfig {
    /// Optional local path to the directory where files for that arena are stored.
    /// If specified, an indexer will be created for this arena.
    pub root: Option<PathBuf>,
    /// Path to the database that contains both index and cache data (required for arena cache).
    pub db: PathBuf,
    /// Path to the directory where blob files are stored (required for arena cache).
    pub blob_dir: PathBuf,

    /// Maximum number of hashers running in parallel.
    ///
    /// Hashing is CPU intensive, so hashing several large files in
    /// parallel can become a problem. It's a good idea to limit
    /// parallelism to a fraction of the available cores.
    pub max_parallel_hashers: Option<usize>,

    /// Set debounce delay for hashing files. This allows some time for
    /// operations in progress to finish.
    pub debounce: Option<HumanDuration>,

    /// Limits how much disk space will be used to store local copies
    /// of remote data.
    ///
    /// Note that it might not be possible to enforce this limitation:
    /// if the size of files to keep goes above that limit, those
    /// files are kept anyways.
    pub disk_usage: Option<DiskUsageLimits>,
}

impl ArenaConfig {
    pub fn new<P1, P2, P3>(root: P1, db: P2, blob_dir: P3) -> Self
    where
        P1: AsRef<std::path::Path>,
        P2: AsRef<std::path::Path>,
        P3: AsRef<std::path::Path>,
    {
        Self {
            root: Some(root.as_ref().to_path_buf()),
            db: db.as_ref().to_path_buf(),
            blob_dir: blob_dir.as_ref().to_path_buf(),
            ..Default::default()
        }
    }

    /// Configure an arena without local root folder.
    pub fn rootless<P1, P2>(db: P1, blob_dir: P2) -> Self
    where
        P1: AsRef<std::path::Path>,
        P2: AsRef<std::path::Path>,
    {
        Self {
            db: db.as_ref().to_path_buf(),
            blob_dir: blob_dir.as_ref().to_path_buf(),
            ..Default::default()
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq, serde::Deserialize, serde::Serialize)]
pub struct DiskUsageLimits {
    /// Try to use at most that many bytes or percent of disk.
    ///
    /// The cache can temporarily go above that value.
    pub max: BytesOrPercent,

    /// Reduce disk usage to keep at keep that many bytes or percent
    /// of the disk free on the disk.
    ///
    /// This is applied after the `max` value.
    pub leave: Option<BytesOrPercent>,
}

impl DiskUsageLimits {
    pub fn max_bytes(v: u64) -> DiskUsageLimits {
        Self {
            max: BytesOrPercent::Bytes(v),
            leave: None,
        }
    }
    pub fn max_percent(v: u32) -> DiskUsageLimits {
        Self {
            max: BytesOrPercent::Percent(v),
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
    /// Parse a string that can be either a percentage (e.g., "10%") or a human-readable size (e.g., "1.5G", "512M")
    fn from_str(s: &str) -> Result<Self, String> {
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
                BytesOrPercent::from_str(v).map_err(E::custom)
            }
        }

        deserializer.deserialize_any(BytesOrPercentVisitor)
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

            [arenas."arena1"]
            root = "/path/to/arena1"
            db = "/path/to/arena1.db"
            blob_dir = "/path/to/arena1/blobs"
            max_parallel_hashers = 4
            debounce = "500ms"

            [arenas."arena2"]
            db = "/path/to/arena2.db"
            blob_dir = "/path/to/arena2/blobs"
            disk_usage = { max = "1G" }
        "#;

        let config: StorageConfig = toml::from_str(toml_str).unwrap();
        let expected_config = StorageConfig {
            cache: CacheConfig {
                db: PathBuf::from("/path/to/cache.db"),
            },
            arenas: HashMap::from([
                (
                    Arena::from("arena1"),
                    ArenaConfig {
                        root: Some(PathBuf::from("/path/to/arena1")),
                        db: PathBuf::from("/path/to/arena1.db"),
                        blob_dir: PathBuf::from("/path/to/arena1/blobs"),
                        max_parallel_hashers: Some(4),
                        debounce: Some(HumanDuration::from_millis(500)),
                        disk_usage: None,
                    },
                ),
                (
                    Arena::from("arena2"),
                    ArenaConfig {
                        root: None,
                        db: PathBuf::from("/path/to/arena2.db"),
                        blob_dir: PathBuf::from("/path/to/arena2/blobs"),
                        max_parallel_hashers: None,
                        debounce: None,
                        disk_usage: Some(DiskUsageLimits {
                            max: BytesOrPercent::Bytes(1073741824),
                            leave: None,
                        }),
                    },
                ),
            ]),
        };

        assert_eq!(config, expected_config);
    }

    #[test]
    fn parse_disk_usage() {
        #[derive(serde::Deserialize)]
        struct ConfigWithDiskUsage {
            disk_usage: Option<DiskUsageLimits>,
        }
        fn parse(str: &str) -> Option<DiskUsageLimits> {
            toml::from_str::<ConfigWithDiskUsage>(str)
                .unwrap()
                .disk_usage
        }
        assert_eq!(
            parse(r#"disk_usage = { max = 1610612736 }"#),
            Some(DiskUsageLimits {
                max: BytesOrPercent::Bytes(1610612736),
                leave: None,
            })
        );
        assert_eq!(parse(""), None);
        assert_eq!(
            parse(r#"disk_usage = { max = "1.5G" }"#),
            Some(DiskUsageLimits {
                max: BytesOrPercent::Bytes(1610612736),
                leave: None,
            })
        );
        assert_eq!(
            parse(r#"disk_usage = { max = "10%" }"#),
            Some(DiskUsageLimits {
                max: BytesOrPercent::Percent(10),
                leave: None,
            })
        );
        assert_eq!(
            parse(r#"disk_usage = { max = "512M", leave = "5%" }"#),
            Some(DiskUsageLimits {
                max: BytesOrPercent::Bytes(536870912),
                leave: Some(BytesOrPercent::Percent(5))
            })
        );
    }

    #[test]
    fn test_bytes_or_percent_from_str() {
        // Test percentage parsing
        assert_eq!(
            BytesOrPercent::from_str("10%").unwrap(),
            BytesOrPercent::Percent(10)
        );
        assert_eq!(
            BytesOrPercent::from_str("0%").unwrap(),
            BytesOrPercent::Percent(0)
        );
        assert_eq!(
            BytesOrPercent::from_str("100%").unwrap(),
            BytesOrPercent::Percent(100)
        );

        // Test byte parsing (no unit)
        assert_eq!(
            BytesOrPercent::from_str("1024").unwrap(),
            BytesOrPercent::Bytes(1024)
        );
        assert_eq!(
            BytesOrPercent::from_str("0").unwrap(),
            BytesOrPercent::Bytes(0)
        );

        // Test human-readable size parsing
        assert_eq!(
            BytesOrPercent::from_str("1B").unwrap(),
            BytesOrPercent::Bytes(1)
        );
        assert_eq!(
            BytesOrPercent::from_str("1KB").unwrap(),
            BytesOrPercent::Bytes(1024)
        );
        assert_eq!(
            BytesOrPercent::from_str("1K").unwrap(),
            BytesOrPercent::Bytes(1024)
        );
        assert_eq!(
            BytesOrPercent::from_str("1MB").unwrap(),
            BytesOrPercent::Bytes(1024 * 1024)
        );
        assert_eq!(
            BytesOrPercent::from_str("1M").unwrap(),
            BytesOrPercent::Bytes(1024 * 1024)
        );
        assert_eq!(
            BytesOrPercent::from_str("1GB").unwrap(),
            BytesOrPercent::Bytes(1024 * 1024 * 1024)
        );
        assert_eq!(
            BytesOrPercent::from_str("1G").unwrap(),
            BytesOrPercent::Bytes(1024 * 1024 * 1024)
        );
        assert_eq!(
            BytesOrPercent::from_str("1TB").unwrap(),
            BytesOrPercent::Bytes(1024 * 1024 * 1024 * 1024)
        );
        assert_eq!(
            BytesOrPercent::from_str("1T").unwrap(),
            BytesOrPercent::Bytes(1024 * 1024 * 1024 * 1024)
        );

        // Test fractional values
        assert_eq!(
            BytesOrPercent::from_str("1.5G").unwrap(),
            BytesOrPercent::Bytes(1610612736)
        );
        assert_eq!(
            BytesOrPercent::from_str("0.5M").unwrap(),
            BytesOrPercent::Bytes(524288)
        );
        assert_eq!(
            BytesOrPercent::from_str("2.5K").unwrap(),
            BytesOrPercent::Bytes(2560)
        );

        // Test error cases
        assert!(BytesOrPercent::from_str("invalid").is_err());
        assert!(BytesOrPercent::from_str("1.5X").is_err()); // Unknown unit
        assert!(BytesOrPercent::from_str("10%invalid").is_err()); // Invalid percentage
        assert!(BytesOrPercent::from_str("").is_err()); // Empty string
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
            let full_toml = format!(
                r#"
                [cache]
                db = "/tmp/test.db"
                
                [arenas."test"]
                root = "/tmp/test"
                db = "/tmp/test.db"
                blob_dir = "/tmp/test/blobs"
                {}
                "#,
                toml_str
            );

            let config: StorageConfig = toml::from_str(&full_toml).unwrap();
            let arena_config = config.arenas.get(&Arena::from("test")).unwrap();

            assert_eq!(
                arena_config.debounce,
                Some(HumanDuration(expected_duration)),
                "Failed for TOML: {}",
                toml_str
            );
        }
    }
}
