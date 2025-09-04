use realize_types::Peer;

#[derive(Clone, serde::Deserialize, serde::Serialize, Debug, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct NetworkConfig {
    #[serde(rename = "peer")]
    pub peers: Vec<PeerConfig>,
}

impl Default for NetworkConfig {
    fn default() -> Self {
        Self::new()
    }
}

impl NetworkConfig {
    pub fn new() -> Self {
        NetworkConfig { peers: Vec::new() }
    }

    /// Get peer config by name
    pub fn peer_config(&self, peer: Peer) -> Option<&PeerConfig> {
        self.peers.iter().find(|c| c.peer == peer)
    }

    /// Get peer config by name (mutable)
    pub fn peer_config_mut(&mut self, peer: Peer) -> Option<&mut PeerConfig> {
        self.peers.iter_mut().find(|c| c.peer == peer)
    }
}

/// Define a peer.
///
/// A peer is identified by [realize_types::Peer].
#[derive(Clone, serde::Deserialize, serde::Serialize, Debug, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct PeerConfig {
    /// The name of this peer.
    #[serde(rename = "name")]
    pub peer: Peer,

    /// Address of the peer, if available.
    ///
    /// Not all peers can be connected to. Peers that can should have
    /// their address listed here.
    pub address: Option<String>,

    /// Specify the peer's public key that'll be used to identify
    /// the peer during connection.
    ///
    /// Must be a PEM-encoded ED25519 public key.
    pub pubkey: String,

    /// Rate limit that applies to batch operations.
    ///
    /// Interactive operations are not limited.
    /// Can be specified as a plain number (bytes) or with units (e.g., "1K", "1MB").
    pub batch_rate_limit: Option<ByteValue>,
}

/// A byte value that can be parsed from human-readable units or plain numbers.
///
/// Supports units: B, K, KB, M, MB, G, GB, T, TB
/// Examples: "1000", "1K", "1KB", "1M", "1MB", "1G", "1GB", "1T", "1TB"
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ByteValue(pub u64);

impl ByteValue {
    /// Parse a string containing a number with optional unit suffix.
    ///
    /// # Arguments
    ///
    /// * `input` - String to parse (e.g., "1000", "1K", "1MB")
    ///
    /// # Returns
    ///
    /// * `Ok(u64)` - The parsed value in bytes
    /// * `Err(String)` - Error message if parsing fails
    pub fn parse(input: &str) -> Result<u64, String> {
        let input = input.trim();

        // Handle plain numbers
        if let Ok(value) = input.parse::<u64>() {
            return Ok(value);
        }

        // Handle values with units
        let (number_str, unit) = Self::split_number_and_unit(input)?;
        let number: u64 = number_str
            .parse()
            .map_err(|_| format!("Invalid number: {}", number_str))?;

        let multiplier = match unit.to_uppercase().as_str() {
            "B" => 1,
            "K" => 1024,
            "KB" => 1024,
            "M" => 1024 * 1024,
            "MB" => 1024 * 1024,
            "G" => 1024 * 1024 * 1024,
            "GB" => 1024 * 1024 * 1024,
            "T" => 1024 * 1024 * 1024 * 1024,
            "TB" => 1024 * 1024 * 1024 * 1024,
            _ => return Err(format!("Unknown unit: {}", unit)),
        };

        Ok(number * multiplier)
    }

    /// Split a string into number and unit parts.
    ///
    /// # Arguments
    ///
    /// * `input` - String to split (e.g., "1K", "512MB")
    ///
    /// # Returns
    ///
    /// * `Ok((String, String))` - Tuple of (number_str, unit)
    /// * `Err(String)` - Error message if splitting fails
    fn split_number_and_unit(input: &str) -> Result<(String, String), String> {
        let mut number_end = 0;
        let chars: Vec<char> = input.chars().collect();

        // Find where the number ends
        for (i, &ch) in chars.iter().enumerate() {
            if ch.is_ascii_digit() {
                number_end = i + 1;
            } else {
                break;
            }
        }

        if number_end == 0 {
            return Err("No number found".to_string());
        }

        let number_str: String = chars[..number_end].iter().collect();
        let unit: String = chars[number_end..].iter().collect();

        if unit.is_empty() {
            return Err("No unit found".to_string());
        }

        Ok((number_str, unit))
    }
}

impl<'de> serde::Deserialize<'de> for ByteValue {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        use serde::de::{self, Visitor};
        use std::fmt;

        struct ByteValueVisitor;

        impl<'de> Visitor<'de> for ByteValueVisitor {
            type Value = ByteValue;

            fn expecting(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
                formatter
                    .write_str("a number or string with byte units (e.g., 1000, \"1K\", \"1MB\")")
            }

            fn visit_u64<E>(self, value: u64) -> Result<Self::Value, E>
            where
                E: de::Error,
            {
                Ok(ByteValue(value))
            }

            fn visit_i64<E>(self, value: i64) -> Result<Self::Value, E>
            where
                E: de::Error,
            {
                if value < 0 {
                    return Err(de::Error::custom("negative values are not allowed"));
                }
                Ok(ByteValue(value as u64))
            }

            fn visit_str<E>(self, value: &str) -> Result<Self::Value, E>
            where
                E: de::Error,
            {
                let bytes = ByteValue::parse(value).map_err(|e| de::Error::custom(e))?;
                Ok(ByteValue(bytes))
            }
        }

        deserializer.deserialize_any(ByteValueVisitor)
    }
}

impl serde::Serialize for ByteValue {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        serializer.serialize_u64(self.0)
    }
}

impl From<u64> for ByteValue {
    fn from(value: u64) -> Self {
        ByteValue(value)
    }
}

impl From<ByteValue> for u64 {
    fn from(value: ByteValue) -> Self {
        value.0
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_byte_value_parse_plain_numbers() {
        assert_eq!(ByteValue::parse("1000").unwrap(), 1000);
        assert_eq!(ByteValue::parse("0").unwrap(), 0);
        assert_eq!(ByteValue::parse("123456789").unwrap(), 123456789);
    }

    #[test]
    fn test_byte_value_parse_with_units() {
        // Test B (bytes)
        assert_eq!(ByteValue::parse("100B").unwrap(), 100);
        assert_eq!(ByteValue::parse("100b").unwrap(), 100);

        // Test K (kilobytes)
        assert_eq!(ByteValue::parse("1K").unwrap(), 1024);
        assert_eq!(ByteValue::parse("1k").unwrap(), 1024);
        assert_eq!(ByteValue::parse("512K").unwrap(), 512 * 1024);

        // Test KB (kilobytes)
        assert_eq!(ByteValue::parse("1KB").unwrap(), 1024);
        assert_eq!(ByteValue::parse("1kb").unwrap(), 1024);
        assert_eq!(ByteValue::parse("2KB").unwrap(), 2 * 1024);

        // Test M (megabytes)
        assert_eq!(ByteValue::parse("1M").unwrap(), 1024 * 1024);
        assert_eq!(ByteValue::parse("1m").unwrap(), 1024 * 1024);
        assert_eq!(ByteValue::parse("10M").unwrap(), 10 * 1024 * 1024);

        // Test MB (megabytes)
        assert_eq!(ByteValue::parse("1MB").unwrap(), 1024 * 1024);
        assert_eq!(ByteValue::parse("1mb").unwrap(), 1024 * 1024);
        assert_eq!(ByteValue::parse("5MB").unwrap(), 5 * 1024 * 1024);

        // Test G (gigabytes)
        assert_eq!(ByteValue::parse("1G").unwrap(), 1024 * 1024 * 1024);
        assert_eq!(ByteValue::parse("1g").unwrap(), 1024 * 1024 * 1024);
        assert_eq!(ByteValue::parse("2G").unwrap(), 2 * 1024 * 1024 * 1024);

        // Test GB (gigabytes)
        assert_eq!(ByteValue::parse("1GB").unwrap(), 1024 * 1024 * 1024);
        assert_eq!(ByteValue::parse("1gb").unwrap(), 1024 * 1024 * 1024);
        assert_eq!(ByteValue::parse("3GB").unwrap(), 3 * 1024 * 1024 * 1024);

        // Test T (terabytes)
        assert_eq!(ByteValue::parse("1T").unwrap(), 1024 * 1024 * 1024 * 1024);
        assert_eq!(ByteValue::parse("1t").unwrap(), 1024 * 1024 * 1024 * 1024);
        assert_eq!(
            ByteValue::parse("2T").unwrap(),
            2 * 1024 * 1024 * 1024 * 1024
        );

        // Test TB (terabytes)
        assert_eq!(ByteValue::parse("1TB").unwrap(), 1024 * 1024 * 1024 * 1024);
        assert_eq!(ByteValue::parse("1tb").unwrap(), 1024 * 1024 * 1024 * 1024);
        assert_eq!(
            ByteValue::parse("4TB").unwrap(),
            4 * 1024 * 1024 * 1024 * 1024
        );
    }

    #[test]
    fn test_byte_value_parse_with_whitespace() {
        assert_eq!(ByteValue::parse(" 1000 ").unwrap(), 1000);
        assert_eq!(ByteValue::parse(" 1K ").unwrap(), 1024);
        assert_eq!(ByteValue::parse(" 512MB ").unwrap(), 512 * 1024 * 1024);
    }

    #[test]
    fn test_byte_value_parse_errors() {
        // Invalid units
        assert!(ByteValue::parse("1X").is_err());
        assert!(ByteValue::parse("1KBX").is_err());
        assert!(ByteValue::parse("1MKB").is_err());

        // No number
        assert!(ByteValue::parse("K").is_err());
        assert!(ByteValue::parse("MB").is_err());
        assert!(ByteValue::parse("").is_err());
        assert!(ByteValue::parse("   ").is_err());

        // Invalid number
        assert!(ByteValue::parse("abcK").is_err());
        assert!(ByteValue::parse("12.34K").is_err());
        assert!(ByteValue::parse("-1K").is_err());
    }

    #[test]
    fn test_byte_value_serde() {
        // Test deserialization from number
        let toml_str = r#"value = 1000"#;
        let parsed: std::collections::HashMap<String, ByteValue> =
            toml::from_str(toml_str).unwrap();
        assert_eq!(parsed["value"], ByteValue(1000));

        // Test deserialization from string
        let toml_str = r#"value = "1K""#;
        let parsed: std::collections::HashMap<String, ByteValue> =
            toml::from_str(toml_str).unwrap();
        assert_eq!(parsed["value"], ByteValue(1024));

        // Test serialization in a struct
        #[derive(serde::Serialize)]
        struct TestStruct {
            value: ByteValue,
        }

        let test_struct = TestStruct {
            value: ByteValue(2048),
        };
        let toml_str = toml::to_string(&test_struct).unwrap();
        assert!(toml_str.contains("2048"));
    }

    #[test]
    fn test_byte_value_conversions() {
        let value = ByteValue(1024);
        let u64_value: u64 = value.into();
        assert_eq!(u64_value, 1024);

        let value = ByteValue::from(2048);
        assert_eq!(value, ByteValue(2048));
    }

    #[test]
    fn parse_network_config() {
        let toml_str = r#"
            [[peer]]
            name = "peer1"
            address = "192.168.1.100:8080"
            pubkey = "-----BEGIN PUBLIC KEY-----\nMIIBIjANBgkqhkiG9w0BAQEFAAOCAQ8AMIIBCgKCAQEA...\n-----END PUBLIC KEY-----"
            batch_rate_limit = 1000

            [[peer]]
            name = "peer2"
            pubkey = "-----BEGIN PUBLIC KEY-----\nMIIBIjANBgkqhkiG9w0BAQEFAAOCAQ8AMIIBCgKCAQEA...\n-----END PUBLIC KEY-----"

            [[peer]]
            name = "peer3"
            address = "10.0.0.1:9000"
            pubkey = "-----BEGIN PUBLIC KEY-----\nMIIBIjANBgkqhkiG9w0BAQEFAAOCAQ8AMIIBCgKCAQEA...\n-----END PUBLIC KEY-----"
            batch_rate_limit = "512K"
        "#;

        let config: NetworkConfig = toml::from_str(toml_str).unwrap();

        assert_eq!(config, NetworkConfig {
            peers: vec![
                PeerConfig {
                    peer: Peer::from("peer1"),
                    address: Some("192.168.1.100:8080".to_string()),
                    pubkey: "-----BEGIN PUBLIC KEY-----\nMIIBIjANBgkqhkiG9w0BAQEFAAOCAQ8AMIIBCgKCAQEA...\n-----END PUBLIC KEY-----".to_string(),
                    batch_rate_limit: Some(ByteValue(1000)),
                },
                PeerConfig {
                    peer: Peer::from("peer2"),
                    address: None,
                    pubkey: "-----BEGIN PUBLIC KEY-----\nMIIBIjANBgkqhkiG9w0BAQEFAAOCAQ8AMIIBCgKCAQEA...\n-----END PUBLIC KEY-----".to_string(),
                    batch_rate_limit: None,
                },
                PeerConfig {
                    peer: Peer::from("peer3"),
                    address: Some("10.0.0.1:9000".to_string()),
                    pubkey: "-----BEGIN PUBLIC KEY-----\nMIIBIjANBgkqhkiG9w0BAQEFAAOCAQ8AMIIBCgKCAQEA...\n-----END PUBLIC KEY-----".to_string(),
                    batch_rate_limit: Some(ByteValue(512*1024)),
                },
            ],
        });
    }

    #[test]
    fn wrong_field_name() {
        let toml_str = r#"
            [[peer]]
            name = "peer1"
            pubkey = "..."
            wrong_field_name = 1 
        "#;

        assert!(toml::from_str::<NetworkConfig>(toml_str).is_err());
    }
}
