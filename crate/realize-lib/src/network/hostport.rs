use anyhow::Context as _;
use std::fmt;
use std::net::{IpAddr, SocketAddr};

/// HostPort represents a resolved host:port pair, with DNS resolution.
#[derive(Clone, PartialEq, Eq, Debug, Hash)]
pub struct HostPort {
    host: String,
    port: u16,
    addr: SocketAddr,
}

impl HostPort {
    /// Parse a host:port string, resolve DNS, and return a HostPort.
    pub async fn parse(s: &str) -> anyhow::Result<Self> {
        // Handle [::1]:port for IPv6
        let (host, port) = if let Some(idx) = s.rfind(':') {
            let (host, port_str) = s.split_at(idx);
            let port = port_str[1..]
                .parse::<u16>()
                .map_err(|_| anyhow::anyhow!("Invalid port in address: {s}"))?;
            let host = if host.starts_with('[') && host.ends_with(']') {
                &host[1..host.len() - 1]
            } else {
                host
            };
            (host.to_string(), port)
        } else {
            return Err(anyhow::anyhow!("Missing port in address: {s}"));
        };
        // DNS resolution
        let mut addrs = tokio::net::lookup_host(s)
            .await
            .with_context(|| format!("DNS lookup failed for {s}"))?;
        let addr = addrs
            .next()
            .ok_or_else(|| anyhow::anyhow!("DNS lookup failed for {s}"))?;
        Ok(HostPort { host, port, addr })
    }
    pub fn host(&self) -> &str {
        &self.host
    }
    pub fn port(&self) -> u16 {
        self.port
    }
    pub fn addr(&self) -> SocketAddr {
        self.addr
    }
}

impl fmt::Display for HostPort {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}:{}", self.host, self.port)
    }
}

impl From<SocketAddr> for HostPort {
    fn from(addr: SocketAddr) -> Self {
        let host = match addr.ip() {
            IpAddr::V4(ip) => ip.to_string(),
            IpAddr::V6(ip) => ip.to_string(),
        };
        let port = addr.port();
        HostPort { host, port, addr }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_parse_ipv4() {
        let hp = HostPort::parse("127.0.0.1:8000").await.unwrap();
        assert_eq!(hp.host(), "127.0.0.1");
        assert_eq!(hp.port(), 8000);
        assert_eq!(hp.addr().port(), 8000);
    }

    #[tokio::test]
    async fn test_parse_ipv6() {
        let hp = HostPort::parse("[::1]:8000").await.unwrap();
        assert_eq!(hp.host(), "::1");
        assert_eq!(hp.port(), 8000);
        assert_eq!(hp.addr().port(), 8000);
    }

    #[tokio::test]
    async fn test_parse_hostname() {
        let hp = HostPort::parse("localhost:1234").await.unwrap();
        assert_eq!(hp.host(), "localhost");
        assert_eq!(hp.port(), 1234);
        assert_eq!(hp.addr().port(), 1234);

        let hp = HostPort::parse("www.google.com:1234").await.unwrap();
        assert_eq!(hp.host(), "www.google.com");
        assert_eq!(hp.port(), 1234);
        assert_eq!(hp.addr().port(), 1234);
    }

    #[tokio::test]
    async fn test_parse_invalid() {
        assert!(HostPort::parse("myhost").await.is_err());
        assert!(HostPort::parse("doesnotexist:1000").await.is_err());
    }

    #[tokio::test]
    async fn test_from_socketaddr() {
        let hp1 = HostPort::parse("127.0.0.1:8000").await.unwrap();
        let hp2 = HostPort::from(hp1.addr());
        assert_eq!(hp1, hp2);
    }

    #[tokio::test]
    async fn test_display() {
        let hp = HostPort::parse("127.0.0.1:8000").await.unwrap();
        assert_eq!(format!("{}", hp), "127.0.0.1:8000");
    }
}