//! Transport layer for Realize - Symmetric File Syncer
//!
//! This module provides secure, rate-limited, and reconnecting network transport for the RealizeService.
//! - `security`: TLS and peer authentication utilities.
//! - `tcp`: TCP transport and connection management.
//! - `rate_limit`: Bandwidth limiting for streams.
pub(crate) mod rate_limit;
pub mod security;
pub mod tcp;
