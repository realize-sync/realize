//! Realize - Symmetric File Syncer Library
//!
//! This crate provides the core library for the Realize file synchronization system.
//! It implements the service definition, algorithms, and utilities for robust, secure,
//! and restartable file syncing between hosts.
//!
//! # Main Components
//!
//! - `algo`: Sync and move algorithms, including rsync-based partial transfer.
//! - `client`: Client-side connection and reconnection logic.
//! - `metrics`: Prometheus metrics integration for monitoring.
//! - `model`: Core types, service definition, and range logic.
//! - `server`: Service implementation and directory management.
//! - `transport`: Secure, rate-limited, and reconnecting network transport.
//! - `utils`: Async and logging helpers.
//!
//! See the design doc for full details and usage examples.

pub mod algo;
pub mod client;
pub mod metrics;
pub mod model;
pub mod server;
pub mod transport;
pub mod utils;
