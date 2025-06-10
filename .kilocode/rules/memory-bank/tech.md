# Technical Stack and Dependencies: Realize File Syncer

## Core Language and Runtime

*   **Rust:** The primary programming language used for the entire project, chosen for its performance, safety, and concurrency features.
*   **Tokio:** Asynchronous runtime for managing concurrent operations, network I/O, and file system interactions.

## Errors

*   Reuse existing single error types if possible (For example, if
    writing a function that fails only because of io::Error, return a
    Result<..., io::Error>)

*   Otherwise, return anyhow::Error, through anyhow::Result<...>

*   In error code, always return anyhow::Result, especially in test
    functions

*   Special case: RPC service must have their own error type that is
    serializable. Build it with thiserror, to group by type, but don't
    store the original error. See implementation of RealStoreServiceError.

## Key Crates and Libraries

### Networking & RPC

*   **`tarpc`**: Used for defining and implementing the RPC (Remote Procedure Call) services between peers (e.g., `RealStoreService`). Provides typed, asynchronous communication.
*   **`rustls` / `tokio-rustls`**: For implementing TLS 1.3 to secure peer-to-peer connections using raw ED25519 public key verification.
*   **`hyper` / `hyper-util`**: Used by `prometheus` for exposing metrics via HTTP and potentially for other HTTP client/server needs.
*   **`async-speed-limit`**: For throttling network upload and download speeds.

### File Synchronization & Hashing

*   **`fast_rsync`**: Implements rsync-like differential synchronization, allowing for efficient transfer of file updates by sending only deltas.
*   **`blake2` / `sha2`**: Hashing algorithms (BLAKE2b-256 is primarily used via `utils/hash.rs`) for file integrity verification and generating `Hash` objects.
*   **`base64`**: For encoding hashes and other binary data into string representations.

### Filesystem & Storage

*   **`walkdir`**: For traversing directory trees.
*   **`pathdiff`**: For calculating relative paths.
*   **`redb`**: (Planned/Specified) Key-value store for the "Unreal" cache metadata and blob storage.
*   **`nfsserve`**: (Planned/Specified) For exporting the "Unreal" cache as an NFSv3 filesystem.
*   **OverlayFS (Kernel)**: (Planned/Specified for Linux) For merging "Real" and "Unreal" filesystems.

### Command Line Interface & Configuration

*   **`clap`**: For parsing command-line arguments in `realize-cmd` and `realize-daemon`.
*   **`indicatif` / `console`**: For displaying progress bars and rich console output in `realize-cmd`.
*   **`toml`**: For parsing TOML configuration files (e.g., daemon config, household definition).
*   **`parse-size` / `humantime`**: For parsing human-readable sizes and durations from CLI arguments.

### Error Handling & Logging

*   **`anyhow` / `thiserror`**: For robust and ergonomic error handling throughout the codebase.
*   **`log` / `env_logger`**: Standard logging facade and implementation, allowing configurable log levels.
*   **`signal-hook` / `signal-hook-tokio`**: For handling OS signals (e.g., Ctrl-C) gracefully.

### Metrics & Monitoring

*   **`prometheus`**: For collecting and exposing application metrics (e.g., RPC call counts, data transferred). Supports both pull (HTTP endpoint) and push (Pushgateway) modes.

### Development & Testing

*   **`assert_cmd`**: For testing command-line applications (`realize-cmd`, `realize-daemon`).
*   **`assert_fs` / `tempfile`**: For creating temporary files and directories for testing filesystem operations.
*   **`predicates`**: For writing expressive assertions in tests.
*   **`portpicker`**: For selecting available network ports during testing.
*   **`reqwest`**: HTTP client for testing metrics endpoints or other HTTP interactions.
*   **`serial_test`**: For running tests that require exclusive access to resources serially.
*   **`test-tag`**: For tagging and filtering tests.
*   **`lazy_static`**: For declaring static variables with non-const initializers (e.g., for global metrics registries).

## Technical Constraints & Patterns

*   **Asynchronous Everywhere:** All I/O operations (network, filesystem) are asynchronous, managed by Tokio.
*   **Custom Path Type:** [`model::Path`](crate/realize-lib/src/model/path.rs) is used internally, with conversions to/from `std::path::PathBuf`.
*   **Raw Public Key TLS:** Security relies on direct verification of peer ED25519 public keys, bypassing traditional Certificate Authorities.
*   **Partial File Handling:** Files being transferred are often stored with a `.partial` suffix until complete; this is managed via a PathResolver, obtained from LocalStorage.
*   **Modularity:** The `realize-lib` crate is divided into `model`, `network`, `storage`, `logic`, and `utils` modules.
*   **Configuration via TOML:** Daemon settings, peer lists, and arena configurations are managed through TOML files.
