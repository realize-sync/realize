# Project Framework and Dependencies

# Production Code

- Code is written in Rust, Edition 2024.
- Use `tokio` and async/await for most code (All except code interfacing with redb and test code)
- Use `redb`, a key/value database, to store local state
- Use `env_logger` and `log`. `env_logger` logging is configured using RUST_LOG
- Use `anyhow` for errors that are just forwarded and `thiserror` to create error types that can be caught and transformed
- Use `capnp` for serialization and `capnp-rpc` for RPC. (Don't use `tarpc` anymore outside of legacy code.)
- Use `moka` for caching
- Use `rustls` and TLS 1.3 for security
- Use `async-walkdir` to go through directories (Don't use `walkdir` anymore outside of legacy code)

# Test Code

- In tests: Use `assert_fs::TempDir` to create temporary directories
- Use `assert_unordered::assert_eq_unordered!(expected, got)` to compare vectors while ignoring order
- Use `portpicker` to choose a part, when necessary
