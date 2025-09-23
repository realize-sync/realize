# WARP.md

This file provides guidance to WARP (warp.dev) when working with code in this repository.

## Commands

### Build and Development
```bash
# Install build dependencies (Linux)
sudo apt-get install capnproto libcapnp-dev fuse3 libfuse3-dev

# Install build dependencies (macOS)
brew install capnp fuse-t

# Build the project
cargo build

# Run all tests
cargo nextest run -P ci

# Run crate-specific test (Example with crate-storage; substitute as appropriate)
cargo nextest run -p crate-storage -P ci

# Run a specific test or a subset of the tests (Example with the cache module; substitute as appropriate)
cargo nextest run -p crate-storage cache

# Format code
cargo fmt

# Check code in crate (required after every change) (Example with crate-storage; substitute as appropriate)
cargo check -p crate-storage
```

The project doesn't use a CHANGELOG. It relies instead on conventional
commits to track changes. Commits that introduce backward-incompatibly
changes are marked with an exclamation mark in their short comment.

cargo nextest has two profile:
 - the ci profile, selected with -P ci, which has 2m timeouts for tests and retries flaky tests once
 - the default profile, which has shorter test timeouts and no retries

### Testing
```bash
# Run specific test module in test_crate
cargo nextest run -p test_crate test_module_name

# Run integration tests only
cargo nextest run integration_test

# Run tests with debug logging
RUST_LOG=realize_=debug cargo next run

# Run single test with full output
cargo nextest run test_name --nocapture

# Debug FUSE tests with detailed logging
RUST_LOG=realize_=debug,fuser=debug cargo nexttest -p realize-core fuse_test_name --nocapture

# Run only FUSE tests (Linux only)
cargo nextest run -p realize-core fuse
```

### Executables
```bash
# Run the daemon
cargo run --bin realized

# Run the control CLI
cargo run --bin realize
```

## Project Architecture

### High-Level Overview
Realize is a decentralized file-syncing solution that presents remote files as a unified filesystem. The architecture is built around two key concepts:

- **The Real**: Files available locally and complete, stored as regular files
- **The Unreal**: Files not completely available locally, kept in a key-value store and served from cache

### Crate Structure
The project uses a workspace architecture with these core crates:

- **realize-core**: Central business logic, filesystem operations, consensus mechanisms, and RPC server implementation
- **realize-daemon** (`realized` binary): Background service that runs the file sync daemon
- **realize-control** (`realize` binary): Command-line interface for interacting with the daemon
- **realize-network**: Network communication layer using Cap'n Proto RPC
- **realize-storage**: Data storage layer with arena-based storage and caching
- **realize-types**: Shared type definitions used across all crates

### Key Architectural Patterns

#### Arena-Based Storage
Files are organized into "Arenas" - logical groupings of files that can be synced independently. Each peer can choose which arenas to participate in and which files to keep locally vs. in cache.

#### Real/Unreal Filesystem Layers
- **Real Layer**: Complete files stored locally on the filesystem
- **Unreal Layer**: Partial/remote files served from cache and key-value store
- Files move between layers based on usage, consensus, and local storage policies

#### Consensus Mechanism
Peers synchronize changes through a consensus protocol where:
- Local modifications are tracked in history
- History is shared between trusted peers
- Conflicts are resolved through configurable strategies
- Files transition from Real to Unreal after consensus

#### Cap'n Proto RPC
All network communication uses Cap'n Proto for:
- Efficient binary serialization
- Schema evolution
- RPC between daemon and control client
- Peer-to-peer communication

## Development Rules and Standards

### Code Quality
- **Always run `cargo check` after every code change** - fix all errors and warnings
- **Always run `cargo nextest -p crate -P ci` after every code change** - fix all test failures
- Run `cargo fmt` before committing any Rust files
- Use `cargo nextest run -P ci` for improved test running with timeouts and retries

### Module Organization
- **Never create `mod.rs` files** - use `module_name.rs` instead for better discoverability
- Follow the pattern: `src/media.rs` not `src/media/mod.rs`

### Async I/O Requirements
**In server code (realize-core), all I/O must be async:**
- Use `tokio::fs::*` instead of `std::fs::*`
- Use `tokio::io::*` instead of `std::io::*`
- Wrap blocking operations in `tokio::task::spawn_blocking`
- Always call `flush().await?` after async writes
- Use `tokio::fs::metadata(&path).await.is_ok()` instead of `path.exists()`

### FUSE Testing Requirements
**When testing FUSE filesystem operations:**
- **Never use synchronous I/O** on FUSE mountpoints - it will hang in single-threaded test environments
- **Always use `tokio::fs::*`** functions when accessing files through FUSE
- **Avoid manual file handle management** - use `tokio::fs::write()` instead of opening/writing/closing manually
- **Use `tokio::task::spawn_blocking`** for system calls like `chown()` that must be synchronous
- **Always drop/close file handles** before attempting to unmount FUSE filesystems
- **Use timeout wrappers** for potentially blocking operations: `tokio::time::timeout(Duration::from_secs(30), operation)`
- **Test both FUSE mountpoint and underlying datadir** to verify writes are properly persisted

### Cap'n Proto Patterns
When working with Cap'n Proto:
- Use `reborrow()` when building nested structures to avoid ownership issues
- Create lists with correct size: `init_res(items.len() as u32)`
- Handle union variants carefully - they return `Result<Reader, Error>`
- Boolean methods return `bool`, not `Result`
- Write round-trip tests for all data structures
- Use `Promise::from_future()` for async RPC methods

### Unit Testing
- Place unit tests in the same file as the code being tested
- Make sure unit tests cover all cases, avoid relying only no integration tests
- Follow the AAA (Arrange-Act-Assert) pattern
- Avoid mocking; prefer having tests working on the real implementation
- Keep tests focused: if several aspects need to be tested, instead of
  writing a large test write multiple tests, well named, with any
  common setup kept in the fixture.

- Never just sleep some arbitrary duration in tests to wait for
  something ha happen. It's not reliable as tests can run fast or slow
  depending on the machine.

  To wait for something to happen, you might, either:
    - wait for some notification to be sent to a notification channel,
      when there is one
    - poll at regular intervals (and give up after a reasonable delay, like 5 or 10s)

### Integration Testing
- Integration tests must use `*_integration_test.rs` suffix
- Use `assert_cmd::Command` instead of `std::process::Command`
- Use `assert_fs` for filesystem operations in tests
- Focus on CLI behavior and end-to-end scenarios

### Dependencies
The project has specific dependency requirements:
- **FUSE**: Linux (fuse3/libfuse3-dev) or macOS (fuse-t)
- **Cap'n Proto**: Required for protocol buffer code generation
- **Tokio**: Async runtime with specific feature flags for each crate
- **Testing**: Uses nextest, assert_cmd, assert_fs for comprehensive testing

### Platform Support
- **Linux**: Full functionality including filesystem layer and inotify
- **macOS**: Subset of functionality with read-only filesystem view
- **Windows**: Planned but not yet supported

Development should prioritize Linux compatibility, with macOS as a secondary target.

## FUSE Development Notes

### Common FUSE Test Issues
**If FUSE tests hang, check for these common causes:**
1. **Synchronous I/O deadlock**: Using `std::fs::*` instead of `tokio::fs::*` on FUSE mountpoints
2. **File handle leaks**: Not properly closing file handles before unmounting
3. **Blocking system calls**: Using synchronous syscalls like `chown()` without `spawn_blocking`
4. **Manual file handle management**: Using `OpenOptions` with manual `read`/`write`/`flush` instead of `tokio::fs::write()`

### FUSE Testing Patterns
**Successful FUSE test pattern:**
```rust
// ✅ Good - uses tokio::fs throughout
let content = tokio::fs::read_to_string(&fuse_path).await?;
tokio::fs::write(&fuse_path, "new content").await?;
let updated = tokio::fs::read_to_string(&fuse_path).await?;

// ❌ Bad - will hang in tests
let content = std::fs::read_to_string(&fuse_path)?;
let mut file = std::fs::OpenOptions::new().write(true).open(&fuse_path)?;
file.write_all(b"data")?;  // This will hang
```

### FUSE Architecture
**File State Transitions:**
- **Cached Files**: Exist in cache, accessed via `Download` reader for read-only operations
- **Realized Files**: When opened for write, cached files are "realized" to actual files in the datadir
- **Real Files**: Files that exist in the datadir and are mapped via the `realpaths` HashMap
- Write operations always require file realization, which downloads the complete file content first
