# Current Context: Realize File Syncer

## Current Work Focus

The project has a foundational implementation for peer-to-peer file synchronization, focusing on the "move directories" scenario. Core RPC services, data models, and basic storage/network layers are in place.

The specification documents outline a more comprehensive system involving "Real" (local) and "Unreal" (cached remote) filesystems, with plans for NFSv3 and OverlayFS integration.

## Recent Changes/State

*   **Core Library (`realize-lib`):**
    *   `model`: Defines `Arena`, `Peer`, `Path`, `ByteRange(s)`, `Hash`, `Signature`, `Delta`.
    *   `network`: Implements `RealizeService` RPC (via `tarpc`) with TLS security (raw public keys), TCP transport, metrics, and peer configuration.
    *   `storage`: Implements `LocalStorage` for "Real" files, including path resolution and partial file handling. `ArenaConfig` for storage paths.
    *   `logic`: Contains the `movedirs` algorithm for synchronizing directories between two peers.
    *   `utils`: Provides helpers for async operations, hashing (BLAKE2b-256), and logging.
    *   In progress implementation of inotify-based file change notification between remote peers in `network/rpc/history.rs`, `storage/real.rs`, `storage/history.rs`
*   **Daemon (`realize-daemon`):** Implements the server-side executable that hosts `RealizeService`. Handles configuration loading (TOML) and signal handling.
*   **CLI (`realize-cmd`):** Implements the client-side tool for initiating sync operations. Handles CLI arguments, progress display, and metrics reporting.
*   **Specification:**
    *   `design.md`: Overall system design, including Real/Unreal concepts, layers, and technical choices (redb, NFSv3, OverlayFS).
    *   `movedirs.md`: Details the initial sync algorithm and `RealizeService` definition.
    *   `unreal.md`: Describes the cache-based remote filesystem (NFSv3, redb, Merkle tree blobs).
    *   `real.md`: Describes merging local and Unreal filesystems (OverlayFS, history tracking via inotify/xattrs).
    *   `future.md`: Lists planned enhancements like household definitions, Merkle tree file hashes, multi-peer syncing design, and various optimizations.

## Next Steps (Inferred from `future.md` and spec TODOs)

*   **Household Definition:** Implement the TOML-based household configuration.
*   **Merkle Tree Hashing:** Integrate Merkle trees for file hashing.
*   **Unreal Filesystem Implementation:** Develop the `redb`-based cache and `nfsserve` integration for the "Unreal" filesystem.
*   **Real Filesystem (OverlayFS):** Implement OverlayFS integration for Linux.
*   **Multi-Peer Syncing:** Design and implement robust multi-peer synchronization logic beyond the initial two-peer `movedirs` algorithm.
*   **Conflict Resolution:** Enhance conflict resolution beyond the initial "don't resolve" strategy.
*   **Cross-Platform Read-Write Unreal:** Extend the "Unreal" filesystem to be read-write for macOS and Windows.
*   Address various TODOs and optimizations listed in `future.md` (e.g., compression, error message improvements, read optimizations).
